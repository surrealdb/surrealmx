// Copyright © SurrealDB Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This module contains the cursor and iterator types for range scans.

use crate::direction::Direction;
use crate::inner::Inner;
use crate::iter::MergeIterator;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

// --------------------------------------------------
// Cursor
// --------------------------------------------------

/// A bidirectional cursor over a range of keys in the database.
///
/// The cursor provides low-level access to iterate forward and backward
/// through a key range, with support for seeking to specific positions.
///
/// # Example
///
/// ```ignore
/// let mut cursor = tx.cursor("a".."z")?;
/// while cursor.valid() {
///     if let Some(key) = cursor.key() {
///         println!("Key: {:?}", key);
///     }
///     if let Some(value) = cursor.value() {
///         println!("Value: {:?}", value);
///     }
///     cursor.next();
/// }
/// ```
pub struct Cursor<'a> {
	/// Reference to the database inner structure
	database: &'a Arc<Inner>,
	/// Reference to the transaction's writeset
	writeset: &'a BTreeMap<Bytes, Option<Bytes>>,
	/// Original range start bound
	beg: Bytes,
	/// Original range end bound
	end: Bytes,
	/// Transaction version for MVCC
	version: u64,
	/// Combined writeset from merge queue (owned, built at cursor creation)
	combined_writeset: BTreeMap<Bytes, Option<Bytes>>,
	/// Current direction of iteration
	direction: Direction,
	/// Current cached entry (key, value) - None means invalid position
	current: Option<(Bytes, Option<Bytes>)>,
	/// Whether the cursor has been positioned
	positioned: bool,
	/// Persistent merge iterator, reused across sequential next/prev calls
	iter: Option<MergeIterator<'a>>,
}

impl<'a> Cursor<'a> {
	/// Create a new cursor over a range.
	///
	/// The cursor starts in an unpositioned state. Call `seek_to_first()`,
	/// `seek_to_last()`, or `seek()` to position it.
	pub(crate) fn new(
		database: &'a Arc<Inner>,
		writeset: &'a BTreeMap<Bytes, Option<Bytes>>,
		beg: Bytes,
		end: Bytes,
		version: u64,
	) -> Self {
		// Build combined writeset from merge queue entries.
		// Fast path: skip entirely when the merge queue is empty (common case).
		let combined_writeset: BTreeMap<Bytes, Option<Bytes>> =
			if database.transaction_merge_queue.is_empty() {
				BTreeMap::new()
			} else {
				let mut ws = BTreeMap::new();
				for entry in database.transaction_merge_queue.range(..=version).rev() {
					for (k, v) in entry.value().writeset.range::<Bytes, _>(&beg..&end) {
						ws.entry(k.clone()).or_insert_with(|| v.clone());
					}
				}
				ws
			};

		Cursor {
			database,
			writeset,
			beg,
			end,
			version,
			combined_writeset,
			direction: Direction::Forward,
			current: None,
			positioned: false,
			iter: None,
		}
	}

	/// Check if the cursor is positioned at a valid entry.
	#[inline]
	pub fn valid(&self) -> bool {
		self.current.is_some()
	}

	/// Get the current key, or None if the cursor is not valid.
	#[inline]
	pub fn key(&self) -> Option<&Bytes> {
		self.current.as_ref().map(|(k, _)| k)
	}

	/// Get the current value, or None if the cursor is not valid or the entry
	/// is deleted.
	#[inline]
	pub fn value(&self) -> Option<&Bytes> {
		self.current.as_ref().and_then(|(_, v)| v.as_ref())
	}

	/// Check if the current entry exists (is not deleted).
	#[inline]
	pub fn exists(&self) -> bool {
		self.current.as_ref().map(|(_, v)| v.is_some()).unwrap_or(false)
	}

	/// Move the cursor to the first entry in the range.
	pub fn seek_to_first(&mut self) {
		self.direction = Direction::Forward;
		self.positioned = true;
		let beg = self.beg.clone();
		let end = self.end.clone();
		self.create_iterator(&beg, &end, Direction::Forward);
		self.advance_current();
	}

	/// Move the cursor to the last entry in the range.
	pub fn seek_to_last(&mut self) {
		self.direction = Direction::Reverse;
		self.positioned = true;
		let beg = self.beg.clone();
		let end = self.end.clone();
		self.create_iterator(&beg, &end, Direction::Reverse);
		self.advance_current();
	}

	/// Seek to the first entry with a key >= the given key.
	pub fn seek<K: AsRef<[u8]>>(&mut self, key: K) {
		self.direction = Direction::Forward;
		self.positioned = true;
		let key_bytes = Bytes::copy_from_slice(key.as_ref());
		// Clamp to range bounds
		let seek_key = if key_bytes < self.beg {
			self.beg.clone()
		} else if key_bytes >= self.end {
			// Beyond range, invalidate
			self.current = None;
			self.iter = None;
			return;
		} else {
			key_bytes
		};
		let end = self.end.clone();
		self.create_iterator(&seek_key, &end, Direction::Forward);
		self.advance_current();
	}

	/// Seek to the last entry with a key <= the given key.
	pub fn seek_for_prev<K: AsRef<[u8]>>(&mut self, key: K) {
		self.direction = Direction::Reverse;
		self.positioned = true;
		let key_bytes = Bytes::copy_from_slice(key.as_ref());
		// Clamp to range bounds
		let seek_key = if key_bytes >= self.end {
			self.end.clone()
		} else if key_bytes < self.beg {
			// Before range, invalidate
			self.current = None;
			self.iter = None;
			return;
		} else {
			key_bytes
		};
		let beg = self.beg.clone();
		self.create_iterator(&beg, &seek_key, Direction::Reverse);
		self.advance_current();
	}

	/// Move to the next entry (forward direction).
	pub fn next(&mut self) {
		if !self.positioned {
			self.seek_to_first();
			return;
		}

		// If direction changed, recreate the iterator from the current position
		if matches!(self.direction, Direction::Reverse) {
			self.direction = Direction::Forward;
			if let Some((ref key, _)) = self.current {
				let next_start = next_key(key);
				if next_start >= self.end {
					self.current = None;
					self.iter = None;
					return;
				}
				let end = self.end.clone();
				self.create_iterator(&next_start, &end, Direction::Forward);
			} else {
				self.iter = None;
			}
			self.advance_current();
			return;
		}

		// Same direction (forward), reuse existing iterator
		self.advance_current();
	}

	/// Move to the previous entry (reverse direction).
	pub fn prev(&mut self) {
		if !self.positioned {
			self.seek_to_last();
			return;
		}

		// If direction changed, recreate the iterator from the current position
		if matches!(self.direction, Direction::Forward) {
			self.direction = Direction::Reverse;
			if let Some((ref key, _)) = self.current {
				if key <= &self.beg {
					self.current = None;
					self.iter = None;
					return;
				}
				let beg = self.beg.clone();
				let key_clone = key.clone();
				self.create_iterator(&beg, &key_clone, Direction::Reverse);
			} else {
				self.iter = None;
			}
			self.advance_current();
			return;
		}

		// Same direction (reverse), reuse existing iterator
		self.advance_current();
	}

	// --------------------------------------------------
	// Internal methods
	// --------------------------------------------------

	/// Create a new merge iterator over the given range and direction.
	/// Replaces any existing iterator.
	fn create_iterator(&mut self, start: &Bytes, end: &Bytes, direction: Direction) {
		let join: BTreeMap<Bytes, Option<Bytes>> = self
			.combined_writeset
			.range::<Bytes, _>(start..end)
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect();

		self.iter = Some(MergeIterator::new(
			self.database
				.datastore
				.range((Bound::Included(start.clone()), Bound::Excluded(end.clone()))),
			join,
			self.writeset.range::<Bytes, _>(start..end),
			direction,
			self.version,
			0,
		));
	}

	/// Advance the persistent iterator and update the current entry.
	#[inline]
	fn advance_current(&mut self) {
		self.current = match &mut self.iter {
			Some(iter) => iter.next(),
			None => None,
		};
	}
}

// --------------------------------------------------
// KeyIterator
// --------------------------------------------------

/// An iterator over keys in a range.
///
/// This iterator skips deleted entries and only yields existing keys.
///
/// # Example
///
/// ```ignore
/// for key in tx.keys("a".."z")? {
///     println!("Key: {:?}", key);
/// }
/// ```
pub struct KeyIterator<'a> {
	/// The underlying cursor
	cursor: Cursor<'a>,
	/// Whether this is a reverse iterator
	reverse: bool,
}

impl<'a> KeyIterator<'a> {
	/// Create a new forward key iterator.
	pub(crate) fn new(cursor: Cursor<'a>) -> Self {
		let mut iter = KeyIterator {
			cursor,
			reverse: false,
		};
		// Position at the first entry
		iter.cursor.seek_to_first();
		iter
	}

	/// Create a new reverse key iterator.
	pub(crate) fn new_reverse(cursor: Cursor<'a>) -> Self {
		let mut iter = KeyIterator {
			cursor,
			reverse: true,
		};
		// Position at the last entry
		iter.cursor.seek_to_last();
		iter
	}
}

impl<'a> Iterator for KeyIterator<'a> {
	type Item = Bytes;

	fn next(&mut self) -> Option<Self::Item> {
		// Skip deleted entries
		while self.cursor.valid() {
			if self.cursor.exists() {
				let key = self.cursor.key()?.clone();
				if self.reverse {
					self.cursor.prev();
				} else {
					self.cursor.next();
				}
				return Some(key);
			}
			if self.reverse {
				self.cursor.prev();
			} else {
				self.cursor.next();
			}
		}
		None
	}
}

impl<'a> DoubleEndedIterator for KeyIterator<'a> {
	fn next_back(&mut self) -> Option<Self::Item> {
		// When iterating from the back, we go in the opposite direction
		let was_reverse = self.reverse;
		self.reverse = !was_reverse;

		// If we haven't positioned yet for back iteration, do so now
		if was_reverse {
			// Was reverse (going backward), now go forward from start
			if !self.cursor.positioned {
				self.cursor.seek_to_first();
			}
		} else {
			// Was forward (going forward), now go backward from end
			if !self.cursor.positioned {
				self.cursor.seek_to_last();
			}
		}

		let result = self.next();
		self.reverse = was_reverse;
		result
	}
}

// --------------------------------------------------
// ScanIterator
// --------------------------------------------------

/// An iterator over key-value pairs in a range.
///
/// This iterator skips deleted entries and only yields existing key-value
/// pairs.
///
/// # Example
///
/// ```ignore
/// for (key, value) in tx.scan("a".."z")? {
///     println!("Key: {:?}, Value: {:?}", key, value);
/// }
/// ```
pub struct ScanIterator<'a> {
	/// The underlying cursor
	cursor: Cursor<'a>,
	/// Whether this is a reverse iterator
	reverse: bool,
}

impl<'a> ScanIterator<'a> {
	/// Create a new forward scan iterator.
	pub(crate) fn new(cursor: Cursor<'a>) -> Self {
		let mut iter = ScanIterator {
			cursor,
			reverse: false,
		};
		// Position at the first entry
		iter.cursor.seek_to_first();
		iter
	}

	/// Create a new reverse scan iterator.
	pub(crate) fn new_reverse(cursor: Cursor<'a>) -> Self {
		let mut iter = ScanIterator {
			cursor,
			reverse: true,
		};
		// Position at the last entry
		iter.cursor.seek_to_last();
		iter
	}
}

impl<'a> Iterator for ScanIterator<'a> {
	type Item = (Bytes, Bytes);

	fn next(&mut self) -> Option<Self::Item> {
		// Skip deleted entries
		while self.cursor.valid() {
			if let Some(value) = self.cursor.value() {
				let key = self.cursor.key()?.clone();
				let value = value.clone();
				if self.reverse {
					self.cursor.prev();
				} else {
					self.cursor.next();
				}
				return Some((key, value));
			}
			if self.reverse {
				self.cursor.prev();
			} else {
				self.cursor.next();
			}
		}
		None
	}
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
	fn next_back(&mut self) -> Option<Self::Item> {
		// When iterating from the back, we go in the opposite direction
		let was_reverse = self.reverse;
		self.reverse = !was_reverse;

		// If we haven't positioned yet for back iteration, do so now
		if was_reverse {
			// Was reverse (going backward), now go forward from start
			if !self.cursor.positioned {
				self.cursor.seek_to_first();
			}
		} else {
			// Was forward (going forward), now go backward from end
			if !self.cursor.positioned {
				self.cursor.seek_to_last();
			}
		}

		let result = self.next();
		self.reverse = was_reverse;
		result
	}
}

// --------------------------------------------------
// Helper functions
// --------------------------------------------------

/// Compute the next key after the given key (lexicographically).
/// This is used for exclusive lower bounds.
fn next_key(key: &Bytes) -> Bytes {
	let mut next = key.to_vec();
	// Append a zero byte to get the next key
	next.push(0);
	Bytes::from(next)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_next_key() {
		let key = Bytes::from_static(b"hello");
		let next = next_key(&key);
		assert_eq!(next.as_ref(), b"hello\0");
		assert!(next > key);
	}
}
