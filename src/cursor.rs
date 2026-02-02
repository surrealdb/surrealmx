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
		// Build combined writeset from merge queue entries
		let mut combined_writeset: BTreeMap<Bytes, Option<Bytes>> = BTreeMap::new();
		for entry in database.transaction_merge_queue.range(..=version).rev() {
			for (k, v) in entry.value().writeset.range::<Bytes, _>(&beg..&end) {
				combined_writeset.entry(k.clone()).or_insert_with(|| v.clone());
			}
		}

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

	/// Get the current value, or None if the cursor is not valid or the entry is deleted.
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
		self.seek_internal(&self.beg.clone());
	}

	/// Move the cursor to the last entry in the range.
	pub fn seek_to_last(&mut self) {
		self.direction = Direction::Reverse;
		self.positioned = true;
		self.seek_for_prev_internal(&self.end.clone());
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
			return;
		} else {
			key_bytes
		};
		self.seek_internal(&seek_key);
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
			return;
		} else {
			key_bytes
		};
		self.seek_for_prev_internal(&seek_key);
	}

	/// Move to the next entry (forward direction).
	pub fn next(&mut self) {
		if !self.positioned {
			self.seek_to_first();
			return;
		}

		// If direction changed, we need to handle it
		if matches!(self.direction, Direction::Reverse) {
			self.direction = Direction::Forward;
			// Continue from current position in forward direction
			if let Some((ref key, _)) = self.current {
				let next_key = key.clone();
				self.advance_forward_from(&next_key);
				return;
			}
		}

		// Advance forward from current position
		if let Some((ref key, _)) = self.current.clone() {
			self.advance_forward_from(key);
		} else {
			// Already invalid, stay invalid
		}
	}

	/// Move to the previous entry (reverse direction).
	pub fn prev(&mut self) {
		if !self.positioned {
			self.seek_to_last();
			return;
		}

		// If direction changed, we need to handle it
		if matches!(self.direction, Direction::Forward) {
			self.direction = Direction::Reverse;
			// Continue from current position in reverse direction
			if let Some((ref key, _)) = self.current {
				let prev_key = key.clone();
				self.advance_backward_from(&prev_key);
				return;
			}
		}

		// Advance backward from current position
		if let Some((ref key, _)) = self.current.clone() {
			self.advance_backward_from(key);
		} else {
			// Already invalid, stay invalid
		}
	}

	// --------------------------------------------------
	// Internal methods
	// --------------------------------------------------

	/// Internal seek to a key (forward direction).
	fn seek_internal(&mut self, seek_key: &Bytes) {
		// Create a new merge iterator starting from seek_key
		let mut iter = MergeIterator::new(
			&self.database.datastore,
			seek_key,
			&self.end,
			self.combined_writeset
				.range::<Bytes, _>(seek_key..&self.end)
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect(),
			self.writeset.range::<Bytes, _>(seek_key..&self.end),
			Direction::Forward,
			self.version,
			0,
		);

		// Get the first entry
		self.current = iter.next();
	}

	/// Internal seek for prev (reverse direction).
	fn seek_for_prev_internal(&mut self, seek_key: &Bytes) {
		// Create a new merge iterator for reverse iteration ending at seek_key
		let mut iter = MergeIterator::new(
			&self.database.datastore,
			&self.beg,
			seek_key,
			self.combined_writeset
				.range::<Bytes, _>(&self.beg..seek_key)
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect(),
			self.writeset.range::<Bytes, _>(&self.beg..seek_key),
			Direction::Reverse,
			self.version,
			0,
		);

		// Get the first entry (which is the last in the range due to reverse)
		self.current = iter.next();
	}

	/// Advance forward from the given key (exclusive).
	fn advance_forward_from(&mut self, from_key: &Bytes) {
		// We need to find the next key after from_key
		// Create an iterator starting just after from_key
		let next_start = next_key(from_key);
		if next_start >= self.end {
			self.current = None;
			return;
		}

		let mut iter = MergeIterator::new(
			&self.database.datastore,
			&next_start,
			&self.end,
			self.combined_writeset
				.range::<Bytes, _>(&next_start..&self.end)
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect(),
			self.writeset.range::<Bytes, _>(&next_start..&self.end),
			Direction::Forward,
			self.version,
			0,
		);

		self.current = iter.next();
	}

	/// Advance backward from the given key (exclusive).
	fn advance_backward_from(&mut self, from_key: &Bytes) {
		// We need to find the previous key before from_key
		if from_key <= &self.beg {
			self.current = None;
			return;
		}

		let mut iter = MergeIterator::new(
			&self.database.datastore,
			&self.beg,
			from_key,
			self.combined_writeset
				.range::<Bytes, _>(&self.beg..from_key)
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect(),
			self.writeset.range::<Bytes, _>(&self.beg..from_key),
			Direction::Reverse,
			self.version,
			0,
		);

		self.current = iter.next();
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
/// This iterator skips deleted entries and only yields existing key-value pairs.
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
