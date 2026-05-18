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

//! This module contains the merge iterator for scanning across multiple data
//! sources.

use crate::direction::Direction;
use crate::inner::DataValue;
use crate::queue::Merge;
use bytes::Bytes;
use scc::Guard;
use scc::tree_index::Range as TreeRange;
use scc::TreeIndex;
use std::collections::btree_map::Range as BTreeRange;
use std::ops::Bound;
use std::sync::Arc;

/// Owned range bounds for the tree range iterator.
/// Using owned Bytes avoids lifetime coupling between range bounds and the
/// MergeIterator, enabling persistent storage (e.g., in a Cursor).
pub(crate) type SkipBounds = (Bound<Bytes>, Bound<Bytes>);

/// Concrete type of the `scc::TreeIndex` range iterator over the datastore.
type DataRange<'a> = TreeRange<'a, Bytes, DataValue, Bytes, SkipBounds>;

/// Wraps a `scc::TreeIndex::range` iterator together with its owning EBR
/// `Guard`. `scc::TreeIndex` returns references that are only valid while the
/// `Guard` is alive, so the iterator and the guard must be held together.
///
/// Drop order matters: `range` is dropped before `_guard` (struct fields drop
/// in declaration order), so any references the iterator may still hold are
/// torn down while the EBR pin is still active.
pub(crate) struct TreeIter<'a> {
	// Range is constructed with the guard's lifetime extended to `'a` via
	// `transmute`. Safety relies on the guard living as long as the range —
	// guaranteed by storing the guard in the same struct and on field-order
	// drop semantics. The range is dropped first; the guard last.
	range: DataRange<'a>,
	_guard: Box<Guard>,
}

impl<'a> TreeIter<'a> {
	/// Build a new tree iterator over `[beg, end)` for the given tree.
	#[inline]
	pub(crate) fn new(tree: &'a TreeIndex<Bytes, DataValue>, beg: &Bytes, end: &Bytes) -> Self {
		let guard = Box::new(Guard::new());
		// SAFETY: `guard` is owned by this struct via `Box`, so the heap
		// allocation has a stable address. The transmuted reference is used
		// to construct `range`, which is dropped before `_guard` thanks to
		// the field declaration order. No other code may observe the
		// transmuted reference.
		let guard_ref: &'a Guard =
			unsafe { std::mem::transmute::<&Guard, &'a Guard>(&*guard) };
		let bounds: SkipBounds =
			(Bound::Included(beg.clone()), Bound::Excluded(end.clone()));
		let range = tree.range::<Bytes, _>(bounds, guard_ref);
		Self {
			range,
			_guard: guard,
		}
	}

	/// Yield the next entry in `direction` order.
	#[inline]
	pub(crate) fn next(&mut self, direction: Direction) -> Option<(&Bytes, &DataValue)> {
		match direction {
			Direction::Forward => self.range.next(),
			Direction::Reverse => self.range.next_back(),
		}
	}
}

/// Cached entry from the tree iterator: (key, exists_at_version).
///
/// Only the key and existence flag are precomputed — the value at the current
/// version is fetched lazily at emit time, while the tree iterator is parked
/// past this entry. This avoids cloning a value byte slice for entries that
/// get skipped or that the caller only needs existence for (`next_count`,
/// `next_key`).
///
/// The lazy fetch is a separate `peek_with` lookup on the underlying tree
/// rather than holding the iterator on the entry. This trades one extra tree
/// lookup per yielded value for the simplicity of single-threaded iterator
/// advancement and avoids `scc::TreeIndex`'s warning about stale value
/// references during concurrent leaf splits — the cached `Arc<RwLock>` we
/// pull out via `peek_with` is the canonical pointer, not a snapshot copy.
type CachedTreeEntry = Option<(Bytes, DataValue, bool)>;

/// Lazy k-way merge iterator over committed merge-queue writesets.
///
/// Yields `(Bytes, Option<Bytes>)` pairs in sorted order with newest-wins
/// dedup. Sources must be passed in newest-first order (index 0 = newest).
/// On a tie, the lowest-index source wins; older sources at the same key
/// are advanced past it.
///
/// Holds an `Arc<Merge>` per source to keep the underlying `Arc<BTreeMap>`
/// alive without storing any borrows from it; advancement re-seeks the
/// BTreeMap each step (O(log n) per advance).
pub(crate) struct MergeQueueIter {
	sources: Vec<Arc<Merge>>,
	heads: Vec<Option<(Bytes, Option<Bytes>)>>,
	beg: Bytes,
	end: Bytes,
	direction: Direction,
}

impl MergeQueueIter {
	/// Build a new lazy merge iterator.
	///
	/// `sources` must be ordered newest-first (typically by collecting
	/// `transaction_merge_queue.range(..=version).rev()`). `beg` is included,
	/// `end` is excluded.
	pub(crate) fn new(
		sources: Vec<Arc<Merge>>,
		beg: Bytes,
		end: Bytes,
		direction: Direction,
	) -> Self {
		let mut heads = Vec::with_capacity(sources.len());
		for src in &sources {
			heads.push(seek_in_writeset(src, direction, &beg, &end, None));
		}
		Self {
			sources,
			heads,
			beg,
			end,
			direction,
		}
	}
}

/// Seek the next entry within `[beg, end)` for `direction`, optionally past
/// `after`. Returns owned `(Bytes, Option<Bytes>)` (refcount clones only).
fn seek_in_writeset(
	src: &Arc<Merge>,
	direction: Direction,
	beg: &Bytes,
	end: &Bytes,
	after: Option<&Bytes>,
) -> Option<(Bytes, Option<Bytes>)> {
	let ws = &src.writeset;
	let entry = match (direction, after) {
		(Direction::Forward, None) => {
			ws.range::<Bytes, _>((Bound::Included(beg), Bound::Excluded(end))).next()
		}
		(Direction::Forward, Some(k)) => {
			ws.range::<Bytes, _>((Bound::Excluded(k), Bound::Excluded(end))).next()
		}
		(Direction::Reverse, None) => {
			ws.range::<Bytes, _>((Bound::Included(beg), Bound::Excluded(end))).next_back()
		}
		(Direction::Reverse, Some(k)) => {
			ws.range::<Bytes, _>((Bound::Included(beg), Bound::Excluded(k))).next_back()
		}
	};
	entry.map(|(k, v)| (k.clone(), v.clone()))
}

impl Iterator for MergeQueueIter {
	type Item = (Bytes, Option<Bytes>);

	fn next(&mut self) -> Option<Self::Item> {
		// Find the winning source: smallest (Forward) or largest (Reverse)
		// head. On ties, lower index wins (newest), so a strict comparison
		// keeps the first-seen source as the winner.
		let mut winner: Option<usize> = None;
		for (i, head) in self.heads.iter().enumerate() {
			let Some((k, _)) = head else {
				continue;
			};
			match winner {
				None => winner = Some(i),
				Some(wi) => {
					let (wk, _) = self.heads[wi].as_ref().unwrap();
					let take = match self.direction {
						Direction::Forward => k < wk,
						Direction::Reverse => k > wk,
					};
					if take {
						winner = Some(i);
					}
				}
			}
		}
		let winner = winner?;
		let (out_key, out_val) = self.heads[winner].take().unwrap();
		// Discard older duplicates at the same key, re-seeking past it.
		for i in (winner + 1)..self.heads.len() {
			let same = self.heads[i].as_ref().map(|(k, _)| k == &out_key).unwrap_or(false);
			if same {
				let new = seek_in_writeset(
					&self.sources[i],
					self.direction,
					&self.beg,
					&self.end,
					Some(&out_key),
				);
				self.heads[i] = new;
			}
		}
		// Advance the winning source.
		let new = seek_in_writeset(
			&self.sources[winner],
			self.direction,
			&self.beg,
			&self.end,
			Some(&out_key),
		);
		self.heads[winner] = new;
		Some((out_key, out_val))
	}
}

/// Three-way merge iterator over tree, merge queue, and current transaction
/// writesets.
pub struct MergeIterator<'a> {
	// Source iterators
	pub(crate) tree_iter: TreeIter<'a>,
	pub(crate) self_iter: BTreeRange<'a, Bytes, Option<Bytes>>,

	// Lazy iterator over committed merge-queue writesets
	pub(crate) join_iter: Box<dyn Iterator<Item = (Bytes, Option<Bytes>)> + 'a>,

	// Current buffered entries from each source
	pub(crate) tree_next: CachedTreeEntry,
	pub(crate) join_next: Option<(Bytes, Option<Bytes>)>,
	pub(crate) self_next: Option<(&'a Bytes, &'a Option<Bytes>)>,

	// Iterator configuration
	pub(crate) direction: Direction,
	pub(crate) version: u64,

	// Number of items to skip
	pub(crate) skip_remaining: usize,
}

// Source of a key during three-way merge
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeySource {
	None,
	Datastore,
	Committed,
	Transaction,
}

impl<'a> MergeIterator<'a> {
	pub fn new(
		mut tree_iter: TreeIter<'a>,
		mut join_iter: Box<dyn Iterator<Item = (Bytes, Option<Bytes>)> + 'a>,
		mut self_iter: BTreeRange<'a, Bytes, Option<Bytes>>,
		direction: Direction,
		version: u64,
		skip: usize,
	) -> Self {
		// Prime the tree-side from the underlying scc Range. We pull a
		// `(key, Arc, exists)` triple up front and keep them cached. The
		// Arc clone is cheap (refcount) and lets us release the borrow on
		// the iterator so subsequent advances don't conflict.
		let tree_next = fetch_tree_entry(&mut tree_iter, direction, version);

		let self_next = match direction {
			Direction::Forward => self_iter.next(),
			Direction::Reverse => self_iter.next_back(),
		};

		let join_next = join_iter.next();

		MergeIterator {
			tree_iter,
			self_iter,
			join_iter,
			tree_next,
			join_next,
			self_next,
			direction,
			version,
			skip_remaining: skip,
		}
	}

	#[inline]
	fn advance_join(&mut self) {
		self.join_next = self.join_iter.next();
	}

	#[inline]
	fn advance_self(&mut self) {
		self.self_next = match self.direction {
			Direction::Forward => self.self_iter.next(),
			Direction::Reverse => self.self_iter.next_back(),
		};
	}

	#[inline]
	fn advance_tree(&mut self) {
		self.tree_next = fetch_tree_entry(&mut self.tree_iter, self.direction, self.version);
	}

	#[inline]
	fn tree_key(&self) -> Option<&Bytes> {
		self.tree_next.as_ref().map(|(k, _, _)| k)
	}

	#[inline]
	fn tree_exists(&self) -> bool {
		self.tree_next.as_ref().map(|(_, _, e)| *e).unwrap_or(false)
	}

	/// Decide which source has the next key to process. Pure inspection,
	/// no advancing.
	#[inline]
	fn next_source(&self) -> KeySource {
		let mut next_key: Option<&Bytes> = None;
		let mut next_source = KeySource::None;

		// Check self iterator (highest priority on tie)
		if let Some((sk, _)) = self.self_next {
			next_key = Some(sk);
			next_source = KeySource::Transaction;
		}

		// Check join iterator (merge queue)
		if let Some((jk, _)) = &self.join_next {
			let should_use = match (next_key, &self.direction) {
				(None, _) => true,
				(Some(k), Direction::Forward) => jk < k,
				(Some(k), Direction::Reverse) => jk > k,
			};
			if should_use {
				next_key = Some(jk);
				next_source = KeySource::Committed;
			} else if next_key == Some(jk) {
				// Same key in both self and join - self wins
				next_source = KeySource::Transaction;
			}
		}

		// Check tree iterator
		if let Some(tk) = self.tree_key() {
			let should_use = match (next_key, &self.direction) {
				(None, _) => true,
				(Some(k), Direction::Forward) => tk < k,
				(Some(k), Direction::Reverse) => tk > k,
			};
			if should_use {
				next_source = KeySource::Datastore;
			}
		}

		next_source
	}

	/// Get next entry existence only (no key or value cloning) - optimized
	/// for counting.
	pub fn next_count(&mut self) -> Option<bool> {
		loop {
			let exists = match self.next_source() {
				KeySource::Transaction => {
					let (sk, sv) = self.self_next.unwrap();
					let exists = sv.is_some();
					let skip_join = self.join_next.as_ref().map(|(jk, _)| jk) == Some(sk);
					let skip_tree = self.tree_key() == Some(sk);

					self.advance_self();
					if skip_join {
						self.advance_join();
					}
					if skip_tree {
						self.advance_tree();
					}

					exists
				}
				KeySource::Committed => {
					let exists = self.join_next.as_ref().unwrap().1.is_some();
					let skip_tree = self.tree_key() == self.join_next.as_ref().map(|(jk, _)| jk);

					self.advance_join();
					if skip_tree {
						self.advance_tree();
					}

					exists
				}
				KeySource::Datastore => {
					let exists = self.tree_exists();
					self.advance_tree();
					exists
				}
				KeySource::None => return None,
			};

			if exists && self.skip_remaining > 0 {
				self.skip_remaining -= 1;
				continue;
			}

			return Some(exists);
		}
	}

	/// Get next entry with key (no value cloning) - optimized for key
	/// iteration.
	pub fn next_key(&mut self) -> Option<(Bytes, bool)> {
		loop {
			match self.next_source() {
				KeySource::Transaction => {
					let (sk, sv) = self.self_next.unwrap();
					let exists = sv.is_some();
					let key_ref = sk;
					let skip_join = self.join_next.as_ref().map(|(jk, _)| jk) == Some(sk);
					let skip_tree = self.tree_key() == Some(sk);

					self.advance_self();
					if skip_join {
						self.advance_join();
					}
					if skip_tree {
						self.advance_tree();
					}

					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

					return Some((key_ref.clone(), exists));
				}
				KeySource::Committed => {
					let (jk, jv) = self.join_next.as_ref().unwrap();

					if jv.is_some() && self.skip_remaining > 0 {
						let skip_tree = self.tree_key() == Some(jk);
						self.advance_join();
						if skip_tree {
							self.advance_tree();
						}
						self.skip_remaining -= 1;
						continue;
					}

					let exists = jv.is_some();
					let key = jk.clone();
					let skip_tree = self.tree_key() == Some(&key);

					self.advance_join();
					if skip_tree {
						self.advance_tree();
					}

					return Some((key, exists));
				}
				KeySource::Datastore => {
					let exists = self.tree_exists();

					if exists && self.skip_remaining > 0 {
						self.advance_tree();
						self.skip_remaining -= 1;
						continue;
					}

					let key = self.tree_next.as_ref().map(|(k, _, _)| k.clone()).unwrap();
					self.advance_tree();
					return Some((key, exists));
				}
				KeySource::None => return None,
			}
		}
	}
}

/// Pull the next entry from the underlying tree iterator, capturing
/// `(key, Arc<RwLock<Versions>>, exists_at_version)`. The Arc clone is a
/// refcount bump; resolving existence is cheap (no value clone). The actual
/// value bytes are fetched lazily in `Iterator::next` only when needed.
#[inline]
fn fetch_tree_entry(
	tree_iter: &mut TreeIter<'_>,
	direction: Direction,
	version: u64,
) -> CachedTreeEntry {
	tree_iter.next(direction).map(|(k, arc)| {
		let exists = match arc.try_read() {
			Some(g) => g.exists_version(version),
			None => arc.read().exists_version(version),
		};
		(k.clone(), arc.clone(), exists)
	})
}

impl<'a> Iterator for MergeIterator<'a> {
	type Item = (Bytes, Option<Bytes>);

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			match self.next_source() {
				KeySource::Transaction => {
					let (sk, sv) = self.self_next.unwrap();
					let key_ref = sk;
					let exists = sv.is_some();
					let skip_join = self.join_next.as_ref().map(|(jk, _)| jk) == Some(sk);
					let skip_tree = self.tree_key() == Some(sk);

					self.advance_self();
					if skip_join {
						self.advance_join();
					}
					if skip_tree {
						self.advance_tree();
					}

					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

					return Some((key_ref.clone(), sv.clone()));
				}
				KeySource::Committed => {
					let (jk, jv) = self.join_next.as_ref().unwrap();

					if jv.is_some() && self.skip_remaining > 0 {
						let skip_tree = self.tree_key() == Some(jk);
						self.advance_join();
						if skip_tree {
							self.advance_tree();
						}
						self.skip_remaining -= 1;
						continue;
					}

					let key = jk.clone();
					let value = jv.clone();
					let skip_tree = self.tree_key() == Some(&key);

					self.advance_join();
					if skip_tree {
						self.advance_tree();
					}

					return Some((key, value));
				}
				KeySource::Datastore => {
					let exists = self.tree_exists();

					if exists && self.skip_remaining > 0 {
						self.advance_tree();
						self.skip_remaining -= 1;
						continue;
					}

					// Resolve the value lazily from the cached Arc, then
					// advance past the entry.
					let (key, arc, _) = self.tree_next.as_ref().unwrap();
					let value = if exists {
						match arc.try_read() {
							Some(g) => g.fetch_version(self.version),
							None => arc.read().fetch_version(self.version),
						}
					} else {
						None
					};
					let key = key.clone();
					self.advance_tree();
					return Some((key, value));
				}
				KeySource::None => return None,
			}
		}
	}
}
