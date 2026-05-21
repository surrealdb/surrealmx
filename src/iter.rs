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
use crate::queue::Merge;
use crate::versions::Versions;
use bytes::Bytes;
use ferntree::iter::{Range as FernRange, RangeRev};
use ferntree::Tree;
use std::collections::btree_map::Range as TreeRange;
use std::ops::Bound;
use std::ops::ControlFlow;
use std::sync::Arc;

// Tree fanout used by the surrealmx datastore tree.
// Kept aliased here so iterator types match the tree configured in `inner.rs`.
const IC: usize = 64;
const LC: usize = 64;

/// Forward or reverse iterator over the underlying tree.
///
/// The struct carries the requested `[beg, end)` bounds in addition to the
/// inner ferntree iterator. `peek` and `next` re-check those bounds on every
/// access so the scan contract is enforced even if the underlying B+ tree
/// iterator drifts outside the range under concurrent leaf mutations.
pub(crate) struct TreeIterState<'a> {
	inner: TreeIterInner<'a>,
	beg: Bytes,
	end: Bytes,
}

enum TreeIterInner<'a> {
	Forward(FernRange<'a, Bytes, Versions, IC, LC>),
	Reverse(RangeRev<'a, Bytes, Versions, IC, LC>),
}

impl<'a> TreeIterState<'a> {
	/// Build a tree iterator over `[beg, end)` for the given direction.
	#[inline]
	pub(crate) fn build(
		tree: &'a Tree<Bytes, Versions>,
		beg: &Bytes,
		end: &Bytes,
		direction: Direction,
	) -> Self {
		let inner = match direction {
			Direction::Forward => {
				TreeIterInner::Forward(tree.range(Bound::Included(beg), Bound::Excluded(end)))
			}
			Direction::Reverse => {
				TreeIterInner::Reverse(tree.range_rev(Bound::Included(beg), Bound::Excluded(end)))
			}
		};
		Self {
			inner,
			beg: beg.clone(),
			end: end.clone(),
		}
	}

	/// True when `k` lies inside `[beg, end)`.
	#[inline]
	fn in_range(&self, k: &Bytes) -> bool {
		k.as_ref() >= self.beg.as_ref() && k.as_ref() < self.end.as_ref()
	}

	/// Peek the next entry that lies inside `[beg, end)`, projecting it
	/// through `f` while the borrow on the underlying iterator is live.
	///
	/// Ferntree's `Range::peek` only validates the "far" bound (upper for
	/// forward, lower for reverse). Under concurrent leaf mutation the raw
	/// iterator can briefly surface a key on the wrong side of the "near"
	/// bound; we consume any such key and retry so callers only ever see
	/// in-range entries. The projection closure runs on the exact key that
	/// passed the bound check, so the returned data and the validation are
	/// always tied to the same peek.
	#[inline]
	fn peek_next_in_range<R>(
		&mut self,
		mut f: impl FnMut(&Bytes, &Versions) -> R,
	) -> Option<(Bytes, R)> {
		loop {
			let entry = match &mut self.inner {
				TreeIterInner::Forward(range) => range.peek().map(|(k, v)| (k.clone(), f(k, v))),
				TreeIterInner::Reverse(range) => range.peek().map(|(k, v)| (k.clone(), f(k, v))),
			};
			match entry {
				Some((k, _)) if !self.in_range(&k) => self.advance(),
				other => return other,
			}
		}
	}

	/// Consume the entry the iterator is parked on, then re-park on the next
	/// in-range entry (if any). Mirrors the post-emit advance done by the
	/// merge layer.
	#[inline]
	fn advance(&mut self) {
		match &mut self.inner {
			TreeIterInner::Forward(range) => {
				range.next();
			}
			TreeIterInner::Reverse(range) => {
				range.next();
			}
		}
	}
}

/// Walk every `(key, versions)` pair in `[beg, end)` in forward order,
/// dispatching `f` per entry. The closure returns `ControlFlow::Break` to
/// stop iteration early.
///
/// Internally this uses ferntree's `for_each_in_leaf`, which processes a
/// whole leaf's `SmallVec<(K, V)>` in a tight loop without re-entering the
/// iterator state machine. The cross-leaf step still goes through the
/// regular `next()` path, so one entry per leaf boundary pays the normal
/// iterator-advance cost.
#[inline]
pub(crate) fn for_each_in_range<F>(tree: &Tree<Bytes, Versions>, beg: &Bytes, end: &Bytes, mut f: F)
where
	F: FnMut(&Bytes, &Versions) -> ControlFlow<()>,
{
	let mut iter = tree.raw_iter();
	iter.seek(beg);
	let mut stop = false;
	loop {
		let has_more_leaves = iter.for_each_in_leaf(|k, v| {
			if stop {
				return;
			}
			// Defensive bound check on both sides. The raw iterator was seeked
			// to `beg` and is bounded above by the `k >= end` early-exit, but
			// under concurrent leaf mutation it can briefly surface keys on
			// either side of the requested range; skip such stragglers so the
			// caller never observes them.
			if k < beg {
				return;
			}
			if k >= end {
				stop = true;
				return;
			}
			if matches!(f(k, v), ControlFlow::Break(())) {
				stop = true;
			}
		});
		if stop || !has_more_leaves {
			break;
		}
		// Advance into the next leaf. `next()` yields its first entry, which
		// would otherwise be skipped by `for_each_in_leaf`'s cursor handling.
		let Some((k, v)) = iter.next() else {
			break;
		};
		if k >= end {
			break;
		}
		if k < beg {
			continue;
		}
		if matches!(f(k, v), ControlFlow::Break(())) {
			break;
		}
	}
}

/// Cached entry from the tree iterator: (key, exists_at_version).
/// Only the key and existence flag are precomputed — the value at the current
/// version is fetched lazily at emit time, while the tree iterator is still
/// parked at this entry (so `peek()` still returns it). This avoids cloning a
/// value byte slice for entries that get skipped or that the caller only needs
/// existence for (`next_count`, `next_key`).
type CachedTreeEntry = Option<(Bytes, bool)>;

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
	pub(crate) tree_iter: TreeIterState<'a>,
	pub(crate) self_iter: TreeRange<'a, Bytes, Option<Bytes>>,

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
		tree_iter: TreeIterState<'a>,
		mut join_iter: Box<dyn Iterator<Item = (Bytes, Option<Bytes>)> + 'a>,
		mut self_iter: TreeRange<'a, Bytes, Option<Bytes>>,
		direction: Direction,
		version: u64,
		skip: usize,
	) -> Self {
		// Prime the join-side from the lazy iterator (direction-baked).
		let join_next = join_iter.next();

		// Prime the transaction-side iterator
		let self_next = match direction {
			Direction::Forward => self_iter.next(),
			Direction::Reverse => self_iter.next_back(),
		};

		let mut me = MergeIterator {
			tree_iter,
			self_iter,
			join_iter,
			tree_next: None,
			join_next,
			self_next,
			direction,
			version,
			skip_remaining: skip,
		};

		// Prime the tree-side cache (peek + resolve version once)
		me.tree_next = Self::fetch_tree_entry(&mut me.tree_iter, version);

		me
	}

	/// Peek at the next in-range tree entry and cache its key + existence at
	/// the current MVCC version. Value resolution is deferred to
	/// `peek_tree_value` so paths that only need keys or counts don't pay the
	/// byte clone.
	#[inline]
	fn fetch_tree_entry(tree_iter: &mut TreeIterState<'_>, version: u64) -> CachedTreeEntry {
		tree_iter.peek_next_in_range(|_, v| v.exists_version(version))
	}

	/// Fetch the value at the current MVCC version for the entry that the
	/// tree iterator is parked on, **provided** it still matches the entry
	/// the caller cached in `tree_next`. The merge layer relies on this
	/// equivalence: if the underlying iterator surfaces a different key
	/// (which should not happen — `peek` is idempotent under the shared-leaf
	/// guard) we treat the slot as deleted so the caller falls through to
	/// the next emit cycle instead of returning a mismatched value.
	#[inline]
	fn peek_tree_value(&mut self, expected_key: &Bytes) -> Option<Bytes> {
		let version = self.version;
		self.tree_iter
			.peek_next_in_range(|k, v| {
				if k == expected_key {
					v.fetch_version(version)
				} else {
					None
				}
			})
			.and_then(|(_, v)| v)
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
		self.tree_iter.advance();
		self.tree_next = Self::fetch_tree_entry(&mut self.tree_iter, self.version);
	}

	#[inline]
	fn tree_key(&self) -> Option<&Bytes> {
		self.tree_next.as_ref().map(|(k, _)| k)
	}

	#[inline]
	fn tree_exists(&self) -> bool {
		self.tree_next.as_ref().map(|(_, e)| *e).unwrap_or(false)
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

	/// Get next entry existence only (no key or value cloning) - optimized for
	/// counting.
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

					let key = self.tree_next.as_ref().map(|(k, _)| k.clone()).unwrap();
					self.advance_tree();
					return Some((key, exists));
				}
				KeySource::None => return None,
			}
		}
	}
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

					// Clone the cached key first, then resolve the value while
					// the tree iter is still parked on the entry. Passing the
					// cached key into `peek_tree_value` ties the returned value
					// to the same entry we validated in `fetch_tree_entry`.
					let key = self.tree_next.as_ref().map(|(k, _)| k.clone()).unwrap();
					let value = if exists {
						self.peek_tree_value(&key)
					} else {
						None
					};
					self.advance_tree();
					return Some((key, value));
				}
				KeySource::None => return None,
			}
		}
	}
}
