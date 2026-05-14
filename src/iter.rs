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
use crate::versions::Versions;
use bytes::Bytes;
use ferntree::iter::{Range as FernRange, RangeRev};
use ferntree::Tree;
use std::collections::btree_map::Range as TreeRange;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::ops::Bound;
use std::ops::ControlFlow;

// Tree fanout used by the surrealmx datastore tree.
// Kept aliased here so iterator types match the tree configured in `inner.rs`.
const IC: usize = 64;
const LC: usize = 64;

/// Forward or reverse iterator over the underlying tree.
pub(crate) enum TreeIterState<'a> {
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
		match direction {
			Direction::Forward => {
				TreeIterState::Forward(tree.range(Bound::Included(beg), Bound::Excluded(end)))
			}
			Direction::Reverse => {
				TreeIterState::Reverse(tree.range_rev(Bound::Included(beg), Bound::Excluded(end)))
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
pub(crate) fn for_each_in_range<F>(
	tree: &Tree<Bytes, Versions>,
	beg: &Bytes,
	end: &Bytes,
	mut f: F,
) where
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
		let Some((k, v)) = iter.next() else { break };
		if k >= end {
			break;
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

/// Three-way merge iterator over tree, merge queue, and current transaction
/// writesets.
pub struct MergeIterator<'a> {
	// Source iterators
	pub(crate) tree_iter: TreeIterState<'a>,
	pub(crate) self_iter: TreeRange<'a, Bytes, Option<Bytes>>,

	// Join entries from merge queue, consumed from front (forward) or back (reverse)
	pub(crate) join_entries: VecDeque<(Bytes, Option<Bytes>)>,

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
	pub(crate) fn new(
		tree_iter: TreeIterState<'a>,
		join_storage: BTreeMap<Bytes, Option<Bytes>>,
		mut self_iter: TreeRange<'a, Bytes, Option<Bytes>>,
		direction: Direction,
		version: u64,
		skip: usize,
	) -> Self {
		let mut me = MergeIterator {
			tree_iter,
			self_iter: TreeRange::default(),
			join_entries: VecDeque::new(),
			tree_next: None,
			join_next: None,
			self_next: None,
			direction,
			version,
			skip_remaining: skip,
		};

		// Convert BTreeMap to VecDeque for O(1) sequential access
		me.join_entries = join_storage.into_iter().collect();
		me.join_next = match direction {
			Direction::Forward => me.join_entries.pop_front(),
			Direction::Reverse => me.join_entries.pop_back(),
		};

		// Prime the transaction-side iterator
		me.self_next = match direction {
			Direction::Forward => self_iter.next(),
			Direction::Reverse => self_iter.next_back(),
		};
		me.self_iter = self_iter;

		// Prime the tree-side cache
		me.tree_next = Self::fetch_tree_entry(&mut me.tree_iter, version);

		me
	}

	/// Peek at the next tree entry and cache its key + existence at the
	/// current MVCC version. Value resolution is deferred to `peek_tree_value`
	/// so paths that only need keys or counts don't pay the byte clone.
	#[inline]
	fn fetch_tree_entry(tree_iter: &mut TreeIterState<'_>, version: u64) -> CachedTreeEntry {
		match tree_iter {
			TreeIterState::Forward(range) => {
				range.peek().map(|(k, v)| (k.clone(), v.exists_version(version)))
			}
			TreeIterState::Reverse(range) => {
				range.peek().map(|(k, v)| (k.clone(), v.exists_version(version)))
			}
		}
	}

	/// Fetch the value at the current MVCC version for the entry that the
	/// tree iterator is currently parked on. Call this in `Iterator::next`
	/// right before advancing past the entry.
	#[inline]
	fn peek_tree_value(&mut self) -> Option<Bytes> {
		let version = self.version;
		match &mut self.tree_iter {
			TreeIterState::Forward(range) => {
				range.peek().and_then(|(_, v)| v.fetch_version(version))
			}
			TreeIterState::Reverse(range) => {
				range.peek().and_then(|(_, v)| v.fetch_version(version))
			}
		}
	}

	#[inline]
	fn advance_join(&mut self) {
		self.join_next = match self.direction {
			Direction::Forward => self.join_entries.pop_front(),
			Direction::Reverse => self.join_entries.pop_back(),
		};
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
		match &mut self.tree_iter {
			TreeIterState::Forward(range) => {
				range.next();
			}
			TreeIterState::Reverse(range) => {
				range.next();
			}
		}
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
					let skip_tree =
						self.tree_key() == self.join_next.as_ref().map(|(jk, _)| jk);

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

					// Resolve the value lazily while the tree iter is still
					// parked on this entry, then clone the key and advance.
					let value = if exists {
						self.peek_tree_value()
					} else {
						None
					};
					let key = self.tree_next.as_ref().map(|(k, _)| k.clone()).unwrap();
					self.advance_tree();
					return Some((key, value));
				}
				KeySource::None => return None,
			}
		}
	}
}
