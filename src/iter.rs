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

//! This module contains the merge iterator for scanning across multiple data sources.

use crate::direction::Direction;
use crate::versions::Versions;
use bytes::Bytes;
use ferntree::iter::Range as FernRange;
use ferntree::iter::RangeRev;
use ferntree::GenericTree;
use std::collections::btree_map::Range as TreeRange;
use std::collections::BTreeMap;
use std::ops::Bound;

/// Enum to handle forward/reverse iteration with different iterator types
pub(crate) enum TreeIterState<'a> {
	/// Forward iteration using the Range iterator with peek()
	Forward(FernRange<'a, Bytes, Versions, 64, 64>),
	/// Reverse iteration using RangeRev with peek()
	Reverse(RangeRev<'a, Bytes, Versions, 64, 64>),
}

/// Cached tree entry: (key, exists_at_version, value_at_version)
/// We cache this to avoid repeated peek() calls and cloning on every comparison
type CachedTreeEntry = Option<(Bytes, bool, Option<Bytes>)>;

/// Three-way merge iterator over tree, merge queue, and current transaction writesets
pub struct MergeIterator<'a> {
	// Source iterator (forward or reverse)
	pub(crate) tree_iter: TreeIterState<'a>,
	pub(crate) self_iter: TreeRange<'a, Bytes, Option<Bytes>>,

	// Join iterator and its storage
	pub(crate) join_storage: BTreeMap<Bytes, Option<Bytes>>,

	// Current buffered entries from join and self sources
	pub(crate) join_next: Option<(Bytes, Option<Bytes>)>,
	pub(crate) self_next: Option<(&'a Bytes, &'a Option<Bytes>)>,

	// Cached tree entry to avoid repeated peek() and cloning
	pub(crate) tree_next: CachedTreeEntry,

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
		tree: &'a GenericTree<Bytes, Versions, 64, 64>,
		beg: &Bytes,
		end: &Bytes,
		join_storage: BTreeMap<Bytes, Option<Bytes>>,
		mut self_iter: TreeRange<'a, Bytes, Option<Bytes>>,
		direction: Direction,
		version: u64,
		skip: usize,
	) -> Self {
		// Create the appropriate iterator based on direction
		let mut tree_iter = match direction {
			Direction::Forward => {
				TreeIterState::Forward(tree.range(Bound::Included(beg), Bound::Excluded(end)))
			}
			Direction::Reverse => {
				TreeIterState::Reverse(tree.range_rev(Bound::Included(beg), Bound::Excluded(end)))
			}
		};

		let self_next = match direction {
			Direction::Forward => self_iter.next(),
			Direction::Reverse => self_iter.next_back(),
		};

		// Get first join entry
		let join_next = match direction {
			Direction::Forward => join_storage.iter().next().map(|(k, v)| (k.clone(), v.clone())),
			Direction::Reverse => {
				join_storage.iter().next_back().map(|(k, v)| (k.clone(), v.clone()))
			}
		};

		// Cache the first tree entry
		let tree_next = Self::fetch_tree_entry(&mut tree_iter, version);

		MergeIterator {
			tree_iter,
			self_iter,
			join_storage,
			join_next,
			self_next,
			tree_next,
			direction,
			version,
			skip_remaining: skip,
		}
	}

	/// Fetch and cache the next tree entry (key, exists, value)
	/// This clones once when advancing, then we can access via references
	fn fetch_tree_entry(tree_iter: &mut TreeIterState<'_>, version: u64) -> CachedTreeEntry {
		match tree_iter {
			TreeIterState::Forward(range) => range.peek().map(|(k, v)| {
				let exists = v.exists_version(version);
				let value = v.fetch_version(version);
				(k.clone(), exists, value)
			}),
			TreeIterState::Reverse(range) => range.peek().map(|(k, v)| {
				let exists = v.exists_version(version);
				let value = v.fetch_version(version);
				(k.clone(), exists, value)
			}),
		}
	}

	fn advance_join(&mut self) {
		if let Some((current_key, _)) = &self.join_next {
			// Find next entry after current key
			let next = match self.direction {
				Direction::Forward => self
					.join_storage
					.range((Bound::Excluded(current_key.clone()), Bound::Unbounded))
					.next()
					.map(|(k, v)| (k.clone(), v.clone())),
				Direction::Reverse => self
					.join_storage
					.range((Bound::Unbounded, Bound::Excluded(current_key.clone())))
					.next_back()
					.map(|(k, v)| (k.clone(), v.clone())),
			};
			self.join_next = next;
		}
	}

	fn advance_self(&mut self) {
		self.self_next = match self.direction {
			Direction::Forward => self.self_iter.next(),
			Direction::Reverse => self.self_iter.next_back(),
		};
	}

	/// Advance the tree iterator and update the cache
	fn advance_tree(&mut self) {
		match &mut self.tree_iter {
			TreeIterState::Forward(range) => {
				range.next();
			}
			TreeIterState::Reverse(range) => {
				range.next();
			}
		}
		// Update the cache
		self.tree_next = Self::fetch_tree_entry(&mut self.tree_iter, self.version);
	}

	// Reference accessors for cached tree entry
	#[inline]
	fn tree_key(&self) -> Option<&Bytes> {
		self.tree_next.as_ref().map(|(k, _, _)| k)
	}

	#[inline]
	fn tree_exists(&self) -> bool {
		self.tree_next.as_ref().map(|(_, e, _)| *e).unwrap_or(false)
	}

	// Reference accessors for join entry
	#[inline]
	fn join_key(&self) -> Option<&Bytes> {
		self.join_next.as_ref().map(|(k, _)| k)
	}

	// Reference accessors for self entry
	#[inline]
	fn self_key(&self) -> Option<&Bytes> {
		self.self_next.map(|(k, _)| k)
	}

	/// Determine which source has the next key to process
	/// Returns only the KeySource - no cloning happens here
	fn determine_next_source(&self) -> KeySource {
		let self_key = self.self_key();
		let join_key = self.join_key();
		let tree_key = self.tree_key();

		let mut next_key: Option<&Bytes> = None;
		let mut next_source = KeySource::None;

		// Check self iterator (highest priority)
		if let Some(sk) = self_key {
			next_key = Some(sk);
			next_source = KeySource::Transaction;
		}

		// Check join iterator (merge queue)
		if let Some(jk) = join_key {
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
		if let Some(tk) = tree_key {
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

	/// Get next entry existence only (no key or value cloning) - optimized for counting
	pub fn next_count(&mut self) -> Option<bool> {
		loop {
			let source = self.determine_next_source();

			let exists = match source {
				KeySource::Transaction => {
					let (sk, sv) = self.self_next.unwrap();
					let exists = sv.is_some();

					// Check if we need to skip same key in other iterators before advancing
					let skip_join = self.join_key() == Some(sk);
					let skip_tree = self.tree_key() == Some(sk);

					// Advance self iterator
					self.advance_self();

					// Skip same key in other iterators
					if skip_join {
						self.advance_join();
					}
					if skip_tree {
						self.advance_tree();
					}

					exists
				}
				KeySource::Committed => {
					let (_, jv) = self.join_next.as_ref().unwrap();
					let exists = jv.is_some();

					// Check if we need to skip same key in tree before advancing join
					let skip_tree = self.tree_key() == self.join_key();

					// Advance join iterator
					self.advance_join();

					// Skip same key in tree iterator if needed
					if skip_tree {
						self.advance_tree();
					}

					exists
				}
				KeySource::Datastore => {
					let exists = self.tree_exists();

					// Advance tree iterator
					self.advance_tree();

					exists
				}
				KeySource::None => return None,
			};

			// Handle skipping
			if exists && self.skip_remaining > 0 {
				self.skip_remaining -= 1;
				continue;
			}

			return Some(exists);
		}
	}

	/// Get next entry with key (no value cloning) - optimized for key iteration
	pub fn next_key(&mut self) -> Option<(Bytes, bool)> {
		loop {
			let source = self.determine_next_source();

			match source {
				KeySource::Transaction => {
					let (sk, sv) = self.self_next.unwrap();
					let exists = sv.is_some();
					let key = sk.clone(); // Clone only when returning

					// Check if we need to skip same key in other iterators before advancing
					let skip_join = self.join_key() == Some(sk);
					let skip_tree = self.tree_key() == Some(sk);

					// Advance self iterator
					self.advance_self();

					// Skip same key in other iterators
					if skip_join {
						self.advance_join();
					}
					if skip_tree {
						self.advance_tree();
					}

					// Handle skipping
					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

					return Some((key, exists));
				}
				KeySource::Committed => {
					let (jk, jv) = self.join_next.as_ref().unwrap();
					let exists = jv.is_some();
					let key = jk.clone(); // Clone only when returning

					// Check if we need to skip same key in tree before advancing join
					let skip_tree = self.tree_key() == Some(jk);

					// Advance join iterator
					self.advance_join();

					// Skip same key in tree iterator if needed
					if skip_tree {
						self.advance_tree();
					}

					// Handle skipping
					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

					return Some((key, exists));
				}
				KeySource::Datastore => {
					let exists = self.tree_exists();
					// Take key from cache (will be repopulated on advance)
					let key = self.tree_next.as_ref().map(|(k, _, _)| k.clone()).unwrap();

					// Advance tree iterator
					self.advance_tree();

					// Handle skipping
					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

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
			let source = self.determine_next_source();

			match source {
				KeySource::Transaction => {
					let (sk, sv) = self.self_next.unwrap();
					let exists = sv.is_some();
					let key = sk.clone(); // Clone only when returning
					let value = sv.clone();

					// Check if we need to skip same key in other iterators before advancing
					let skip_join = self.join_key() == Some(sk);
					let skip_tree = self.tree_key() == Some(sk);

					// Advance self iterator
					self.advance_self();

					// Skip same key in other iterators
					if skip_join {
						self.advance_join();
					}
					if skip_tree {
						self.advance_tree();
					}

					// Handle skipping
					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

					return Some((key, value));
				}
				KeySource::Committed => {
					let (jk, jv) = self.join_next.as_ref().unwrap();
					let exists = jv.is_some();
					let key = jk.clone(); // Clone only when returning
					let value = jv.clone();

					// Check if we need to skip same key in tree before advancing join
					let skip_tree = self.tree_key() == Some(jk);

					// Advance join iterator
					self.advance_join();

					// Skip same key in tree iterator if needed
					if skip_tree {
						self.advance_tree();
					}

					// Handle skipping
					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

					return Some((key, value));
				}
				KeySource::Datastore => {
					let exists = self.tree_next.as_ref().map(|(_, e, _)| *e).unwrap_or(false);
					// Get key and value from cache
					let (key, value) =
						self.tree_next.as_ref().map(|(k, _, v)| (k.clone(), v.clone())).unwrap();

					// Advance tree iterator
					self.advance_tree();

					// Handle skipping
					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

					return Some((key, value));
				}
				KeySource::None => return None,
			}
		}
	}
}
