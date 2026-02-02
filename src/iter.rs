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
		let tree_iter = match direction {
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

		MergeIterator {
			tree_iter,
			self_iter,
			join_storage,
			join_next,
			self_next,
			direction,
			version,
			skip_remaining: skip,
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

	/// Peek at the next tree entry without advancing, returning cloned key for comparison
	fn peek_tree_key(&mut self) -> Option<Bytes> {
		match &mut self.tree_iter {
			TreeIterState::Forward(range) => range.peek().map(|(k, _)| k.clone()),
			TreeIterState::Reverse(range) => range.peek().map(|(k, _)| k.clone()),
		}
	}

	/// Check if tree entry exists at current version (for use after determining source)
	fn tree_exists_version(&mut self) -> bool {
		match &mut self.tree_iter {
			TreeIterState::Forward(range) => {
				range.peek().is_some_and(|(_, v)| v.exists_version(self.version))
			}
			TreeIterState::Reverse(range) => {
				range.peek().is_some_and(|(_, v)| v.exists_version(self.version))
			}
		}
	}

	/// Fetch value from tree at current version
	fn tree_fetch_version(&mut self) -> Option<Bytes> {
		match &mut self.tree_iter {
			TreeIterState::Forward(range) => {
				range.peek().and_then(|(_, v)| v.fetch_version(self.version))
			}
			TreeIterState::Reverse(range) => {
				range.peek().and_then(|(_, v)| v.fetch_version(self.version))
			}
		}
	}

	/// Advance the tree iterator
	fn advance_tree(&mut self) {
		match &mut self.tree_iter {
			TreeIterState::Forward(range) => {
				range.next();
			}
			TreeIterState::Reverse(range) => {
				range.next();
			}
		}
	}

	/// Determine which source has the next key to process
	fn determine_next_source(
		&mut self,
	) -> (KeySource, Option<Bytes>, Option<Bytes>, Option<Bytes>) {
		// Gather keys from all sources (cloning to avoid borrow issues)
		let self_key = self.self_next.map(|(k, _)| k.clone());
		let join_key = self.join_next.as_ref().map(|(k, _)| k.clone());
		let tree_key = self.peek_tree_key();

		let mut next_key: Option<&Bytes> = None;
		let mut next_source = KeySource::None;

		// Check self iterator (highest priority)
		if let Some(ref sk) = self_key {
			next_key = Some(sk);
			next_source = KeySource::Transaction;
		}

		// Check join iterator (merge queue)
		if let Some(ref jk) = join_key {
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
		if let Some(ref tk) = tree_key {
			let should_use = match (next_key, &self.direction) {
				(None, _) => true,
				(Some(k), Direction::Forward) => tk < k,
				(Some(k), Direction::Reverse) => tk > k,
			};
			if should_use {
				next_source = KeySource::Datastore;
			}
		}

		(next_source, self_key, join_key, tree_key)
	}

	/// Get next entry existence only (no key or value cloning) - optimized for counting
	pub fn next_count(&mut self) -> Option<bool> {
		loop {
			let (next_source, self_key, join_key, tree_key) = self.determine_next_source();

			// Process the selected source
			let exists = match next_source {
				KeySource::Transaction => {
					let (_, sv) = self.self_next.unwrap();
					let exists = sv.is_some();
					let sk = self_key.unwrap();

					// Advance self iterator
					self.self_next = match self.direction {
						Direction::Forward => self.self_iter.next(),
						Direction::Reverse => self.self_iter.next_back(),
					};

					// Skip same key in other iterators
					if join_key.as_ref() == Some(&sk) {
						self.advance_join();
					}
					if tree_key.as_ref() == Some(&sk) {
						self.advance_tree();
					}

					exists
				}
				KeySource::Committed => {
					let (_, jv) = self.join_next.as_ref().unwrap();
					let exists = jv.is_some();
					let jk = join_key.unwrap();

					// Check if we need to skip same key in tree before advancing join
					let should_skip_tree = tree_key.as_ref() == Some(&jk);

					// Advance join iterator
					self.advance_join();

					// Skip same key in tree iterator if needed
					if should_skip_tree {
						self.advance_tree();
					}

					exists
				}
				KeySource::Datastore => {
					let exists = self.tree_exists_version();

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
			let (next_source, self_key, join_key, tree_key) = self.determine_next_source();

			// Process the selected source
			match next_source {
				KeySource::Transaction => {
					let (_, sv) = self.self_next.unwrap();
					let exists = sv.is_some();
					let sk = self_key.unwrap();

					// Advance self iterator
					self.self_next = match self.direction {
						Direction::Forward => self.self_iter.next(),
						Direction::Reverse => self.self_iter.next_back(),
					};

					// Skip same key in other iterators
					if join_key.as_ref() == Some(&sk) {
						self.advance_join();
					}
					if tree_key.as_ref() == Some(&sk) {
						self.advance_tree();
					}

					// Handle skipping
					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

					return Some((sk, exists));
				}
				KeySource::Committed => {
					let (_, jv) = self.join_next.as_ref().unwrap();
					let exists = jv.is_some();
					let jk = join_key.unwrap();

					// Check if we should skip (only skip existing entries)
					if exists && self.skip_remaining > 0 {
						// Check if we need to skip same key in tree before advancing join
						let should_skip_tree = tree_key.as_ref() == Some(&jk);

						// Advance join iterator
						self.advance_join();

						// Skip same key in tree iterator if needed
						if should_skip_tree {
							self.advance_tree();
						}

						self.skip_remaining -= 1;
						continue;
					}

					// Advance join iterator
					self.advance_join();

					// Skip same key in tree iterator
					if tree_key.as_ref() == Some(&jk) {
						self.advance_tree();
					}

					return Some((jk, exists));
				}
				KeySource::Datastore => {
					let exists = self.tree_exists_version();
					let tk = tree_key.unwrap();

					// Handle skipping
					if exists && self.skip_remaining > 0 {
						// Advance tree iterator
						self.advance_tree();
						self.skip_remaining -= 1;
						continue;
					}

					// Advance tree iterator
					self.advance_tree();

					return Some((tk, exists));
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
			let (next_source, self_key, join_key, tree_key) = self.determine_next_source();

			// Process the selected source
			match next_source {
				KeySource::Transaction => {
					let (_, sv) = self.self_next.unwrap();
					let exists = sv.is_some();
					let sk = self_key.unwrap();
					let value = sv.clone();

					// Advance self iterator
					self.self_next = match self.direction {
						Direction::Forward => self.self_iter.next(),
						Direction::Reverse => self.self_iter.next_back(),
					};

					// Skip same key in other iterators
					if join_key.as_ref() == Some(&sk) {
						self.advance_join();
					}
					if tree_key.as_ref() == Some(&sk) {
						self.advance_tree();
					}

					// Handle skipping
					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

					return Some((sk, value));
				}
				KeySource::Committed => {
					let (_, jv) = self.join_next.as_ref().unwrap();
					let exists = jv.is_some();
					let jk = join_key.unwrap();
					let value = jv.clone();

					// Check if we should skip (only skip existing entries)
					if exists && self.skip_remaining > 0 {
						// Check if we need to skip same key in tree before advancing join
						let should_skip_tree = tree_key.as_ref() == Some(&jk);

						// Advance join iterator
						self.advance_join();

						// Skip same key in tree iterator if needed
						if should_skip_tree {
							self.advance_tree();
						}

						self.skip_remaining -= 1;
						continue;
					}

					// Advance join iterator
					self.advance_join();

					// Skip same key in tree iterator
					if tree_key.as_ref() == Some(&jk) {
						self.advance_tree();
					}

					return Some((jk, value));
				}
				KeySource::Datastore => {
					let value = self.tree_fetch_version();
					let exists = value.is_some();
					let tk = tree_key.unwrap();

					// Handle skipping
					if exists && self.skip_remaining > 0 {
						// Advance tree iterator
						self.advance_tree();
						self.skip_remaining -= 1;
						continue;
					}

					// Advance tree iterator
					self.advance_tree();

					return Some((tk, value));
				}
				KeySource::None => return None,
			}
		}
	}
}
