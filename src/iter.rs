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
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::map::Range as SkipRange;
use parking_lot::RwLock;
use std::collections::btree_map::Range as TreeRange;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::ops::Bound;

/// Owned range bounds for the skip list range iterator.
/// Using owned Bytes avoids lifetime coupling between range bounds
/// and the MergeIterator, enabling persistent storage (e.g., in a Cursor).
pub(crate) type SkipBounds = (Bound<Bytes>, Bound<Bytes>);

/// Three-way merge iterator over tree, merge queue, and current transaction writesets
pub struct MergeIterator<'a> {
	// Source iterators
	pub(crate) tree_iter: SkipRange<'a, Bytes, SkipBounds, Bytes, RwLock<Versions>>,
	pub(crate) self_iter: TreeRange<'a, Bytes, Option<Bytes>>,

	// Join entries from merge queue, consumed from front (forward) or back (reverse)
	pub(crate) join_entries: VecDeque<(Bytes, Option<Bytes>)>,

	// Current buffered entries from each source
	pub(crate) tree_next: Option<Entry<'a, Bytes, RwLock<Versions>>>,
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
		mut tree_iter: SkipRange<'a, Bytes, SkipBounds, Bytes, RwLock<Versions>>,
		join_storage: BTreeMap<Bytes, Option<Bytes>>,
		mut self_iter: TreeRange<'a, Bytes, Option<Bytes>>,
		direction: Direction,
		version: u64,
		skip: usize,
	) -> Self {
		// Get initial entries based on direction
		let tree_next = match direction {
			Direction::Forward => tree_iter.next(),
			Direction::Reverse => tree_iter.next_back(),
		};

		let self_next = match direction {
			Direction::Forward => self_iter.next(),
			Direction::Reverse => self_iter.next_back(),
		};

		// Convert BTreeMap to VecDeque for O(1) sequential access
		let mut join_entries: VecDeque<(Bytes, Option<Bytes>)> =
			join_storage.into_iter().collect();

		// Get first join entry based on direction
		let join_next = match direction {
			Direction::Forward => join_entries.pop_front(),
			Direction::Reverse => join_entries.pop_back(),
		};

		MergeIterator {
			tree_iter,
			self_iter,
			join_entries,
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
		self.join_next = match self.direction {
			Direction::Forward => self.join_entries.pop_front(),
			Direction::Reverse => self.join_entries.pop_back(),
		};
	}

	/// Get next entry existence only (no key or value cloning) - optimized for counting
	pub fn next_count(&mut self) -> Option<bool> {
		loop {
			// Find the next key to process (smallest for Forward, largest for Reverse)
			let mut next_key: Option<&Bytes> = None;
			let mut next_source = KeySource::None;

			// Check self iterator (highest priority)
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
			if let Some(t_entry) = &self.tree_next {
				let tk = t_entry.key();
				let should_use = match (next_key, &self.direction) {
					(None, _) => true,
					(Some(k), Direction::Forward) => tk < k,
					(Some(k), Direction::Reverse) => tk > k,
				};
				if should_use {
					next_source = KeySource::Datastore;
				}
			}

			// Process the selected source
			let exists = match next_source {
				KeySource::Transaction => {
					let (sk, sv) = self.self_next.unwrap();
					let exists = sv.is_some();

					// Advance self iterator
					self.self_next = match self.direction {
						Direction::Forward => self.self_iter.next(),
						Direction::Reverse => self.self_iter.next_back(),
					};

					// Skip same key in other iterators
					if let Some((jk, _)) = &self.join_next {
						if jk == sk {
							self.advance_join();
						}
					}
					if let Some(t_entry) = &self.tree_next {
						if t_entry.key() == sk {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}
					}

					exists
				}
				KeySource::Committed => {
					let exists = self.join_next.as_ref().unwrap().1.is_some();

					// Check if we need to skip same key in tree before advancing join
					let should_skip_tree = if let Some(t_entry) = &self.tree_next {
						if let Some((jk, _)) = &self.join_next {
							t_entry.key() == jk
						} else {
							false
						}
					} else {
						false
					};

					// Advance join iterator
					self.advance_join();

					// Skip same key in tree iterator if needed
					if should_skip_tree {
						self.tree_next = match self.direction {
							Direction::Forward => self.tree_iter.next(),
							Direction::Reverse => self.tree_iter.next_back(),
						};
					}

					exists
				}
				KeySource::Datastore => {
					let t_entry = self.tree_next.as_ref().unwrap();
					let tv = match t_entry.value().try_read() {
						Some(guard) => guard,
						None => t_entry.value().read(),
					};
					let exists = tv.exists_version(self.version);
					drop(tv);

					// Advance tree iterator
					self.tree_next = match self.direction {
						Direction::Forward => self.tree_iter.next(),
						Direction::Reverse => self.tree_iter.next_back(),
					};

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
			// Find the next key to process (smallest for Forward, largest for Reverse)
			let mut next_key: Option<&Bytes> = None;
			let mut next_source = KeySource::None;

			// Check self iterator (highest priority)
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
			if let Some(t_entry) = &self.tree_next {
				let tk = t_entry.key();
				let should_use = match (next_key, &self.direction) {
					(None, _) => true,
					(Some(k), Direction::Forward) => tk < k,
					(Some(k), Direction::Reverse) => tk > k,
				};
				if should_use {
					next_source = KeySource::Datastore;
				}
			}

			// Process the selected source - first determine if entry exists
			match next_source {
				KeySource::Transaction => {
					let (sk, sv) = self.self_next.unwrap();
					let exists = sv.is_some();

					// Store key reference for later cloning if needed
					let key_ref = sk;

					// Advance self iterator
					self.self_next = match self.direction {
						Direction::Forward => self.self_iter.next(),
						Direction::Reverse => self.self_iter.next_back(),
					};

					// Skip same key in other iterators
					if let Some((jk, _)) = &self.join_next {
						if jk == key_ref {
							self.advance_join();
						}
					}
					if let Some(t_entry) = &self.tree_next {
						if t_entry.key() == key_ref {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}
					}

					// Handle skipping BEFORE cloning
					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

					// Only clone if we're returning it
					return Some((key_ref.clone(), exists));
				}
				KeySource::Committed => {
					let (jk, jv) = self.join_next.as_ref().unwrap();

					// Check if we should skip (only skip existing entries)
					if jv.is_some() && self.skip_remaining > 0 {
						// Check if we need to skip same key in tree before advancing join
						let should_skip_tree = if let Some(t_entry) = &self.tree_next {
							t_entry.key() == jk
						} else {
							false
						};

						// Advance join iterator
						self.advance_join();

						// Skip same key in tree iterator if needed
						if should_skip_tree {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}

						self.skip_remaining -= 1;
						continue;
					}

					// Read existence before advancing (avoids value clone)
					let exists = jv.is_some();
					let key = jk.clone();

					// Advance join iterator
					self.advance_join();

					// Skip same key in tree iterator
					if let Some(t_entry) = &self.tree_next {
						if t_entry.key() == &key {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}
					}

					return Some((key, exists));
				}
				KeySource::Datastore => {
					let t_entry = self.tree_next.as_ref().unwrap();
					let tv = match t_entry.value().try_read() {
						Some(guard) => guard,
						None => t_entry.value().read(),
					};
					let exists = tv.exists_version(self.version);
					drop(tv);

					// Handle skipping BEFORE cloning the key
					if exists && self.skip_remaining > 0 {
						// Advance tree iterator
						self.tree_next = match self.direction {
							Direction::Forward => self.tree_iter.next(),
							Direction::Reverse => self.tree_iter.next_back(),
						};
						self.skip_remaining -= 1;
						continue;
					}

					// Only clone key if we're returning it
					let tk = t_entry.key().clone();

					// Advance tree iterator
					self.tree_next = match self.direction {
						Direction::Forward => self.tree_iter.next(),
						Direction::Reverse => self.tree_iter.next_back(),
					};

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
			// Find the next key to process (smallest for Forward, largest for Reverse)
			let mut next_key: Option<&Bytes> = None;
			let mut next_source = KeySource::None;

			// Check self iterator (highest priority)
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
			if let Some(t_entry) = &self.tree_next {
				let tk = t_entry.key();
				let should_use = match (next_key, &self.direction) {
					(None, _) => true,
					(Some(k), Direction::Forward) => tk < k,
					(Some(k), Direction::Reverse) => tk > k,
				};
				if should_use {
					next_source = KeySource::Datastore;
				}
			}

			// Process the selected source
			match next_source {
				KeySource::Transaction => {
					let (sk, sv) = self.self_next.unwrap();
					let exists = sv.is_some();

					// Store key reference for deferred cloning
					let key_ref = sk;

					// Advance self iterator
					self.self_next = match self.direction {
						Direction::Forward => self.self_iter.next(),
						Direction::Reverse => self.self_iter.next_back(),
					};

					// Skip same key in other iterators
					if let Some((jk, _)) = &self.join_next {
						if jk == key_ref {
							self.advance_join();
						}
					}
					if let Some(t_entry) = &self.tree_next {
						if t_entry.key() == key_ref {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}
					}

					// Check if we should skip (only skip existing entries)
					if exists && self.skip_remaining > 0 {
						self.skip_remaining -= 1;
						continue;
					}

					// Only clone key and value when returning
					return Some((key_ref.clone(), sv.clone()));
				}
				KeySource::Committed => {
					let (jk, jv) = self.join_next.as_ref().unwrap();

					// Check if we should skip (only skip existing entries)
					if jv.is_some() && self.skip_remaining > 0 {
						// Check if we need to skip same key in tree before advancing join
						let should_skip_tree = if let Some(t_entry) = &self.tree_next {
							t_entry.key() == jk
						} else {
							false
						};

						// Advance join iterator
						self.advance_join();

						// Skip same key in tree iterator if needed
						if should_skip_tree {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}

						self.skip_remaining -= 1;
						continue;
					}

					// Only clone key and value when returning
					let key = jk.clone();
					let value_opt = jv.clone();

					// Advance join iterator
					self.advance_join();

					// Skip same key in tree iterator
					if let Some(t_entry) = &self.tree_next {
						if t_entry.key() == &key {
							self.tree_next = match self.direction {
								Direction::Forward => self.tree_iter.next(),
								Direction::Reverse => self.tree_iter.next_back(),
							};
						}
					}

					return Some((key, value_opt));
				}
				KeySource::Datastore => {
					let t_entry = self.tree_next.as_ref().unwrap();
					let tv = match t_entry.value().try_read() {
						Some(guard) => guard,
						None => t_entry.value().read(),
					};
					let value_opt = tv.fetch_version(self.version);
					let exists = value_opt.is_some();
					drop(tv);

					// Handle skipping BEFORE cloning the key
					if exists && self.skip_remaining > 0 {
						// Advance tree iterator
						self.tree_next = match self.direction {
							Direction::Forward => self.tree_iter.next(),
							Direction::Reverse => self.tree_iter.next_back(),
						};
						self.skip_remaining -= 1;
						continue;
					}

					// Only clone key if we're returning it
					let key_clone = t_entry.key().clone();

					// Advance tree iterator
					self.tree_next = match self.direction {
						Direction::Forward => self.tree_iter.next(),
						Direction::Reverse => self.tree_iter.next_back(),
					};

					return Some((key_clone, value_opt));
				}
				KeySource::None => return None,
			}
		}
	}
}
