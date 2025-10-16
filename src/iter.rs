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
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::map::Range as SkipRange;
use parking_lot::RwLock;
use std::collections::btree_map::Range as TreeRange;
use std::ops::Bound;
use std::sync::Arc;

type RangeBounds<'a, K> = (Bound<&'a K>, Bound<&'a K>);

/// Two-way merge iterator over tree and current transaction writesets
pub struct MergeIterator<'a, K, V>
where
	K: Ord + Clone + Sync + Send + 'static,
	V: Eq + Clone + Sync + Send + 'static,
{
	// Source iterators
	pub(crate) tree_iter: SkipRange<'a, K, RangeBounds<'a, K>, K, RwLock<Versions<V>>>,
	pub(crate) self_iter: TreeRange<'a, K, Option<Arc<V>>>,

	// Current buffered entries from each source
	pub(crate) tree_next: Option<Entry<'a, K, RwLock<Versions<V>>>>,
	pub(crate) self_next: Option<(&'a K, &'a Option<Arc<V>>)>,

	// Iterator configuration
	pub(crate) direction: Direction,
	pub(crate) version: u64,

	// Number of items to skip
	pub(crate) skip_remaining: usize,
}

// Source of a key during two-way merge
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeySource {
	None,
	Datastore,
	Transaction,
}

impl<'a, K, V> MergeIterator<'a, K, V>
where
	K: Ord + Clone + Sync + Send + 'static,
	V: Eq + Clone + Sync + Send + 'static,
{
	pub fn new(
		mut tree_iter: SkipRange<'a, K, RangeBounds<'a, K>, K, RwLock<Versions<V>>>,
		mut self_iter: TreeRange<'a, K, Option<Arc<V>>>,
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

		MergeIterator {
			tree_iter,
			self_iter,
			tree_next,
			self_next,
			direction,
			version,
			skip_remaining: skip,
		}
	}

	/// Get next entry existence only (no key or value cloning) - optimized for counting
	pub fn next_count(&mut self) -> Option<bool> {
		loop {
			// Find the next key to process (smallest for Forward, largest for Reverse)
			let mut next_key: Option<&K> = None;
			let mut next_source = KeySource::None;

			// Check self iterator (highest priority)
			if let Some((sk, _)) = self.self_next {
				next_key = Some(sk);
				next_source = KeySource::Transaction;
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

					// Skip same key in tree iterator
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
				KeySource::Datastore => {
					let t_entry = self.tree_next.as_ref().unwrap();
					let tv = t_entry.value().read();
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
	pub fn next_key(&mut self) -> Option<(K, bool)> {
		loop {
			// Find the next key to process (smallest for Forward, largest for Reverse)
			let mut next_key: Option<&K> = None;
			let mut next_source = KeySource::None;

			// Check self iterator (highest priority)
			if let Some((sk, _)) = self.self_next {
				next_key = Some(sk);
				next_source = KeySource::Transaction;
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

					// Skip same key in tree iterator
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
				KeySource::Datastore => {
					let t_entry = self.tree_next.as_ref().unwrap();
					let tv = t_entry.value().read();
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

impl<'a, K, V> Iterator for MergeIterator<'a, K, V>
where
	K: Ord + Clone + Sync + Send + 'static,
	V: Eq + Clone + Sync + Send + 'static,
{
	type Item = (K, Option<Arc<V>>);

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			// Find the next key to process (smallest for Forward, largest for Reverse)
			let mut next_key: Option<&K> = None;
			let mut next_source = KeySource::None;

			// Check self iterator (highest priority)
			if let Some((sk, _)) = self.self_next {
				next_key = Some(sk);
				next_source = KeySource::Transaction;
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

					// Check if we should skip (only skip existing entries)
					if sv.is_some() && self.skip_remaining > 0 {
						// Advance self iterator
						self.self_next = match self.direction {
							Direction::Forward => self.self_iter.next(),
							Direction::Reverse => self.self_iter.next_back(),
						};

						// Skip same key in tree iterator
						if let Some(t_entry) = &self.tree_next {
							if t_entry.key() == sk {
								self.tree_next = match self.direction {
									Direction::Forward => self.tree_iter.next(),
									Direction::Reverse => self.tree_iter.next_back(),
								};
							}
						}

						self.skip_remaining -= 1;
						continue;
					}

					// Only clone key and value when returning
					let key = sk.clone();
					let value_opt = sv.clone();

					// Advance self iterator
					self.self_next = match self.direction {
						Direction::Forward => self.self_iter.next(),
						Direction::Reverse => self.self_iter.next_back(),
					};

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
					let tv = t_entry.value().read();
					let value_opt = tv.fetch_version(self.version);
					drop(tv);

					// Check if we should skip (only skip existing entries)
					if value_opt.is_some() && self.skip_remaining > 0 {
						// Advance tree iterator
						self.tree_next = match self.direction {
							Direction::Forward => self.tree_iter.next(),
							Direction::Reverse => self.tree_iter.next_back(),
						};

						self.skip_remaining -= 1;
						continue;
					}

					// Only clone key and entry when returning
					let tk = t_entry.key().clone();

					// Advance tree iterator
					self.tree_next = match self.direction {
						Direction::Forward => self.tree_iter.next(),
						Direction::Reverse => self.tree_iter.next_back(),
					};

					return Some((tk, value_opt));
				}
				KeySource::None => return None,
			}
		}
	}
}
