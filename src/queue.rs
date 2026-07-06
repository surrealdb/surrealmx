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

//! This module stores the transaction commit and merge queues.

use crate::bloom::BloomFilter;
use crate::LOG_TARGET_CONFLICTS;
use bytes::Bytes;
use papaya::HashSet;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::debug;

/// A transaction entry in the transaction commit queue
pub struct Commit {
	/// The unique id of this commit attempt
	pub(crate) id: u64,
	/// The sorted keys of the local updates and deletes. Conflict
	/// detection only ever inspects keys, so the values are
	/// intentionally not stored here: a commit queue entry outlives
	/// the corresponding merge queue entry (it is only trimmed later
	/// by the cleanup worker), and holding the values would pin them
	/// in memory for that entire window.
	pub(crate) keys: Vec<Bytes>,
	/// Bloom filter over writeset keys for fast conflict pre-checks
	pub(crate) writeset_bloom: BloomFilter,
}

/// A transaction entry in the transaction merge queue
pub struct Merge {
	/// The unique id of this commit attempt
	pub(crate) id: u64,
	/// The local set of updates and deletes
	pub(crate) writeset: Arc<BTreeMap<Bytes, Option<Bytes>>>,
}

impl Commit {
	/// The smallest key in the writeset (for range overlap checks)
	#[inline]
	fn min_key(&self) -> Option<&Bytes> {
		self.keys.first()
	}

	/// The largest key in the writeset (for range overlap checks)
	#[inline]
	fn max_key(&self) -> Option<&Bytes> {
		self.keys.last()
	}

	/// Returns true if the writeset contains the specified key
	#[inline]
	fn contains_key(&self, key: &Bytes) -> bool {
		self.keys.binary_search(key).is_ok()
	}

	/// Returns true if self has no elements in common with other.
	/// Uses a bloom filter for a fast pre-check before the exact intersection.
	pub fn is_disjoint_readset_bloom(&self, other: &HashSet<Bytes>, bloom: &BloomFilter) -> bool {
		// Fast path: if the bloom filter is empty, there are no reads to conflict with
		if bloom.is_empty() {
			return true;
		}
		// Check writeset keys against the bloom filter first
		let mut any_possible = false;
		for key in self.keys.iter() {
			if bloom.may_contain(key) {
				any_possible = true;
				break;
			}
		}
		// If no writeset key passes the bloom filter, there is definitely no overlap
		if !any_possible {
			return true;
		}
		// Fall through to exact check
		self.is_disjoint_readset(other)
	}

	/// Returns true if self has no elements in common with other
	pub fn is_disjoint_readset(&self, other: &HashSet<Bytes>) -> bool {
		// Pin the readset for access
		let other = other.pin();
		// Check if the readset is not empty
		if !other.is_empty() {
			// Choose iteration direction based on size to minimize iterations
			if other.len() < self.keys.len() {
				// Check if any key in readset exists in the writeset
				for key in other.iter() {
					if self.contains_key(key) {
						// Log the error for debug purposes
						#[cfg(debug_assertions)]
						debug!(target: LOG_TARGET_CONFLICTS, "KeyReadConflict involving {:?}", key);
						return false;
					}
				}
			} else {
				// Check if any key in writeset exists in the readset
				for key in self.keys.iter() {
					if other.contains(key) {
						// Log the error for debug purposes
						#[cfg(debug_assertions)]
						debug!(target: LOG_TARGET_CONFLICTS, "KeyReadConflict involving {:?}", key);
						return false;
					}
				}
			}
		}
		// No overlap was found
		true
	}

	/// Returns true if self has no elements in common with other.
	/// Uses bloom filters and key bounds for fast pre-checks before the
	/// exact sorted merge.
	pub fn is_disjoint_writeset_bloom(&self, other: &Arc<Commit>) -> bool {
		// Fast path: check if the key ranges overlap at all
		match (self.min_key(), self.max_key(), other.min_key(), other.max_key()) {
			(Some(self_min), Some(self_max), Some(other_min), Some(other_max)) => {
				if self_max < other_min || other_max < self_min {
					return true;
				}
			}
			// An empty writeset cannot conflict
			_ => return true,
		}
		// Check our writeset keys against the other's bloom filter
		let mut any_possible = false;
		for key in self.keys.iter() {
			if other.writeset_bloom.may_contain(key) {
				any_possible = true;
				break;
			}
		}
		// If no key passes the bloom filter, there is definitely no overlap
		if !any_possible {
			return true;
		}
		// Fall through to exact check
		self.is_disjoint_writeset(other)
	}

	/// Returns true if this commit's writeset may contain keys within the
	/// given range. Uses the min/max key bounds for a fast range overlap
	/// check before iterating writeset keys for scan conflict detection.
	pub fn may_overlap_range(&self, range_start: &Bytes, range_end: &Bytes) -> bool {
		// Check if the writeset key range overlaps the scan range
		match (self.min_key(), self.max_key()) {
			(Some(min_key), Some(max_key)) => min_key < range_end && max_key >= range_start,
			// An empty writeset cannot overlap any range
			_ => false,
		}
	}

	/// Returns true if self has no elements in common with other
	pub fn is_disjoint_writeset(&self, other: &Arc<Commit>) -> bool {
		// Create a key iterator for each writeset
		let mut a = self.keys.iter();
		let mut b = other.keys.iter();
		// Move to the next value in each iterator
		let mut next_a = a.next();
		let mut next_b = b.next();
		// Advance each iterator independently in order
		while let (Some(ka), Some(kb)) = (next_a, next_b) {
			match ka.cmp(kb) {
				std::cmp::Ordering::Less => next_a = a.next(),
				std::cmp::Ordering::Greater => next_b = b.next(),
				std::cmp::Ordering::Equal => {
					// Log the error for debug purposes
					#[cfg(debug_assertions)]
					debug!(target: LOG_TARGET_CONFLICTS, "KeyWriteConflict involving {:?}", ka);
					return false;
				}
			}
		}
		// No overlap was found
		true
	}
}
