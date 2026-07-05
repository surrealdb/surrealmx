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
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::debug;

/// A transaction entry in the transaction commit queue
pub struct Commit {
	/// The unique id of this commit attempt
	pub(crate) id: u64,
	/// The sorted writeset keys. Values are never read during conflict
	/// detection, so they are not retained here: the merge queue holds
	/// the full writeset only until the commit is applied, while this
	/// entry survives until queue cleanup without pinning value memory.
	pub(crate) keys: Arc<[Bytes]>,
	/// Bloom filter over writeset keys for fast conflict pre-checks
	pub(crate) writeset_bloom: BloomFilter,
	/// The merge version this commit published, or zero while the commit
	/// is still in flight. Set by the owning transaction immediately
	/// after its merge version becomes loadable from the clock. Consumed
	/// by the commit-watermark advance (readers must never take a
	/// conflict-window base covering a commit whose merge version they
	/// cannot yet see) and by the conflict loop, which skips commits
	/// whose merge version is visible in the checking transaction's
	/// snapshot: a visible commit happened strictly before the snapshot
	/// in version order, so it is not concurrent and first-committer-wins
	/// does not apply to it.
	pub(crate) merge_version: AtomicU64,
}

/// A transaction entry in the transaction merge queue
pub struct Merge {
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

#[cfg(test)]
mod tests {
	use super::*;

	/// Build a commit entry from an unsorted set of string keys
	fn commit(input: &[&str]) -> Arc<Commit> {
		let mut v: Vec<Bytes> = input.iter().map(|k| Bytes::from(k.to_string())).collect();
		v.sort();
		v.dedup();
		let keys: Arc<[Bytes]> = v.into();
		let mut writeset_bloom = BloomFilter::new();
		for k in keys.iter() {
			writeset_bloom.insert(k);
		}
		let min_key = keys.first().cloned().unwrap_or_default();
		let max_key = keys.last().cloned().unwrap_or_default();
		Arc::new(Commit {
			id: 1,
			keys,
			writeset_bloom,
			min_key,
			max_key,
			merge_version: AtomicU64::new(0),
		})
	}

	/// Build a readset from a set of string keys
	fn readset(input: &[&str]) -> HashSet<Bytes> {
		let set = HashSet::new();
		{
			let pin = set.pin();
			for k in input {
				pin.insert(Bytes::from(k.to_string()));
			}
		}
		set
	}

	#[test]
	fn readset_disjoint_small_readset_probes_keys() {
		// Readset smaller than the writeset: the binary-search direction
		let c = commit(&["a", "b", "c", "d", "e"]);
		assert!(c.is_disjoint_readset(&readset(&["x", "y"])));
		assert!(!c.is_disjoint_readset(&readset(&["x", "c"])));
	}

	#[test]
	fn readset_disjoint_large_readset_iterates_keys() {
		// Readset larger than the writeset: the writeset-iteration direction
		let c = commit(&["m"]);
		assert!(c.is_disjoint_readset(&readset(&["a", "b", "c", "d"])));
		assert!(!c.is_disjoint_readset(&readset(&["a", "b", "m", "d"])));
	}

	#[test]
	fn writeset_disjoint_two_pointer_merge() {
		let a = commit(&["a", "c", "e"]);
		let b = commit(&["b", "d", "f"]);
		assert!(a.is_disjoint_writeset(&b));
		assert!(b.is_disjoint_writeset(&a));
		let c = commit(&["e", "g"]);
		assert!(!a.is_disjoint_writeset(&c));
		assert!(!c.is_disjoint_writeset(&a));
	}

	#[test]
	fn writeset_bloom_pre_checks_agree_with_exact() {
		let a = commit(&["a", "c", "e"]);
		let b = commit(&["b", "d", "f"]);
		assert!(a.is_disjoint_writeset_bloom(&b));
		let c = commit(&["e", "g"]);
		assert!(!a.is_disjoint_writeset_bloom(&c));
		// Non-overlapping key ranges short-circuit on the min/max bounds
		let lo = commit(&["a", "b"]);
		let hi = commit(&["x", "y"]);
		assert!(lo.is_disjoint_writeset_bloom(&hi));
	}
}
