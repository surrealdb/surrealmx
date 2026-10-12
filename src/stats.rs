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

//! Runtime statistics and memory accounting for the database.

use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicI64, Ordering};

/// Runtime statistics for a database instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseStats {
	/// Number of live keys currently in the datastore.
	pub live_keys: usize,
	/// Estimated count of live committed versions across all keys.
	pub live_versions: usize,
	/// Estimated size in bytes of live keys and values held in memory.
	pub approx_data_bytes: usize,
	/// Number of keys currently tracked as candidates for garbage collection.
	pub gc_candidate_keys: usize,
	/// Snapshot version of the oldest active reader transaction, if any.
	pub oldest_pinned_version: Option<u64>,
	/// Number of active reader transactions holding a snapshot pin.
	pub active_readers: usize,
	/// Number of unretired commits in the commit ring buffer.
	pub commit_ring_occupancy: usize,
	/// Number of unretired merges in the merge ring buffer.
	pub merge_ring_occupancy: usize,
	/// Number of entries currently queued in the asynchronous persistence
	/// buffer.
	pub async_queue_depth: usize,
	/// Estimated size in bytes of entries queued in the asynchronous
	/// persistence buffer.
	pub async_queue_bytes: usize,
	/// Total number of dropped / failed asynchronous persistence batches.
	pub async_dropped_batches: u64,
	/// Whether the database has been stopped due to an unrecoverable or
	/// persistence failure.
	pub is_poisoned: bool,
	/// The reason the database was stopped, if poisoned.
	pub poison_reason: Option<String>,
}

/// Sharded, cache-line padded atomic counters for low-contention memory
/// accounting.
pub(crate) struct MemoryTracker {
	shards_bytes: [CachePadded<AtomicI64>; 32],
	shards_versions: [CachePadded<AtomicI64>; 32],
}

impl MemoryTracker {
	pub(crate) fn new() -> Self {
		Self {
			shards_bytes: std::array::from_fn(|_| CachePadded::new(AtomicI64::new(0))),
			shards_versions: std::array::from_fn(|_| CachePadded::new(AtomicI64::new(0))),
		}
	}

	#[inline]
	fn shard() -> usize {
		thread_local! {
			static SHARD_IDX: usize = {
				use std::hash::{Hash, Hasher};
				let mut hasher = std::collections::hash_map::DefaultHasher::new();
				std::thread::current().id().hash(&mut hasher);
				usize::try_from(hasher.finish() % 32).unwrap_or(0)
			};
		}
		SHARD_IDX.with(|idx| *idx)
	}

	#[inline]
	pub(crate) fn add(&self, versions: i64, bytes: i64) {
		let s = Self::shard();
		self.shards_versions[s].fetch_add(versions, Ordering::Relaxed);
		self.shards_bytes[s].fetch_add(bytes, Ordering::Relaxed);
	}

	/// Account for the versions removed between two footprints of one chain.
	#[inline]
	pub(crate) fn release(&self, before: (usize, usize), after: (usize, usize)) {
		let versions = before.0.saturating_sub(after.0);
		let bytes = before.1.saturating_sub(after.1);
		if versions > 0 || bytes > 0 {
			self.sub(
				i64::try_from(versions).unwrap_or(i64::MAX),
				i64::try_from(bytes).unwrap_or(i64::MAX),
			);
		}
	}

	#[inline]
	pub(crate) fn sub(&self, versions: i64, bytes: i64) {
		let s = Self::shard();
		self.shards_versions[s].fetch_sub(versions, Ordering::Relaxed);
		self.shards_bytes[s].fetch_sub(bytes, Ordering::Relaxed);
	}

	#[inline]
	pub(crate) fn total_bytes(&self) -> usize {
		let mut sum: i64 = 0;
		for s in &self.shards_bytes {
			sum += s.load(Ordering::Relaxed);
		}
		usize::try_from(sum).unwrap_or(0)
	}

	#[inline]
	pub(crate) fn total_versions(&self) -> usize {
		let mut sum: i64 = 0;
		for s in &self.shards_versions {
			sum += s.load(Ordering::Relaxed);
		}
		usize::try_from(sum).unwrap_or(0)
	}
}
