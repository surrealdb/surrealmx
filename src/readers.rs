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

//! Sharded, cache-padded active reader map.
//!
//! Under high concurrency (e.g. 64–128 threads on modern multi-core CPUs), a
//! single global skiplist incurs severe cache-line bouncing and atomic CAS
//! contention on every transaction checkout (`pin_slot`) and completion.
//!
//! This module partitions reader slots across [`NUM_SHARDS`] cache-padded
//! shards. Each thread stripes to a shard via thread-local identification,
//! incrementing a shard-local monotonic counter and inserting into a
//! shard-local [`SkipMap`].
//!
//! The allocated `slot_id` embeds the shard index in its lowest [`SHARD_BITS`],
//! allowing $O(1)$ routing on `remove(&slot_id)` even if a transaction is
//! unpinned from a different thread (e.g. in multi-threaded async executors).

use crate::inner::{Slot, SLOT_PINNING};
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use crossbeam_utils::CachePadded;
use std::sync::atomic::{fence, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Number of reader shards. Must be a power of two.
pub(crate) const NUM_SHARDS: usize = 32;

/// Number of low bits used to encode the shard index in `slot_id`.
pub(crate) const SHARD_BITS: u32 = 5;

/// Bitmask to extract the shard index from a `slot_id`.
pub(crate) const SHARD_MASK: u64 = (1 << SHARD_BITS) - 1;

/// A single cache-isolated shard holding active reader slots.
pub(crate) struct ReaderShard {
	/// Shard-local monotonic sequence counter.
	pub(crate) counter: AtomicU64,
	/// Shard-local skiplist storing active slots.
	pub(crate) map: SkipMap<u64, Arc<Slot>>,
}

impl ReaderShard {
	fn new() -> Self {
		Self {
			counter: AtomicU64::new(0),
			map: SkipMap::new(),
		}
	}
}

/// Sharded active reader registry.
pub(crate) struct Readers {
	shards: Box<[CachePadded<ReaderShard>; NUM_SHARDS]>,
}

impl Default for Readers {
	fn default() -> Self {
		Self::new()
	}
}

impl Readers {
	/// Create a new sharded reader registry.
	pub(crate) fn new() -> Self {
		// Initialize each cache-padded shard
		let shard_vec: Vec<CachePadded<ReaderShard>> =
			(0..NUM_SHARDS).map(|_| CachePadded::new(ReaderShard::new())).collect();
		let boxed_slice = shard_vec.into_boxed_slice();
		let Ok(shards) = boxed_slice.try_into() else {
			unreachable!("length verified to be NUM_SHARDS");
		};
		Self {
			shards,
		}
	}

	/// Pin a transaction slot into a thread-preferred shard.
	///
	/// Generates a globally unique `slot_id` encoding the shard index in the
	/// low bits and the shard-local counter in the upper bits.
	#[inline]
	pub(crate) fn pin(&self, slot: &Arc<Slot>) -> u64 {
		let shard_idx = current_thread_shard();
		let shard = &self.shards[shard_idx];
		let seq = shard.counter.fetch_add(1, Ordering::Relaxed) + 1;
		let slot_id = (seq << SHARD_BITS) | (shard_idx as u64);
		shard.map.insert(slot_id, Arc::clone(slot));
		slot_id
	}

	/// Insert a slot with an explicit `slot_id` into its target shard.
	#[inline]
	pub(crate) fn insert(&self, slot_id: u64, slot: Arc<Slot>) -> Entry<'_, u64, Arc<Slot>> {
		let shard_idx = (slot_id & SHARD_MASK) as usize % NUM_SHARDS;
		self.shards[shard_idx].map.insert(slot_id, slot)
	}

	/// Remove a reader slot by `slot_id`.
	///
	/// Computes the owning shard directly from `slot_id & SHARD_MASK` in
	/// $O(1)$.
	#[inline]
	pub(crate) fn remove(&self, slot_id: u64) -> Option<Entry<'_, u64, Arc<Slot>>> {
		let shard_idx = (slot_id & SHARD_MASK) as usize % NUM_SHARDS;
		self.shards[shard_idx].map.remove(&slot_id)
	}

	/// Returns true if all shards are empty.
	#[inline]
	pub(crate) fn is_empty(&self) -> bool {
		self.shards.iter().all(|s| s.map.is_empty())
	}

	/// Returns total number of active reader slots across all shards.
	#[inline]
	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		self.shards.iter().map(|s| s.map.len()).sum()
	}

	/// Returns the entry with the smallest `slot_id` across all shards, if any.
	#[cfg(test)]
	pub(crate) fn front(&self) -> Option<Entry<'_, u64, Arc<Slot>>> {
		let mut min_entry: Option<Entry<'_, u64, Arc<Slot>>> = None;
		for shard in self.shards.iter() {
			if let Some(entry) = shard.map.front() {
				match min_entry {
					None => min_entry = Some(entry),
					Some(ref current) if entry.key() < current.key() => {
						min_entry = Some(entry);
					}
					_ => {}
				}
			}
		}
		min_entry
	}

	/// Compute the earliest pinned watermark value across all shards.
	///
	/// Returns `None` if any slot is in [`SLOT_PINNING`] state.
	#[inline]
	pub(crate) fn earliest_pinned(
		&self,
		dim: impl Fn(&Slot) -> &AtomicU64,
		fallback: u64,
		exclude: Option<u64>,
	) -> Option<u64> {
		fence(Ordering::SeqCst);
		if self.is_empty() {
			return Some(fallback);
		}
		let mut min = fallback;
		for shard in self.shards.iter() {
			if shard.map.is_empty() {
				continue;
			}
			for entry in &shard.map {
				if Some(*entry.key()) == exclude {
					continue;
				}
				match dim(entry.value()).load(Ordering::SeqCst) {
					SLOT_PINNING => return None,
					v => min = min.min(v),
				}
			}
		}
		Some(min)
	}
}

/// Retrieve the calling thread's preferred shard index in `0..NUM_SHARDS`.
#[inline]
fn current_thread_shard() -> usize {
	thread_local! {
		static SHARD_ID: usize = {
			static NEXT: AtomicUsize = AtomicUsize::new(0);
			NEXT.fetch_add(1, Ordering::Relaxed) & (NUM_SHARDS - 1)
		};
	}
	SHARD_ID.with(|&id| id)
}
