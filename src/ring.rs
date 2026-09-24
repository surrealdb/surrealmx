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

//! A lock-free, fixed-size, power-of-two circular ring buffer for the commit
//! pipeline.
//!
//! Replaces dynamic skiplist commit queue allocations and lockstep predecessor
//! loops with contiguous memory access and atomic slot claims.

use crate::queue::Commit;
use crossbeam_utils::CachePadded;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Sentinel indicating that a ring slot is free and unallocated.
pub(crate) const SLOT_EMPTY: u64 = 0;

/// Default capacity of the commit ring buffer (must be a power of two).
pub(crate) const DEFAULT_COMMIT_RING_CAPACITY: usize = 65536;

/// A single pre-allocated slot in the OCC Commit Ring.
pub(crate) struct CommitSlot {
	/// The sequence number published for this slot.
	/// Published with Release ordering once the commit Arc is stored.
	pub(crate) seq: AtomicU64,
	/// The committed transaction writeset and bloom filter.
	pub(crate) commit: RwLock<Option<Arc<Commit>>>,
}

impl CommitSlot {
	pub(crate) const fn new() -> Self {
		Self {
			seq: AtomicU64::new(SLOT_EMPTY),
			commit: RwLock::new(None),
		}
	}

	/// Checks if the slot has been published with the given sequence number.
	#[inline(always)]
	pub(crate) fn is_published(&self, expected_seq: u64) -> bool {
		self.seq.load(Ordering::Acquire) == expected_seq
	}

	/// Stores the commit data and publishes the slot with Release ordering.
	#[inline]
	pub(crate) fn publish(&self, seq: u64, commit: Arc<Commit>) {
		*self.commit.write() = Some(commit);
		self.seq.store(seq, Ordering::Release);
	}
}

/// A fixed-size power-of-two lock-free ring buffer for OCC commits.
pub(crate) struct CommitRing {
	/// The pre-allocated circular array of slots.
	slots: Box<[CommitSlot]>,
	/// Bitmask for fast circular indexing: `seq & mask`.
	mask: usize,
	/// The next sequence number to hand out to a committing transaction.
	pub(crate) next: CachePadded<AtomicU64>,
	/// The highest sequence number that has been retired.
	/// Slots at or below this watermark are safe to be reclaimed for the next
	/// lap.
	pub(crate) taken: CachePadded<AtomicU64>,
	/// The contiguous published prefix bound: every sequence at or below
	/// this bound has been written into its slot.
	pub(crate) published_prefix: CachePadded<AtomicU64>,
}

impl CommitRing {
	/// Creates a new commit ring buffer with a capacity rounded up to the next
	/// power of two.
	pub(crate) fn new(capacity: usize, start_seq: u64) -> Self {
		let cap = capacity.max(64).next_power_of_two();
		let mask = cap - 1;
		let mut slots = Vec::with_capacity(cap);
		for _ in 0..cap {
			slots.push(CommitSlot::new());
		}

		Self {
			slots: slots.into_boxed_slice(),
			mask,
			next: CachePadded::new(AtomicU64::new(start_seq)),
			taken: CachePadded::new(AtomicU64::new(start_seq.saturating_sub(1))),
			published_prefix: CachePadded::new(AtomicU64::new(start_seq.saturating_sub(1))),
		}
	}

	/// Atomically claims the next sequential slot in O(1).
	#[inline(always)]
	pub(crate) fn claim(&self) -> u64 {
		self.next.fetch_add(1, Ordering::Relaxed)
	}

	/// Accesses the slot for the given sequence number in O(1).
	#[inline(always)]
	pub(crate) fn slot(&self, seq: u64) -> &CommitSlot {
		let idx = usize::try_from(seq & (self.mask as u64)).unwrap_or(0);
		&self.slots[idx]
	}

	/// Stores commit data and publishes the slot.
	#[inline(always)]
	pub(crate) fn publish(&self, seq: u64, commit: Arc<Commit>) {
		self.slot(seq).publish(seq, commit);
	}

	/// Advances the retired watermark `taken` in O(1).
	pub(crate) fn advance_taken(&self, new_taken: u64) {
		let mut cur = self.taken.load(Ordering::Acquire);
		while new_taken > cur {
			if self
				.taken
				.compare_exchange_weak(cur, new_taken, Ordering::SeqCst, Ordering::Acquire)
				.is_ok()
			{
				break;
			}
			cur = self.taken.load(Ordering::Acquire);
		}
	}

	/// Opportunistically advances the contiguous published prefix.
	pub(crate) fn advance_published_prefix(&self) {
		let max_claimed = self.next.load(Ordering::Relaxed);
		let cur = self.published_prefix.load(Ordering::Acquire);
		let mut target = cur;
		while target < max_claimed {
			let next = target + 1;
			if !self.slot(next).is_published(next) {
				break;
			}
			target = next;
		}
		if target > cur {
			let _ = self.published_prefix.compare_exchange(
				cur,
				target,
				Ordering::Release,
				Ordering::Relaxed,
			);
		}
	}
}
