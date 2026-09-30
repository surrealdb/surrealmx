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

//! The registry of transaction slots.
//!
//! Every transaction owns one [`Slot`] in the registry for as long as it
//! exists, including while it sits in the transaction pool, so pinning and
//! unpinning a snapshot are plain stores to the transaction's own slot. A
//! watermark scan loads every registered slot and skips the unpinned ones.
//!
//! Slots live in fixed chunks that are allocated on first use and never
//! move, so a slot is reached by its index without locking, and a released
//! index is reused by the next registration. Each slot is cache padded, so
//! transactions pinning neighbouring slots do not share a cache line.

use crate::inner::{Slot, SLOT_PINNING, SLOT_UNPINNED};
use crossbeam_utils::CachePadded;
use std::sync::atomic::{fence, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};

/// Number of low bits of a slot index that select the slot in its chunk.
const CHUNK_BITS: u32 = 10;

/// Number of slots in each chunk.
const CHUNK_SIZE: usize = 1 << CHUNK_BITS;

/// Maximum number of chunks, bounding the number of transactions that can
/// exist at once.
const MAX_CHUNKS: usize = 1 << 10;

/// A fixed chunk of cache-padded slots.
type Chunk = Box<[CachePadded<Slot>]>;

/// The registry of transaction slots.
pub(crate) struct Readers {
	/// Lazily allocated chunks of slots
	chunks: Box<[OnceLock<Chunk>]>,
	/// One past the highest slot index handed out
	len: CachePadded<AtomicUsize>,
	/// Released slot indices, reused before growing
	free: Mutex<Vec<usize>>,
}

impl Default for Readers {
	fn default() -> Self {
		Self::new()
	}
}

impl Readers {
	/// Create an empty registry.
	pub(crate) fn new() -> Self {
		Self {
			chunks: (0..MAX_CHUNKS).map(|_| OnceLock::new()).collect(),
			len: CachePadded::new(AtomicUsize::new(0)),
			free: Mutex::new(Vec::new()),
		}
	}

	/// Register a new, unpinned slot and return its index.
	///
	/// # Panics
	///
	/// Panics if more transactions exist at once than the registry holds.
	pub(crate) fn register(&self) -> usize {
		let reused = self.free.lock().unwrap_or_else(PoisonError::into_inner).pop();
		if let Some(index) = reused {
			return index;
		}
		let index = self.len.fetch_add(1, Ordering::SeqCst);
		let chunk = index >> CHUNK_BITS;
		assert!(chunk < MAX_CHUNKS, "too many concurrent transactions");
		self.chunks[chunk]
			.get_or_init(|| (0..CHUNK_SIZE).map(|_| CachePadded::new(Slot::unpinned())).collect());
		index
	}

	/// Release a slot for reuse by a later registration.
	pub(crate) fn release(&self, index: usize) {
		self.unpin(index);
		self.free.lock().unwrap_or_else(PoisonError::into_inner).push(index);
	}

	/// The slot at `index`, which must have been registered.
	#[inline]
	pub(crate) fn slot(&self, index: usize) -> &Slot {
		let chunk = self.chunks[index >> CHUNK_BITS].get().expect("registered slot index");
		&chunk[index & (CHUNK_SIZE - 1)]
	}

	/// Mark the slot at `index` as holding no snapshot.
	#[inline]
	pub(crate) fn unpin(&self, index: usize) {
		let slot = self.slot(index);
		slot.version.store(SLOT_UNPINNED, Ordering::SeqCst);
		slot.commit.store(SLOT_UNPINNED, Ordering::SeqCst);
	}

	/// Returns the number of registered slots that have not been released.
	#[cfg(test)]
	pub(crate) fn registered(&self) -> usize {
		let free = self.free.lock().unwrap_or_else(PoisonError::into_inner).len();
		self.len.load(Ordering::SeqCst) - free
	}

	/// Returns the number of pinned slots.
	#[cfg(test)]
	pub(crate) fn pinned(&self) -> usize {
		let len = self.len.load(Ordering::SeqCst);
		(0..len)
			.filter(|&i| self.chunks[i >> CHUNK_BITS].get().is_some())
			.filter(|&i| self.slot(i).version.load(Ordering::SeqCst) != SLOT_UNPINNED)
			.count()
	}

	/// Compute the earliest pinned watermark value across all slots.
	///
	/// Returns `None` if any slot is in [`SLOT_PINNING`] state.
	#[inline]
	pub(crate) fn earliest_pinned(
		&self,
		dim: impl Fn(&Slot) -> &AtomicU64,
		fallback: u64,
		exclude: Option<usize>,
	) -> Option<u64> {
		fence(Ordering::SeqCst);
		let len = self.len.load(Ordering::SeqCst);
		let mut min = fallback;
		for (c, chunk) in self.chunks.iter().enumerate().take(len.div_ceil(CHUNK_SIZE)) {
			// A chunk is allocated before any of its slots is pinned
			let Some(chunk) = chunk.get() else {
				continue;
			};
			let base = c << CHUNK_BITS;
			for (i, slot) in chunk.iter().enumerate().take(len - base) {
				if Some(base + i) == exclude {
					continue;
				}
				match dim(slot).load(Ordering::SeqCst) {
					SLOT_PINNING => return None,
					SLOT_UNPINNED => {}
					v => min = min.min(v),
				}
			}
		}
		Some(min)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn released_indices_are_reused() {
		let readers = Readers::new();
		let a = readers.register();
		let b = readers.register();
		assert_ne!(a, b);
		readers.release(a);
		assert_eq!(readers.register(), a);
	}

	#[test]
	fn registration_spans_chunks() {
		let readers = Readers::new();
		let indices: Vec<_> = (0..CHUNK_SIZE + 3).map(|_| readers.register()).collect();
		let last = *indices.last().unwrap();
		assert_eq!(last, CHUNK_SIZE + 2);
		readers.slot(last).version.store(5, Ordering::SeqCst);
		readers.slot(last).commit.store(5, Ordering::SeqCst);
		assert_eq!(readers.earliest_pinned(|s| &s.version, 9, None), Some(5));
	}

	#[test]
	fn watermark_skips_unpinned_and_excluded_slots() {
		let readers = Readers::new();
		let pinned = readers.register();
		let excluded = readers.register();
		let _unpinned = readers.register();
		readers.slot(pinned).version.store(7, Ordering::SeqCst);
		readers.slot(excluded).version.store(3, Ordering::SeqCst);
		assert_eq!(readers.earliest_pinned(|s| &s.version, 10, Some(excluded)), Some(7));
		assert_eq!(readers.earliest_pinned(|s| &s.version, 10, None), Some(3));
		readers.unpin(pinned);
		assert_eq!(readers.earliest_pinned(|s| &s.version, 10, Some(excluded)), Some(10));
	}

	#[test]
	fn a_pinning_slot_makes_the_watermark_unknown() {
		let readers = Readers::new();
		let index = readers.register();
		readers.slot(index).version.store(SLOT_PINNING, Ordering::SeqCst);
		assert_eq!(readers.earliest_pinned(|s| &s.version, 10, None), None);
		readers.release(index);
		assert_eq!(readers.earliest_pinned(|s| &s.version, 10, None), Some(10));
	}
}
