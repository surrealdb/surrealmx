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

//! This module stores the monotonic logical clock for merge versions.

use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// The number of recent merge versions tracked in [`Oracle::inserted`].
///
/// A committer publishes its claimed version before it can claim another,
/// so at most one claimed but unpublished version exists per committing
/// thread, and only those need a slot.
const INSERTED_CAPACITY: u64 = 1 << 12;

/// A monotonic logical clock minting merge versions.
///
/// Merge versions are claimed from `alloc`, a dense allocation counter,
/// and published to `timestamp` opportunistically once the claimed
/// version's merge-queue entry has been inserted. The two counters must
/// be separate: claiming by merge-queue slot insertion alone is unsound,
/// because a committer removes its merge entry once applied, and a slow
/// concurrent committer that loaded the clock before the publish could
/// then re-claim the vacated slot — minting the same version twice and
/// silently overwriting a committed value in the version chain.
///
/// `timestamp` holds the latest *published* merge version (`0` when no
/// merge has happened yet). Readers snapshot it directly. A committer
/// publishes by waiting for `timestamp` to reach its own claimed
/// version's predecessor, then advancing it to its own version in one
/// step (`Inner::atomic_merge`): every publish is in strict claim order,
/// so a thread always sees its own immediately-prior commit reflected in
/// `timestamp` before it can start another. `Inner::try_advance_merge_clock`
/// additionally walks `timestamp` forward opportunistically through
/// consecutive claimed-and-inserted versions on the cleanup/GC paths,
/// where no thread is waiting on the result. Either way, a snapshot at
/// `v` sees every merge `<= v` (in the merge queue or already applied):
/// `timestamp` only ever crosses a version once `inserted` records that
/// its entry was inserted, and merge entries are never physically
/// removed before the clock has already passed them (see
/// `Inner::merge_retire_id` and the persistence-failure path in
/// `TransactionInner::commit`). On persistent databases both counters are
/// seeded at load time with the maximum version found across the snapshot file
/// and the append-only log, so newly minted versions always continue strictly
/// above every persisted version.
pub(crate) struct Oracle {
	/// The merge version allocation counter
	pub(crate) alloc: CachePadded<AtomicU64>,
	/// The latest published merge version
	pub(crate) timestamp: CachePadded<AtomicU64>,
	/// For each slot `version % INSERTED_CAPACITY`, the latest version
	/// whose merge-queue entry has been inserted. Advancing the clock
	/// checks these rather than looking each version up in the queue.
	inserted: Box<[AtomicU64]>,
}

impl Oracle {
	/// Creates a new logical clock starting at version zero
	pub fn new() -> Arc<Self> {
		Arc::new(Self {
			alloc: CachePadded::new(AtomicU64::new(0)),
			timestamp: CachePadded::new(AtomicU64::new(0)),
			inserted: (0..INSERTED_CAPACITY).map(|_| AtomicU64::new(0)).collect(),
		})
	}

	/// Records that the merge-queue entry for `version` has been inserted.
	///
	/// The slot is shared with the version one lap earlier, so this waits
	/// until the clock has published that version.
	pub(crate) fn mark_inserted(&self, version: u64) {
		let mut spins = 0;
		while self.timestamp.load(Ordering::SeqCst) + INSERTED_CAPACITY < version {
			crate::sync::backoff(spins);
			spins += 1;
		}
		self.inserted[Self::slot(version)].store(version, Ordering::Release);
	}

	/// Whether the merge-queue entry for `version` has been inserted.
	#[inline]
	pub(crate) fn is_inserted(&self, version: u64) -> bool {
		self.inserted[Self::slot(version)].load(Ordering::Acquire) == version
	}

	/// The `inserted` slot for `version`.
	#[inline]
	const fn slot(version: u64) -> usize {
		(version % INSERTED_CAPACITY) as usize
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::mpsc;
	use std::time::Duration;

	#[test]
	fn inserted_tracks_each_marked_version() {
		let oracle = Oracle::new();
		assert!(!oracle.is_inserted(1));
		oracle.mark_inserted(1);
		assert!(oracle.is_inserted(1));
		assert!(!oracle.is_inserted(2));
	}

	#[test]
	fn inserted_slot_is_reused_one_lap_later() {
		let oracle = Oracle::new();
		oracle.mark_inserted(1);
		oracle.timestamp.store(1, Ordering::SeqCst);
		oracle.mark_inserted(1 + INSERTED_CAPACITY);
		assert!(oracle.is_inserted(1 + INSERTED_CAPACITY));
		assert!(!oracle.is_inserted(1));
	}

	#[test]
	fn mark_inserted_waits_for_the_previous_lap_to_publish() {
		let oracle = Oracle::new();
		oracle.mark_inserted(1);
		let (tx, rx) = mpsc::channel();
		let marker = {
			let oracle = Arc::clone(&oracle);
			std::thread::spawn(move || {
				oracle.mark_inserted(1 + INSERTED_CAPACITY);
				tx.send(()).unwrap();
			})
		};
		// Version 1 still owns the slot until the clock publishes it
		assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
		assert!(oracle.is_inserted(1));
		oracle.timestamp.store(1, Ordering::SeqCst);
		rx.recv_timeout(Duration::from_secs(10)).unwrap();
		marker.join().unwrap();
		assert!(oracle.is_inserted(1 + INSERTED_CAPACITY));
	}
}
