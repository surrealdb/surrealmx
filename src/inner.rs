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

//! This module stores the inner in-memory database type.

use crate::oracle::Oracle;
#[cfg(not(target_arch = "wasm32"))]
use crate::persistence::Persistence;
use crate::queue::{Commit, Merge};
use crate::versions::Versions;
use crate::DatabaseOptions;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use std::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::thread::JoinHandle;

/// Sentinel published in a slot field while its owning transaction is
/// choosing its snapshot. Merge versions and commit ids are logical
/// counters seeded from persisted data and guarded at load time, so they
/// can never reach this value.
pub(crate) const SLOT_PINNING: u64 = u64::MAX;

/// A pinned transaction registration.
///
/// A slot is inserted into [`Inner::readers`] with both fields holding
/// [`SLOT_PINNING`] BEFORE the owning transaction loads its snapshot
/// (pin-then-read), so every watermark scan either observes the final
/// snapshot values or the sentinel — and a sentinel forces the sweeper
/// to treat the watermark as unknown and skip reclamation for that pass.
/// Each field independently carries the sentinel: a sweeper can scan
/// between the two value stores, so neither field may be interpreted
/// before it has left the pinning state. One slot exists per live
/// transaction and is exclusively owned by it, so state transitions are
/// plain stores — no CAS protocol is required.
pub(crate) struct Slot {
	/// The owner's snapshot merge version, or SLOT_PINNING
	pub(crate) version: AtomicU64,
	/// The owner's snapshot commit id, or SLOT_PINNING
	pub(crate) commit: AtomicU64,
}

impl Slot {
	/// Create a new slot in the pinning state
	pub(crate) fn pinning() -> Self {
		Self {
			version: AtomicU64::new(SLOT_PINNING),
			commit: AtomicU64::new(SLOT_PINNING),
		}
	}
}

/// The inner structure of the transactional in-memory database
pub struct Inner {
	/// The timestamp version oracle
	pub(crate) oracle: Arc<Oracle>,
	/// The underlying lock-free skip-list datastructure
	pub(crate) datastore: SkipMap<Bytes, RwLock<Versions>>,
	/// Registered transaction snapshot slots, keyed by allocation order.
	/// Contains exactly the live transactions: slots are inserted at
	/// registration and removed on transaction drop, so watermark scans
	/// walk a map sized by concurrency, not by the transaction pool.
	pub(crate) readers: SkipMap<u64, Arc<Slot>>,
	/// Monotonic slot id allocator for the readers map
	pub(crate) reader_slot_id: AtomicU64,
	/// The contiguous completed prefix of the commit queue: every commit
	/// with an id at or below this watermark has either published its
	/// merge version or aborted. Readers take their commit snapshot from
	/// here rather than from `transaction_commit_id`, which closes a
	/// lost-update anomaly: a commit id becomes visible before its merge
	/// version is published, so a reader snapshotting the raw commit id
	/// could exclude a commit from its conflict window while also being
	/// unable to see that commit's writes. Every commit at or below the
	/// watermark published its merge version before the watermark
	/// advanced past it, so a reader's version snapshot (loaded after
	/// its commit snapshot) always covers its entire excluded prefix.
	pub(crate) commit_watermark: AtomicU64,
	/// The transaction commit queue attempt sequence number
	pub(crate) transaction_queue_id: AtomicU64,
	/// The transaction commit queue success sequence number
	pub(crate) transaction_commit_id: AtomicU64,
	/// The transaction commit queue list of modifications
	pub(crate) transaction_commit_queue: SkipMap<u64, Arc<Commit>>,
	/// Transaction updates which are committed but not yet applied
	pub(crate) transaction_merge_queue: SkipMap<u64, Arc<Merge>>,
	/// Optional persistence handler
	#[cfg(not(target_arch = "wasm32"))]
	pub(crate) persistence: RwLock<Option<Arc<Persistence>>>,
	/// Specifies whether background worker threads are enabled
	pub(crate) background_threads_enabled: AtomicBool,
	/// Stores a handle to the current transaction cleanup background thread
	#[cfg(not(target_arch = "wasm32"))]
	pub(crate) transaction_cleanup_handle: RwLock<Option<JoinHandle<()>>>,
	/// Stores a handle to the current garbage collection background thread
	#[cfg(not(target_arch = "wasm32"))]
	pub(crate) garbage_collection_handle: RwLock<Option<JoinHandle<()>>>,
	/// Threshold after which transaction state is reset
	pub(crate) reset_threshold: usize,
}

impl Inner {
	/// Create a new [`Inner`] structure with the given options.
	pub fn new(opts: &DatabaseOptions) -> Self {
		Self {
			oracle: Oracle::new(),
			datastore: SkipMap::new(),
			readers: SkipMap::new(),
			reader_slot_id: AtomicU64::new(0),
			commit_watermark: AtomicU64::new(0),
			transaction_queue_id: AtomicU64::new(0),
			transaction_commit_id: AtomicU64::new(0),
			transaction_commit_queue: SkipMap::new(),
			transaction_merge_queue: SkipMap::new(),
			#[cfg(not(target_arch = "wasm32"))]
			persistence: RwLock::new(None),
			background_threads_enabled: AtomicBool::new(true),
			#[cfg(not(target_arch = "wasm32"))]
			transaction_cleanup_handle: RwLock::new(None),
			#[cfg(not(target_arch = "wasm32"))]
			garbage_collection_handle: RwLock::new(None),
			reset_threshold: opts.reset_threshold,
		}
	}
}

impl Inner {
	/// Returns the minimum snapshot merge version across all pinned
	/// transaction slots, bounded by `fallback`, or `None` when any slot
	/// is mid-registration. See [`earliest_pinned`].
	#[inline]
	pub(crate) fn earliest_active_version(&self, fallback: u64) -> Option<u64> {
		earliest_pinned(&self.readers, |s| &s.version, fallback, None)
	}

	/// Returns the minimum snapshot commit id across all pinned
	/// transaction slots, bounded by `fallback`, or `None` when any slot
	/// is mid-registration. See [`earliest_pinned`].
	#[inline]
	pub(crate) fn earliest_active_commit(&self, fallback: u64) -> Option<u64> {
		earliest_pinned(&self.readers, |s| &s.commit, fallback, None)
	}

	/// Trim commit-queue entries which no active or future transaction can
	/// need for conflict detection.
	///
	/// The fallback bound for an idle database is the current commit id,
	/// loaded BEFORE the fence-and-scan over the slots. This ordering is
	/// load-bearing: a transaction missed by the scan pinned its slot
	/// after the scan, so its subsequent commit-snapshot load is ordered
	/// after our bound load in the SeqCst total order and (the watermark
	/// being monotonic) returns at least our bound — its conflict window
	/// `snapshot + 1 ..` sits strictly above everything we trim. A
	/// transaction seen by the scan bounds the trim directly, and a slot
	/// still pinning aborts the pass entirely. A writer mid-commit holds
	/// its own slot until drop, so its conflict-check iteration is
	/// protected identically.
	pub(crate) fn cleanup_commit_queue(&self) {
		// Load the idle-database bound before the fence-and-scan
		let fallback = self.transaction_commit_id.load(Ordering::SeqCst);
		// Bound by the earliest registered transaction, if any
		if let Some(oldest) = self.earliest_active_commit(fallback) {
			// Remove all entries below the bound
			self.transaction_commit_queue.range(..oldest).for_each(|e| {
				e.remove();
			});
		}
	}

	/// Compute the watermark for commit-time inline garbage collection,
	/// or `None` when a registration is in flight — in which case the
	/// committer simply skips inline reclamation for this commit and the
	/// next commit to each key (or the background full scan) catches up.
	///
	/// The committer's own slot is excluded: `commit` takes the
	/// transaction by mutable reference and marks it done, so no further
	/// reads can occur at its snapshot. Excluding ANY other slot is
	/// forbidden — in particular a concurrent committer's slot (pinned at
	/// its start version, strictly below its merge version) is what
	/// prevents a delete-collapse from unlinking a chain that a slower
	/// committer is still about to push an earlier version into, which
	/// would otherwise resurrect deleted data through the
	/// `get_or_insert_with` re-seed path.
	pub(crate) fn inline_gc_watermark(&self, own_slot: u64) -> Option<u64> {
		// Load the clock bound before the fence-and-scan
		let now = self.oracle.timestamp.load(Ordering::SeqCst);
		// Bound by every other registered transaction
		earliest_pinned(&self.readers, |s| &s.version, now, Some(own_slot))
	}

	/// Compute the next `cleanup_ts` below which no live or future
	/// transaction can observe a version, or `None` when registrations
	/// are in flight and the watermark cannot be established.
	///
	/// The proposed value is bounded by the published logical clock,
	/// loaded BEFORE the fence-and-scan over the slots: a transaction
	/// missed by the scan pinned after the scan, so its snapshot load
	/// returns at least the clock value we load here, and version
	/// reclamation always retains the entry visible at the watermark.
	/// A bounded number of retries absorbs the nanosecond-scale window
	/// in which a registering transaction is still pinning.
	pub(crate) fn compute_cleanup_ts(&self) -> Option<u64> {
		// Retry a bounded number of times while registrations are pinning
		for _ in 0..3 {
			// Load the clock bound before the fence-and-scan
			let now = self.oracle.timestamp.load(Ordering::SeqCst);
			// Bound by the earliest registered transaction, if any
			if let Some(earliest) = self.earliest_active_version(now) {
				return Some(earliest.min(now));
			}
			// A registration is mid-pin; give it a beat and retry
			std::hint::spin_loop();
		}
		// Registrations kept arriving; skip this reclamation pass
		None
	}

	/// Advance the contiguous completed prefix of the commit queue.
	///
	/// Called by every committer once its commit-queue entry completes:
	/// either its merge version has been published, or it aborted and
	/// removed its entry (a missing entry at or below the current commit
	/// id therefore counts as complete — aborted-and-removed, or already
	/// trimmed by cleanup). The watermark is the value readers snapshot
	/// as their conflict-window base, so it must only ever cover commits
	/// whose merge versions are already published. Amortised O(1): every
	/// slot is stepped over exactly once across all callers, and the CAS
	/// simply resolves which caller performs each step.
	pub(crate) fn advance_commit_watermark(&self) {
		loop {
			// Load the current watermark and the claimed commit ids
			let wm = self.commit_watermark.load(Ordering::SeqCst);
			let next = wm + 1;
			// Stop at the end of the claimed commit id range
			if next > self.transaction_commit_id.load(Ordering::SeqCst) {
				break;
			}
			// Check whether the next commit in sequence has completed
			let complete = match self.transaction_commit_queue.get(&next) {
				Some(entry) => entry.value().merge_version.load(Ordering::SeqCst) != 0,
				None => true,
			};
			if !complete {
				break;
			}
			// Advance by one step; on CAS failure another caller advanced
			// past us, so reload and continue from the fresh watermark
			let _ = self.commit_watermark.compare_exchange(
				wm,
				next,
				Ordering::SeqCst,
				Ordering::SeqCst,
			);
		}
	}

	/// Scan the entire datastore, reclaiming stale versions on every key.
	///
	/// Steady-state reclamation happens inline at commit time, so this
	/// sweep is a low-frequency safety net for garbage which no future
	/// commit will visit: versions pinned by a since-departed reader on a
	/// key that is never written again, and the inert below-watermark
	/// entries which an out-of-order apply can leave behind.
	pub(crate) fn run_gc_full(&self, cleanup_ts: u64) {
		// Iterate over the entire datastore
		for entry in self.datastore.iter() {
			// Get a mutable reference to the versions list
			let mut versions = entry.value().write();
			// Clean up unnecessary older versions
			if versions.gc_older_versions(cleanup_ts) == 0 {
				// Remove the entry while still holding the version write lock,
				// so a committer blocked on that lock observes `is_removed()`
				// and re-inserts rather than writing into a node we are about
				// to unlink. `Entry::remove` also unlinks at the cursor with
				// no second key lookup.
				entry.remove();
			}
		}
	}
}

/// Returns the minimum value of one slot dimension across all pinned
/// slots, bounded by `fallback`, or `None` when any scanned slot still
/// holds [`SLOT_PINNING`] in that dimension.
///
/// The fence pairs with the fence in the pin-then-read registration
/// protocol: a transaction whose pin-fence precedes ours in the SeqCst
/// total order is visible to this scan (as a value or as the sentinel);
/// a transaction whose pin-fence follows ours performs its snapshot
/// loads after the caller's bound load, so its snapshot is at least the
/// caller's fallback bound. `exclude` skips a single slot id — used by
/// a committer to exclude its own slot, which is safe only because a
/// committing transaction performs no further reads.
#[inline]
pub(crate) fn earliest_pinned(
	map: &SkipMap<u64, Arc<Slot>>,
	dim: impl Fn(&Slot) -> &AtomicU64,
	fallback: u64,
	exclude: Option<u64>,
) -> Option<u64> {
	fence(Ordering::SeqCst);
	let mut min = fallback;
	for entry in map.iter() {
		if Some(*entry.key()) == exclude {
			continue;
		}
		match dim(entry.value()).load(Ordering::SeqCst) {
			SLOT_PINNING => return None,
			v => min = min.min(v),
		}
	}
	Some(min)
}

impl Default for Inner {
	fn default() -> Self {
		Self::new(&DatabaseOptions::default())
	}
}
