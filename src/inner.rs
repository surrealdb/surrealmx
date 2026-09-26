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
use crate::queue::Merge;
use crate::readers::Readers;
use crate::ring::{CommitRing, DEFAULT_COMMIT_RING_CAPACITY};
use crate::DatabaseOptions;
use byteslice::ByteSlice;
use crossbeam_skiplist::SkipMap;
use crossbeam_utils::CachePadded;
use papaya::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::thread::JoinHandle;

/// Sentinel published in a slot field while its owning transaction is
/// choosing its snapshot. Merge versions and commit ids are logical
/// counters seeded from persisted data and guarded at load time, so they
/// can never reach this value.
pub(crate) const SLOT_PINNING: u64 = u64::MAX;

/// Sentinel stored in a commit-queue entry's `merge_version` when the
/// owning transaction unwound without completing its commit. An aborted
/// entry counts as complete for the commit-watermark advance (its writes
/// will never be published) and is skipped by the conflict loop. Real
/// merge versions are guarded at load time and can never reach this
/// value.
pub(crate) const COMMIT_ABORTED: u64 = u64::MAX;

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
	/// The owner's snapshot merge version, or `SLOT_PINNING`
	pub(crate) version: AtomicU64,
	/// The owner's snapshot commit id, or `SLOT_PINNING`
	pub(crate) commit: AtomicU64,
}

impl Slot {
	/// Create a new slot in the pinning state
	pub(crate) const fn pinning() -> Self {
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
	/// The underlying concurrent ART datastructure
	pub(crate) datastore: artmap::VersionedArtMap<ByteSlice, Option<ByteSlice>>,
	/// Registered transaction snapshot slots, partitioned across cache-padded
	/// shards. Contains exactly the live transactions: slots are inserted at
	/// registration and removed on transaction drop, so watermark scans
	/// walk a map sized by concurrency, not by the transaction pool.
	pub(crate) readers: Readers,
	/// Monotonic slot id allocator for the readers map
	pub(crate) reader_slot_id: CachePadded<AtomicU64>,
	/// The contiguous completed prefix of the commit ring: every commit
	/// with an id at or below this watermark has either published its
	/// merge version or aborted.
	pub(crate) commit_watermark: CachePadded<AtomicU64>,
	/// The fixed-size, lock-free OCC circular commit ring buffer.
	pub(crate) commit_ring: CommitRing,
	/// Transaction updates which are committed but not yet applied
	pub(crate) transaction_merge_queue: SkipMap<u64, Arc<Merge>>,
	/// The contiguous retired prefix of the merge queue: every merge
	/// version at or below this watermark has been fully applied to the
	/// datastore and its queue entry removed. Merge entries are retired
	/// strictly in version order — never individually on completion —
	/// because reads resolve the queue overlay with priority over the
	/// datastore chain: if a newer version's entry were removed while an
	/// older version was still applying, a reader would find the older
	/// surviving entry and return a stale value for a snapshot that
	/// should see the newer one. In-order retirement guarantees every
	/// version above the watermark is still present in the queue, so the
	/// newest overlay hit at or below a snapshot is the newest write.
	/// Bounded by, and advanced only after, the published merge clock
	/// (`oracle.timestamp`).
	pub(crate) merge_retire_id: CachePadded<AtomicU64>,
	/// Keys whose version chains may still hold reclaimable garbage:
	/// chains a commit could not trim to a single live value because a
	/// reader watermark pinned older versions (or the watermark scan was
	/// skipped mid-registration), and chains whose newest entry is a
	/// delete tombstone awaiting collapse. The background sweep visits
	/// only these keys instead of scanning the whole datastore, so sweep
	/// cost scales with the amount of pinned garbage rather than the
	/// dataset size. Only keys are stored (deduplicated, refcounted
	/// Only keys are stored (deduplicated, refcounted
	/// `ByteSlice` clones) — never values, which would pin the very memory
	/// the sweep exists to reclaim. While a long-lived reader pins the
	/// watermark, every distinct key overwritten during its lifetime
	/// stays tracked and is revisited (and re-tracked) by each sweep
	/// tick until the reader departs — the deliberate trade for exact
	/// reclamation the moment the pin clears; the per-tick cost is one
	/// chain-lock-and-trim attempt per tracked key.
	pub(crate) gc_candidates: HashSet<ByteSlice>,
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
			datastore: artmap::VersionedArtMap::new(),
			readers: Readers::new(),
			reader_slot_id: CachePadded::new(AtomicU64::new(0)),
			commit_watermark: CachePadded::new(AtomicU64::new(0)),
			commit_ring: CommitRing::new(DEFAULT_COMMIT_RING_CAPACITY, 1),
			transaction_merge_queue: SkipMap::new(),
			merge_retire_id: CachePadded::new(AtomicU64::new(0)),
			gc_candidates: HashSet::new(),
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
		self.readers.earliest_pinned(|s| &s.version, fallback, None)
	}

	/// Returns the minimum snapshot commit id across all pinned
	/// transaction slots, bounded by `fallback`, or `None` when any slot
	/// is mid-registration. See [`earliest_pinned`].
	#[inline]
	pub(crate) fn earliest_active_commit(&self, fallback: u64) -> Option<u64> {
		self.readers.earliest_pinned(|s| &s.commit, fallback, None)
	}

	/// Returns the number of unretired commits in the commit ring.
	pub fn unretired_commits(&self) -> u64 {
		let prefix = self.commit_ring.published_prefix.load(Ordering::SeqCst);
		let taken = self.commit_ring.taken.load(Ordering::SeqCst);
		prefix.saturating_sub(taken)
	}

	/// Trim commit-queue entries which no active or future transaction can
	/// need for conflict detection.
	pub(crate) fn cleanup_commit_queue(&self) {
		self.refresh_commit_watermark();
		let fallback = self.commit_watermark.load(Ordering::SeqCst);
		if let Some(oldest) = self.earliest_active_commit(fallback) {
			self.commit_ring.advance_taken(oldest.saturating_sub(1));
		}
	}

	/// Compute the watermark for commit-time inline garbage collection,
	/// or `None` when a registration is in flight — in which case the
	/// committer skips inline reclamation for this commit and instead
	/// tracks each key it touched in [`Inner::gc_candidates`], so the
	/// tracked background sweep (or the next commit to the key) catches
	/// up.
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
		self.readers.earliest_pinned(|s| &s.version, now, Some(own_slot))
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
		// Retire applied merge-queue entries to free memory before computing
		// the cleanup bound. Clock advancement is handled continuously by
		// TransactionInner::atomic_merge.
		self.refresh_merge_watermark();
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

	/// Opportunistically advance the contiguous inserted prefix of the
	/// commit ring as far as currently possible.
	pub(crate) fn try_advance_commit_prefix(&self) {
		self.commit_ring.advance_published_prefix();
	}

	/// Opportunistically advance the published merge clock as far as
	/// currently possible. See [`Inner::try_advance_commit_prefix`] for
	/// the non-blocking shape; the same safety argument applies with
	/// `oracle.alloc` in place of `transaction_queue_id` and
	/// `transaction_merge_queue` in place of the commit queue — a merge
	/// entry is likewise never physically removed (by ordinary
	/// retirement, which is bounded by this very clock, or by the
	/// persistence-failure path, which marks rather than removes) before
	/// the clock has confirmably passed it.
	pub(crate) fn try_advance_merge_clock(&self) {
		let max_claimed = self.oracle.alloc.load(Ordering::SeqCst);
		let cur = self.oracle.timestamp.load(Ordering::SeqCst);
		let mut target = cur;
		while target < max_claimed && self.transaction_merge_queue.get(&(target + 1)).is_some() {
			target += 1;
		}
		if target > cur {
			let _ = self.oracle.timestamp.compare_exchange(
				cur,
				target,
				Ordering::SeqCst,
				Ordering::SeqCst,
			);
		}
	}

	/// Opportunistically advance both the inserted-prefix bound and the
	/// completed-prefix watermark of the commit queue as far as
	/// currently possible. Called by writers after their own commit-slot
	/// insert to help other in-flight committers make progress, and by
	/// the cleanup path before computing its trim bound, for freshness.
	pub(crate) fn refresh_commit_watermark(&self) {
		self.try_advance_commit_prefix();
		self.advance_commit_watermark();
	}

	/// Opportunistically advance both the published merge clock and the
	/// retired prefix of the merge queue as far as currently possible.
	/// Called by writers after their own merge insert, and by the
	/// garbage-collection path before computing its cleanup bound.
	pub(crate) fn refresh_merge_watermark(&self) {
		self.try_advance_merge_clock();
		self.advance_merge_retirement();
	}

	/// Advance the contiguous completed prefix of the commit ring.
	pub(crate) fn advance_commit_watermark(&self) {
		let max_prefix = self.commit_ring.published_prefix.load(Ordering::Acquire);
		let wm = self.commit_watermark.load(Ordering::Acquire);
		let mut target = wm;
		while target < max_prefix {
			let next = target + 1;
			let slot = self.commit_ring.slot(next);
			let guard = slot.commit.read();
			let complete = match *guard {
				Some(ref entry) => entry.merge_version.load(Ordering::SeqCst) != 0,
				None => true,
			};
			drop(guard);
			if !complete {
				break;
			}
			target = next;
		}
		if target > wm {
			let _ = self.commit_watermark.compare_exchange(
				wm,
				target,
				Ordering::Release,
				Ordering::Relaxed,
			);
		}
	}

	/// Advance the contiguous retired prefix of the merge queue, removing
	/// entries as the watermark passes them.
	///
	/// Called by every committer once its merge entry is fully applied to
	/// the datastore. Entries are removed strictly in version order (see
	/// the `merge_retire_id` field documentation): the bound is the
	/// published clock, below which every version's entry was inserted,
	/// and a missing entry means it was already retired by a racer or
	/// removed early on the persistence-failure path — in either case its
	/// data is in the datastore chains, so the watermark may pass it.
	pub(crate) fn advance_merge_retirement(&self) {
		let max_published = self.oracle.timestamp.load(Ordering::SeqCst);
		let wm = self.merge_retire_id.load(Ordering::SeqCst);
		let mut target = wm;
		while target < max_published {
			let next = target + 1;
			if let Some(entry) = self.transaction_merge_queue.get(&next) {
				if !entry.value().applied.load(Ordering::SeqCst) {
					break;
				}
				entry.remove();
			}
			target = next;
		}
		if target > wm {
			let _ = self.merge_retire_id.compare_exchange(
				wm,
				target,
				Ordering::SeqCst,
				Ordering::SeqCst,
			);
		}
	}

	/// Reclaim stale versions on the tracked candidate keys only.
	///
	/// Steady-state reclamation happens inline at commit time; whenever a
	/// commit cannot trim a chain to a single live value it tracks the
	/// key in [`Inner::gc_candidates`], so this sweep visits exactly the
	/// keys which may still hold garbage — cost scales with the amount of
	/// pinned garbage, not the dataset size.
	///
	/// A key is removed from the candidate set BEFORE its chain is
	/// examined. That ordering makes the untrack race-free against
	/// concurrent commits: a committer inserts its key only after
	/// pushing the garbage-leaving version under the chain write lock,
	/// so any garbage added after this sweep's trim re-inserts the key
	/// for the next pass — the removal here can never orphan it. When
	/// the trimmed chain still holds reclaimable versions (a reader
	/// watermark is pinning them), the key is re-tracked for the next
	/// sweep.
	pub(crate) fn run_gc_tracked(&self, cleanup_ts: u64) {
		let candidates = self.gc_candidates.pin();
		if candidates.is_empty() {
			return;
		}
		let mut keys: Vec<ByteSlice> = Vec::with_capacity(candidates.len());
		keys.extend(candidates.iter().cloned());
		for key in keys {
			candidates.remove(&key);
			self.datastore.prune_key(&key, cleanup_ts, Option::is_none);
			if self.datastore.version_count(&key) > 1 {
				candidates.insert(key);
			}
		}
	}

	/// Scan the entire datastore, reclaiming stale versions on every key.
	pub(crate) fn run_gc_full(&self, cleanup_ts: u64) {
		for entry in &self.datastore {
			let key = entry.key();
			self.datastore.prune_key(key, cleanup_ts, Option::is_none);
			if self.datastore.version_count(key) > 1 {
				self.gc_candidates.pin().insert(key.clone());
			}
		}
	}
}

impl Default for Inner {
	fn default() -> Self {
		Self::new(&DatabaseOptions::default())
	}
}
