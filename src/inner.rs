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
use papaya::HashSet;
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
	/// Bounded by, and advanced only after, `transaction_commit_id`.
	pub(crate) commit_watermark: AtomicU64,
	/// The commit-queue slot allocation counter. Slots are claimed from
	/// here (dense and never re-used) rather than by probing queue
	/// membership, because commit entries are removed on conflict aborts
	/// and by cleanup, and a re-claimed vacated slot could sit below the
	/// completed watermark with an unpublished merge version.
	pub(crate) transaction_queue_id: AtomicU64,
	/// The contiguous inserted prefix of the commit queue: every slot at
	/// or below this value has definitely been inserted into
	/// `transaction_commit_queue`. Advanced opportunistically by
	/// [`Inner::try_advance_commit_prefix`] — no committer waits for it
	/// to reach their own slot, they merely try to help it along after
	/// their own insert. This is safe only because a commit-queue entry
	/// is never physically removed before this bound has confirmably
	/// passed it: a missing entry at the very next unadvanced slot can
	/// therefore only mean "not yet inserted", never "removed early" —
	/// see the conflict-abort branches in `TransactionInner::commit`,
	/// which mark an aborted entry rather than removing it, leaving
	/// physical removal to `cleanup_commit_queue` once the entry is
	/// safely below this bound.
	pub(crate) transaction_commit_id: AtomicU64,
	/// The transaction commit queue list of modifications
	pub(crate) transaction_commit_queue: SkipMap<u64, Arc<Commit>>,
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
	pub(crate) merge_retire_id: AtomicU64,
	/// Keys whose version chains may still hold reclaimable garbage:
	/// chains a commit could not trim to a single live value because a
	/// reader watermark pinned older versions (or the watermark scan was
	/// skipped mid-registration), and chains whose newest entry is a
	/// delete tombstone awaiting collapse. The background sweep visits
	/// only these keys instead of scanning the whole datastore, so sweep
	/// cost scales with the amount of pinned garbage rather than the
	/// dataset size. Only keys are stored (deduplicated, refcounted
	/// `Bytes` clones) — never values, which would pin the very memory
	/// the sweep exists to reclaim. While a long-lived reader pins the
	/// watermark, every distinct key overwritten during its lifetime
	/// stays tracked and is revisited (and re-tracked) by each sweep
	/// tick until the reader departs — the deliberate trade for exact
	/// reclamation the moment the pin clears; the per-tick cost is one
	/// chain-lock-and-trim attempt per tracked key.
	pub(crate) gc_candidates: HashSet<Bytes>,
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
			merge_retire_id: AtomicU64::new(0),
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
	/// after our bound load in the `SeqCst` total order and (the watermark
	/// being monotonic) returns at least our bound — its conflict window
	/// `snapshot + 1 ..` sits strictly above everything we trim. A
	/// transaction seen by the scan bounds the trim directly, and a slot
	/// still pinning aborts the pass entirely. A writer mid-commit holds
	/// its own slot until drop, so its conflict-check iteration is
	/// protected identically.
	pub(crate) fn cleanup_commit_queue(&self) {
		// Give the opportunistic watermarks a chance to catch up before
		// computing the trim bound: this call is infrequent (background
		// worker or manual), so the extra freshness is cheap here even
		// though it is deliberately skipped on the hot commit path.
		self.refresh_commit_watermark();
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
		// Retire whatever merge-queue entries are already fully applied
		// before computing the bound; see the equivalent call in
		// `cleanup_commit_queue`. The publish step itself is no longer
		// opportunistic (see `TransactionInner::atomic_merge`), so this
		// exists to free retired entries, not to advance the clock.
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
	/// commit queue as far as currently possible.
	///
	/// Non-blocking: unlike a claim-order publish barrier that makes a
	/// committer wait for its predecessor, this never waits for a
	/// specific slot to be inserted — it walks forward while the next
	/// slot is present, and stops the instant it finds a gap (a slot
	/// that has been claimed via `transaction_queue_id` but not yet
	/// inserted), leaving that gap for a later call — from the slow
	/// claimant's own insert, from another committer's courtesy call, or
	/// from cleanup's/GC's opportunistic refresh — to close. The caller
	/// never needs its own slot reflected in this bound before
	/// proceeding: see the doc comment on `transaction_commit_id` for
	/// why "absent" unambiguously means "not yet inserted" here, and why
	/// every downstream consumer of a lagging value stays safe (
	/// `cleanup_commit_queue`'s idle fallback only trims less;
	/// `advance_commit_watermark`'s loop bound only holds
	/// `commit_watermark` back, never advances it past an unconfirmed
	/// slot).
	pub(crate) fn try_advance_commit_prefix(&self) {
		let mut spins = 0;
		loop {
			// Load the current bound and the claimed-slot ceiling
			let cur = self.transaction_commit_id.load(Ordering::SeqCst);
			let next = cur + 1;
			// Nothing has claimed this slot yet
			if next > self.transaction_queue_id.load(Ordering::SeqCst) {
				break;
			}
			// Claimed but not yet inserted: stop, don't wait for it
			if self.transaction_commit_queue.get(&next).is_none() {
				break;
			}
			// Advance by one step; on CAS failure another caller advanced
			// past us, so reload and continue from the fresh bound. Back
			// off on contention: a caller of this function (e.g. a
			// cleanup sweep with no delay between iterations) can end up
			// calling it far more often than intended, and retrying here
			// without a delay would otherwise hammer this cache line
			// against every other thread doing the same.
			if self
				.transaction_commit_id
				.compare_exchange_weak(cur, next, Ordering::SeqCst, Ordering::SeqCst)
				.is_err()
			{
				crate::tx::backoff(spins);
				spins += 1;
			}
		}
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
		let mut spins = 0;
		loop {
			let cur = self.oracle.timestamp.load(Ordering::SeqCst);
			let next = cur + 1;
			if next > self.oracle.alloc.load(Ordering::SeqCst) {
				break;
			}
			if self.transaction_merge_queue.get(&next).is_none() {
				break;
			}
			// Back off on contention — see the equivalent comment in
			// `try_advance_commit_prefix`.
			if self
				.oracle
				.timestamp
				.compare_exchange_weak(cur, next, Ordering::SeqCst, Ordering::SeqCst)
				.is_err()
			{
				crate::tx::backoff(spins);
				spins += 1;
			}
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

	/// Advance the contiguous completed prefix of the commit queue.
	///
	/// Called by every committer once its commit-queue entry completes:
	/// its merge version has been published, it aborted on a conflict and
	/// removed its entry, or it unwound and its guard marked the entry
	/// [`COMMIT_ABORTED`]. Commit slots are claimed from a dense allocator
	/// and `transaction_commit_id` is published strictly in claim order
	/// once the entry is inserted, so every slot at or below it was
	/// inserted exactly once and can never be re-claimed — a missing
	/// entry therefore only ever means aborted-and-removed or already
	/// trimmed by cleanup, both of which count as complete. The watermark
	/// is the value readers snapshot as their conflict-window base, so it
	/// must only ever cover commits whose merge versions are published or
	/// which will never publish one. Amortised O(1): every slot is
	/// stepped over exactly once across all callers, and the CAS simply
	/// resolves which caller performs each step.
	pub(crate) fn advance_commit_watermark(&self) {
		loop {
			// Load the current watermark and the inserted prefix bound
			let wm = self.commit_watermark.load(Ordering::SeqCst);
			let next = wm + 1;
			// Stop at the end of the inserted commit slot prefix
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
		loop {
			// Load the current watermark and the published prefix bound
			let wm = self.merge_retire_id.load(Ordering::SeqCst);
			let next = wm + 1;
			// Stop at the end of the published merge version prefix
			if next > self.oracle.timestamp.load(Ordering::SeqCst) {
				break;
			}
			// Check whether the next merge in sequence has been applied
			if let Some(entry) = self.transaction_merge_queue.get(&next) {
				if !entry.value().applied.load(Ordering::SeqCst) {
					break;
				}
				// Remove the applied entry as the watermark passes it
				entry.remove();
			}
			// Advance by one step; on CAS failure another caller advanced
			// past us, so reload and continue from the fresh watermark
			let _ =
				self.merge_retire_id.compare_exchange(wm, next, Ordering::SeqCst, Ordering::SeqCst);
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
	#[expect(
		clippy::significant_drop_tightening,
		reason = "the version write guard must cover entry.remove() so a committer blocked on it observes is_removed() and re-inserts"
	)]
	pub(crate) fn run_gc_tracked(&self, cleanup_ts: u64) {
		// A single map guard serves the whole sweep: guard churn per
		// candidate costs more than holding one across the pass, and the
		// candidate map holds only keys, so delaying its internal
		// reclamation for the duration of a sweep is immaterial.
		let candidates = self.gc_candidates.pin();
		// The steady state is an empty candidate set: every chain fully
		// trimmed at commit time. Skip the snapshot allocation entirely.
		if candidates.is_empty() {
			return;
		}
		// Snapshot the candidate keys: the snapshot is the sweep's
		// working set, and keys tracked by commits racing with this
		// sweep are picked up by the next pass.
		let mut keys: Vec<Bytes> = Vec::with_capacity(candidates.len());
		keys.extend(candidates.iter().cloned());
		// Process each candidate key in turn
		for key in keys {
			// Untrack the key first — see the ordering argument above
			candidates.remove(&key);
			// The chain may have been unlinked by an earlier collapse
			let Some(entry) = self.datastore.get(&key) else {
				continue;
			};
			// Get a mutable reference to the versions list. The write guard is
			// deliberately held across `entry.remove()` below: a committer
			// blocked on this lock must observe `is_removed()` and re-insert,
			// rather than writing into a node we are about to unlink.
			let mut versions = entry.value().write();
			// A sweep or commit-time collapse unlinked this node between
			// lookup and lock; any recreation re-tracks the key itself
			if entry.is_removed() {
				continue;
			}
			// Clean up unnecessary older versions
			if versions.gc_older_versions(cleanup_ts) == 0 {
				// Remove the entry while still holding the version write
				// lock — see the equivalent removal in `run_gc_full`.
				entry.remove();
			} else if versions.needs_gc() {
				// Versions remain pinned by a reader watermark: re-track
				// the key so a later sweep can finish the job.
				candidates.insert(key);
			}
		}
	}

	/// Scan the entire datastore, reclaiming stale versions on every key.
	///
	/// The background sweep visits only tracked candidate keys (see
	/// [`Inner::run_gc_tracked`]); this full scan backs the manual
	/// [`crate::Database::run_gc`] entry point and the one-shot pass at
	/// persistence load time, which collapses the multi-version chains
	/// that append-only-log replay builds up. Since a full scan visits
	/// every key, it supersedes the candidate set: callers clear the set
	/// before scanning (commits racing with the scan re-track their keys
	/// as usual).
	#[expect(
		clippy::significant_drop_tightening,
		reason = "the version write guard must cover entry.remove() so a committer blocked on it observes is_removed() and re-inserts"
	)]
	pub(crate) fn run_gc_full(&self, cleanup_ts: u64) {
		// Iterate over the entire datastore
		for entry in &self.datastore {
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
			} else if versions.needs_gc() {
				// A reader watermark is pinning reclaimable versions. Track
				// the key so the targeted sweep revisits it once the pin
				// clears: the caller cleared the candidate set before this
				// scan, and no future commit or sweep would otherwise ever
				// visit a key that is never written again.
				self.gc_candidates.pin().insert(entry.key().clone());
			}
		}
	}
}

/// Returns the minimum value of one slot dimension across all pinned
/// slots, bounded by `fallback`, or `None` when any scanned slot still
/// holds [`SLOT_PINNING`] in that dimension.
///
/// The fence pairs with the fence in the pin-then-read registration
/// protocol: a transaction whose pin-fence precedes ours in the `SeqCst`
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
	for entry in map {
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
