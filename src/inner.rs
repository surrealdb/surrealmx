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
use crossbeam_queue::SegQueue;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::thread::JoinHandle;
use std::time::Duration;

/// Sentinel value stored in a counter entry while its owning [`Inner`]
/// SkipMap entry is being removed by [`crate::tx::Transaction::drop`]. A
/// concurrent `register_counter` will observe this marker and retry with a
/// fresh entry rather than incrementing a detached counter.
pub(crate) const COUNTER_TOMBSTONE: u64 = u64::MAX;

/// The inner structure of the transactional in-memory database
pub struct Inner {
	/// The timestamp version oracle
	pub(crate) oracle: Arc<Oracle>,
	/// The underlying lock-free skip-list datastructure
	pub(crate) datastore: SkipMap<Bytes, RwLock<Versions>>,
	/// A count of total transactions grouped by oracle version
	pub(crate) counter_by_oracle: SkipMap<u64, Arc<AtomicU64>>,
	/// A count of total transactions grouped by commit id
	pub(crate) counter_by_commit: SkipMap<u64, Arc<AtomicU64>>,
	/// The transaction commit queue attempt sequence number
	pub(crate) transaction_queue_id: AtomicU64,
	/// The transaction commit queue success sequence number
	pub(crate) transaction_commit_id: AtomicU64,
	/// The transaction merge queue attempt sequence number
	pub(crate) transaction_merge_id: AtomicU64,
	/// The transaction commit queue list of modifications
	pub(crate) transaction_commit_queue: SkipMap<u64, Arc<Commit>>,
	/// Transaction updates which are committed but not yet applied
	pub(crate) transaction_merge_queue: SkipMap<u64, Arc<Merge>>,
	/// The epoch duration to determine how long to store versioned data
	pub(crate) garbage_collection_epoch: RwLock<Option<Duration>>,
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
	/// Keys with stale versions pending incremental garbage collection
	pub(crate) gc_dirty_keys: SegQueue<Bytes>,
	/// Watermark below which versions are about to be (or have been)
	/// reclaimed by the garbage collector. The GC sweeper publishes its
	/// intended `cleanup_ts` here with a SeqCst `fetch_max` *before*
	/// actually reclaiming any versions. `register_counter` validates
	/// `v_r >= gc_floor` after publishing its counter; if a reader
	/// finds `gc_floor > v_r`, it rolls back and reloads the oracle,
	/// landing on a fresh snapshot above the floor. This closes the
	/// scan/gc race in the BG sweeper: a sweeper that misses an
	/// in-flight reader on its first scan publishes `gc_floor`, fences,
	/// then re-scans — and either the reader saw the new floor and
	/// retried, or its publish is now visible to the re-scan so the
	/// sweeper's final cleanup_ts is bounded by it.
	pub(crate) gc_floor: AtomicU64,
	/// Threshold after which transaction state is reset
	pub(crate) reset_threshold: usize,
}

impl Inner {
	/// Create a new [`Inner`] structure with the given oracle resync interval.
	pub fn new(opts: &DatabaseOptions) -> Self {
		Self {
			oracle: Oracle::new(opts.resync_interval),
			datastore: SkipMap::new(),
			counter_by_oracle: SkipMap::new(),
			counter_by_commit: SkipMap::new(),
			transaction_queue_id: AtomicU64::new(0),
			transaction_commit_id: AtomicU64::new(0),
			transaction_merge_id: AtomicU64::new(0),
			transaction_commit_queue: SkipMap::new(),
			transaction_merge_queue: SkipMap::new(),
			garbage_collection_epoch: RwLock::new(None),
			#[cfg(not(target_arch = "wasm32"))]
			persistence: RwLock::new(None),
			background_threads_enabled: AtomicBool::new(true),
			#[cfg(not(target_arch = "wasm32"))]
			transaction_cleanup_handle: RwLock::new(None),
			#[cfg(not(target_arch = "wasm32"))]
			garbage_collection_handle: RwLock::new(None),
			gc_dirty_keys: SegQueue::new(),
			gc_floor: AtomicU64::new(0),
			reset_threshold: opts.reset_threshold,
		}
	}
}

impl Inner {
	/// Returns the earliest active reader's snapshot version, or `fallback`
	/// if no reader is currently registered. Pairs with the SeqCst
	/// load-and-fence in `register_counter` to give writers a watermark
	/// that observes every reader whose registration is totally ordered
	/// before the fence below.
	#[inline]
	pub(crate) fn earliest_active_version(&self, fallback: u64) -> u64 {
		earliest_active(&self.counter_by_oracle, fallback)
	}

	/// Returns the earliest active reader's start commit id, or `fallback`
	/// if no reader is currently registered. See `earliest_active_version`.
	#[inline]
	pub(crate) fn earliest_active_commit(&self, fallback: u64) -> u64 {
		earliest_active(&self.counter_by_commit, fallback)
	}

	/// Trim the transaction commit queue below the earliest active
	/// transaction's commit snapshot, or below the current commit id when
	/// no transaction is registered at all.
	///
	/// Commit queue entries are only ever read by conflict detection,
	/// which scans `(tx.commit, version)` — strictly above the committing
	/// transaction's registered snapshot — so entries at or below every
	/// registered snapshot are unreachable.
	///
	/// The idle fallback is safe against concurrent registration because
	/// the fallback is loaded from `transaction_commit_id` *before* the
	/// counter map is scanned: a registering transaction that the scan
	/// misses validates `transaction_commit_id` against its snapshot
	/// *after* publishing its counter (see `register_counter`), so by
	/// monotonicity of the commit id its snapshot is `>=` our fallback,
	/// and its conflict window `(snapshot, ..)` is untouched by the trim.
	pub(crate) fn run_cleanup_inner(&self) {
		// Load the idle fallback BEFORE scanning the counter map (see above)
		let fallback = self.transaction_commit_id.load(Ordering::SeqCst);
		// Find the earliest registered commit snapshot, if any
		let oldest = self.earliest_active_commit(fallback);
		// Trim all commit queue entries below the watermark
		self.transaction_commit_queue.range(..oldest).for_each(|e| {
			e.remove();
		});
	}

	/// Compute the next `cleanup_ts`, publishing it into `gc_floor` so
	/// concurrent `register_counter` retries any reader whose snapshot
	/// is below it, then re-scan to bound by any newly-arrived reader.
	///
	/// The proposed value is capped at the current oracle timestamp.
	/// Without that cap, an idle database (oracle frozen while wall
	/// clock advances) would push `gc_floor` above any value a future
	/// reader could load — causing `register_counter` to spin forever
	/// retrying.
	pub(crate) fn compute_cleanup_ts(&self) -> u64 {
		let now = self.oracle.current_time_ns();
		let history = self.garbage_collection_epoch.read().unwrap_or_default().as_nanos();
		let history_cutoff = now.saturating_sub(history as u64);
		let earliest_tx = self.earliest_active_version(now);
		let oracle_now = self.oracle.inner.timestamp.load(Ordering::SeqCst);
		// `gc_floor` must stay <= oracle so any future reader's load
		// (which returns >= current oracle) satisfies the floor check.
		let proposed = history_cutoff.min(earliest_tx).min(oracle_now);
		// Publish proposed cleanup_ts via `gc_floor` BEFORE reclaiming.
		// A `register_counter` that fences after CAS-publish will see
		// either the old floor (and its publish becomes visible to our
		// re-scan below via fence-fence SC ordering) or the new floor
		// (and will retry to a fresh snapshot above it).
		self.gc_floor.fetch_max(proposed, Ordering::SeqCst);
		fence(Ordering::SeqCst);
		let earliest_after = self.earliest_active_version(now);
		proposed.min(earliest_after)
	}

	/// Drain the dirty-key queue, reclaiming stale versions on each key and
	/// unlinking any whose version chain becomes empty.
	///
	/// Keys are de-duplicated within a pass: a hot key committed many times
	/// between gc cycles is enqueued once per commit, but reclaiming it more
	/// than once under a fixed `cleanup_ts` is wasted lock traffic — every
	/// pass after the first is a no-op. A version added by a commit that
	/// re-dirties the key mid-drain is newer than `cleanup_ts` and so not
	/// reclaimable this pass anyway; it is caught on the next commit or the
	/// periodic full scan.
	pub(crate) fn run_gc_dirty_inner(&self, cleanup_ts: u64) {
		let mut seen = HashSet::new();
		// Drain all keys from the dirty queue
		while let Some(key) = self.gc_dirty_keys.pop() {
			// Skip keys already reclaimed in this pass
			if !seen.insert(key.clone()) {
				continue;
			}
			// Reclaim stale versions on this key
			self.gc_key(&key, cleanup_ts);
		}
	}

	/// Scan the entire datastore, reclaiming stale versions on every key.
	pub(crate) fn run_gc_full(&self, cleanup_ts: u64) {
		// Iterate over the entire datastore
		for entry in self.datastore.iter() {
			// Get a mutable reference to the versions list
			let mut versions = entry.value().write();
			// Clean up unnecessary older versions
			if versions.gc_older_versions(cleanup_ts) == 0 {
				// Remove under the version write lock (see `gc_key`).
				entry.remove();
			}
		}
	}

	/// Reclaim stale versions on a single key, unlinking the entry if its
	/// version chain becomes empty.
	fn gc_key(&self, key: &Bytes, cleanup_ts: u64) {
		// Look the key up in the datastore
		if let Some(entry) = self.datastore.get(key) {
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

#[inline]
fn earliest_active(map: &SkipMap<u64, Arc<AtomicU64>>, fallback: u64) -> u64 {
	fence(Ordering::SeqCst);
	for entry in map.iter() {
		let c = entry.value().load(Ordering::Acquire);
		if c != 0 && c != COUNTER_TOMBSTONE {
			return *entry.key();
		}
	}
	fallback
}

impl Default for Inner {
	fn default() -> Self {
		Self::new(&DatabaseOptions::default())
	}
}
