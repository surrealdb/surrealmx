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
	/// The underlying lock-free Skip Map datastructure
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
			reset_threshold: opts.reset_threshold,
		}
	}
}

impl Inner {
	/// Returns the earliest active reader's snapshot version, or `fallback`
	/// if no reader is currently registered. Pairs with the `SeqCst` load
	/// + fence in `register_counter` to give writers a watermark that
	/// observes every reader whose registration is totally ordered before
	/// the fence below.
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
