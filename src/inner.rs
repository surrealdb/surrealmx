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

use crate::options::DEFAULT_RESET_THRESHOLD;
use crate::oracle::Oracle;
use crate::persistence::Persistence;
use crate::queue::{Commit, Merge};
use crate::versions::Versions;
use crate::DatabaseOptions;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// The inner structure of the transactional in-memory database
pub struct Inner<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The timestamp version oracle
	pub(crate) oracle: Arc<Oracle>,
	/// The underlying lock-free Skip Map datastructure
	pub(crate) datastore: SkipMap<K, RwLock<Versions<V>>>,
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
	pub(crate) transaction_commit_queue: SkipMap<u64, Arc<Commit<K, V>>>,
	/// Transaction updates which are committed but not yet applied
	pub(crate) transaction_merge_queue: SkipMap<u64, Arc<Merge<K, V>>>,
	/// The epoch duration to determine how long to store versioned data
	pub(crate) garbage_collection_epoch: RwLock<Option<Duration>>,
	/// Optional persistence handler
	pub(crate) persistence: RwLock<Option<Arc<Persistence<K, V>>>>,
	/// Specifies whether background worker threads are enabled
	pub(crate) background_threads_enabled: AtomicBool,
	/// Stores a handle to the current transaction cleanup background thread
	pub(crate) transaction_cleanup_handle: RwLock<Option<JoinHandle<()>>>,
	/// Stores a handle to the current garbage collection background thread
	pub(crate) garbage_collection_handle: RwLock<Option<JoinHandle<()>>>,
	/// Threshold after which transaction state is reset
	pub(crate) reset_threshold: usize,
}

impl<K, V> Inner<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
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
			persistence: RwLock::new(None),
			background_threads_enabled: AtomicBool::new(true),
			transaction_cleanup_handle: RwLock::new(None),
			garbage_collection_handle: RwLock::new(None),
			reset_threshold: DEFAULT_RESET_THRESHOLD,
		}
	}
}

impl<K, V> Default for Inner<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	fn default() -> Self {
		Self::new(&DatabaseOptions::default())
	}
}
