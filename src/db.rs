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

//! This module stores the core in-memory database type.

use crate::inner::Inner;
use crate::options::DatabaseOptions;
#[cfg(not(target_arch = "wasm32"))]
use crate::options::{DEFAULT_CLEANUP_INTERVAL, DEFAULT_GC_INTERVAL};
#[cfg(not(target_arch = "wasm32"))]
use crate::persistence::Persistence;
use crate::pool::Pool;
use crate::pool::DEFAULT_POOL_SIZE;
use crate::tx::Transaction;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

// --------------------------------------------------
// Database
// --------------------------------------------------

/// A transactional in-memory database
pub struct Database {
	/// The inner structure of the database
	inner: Arc<Inner>,
	/// The database transaction pool
	pool: Arc<Pool>,
	/// Optional persistence configuration
	#[cfg(not(target_arch = "wasm32"))]
	persistence: Option<Persistence>,
	/// Interval used by the garbage collector thread
	#[cfg(not(target_arch = "wasm32"))]
	gc_interval: Duration,
	/// Interval used by the cleanup thread
	#[cfg(not(target_arch = "wasm32"))]
	cleanup_interval: Duration,
}

impl Default for Database {
	fn default() -> Self {
		let inner = Arc::new(Inner::default());
		let pool = Pool::new(Arc::clone(&inner), DEFAULT_POOL_SIZE);
		Self {
			inner,
			pool,
			#[cfg(not(target_arch = "wasm32"))]
			persistence: None,
			#[cfg(not(target_arch = "wasm32"))]
			gc_interval: DEFAULT_GC_INTERVAL,
			#[cfg(not(target_arch = "wasm32"))]
			cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
		}
	}
}

impl Drop for Database {
	fn drop(&mut self) {
		self.shutdown();
	}
}

impl Deref for Database {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl Database {
	/// Create a new transactional in-memory database
	pub fn new() -> Self {
		Self::new_with_options(DatabaseOptions::default())
	}

	/// Create a new transactional in-memory database with custom options
	pub fn new_with_options(opts: DatabaseOptions) -> Self {
		//  Create a new inner database
		let inner = Arc::new(Inner::new(&opts));
		// Initialise a transaction pool
		let pool = Pool::new(Arc::clone(&inner), opts.pool_size);
		// Create the database
		let db = Self {
			inner,
			pool,
			#[cfg(not(target_arch = "wasm32"))]
			persistence: None,
			#[cfg(not(target_arch = "wasm32"))]
			gc_interval: opts.gc_interval,
			#[cfg(not(target_arch = "wasm32"))]
			cleanup_interval: opts.cleanup_interval,
		};
		// Start background tasks when enabled
		#[cfg(not(target_arch = "wasm32"))]
		{
			if opts.enable_cleanup {
				db.initialise_cleanup_worker();
			}
			if opts.enable_gc {
				db.initialise_garbage_worker();
			}
		}
		// Return the database
		db
	}

	/// Create a new persistent database with custom options and persistence
	/// settings
	#[cfg(not(target_arch = "wasm32"))]
	pub fn new_with_persistence(
		opts: DatabaseOptions,
		persistence_opts: crate::PersistenceOptions,
	) -> std::io::Result<Self> {
		//  Create a new inner database
		let inner = Arc::new(Inner::new(&opts));
		// Initialise a transaction pool
		let pool = Pool::new(Arc::clone(&inner), opts.pool_size);
		// Create a new persistence layer with options
		let persist = Persistence::new_with_options(persistence_opts, Arc::clone(&inner))
			.map_err(std::io::Error::other)?;
		// Replace the persistence layer in the database
		inner.persistence.write().replace(Arc::new(persist.clone()));
		// Create the database
		let db = Self {
			inner,
			pool,
			persistence: Some(persist),
			gc_interval: opts.gc_interval,
			cleanup_interval: opts.cleanup_interval,
		};
		// Start background tasks when enabled
		if opts.enable_cleanup {
			db.initialise_cleanup_worker();
		}
		if opts.enable_gc {
			db.initialise_garbage_worker();
		}
		// Return the database
		Ok(db)
	}

	/// Start a new transaction on this database
	pub fn transaction(&self, write: bool) -> Transaction {
		self.pool.get(write)
	}

	/// Get a reference to the persistence layer if enabled
	#[cfg(not(target_arch = "wasm32"))]
	pub const fn persistence(&self) -> Option<&Persistence> {
		self.persistence.as_ref()
	}

	/// Manually perform transaction queue cleanup.
	///
	/// This trims commit queue entries below the earliest active
	/// transaction's commit snapshot — or, when no transaction is
	/// registered, below the current commit id — so an idle database
	/// releases the whole queue rather than pinning it at its peak.
	///
	/// This should be called when automatic cleanup is disabled via
	/// [`DatabaseOptions::enable_cleanup`].
	pub fn run_cleanup(&self) {
		self.inner.cleanup_commit_queue();
	}

	/// Manually perform a full garbage collection sweep.
	///
	/// Steady-state reclamation happens inline at commit time: every
	/// commit trims the chains it touches down to the current watermark,
	/// and tracks any chain it could not fully trim for the targeted
	/// sweep (see [`Database::run_gc_tracked`]). This full scan visits
	/// every key regardless of tracking, so it also supersedes the
	/// candidate set — the set is cleared before scanning, and commits
	/// racing with the scan re-track their keys as usual. It is useful
	/// when automatic background GC is disabled via
	/// [`DatabaseOptions::enable_gc`], for example on wasm targets,
	/// which have no background threads.
	pub fn run_gc(&self) {
		// Skip the pass entirely when registrations are in flight
		if let Some(cleanup_ts) = self.compute_cleanup_ts() {
			// The full scan visits every candidate anyway
			self.gc_candidates.pin().clear();
			// Perform a full datastore scan for stale versions
			self.run_gc_full(cleanup_ts);
		}
	}

	/// Manually sweep only the keys tracked as holding reclaimable
	/// version garbage.
	///
	/// Commits track every chain they could not trim to a single live
	/// value (older versions pinned by a reader, or a newest-entry
	/// delete tombstone awaiting collapse), so this sweep's cost scales
	/// with the amount of pinned garbage rather than the dataset size.
	/// This is the pass the background garbage collector runs on every
	/// tick; call it manually when background GC is disabled via
	/// [`DatabaseOptions::enable_gc`], reserving the full-scan
	/// [`Database::run_gc`] for occasional use.
	pub fn run_gc_tracked(&self) {
		// The steady state is an empty candidate set: skip the watermark
		// computation (a fence and full slot scan) entirely when there
		// is nothing to sweep. A key tracked concurrently with this
		// check is picked up by the next pass.
		if self.gc_candidates.pin().is_empty() {
			return;
		}
		// Skip the pass entirely when registrations are in flight
		if let Some(cleanup_ts) = self.compute_cleanup_ts() {
			// Sweep only the tracked candidate keys
			self.inner.run_gc_tracked(cleanup_ts);
		}
	}

	/// Shutdown the datastore, waiting for background threads to exit
	fn shutdown(&self) {
		#[cfg(not(target_arch = "wasm32"))]
		{
			// First, disable Persistence background workers if present
			if let Some(ref persistence) = self.persistence {
				// Disable the persistence background workers
				persistence.background_threads_enabled.store(false, Ordering::Release);
				// Wait for persistence threads to exit. Each `take` is hoisted
				// into its own statement so the write guard is released before
				// the blocking `join`, instead of being held across it.
				let fsync = persistence.fsync_handle.write().take();
				if let Some(handle) = fsync {
					handle.thread().unpark();
					let _ = handle.join();
				}
				let snapshot = persistence.snapshot_handle.write().take();
				if let Some(handle) = snapshot {
					handle.thread().unpark();
					let _ = handle.join();
				}
				let appender = persistence.appender_handle.write().take();
				if let Some(handle) = appender {
					handle.thread().unpark();
					let _ = handle.join();
				}
			}
		}
		// Then disable Database background workers
		self.background_threads_enabled.store(false, Ordering::Relaxed);
		#[cfg(not(target_arch = "wasm32"))]
		{
			// Wait for the transaction cleanup thread to exit
			let cleanup = self.transaction_cleanup_handle.write().take();
			if let Some(handle) = cleanup {
				handle.thread().unpark();
				let _ = handle.join();
			}
			// Wait for the garbage collector thread to exit
			let gc = self.garbage_collection_handle.write().take();
			if let Some(handle) = gc {
				handle.thread().unpark();
				let _ = handle.join();
			}
		}
	}

	/// Start the transaction commit queue cleanup thread after creating the
	/// database
	#[cfg(not(target_arch = "wasm32"))]
	fn initialise_cleanup_worker(&self) {
		// Clone the underlying datastore inner
		let db = Arc::clone(&self.inner);
		// Check if a background thread is already running
		if db.transaction_cleanup_handle.read().is_none() {
			// Get the specified interval
			let interval = self.cleanup_interval;
			// Spawn a new thread to handle periodic cleanup
			let handle = std::thread::spawn(move || {
				// Check whether the garbage collection process is enabled
				while db.background_threads_enabled.load(Ordering::Relaxed) {
					// Wait for a specified time interval
					std::thread::park_timeout(interval);
					// Check shutdown flag again after waking
					if !db.background_threads_enabled.load(Ordering::Relaxed) {
						break;
					}
					// Clean up the transaction commit queue
					db.cleanup_commit_queue();
				}
			});
			// Store and track the thread handle
			*self.inner.transaction_cleanup_handle.write() = Some(handle);
		}
	}

	/// Start the garbage collection thread after creating the database
	#[cfg(not(target_arch = "wasm32"))]
	fn initialise_garbage_worker(&self) {
		// Clone the underlying datastore inner
		let db = Arc::clone(&self.inner);
		// Check if a background thread is already running
		if db.garbage_collection_handle.read().is_none() {
			// Get the specified interval
			let interval = self.gc_interval;
			// Spawn a new thread to handle the periodic tracked sweep.
			// Steady-state reclamation happens inline at commit time, and
			// every chain a commit could not fully trim is tracked in the
			// candidate set — so this sweep visits only tracked keys, and
			// its cost scales with the amount of pinned garbage rather
			// than the dataset size.
			let handle = std::thread::spawn(move || {
				// Check whether the garbage collection process is enabled
				while db.background_threads_enabled.load(Ordering::Relaxed) {
					// Wait for a specified time interval
					std::thread::park_timeout(interval);
					// Check shutdown flag again after waking
					if !db.background_threads_enabled.load(Ordering::Relaxed) {
						break;
					}
					// The steady state is an empty candidate set: skip
					// the watermark computation (a fence and full slot
					// scan) entirely when there is nothing to sweep.
					if db.gc_candidates.pin().is_empty() {
						continue;
					}
					// Compute the next cleanup_ts by scanning the slot
					// map: any transaction registering concurrently is
					// either visible to the scan (and bounds the final
					// cleanup_ts) or pins after it (and takes a snapshot
					// at or above the clock bound). A pass is skipped
					// entirely while a registration is pinning.
					let Some(cleanup_ts) = db.compute_cleanup_ts() else {
						continue;
					};
					// Sweep only the tracked candidate keys.
					db.run_gc_tracked(cleanup_ts);
				}
			});
			// Store and track the thread handle
			*self.inner.garbage_collection_handle.write() = Some(handle);
		}
	}
}

#[cfg(test)]
#[allow(
	clippy::significant_drop_tightening,
	reason = "lock contention is irrelevant in single-threaded assertions"
)]
mod tests {

	use super::*;

	#[test]
	fn begin_tx() {
		let db = Database::new();
		db.transaction(false);
	}

	#[test]
	fn finished_tx_not_writeable() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		let res = tx.cancel();
		assert!(res.is_ok());
		let res = tx.put("test", "something");
		assert!(res.is_err());
		let res = tx.set("test", "something");
		assert!(res.is_err());
		let res = tx.del("test");
		assert!(res.is_err());
		let res = tx.commit();
		assert!(res.is_err());
		let res = tx.cancel();
		assert!(res.is_err());
	}

	#[test]
	fn cancelled_tx_is_cancelled() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res.as_deref(), Some(b"something" as &[u8]));
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.exists("test").unwrap();
		assert!(!res);
		let res = tx.get("test").unwrap();
		assert_eq!(res, None);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn committed_tx_is_committed() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res.as_deref(), Some(b"something" as &[u8]));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res.as_deref(), Some(b"something" as &[u8]));
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn multiple_concurrent_readers() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res.as_deref(), Some(b"something" as &[u8]));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx1 = db.transaction(false);
		let res = tx1.exists("test").unwrap();
		assert!(res);
		let res = tx1.exists("temp").unwrap();
		assert!(!res);
		// ----------
		let mut tx2 = db.transaction(false);
		let res = tx2.exists("test").unwrap();
		assert!(res);
		let res = tx2.exists("temp").unwrap();
		assert!(!res);
		// ----------
		let res = tx1.cancel();
		assert!(res.is_ok());
		let res = tx2.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn multiple_concurrent_operators() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("test", "something").unwrap();
		let res = tx.exists("test").unwrap();
		assert!(res);
		let res = tx.get("test").unwrap();
		assert_eq!(res.as_deref(), Some(b"something" as &[u8]));
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx1 = db.transaction(false);
		let res = tx1.exists("test").unwrap();
		assert!(res);
		let res = tx1.exists("temp").unwrap();
		assert!(!res);
		// ----------
		let mut txw = db.transaction(true);
		txw.put("temp", "other").unwrap();
		let res = txw.exists("test").unwrap();
		assert!(res);
		let res = txw.exists("temp").unwrap();
		assert!(res);
		let res = txw.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx2 = db.transaction(false);
		let res = tx2.exists("test").unwrap();
		assert!(res);
		let res = tx2.exists("temp").unwrap();
		assert!(res);
		// ----------
		let res = tx1.exists("temp").unwrap();
		assert!(!res);
		// ----------
		let res = tx1.cancel();
		assert!(res.is_ok());
		let res = tx2.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn iterate_keys_forward() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "a").unwrap();
		tx.put("b", "b").unwrap();
		tx.put("c", "c").unwrap();
		tx.put("d", "d").unwrap();
		tx.put("e", "e").unwrap();
		tx.put("f", "f").unwrap();
		tx.put("g", "g").unwrap();
		tx.put("h", "h").unwrap();
		tx.put("i", "i").unwrap();
		tx.put("j", "j").unwrap();
		tx.put("k", "k").unwrap();
		tx.put("l", "l").unwrap();
		tx.put("m", "m").unwrap();
		tx.put("n", "n").unwrap();
		tx.put("o", "o").unwrap();
		let res = tx.keys("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0].as_ref(), b"c");
		assert_eq!(res[1], "d");
		assert_eq!(res[2], "e");
		assert_eq!(res[3], "f");
		assert_eq!(res[4], "g");
		assert_eq!(res[5], "h");
		assert_eq!(res[6], "i");
		assert_eq!(res[7], "j");
		assert_eq!(res[8], "k");
		assert_eq!(res[9], "l");
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.keys("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0].as_ref(), b"c");
		assert_eq!(res[1], "d");
		assert_eq!(res[2], "e");
		assert_eq!(res[3], "f");
		assert_eq!(res[4], "g");
		assert_eq!(res[5], "h");
		assert_eq!(res[6], "i");
		assert_eq!(res[7], "j");
		assert_eq!(res[8], "k");
		assert_eq!(res[9], "l");
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.keys("c".."z", Some(3), Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], "f");
		assert_eq!(res[1], "g");
		assert_eq!(res[2], "h");
		assert_eq!(res[3], "i");
		assert_eq!(res[4], "j");
		assert_eq!(res[5], "k");
		assert_eq!(res[6], "l");
		assert_eq!(res[7], "m");
		assert_eq!(res[8], "n");
		assert_eq!(res[9], "o");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn iterate_keys_reverse() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "a").unwrap();
		tx.put("b", "b").unwrap();
		tx.put("c", "c").unwrap();
		tx.put("d", "d").unwrap();
		tx.put("e", "e").unwrap();
		tx.put("f", "f").unwrap();
		tx.put("g", "g").unwrap();
		tx.put("h", "h").unwrap();
		tx.put("i", "i").unwrap();
		tx.put("j", "j").unwrap();
		tx.put("k", "k").unwrap();
		tx.put("l", "l").unwrap();
		tx.put("m", "m").unwrap();
		tx.put("n", "n").unwrap();
		tx.put("o", "o").unwrap();
		let res = tx.keys_reverse("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], "o");
		assert_eq!(res[1], "n");
		assert_eq!(res[2], "m");
		assert_eq!(res[3], "l");
		assert_eq!(res[4], "k");
		assert_eq!(res[5], "j");
		assert_eq!(res[6], "i");
		assert_eq!(res[7], "h");
		assert_eq!(res[8], "g");
		assert_eq!(res[9], "f");
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.keys_reverse("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], "o");
		assert_eq!(res[1], "n");
		assert_eq!(res[2], "m");
		assert_eq!(res[3], "l");
		assert_eq!(res[4], "k");
		assert_eq!(res[5], "j");
		assert_eq!(res[6], "i");
		assert_eq!(res[7], "h");
		assert_eq!(res[8], "g");
		assert_eq!(res[9], "f");
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.keys_reverse("c".."z", Some(3), Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0], "l");
		assert_eq!(res[1], "k");
		assert_eq!(res[2], "j");
		assert_eq!(res[3], "i");
		assert_eq!(res[4], "h");
		assert_eq!(res[5], "g");
		assert_eq!(res[6], "f");
		assert_eq!(res[7], "e");
		assert_eq!(res[8], "d");
		assert_eq!(res[9], "c");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn iterate_keys_values_forward() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "a").unwrap();
		tx.put("b", "b").unwrap();
		tx.put("c", "c").unwrap();
		tx.put("d", "d").unwrap();
		tx.put("e", "e").unwrap();
		tx.put("f", "f").unwrap();
		tx.put("g", "g").unwrap();
		tx.put("h", "h").unwrap();
		tx.put("i", "i").unwrap();
		tx.put("j", "j").unwrap();
		tx.put("k", "k").unwrap();
		tx.put("l", "l").unwrap();
		tx.put("m", "m").unwrap();
		tx.put("n", "n").unwrap();
		tx.put("o", "o").unwrap();
		let res = tx.scan("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0].0.as_ref(), b"c");
		assert_eq!(res[0].1.as_ref(), b"c");
		assert_eq!(res[1].0.as_ref(), b"d");
		assert_eq!(res[1].1.as_ref(), b"d");
		assert_eq!(res[2].0.as_ref(), b"e");
		assert_eq!(res[3].0.as_ref(), b"f");
		assert_eq!(res[4].0.as_ref(), b"g");
		assert_eq!(res[5].0.as_ref(), b"h");
		assert_eq!(res[6].0.as_ref(), b"i");
		assert_eq!(res[7].0.as_ref(), b"j");
		assert_eq!(res[8].0.as_ref(), b"k");
		assert_eq!(res[9].0.as_ref(), b"l");
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.scan("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0].0.as_ref(), b"c");
		assert_eq!(res[0].1.as_ref(), b"c");
		assert_eq!(res[1].0.as_ref(), b"d");
		assert_eq!(res[1].1.as_ref(), b"d");
		assert_eq!(res[2].0.as_ref(), b"e");
		assert_eq!(res[3].0.as_ref(), b"f");
		assert_eq!(res[4].0.as_ref(), b"g");
		assert_eq!(res[5].0.as_ref(), b"h");
		assert_eq!(res[6].0.as_ref(), b"i");
		assert_eq!(res[7].0.as_ref(), b"j");
		assert_eq!(res[8].0.as_ref(), b"k");
		assert_eq!(res[9].0.as_ref(), b"l");
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.scan("c".."z", Some(3), Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0].0.as_ref(), b"f");
		assert_eq!(res[1].0.as_ref(), b"g");
		assert_eq!(res[2].0.as_ref(), b"h");
		assert_eq!(res[3].0.as_ref(), b"i");
		assert_eq!(res[4].0.as_ref(), b"j");
		assert_eq!(res[5].0.as_ref(), b"k");
		assert_eq!(res[6].0.as_ref(), b"l");
		assert_eq!(res[7].0.as_ref(), b"m");
		assert_eq!(res[8].0.as_ref(), b"n");
		assert_eq!(res[9].0.as_ref(), b"o");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn iterate_keys_values_reverse() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "a").unwrap();
		tx.put("b", "b").unwrap();
		tx.put("c", "c").unwrap();
		tx.put("d", "d").unwrap();
		tx.put("e", "e").unwrap();
		tx.put("f", "f").unwrap();
		tx.put("g", "g").unwrap();
		tx.put("h", "h").unwrap();
		tx.put("i", "i").unwrap();
		tx.put("j", "j").unwrap();
		tx.put("k", "k").unwrap();
		tx.put("l", "l").unwrap();
		tx.put("m", "m").unwrap();
		tx.put("n", "n").unwrap();
		tx.put("o", "o").unwrap();
		let res = tx.scan_reverse("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0].0.as_ref(), b"o");
		assert_eq!(res[1].0.as_ref(), b"n");
		assert_eq!(res[2].0.as_ref(), b"m");
		assert_eq!(res[3].0.as_ref(), b"l");
		assert_eq!(res[4].0.as_ref(), b"k");
		assert_eq!(res[5].0.as_ref(), b"j");
		assert_eq!(res[6].0.as_ref(), b"i");
		assert_eq!(res[7].0.as_ref(), b"h");
		assert_eq!(res[8].0.as_ref(), b"g");
		assert_eq!(res[9].0.as_ref(), b"f");
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.scan_reverse("c".."z", None, Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0].0.as_ref(), b"o");
		assert_eq!(res[1].0.as_ref(), b"n");
		assert_eq!(res[2].0.as_ref(), b"m");
		assert_eq!(res[3].0.as_ref(), b"l");
		assert_eq!(res[4].0.as_ref(), b"k");
		assert_eq!(res[5].0.as_ref(), b"j");
		assert_eq!(res[6].0.as_ref(), b"i");
		assert_eq!(res[7].0.as_ref(), b"h");
		assert_eq!(res[8].0.as_ref(), b"g");
		assert_eq!(res[9].0.as_ref(), b"f");
		let res = tx.cancel();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.scan_reverse("c".."z", Some(3), Some(10)).unwrap();
		assert_eq!(res.len(), 10);
		assert_eq!(res[0].0.as_ref(), b"l");
		assert_eq!(res[1].0.as_ref(), b"k");
		assert_eq!(res[2].0.as_ref(), b"j");
		assert_eq!(res[3].0.as_ref(), b"i");
		assert_eq!(res[4].0.as_ref(), b"h");
		assert_eq!(res[5].0.as_ref(), b"g");
		assert_eq!(res[6].0.as_ref(), b"f");
		assert_eq!(res[7].0.as_ref(), b"e");
		assert_eq!(res[8].0.as_ref(), b"d");
		assert_eq!(res[9].0.as_ref(), b"c");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn count_keys_values() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "a").unwrap();
		tx.put("b", "b").unwrap();
		tx.put("c", "c").unwrap();
		tx.put("d", "d").unwrap();
		tx.put("e", "e").unwrap();
		tx.put("f", "f").unwrap();
		tx.put("g", "g").unwrap();
		tx.put("h", "h").unwrap();
		tx.put("i", "i").unwrap();
		tx.put("j", "j").unwrap();
		tx.put("k", "k").unwrap();
		tx.put("l", "l").unwrap();
		tx.put("m", "m").unwrap();
		tx.put("n", "n").unwrap();
		tx.put("o", "o").unwrap();
		let res = tx.total("c".."z", None, Some(10)).unwrap();
		assert_eq!(res, 10);
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let res = tx.total("c".."z", Some(3), Some(10)).unwrap();
		assert_eq!(res, 10);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	// --------------------------------------------------
	// Cursor and Iterator Tests
	// --------------------------------------------------

	#[test]
	fn cursor_forward_iteration() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("b", "2").unwrap();
		tx.put("c", "3").unwrap();
		tx.put("d", "4").unwrap();
		tx.put("e", "5").unwrap();
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let mut cursor = tx.cursor("a".."z").unwrap();
		cursor.seek_to_first();
		// Check first entry
		assert!(cursor.valid());
		assert_eq!(cursor.key().unwrap().as_ref(), b"a");
		assert_eq!(cursor.value().unwrap().as_ref(), b"1");
		// Move forward
		cursor.next();
		assert!(cursor.valid());
		assert_eq!(cursor.key().unwrap().as_ref(), b"b");
		// Continue to end
		cursor.next(); // c
		cursor.next(); // d
		cursor.next(); // e
		assert!(cursor.valid());
		assert_eq!(cursor.key().unwrap().as_ref(), b"e");
		cursor.next();
		assert!(!cursor.valid());
		drop(cursor);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn cursor_reverse_iteration() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("b", "2").unwrap();
		tx.put("c", "3").unwrap();
		tx.put("d", "4").unwrap();
		tx.put("e", "5").unwrap();
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let mut cursor = tx.cursor("a".."z").unwrap();
		cursor.seek_to_last();
		// Check last entry
		assert!(cursor.valid());
		assert_eq!(cursor.key().unwrap().as_ref(), b"e");
		assert_eq!(cursor.value().unwrap().as_ref(), b"5");
		// Move backward
		cursor.prev();
		assert!(cursor.valid());
		assert_eq!(cursor.key().unwrap().as_ref(), b"d");
		// Continue to beginning
		cursor.prev(); // c
		cursor.prev(); // b
		cursor.prev(); // a
		assert!(cursor.valid());
		assert_eq!(cursor.key().unwrap().as_ref(), b"a");
		cursor.prev();
		assert!(!cursor.valid());
		drop(cursor);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn cursor_seek_operations() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("c", "3").unwrap();
		tx.put("e", "5").unwrap();
		tx.put("g", "7").unwrap();
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let mut cursor = tx.cursor("a".."z").unwrap();
		// Seek to exact key
		cursor.seek("c");
		assert!(cursor.valid());
		assert_eq!(cursor.key().unwrap().as_ref(), b"c");
		// Seek to non-existent key (should land on next)
		cursor.seek("d");
		assert!(cursor.valid());
		assert_eq!(cursor.key().unwrap().as_ref(), b"e");
		// Seek for prev to exact key
		cursor.seek_for_prev("e");
		assert!(cursor.valid());
		assert_eq!(cursor.key().unwrap().as_ref(), b"c");
		// Seek beyond range
		cursor.seek("z");
		assert!(!cursor.valid());
		drop(cursor);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn cursor_bidirectional_switch() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("b", "2").unwrap();
		tx.put("c", "3").unwrap();
		tx.put("d", "4").unwrap();
		tx.put("e", "5").unwrap();
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let mut cursor = tx.cursor("a".."z").unwrap();
		// Start forward
		cursor.seek_to_first();
		assert_eq!(cursor.key().unwrap().as_ref(), b"a");
		cursor.next();
		assert_eq!(cursor.key().unwrap().as_ref(), b"b");
		cursor.next();
		assert_eq!(cursor.key().unwrap().as_ref(), b"c");
		// Switch to reverse
		cursor.prev();
		assert!(cursor.valid());
		assert_eq!(cursor.key().unwrap().as_ref(), b"b");
		// Switch back to forward
		cursor.next();
		assert!(cursor.valid());
		assert_eq!(cursor.key().unwrap().as_ref(), b"c");
		drop(cursor);
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn keys_iterator_forward() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("b", "2").unwrap();
		tx.put("c", "3").unwrap();
		tx.put("d", "4").unwrap();
		tx.put("e", "5").unwrap();
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let keys: Vec<_> = tx.keys_iter("b".."e").unwrap().collect();
		assert_eq!(keys.len(), 3);
		assert_eq!(keys[0].as_ref(), b"b");
		assert_eq!(keys[1].as_ref(), b"c");
		assert_eq!(keys[2].as_ref(), b"d");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn keys_iterator_reverse() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("b", "2").unwrap();
		tx.put("c", "3").unwrap();
		tx.put("d", "4").unwrap();
		tx.put("e", "5").unwrap();
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let keys: Vec<_> = tx.keys_iter_reverse("b".."e").unwrap().collect();
		assert_eq!(keys.len(), 3);
		assert_eq!(keys[0].as_ref(), b"d");
		assert_eq!(keys[1].as_ref(), b"c");
		assert_eq!(keys[2].as_ref(), b"b");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn keys_iterator_with_take() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("b", "2").unwrap();
		tx.put("c", "3").unwrap();
		tx.put("d", "4").unwrap();
		tx.put("e", "5").unwrap();
		tx.put("f", "6").unwrap();
		tx.put("g", "7").unwrap();
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let keys: Vec<_> = tx.keys_iter("a".."z").unwrap().take(3).collect();
		assert_eq!(keys.len(), 3);
		assert_eq!(keys[0].as_ref(), b"a");
		assert_eq!(keys[1].as_ref(), b"b");
		assert_eq!(keys[2].as_ref(), b"c");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn keys_iterator_with_skip() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("b", "2").unwrap();
		tx.put("c", "3").unwrap();
		tx.put("d", "4").unwrap();
		tx.put("e", "5").unwrap();
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let keys: Vec<_> = tx.keys_iter("a".."z").unwrap().skip(2).collect();
		assert_eq!(keys.len(), 3);
		assert_eq!(keys[0].as_ref(), b"c");
		assert_eq!(keys[1].as_ref(), b"d");
		assert_eq!(keys[2].as_ref(), b"e");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn scan_iterator_forward() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("b", "2").unwrap();
		tx.put("c", "3").unwrap();
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let pairs: Vec<_> = tx.scan_iter("a".."z").unwrap().collect();
		assert_eq!(pairs.len(), 3);
		assert_eq!(pairs[0].0.as_ref(), b"a");
		assert_eq!(pairs[0].1.as_ref(), b"1");
		assert_eq!(pairs[1].0.as_ref(), b"b");
		assert_eq!(pairs[1].1.as_ref(), b"2");
		assert_eq!(pairs[2].0.as_ref(), b"c");
		assert_eq!(pairs[2].1.as_ref(), b"3");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn scan_iterator_reverse() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("b", "2").unwrap();
		tx.put("c", "3").unwrap();
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let pairs: Vec<_> = tx.scan_iter_reverse("a".."z").unwrap().collect();
		assert_eq!(pairs.len(), 3);
		assert_eq!(pairs[0].0.as_ref(), b"c");
		assert_eq!(pairs[0].1.as_ref(), b"3");
		assert_eq!(pairs[1].0.as_ref(), b"b");
		assert_eq!(pairs[1].1.as_ref(), b"2");
		assert_eq!(pairs[2].0.as_ref(), b"a");
		assert_eq!(pairs[2].1.as_ref(), b"1");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn scan_iterator_with_take() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("b", "2").unwrap();
		tx.put("c", "3").unwrap();
		tx.put("d", "4").unwrap();
		tx.put("e", "5").unwrap();
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(false);
		let pairs: Vec<_> = tx.scan_iter("a".."z").unwrap().take(2).collect();
		assert_eq!(pairs.len(), 2);
		assert_eq!(pairs[0].0.as_ref(), b"a");
		assert_eq!(pairs[1].0.as_ref(), b"b");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn iterator_sees_uncommitted_writes() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("b", "2").unwrap();
		// Before commit, iterator should see uncommitted writes
		let keys: Vec<_> = tx.keys_iter("a".."z").unwrap().collect();
		assert_eq!(keys.len(), 2);
		assert_eq!(keys[0].as_ref(), b"a");
		assert_eq!(keys[1].as_ref(), b"b");
		let res = tx.commit();
		assert!(res.is_ok());
	}

	#[test]
	fn cursor_handles_deleted_entries() {
		let db = Database::new();
		// ----------
		let mut tx = db.transaction(true);
		tx.put("a", "1").unwrap();
		tx.put("b", "2").unwrap();
		tx.put("c", "3").unwrap();
		let res = tx.commit();
		assert!(res.is_ok());
		// ----------
		let mut tx = db.transaction(true);
		tx.del("b").unwrap();
		// Iterator should skip deleted entry
		let keys: Vec<_> = tx.keys_iter("a".."z").unwrap().collect();
		assert_eq!(keys.len(), 2);
		assert_eq!(keys[0].as_ref(), b"a");
		assert_eq!(keys[1].as_ref(), b"c");
		let res = tx.cancel();
		assert!(res.is_ok());
	}

	#[test]
	fn cleanup_trims_commit_queue_when_idle() {
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		for i in 0..10 {
			let mut tx = db.transaction(true);
			tx.set(format!("key{i}"), "value").unwrap();
			tx.commit().unwrap();
		}
		assert_eq!(db.transaction_commit_queue.len(), 10);
		db.run_cleanup();
		// With no registered readers the trim bound falls back to the
		// current commit id: everything below it is unreachable by any
		// future transaction's conflict window, which starts strictly
		// above the commit id the transaction registers at. The exclusive
		// bound leaves exactly the entry at the current commit id.
		assert_eq!(db.transaction_commit_queue.len(), 1);
	}

	#[test]
	fn cleanup_respects_active_reader() {
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		// Commit five transactions before the reader starts
		for i in 0..5 {
			let mut tx = db.transaction(true);
			tx.set(format!("pre{i}"), "value").unwrap();
			tx.commit().unwrap();
		}
		// Register a reader pinned at the current commit id
		let reader = db.transaction(false);
		// Commit five more transactions after the reader registered
		for i in 0..5 {
			let mut tx = db.transaction(true);
			tx.set(format!("post{i}"), "value").unwrap();
			tx.commit().unwrap();
		}
		assert_eq!(db.transaction_commit_queue.len(), 10);
		db.run_cleanup();
		// Entries above the reader's snapshot commit id must survive:
		// they sit inside its potential conflict window. Entries at ids
		// 1-4 are trimmed; the entry at the reader's snapshot (5) and
		// everything above remain.
		assert_eq!(db.transaction_commit_queue.len(), 6);
		drop(reader);
		db.run_cleanup();
		// With the reader gone the idle fallback applies again
		assert_eq!(db.transaction_commit_queue.len(), 1);
	}

	#[test]
	fn logical_clock_is_dense() {
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		for i in 0..10 {
			let mut tx = db.transaction(true);
			tx.set(format!("key{i}"), "value").unwrap();
			tx.commit().unwrap();
		}
		// Merge versions are dense sequential integers: ten commits
		// publish exactly versions 1 through 10.
		assert_eq!(db.oracle.timestamp.load(std::sync::atomic::Ordering::SeqCst), 10);
		let entry = db.datastore.get(b"key0".as_slice()).expect("key0 missing");
		let guard = entry.value().read();
		assert_eq!(guard.as_slice()[0].version, 1);
		let entry = db.datastore.get(b"key9".as_slice()).expect("key9 missing");
		let guard = entry.value().read();
		assert_eq!(guard.as_slice()[0].version, 10);
	}

	#[test]
	fn logical_clock_continues_above_seed() {
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		// Simulate a datastore seeded from files written by a release
		// which minted wall-clock nanosecond versions. Real persistence
		// load seeds all three counters together (see
		// `Persistence::load`); seeding only the clock and leaving
		// `merge_retire_id` at its unseeded default would make the
		// opportunistic retirement walk cross an astronomical gap one
		// step at a time.
		let seed = 1_700_000_000_000_000_000u64;
		db.oracle.alloc.store(seed, std::sync::atomic::Ordering::SeqCst);
		db.oracle.timestamp.store(seed, std::sync::atomic::Ordering::SeqCst);
		db.merge_retire_id.store(seed, std::sync::atomic::Ordering::SeqCst);
		let mut tx = db.transaction(true);
		tx.set("key", "value").unwrap();
		tx.commit().unwrap();
		// The next minted version continues strictly above the seed
		let entry = db.datastore.get(b"key".as_slice()).expect("key missing");
		let guard = entry.value().read();
		assert_eq!(guard.as_slice()[0].version, seed + 1);
		// And the write is visible to a fresh reader
		let mut tx = db.transaction(false);
		assert_eq!(tx.get("key").unwrap().as_deref(), Some(b"value" as &[u8]));
		tx.cancel().unwrap();
	}

	#[test]
	fn slot_lifecycle() {
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		assert_eq!(db.readers.len(), 0);
		let tx = db.transaction(false);
		assert_eq!(db.readers.len(), 1);
		let first_id = *db.readers.front().unwrap().key();
		drop(tx);
		assert_eq!(db.readers.len(), 0);
		// Pool reuse re-pins the retained slot under a fresh id
		let tx = db.transaction(false);
		assert_eq!(db.readers.len(), 1);
		let second_id = *db.readers.front().unwrap().key();
		assert!(second_id > first_id);
		drop(tx);
		assert_eq!(db.readers.len(), 0);
	}

	#[test]
	fn watermark_conservative_while_pinning() {
		use crate::inner::Slot;
		use std::sync::Arc;
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		// Commit some entries which would otherwise be trimmed
		for i in 0..5 {
			let mut tx = db.transaction(true);
			tx.set(format!("key{i}"), "value").unwrap();
			tx.commit().unwrap();
		}
		// Insert a slot stuck in the pinning state, simulating a
		// transaction mid-registration
		db.readers.insert(u64::MAX, Arc::new(Slot::pinning()));
		// Every sweep must treat the watermark as unknown and skip
		assert_eq!(db.compute_cleanup_ts(), None);
		let before = db.transaction_commit_queue.len();
		db.run_cleanup();
		assert_eq!(db.transaction_commit_queue.len(), before);
		// Remove the pinning slot; sweeps proceed again
		db.readers.remove(&u64::MAX);
		assert!(db.compute_cleanup_ts().is_some());
		db.run_cleanup();
		assert_eq!(db.transaction_commit_queue.len(), 1);
	}

	#[test]
	fn commit_watermark_tracks_commits() {
		use std::sync::atomic::Ordering;
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		for i in 0..5 {
			let mut tx = db.transaction(true);
			tx.set(format!("key{i}"), "value").unwrap();
			tx.commit().unwrap();
		}
		// Every commit completed, so the watermark covers all claimed ids
		assert_eq!(db.commit_watermark.load(Ordering::SeqCst), 5);
		assert_eq!(db.transaction_commit_id.load(Ordering::SeqCst), 5);
		// An aborted commit removes its entry, which also counts as
		// complete: the watermark must still cover the aborted id.
		let mut tx1 = db.transaction(true);
		let mut tx2 = db.transaction(true);
		tx1.set("clash", "one").unwrap();
		tx2.set("clash", "two").unwrap();
		tx1.commit().unwrap();
		assert!(tx2.commit().is_err());
		assert_eq!(
			db.commit_watermark.load(Ordering::SeqCst),
			db.transaction_commit_id.load(Ordering::SeqCst)
		);
	}

	#[test]
	fn inline_gc_trims_hot_key_at_commit() {
		// The deterministic memory bound: with no readers pinning older
		// versions, every commit trims the chain it touches down to the
		// latest version — no background worker, no sleeps, and the same
		// behaviour on wasm targets which have no threads at all.
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		for i in 0..100 {
			let mut tx = db.transaction(true);
			tx.set("hotkey", format!("v{i}")).unwrap();
			tx.commit().unwrap();
		}
		let entry = db.datastore.get(b"hotkey".as_slice()).expect("hotkey missing");
		let guard = entry.value().read();
		let chain = guard.as_slice();
		assert_eq!(chain.len(), 1, "inline GC should trim superseded versions at commit");
		assert_eq!(chain[0].value.as_deref(), Some(b"v99" as &[u8]));
	}

	#[test]
	fn inline_gc_unlinks_deleted_key_at_commit() {
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		// Scoped: a shadowed-but-live transaction would keep its slot
		// registered and pin the delete's watermark below the tombstone
		{
			let mut tx = db.transaction(true);
			tx.set("key", "value").unwrap();
			tx.commit().unwrap();
		}
		assert!(db.datastore.get(b"key".as_slice()).is_some());
		// With no readers below the delete, the tombstone collapses the
		// chain at commit time and the node is unlinked immediately.
		{
			let mut tx = db.transaction(true);
			tx.del("key").unwrap();
			tx.commit().unwrap();
		}
		assert!(
			db.datastore.get(b"key".as_slice()).is_none(),
			"a delete with no readers should unlink the node at commit"
		);
	}

	#[test]
	fn safety_net_reclaims_after_reader_departs() {
		// Garbage pinned by a reader on a key that is never written again
		// is invisible to inline commit-time GC: the manual full scan
		// must reclaim it just like the tracked sweep does.
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		{
			let mut tx = db.transaction(true);
			tx.set("key", "v0").unwrap();
			tx.commit().unwrap();
		}
		// Pin the initial version while overwriting the key
		let reader = db.transaction(false);
		for i in 1..=50 {
			let mut tx = db.transaction(true);
			tx.set("key", format!("v{i}")).unwrap();
			tx.commit().unwrap();
		}
		{
			let entry = db.datastore.get(b"key".as_slice()).expect("key missing");
			let guard = entry.value().read();
			assert!(guard.as_slice().len() > 1, "the pinned reader should retain history");
		}
		// Drop the reader; no further commits touch the key, so only the
		// manual (or background) full sweep can reclaim the garbage
		drop(reader);
		db.run_gc();
		let entry = db.datastore.get(b"key".as_slice()).expect("key missing");
		let guard = entry.value().read();
		let chain = guard.as_slice();
		assert_eq!(chain.len(), 1, "the safety-net sweep should reclaim departed-reader garbage");
		assert_eq!(chain[0].value.as_deref(), Some(b"v50" as &[u8]));
	}

	#[test]
	fn tracked_sweep_reclaims_departed_reader_garbage() {
		// The commit path tracks every chain it cannot trim to a single
		// live value, so the targeted sweep must reclaim departed-reader
		// garbage without a full datastore scan.
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		{
			let mut tx = db.transaction(true);
			tx.set("key", "v0").unwrap();
			tx.commit().unwrap();
		}
		// Pin the initial version while overwriting the key
		let reader = db.transaction(false);
		for i in 1..=50 {
			let mut tx = db.transaction(true);
			tx.set("key", format!("v{i}")).unwrap();
			tx.commit().unwrap();
		}
		// The pinned overwrites must have tracked the key
		assert!(
			db.gc_candidates.pin().contains(b"key".as_slice()),
			"a commit that leaves garbage should track its key"
		);
		// While the reader is pinned, the sweep trims what it can and
		// keeps the key tracked for the next pass
		db.run_gc_tracked();
		{
			let entry = db.datastore.get(b"key".as_slice()).expect("key missing");
			assert!(entry.value().read().as_slice().len() > 1);
			assert!(
				db.gc_candidates.pin().contains(b"key".as_slice()),
				"a still-pinned chain should stay tracked after a sweep"
			);
		}
		// Once the reader departs, the tracked sweep finishes the job
		drop(reader);
		db.run_gc_tracked();
		let entry = db.datastore.get(b"key".as_slice()).expect("key missing");
		let guard = entry.value().read();
		let chain = guard.as_slice();
		assert_eq!(chain.len(), 1, "the tracked sweep should reclaim departed-reader garbage");
		assert_eq!(chain[0].value.as_deref(), Some(b"v50" as &[u8]));
		drop(guard);
		assert!(
			!db.gc_candidates.pin().contains(b"key".as_slice()),
			"a terminal chain should be untracked after the sweep"
		);
	}

	#[test]
	fn tracked_sweep_unlinks_pinned_tombstone() {
		// A delete committed while a reader pins the prior value cannot
		// collapse at commit time; the tracked sweep must unlink it after
		// the reader departs.
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		{
			let mut tx = db.transaction(true);
			tx.set("key", "value").unwrap();
			tx.commit().unwrap();
		}
		let reader = db.transaction(false);
		{
			let mut tx = db.transaction(true);
			tx.del("key").unwrap();
			tx.commit().unwrap();
		}
		// The pinned tombstone keeps the node linked and tracked
		assert!(db.datastore.get(b"key".as_slice()).is_some());
		assert!(db.gc_candidates.pin().contains(b"key".as_slice()));
		// After the reader departs the sweep collapses and unlinks it
		drop(reader);
		db.run_gc_tracked();
		assert!(
			db.datastore.get(b"key".as_slice()).is_none(),
			"the tracked sweep should unlink a departed-reader tombstone"
		);
		assert!(!db.gc_candidates.pin().contains(b"key".as_slice()));
	}

	#[test]
	fn full_scan_supersedes_candidate_set() {
		// The manual full scan visits every key, so it clears the
		// candidate set rather than leaving stale entries behind.
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		{
			let mut tx = db.transaction(true);
			tx.set("key", "v0").unwrap();
			tx.commit().unwrap();
		}
		let reader = db.transaction(false);
		{
			let mut tx = db.transaction(true);
			tx.set("key", "v1").unwrap();
			tx.commit().unwrap();
		}
		assert!(db.gc_candidates.pin().contains(b"key".as_slice()));
		drop(reader);
		db.run_gc();
		assert_eq!(db.gc_candidates.pin().len(), 0, "the full scan should clear the candidate set");
		let entry = db.datastore.get(b"key".as_slice()).expect("key missing");
		assert_eq!(entry.value().read().as_slice().len(), 1);
	}

	#[cfg(not(target_arch = "wasm32"))]
	#[test]
	fn load_collapses_aol_replay_chains() {
		// Append-only-log replay pushes one chain entry per record, so a
		// key updated N times holds N versions after replay — garbage no
		// commit or tracked sweep would ever visit on a key that is never
		// written again. The one-shot sweep at load time must collapse
		// every chain to its latest version.
		let temp_dir = tempfile::TempDir::new().unwrap();
		let persistence = crate::PersistenceOptions::new(temp_dir.path())
			.with_aol_mode(crate::AolMode::SynchronousOnCommit)
			.with_snapshot_mode(crate::SnapshotMode::Never)
			.with_fsync_mode(crate::FsyncMode::EveryAppend);
		{
			let db = Database::new_with_persistence(
				crate::DatabaseOptions::default(),
				persistence.clone(),
			)
			.unwrap();
			for i in 0..5 {
				let mut tx = db.transaction(true);
				tx.set("key", format!("v{i}")).unwrap();
				tx.commit().unwrap();
			}
		}
		// Reopen: replay rebuilds the chain, then the load sweep trims it
		let db =
			Database::new_with_persistence(crate::DatabaseOptions::default(), persistence).unwrap();
		let entry = db.datastore.get(b"key".as_slice()).expect("key missing after reload");
		let guard = entry.value().read();
		let chain = guard.as_slice();
		assert_eq!(chain.len(), 1, "the load-time sweep should collapse replayed chains");
		assert_eq!(chain[0].value.as_deref(), Some(b"v4" as &[u8]));
	}

	#[test]
	fn full_scan_retracks_pinned_chains() {
		// A manual full scan clears the candidate set, so any chain it
		// cannot trim to a terminal state must be re-tracked — otherwise
		// its garbage would be stranded once the pinning reader departs,
		// since the background sweep never scans the full datastore.
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		{
			let mut tx = db.transaction(true);
			tx.set("key", "v0").unwrap();
			tx.commit().unwrap();
		}
		let reader = db.transaction(false);
		{
			let mut tx = db.transaction(true);
			tx.set("key", "v1").unwrap();
			tx.commit().unwrap();
		}
		// The full scan runs while the reader still pins the old version
		db.run_gc();
		assert!(
			db.gc_candidates.pin().contains(b"key".as_slice()),
			"the full scan should re-track a chain it could not trim"
		);
		// After the reader departs the tracked sweep reclaims the garbage
		drop(reader);
		db.run_gc_tracked();
		let entry = db.datastore.get(b"key".as_slice()).expect("key missing");
		assert_eq!(entry.value().read().as_slice().len(), 1);
	}

	#[test]
	fn pin_then_read_stress() {
		use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
		use std::sync::Arc;
		use std::thread;
		use std::time::{Duration, Instant};
		// Workers are disabled; a dedicated thread hammers the manual
		// sweep entry points instead, maximising sweep frequency against
		// the pin-then-read registration protocol.
		let db = Arc::new(Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		));
		{
			let mut tx = db.transaction(true);
			tx.set("key", "v0").unwrap();
			tx.commit().unwrap();
		}
		let stop = Arc::new(AtomicBool::new(false));
		let none_reads = Arc::new(AtomicUsize::new(0));
		let total_reads = Arc::new(AtomicUsize::new(0));
		// Single writer thread keeps overwriting the same key
		let writer = {
			let db = Arc::clone(&db);
			let stop = Arc::clone(&stop);
			thread::spawn(move || {
				let mut counter: u64 = 0;
				while !stop.load(Ordering::Relaxed) {
					let mut tx = db.transaction(true);
					tx.set("key", format!("v{counter}")).unwrap();
					tx.commit().unwrap();
					counter = counter.wrapping_add(1);
				}
			})
		};
		// Sweeper thread hammers cleanup and both gc paths continuously
		let sweeper = {
			let db = Arc::clone(&db);
			let stop = Arc::clone(&stop);
			thread::spawn(move || {
				while !stop.load(Ordering::Relaxed) {
					db.run_cleanup();
					db.run_gc();
					db.run_gc_tracked();
				}
			})
		};
		// Reader threads open many short-lived snapshots
		let mut readers = Vec::new();
		for _ in 0..6 {
			let db = Arc::clone(&db);
			let stop = Arc::clone(&stop);
			let none_reads = Arc::clone(&none_reads);
			let total_reads = Arc::clone(&total_reads);
			readers.push(thread::spawn(move || {
				while !stop.load(Ordering::Relaxed) {
					let mut tx = db.transaction(false);
					let value = tx.get("key").unwrap();
					total_reads.fetch_add(1, Ordering::Relaxed);
					if value.is_none() {
						none_reads.fetch_add(1, Ordering::Relaxed);
					}
					tx.cancel().unwrap();
				}
			}));
		}
		let started = Instant::now();
		while started.elapsed() < Duration::from_millis(500) {
			thread::sleep(Duration::from_millis(10));
		}
		stop.store(true, Ordering::Relaxed);
		writer.join().unwrap();
		sweeper.join().unwrap();
		for r in readers {
			r.join().unwrap();
		}
		let nones = none_reads.load(Ordering::Relaxed);
		let total = total_reads.load(Ordering::Relaxed);
		assert_eq!(
			nones, 0,
			"reader observed `None` for a key that always has a committed \
			 value ({nones} of {total} reads): the pin-then-read protocol \
			 failed to protect a registering reader from a sweep"
		);
	}
}
