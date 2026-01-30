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
use crate::options::{DatabaseOptions, DEFAULT_CLEANUP_INTERVAL, DEFAULT_GC_INTERVAL};
use crate::persistence::Persistence;
use crate::pool::Pool;
use crate::pool::DEFAULT_POOL_SIZE;
use crate::tx::Transaction;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
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
	persistence: Option<Persistence>,
	/// Interval used by the garbage collector thread
	gc_interval: Duration,
	/// Interval used by the cleanup thread
	cleanup_interval: Duration,
}

impl Default for Database {
	fn default() -> Self {
		let inner = Arc::new(Inner::default());
		let pool = Pool::new(inner.clone(), DEFAULT_POOL_SIZE);
		Database {
			inner,
			pool,
			persistence: None,
			gc_interval: DEFAULT_GC_INTERVAL,
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
		let pool = Pool::new(inner.clone(), opts.pool_size);
		// Create the database
		let db = Database {
			inner,
			pool,
			persistence: None,
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
		db
	}

	/// Create a new persistent database with custom options and persistence settings
	pub fn new_with_persistence(
		opts: DatabaseOptions,
		persistence_opts: crate::PersistenceOptions,
	) -> std::io::Result<Self> {
		//  Create a new inner database
		let inner = Arc::new(Inner::new(&opts));
		// Initialise a transaction pool
		let pool = Pool::new(inner.clone(), opts.pool_size);
		// Create a new persistence layer with options
		let persist = Persistence::new_with_options(persistence_opts, inner.clone())
			.map_err(std::io::Error::other)?;
		// Replace the persistence layer in the database
		inner.persistence.write().replace(Arc::new(persist.clone()));
		// Create the database
		let db = Database {
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

	/// Configure the database to use immediate garbage collection.
	///
	/// In this mode, old MVCC transaction entries are cleaned up
	/// during transaction commits as soon as they are no longer
	/// needed by any active read transactions. This ensures minimal
	/// memory usage while maintaining correctness for concurrent
	/// transactions. Additionally, if [`DatabaseOptions::enable_gc`]
	/// is set, a background thread will periodically clean up any
	/// stale versions.
	pub fn with_gc(self) -> Self {
		// Store the garbage collection epoch
		*self.garbage_collection_epoch.write() = None;
		// Return the database
		self
	}

	/// Configure the database to preserve versions for a specified duration.
	///
	/// In this mode, MVCC transaction entries are retained for at least
	/// the specified duration, allowing point-in-time reads within that
	/// window. Old versions are cleaned up during transaction commits
	/// once they exceed the history duration and are no longer needed
	/// by active transactions. Additionally, if [`DatabaseOptions::enable_gc`]
	/// is set, a background thread will periodically clean up stale
	/// versions across the entire datastore.
	pub fn with_gc_history(self, history: Duration) -> Self {
		// Store the garbage collection epoch
		*self.garbage_collection_epoch.write() = Some(history);
		// Return the database
		self
	}

	/// Start a new transaction on this database
	pub fn transaction(&self, write: bool) -> Transaction {
		self.pool.get(write)
	}

	/// Get a reference to the persistence layer if enabled
	pub fn persistence(&self) -> Option<&Persistence> {
		self.persistence.as_ref()
	}

	/// Manually perform transaction queue cleanup.
	///
	/// This should be called when automatic cleanup is disabled via
	/// [`DatabaseOptions::enable_cleanup`].
	pub fn run_cleanup(&self) {
		// Get the oldest commit entry which is still active
		if let Some(entry) = self.counter_by_commit.front() {
			// Get the oldest commit version
			let oldest = entry.key();
			// Remove commits up to this commit queue id from the transaction queue
			self.transaction_commit_queue.range(..oldest).for_each(|e| {
				e.remove();
			});
		}
	}

	/// Manually perform garbage collection of stale record versions.
	///
	/// This function performs a full datastore scan to clean up old versions
	/// across all keys. Note that inline garbage collection happens automatically
	/// during transaction commits, but only for keys being modified. This function
	/// is useful for cleaning up stale versions on keys that haven't been recently
	/// modified, or when automatic background GC is disabled via
	/// [`DatabaseOptions::enable_gc`].
	pub fn run_gc(&self) {
		// Get the current time in nanoseconds
		let now = self.oracle.current_time_ns();
		// Get the garbage collection epoch as nanoseconds
		let history = self.garbage_collection_epoch.read().unwrap_or_default().as_nanos();
		// Calculate the history cutoff (current time - history duration)
		let history_cutoff = now.saturating_sub(history as u64);
		// Get the earliest active transaction version
		let earliest_tx = self.counter_by_oracle.front().map(|e| *e.key()).unwrap_or(now);
		// Use the earlier of history cutoff or earliest transaction to protect active transactions
		let cleanup_ts = history_cutoff.min(earliest_tx);
		// Iterate over the entire tree
		for entry in self.datastore.iter() {
			// Fetch the entry value
			let versions = entry.value();
			// Modify the version entries
			let mut versions = versions.write();
			// Clean up unnecessary older versions
			if versions.gc_older_versions(cleanup_ts) == 0 {
				// Drop the version reference
				drop(versions);
				// Remove the entry from the datastore
				self.datastore.remove(entry.key());
			}
		}
	}

	/// Shutdown the datastore, waiting for background threads to exit
	fn shutdown(&self) {
		// First, disable Persistence background workers if present
		if let Some(ref persistence) = self.persistence {
			// Disable the persistence background workers
			persistence.background_threads_enabled.store(false, Ordering::Release);
			// Wait for persistence threads to exit
			if let Some(handle) = persistence.fsync_handle.write().take() {
				handle.thread().unpark();
				let _ = handle.join();
			}
			if let Some(handle) = persistence.snapshot_handle.write().take() {
				handle.thread().unpark();
				let _ = handle.join();
			}
			if let Some(handle) = persistence.appender_handle.write().take() {
				handle.thread().unpark();
				let _ = handle.join();
			}
		}
		// Then disable Database background workers
		self.background_threads_enabled.store(false, Ordering::Relaxed);
		// Wait for the transaction cleanup thread to exit
		if let Some(handle) = self.transaction_cleanup_handle.write().take() {
			handle.thread().unpark();
			let _ = handle.join();
		}
		// Wait for the garbage collector thread to exit
		if let Some(handle) = self.garbage_collection_handle.write().take() {
			handle.thread().unpark();
			let _ = handle.join();
		}
	}

	/// Start the transaction commit queue cleanup thread after creating the database
	fn initialise_cleanup_worker(&self) {
		// Clone the underlying datastore inner
		let db = self.inner.clone();
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
					// Get the oldest commit entry which is still active
					if let Some(entry) = db.counter_by_commit.front() {
						// Get the oldest commit version
						let oldest = entry.key();
						// Remove the commits up to this commit queue id from the transaction queue
						db.transaction_commit_queue.range(..oldest).for_each(|e| {
							e.remove();
						});
					}
				}
			});
			// Store and track the thread handle
			*self.inner.transaction_cleanup_handle.write() = Some(handle);
		}
	}

	/// Start the garbage collection thread after creating the database
	fn initialise_garbage_worker(&self) {
		// Clone the underlying datastore inner
		let db = self.inner.clone();
		// Check if a background thread is already running
		if db.garbage_collection_handle.read().is_none() {
			// Get the specified interval
			let interval = self.gc_interval;
			// Spawn a new thread to handle periodic garbage collection
			let handle = std::thread::spawn(move || {
				// Check whether the garbage collection process is enabled
				while db.background_threads_enabled.load(Ordering::Relaxed) {
					// Wait for a specified time interval
					std::thread::park_timeout(interval);
					// Check shutdown flag again after waking
					if !db.background_threads_enabled.load(Ordering::Relaxed) {
						break;
					}
					// Get the current time in nanoseconds
					let now = db.oracle.current_time_ns();
					// Get the garbage collection epoch as nanoseconds
					let history = db.garbage_collection_epoch.read().unwrap_or_default().as_nanos();
					// Calculate the history cutoff (current time - history duration)
					let history_cutoff = now.saturating_sub(history as u64);
					// Get the earliest active transaction version
					let earliest_tx = db.counter_by_oracle.front().map(|e| *e.key()).unwrap_or(now);
					// Use the earlier of history cutoff or earliest transaction to protect active transactions
					let cleanup_ts = history_cutoff.min(earliest_tx);
					// Iterate over the entire tree
					for entry in db.datastore.iter() {
						// Fetch the entry value
						let versions = entry.value();
						// Modify the version entries
						let mut versions = versions.write();
						// Clean up unnecessary older versions
						if versions.gc_older_versions(cleanup_ts) == 0 {
							// Drop the version reference
							drop(versions);
							// Remove the entry from the datastore
							db.datastore.remove(entry.key());
						}
					}
				}
			});
			// Store and track the thread handle
			*self.inner.garbage_collection_handle.write() = Some(handle);
		}
	}
}

#[cfg(test)]
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
}
