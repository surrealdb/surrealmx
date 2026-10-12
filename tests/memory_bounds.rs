#![cfg(not(target_arch = "wasm32"))]
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

//! Memory bounds, transaction age limit, and database stats tests for
//! `SurrealMX`.

use std::thread;
use std::time::Duration;
use surrealmx::{AolMode, Database, DatabaseOptions, Error, PersistenceOptions, SnapshotMode};
use tempfile::TempDir;

#[test]
fn test_memory_limit_enforcement() {
	// Set an explicit memory limit of 1024 bytes
	let opts = DatabaseOptions::default().with_memory_limit(1024);
	let db = Database::new_with_options(opts);

	// Small writes that easily fit under 1024 bytes
	{
		let mut tx = db.transaction(true);
		tx.set("k1", "small_val").unwrap();
		tx.commit().unwrap();
	}

	// Large write that exceeds 1024 bytes
	{
		let mut tx = db.transaction(true);
		let large_data = vec![b'x'; 2048];
		tx.set("large_key", large_data).unwrap();
		match tx.commit() {
			Err(Error::MemoryLimit) => {}
			other => panic!("Expected Error::MemoryLimit, got: {other:?}"),
		}
	}

	// Verify that the database is still functional and k1 is preserved
	let tx = db.transaction(false);
	assert_eq!(tx.get("k1").unwrap(), Some("small_val".into()));
	assert_eq!(tx.get("large_key").unwrap(), None);
}

#[test]
fn test_transaction_max_age_timeout() {
	let opts = DatabaseOptions::default().with_max_transaction_age(Duration::from_millis(50));
	let db = Database::new_with_options(opts);

	let mut tx = db.transaction(true);
	tx.set("key", "val1").unwrap();

	// Wait past the transaction expiration threshold
	thread::sleep(Duration::from_millis(100));

	// Any subsequent operation on the transaction should fail with TxTooOld
	match tx.set("key2", "val2") {
		Err(Error::TxTooOld) => {}
		other => panic!("Expected TxTooOld on set, got {other:?}"),
	}

	match tx.commit() {
		Err(Error::TxTooOld) => {}
		other => panic!("Expected TxTooOld on commit, got {other:?}"),
	}

	// A fresh transaction succeeds normally
	let mut fresh_tx = db.transaction(true);
	fresh_tx.set("fresh_key", "fresh_val").unwrap();
	fresh_tx.commit().unwrap();

	let read_tx = db.transaction(false);
	assert_eq!(read_tx.get("fresh_key").unwrap(), Some("fresh_val".into()));
	assert_eq!(read_tx.get("key").unwrap(), None);
}

#[test]
fn test_database_stats_tracking() {
	let db = Database::new();

	let stats0 = db.stats();
	assert_eq!(stats0.live_keys, 0);
	assert_eq!(stats0.live_versions, 0);
	assert_eq!(stats0.active_readers, 0);
	assert!(!stats0.is_poisoned);

	// Start a read transaction and verify active_readers incremented
	let read_tx = db.transaction(false);
	let stats1 = db.stats();
	assert_eq!(stats1.active_readers, 1);
	assert!(stats1.oldest_pinned_version.is_some());

	// Write some keys
	{
		let mut tx = db.transaction(true);
		tx.set("k1", "v1").unwrap();
		tx.set("k2", "v2").unwrap();
		tx.commit().unwrap();
	}

	let stats2 = db.stats();
	assert_eq!(stats2.live_keys, 2);
	assert!(stats2.live_versions >= 2);
	assert!(stats2.approx_data_bytes > 0);

	// Drop the reader and verify active_readers count updates
	drop(read_tx);
	let stats3 = db.stats();
	assert_eq!(stats3.active_readers, 0);
}

#[test]
fn test_async_queue_backpressure_options() {
	let dir = TempDir::new().unwrap();
	// Set bytes limit too small to fit a batch
	let popts = PersistenceOptions::new(dir.path())
		.with_aol_mode(AolMode::AsynchronousAfterCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_max_async_queue_depth(10)
		.with_max_async_queue_bytes(10);

	let db = Database::new_with_persistence(DatabaseOptions::default(), popts).unwrap();

	let mut tx = db.transaction(true);
	tx.set("k", "v").unwrap();
	match tx.commit() {
		Err(Error::AsyncQueueFull) => {}
		other => panic!("Expected AsyncQueueFull error, got: {other:?}"),
	}

	let stats = db.stats();
	assert_eq!(stats.async_dropped_batches, 1);
	assert!(!stats.is_poisoned);

	// With sufficient capacity, commit succeeds
	let dir2 = TempDir::new().unwrap();
	let popts2 = PersistenceOptions::new(dir2.path())
		.with_aol_mode(AolMode::AsynchronousAfterCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_max_async_queue_depth(100)
		.with_max_async_queue_bytes(1024);

	let db2 = Database::new_with_persistence(DatabaseOptions::default(), popts2).unwrap();
	let mut tx2 = db2.transaction(true);
	tx2.set("k", "v").unwrap();
	assert!(tx2.commit().is_ok());
}

#[test]
fn test_database_poison_state() {
	let db = Database::new();
	assert!(!db.is_poisoned());
	assert!(db.check_poisoned().is_ok());

	// Direct point operations work
	db.set("test_key", "test_val").unwrap();
	assert_eq!(db.get("test_key").unwrap(), Some("test_val".into()));
}
