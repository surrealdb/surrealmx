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

//! Persistence edge case tests for SurrealMX.
//!
//! Tests recovery scenarios, edge cases, and persistence behavior.

use bytes::Bytes;
use std::time::Duration;
use surrealmx::{AolMode, Database, DatabaseOptions, FsyncMode, PersistenceOptions, SnapshotMode};
use tempfile::TempDir;

// =============================================================================
// Recovery Edge Cases
// =============================================================================

#[test]
fn recovery_with_empty_aol() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	// Create database but don't add any data
	{
		let _db =
			Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();
		// No operations - AOL file might be empty or not exist
	}

	// Create new database from same path
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		// Should work with empty/no AOL
		let mut tx = db.transaction(false);
		let keys = tx.keys("".."z", None, None).unwrap();
		assert!(keys.is_empty(), "Should recover empty database");
		tx.cancel().unwrap();
	}
}

#[test]
fn recovery_snapshot_only_no_aol() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never);

	// Create data and snapshot
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);
		tx.set("key1", "value1").unwrap();
		tx.set("key2", "value2").unwrap();
		tx.commit().unwrap();

		if let Some(persistence) = db.persistence() {
			persistence.snapshot().unwrap();
		}
	}

	// Recovery from snapshot only (no AOL)
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);
		assert_eq!(tx.get("key1").unwrap(), Some(Bytes::from("value1")));
		assert_eq!(tx.get("key2").unwrap(), Some(Bytes::from("value2")));
		tx.cancel().unwrap();
	}
}

#[test]
fn recovery_aol_only_no_snapshot() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_fsync_mode(FsyncMode::EveryAppend);

	// Create data (no snapshot)
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);
		tx.set("key1", "value1").unwrap();
		tx.commit().unwrap();

		let mut tx = db.transaction(true);
		tx.set("key2", "value2").unwrap();
		tx.commit().unwrap();

		// No snapshot created
	}

	// Recovery from AOL only
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);
		assert_eq!(tx.get("key1").unwrap(), Some(Bytes::from("value1")));
		assert_eq!(tx.get("key2").unwrap(), Some(Bytes::from("value2")));
		tx.cancel().unwrap();
	}
}

#[test]
fn recovery_combined_snapshot_and_aol() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_fsync_mode(FsyncMode::EveryAppend);

	// Create data, snapshot, then more data
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		// Data before snapshot
		let mut tx = db.transaction(true);
		tx.set("before_snap", "value1").unwrap();
		tx.commit().unwrap();

		// Create snapshot
		if let Some(persistence) = db.persistence() {
			persistence.snapshot().unwrap();
		}

		// Data after snapshot (goes to AOL)
		let mut tx = db.transaction(true);
		tx.set("after_snap", "value2").unwrap();
		tx.commit().unwrap();
	}

	// Recovery should include both
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);
		assert_eq!(tx.get("before_snap").unwrap(), Some(Bytes::from("value1")));
		assert_eq!(tx.get("after_snap").unwrap(), Some(Bytes::from("value2")));
		tx.cancel().unwrap();
	}
}

// =============================================================================
// Snapshot During Operations Tests
// =============================================================================

#[test]
fn snapshot_during_read_transaction() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never);

	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Add data
	let mut tx = db.transaction(true);
	tx.set("key", "value").unwrap();
	tx.commit().unwrap();

	// Start read transaction
	let read_tx = db.transaction(false);

	// Create snapshot while read is active
	if let Some(persistence) = db.persistence() {
		let result = persistence.snapshot();
		assert!(result.is_ok(), "Snapshot during read should succeed");
	}

	// Read should still see data
	assert_eq!(read_tx.get("key").unwrap(), Some(Bytes::from("value")));
}

#[test]
fn snapshot_preserves_delete_state() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never);

	// Create and delete data, then snapshot
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);
		tx.set("keep", "keeper").unwrap();
		tx.set("delete_me", "deleted").unwrap();
		tx.commit().unwrap();

		let mut tx = db.transaction(true);
		tx.del("delete_me").unwrap();
		tx.commit().unwrap();

		if let Some(persistence) = db.persistence() {
			persistence.snapshot().unwrap();
		}
	}

	// Recovery should not have deleted key
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);
		assert_eq!(tx.get("keep").unwrap(), Some(Bytes::from("keeper")));
		assert!(tx.get("delete_me").unwrap().is_none(), "Deleted key should stay deleted");
		tx.cancel().unwrap();
	}
}

// =============================================================================
// Interval Snapshot Tests
// =============================================================================

#[test]
fn interval_snapshot_triggers() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Interval(Duration::from_millis(100)));

	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Add data
	let mut tx = db.transaction(true);
	tx.set("key", "value").unwrap();
	tx.commit().unwrap();

	// Wait for interval snapshot
	std::thread::sleep(Duration::from_millis(300));

	// Snapshot should exist
	let snapshot_path = temp_path.join("snapshot.bin");
	assert!(snapshot_path.exists(), "Interval snapshot should be created");
}

// =============================================================================
// Async AOL Tests
// =============================================================================

#[test]
fn async_aol_eventual_persistence() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::AsynchronousAfterCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	// Write data with async AOL
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);
		tx.set("async_key", "async_value").unwrap();
		tx.commit().unwrap();

		// Wait for async write
		std::thread::sleep(Duration::from_millis(200));
	}

	// Verify AOL file has content
	let aol_path = temp_path.join("aol.bin");
	assert!(aol_path.exists(), "AOL file should exist");

	let aol_size = std::fs::metadata(&aol_path).unwrap().len();
	assert!(aol_size > 0, "AOL should have content");

	// Recovery should work
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);
		assert_eq!(tx.get("async_key").unwrap(), Some(Bytes::from("async_value")));
		tx.cancel().unwrap();
	}
}

// =============================================================================
// Fsync Mode Tests
// =============================================================================

#[test]
fn fsync_interval_mode() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_fsync_mode(FsyncMode::Interval(Duration::from_millis(100)));

	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Write multiple transactions
	for i in 0..5 {
		let mut tx = db.transaction(true);
		tx.set(format!("key_{}", i), format!("value_{}", i)).unwrap();
		tx.commit().unwrap();
	}

	// Wait for fsync interval
	std::thread::sleep(Duration::from_millis(200));

	// Data should be persisted
	let aol_path = temp_path.join("aol.bin");
	assert!(aol_path.exists());
}

#[test]
fn fsync_never_mode() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_fsync_mode(FsyncMode::Never);

	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);
		tx.set("key", "value").unwrap();
		tx.commit().unwrap();
	}

	// Should still work for recovery (OS buffers)
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);
		assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("value")));
		tx.cancel().unwrap();
	}
}

// =============================================================================
// Custom Path Tests
// =============================================================================

#[test]
fn custom_aol_and_snapshot_paths() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	// Create custom directories
	let custom_aol_dir = temp_path.join("custom_logs");
	let custom_snap_dir = temp_path.join("custom_snaps");
	std::fs::create_dir_all(&custom_aol_dir).unwrap();
	std::fs::create_dir_all(&custom_snap_dir).unwrap();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_aol_path(custom_aol_dir.join("my.aol"))
		.with_snapshot_path(custom_snap_dir.join("my.snap"));

	// Create data
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);
		tx.set("custom_key", "custom_value").unwrap();
		tx.commit().unwrap();

		if let Some(persistence) = db.persistence() {
			persistence.snapshot().unwrap();
		}
	}

	// Verify custom paths used
	assert!(custom_aol_dir.join("my.aol").exists(), "Custom AOL path should be used");
	assert!(custom_snap_dir.join("my.snap").exists(), "Custom snapshot path should be used");

	// Verify recovery works from custom paths
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);
		assert_eq!(tx.get("custom_key").unwrap(), Some(Bytes::from("custom_value")));
		tx.cancel().unwrap();
	}
}

// =============================================================================
// Multiple Restart Tests
// =============================================================================

#[test]
fn multiple_restarts_accumulate_data() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_fsync_mode(FsyncMode::EveryAppend);

	// First session
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);
		tx.set("key1", "value1").unwrap();
		tx.commit().unwrap();
	}

	// Second session
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		// Should see key1
		let tx = db.transaction(false);
		assert_eq!(tx.get("key1").unwrap(), Some(Bytes::from("value1")));

		// Add key2
		let mut tx = db.transaction(true);
		tx.set("key2", "value2").unwrap();
		tx.commit().unwrap();
	}

	// Third session
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		// Should see both keys
		let tx = db.transaction(false);
		assert_eq!(tx.get("key1").unwrap(), Some(Bytes::from("value1")));
		assert_eq!(tx.get("key2").unwrap(), Some(Bytes::from("value2")));

		// Add key3
		let mut tx = db.transaction(true);
		tx.set("key3", "value3").unwrap();
		tx.commit().unwrap();
	}

	// Final verification
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);
		assert_eq!(tx.get("key1").unwrap(), Some(Bytes::from("value1")));
		assert_eq!(tx.get("key2").unwrap(), Some(Bytes::from("value2")));
		assert_eq!(tx.get("key3").unwrap(), Some(Bytes::from("value3")));
		tx.cancel().unwrap();
	}
}

#[test]
fn snapshot_truncates_aol() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_fsync_mode(FsyncMode::EveryAppend);

	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Add lots of data to AOL
	for i in 0..100 {
		let mut tx = db.transaction(true);
		tx.set(format!("key_{:04}", i), format!("value_{}", i)).unwrap();
		tx.commit().unwrap();
	}

	let aol_path = temp_path.join("aol.bin");
	let aol_size_before = std::fs::metadata(&aol_path).unwrap().len();

	// Create snapshot (should truncate AOL)
	if let Some(persistence) = db.persistence() {
		persistence.snapshot().unwrap();
	}

	let aol_size_after = std::fs::metadata(&aol_path).unwrap().len();

	assert!(
		aol_size_after < aol_size_before,
		"AOL should be truncated after snapshot ({} < {})",
		aol_size_after,
		aol_size_before
	);
}
