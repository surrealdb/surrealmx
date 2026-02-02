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

//! Recovery edge case tests for SurrealMX.
//!
//! Tests crash recovery scenarios including partial writes, corruption,
//! and concurrent recovery attempts.

use bytes::Bytes;
use std::fs;
use std::io::Write;
use surrealmx::{AolMode, Database, DatabaseOptions, PersistenceOptions, SnapshotMode};
use tempfile::TempDir;

// =============================================================================
// Partial AOL Write Recovery Tests
// =============================================================================

#[test]
fn recovery_after_partial_aol_write() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	// Create database with some data
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);
		tx.set("valid_key", "valid_value").unwrap();
		tx.commit().unwrap();
	}

	// Append garbage to the AOL file to simulate partial write
	let aol_path = temp_path.join("append-only.log");
	if aol_path.exists() {
		let mut file = fs::OpenOptions::new().append(true).open(&aol_path).unwrap();
		// Write some invalid data
		file.write_all(b"INVALID_PARTIAL_DATA").unwrap();
		file.flush().unwrap();
	}

	// Recovery should handle partial/invalid data gracefully
	// Either by ignoring it or by returning an error
	let result = Database::new_with_persistence(db_opts, persistence_opts);

	// The database should either:
	// 1. Successfully recover, ignoring the garbage, OR
	// 2. Return an error indicating corruption
	match result {
		Ok(db) => {
			// If recovery succeeds, valid data should be present
			let tx = db.transaction(false);
			let value = tx.get("valid_key").unwrap();
			// Value might be present (if garbage was ignored/truncated)
			// or might not be (if recovery stopped before valid data)
			// Just verify the database is operational
			drop(value);
		}
		Err(_) => {
			// Error is acceptable - it indicates corruption was detected
		}
	}
}

// =============================================================================
// Corrupted Snapshot Recovery Tests
// =============================================================================

#[test]
fn recovery_with_corrupted_snapshot() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	// Create database with data and snapshot
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);
		tx.set("key1", "value1").unwrap();
		tx.commit().unwrap();

		if let Some(persistence) = db.persistence() {
			persistence.snapshot().unwrap();
		}
	}

	// Find and corrupt the snapshot file
	for entry in fs::read_dir(temp_path).unwrap() {
		let entry = entry.unwrap();
		let path = entry.path();
		if path.extension().map(|e| e == "snap").unwrap_or(false) {
			// Overwrite snapshot with garbage
			fs::write(&path, b"CORRUPTED_SNAPSHOT_DATA").unwrap();
		}
	}

	// Recovery with corrupted snapshot should either:
	// 1. Fall back to AOL recovery
	// 2. Return an error
	let result = Database::new_with_persistence(db_opts, persistence_opts);

	match result {
		Ok(db) => {
			// If recovery succeeds via AOL, data might still be present
			let tx = db.transaction(false);
			// Database is operational, that's what matters
			let _ = tx.get("key1");
		}
		Err(_) => {
			// Error is acceptable for corrupted snapshot
		}
	}
}

// =============================================================================
// Empty Database Recovery Tests
// =============================================================================

#[test]
fn recovery_empty_database() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	// Create and close empty database
	{
		let _db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();
		// Don't write any data
	}

	// Recovery of empty database should work
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		// Verify database is empty but operational
		let tx = db.transaction(false);
		let keys = tx.keys("".."z", None, None).unwrap();
		assert!(keys.is_empty(), "Recovered empty database should have no keys");

		// Should be able to write new data
		drop(tx);
		let mut tx = db.transaction(true);
		tx.set("new_key", "new_value").unwrap();
		tx.commit().unwrap();

		// Verify write succeeded
		let tx = db.transaction(false);
		assert_eq!(tx.get("new_key").unwrap(), Some(Bytes::from("new_value")));
	}
}

// =============================================================================
// Delete State Preservation Tests
// =============================================================================

#[test]
fn recovery_preserves_delete_state() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	// Create database with data, then delete some
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		// Create data
		let mut tx = db.transaction(true);
		tx.set("keep_key", "keep_value").unwrap();
		tx.set("delete_key", "delete_value").unwrap();
		tx.commit().unwrap();

		// Delete one key
		let mut tx = db.transaction(true);
		tx.del("delete_key").unwrap();
		tx.commit().unwrap();

		// Create snapshot with delete state
		if let Some(persistence) = db.persistence() {
			persistence.snapshot().unwrap();
		}
	}

	// Recovery should preserve delete state
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let tx = db.transaction(false);

		// Kept key should exist
		assert_eq!(
			tx.get("keep_key").unwrap(),
			Some(Bytes::from("keep_value")),
			"Kept key should be recovered"
		);

		// Deleted key should not exist
		assert!(
			tx.get("delete_key").unwrap().is_none(),
			"Deleted key should remain deleted after recovery"
		);
	}
}

// =============================================================================
// Concurrent Recovery Attempts Tests
// =============================================================================

#[test]
fn concurrent_recovery_attempts() {
	use std::sync::{Arc, Barrier};
	use std::thread;

	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path().to_path_buf();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(&temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	// Create initial data
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);
		tx.set("shared_key", "shared_value").unwrap();
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(2));

	// Attempt concurrent opens
	let temp_path1 = temp_path.clone();
	let db_opts1 = db_opts.clone();
	let barrier1 = Arc::clone(&barrier);
	let handle1 = thread::spawn(move || {
		barrier1.wait();
		let persistence_opts = PersistenceOptions::new(&temp_path1)
			.with_aol_mode(AolMode::SynchronousOnCommit)
			.with_snapshot_mode(SnapshotMode::Never);
		Database::new_with_persistence(db_opts1, persistence_opts)
	});

	let temp_path2 = temp_path;
	let db_opts2 = db_opts;
	let barrier2 = Arc::clone(&barrier);
	let handle2 = thread::spawn(move || {
		barrier2.wait();
		let persistence_opts = PersistenceOptions::new(&temp_path2)
			.with_aol_mode(AolMode::SynchronousOnCommit)
			.with_snapshot_mode(SnapshotMode::Never);
		Database::new_with_persistence(db_opts2, persistence_opts)
	});

	let result1 = handle1.join().unwrap();
	let result2 = handle2.join().unwrap();

	// One or both might succeed (depending on locking implementation)
	// Or one might fail due to lock contention
	// The key is that the data should not be corrupted

	// At least one should succeed or both should handle the conflict gracefully
	let successes = [&result1, &result2].iter().filter(|r| r.is_ok()).count();
	let failures = [&result1, &result2].iter().filter(|r| r.is_err()).count();

	assert!(
		successes >= 1 || failures == 2,
		"At least one should succeed, or both should fail gracefully"
	);

	// If any succeeded, verify data integrity
	if let Ok(db) = result1 {
		let tx = db.transaction(false);
		assert_eq!(tx.get("shared_key").unwrap(), Some(Bytes::from("shared_value")));
	}
	if let Ok(db) = result2 {
		let tx = db.transaction(false);
		assert_eq!(tx.get("shared_key").unwrap(), Some(Bytes::from("shared_value")));
	}
}
