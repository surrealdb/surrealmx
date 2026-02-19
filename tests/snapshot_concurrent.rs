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

//! Concurrent snapshot operation tests for SurrealMX.
//!
//! Tests snapshot behavior under concurrent access including writes during
//! snapshot, multiple concurrent snapshots, and reads during snapshot.

use bytes::Bytes;
use std::sync::{Arc, Barrier};
use std::thread;
use surrealmx::{AolMode, Database, DatabaseOptions, PersistenceOptions, SnapshotMode};
use tempfile::TempDir;

// =============================================================================
// Snapshot During Writes Tests
// =============================================================================

#[test]
fn snapshot_during_writes() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	let db = Arc::new(Database::new_with_persistence(db_opts, persistence_opts).unwrap());

	// Create some initial data
	{
		let mut tx = db.transaction(true);
		for i in 0..50 {
			tx.set(format!("initial_{:03}", i), format!("value_{}", i)).unwrap();
		}
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(2));

	// Thread 1: Perform snapshot
	let db1 = Arc::clone(&db);
	let barrier1 = Arc::clone(&barrier);
	let snapshot_handle = thread::spawn(move || {
		barrier1.wait();
		if let Some(persistence) = db1.persistence() {
			persistence.snapshot()
		} else {
			Ok(())
		}
	});

	// Thread 2: Perform writes during snapshot
	let db2 = Arc::clone(&db);
	let barrier2 = Arc::clone(&barrier);
	let write_handle = thread::spawn(move || {
		barrier2.wait();
		for i in 0..50 {
			let mut tx = db2.transaction(true);
			tx.set(format!("concurrent_{:03}", i), format!("value_{}", i)).unwrap();
			tx.commit().unwrap();
		}
	});

	// Wait for both to complete
	let snapshot_result = snapshot_handle.join().unwrap();
	write_handle.join().unwrap();

	// Snapshot should succeed
	assert!(snapshot_result.is_ok(), "Snapshot during writes should succeed");

	// All data should be present
	let tx = db.transaction(false);
	let initial_count = tx.scan("initial_".."initial_z", None, None).unwrap().len();
	let concurrent_count = tx.scan("concurrent_".."concurrent_z", None, None).unwrap().len();

	assert_eq!(initial_count, 50, "All initial keys should exist");
	assert_eq!(concurrent_count, 50, "All concurrent keys should exist");
}

// =============================================================================
// Multiple Concurrent Snapshots Tests
// =============================================================================

#[test]
fn multiple_concurrent_snapshots() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	let db = Arc::new(Database::new_with_persistence(db_opts, persistence_opts).unwrap());

	// Create data
	{
		let mut tx = db.transaction(true);
		for i in 0..100 {
			tx.set(format!("key_{:04}", i), format!("value_{}", i)).unwrap();
		}
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(3));

	// Spawn multiple threads that try to snapshot concurrently
	let handles: Vec<_> = (0..3)
		.map(|_| {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);

			thread::spawn(move || {
				barrier.wait();
				if let Some(persistence) = db.persistence() {
					persistence.snapshot()
				} else {
					Ok(())
				}
			})
		})
		.collect();

	let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

	// At least one should succeed, or all might succeed if serialized
	let successes = results.iter().filter(|r| r.is_ok()).count();
	assert!(successes >= 1, "At least one snapshot should succeed");

	// Data should be intact
	let tx = db.transaction(false);
	let count = tx.scan("key_".."key_z", None, None).unwrap().len();
	assert_eq!(count, 100, "All keys should exist after concurrent snapshots");
}

// =============================================================================
// Read During Snapshot Tests
// =============================================================================

#[test]
fn read_during_snapshot() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	let db = Arc::new(Database::new_with_persistence(db_opts, persistence_opts).unwrap());

	// Create data
	{
		let mut tx = db.transaction(true);
		for i in 0..100 {
			tx.set(format!("read_key_{:04}", i), format!("value_{}", i)).unwrap();
		}
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(2));

	// Thread 1: Perform snapshot
	let db1 = Arc::clone(&db);
	let barrier1 = Arc::clone(&barrier);
	let snapshot_handle = thread::spawn(move || {
		barrier1.wait();
		if let Some(persistence) = db1.persistence() {
			persistence.snapshot()
		} else {
			Ok(())
		}
	});

	// Thread 2: Perform reads during snapshot
	let db2 = Arc::clone(&db);
	let barrier2 = Arc::clone(&barrier);
	let read_handle = thread::spawn(move || {
		barrier2.wait();
		let mut read_count = 0;
		for i in 0..100 {
			let tx = db2.transaction(false);
			let key = format!("read_key_{:04}", i);
			if tx.get(&key).unwrap().is_some() {
				read_count += 1;
			}
		}
		read_count
	});

	snapshot_handle.join().unwrap().unwrap();
	let read_count = read_handle.join().unwrap();

	// All reads should succeed during snapshot
	assert_eq!(read_count, 100, "All reads during snapshot should succeed");
}

// =============================================================================
// Recovery From Concurrent Snapshot Tests
// =============================================================================

#[test]
fn recovery_from_concurrent_snapshot() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();

	// Create database with data and snapshot during writes
	{
		let persistence_opts = PersistenceOptions::new(temp_path)
			.with_aol_mode(AolMode::SynchronousOnCommit)
			.with_snapshot_mode(SnapshotMode::Never);

		let db =
			Arc::new(Database::new_with_persistence(db_opts.clone(), persistence_opts).unwrap());

		// Initial data
		{
			let mut tx = db.transaction(true);
			for i in 0..50 {
				tx.set(format!("before_snap_{:03}", i), "before").unwrap();
			}
			tx.commit().unwrap();
		}

		// Take a snapshot
		if let Some(p) = db.persistence() {
			p.snapshot().unwrap();
		}

		// Write more data after snapshot
		{
			let mut tx = db.transaction(true);
			for i in 0..50 {
				tx.set(format!("after_snap_{:03}", i), "after").unwrap();
			}
			tx.commit().unwrap();
		}
	}

	// Recover from persistence
	{
		let persistence_opts = PersistenceOptions::new(temp_path)
			.with_aol_mode(AolMode::SynchronousOnCommit)
			.with_snapshot_mode(SnapshotMode::Never);

		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let tx = db.transaction(false);

		// All data should be recovered
		let before_count = tx.scan("before_snap_".."before_snap_z", None, None).unwrap().len();
		let after_count = tx.scan("after_snap_".."after_snap_z", None, None).unwrap().len();

		assert_eq!(before_count, 50, "Before-snapshot data should be recovered");
		assert_eq!(after_count, 50, "After-snapshot data should be recovered");

		// Verify specific values
		assert_eq!(tx.get("before_snap_000").unwrap(), Some(Bytes::from("before")));
		assert_eq!(tx.get("after_snap_049").unwrap(), Some(Bytes::from("after")));
	}
}
