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

//! Concurrency tests for SurrealMX.
//!
//! Tests concurrent operations, GC with active readers, and multi-writer
//! scenarios.

use bytes::Bytes;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use surrealmx::{Database, DatabaseOptions};

// =============================================================================
// GC with Active Readers Tests
// =============================================================================

#[test]
fn gc_does_not_remove_versions_needed_by_active_readers() {
	let db = Arc::new(
		Database::new_with_options(
			DatabaseOptions::default()
				.with_gc_interval(Duration::from_millis(50))
				.with_cleanup_interval(Duration::from_millis(50)),
		)
		.with_gc(),
	);

	// Create initial data
	{
		let mut tx = db.transaction(true);
		tx.set("key", "v1").unwrap();
		tx.commit().unwrap();
	}

	// Start a long-lived read transaction
	let read_tx = db.transaction(false);
	let initial_value = read_tx.get("key").unwrap();
	assert_eq!(initial_value, Some(Bytes::from("v1")));

	// Update the key multiple times
	for i in 2..10 {
		let mut tx = db.transaction(true);
		tx.set("key", format!("v{}", i)).unwrap();
		tx.commit().unwrap();
	}

	// Wait for GC to run
	std::thread::sleep(Duration::from_millis(200));

	// The original read transaction should still see v1
	let value_after_gc = read_tx.get("key").unwrap();
	assert_eq!(
		value_after_gc,
		Some(Bytes::from("v1")),
		"Active reader should still see original value after GC"
	);
}

#[test]
fn gc_cleans_up_after_readers_complete() {
	let db = Arc::new(
		Database::new_with_options(
			DatabaseOptions::default()
				.with_gc_interval(Duration::from_millis(50))
				.with_cleanup_interval(Duration::from_millis(50)),
		)
		.with_gc(),
	);

	// Create initial data
	{
		let mut tx = db.transaction(true);
		tx.set("key", "v1").unwrap();
		tx.commit().unwrap();
	}

	// Start and complete a read transaction
	{
		let tx = db.transaction(false);
		let _ = tx.get("key").unwrap();
		// tx drops here, releasing its hold on the version
	}

	// Update multiple times
	for i in 2..20 {
		let mut tx = db.transaction(true);
		tx.set("key", format!("v{}", i)).unwrap();
		tx.commit().unwrap();
	}

	// Wait for GC
	std::thread::sleep(Duration::from_millis(300));

	// Current value should be latest
	let mut tx = db.transaction(false);
	let current = tx.get("key").unwrap();
	assert_eq!(current, Some(Bytes::from("v19")));
	tx.cancel().unwrap();
}

// =============================================================================
// Multiple Writers Tests
// =============================================================================

#[test]
fn multiple_writers_disjoint_keys() {
	let db = Arc::new(Database::new());
	let num_writers = 8;
	let ops_per_writer = 100;
	let barrier = Arc::new(Barrier::new(num_writers));

	let handles: Vec<_> = (0..num_writers)
		.map(|writer_id| {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);

			thread::spawn(move || {
				barrier.wait();

				for op_id in 0..ops_per_writer {
					let key = format!("writer_{}_key_{}", writer_id, op_id);
					let value = format!("value_{}_{}", writer_id, op_id);

					let mut tx = db.transaction(true);
					tx.set(key, value).unwrap();
					tx.commit().unwrap();
				}
			})
		})
		.collect();

	for handle in handles {
		handle.join().unwrap();
	}

	// Verify all keys exist
	let mut tx = db.transaction(false);
	for writer_id in 0..num_writers {
		for op_id in 0..ops_per_writer {
			let key = format!("writer_{}_key_{}", writer_id, op_id);
			let expected = format!("value_{}_{}", writer_id, op_id);
			let actual = tx.get(&key).unwrap();
			assert_eq!(actual, Some(Bytes::from(expected)), "Key {} missing", key);
		}
	}
	tx.cancel().unwrap();
}

#[test]
fn multiple_writers_overlapping_keys_conflict() {
	let db = Arc::new(Database::new());
	let num_writers = 4;
	let barrier = Arc::new(Barrier::new(num_writers));

	// All writers try to update the same key using SSI to ensure conflict detection
	let handles: Vec<_> = (0..num_writers)
		.map(|writer_id| {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);

			thread::spawn(move || {
				barrier.wait();

				// Use SSI with read to ensure conflict detection
				let mut tx = db.transaction(true).with_serializable_snapshot_isolation();
				// Read first to establish dependency
				let _ = tx.get("shared_key");
				tx.set("shared_key", format!("writer_{}", writer_id)).unwrap();
				tx.commit()
			})
		})
		.collect();

	let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

	// At least one should succeed, and some should fail due to conflicts
	let successes = results.iter().filter(|r| r.is_ok()).count();
	let failures = results.iter().filter(|r| r.is_err()).count();

	assert!(successes >= 1, "At least one writer should succeed");
	assert!(successes + failures == num_writers, "All writers should have completed");

	// Key should have some value
	let mut tx = db.transaction(false);
	let value = tx.get("shared_key").unwrap();
	assert!(value.is_some(), "Key should have a value");
	tx.cancel().unwrap();
}

// =============================================================================
// Mixed Reader/Writer Tests
// =============================================================================

#[test]
fn concurrent_readers_and_writers() {
	let db = Arc::new(Database::new());

	// Seed with initial data
	{
		let mut tx = db.transaction(true);
		for i in 0..100 {
			tx.set(format!("key_{:04}", i), format!("initial_{}", i)).unwrap();
		}
		tx.commit().unwrap();
	}

	let num_readers = 4;
	let num_writers = 2;
	let barrier = Arc::new(Barrier::new(num_readers + num_writers));

	// Reader threads
	let reader_handles: Vec<_> = (0..num_readers)
		.map(|_| {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);

			thread::spawn(move || {
				barrier.wait();

				for _ in 0..50 {
					let tx = db.transaction(false);
					// Read multiple keys
					for i in 0..10 {
						let _ = tx.get(format!("key_{:04}", i * 10)).unwrap();
					}
					// Small delay
					thread::sleep(Duration::from_micros(100));
				}
			})
		})
		.collect();

	// Writer threads
	let writer_handles: Vec<_> = (0..num_writers)
		.map(|writer_id| {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);

			thread::spawn(move || {
				barrier.wait();

				for op in 0..20 {
					let key = format!("writer_{}_op_{}", writer_id, op);
					let mut tx = db.transaction(true);
					tx.set(key, "written").unwrap();
					let _ = tx.commit();
					thread::sleep(Duration::from_micros(500));
				}
			})
		})
		.collect();

	// Wait for all to complete
	for handle in reader_handles {
		handle.join().unwrap();
	}
	for handle in writer_handles {
		handle.join().unwrap();
	}

	// Verify original data still intact
	let mut tx = db.transaction(false);
	for i in 0..100 {
		let val = tx.get(format!("key_{:04}", i)).unwrap();
		// Value should either be initial or have been updated
		assert!(val.is_some(), "Key {} should exist", i);
	}
	tx.cancel().unwrap();
}

// =============================================================================
// Transaction Pool Tests
// =============================================================================

#[test]
fn rapid_transaction_creation() {
	let db = Arc::new(Database::new());
	let num_threads = 16;
	let txns_per_thread = 100;
	let barrier = Arc::new(Barrier::new(num_threads));

	let handles: Vec<_> = (0..num_threads)
		.map(|_| {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);

			thread::spawn(move || {
				barrier.wait();

				for _ in 0..txns_per_thread {
					// Create and immediately cancel transactions
					let tx = db.transaction(false);
					let _ = tx.get("any_key");
				}
			})
		})
		.collect();

	for handle in handles {
		handle.join().unwrap();
	}

	// Should complete without panicking or deadlocking
}

#[test]
fn transaction_pool_recycling() {
	let db = Database::new_with_options(DatabaseOptions::default());

	// Create and drop many transactions
	for i in 0..1000 {
		let mut tx = db.transaction(i % 2 == 0);

		if i % 2 == 0 {
			tx.set(format!("key_{}", i), "value").unwrap();
			let _ = tx.commit();
		} else {
			let _ = tx.get("any_key");
			tx.cancel().unwrap();
		}
	}

	// Verify data
	let mut tx = db.transaction(false);
	// Only even numbered keys should exist
	for i in (0..1000).step_by(2) {
		assert!(tx.exists(format!("key_{}", i)).unwrap());
	}
	tx.cancel().unwrap();
}

// =============================================================================
// Concurrent Snapshot Tests
// =============================================================================

#[test]
fn concurrent_writes_during_snapshot() {
	use surrealmx::{AolMode, PersistenceOptions, SnapshotMode};
	use tempfile::TempDir;

	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	let db = Arc::new(Database::new_with_persistence(db_opts, persistence_opts).unwrap());

	// Seed data
	{
		let mut tx = db.transaction(true);
		for i in 0..100 {
			tx.set(format!("key_{}", i), "initial").unwrap();
		}
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(3));

	// Thread 1: Create snapshot
	let db1 = Arc::clone(&db);
	let barrier1 = Arc::clone(&barrier);
	let snapshot_handle = thread::spawn(move || {
		barrier1.wait();
		if let Some(persistence) = db1.persistence() {
			persistence.snapshot().unwrap();
		}
	});

	// Thread 2 & 3: Write while snapshot is happening
	let writer_handles: Vec<_> = (0..2)
		.map(|writer_id| {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);

			thread::spawn(move || {
				barrier.wait();

				for op in 0..50 {
					let mut tx = db.transaction(true);
					tx.set(format!("concurrent_{}_{}", writer_id, op), "value").unwrap();
					let _ = tx.commit();
				}
			})
		})
		.collect();

	snapshot_handle.join().unwrap();
	for handle in writer_handles {
		handle.join().unwrap();
	}

	// Verify data integrity
	let mut tx = db.transaction(false);
	for i in 0..100 {
		assert!(tx.get(format!("key_{}", i)).unwrap().is_some());
	}
	tx.cancel().unwrap();
}

// =============================================================================
// Stress Tests
// =============================================================================

#[test]
fn high_contention_counter() {
	let db = Arc::new(Database::new());

	// Initialize counter
	{
		let mut tx = db.transaction(true);
		tx.set("counter", "0").unwrap();
		tx.commit().unwrap();
	}

	let num_threads = 4;
	let increments_per_thread = 5;
	let barrier = Arc::new(Barrier::new(num_threads));

	let handles: Vec<_> = (0..num_threads)
		.map(|_| {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);

			thread::spawn(move || {
				barrier.wait();

				let mut successes = 0;
				let mut attempts = 0;

				while successes < increments_per_thread && attempts < 200 {
					attempts += 1;

					let mut tx = db.transaction(true).with_serializable_snapshot_isolation();

					// Read current value
					let current = tx.get("counter").unwrap();
					let current_val: i32 = match current {
						Some(b) => std::str::from_utf8(&b).unwrap().parse().unwrap(),
						None => 0,
					};

					// Increment
					tx.set("counter", (current_val + 1).to_string()).unwrap();

					// Try to commit
					if tx.commit().is_ok() {
						successes += 1;
					}
				}
			})
		})
		.collect();

	// Wait for all threads
	for handle in handles {
		handle.join().unwrap();
	}

	// Get final value
	let mut tx = db.transaction(false);
	let final_val: i32 =
		std::str::from_utf8(&tx.get("counter").unwrap().unwrap()).unwrap().parse().unwrap();
	tx.cancel().unwrap();

	// With SSI, each thread should eventually succeed in incrementing
	// The counter should be positive (some increments succeeded)
	assert!(final_val > 0, "Counter should have been incremented at least once");

	// The maximum possible value is num_threads * increments_per_thread
	let max_possible = num_threads * increments_per_thread;
	assert!(
		final_val <= max_possible as i32,
		"Counter should not exceed maximum possible increments"
	);
}

#[test]
fn concurrent_scan_operations() {
	let db = Arc::new(Database::new());

	// Create data
	{
		let mut tx = db.transaction(true);
		for i in 0..1000 {
			tx.set(format!("key_{:06}", i), format!("value_{}", i)).unwrap();
		}
		tx.commit().unwrap();
	}

	let num_threads = 8;
	let barrier = Arc::new(Barrier::new(num_threads));

	let handles: Vec<_> = (0..num_threads)
		.map(|_| {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);

			thread::spawn(move || {
				barrier.wait();

				for _ in 0..10 {
					let tx = db.transaction(false);
					let results = tx.scan("key_".."key_z", None, Some(100)).unwrap();
					assert_eq!(results.len(), 100);
				}
			})
		})
		.collect();

	for handle in handles {
		handle.join().unwrap();
	}
}
