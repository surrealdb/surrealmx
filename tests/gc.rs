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

//! Garbage collection tests for SurrealMX.
//!
//! Tests manual `run_gc()`, background GC behavior, and GC history modes.

use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use surrealmx::{Database, DatabaseOptions};

// =============================================================================
// Manual GC Tests
// =============================================================================

#[test]

fn manual_gc_removes_stale_versions() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_secs(3600)) // Disable background GC
			.with_cleanup_interval(Duration::from_secs(3600)),
	)
	.with_gc();

	// Create multiple versions
	for i in 0..10 {
		let mut tx = db.transaction(true);

		tx.set("key", format!("v{}", i)).unwrap();

		tx.commit().unwrap();
	}

	// Run manual GC
	db.run_gc();

	// Current value should still exist
	let mut tx = db.transaction(false);

	let current = tx.get("key").unwrap();

	assert!(current.is_some(), "Current value should exist after GC");

	assert_eq!(current.unwrap().as_ref(), b"v9", "Should have latest value");

	tx.cancel().unwrap();
}

#[test]

fn manual_gc_respects_active_transactions() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_secs(3600))
			.with_cleanup_interval(Duration::from_secs(3600)),
	)
	.with_gc();

	// Create initial version
	let mut tx = db.transaction(true);

	tx.set("key", "v1").unwrap();

	tx.commit().unwrap();

	// Start long-lived read transaction
	let read_tx = db.transaction(false);

	let initial = read_tx.get("key").unwrap();

	assert_eq!(initial, Some(Bytes::from("v1")));

	// Update multiple times
	for i in 2..10 {
		let mut tx = db.transaction(true);

		tx.set("key", format!("v{}", i)).unwrap();

		tx.commit().unwrap();
	}

	// Run manual GC
	db.run_gc();

	// Active read transaction should still see original value
	let after_gc = read_tx.get("key").unwrap();

	assert_eq!(
		after_gc,
		Some(Bytes::from("v1")),
		"Active transaction should still see v1 after GC"
	);
}

#[test]

fn manual_gc_removes_deleted_keys() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_secs(3600))
			.with_cleanup_interval(Duration::from_secs(3600)),
	)
	.with_gc();

	// Create and delete key
	let mut tx = db.transaction(true);

	tx.set("deleted_key", "value").unwrap();

	tx.commit().unwrap();

	let mut tx = db.transaction(true);

	tx.del("deleted_key").unwrap();

	tx.commit().unwrap();

	// Wait a tiny bit to ensure delete timestamp is old enough
	std::thread::sleep(Duration::from_millis(10));

	// Run manual GC
	db.run_gc();

	// Key should not exist
	let mut tx = db.transaction(false);

	assert!(tx.get("deleted_key").unwrap().is_none());

	tx.cancel().unwrap();
}

// =============================================================================
// Background GC Tests
// =============================================================================

#[test]

fn background_gc_runs_automatically() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(100))
			.with_cleanup_interval(Duration::from_millis(100)),
	)
	.with_gc();

	// Create many versions quickly
	for i in 0..20 {
		let mut tx = db.transaction(true);

		tx.set("gc_key", format!("value_{}", i)).unwrap();

		tx.commit().unwrap();
	}

	// Wait for background GC to run
	std::thread::sleep(Duration::from_millis(500));

	// Current value should still be available
	let mut tx = db.transaction(false);

	let val = tx.get("gc_key").unwrap();

	assert!(val.is_some());

	tx.cancel().unwrap();
}

#[test]

fn disabled_gc_preserves_all_versions() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	);

	// Note: NOT calling .with_gc() or .with_gc_history() - no GC enabled

	// Create v1
	let mut tx = db.transaction(true);

	tx.set("key", "v1").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(10));

	// Capture version after v1 commits
	let snapshot1 = db.transaction(false);

	let version1 = snapshot1.version();

	std::thread::sleep(Duration::from_millis(10));

	// Create v2
	let mut tx = db.transaction(true);

	tx.set("key", "v2").unwrap();

	tx.commit().unwrap();

	// Wait for any potential GC
	std::thread::sleep(Duration::from_millis(200));

	// Advance time with a dummy transaction
	let mut tx = db.transaction(true);

	tx.set("dummy", "value").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(10));

	// Old version should still be readable (no GC to clean it up)
	let tx = db.transaction(false);

	let v1 = tx.get_at_version("key", version1).unwrap();

	assert_eq!(v1, Some(Bytes::from("v1")), "Without GC, old versions should persist");
}

// =============================================================================
// GC History Mode Tests
// =============================================================================

#[test]

fn gc_history_mode_preserves_recent_versions() {
	let history = Duration::from_secs(60); // 60 second history window

	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	)
	.with_gc_history(history);

	// Create v1
	let mut tx = db.transaction(true);

	tx.set("key", "v1").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(10));

	// Capture version after v1 commits
	let snapshot1 = db.transaction(false);

	let version1 = snapshot1.version();

	std::thread::sleep(Duration::from_millis(10));

	// Update to v2
	let mut tx = db.transaction(true);

	tx.set("key", "v2").unwrap();

	tx.commit().unwrap();

	// Wait for GC
	std::thread::sleep(Duration::from_millis(200));

	// Advance time with a dummy transaction
	let mut tx = db.transaction(true);

	tx.set("dummy", "value").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(10));

	// Version1 should still be readable (within history window)
	let tx = db.transaction(false);

	let v1 = tx.get_at_version("key", version1).unwrap();

	assert_eq!(v1, Some(Bytes::from("v1")), "Version within history window should be preserved");
}

#[test]

fn gc_with_zero_history_cleans_aggressively() {
	// with_gc() sets history to None/zero - most aggressive cleanup
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	)
	.with_gc();

	// Create versions quickly
	for i in 0..10 {
		let mut tx = db.transaction(true);

		tx.set("key", format!("v{}", i)).unwrap();

		tx.commit().unwrap();
	}

	// Wait for aggressive GC
	std::thread::sleep(Duration::from_millis(300));

	// Current should exist
	let mut tx = db.transaction(false);

	let current = tx.get("key").unwrap();

	assert!(current.is_some());

	tx.cancel().unwrap();
}

// =============================================================================
// GC with Multiple Keys
// =============================================================================

#[test]

fn gc_handles_multiple_keys() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(100))
			.with_cleanup_interval(Duration::from_millis(100)),
	)
	.with_gc();

	// Create multiple keys with multiple versions
	for key_id in 0..10 {
		for version in 0..5 {
			let mut tx = db.transaction(true);

			tx.set(format!("key_{}", key_id), format!("v{}", version)).unwrap();

			tx.commit().unwrap();
		}
	}

	// Wait for GC
	std::thread::sleep(Duration::from_millis(300));

	// All keys should still have their latest values
	let mut tx = db.transaction(false);

	for key_id in 0..10 {
		let val = tx.get(format!("key_{}", key_id)).unwrap();

		assert_eq!(val, Some(Bytes::from("v4")), "Key {} should have latest value", key_id);
	}

	tx.cancel().unwrap();
}

#[test]

fn gc_with_mixed_deletes() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	)
	.with_gc();

	// Create some keys
	let mut tx = db.transaction(true);

	tx.set("keep1", "value1").unwrap();

	tx.set("keep2", "value2").unwrap();

	tx.set("delete1", "value").unwrap();

	tx.set("delete2", "value").unwrap();

	tx.commit().unwrap();

	// Delete some
	let mut tx = db.transaction(true);

	tx.del("delete1").unwrap();

	tx.del("delete2").unwrap();

	tx.commit().unwrap();

	// Wait for GC
	std::thread::sleep(Duration::from_millis(200));

	// Verify state
	let mut tx = db.transaction(false);

	assert_eq!(tx.get("keep1").unwrap(), Some(Bytes::from("value1")));

	assert_eq!(tx.get("keep2").unwrap(), Some(Bytes::from("value2")));

	assert!(tx.get("delete1").unwrap().is_none());

	assert!(tx.get("delete2").unwrap().is_none());

	tx.cancel().unwrap();
}

// =============================================================================
// Transaction Cleanup Tests
// =============================================================================

#[test]

fn transaction_cleanup_runs_automatically() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_secs(3600))
			.with_cleanup_interval(Duration::from_millis(100)),
	);

	// Create and complete many transactions
	for i in 0..50 {
		let mut tx = db.transaction(true);

		tx.set(format!("key_{}", i), "value").unwrap();

		tx.commit().unwrap();
	}

	// Wait for cleanup to run
	std::thread::sleep(Duration::from_millis(300));

	// New transactions should work fine
	let mut tx = db.transaction(false);

	for i in 0..50 {
		assert!(tx.exists(format!("key_{}", i)).unwrap());
	}

	tx.cancel().unwrap();
}

#[test]

fn manual_cleanup() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_secs(3600))
			.with_cleanup_interval(Duration::from_secs(3600)), // Disable auto cleanup
	);

	// Create transactions
	for i in 0..20 {
		let mut tx = db.transaction(true);

		tx.set(format!("key_{}", i), "value").unwrap();

		tx.commit().unwrap();
	}

	// Run manual cleanup
	db.run_cleanup();

	// Should still work
	let mut tx = db.transaction(false);

	for i in 0..20 {
		assert!(tx.exists(format!("key_{}", i)).unwrap());
	}

	tx.cancel().unwrap();
}

// =============================================================================
// Concurrent GC Tests
// =============================================================================

#[test]

fn concurrent_gc_and_transactions() {
	let db = Arc::new(
		Database::new_with_options(
			DatabaseOptions::default()
				.with_gc_interval(Duration::from_millis(50))
				.with_cleanup_interval(Duration::from_millis(50)),
		)
		.with_gc(),
	);

	let num_threads = 4;

	let ops_per_thread = 50;

	let handles: Vec<_> = (0..num_threads)
		.map(|thread_id| {
			let db = Arc::clone(&db);

			std::thread::spawn(move || {
				for op in 0..ops_per_thread {
					let key = format!("thread_{}_key_{}", thread_id, op % 10);

					// Write
					let mut tx = db.transaction(true);

					tx.set(&key, format!("value_{}_{}", thread_id, op)).unwrap();

					let _ = tx.commit();

					// Read
					let tx = db.transaction(false);

					let _ = tx.get(&key);
				}
			})
		})
		.collect();

	for handle in handles {
		handle.join().unwrap();
	}

	// Verify integrity
	let mut tx = db.transaction(false);

	for thread_id in 0..num_threads {
		for key_id in 0..10 {
			let key = format!("thread_{}_key_{}", thread_id, key_id);

			let val = tx.get(&key).unwrap();

			assert!(val.is_some(), "Key {} should exist", key);
		}
	}

	tx.cancel().unwrap();
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]

fn gc_with_only_deleted_keys() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	)
	.with_gc();

	// Create and immediately delete
	let mut tx = db.transaction(true);

	for i in 0..10 {
		tx.set(format!("temp_{}", i), "value").unwrap();
	}

	tx.commit().unwrap();

	let mut tx = db.transaction(true);

	for i in 0..10 {
		tx.del(format!("temp_{}", i)).unwrap();
	}

	tx.commit().unwrap();

	// Wait for GC
	std::thread::sleep(Duration::from_millis(200));

	// All should be gone
	let mut tx = db.transaction(false);

	for i in 0..10 {
		assert!(tx.get(format!("temp_{}", i)).unwrap().is_none());
	}

	tx.cancel().unwrap();
}

#[test]

fn gc_empty_database() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	)
	.with_gc();

	// Run GC on empty database
	db.run_gc();

	db.run_cleanup();

	// Should work fine
	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	tx.commit().unwrap();

	let mut tx = db.transaction(false);

	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("value")));

	tx.cancel().unwrap();
}

#[test]

fn gc_preserves_tombstones_for_active_readers() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	)
	.with_gc();

	// Create and then delete a key
	let mut tx = db.transaction(true);

	tx.set("key", "original").unwrap();

	tx.commit().unwrap();

	// Start a read that sees the key
	let read_tx = db.transaction(false);

	assert_eq!(read_tx.get("key").unwrap(), Some(Bytes::from("original")));

	// Delete the key
	let mut tx = db.transaction(true);

	tx.del("key").unwrap();

	tx.commit().unwrap();

	// Wait for GC
	std::thread::sleep(Duration::from_millis(200));

	// Old reader should still see original value
	assert_eq!(
		read_tx.get("key").unwrap(),
		Some(Bytes::from("original")),
		"Reader should see value from its snapshot"
	);

	// New reader should see deletion
	let new_tx = db.transaction(false);

	assert!(new_tx.get("key").unwrap().is_none());
}
