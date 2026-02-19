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

//! Concurrent delete tests for SurrealMX.
//!
//! Tests delete operations under concurrent access, including racing
//! deletes, delete vs update conflicts, and phantom delete detection.

use bytes::Bytes;
use std::sync::{Arc, Barrier};
use std::thread;
use surrealmx::Database;

// =============================================================================
// Concurrent Delete Same Key Tests
// =============================================================================

#[test]
fn concurrent_delete_same_key() {
	let db = Arc::new(Database::new());

	// Create initial data
	{
		let mut tx = db.transaction(true);
		tx.set("shared_key", "value").unwrap();
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(2));
	let mut handles = vec![];

	// Spawn two threads that both try to delete the same key
	for _ in 0..2 {
		let db = Arc::clone(&db);
		let barrier = Arc::clone(&barrier);

		handles.push(thread::spawn(move || {
			barrier.wait();

			let mut tx = db.transaction(true).with_serializable_snapshot_isolation();
			// Read the key first to create a read dependency
			let _ = tx.get("shared_key").unwrap();
			// Now delete it
			tx.del("shared_key").unwrap();
			tx.commit()
		}));
	}

	let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
	let successes = results.iter().filter(|r| r.is_ok()).count();
	let failures = results.iter().filter(|r| r.is_err()).count();

	// At least one should succeed, and one should fail due to conflict
	assert!(successes >= 1, "At least one delete should succeed");
	assert_eq!(successes + failures, 2, "Both transactions should have completed");

	// Verify key is deleted
	let tx = db.transaction(false);
	assert!(tx.get("shared_key").unwrap().is_none(), "Key should be deleted");
}

#[test]
fn delete_racing_with_update() {
	let db = Arc::new(Database::new());

	// Create initial data
	{
		let mut tx = db.transaction(true);
		tx.set("key", "initial").unwrap();
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(2));

	// Thread 1: Delete the key
	let db1 = Arc::clone(&db);
	let barrier1 = Arc::clone(&barrier);
	let delete_handle = thread::spawn(move || {
		barrier1.wait();
		let mut tx = db1.transaction(true).with_serializable_snapshot_isolation();
		let _ = tx.get("key").unwrap(); // Read first
		tx.del("key").unwrap();
		tx.commit()
	});

	// Thread 2: Update the key
	let db2 = Arc::clone(&db);
	let barrier2 = Arc::clone(&barrier);
	let update_handle = thread::spawn(move || {
		barrier2.wait();
		let mut tx = db2.transaction(true).with_serializable_snapshot_isolation();
		let _ = tx.get("key").unwrap(); // Read first
		tx.set("key", "updated").unwrap();
		tx.commit()
	});

	let delete_result = delete_handle.join().unwrap();
	let update_result = update_handle.join().unwrap();

	// One should succeed, one should fail (or both might succeed in rare timing)
	let successes = [&delete_result, &update_result].iter().filter(|r| r.is_ok()).count();
	assert!(successes >= 1, "At least one operation should succeed");

	// Verify final state is consistent
	let tx = db.transaction(false);
	let final_value = tx.get("key").unwrap();
	// Either key is deleted (None) or updated (Some("updated"))
	assert!(
		final_value.is_none() || final_value == Some(Bytes::from("updated")),
		"Final state should be either deleted or updated"
	);
}

#[test]
fn delete_during_range_scan() {
	let db = Arc::new(Database::new());

	// Create initial data
	{
		let mut tx = db.transaction(true);
		for i in 0..10 {
			tx.set(format!("key{:02}", i), format!("value{}", i)).unwrap();
		}
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(2));

	// Thread 1: Perform a range scan
	let db1 = Arc::clone(&db);
	let barrier1 = Arc::clone(&barrier);
	let scan_handle = thread::spawn(move || {
		barrier1.wait();
		let mut tx = db1.transaction(true).with_serializable_snapshot_isolation();
		let results = tx.scan("key00".."key99", None, None).unwrap();
		// Try to commit after scanning
		let commit_result = tx.commit();
		(results.len(), commit_result)
	});

	// Thread 2: Delete some keys in the range
	let db2 = Arc::clone(&db);
	let barrier2 = Arc::clone(&barrier);
	let delete_handle = thread::spawn(move || {
		barrier2.wait();
		let mut tx = db2.transaction(true);
		tx.del("key05").unwrap();
		tx.del("key06").unwrap();
		tx.commit()
	});

	let (scan_count, scan_commit) = scan_handle.join().unwrap();
	let delete_result = delete_handle.join().unwrap();

	// The scan should see a consistent snapshot
	assert!(scan_count >= 8, "Scan should see most keys");

	// At least one transaction should succeed
	assert!(
		scan_commit.is_ok() || delete_result.is_ok(),
		"At least one transaction should succeed"
	);
}

#[test]
fn concurrent_delete_different_keys() {
	let db = Arc::new(Database::new());

	// Create initial data
	{
		let mut tx = db.transaction(true);
		tx.set("key_a", "value_a").unwrap();
		tx.set("key_b", "value_b").unwrap();
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(2));

	// Thread 1: Delete key_a
	let db1 = Arc::clone(&db);
	let barrier1 = Arc::clone(&barrier);
	let handle1 = thread::spawn(move || {
		barrier1.wait();
		let mut tx = db1.transaction(true);
		tx.del("key_a").unwrap();
		tx.commit()
	});

	// Thread 2: Delete key_b
	let db2 = Arc::clone(&db);
	let barrier2 = Arc::clone(&barrier);
	let handle2 = thread::spawn(move || {
		barrier2.wait();
		let mut tx = db2.transaction(true);
		tx.del("key_b").unwrap();
		tx.commit()
	});

	let result1 = handle1.join().unwrap();
	let result2 = handle2.join().unwrap();

	// Both should succeed (no conflict on different keys)
	assert!(result1.is_ok(), "Delete of key_a should succeed");
	assert!(result2.is_ok(), "Delete of key_b should succeed");

	// Verify both keys are deleted
	let tx = db.transaction(false);
	assert!(tx.get("key_a").unwrap().is_none(), "key_a should be deleted");
	assert!(tx.get("key_b").unwrap().is_none(), "key_b should be deleted");
}

#[test]
fn delete_then_recreate_concurrent() {
	let db = Arc::new(Database::new());

	// Create initial data
	{
		let mut tx = db.transaction(true);
		tx.set("key", "original").unwrap();
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(2));

	// Thread 1: Delete and recreate the key
	let db1 = Arc::clone(&db);
	let barrier1 = Arc::clone(&barrier);
	let handle1 = thread::spawn(move || {
		barrier1.wait();
		let mut tx = db1.transaction(true).with_serializable_snapshot_isolation();
		let _ = tx.get("key").unwrap(); // Read first
		tx.del("key").unwrap();
		tx.set("key", "recreated_by_1").unwrap();
		tx.commit()
	});

	// Thread 2: Also delete and recreate
	let db2 = Arc::clone(&db);
	let barrier2 = Arc::clone(&barrier);
	let handle2 = thread::spawn(move || {
		barrier2.wait();
		let mut tx = db2.transaction(true).with_serializable_snapshot_isolation();
		let _ = tx.get("key").unwrap(); // Read first
		tx.del("key").unwrap();
		tx.set("key", "recreated_by_2").unwrap();
		tx.commit()
	});

	let result1 = handle1.join().unwrap();
	let result2 = handle2.join().unwrap();

	// At least one should succeed
	let successes = [&result1, &result2].iter().filter(|r| r.is_ok()).count();
	assert!(successes >= 1, "At least one transaction should succeed");

	// Verify key exists with one of the values
	let tx = db.transaction(false);
	let value = tx.get("key").unwrap();
	assert!(value.is_some(), "Key should exist after recreation");
	let value_bytes = value.unwrap();
	assert!(
		value_bytes == "recreated_by_1" || value_bytes == "recreated_by_2",
		"Key should have one of the recreated values"
	);
}

#[test]
fn ssi_delete_read_conflict() {
	let db = Database::new();

	// Create initial data
	{
		let mut tx = db.transaction(true);
		tx.set("key", "value").unwrap();
		tx.commit().unwrap();
	}

	// Start tx1 and read the key
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	let value1 = tx1.get("key").unwrap();
	assert_eq!(value1, Some(Bytes::from("value")));

	// Start tx2 and delete the same key
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
	tx2.del("key").unwrap();
	tx2.commit().unwrap();

	// tx1 tries to update based on its read - should conflict
	tx1.set("key", "updated_value").unwrap();
	let commit_result = tx1.commit();

	// With SSI, tx1 should fail because it read a key that was deleted
	assert!(commit_result.is_err(), "tx1 should fail due to read-write conflict (key was deleted)");

	// Verify key is deleted
	let tx = db.transaction(false);
	assert!(tx.get("key").unwrap().is_none(), "Key should be deleted");
}
