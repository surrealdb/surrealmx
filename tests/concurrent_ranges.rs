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

//! Concurrent range operation tests for SurrealMX.
//!
//! Tests range scans under concurrent modifications including inserts,
//! deletes, and updates within scanned ranges.

use bytes::Bytes;
use std::sync::{Arc, Barrier};
use std::thread;
use surrealmx::Database;

// =============================================================================
// Insert Into Scanned Range Tests
// =============================================================================

#[test]
fn insert_into_scanned_range() {
	let db = Arc::new(Database::new());

	// Create initial data with gaps
	{
		let mut tx = db.transaction(true);
		tx.set("key_a", "value_a").unwrap();
		tx.set("key_c", "value_c").unwrap();
		tx.set("key_e", "value_e").unwrap();
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(2));

	// Thread 1: Perform a range scan with SSI
	let db1 = Arc::clone(&db);
	let barrier1 = Arc::clone(&barrier);
	let scan_handle = thread::spawn(move || {
		barrier1.wait();
		let mut tx = db1.transaction(true).with_serializable_snapshot_isolation();
		let results = tx.scan("key_a".."key_z", None, None).unwrap();
		let count = results.len();
		// Try to commit
		let commit_result = tx.commit();
		(count, commit_result)
	});

	// Thread 2: Insert a new key into the range
	let db2 = Arc::clone(&db);
	let barrier2 = Arc::clone(&barrier);
	let insert_handle = thread::spawn(move || {
		barrier2.wait();
		let mut tx = db2.transaction(true);
		tx.set("key_b", "value_b").unwrap(); // Insert between a and c
		tx.commit()
	});

	let (scan_count, scan_commit) = scan_handle.join().unwrap();
	let insert_result = insert_handle.join().unwrap();

	// Scan should see consistent snapshot (either 3 or 4 keys)
	assert!(scan_count >= 3, "Scan should see at least the original keys");

	// At least one should succeed
	assert!(
		scan_commit.is_ok() || insert_result.is_ok(),
		"At least one transaction should succeed"
	);
}

#[test]
fn delete_from_scanned_range() {
	let db = Arc::new(Database::new());

	// Create initial data
	{
		let mut tx = db.transaction(true);
		for i in 0..5 {
			tx.set(format!("key_{}", i), format!("value_{}", i)).unwrap();
		}
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(2));

	// Thread 1: Scan the range and try to commit
	let db1 = Arc::clone(&db);
	let barrier1 = Arc::clone(&barrier);
	let scan_handle = thread::spawn(move || {
		barrier1.wait();
		let mut tx = db1.transaction(true).with_serializable_snapshot_isolation();
		let results = tx.scan("key_".."key_z", None, None).unwrap();
		let count = results.len();
		let commit_result = tx.commit();
		(count, commit_result)
	});

	// Thread 2: Delete a key from the range
	let db2 = Arc::clone(&db);
	let barrier2 = Arc::clone(&barrier);
	let delete_handle = thread::spawn(move || {
		barrier2.wait();
		let mut tx = db2.transaction(true);
		tx.del("key_2").unwrap();
		tx.commit()
	});

	let (scan_count, _scan_commit) = scan_handle.join().unwrap();
	let delete_result = delete_handle.join().unwrap();

	// Scan should see a consistent snapshot
	assert!(scan_count >= 4, "Scan should see most keys in consistent snapshot");

	// Delete should succeed (it doesn't conflict with read-only scan in SI mode)
	assert!(delete_result.is_ok(), "Delete should succeed");
}

#[test]
fn update_within_scanned_range() {
	let db = Arc::new(Database::new());

	// Create initial data
	{
		let mut tx = db.transaction(true);
		for i in 0..5 {
			tx.set(format!("key_{}", i), format!("value_{}", i)).unwrap();
		}
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(2));

	// Thread 1: Scan range with SSI
	let db1 = Arc::clone(&db);
	let barrier1 = Arc::clone(&barrier);
	let scan_handle = thread::spawn(move || {
		barrier1.wait();
		let mut tx = db1.transaction(true).with_serializable_snapshot_isolation();
		let results = tx.scan("key_".."key_z", None, None).unwrap();
		// Collect all values
		let values: Vec<_> = results.iter().map(|(_, v)| v.clone()).collect();
		let commit_result = tx.commit();
		(values, commit_result)
	});

	// Thread 2: Update a key within the range
	let db2 = Arc::clone(&db);
	let barrier2 = Arc::clone(&barrier);
	let update_handle = thread::spawn(move || {
		barrier2.wait();
		let mut tx = db2.transaction(true);
		tx.set("key_2", "updated_value").unwrap();
		tx.commit()
	});

	let (scan_values, _scan_commit) = scan_handle.join().unwrap();
	let update_result = update_handle.join().unwrap();

	// Scan should see consistent values
	assert_eq!(scan_values.len(), 5, "Scan should see all 5 keys");

	// Update should succeed
	assert!(update_result.is_ok(), "Update should succeed");

	// Verify update took effect
	let tx = db.transaction(false);
	assert_eq!(
		tx.get("key_2").unwrap(),
		Some(Bytes::from("updated_value")),
		"Update should be visible"
	);
}

#[test]
fn concurrent_range_scans() {
	let db = Arc::new(Database::new());

	// Create initial data
	{
		let mut tx = db.transaction(true);
		for i in 0..20 {
			tx.set(format!("key_{:02}", i), format!("value_{}", i)).unwrap();
		}
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(3));
	let mut handles = vec![];

	// Spawn multiple concurrent scanners
	for scanner_id in 0..3 {
		let db = Arc::clone(&db);
		let barrier = Arc::clone(&barrier);

		handles.push(thread::spawn(move || {
			barrier.wait();
			let tx = db.transaction(false);
			let results = tx.scan("key_".."key_z", None, None).unwrap();
			(scanner_id, results.len())
		}));
	}

	let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

	// All scanners should see the same count (consistent snapshot)
	for (scanner_id, count) in &results {
		assert_eq!(*count, 20, "Scanner {} should see all 20 keys, got {}", scanner_id, count);
	}
}

#[test]
fn scan_while_bulk_insert() {
	let db = Arc::new(Database::new());

	// Create some initial data
	{
		let mut tx = db.transaction(true);
		for i in 0..10 {
			tx.set(format!("existing_{:02}", i), "initial").unwrap();
		}
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(2));

	// Thread 1: Perform a scan
	let db1 = Arc::clone(&db);
	let barrier1 = Arc::clone(&barrier);
	let scan_handle = thread::spawn(move || {
		barrier1.wait();
		let tx = db1.transaction(false);
		let results = tx.scan("existing_".."existing_z", None, None).unwrap();
		results.len()
	});

	// Thread 2: Bulk insert new keys (in a different prefix to avoid conflicts)
	let db2 = Arc::clone(&db);
	let barrier2 = Arc::clone(&barrier);
	let insert_handle = thread::spawn(move || {
		barrier2.wait();
		let mut tx = db2.transaction(true);
		for i in 0..100 {
			tx.set(format!("new_{:03}", i), "bulk").unwrap();
		}
		tx.commit()
	});

	let scan_count = scan_handle.join().unwrap();
	let insert_result = insert_handle.join().unwrap();

	// Scan should see a consistent snapshot of existing keys
	assert_eq!(scan_count, 10, "Scan should see all 10 existing keys");

	// Insert should succeed
	assert!(insert_result.is_ok(), "Bulk insert should succeed");

	// Verify new keys are visible
	let tx = db.transaction(false);
	let new_keys = tx.scan("new_".."new_z", None, None).unwrap();
	assert_eq!(new_keys.len(), 100, "All new keys should be visible");
}

#[test]
fn range_scan_consistency() {
	let db = Arc::new(Database::new());

	// Create initial data
	{
		let mut tx = db.transaction(true);
		for i in 0..10 {
			tx.set(format!("data_{:02}", i), format!("v{}", i)).unwrap();
		}
		tx.commit().unwrap();
	}

	// Start a long-running scan transaction
	let scan_tx = db.transaction(false);
	let first_scan = scan_tx.scan("data_".."data_z", None, None).unwrap();
	let first_count = first_scan.len();

	// Modify data while scan transaction is open
	{
		let mut tx = db.transaction(true);
		tx.set("data_05", "modified").unwrap();
		tx.del("data_03").unwrap();
		tx.set("data_99", "new_key").unwrap();
		tx.commit().unwrap();
	}

	// Perform another scan in the same transaction
	let second_scan = scan_tx.scan("data_".."data_z", None, None).unwrap();
	let second_count = second_scan.len();

	// Both scans should see the same data (snapshot consistency)
	assert_eq!(
		first_count, second_count,
		"Both scans in same transaction should see consistent snapshot"
	);

	// Values should be identical
	for (first, second) in first_scan.iter().zip(second_scan.iter()) {
		assert_eq!(first.0, second.0, "Keys should match");
		assert_eq!(first.1, second.1, "Values should match");
	}

	// New transaction should see modifications
	let new_tx = db.transaction(false);
	let new_scan = new_tx.scan("data_".."data_z", None, None).unwrap();
	// Original 10 - 1 deleted + 1 new = 10
	assert_eq!(new_scan.len(), 10, "New transaction should see modified state");

	// Verify specific changes
	assert!(new_tx.get("data_03").unwrap().is_none(), "Deleted key should not exist");
	assert_eq!(
		new_tx.get("data_05").unwrap(),
		Some(Bytes::from("modified")),
		"Modified key should have new value"
	);
	assert_eq!(
		new_tx.get("data_99").unwrap(),
		Some(Bytes::from("new_key")),
		"New key should exist"
	);
}
