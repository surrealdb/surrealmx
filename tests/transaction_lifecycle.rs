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

//! Transaction lifecycle tests for SurrealMX.
//!
//! Tests transaction management edge cases including long-lived transactions,
//! transaction drop behavior, and high transaction counts.

use bytes::Bytes;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use surrealmx::{Database, DatabaseOptions};

// =============================================================================
// Long-Lived Read Transaction Tests
// =============================================================================

#[test]
fn long_lived_read_transaction() {
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

	// Perform many updates while read transaction is open
	for i in 2..20 {
		let mut tx = db.transaction(true);
		tx.set("key", format!("v{}", i)).unwrap();
		tx.commit().unwrap();
	}

	// Wait for GC
	std::thread::sleep(Duration::from_millis(200));

	// Long-lived read transaction should still see original value
	let value_after_updates = read_tx.get("key").unwrap();
	assert_eq!(
		value_after_updates,
		Some(Bytes::from("v1")),
		"Long-lived read transaction should maintain snapshot"
	);

	// New transaction should see latest value
	let new_tx = db.transaction(false);
	let latest = new_tx.get("key").unwrap();
	assert_eq!(latest, Some(Bytes::from("v19")), "New transaction should see latest");
}

// =============================================================================
// Transaction Drop Without Close Tests
// =============================================================================

#[test]
fn transaction_drop_without_close() {
	let db = Database::new();

	// Create initial data
	{
		let mut tx = db.transaction(true);
		tx.set("key", "initial").unwrap();
		tx.commit().unwrap();
	}

	// Create a write transaction and drop it without commit or cancel
	{
		let mut tx = db.transaction(true);
		tx.set("key", "uncommitted").unwrap();
		tx.set("new_key", "new_value").unwrap();
		// Drop without commit or cancel
	}

	// Verify uncommitted changes are not visible
	let tx = db.transaction(false);
	assert_eq!(
		tx.get("key").unwrap(),
		Some(Bytes::from("initial")),
		"Dropped transaction should not have committed"
	);
	assert!(
		tx.get("new_key").unwrap().is_none(),
		"New key from dropped transaction should not exist"
	);
}

// =============================================================================
// Many Concurrent Transactions Tests
// =============================================================================

#[test]
fn many_concurrent_transactions() {
	let db = Arc::new(Database::new());
	let num_threads = 10;
	let transactions_per_thread = 50;

	let handles: Vec<_> = (0..num_threads)
		.map(|thread_id| {
			let db = Arc::clone(&db);

			thread::spawn(move || {
				for tx_num in 0..transactions_per_thread {
					let key = format!("t{}_{}", thread_id, tx_num);

					// Write transaction
					{
						let mut tx = db.transaction(true);
						tx.set(&key, "value").unwrap();
						tx.commit().unwrap();
					}

					// Immediately read back
					{
						let tx = db.transaction(false);
						let value = tx.get(&key).unwrap();
						assert!(value.is_some(), "Key {} should exist", key);
					}
				}
			})
		})
		.collect();

	// Wait for all threads
	for handle in handles {
		handle.join().unwrap();
	}

	// Verify total key count
	let tx = db.transaction(false);
	let total = tx.scan("t".."u", None, None).unwrap().len();
	assert_eq!(
		total,
		num_threads * transactions_per_thread,
		"Should have {} total keys",
		num_threads * transactions_per_thread
	);
}

// =============================================================================
// Transaction After GC Tests
// =============================================================================

#[test]
fn transaction_after_gc() {
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	)
	.with_gc();

	// Create data
	{
		let mut tx = db.transaction(true);
		tx.set("gc_test_key", "v1").unwrap();
		tx.commit().unwrap();
	}

	// Update multiple times to create versions
	for i in 2..10 {
		let mut tx = db.transaction(true);
		tx.set("gc_test_key", format!("v{}", i)).unwrap();
		tx.commit().unwrap();
	}

	// Trigger GC
	db.run_gc();
	std::thread::sleep(Duration::from_millis(100));

	// New transactions should work normally after GC
	{
		let mut tx = db.transaction(true);
		tx.set("gc_test_key", "post_gc_value").unwrap();
		tx.commit().unwrap();
	}

	// Verify
	let tx = db.transaction(false);
	assert_eq!(
		tx.get("gc_test_key").unwrap(),
		Some(Bytes::from("post_gc_value")),
		"Transaction after GC should work"
	);
}

// =============================================================================
// Read Transaction Longevity Tests
// =============================================================================

#[test]
fn read_transaction_longevity() {
	let db = Arc::new(Database::new());

	// Create initial data
	{
		let mut tx = db.transaction(true);
		for i in 0..100 {
			tx.set(format!("longevity_{:03}", i), format!("v{}", i)).unwrap();
		}
		tx.commit().unwrap();
	}

	// Start a read transaction
	let read_tx = db.transaction(false);

	// Perform initial read
	let initial_scan: Vec<_> = read_tx.scan("longevity_".."longevity_z", None, None).unwrap();
	let initial_count = initial_scan.len();
	assert_eq!(initial_count, 100);

	// Spawn threads that modify the data heavily
	let handles: Vec<_> = (0..5)
		.map(|thread_id| {
			let db = Arc::clone(&db);

			thread::spawn(move || {
				for i in 0..20 {
					let mut tx = db.transaction(true);
					let key = format!("longevity_{:03}", (thread_id * 20 + i) % 100);
					tx.set(&key, format!("modified_by_{}", thread_id)).unwrap();
					tx.commit().unwrap();
				}
			})
		})
		.collect();

	for handle in handles {
		handle.join().unwrap();
	}

	// The read transaction should still see the original snapshot
	let second_scan: Vec<_> = read_tx.scan("longevity_".."longevity_z", None, None).unwrap();
	assert_eq!(second_scan.len(), initial_count, "Read tx should see same count");

	// Values should be original
	for (i, (key, value)) in initial_scan.iter().enumerate() {
		let (second_key, second_value) = &second_scan[i];
		assert_eq!(key, second_key, "Keys should match");
		assert_eq!(value, second_value, "Values should match original snapshot");
	}
}
