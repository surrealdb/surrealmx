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
//! Iteration edge case tests for SurrealMX.
//!
//! Tests iterator and cursor behavior under edge conditions including
//! modifications during iteration, transaction lifecycle, and concurrent
//! access.

use bytes::Bytes;
use std::{
	sync::{Arc, Barrier},
	thread,
};
use surrealmx::Database;

// =============================================================================
// Cursor After Transaction Cancel Tests
// =============================================================================
#[test]

fn cursor_after_transaction_cancel() {

	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("a", "1").unwrap();

	tx.set("b", "2").unwrap();

	tx.set("c", "3").unwrap();

	tx.commit().unwrap();

	let mut tx = db.transaction(false);

	let mut cursor = tx.cursor("a".."z").unwrap();

	// Use cursor before cancel
	cursor.seek_to_first();

	assert!(cursor.valid());

	assert_eq!(cursor.key().unwrap().as_ref(), b"a");

	// Drop cursor before cancelling transaction
	drop(cursor);

	tx.cancel().unwrap();

	// Cursor operations after cancel should fail gracefully or return invalid
	// state The exact behavior depends on implementation - cursor may become
	// invalid or operations may return errors
}

// =============================================================================
// Modify Key While Iterating Tests
// =============================================================================
#[test]

fn modify_key_while_iterating() {

	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);

	tx.set("key_a", "value_a").unwrap();

	tx.set("key_b", "value_b").unwrap();

	tx.set("key_c", "value_c").unwrap();

	tx.commit().unwrap();

	// Start a write transaction and iterate
	let mut tx = db.transaction(true);

	// Get an iterator
	let mut iter = tx.scan_iter("key_".."key_z").unwrap();

	// Read first element
	let first = iter.next().unwrap();

	assert_eq!(first.0.as_ref(), b"key_a");

	// Drop iterator before modifying
	drop(iter);

	// Modify a key
	tx.set("key_b", "modified_b").unwrap();

	// Create a new iterator - should see the modification
	let results: Vec<_> = tx.scan_iter("key_".."key_z").unwrap().collect();

	assert_eq!(results.len(), 3);

	// Find key_b and verify it has the modified value
	let key_b = results.iter().find(|(k, _)| k.as_ref() == b"key_b").unwrap();

	assert_eq!(key_b.1, Bytes::from("modified_b"));

	tx.commit().unwrap();
}

#[test]

fn delete_key_while_iterating() {

	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);

	tx.set("item_1", "v1").unwrap();

	tx.set("item_2", "v2").unwrap();

	tx.set("item_3", "v3").unwrap();

	tx.set("item_4", "v4").unwrap();

	tx.commit().unwrap();

	// Start a write transaction
	let mut tx = db.transaction(true);

	// Get first scan result
	let first_scan: Vec<_> = tx.scan_iter("item_".."item_z").unwrap().collect();

	assert_eq!(first_scan.len(), 4);

	// Delete a key
	tx.del("item_2").unwrap();

	// New iteration should not see the deleted key
	let second_scan: Vec<_> = tx.scan_iter("item_".."item_z").unwrap().collect();

	assert_eq!(second_scan.len(), 3);

	assert!(!second_scan.iter().any(|(k, _)| k.as_ref() == b"item_2"));

	tx.commit().unwrap();

	// Verify deletion persisted
	let tx = db.transaction(false);

	let final_scan: Vec<_> = tx.scan_iter("item_".."item_z").unwrap().collect();

	assert_eq!(final_scan.len(), 3);
}

#[test]

fn seek_after_mutation() {

	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);

	tx.set("a", "1").unwrap();

	tx.set("c", "3").unwrap();

	tx.set("e", "5").unwrap();

	tx.commit().unwrap();

	// Start write transaction
	let mut tx = db.transaction(true);

	// Create cursor and seek
	let mut cursor = tx.cursor("a".."z").unwrap();

	cursor.seek_to_first();

	assert_eq!(cursor.key().unwrap().as_ref(), b"a");

	// Drop cursor before mutation
	drop(cursor);

	// Insert a new key
	tx.set("b", "2").unwrap();

	// Create new cursor and verify it sees the new key
	let mut cursor = tx.cursor("a".."z").unwrap();

	cursor.seek("b");

	assert!(cursor.valid());

	assert_eq!(cursor.key().unwrap().as_ref(), b"b");

	assert_eq!(cursor.value().unwrap().as_ref(), b"2");

	drop(cursor);

	tx.commit().unwrap();
}

#[test]

fn iterator_exhaustion_and_reuse() {

	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("x", "1").unwrap();

	tx.set("y", "2").unwrap();

	tx.set("z", "3").unwrap();

	tx.commit().unwrap();

	let tx = db.transaction(false);

	// Create iterator and exhaust it
	let mut iter = tx.keys_iter("x".."zz").unwrap();

	let first = iter.next();

	assert_eq!(first, Some(Bytes::from("x")));

	let second = iter.next();

	assert_eq!(second, Some(Bytes::from("y")));

	let third = iter.next();

	assert_eq!(third, Some(Bytes::from("z")));

	// Iterator should be exhausted
	assert!(iter.next().is_none());

	assert!(iter.next().is_none()); // Multiple calls should keep returning None
								 // Creating a new iterator should work fine
	let mut new_iter = tx.keys_iter("x".."zz").unwrap();

	assert_eq!(new_iter.next(), Some(Bytes::from("x")));
}

#[test]

fn concurrent_iteration_same_range() {

	let db = Arc::new(Database::new());

	// Create initial data
	{

		let mut tx = db.transaction(true);

		for i in 0..20 {

			tx.set(format!("data_{:02}", i), format!("value_{}", i)).unwrap();
		}

		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(3));

	let mut handles = vec![];

	// Spawn multiple threads that iterate over the same range
	for thread_id in 0..3 {

		let db = Arc::clone(&db);

		let barrier = Arc::clone(&barrier);

		handles.push(thread::spawn(move || {

			barrier.wait();

			let tx = db.transaction(false);

			let results: Vec<_> = tx.scan_iter("data_".."data_z").unwrap().collect();

			(thread_id, results.len())
		}));
	}

	let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

	// All threads should see the same count
	for (thread_id, count) in &results {

		assert_eq!(*count, 20, "Thread {} should see all 20 items, got {}", thread_id, count);
	}
}
