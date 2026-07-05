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

//! Batch operation tests for SurrealMX.
//!
//! Tests `getm()` for multi-key operations.

use bytes::Bytes;
use surrealmx::Database;

// =============================================================================
// getm() Tests
// =============================================================================

#[test]
fn getm_returns_values_in_order() {
	let db = Database::new();

	// Create keys
	let mut tx = db.transaction(true);
	tx.set("key1", "value1").unwrap();
	tx.set("key2", "value2").unwrap();
	tx.set("key3", "value3").unwrap();
	tx.commit().unwrap();

	// Get multiple keys
	let tx = db.transaction(false);
	let keys = vec!["key1", "key2", "key3"];
	let results = tx.getm(keys).unwrap();

	assert_eq!(results.len(), 3);
	assert_eq!(results[0], Some(Bytes::from("value1")));
	assert_eq!(results[1], Some(Bytes::from("value2")));
	assert_eq!(results[2], Some(Bytes::from("value3")));
}

#[test]
fn getm_handles_missing_keys() {
	let db = Database::new();

	// Create only some keys
	let mut tx = db.transaction(true);
	tx.set("key1", "value1").unwrap();
	tx.set("key3", "value3").unwrap();
	tx.commit().unwrap();

	// Get mix of existing and missing keys
	let tx = db.transaction(false);
	let keys = vec!["key1", "key2", "key3", "key4"];
	let results = tx.getm(keys).unwrap();

	assert_eq!(results.len(), 4);
	assert_eq!(results[0], Some(Bytes::from("value1")));
	assert_eq!(results[1], None, "Missing key2 should be None");
	assert_eq!(results[2], Some(Bytes::from("value3")));
	assert_eq!(results[3], None, "Missing key4 should be None");
}

#[test]
fn getm_handles_all_missing_keys() {
	let db = Database::new();

	let tx = db.transaction(false);
	let keys = vec!["missing1", "missing2", "missing3"];
	let results = tx.getm(keys).unwrap();

	assert_eq!(results.len(), 3);
	assert!(results.iter().all(|r| r.is_none()), "All results should be None");
}

#[test]
fn getm_with_empty_keys() {
	let db = Database::new();

	let tx = db.transaction(false);
	let keys: Vec<&str> = vec![];
	let results = tx.getm(keys).unwrap();

	assert!(results.is_empty(), "Empty keys should return empty results");
}

#[test]
fn getm_with_duplicate_keys() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("key", "value").unwrap();
	tx.commit().unwrap();

	// Get same key multiple times
	let tx = db.transaction(false);
	let keys = vec!["key", "key", "key"];
	let results = tx.getm(keys).unwrap();

	assert_eq!(results.len(), 3);
	assert!(results.iter().all(|r| *r == Some(Bytes::from("value"))));
}

#[test]
fn getm_sees_uncommitted_writes() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("key1", "uncommitted1").unwrap();
	tx.set("key2", "uncommitted2").unwrap();

	// getm should see uncommitted writes in same transaction
	let keys = vec!["key1", "key2"];
	let results = tx.getm(keys).unwrap();

	assert_eq!(results[0], Some(Bytes::from("uncommitted1")));
	assert_eq!(results[1], Some(Bytes::from("uncommitted2")));

	tx.cancel().unwrap();
}

#[test]
fn getm_with_deleted_keys() {
	let db = Database::new();

	// Create keys
	let mut tx = db.transaction(true);
	tx.set("key1", "value1").unwrap();
	tx.set("key2", "value2").unwrap();
	tx.commit().unwrap();

	// Delete one key
	let mut tx = db.transaction(true);
	tx.del("key1").unwrap();

	// getm should see delete
	let keys = vec!["key1", "key2"];
	let results = tx.getm(keys).unwrap();

	assert_eq!(results[0], None, "Deleted key should be None");
	assert_eq!(results[1], Some(Bytes::from("value2")));

	tx.commit().unwrap();
}

#[test]
fn getm_large_batch() {
	let db = Database::new();

	// Create many keys
	let count = 1000;
	let mut tx = db.transaction(true);
	for i in 0..count {
		tx.set(format!("key_{:04}", i), format!("value_{}", i)).unwrap();
	}
	tx.commit().unwrap();

	// Get all keys
	let tx = db.transaction(false);
	let keys: Vec<String> = (0..count).map(|i| format!("key_{:04}", i)).collect();
	let results = tx.getm(keys).unwrap();

	assert_eq!(results.len(), count);
	for (i, result) in results.iter().enumerate() {
		assert_eq!(*result, Some(Bytes::from(format!("value_{}", i))));
	}
}

// =============================================================================
// SSI Interaction Tests
// =============================================================================

#[test]
fn getm_tracks_reads_for_ssi() {
	let db = Database::new();

	// Create initial key
	let mut tx = db.transaction(true);
	tx.set("key1", "value1").unwrap();
	tx.commit().unwrap();

	// tx1 reads keys using getm with SSI
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	let keys = vec!["key1", "key2"];
	let _ = tx1.getm(keys).unwrap();

	// tx2 creates key1 (which tx1 read)
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
	tx2.set("key1", "modified").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 tries to commit - should fail due to read conflict
	tx1.set("other", "value").unwrap();
	assert!(tx1.commit().is_err(), "tx1 should fail due to SSI read conflict");
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn getm_with_binary_keys() {
	let db = Database::new();

	// Create keys with binary data
	let key1: Vec<u8> = vec![0x00, 0x01, 0x02];
	let key2: Vec<u8> = vec![0xFF, 0xFE, 0xFD];

	let mut tx = db.transaction(true);
	tx.set(key1.clone(), "binary1").unwrap();
	tx.set(key2.clone(), "binary2").unwrap();
	tx.commit().unwrap();

	// Get binary keys
	let tx = db.transaction(false);
	let keys = vec![key1, key2];
	let results = tx.getm(keys).unwrap();

	assert_eq!(results[0], Some(Bytes::from("binary1")));
	assert_eq!(results[1], Some(Bytes::from("binary2")));
}

#[test]
fn getm_concurrent_with_writes() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key1", "v1").unwrap();
	tx.set("key2", "v1").unwrap();
	tx.commit().unwrap();

	// Start read transaction
	let read_tx = db.transaction(false);

	// Concurrently update
	let mut write_tx = db.transaction(true);
	write_tx.set("key1", "v2").unwrap();
	write_tx.set("key2", "v2").unwrap();
	write_tx.commit().unwrap();

	// Read transaction should see old values (snapshot)
	let keys = vec!["key1", "key2"];
	let results = read_tx.getm(keys).unwrap();

	assert_eq!(results[0], Some(Bytes::from("v1")));
	assert_eq!(results[1], Some(Bytes::from("v1")));
}
