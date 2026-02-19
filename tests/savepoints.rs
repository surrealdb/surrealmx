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

//! Savepoint functionality tests for SurrealMX.
//!
//! Tests `set_savepoint()` and `rollback_to_savepoint()` behavior
//! including nested savepoints and edge cases.

use bytes::Bytes;
use surrealmx::{Database, Error};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::*;

// =============================================================================
// Basic Savepoint Tests
// =============================================================================

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_basic_rollback() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Initial state
	tx.set("key1", "value1").unwrap();
	tx.set("key2", "value2").unwrap();

	// Set savepoint
	tx.set_savepoint().unwrap();

	// Make changes after savepoint
	tx.set("key1", "modified1").unwrap();
	tx.set("key3", "value3").unwrap();

	// Verify changes are visible
	assert_eq!(tx.get("key1").unwrap(), Some(Bytes::from("modified1")));
	assert_eq!(tx.get("key3").unwrap(), Some(Bytes::from("value3")));

	// Rollback to savepoint
	tx.rollback_to_savepoint().unwrap();

	// Verify rollback - key1 should be original, key3 should not exist
	assert_eq!(
		tx.get("key1").unwrap(),
		Some(Bytes::from("value1")),
		"key1 should be restored to original value"
	);
	assert_eq!(tx.get("key2").unwrap(), Some(Bytes::from("value2")), "key2 should be unchanged");
	assert_eq!(tx.get("key3").unwrap(), None, "key3 should not exist after rollback");

	// Commit and verify final state
	tx.commit().unwrap();

	let mut verify_tx = db.transaction(false);
	assert_eq!(verify_tx.get("key1").unwrap(), Some(Bytes::from("value1")));
	assert_eq!(verify_tx.get("key2").unwrap(), Some(Bytes::from("value2")));
	assert_eq!(verify_tx.get("key3").unwrap(), None);
	verify_tx.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_rollback_restores_deleted_keys() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Create initial keys
	tx.set("keep", "keeper").unwrap();
	tx.set("delete_me", "will_be_deleted").unwrap();

	// Set savepoint
	tx.set_savepoint().unwrap();

	// Delete a key
	tx.del("delete_me").unwrap();

	// Verify it's deleted
	assert_eq!(tx.get("delete_me").unwrap(), None, "Key should be deleted");

	// Rollback
	tx.rollback_to_savepoint().unwrap();

	// Key should be restored
	assert_eq!(
		tx.get("delete_me").unwrap(),
		Some(Bytes::from("will_be_deleted")),
		"Deleted key should be restored after rollback"
	);

	tx.commit().unwrap();

	// Verify persistence
	let mut verify = db.transaction(false);
	assert_eq!(verify.get("delete_me").unwrap(), Some(Bytes::from("will_be_deleted")));
	verify.cancel().unwrap();
}

// =============================================================================
// Nested Savepoint Tests
// =============================================================================

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_nested_multiple_levels() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Level 0: Initial state
	tx.set("level", "0").unwrap();

	// Level 1 savepoint
	tx.set_savepoint().unwrap();
	tx.set("level", "1").unwrap();
	tx.set("added_at_1", "value1").unwrap();

	// Level 2 savepoint
	tx.set_savepoint().unwrap();
	tx.set("level", "2").unwrap();
	tx.set("added_at_2", "value2").unwrap();

	// Level 3 savepoint
	tx.set_savepoint().unwrap();
	tx.set("level", "3").unwrap();
	tx.set("added_at_3", "value3").unwrap();

	// Verify we're at level 3
	assert_eq!(tx.get("level").unwrap(), Some(Bytes::from("3")));
	assert!(tx.get("added_at_3").unwrap().is_some());

	// Rollback to level 2
	tx.rollback_to_savepoint().unwrap();
	assert_eq!(tx.get("level").unwrap(), Some(Bytes::from("2")));
	assert!(tx.get("added_at_3").unwrap().is_none());
	assert!(tx.get("added_at_2").unwrap().is_some());

	// Rollback to level 1
	tx.rollback_to_savepoint().unwrap();
	assert_eq!(tx.get("level").unwrap(), Some(Bytes::from("1")));
	assert!(tx.get("added_at_2").unwrap().is_none());
	assert!(tx.get("added_at_1").unwrap().is_some());

	// Rollback to level 0
	tx.rollback_to_savepoint().unwrap();
	assert_eq!(tx.get("level").unwrap(), Some(Bytes::from("0")));
	assert!(tx.get("added_at_1").unwrap().is_none());

	tx.commit().unwrap();

	// Verify final state
	let mut verify = db.transaction(false);
	assert_eq!(verify.get("level").unwrap(), Some(Bytes::from("0")));
	verify.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_partial_nested_rollback() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("base", "base_value").unwrap();

	// First savepoint
	tx.set_savepoint().unwrap();
	tx.set("sp1_key", "sp1_value").unwrap();

	// Second savepoint
	tx.set_savepoint().unwrap();
	tx.set("sp2_key", "sp2_value").unwrap();

	// Third savepoint
	tx.set_savepoint().unwrap();
	tx.set("sp3_key", "sp3_value").unwrap();

	// Rollback only the third savepoint
	tx.rollback_to_savepoint().unwrap();

	// sp3_key should be gone, others should remain
	assert!(tx.get("sp3_key").unwrap().is_none());
	assert!(tx.get("sp2_key").unwrap().is_some());
	assert!(tx.get("sp1_key").unwrap().is_some());
	assert!(tx.get("base").unwrap().is_some());

	// Commit with sp1 and sp2 changes
	tx.commit().unwrap();

	let mut verify = db.transaction(false);
	assert_eq!(verify.get("base").unwrap(), Some(Bytes::from("base_value")));
	assert_eq!(verify.get("sp1_key").unwrap(), Some(Bytes::from("sp1_value")));
	assert_eq!(verify.get("sp2_key").unwrap(), Some(Bytes::from("sp2_value")));
	assert!(verify.get("sp3_key").unwrap().is_none());
	verify.cancel().unwrap();
}

// =============================================================================
// Savepoint Edge Cases
// =============================================================================

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_rollback_without_savepoint_errors() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("key", "value").unwrap();

	// Try to rollback without setting a savepoint
	let result = tx.rollback_to_savepoint();
	assert!(matches!(result, Err(Error::NoSavepoint)), "Should error when no savepoint exists");

	// Transaction should still be usable
	tx.set("key2", "value2").unwrap();
	tx.commit().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_on_read_transaction() {
	let db = Database::new();

	// Create some data first
	let mut setup_tx = db.transaction(true);
	setup_tx.set("key", "value").unwrap();
	setup_tx.commit().unwrap();

	// Try savepoint on read-only transaction
	let mut read_tx = db.transaction(false);

	// Note: set_savepoint might succeed on read-only tx (depends on implementation)
	// but rollback should work without issues
	if read_tx.set_savepoint().is_ok() {
		// Read some data
		let _ = read_tx.get("key").unwrap();
		// Rollback should be fine (no changes anyway)
		let _ = read_tx.rollback_to_savepoint();
	}

	read_tx.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_modify_after_rollback_then_commit() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("original", "value").unwrap();

	tx.set_savepoint().unwrap();
	tx.set("temp", "will_be_rolled_back").unwrap();
	tx.rollback_to_savepoint().unwrap();

	// Make new modifications after rollback
	tx.set("new_key", "new_value").unwrap();
	tx.set("original", "modified").unwrap();

	tx.commit().unwrap();

	let mut verify = db.transaction(false);
	assert_eq!(verify.get("original").unwrap(), Some(Bytes::from("modified")));
	assert_eq!(verify.get("new_key").unwrap(), Some(Bytes::from("new_value")));
	assert!(verify.get("temp").unwrap().is_none());
	verify.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_set_same_key_multiple_times() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("counter", "0").unwrap();
	tx.set_savepoint().unwrap();

	// Update same key multiple times
	tx.set("counter", "1").unwrap();
	tx.set("counter", "2").unwrap();
	tx.set("counter", "3").unwrap();

	assert_eq!(tx.get("counter").unwrap(), Some(Bytes::from("3")));

	tx.rollback_to_savepoint().unwrap();

	// Should go back to "0", not any intermediate value
	assert_eq!(tx.get("counter").unwrap(), Some(Bytes::from("0")));

	tx.commit().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_empty_savepoint_rollback() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("before", "value").unwrap();

	// Set savepoint but make no changes
	tx.set_savepoint().unwrap();

	// Rollback empty savepoint
	tx.rollback_to_savepoint().unwrap();

	// Original data should still be there
	assert_eq!(tx.get("before").unwrap(), Some(Bytes::from("value")));

	tx.commit().unwrap();

	let mut verify = db.transaction(false);
	assert_eq!(verify.get("before").unwrap(), Some(Bytes::from("value")));
	verify.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_with_scan_operations() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Create initial range
	tx.set("a", "1").unwrap();
	tx.set("b", "2").unwrap();
	tx.set("c", "3").unwrap();

	tx.set_savepoint().unwrap();

	// Add more keys and modify existing
	tx.set("b", "modified").unwrap();
	tx.set("d", "4").unwrap();
	tx.set("e", "5").unwrap();

	// Verify scan sees all changes
	let scan_result = tx.scan("a".."z", None, None).unwrap();
	assert_eq!(scan_result.len(), 5);

	tx.rollback_to_savepoint().unwrap();

	// Scan should now see original state
	let scan_result = tx.scan("a".."z", None, None).unwrap();
	assert_eq!(scan_result.len(), 3);
	assert_eq!(scan_result[1].1.as_ref(), b"2"); // b should be original value

	tx.commit().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_delete_then_recreate_key() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "original").unwrap();
	tx.set_savepoint().unwrap();

	// Delete and recreate
	tx.del("key").unwrap();
	tx.set("key", "recreated").unwrap();

	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("recreated")));

	tx.rollback_to_savepoint().unwrap();

	// Should be back to original
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("original")));

	tx.commit().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_stress_many_savepoints() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	let num_savepoints = 100;

	// Create many nested savepoints
	for i in 0..num_savepoints {
		tx.set_savepoint().unwrap();
		tx.set(format!("key_{}", i), format!("value_{}", i)).unwrap();
	}

	// Verify all keys exist
	for i in 0..num_savepoints {
		assert!(tx.get(format!("key_{}", i)).unwrap().is_some());
	}

	// Rollback half of them
	for _ in 0..(num_savepoints / 2) {
		tx.rollback_to_savepoint().unwrap();
	}

	// First half should still exist
	for i in 0..(num_savepoints / 2) {
		assert!(tx.get(format!("key_{}", i)).unwrap().is_some(), "key_{} should exist", i);
	}

	// Second half should not exist
	for i in (num_savepoints / 2)..num_savepoints {
		assert!(tx.get(format!("key_{}", i)).unwrap().is_none(), "key_{} should not exist", i);
	}

	tx.commit().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_preserves_existing_data() {
	let db = Database::new();

	// Create initial data
	let mut setup = db.transaction(true);
	setup.set("existing1", "value1").unwrap();
	setup.set("existing2", "value2").unwrap();
	setup.commit().unwrap();

	// New transaction with savepoint
	let mut tx = db.transaction(true);

	// Read existing data
	assert_eq!(tx.get("existing1").unwrap(), Some(Bytes::from("value1")));

	tx.set_savepoint().unwrap();

	// Modify existing and add new
	tx.set("existing1", "modified").unwrap();
	tx.set("new_key", "new_value").unwrap();

	tx.rollback_to_savepoint().unwrap();

	// Existing data should still be visible (from database)
	assert_eq!(tx.get("existing1").unwrap(), Some(Bytes::from("value1")));
	assert_eq!(tx.get("existing2").unwrap(), Some(Bytes::from("value2")));
	assert!(tx.get("new_key").unwrap().is_none());

	tx.commit().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_with_conditional_operations() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "initial").unwrap();
	tx.set_savepoint().unwrap();

	// Use conditional put - should fail because key exists
	let result = tx.put("key", "should_fail");
	assert!(result.is_err());

	// Key should still have initial value
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("initial")));

	// Modify with set
	tx.set("key", "modified").unwrap();

	tx.rollback_to_savepoint().unwrap();

	// Should be back to initial
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("initial")));

	tx.commit().unwrap();
}
