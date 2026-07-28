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

//! Savepoint functionality tests for `SurrealMX`.
//!
//! Tests `set_savepoint()`, `rollback_to_savepoint()` and
//! `release_savepoint()` behavior including nested savepoints, conflict
//! detection and edge cases.

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
		tx.set(format!("key_{i}"), format!("value_{i}")).unwrap();
	}

	// Verify all keys exist
	for i in 0..num_savepoints {
		assert!(tx.get(format!("key_{i}")).unwrap().is_some());
	}

	// Rollback half of them
	for _ in 0..(num_savepoints / 2) {
		tx.rollback_to_savepoint().unwrap();
	}

	// First half should still exist
	for i in 0..(num_savepoints / 2) {
		assert!(tx.get(format!("key_{i}")).unwrap().is_some(), "key_{i} should exist");
	}

	// Second half should not exist
	for i in (num_savepoints / 2)..num_savepoints {
		assert!(tx.get(format!("key_{i}")).unwrap().is_none(), "key_{i} should not exist");
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

// =============================================================================
// Savepoint Release Tests
// =============================================================================

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_release_keeps_writes() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key1", "value1").unwrap();

	// Set a savepoint, write, then release it
	tx.set_savepoint().unwrap();
	tx.set("key2", "value2").unwrap();
	tx.release_savepoint().unwrap();

	// Both writes should still be visible
	assert_eq!(tx.get("key1").unwrap(), Some(Bytes::from("value1")));
	assert_eq!(
		tx.get("key2").unwrap(),
		Some(Bytes::from("value2")),
		"a released scope's write should be kept"
	);

	tx.commit().unwrap();

	// And both should be persisted
	let mut verify_tx = db.transaction(false);
	assert_eq!(verify_tx.get("key1").unwrap(), Some(Bytes::from("value1")));
	assert_eq!(verify_tx.get("key2").unwrap(), Some(Bytes::from("value2")));
	verify_tx.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_release_then_rollback_goes_to_enclosing_savepoint() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Outer savepoint and write
	tx.set_savepoint().unwrap();
	tx.set("outer", "value").unwrap();

	// Inner savepoint and write
	tx.set_savepoint().unwrap();
	tx.set("inner", "value").unwrap();

	// Releasing the inner savepoint keeps its write
	tx.release_savepoint().unwrap();
	assert_eq!(tx.get("inner").unwrap(), Some(Bytes::from("value")));

	// A single rollback unwinds past the released savepoint to the enclosing
	// one, so the released scope's write is undone along with the outer one
	tx.rollback_to_savepoint().unwrap();
	assert_eq!(
		tx.get("inner").unwrap(),
		None,
		"a released scope's write must remain undoable by the enclosing savepoint"
	);
	assert_eq!(tx.get("outer").unwrap(), None, "the enclosing scope's write should be undone");

	// The stack should now be empty
	assert!(matches!(tx.rollback_to_savepoint(), Err(Error::NoSavepoint)));

	tx.commit().unwrap();

	let mut verify_tx = db.transaction(false);
	assert_eq!(verify_tx.get("inner").unwrap(), None);
	assert_eq!(verify_tx.get("outer").unwrap(), None);
	verify_tx.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_release_all_then_commit() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set_savepoint().unwrap();
	tx.set("key1", "value1").unwrap();
	tx.set_savepoint().unwrap();
	tx.set("key2", "value2").unwrap();

	// Release every savepoint
	tx.release_savepoint().unwrap();
	tx.release_savepoint().unwrap();
	assert!(matches!(tx.release_savepoint(), Err(Error::NoSavepoint)));

	tx.commit().unwrap();

	// Everything should be persisted
	let mut verify_tx = db.transaction(false);
	assert_eq!(verify_tx.get("key1").unwrap(), Some(Bytes::from("value1")));
	assert_eq!(verify_tx.get("key2").unwrap(), Some(Bytes::from("value2")));
	verify_tx.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_release_and_rollback_interleaved() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Outer savepoint and write
	tx.set_savepoint().unwrap();
	tx.set("keep", "value").unwrap();

	// Inner savepoint and write, rolled back
	tx.set_savepoint().unwrap();
	tx.set("discard", "value").unwrap();
	tx.rollback_to_savepoint().unwrap();

	// Releasing the outer savepoint keeps the surviving write
	tx.release_savepoint().unwrap();

	tx.commit().unwrap();

	let mut verify_tx = db.transaction(false);
	assert_eq!(verify_tx.get("keep").unwrap(), Some(Bytes::from("value")));
	assert_eq!(verify_tx.get("discard").unwrap(), None);
	verify_tx.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_release_many_cycles() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	let num_cycles = 500;

	// A savepoint per unit of work, each one released
	for i in 0..num_cycles {
		tx.set_savepoint().unwrap();
		tx.set(format!("key_{}", i), format!("value_{}", i)).unwrap();
		tx.release_savepoint().unwrap();
	}

	tx.commit().unwrap();

	// Every write should have survived
	let mut verify_tx = db.transaction(false);
	for i in 0..num_cycles {
		assert_eq!(
			verify_tx.get(format!("key_{}", i)).unwrap(),
			Some(Bytes::from(format!("value_{}", i))),
			"key_{} should be committed",
			i
		);
	}
	verify_tx.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_release_deep_then_rollback() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	let num_savepoints = 100;

	// Create many nested savepoints, each with a write
	for i in 0..num_savepoints {
		tx.set_savepoint().unwrap();
		tx.set(format!("key_{}", i), "value").unwrap();
	}

	// Release the top half, keeping their writes
	for _ in 0..(num_savepoints / 2) {
		tx.release_savepoint().unwrap();
	}

	// All keys should still be present
	for i in 0..num_savepoints {
		assert!(tx.get(format!("key_{}", i)).unwrap().is_some(), "key_{} should exist", i);
	}

	// A single rollback unwinds to the deepest surviving savepoint, which is the
	// one set before the first key of the released half
	tx.rollback_to_savepoint().unwrap();

	// The first half's writes remain, minus the one made in the scope we just
	// rolled back into
	for i in 0..(num_savepoints / 2 - 1) {
		assert!(tx.get(format!("key_{}", i)).unwrap().is_some(), "key_{} should exist", i);
	}

	// Everything from the rolled back scope onwards is gone
	for i in (num_savepoints / 2 - 1)..num_savepoints {
		assert!(tx.get(format!("key_{}", i)).unwrap().is_none(), "key_{} should not exist", i);
	}

	tx.commit().unwrap();
}

// =============================================================================
// Savepoint Conflict Detection Tests
// =============================================================================

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_release_still_detects_read_conflict() {
	let db = Database::new();

	// Setup
	let mut tx_setup = db.transaction(true);
	tx_setup.set("read", "initial").unwrap();
	tx_setup.set("other", "other").unwrap();
	tx_setup.commit().unwrap();

	let mut tx1 = db.transaction(true);

	// Read a key inside a savepoint scope, then release the scope
	tx1.set_savepoint().unwrap();
	assert_eq!(tx1.get("read").unwrap(), Some(Bytes::from("initial")));
	tx1.release_savepoint().unwrap();

	// Write elsewhere, based on what was read
	tx1.set("other", "derived").unwrap();

	// A concurrent transaction modifies the key that tx1 read
	let mut tx2 = db.transaction(true);
	tx2.set("read", "changed").unwrap();
	tx2.commit().unwrap();

	// tx1 must abort
	let result = tx1.commit();
	assert!(
		matches!(result, Err(Error::KeyReadConflict)),
		"Should detect a read conflict on a key read inside a released scope, got: {:?}",
		result
	);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_release_without_conflict_commits() {
	// Negative control for `savepoint_release_still_detects_read_conflict`:
	// without this, that test could pass for the wrong reason.

	let db = Database::new();

	// Setup
	let mut tx_setup = db.transaction(true);
	tx_setup.set("read", "initial").unwrap();
	tx_setup.set("other", "other").unwrap();
	tx_setup.commit().unwrap();

	let mut tx1 = db.transaction(true);

	// Read a key inside a savepoint scope, then release the scope
	tx1.set_savepoint().unwrap();
	assert_eq!(tx1.get("read").unwrap(), Some(Bytes::from("initial")));
	tx1.release_savepoint().unwrap();

	tx1.set("other", "derived").unwrap();

	// A concurrent transaction modifies an unrelated key
	let mut tx2 = db.transaction(true);
	tx2.set("unrelated", "changed").unwrap();
	tx2.commit().unwrap();

	// tx1 should commit cleanly
	tx1.commit().unwrap();

	let mut verify_tx = db.transaction(false);
	assert_eq!(verify_tx.get("other").unwrap(), Some(Bytes::from("derived")));
	verify_tx.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn savepoint_rollback_still_detects_read_conflict_from_rolled_back_scope() {
	// Reads are tracked monotonically, so a value read inside a scope that was
	// rolled back still counts for conflict detection. The application may have
	// based a surviving write on the value it saw.

	let db = Database::new();

	// Setup
	let mut tx_setup = db.transaction(true);
	tx_setup.set("read", "initial").unwrap();
	tx_setup.set("other", "other").unwrap();
	tx_setup.commit().unwrap();

	let mut tx1 = db.transaction(true);

	// Read a key inside a savepoint scope, then roll the scope back
	tx1.set_savepoint().unwrap();
	assert_eq!(tx1.get("read").unwrap(), Some(Bytes::from("initial")));
	tx1.rollback_to_savepoint().unwrap();

	// Write elsewhere, based on what was read
	tx1.set("other", "derived").unwrap();

	// A concurrent transaction modifies the key that tx1 read
	let mut tx2 = db.transaction(true);
	tx2.set("read", "changed").unwrap();
	tx2.commit().unwrap();

	// tx1 must abort
	let result = tx1.commit();
	assert!(
		matches!(result, Err(Error::KeyReadConflict)),
		"Should detect a read conflict on a key read inside a rolled back scope, got: {:?}",
		result
	);
}
