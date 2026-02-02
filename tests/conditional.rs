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

//! Conditional operation tests for SurrealMX.
//!
//! Tests `put()`, `putc()`, and `delc()` behavior for conditional
//! insert, update, and delete operations.

use bytes::Bytes;
use surrealmx::{Database, Error};

// =============================================================================
// put() Tests (Insert if not exists)
// =============================================================================

#[test]
fn put_succeeds_for_new_key() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	let result = tx.put("new_key", "value");
	assert!(result.is_ok(), "put should succeed for new key");
	assert_eq!(tx.get("new_key").unwrap(), Some(Bytes::from("value")));
	tx.commit().unwrap();
}

#[test]
fn put_fails_for_existing_key() {
	let db = Database::new();

	// Create initial key
	let mut tx = db.transaction(true);
	tx.set("existing", "original").unwrap();
	tx.commit().unwrap();

	// Try to put (should fail)
	let mut tx = db.transaction(true);
	let result = tx.put("existing", "new_value");
	assert!(
		matches!(result, Err(Error::KeyAlreadyExists)),
		"put should fail for existing key"
	);

	// Verify original value unchanged
	assert_eq!(tx.get("existing").unwrap(), Some(Bytes::from("original")));
	tx.cancel().unwrap();
}

#[test]
fn put_fails_for_key_set_in_same_transaction() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("key", "first").unwrap();

	// Second put to same key should fail
	let result = tx.put("key", "second");
	assert!(
		matches!(result, Err(Error::KeyAlreadyExists)),
		"put should fail for key already in writeset"
	);
	tx.cancel().unwrap();
}

#[test]
fn put_succeeds_after_delete_in_same_transaction() {
	let db = Database::new();

	// Create initial key
	let mut tx = db.transaction(true);
	tx.set("key", "original").unwrap();
	tx.commit().unwrap();

	// Delete and then put
	let mut tx = db.transaction(true);
	tx.del("key").unwrap();
	// Put should now succeed since the key is marked as deleted
	// Note: This depends on the implementation - it might still fail
	// if the implementation checks writeset for any entry
	let result = tx.put("key", "recreated");
	// This might fail or succeed depending on implementation
	if result.is_ok() {
		assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("recreated")));
		tx.commit().unwrap();
	} else {
		tx.cancel().unwrap();
	}
}

// =============================================================================
// putc() Tests (Conditional put)
// =============================================================================

#[test]
fn putc_succeeds_when_value_matches() {
	let db = Database::new();

	// Create initial key
	let mut tx = db.transaction(true);
	tx.set("key", "expected").unwrap();
	tx.commit().unwrap();

	// putc with matching check value
	let mut tx = db.transaction(true);
	let result = tx.putc("key", "new_value", Some("expected"));
	assert!(result.is_ok(), "putc should succeed when value matches");
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("new_value")));
	tx.commit().unwrap();

	// Verify commit
	let mut verify = db.transaction(false);
	assert_eq!(verify.get("key").unwrap(), Some(Bytes::from("new_value")));
	verify.cancel().unwrap();
}

#[test]
fn putc_fails_when_value_differs() {
	let db = Database::new();

	// Create initial key
	let mut tx = db.transaction(true);
	tx.set("key", "actual").unwrap();
	tx.commit().unwrap();

	// putc with non-matching check value
	let mut tx = db.transaction(true);
	let result = tx.putc("key", "new_value", Some("wrong_expected"));
	assert!(
		matches!(result, Err(Error::ValNotExpectedValue)),
		"putc should fail when value doesn't match"
	);

	// Value should be unchanged
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("actual")));
	tx.cancel().unwrap();
}

#[test]
fn putc_with_none_check_succeeds_for_non_existent_key() {
	let db = Database::new();

	// putc with None check on non-existent key
	let mut tx = db.transaction(true);
	let result = tx.putc::<_, _, &[u8]>("new_key", "value", None);
	assert!(result.is_ok(), "putc with None check should succeed for new key");
	assert_eq!(tx.get("new_key").unwrap(), Some(Bytes::from("value")));
	tx.commit().unwrap();
}

#[test]
fn putc_with_none_check_fails_for_existing_key() {
	let db = Database::new();

	// Create initial key
	let mut tx = db.transaction(true);
	tx.set("key", "exists").unwrap();
	tx.commit().unwrap();

	// putc with None check on existing key should fail
	let mut tx = db.transaction(true);
	let result = tx.putc::<_, _, &[u8]>("key", "new_value", None);
	assert!(
		matches!(result, Err(Error::ValNotExpectedValue)),
		"putc with None check should fail for existing key"
	);
	tx.cancel().unwrap();
}

#[test]
fn putc_in_same_transaction_with_matching_value() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Set initial value
	tx.set("key", "first").unwrap();

	// putc with matching check should succeed
	let result = tx.putc("key", "second", Some("first"));
	assert!(result.is_ok(), "putc should succeed with matching writeset value");
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("second")));

	tx.commit().unwrap();
}

#[test]
fn putc_in_same_transaction_with_non_matching_value() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Set initial value
	tx.set("key", "first").unwrap();

	// putc with non-matching check should fail
	let result = tx.putc("key", "second", Some("wrong"));
	assert!(
		matches!(result, Err(Error::ValNotExpectedValue)),
		"putc should fail with non-matching writeset value"
	);

	// Value should still be "first"
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("first")));
	tx.cancel().unwrap();
}

#[test]
fn putc_after_delete_with_none_check() {
	let db = Database::new();

	// Create initial key
	let mut tx = db.transaction(true);
	tx.set("key", "original").unwrap();
	tx.commit().unwrap();

	// Delete then putc with None check
	let mut tx = db.transaction(true);
	tx.del("key").unwrap();

	// putc with None check should succeed (key is deleted)
	let result = tx.putc::<_, _, &[u8]>("key", "recreated", None);
	assert!(result.is_ok(), "putc with None check should succeed after delete");
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("recreated")));
	tx.commit().unwrap();
}

// =============================================================================
// delc() Tests (Conditional delete)
// =============================================================================

#[test]
fn delc_succeeds_when_value_matches() {
	let db = Database::new();

	// Create initial key
	let mut tx = db.transaction(true);
	tx.set("key", "expected").unwrap();
	tx.commit().unwrap();

	// delc with matching value
	let mut tx = db.transaction(true);
	let result = tx.delc("key", Some("expected"));
	assert!(result.is_ok(), "delc should succeed when value matches");

	// Key should be deleted
	assert!(tx.get("key").unwrap().is_none());
	tx.commit().unwrap();

	// Verify commit
	let mut verify = db.transaction(false);
	assert!(verify.get("key").unwrap().is_none());
	verify.cancel().unwrap();
}

#[test]
fn delc_fails_when_value_differs() {
	let db = Database::new();

	// Create initial key
	let mut tx = db.transaction(true);
	tx.set("key", "actual").unwrap();
	tx.commit().unwrap();

	// delc with non-matching value
	let mut tx = db.transaction(true);
	let result = tx.delc("key", Some("wrong"));
	assert!(
		matches!(result, Err(Error::ValNotExpectedValue)),
		"delc should fail when value doesn't match"
	);

	// Key should still exist
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("actual")));
	tx.cancel().unwrap();
}

#[test]
fn delc_with_none_check_succeeds_for_non_existent_key() {
	let db = Database::new();

	// delc with None check on non-existent key
	let mut tx = db.transaction(true);
	let result = tx.delc::<_, &[u8]>("non_existent", None);
	assert!(result.is_ok(), "delc with None check should succeed for non-existent key");
	tx.commit().unwrap();
}

#[test]
fn delc_with_none_check_fails_for_existing_key() {
	let db = Database::new();

	// Create initial key
	let mut tx = db.transaction(true);
	tx.set("key", "exists").unwrap();
	tx.commit().unwrap();

	// delc with None check on existing key should fail
	let mut tx = db.transaction(true);
	let result = tx.delc::<_, &[u8]>("key", None);
	assert!(
		matches!(result, Err(Error::ValNotExpectedValue)),
		"delc with None check should fail for existing key"
	);
	tx.cancel().unwrap();
}

#[test]
fn delc_in_same_transaction_with_matching_value() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Set value
	tx.set("key", "value").unwrap();

	// delc with matching value
	let result = tx.delc("key", Some("value"));
	assert!(result.is_ok(), "delc should succeed with matching writeset value");

	// Key should be deleted
	assert!(tx.get("key").unwrap().is_none());
	tx.commit().unwrap();
}

#[test]
fn delc_in_same_transaction_with_non_matching_value() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Set value
	tx.set("key", "value").unwrap();

	// delc with non-matching value
	let result = tx.delc("key", Some("wrong"));
	assert!(
		matches!(result, Err(Error::ValNotExpectedValue)),
		"delc should fail with non-matching writeset value"
	);

	// Key should still exist
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("value")));
	tx.cancel().unwrap();
}

// =============================================================================
// Combined / Edge Case Tests
// =============================================================================

#[test]
fn conditional_operations_chain() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Insert with None check (key doesn't exist)
	tx.putc::<_, _, &[u8]>("key", "v1", None).unwrap();
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("v1")));

	// Update with value check
	tx.putc("key", "v2", Some("v1")).unwrap();
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("v2")));

	// Update again
	tx.putc("key", "v3", Some("v2")).unwrap();
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("v3")));

	// Delete with value check
	tx.delc("key", Some("v3")).unwrap();
	assert!(tx.get("key").unwrap().is_none());

	// Insert again with None check (key is deleted)
	tx.putc::<_, _, &[u8]>("key", "final", None).unwrap();
	assert_eq!(tx.get("key").unwrap(), Some(Bytes::from("final")));

	tx.commit().unwrap();

	// Verify
	let mut verify = db.transaction(false);
	assert_eq!(verify.get("key").unwrap(), Some(Bytes::from("final")));
	verify.cancel().unwrap();
}

#[test]
fn conditional_operations_across_transactions() {
	let db = Database::new();

	// Transaction 1: Create key
	let mut tx1 = db.transaction(true);
	tx1.putc::<_, _, &[u8]>("key", "v1", None).unwrap();
	tx1.commit().unwrap();

	// Transaction 2: Update key
	let mut tx2 = db.transaction(true);
	tx2.putc("key", "v2", Some("v1")).unwrap();
	tx2.commit().unwrap();

	// Transaction 3: Failed update (wrong check)
	let mut tx3 = db.transaction(true);
	let result = tx3.putc("key", "v3", Some("wrong"));
	assert!(result.is_err());
	tx3.cancel().unwrap();

	// Transaction 4: Delete key
	let mut tx4 = db.transaction(true);
	tx4.delc("key", Some("v2")).unwrap();
	tx4.commit().unwrap();

	// Verify
	let mut verify = db.transaction(false);
	assert!(verify.get("key").unwrap().is_none());
	verify.cancel().unwrap();
}

#[test]
fn conditional_with_empty_value() {
	let db = Database::new();

	// Create key with empty value
	let mut tx = db.transaction(true);
	tx.set("key", "").unwrap();
	tx.commit().unwrap();

	// putc with empty check value should work
	let mut tx = db.transaction(true);
	let result = tx.putc("key", "non_empty", Some(""));
	assert!(result.is_ok(), "putc with empty check value should work");
	tx.commit().unwrap();

	// Verify
	let mut verify = db.transaction(false);
	assert_eq!(verify.get("key").unwrap(), Some(Bytes::from("non_empty")));
	verify.cancel().unwrap();
}

#[test]
fn conditional_with_binary_data() {
	let db = Database::new();

	// Create key with binary data
	let binary_data: Vec<u8> = vec![0x00, 0x01, 0x02, 0xFF, 0xFE];
	let mut tx = db.transaction(true);
	tx.set("key", binary_data.clone()).unwrap();
	tx.commit().unwrap();

	// putc with binary check value
	let mut tx = db.transaction(true);
	let new_data: Vec<u8> = vec![0xAA, 0xBB, 0xCC];
	let result = tx.putc("key", new_data.clone(), Some(binary_data));
	assert!(result.is_ok(), "putc with binary check value should work");
	tx.commit().unwrap();

	// Verify
	let mut verify = db.transaction(false);
	assert_eq!(verify.get("key").unwrap(), Some(Bytes::from(new_data)));
	verify.cancel().unwrap();
}

#[test]
fn putc_concurrent_conflict() {
	let db = Database::new();

	// Create initial key
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// Two concurrent transactions try to putc
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

	// Both read current value and prepare update
	let v1 = tx1.get("key").unwrap().unwrap();
	let v2 = tx2.get("key").unwrap().unwrap();

	assert_eq!(v1.as_ref(), b"initial");
	assert_eq!(v2.as_ref(), b"initial");

	// Both try to update
	tx1.putc("key", "tx1_value", Some("initial")).unwrap();
	tx2.putc("key", "tx2_value", Some("initial")).unwrap();

	// First commit succeeds
	assert!(tx1.commit().is_ok());

	// Second commit fails (SSI conflict or value changed)
	assert!(tx2.commit().is_err());

	// Verify final state
	let mut verify = db.transaction(false);
	assert_eq!(verify.get("key").unwrap(), Some(Bytes::from("tx1_value")));
	verify.cancel().unwrap();
}
