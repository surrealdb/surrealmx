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

//! Error handling tests for SurrealMX.
//!
//! Tests error conditions and proper error handling behavior.

use surrealmx::{Database, Error};

// =============================================================================
// Transaction Closed Errors
// =============================================================================

#[test]

fn operations_after_commit_fail() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	tx.commit().unwrap();

	// All operations after commit should fail
	assert!(matches!(tx.get("key"), Err(Error::TxClosed)));

	assert!(matches!(tx.set("key2", "value2"), Err(Error::TxClosed)));

	assert!(matches!(tx.put("key3", "value3"), Err(Error::TxClosed)));

	assert!(matches!(tx.del("key"), Err(Error::TxClosed)));

	assert!(matches!(tx.exists("key"), Err(Error::TxClosed)));

	assert!(matches!(tx.commit(), Err(Error::TxClosed)));

	assert!(matches!(tx.cancel(), Err(Error::TxClosed)));
}

#[test]

fn operations_after_cancel_fail() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	tx.cancel().unwrap();

	// All operations after cancel should fail
	assert!(matches!(tx.get("key"), Err(Error::TxClosed)));

	assert!(matches!(tx.set("key2", "value2"), Err(Error::TxClosed)));

	assert!(matches!(tx.commit(), Err(Error::TxClosed)));

	assert!(matches!(tx.cancel(), Err(Error::TxClosed)));
}

#[test]

fn double_commit_fails() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	assert!(tx.commit().is_ok(), "First commit should succeed");

	assert!(matches!(tx.commit(), Err(Error::TxClosed)), "Second commit should fail with TxClosed");
}

#[test]

fn double_cancel_fails() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	assert!(tx.cancel().is_ok(), "First cancel should succeed");

	assert!(matches!(tx.cancel(), Err(Error::TxClosed)), "Second cancel should fail with TxClosed");
}

#[test]

fn commit_after_cancel_fails() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	tx.cancel().unwrap();

	assert!(matches!(tx.commit(), Err(Error::TxClosed)), "Commit after cancel should fail");
}

#[test]

fn cancel_after_commit_fails() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	tx.commit().unwrap();

	assert!(matches!(tx.cancel(), Err(Error::TxClosed)), "Cancel after commit should fail");
}

// =============================================================================
// Not Writable Errors
// =============================================================================

#[test]

fn write_operations_on_read_transaction_fail() {
	let db = Database::new();

	// Create some data first
	{
		let mut tx = db.transaction(true);

		tx.set("existing_key", "value").unwrap();

		tx.commit().unwrap();
	}

	// Start read-only transaction
	let mut read_tx = db.transaction(false);

	// All write operations should fail
	assert!(
		matches!(read_tx.set("key", "value"), Err(Error::TxNotWritable)),
		"set on read tx should fail"
	);

	assert!(
		matches!(read_tx.put("key", "value"), Err(Error::TxNotWritable)),
		"put on read tx should fail"
	);

	assert!(
		matches!(read_tx.del("existing_key"), Err(Error::TxNotWritable)),
		"del on read tx should fail"
	);

	assert!(
		matches!(read_tx.putc("key", "value", Some("check")), Err(Error::TxNotWritable)),
		"putc on read tx should fail"
	);

	assert!(
		matches!(read_tx.delc("existing_key", Some("value")), Err(Error::TxNotWritable)),
		"delc on read tx should fail"
	);

	// Read operations should still work
	assert!(read_tx.get("existing_key").is_ok());

	assert!(read_tx.exists("existing_key").is_ok());

	read_tx.cancel().unwrap();
}

// =============================================================================
// Key Already Exists Errors
// =============================================================================

#[test]

fn put_existing_key_fails() {
	let db = Database::new();

	// Create initial key
	let mut tx = db.transaction(true);

	tx.set("key", "original").unwrap();

	tx.commit().unwrap();

	// Try to put (insert) same key
	let mut tx = db.transaction(true);

	let result = tx.put("key", "new_value");

	assert!(matches!(result, Err(Error::KeyAlreadyExists)), "put existing key should fail");

	tx.cancel().unwrap();
}

#[test]

fn put_key_in_same_transaction_fails() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "first").unwrap();

	let result = tx.put("key", "second");

	assert!(matches!(result, Err(Error::KeyAlreadyExists)), "put after set in same tx should fail");

	tx.cancel().unwrap();
}

// =============================================================================
// Value Not Expected Errors
// =============================================================================

#[test]

fn putc_wrong_check_value_fails() {
	let db = Database::new();

	// Create key with known value
	let mut tx = db.transaction(true);

	tx.set("key", "actual_value").unwrap();

	tx.commit().unwrap();

	// Try putc with wrong check
	let mut tx = db.transaction(true);

	let result = tx.putc("key", "new_value", Some("wrong_check"));

	assert!(matches!(result, Err(Error::ValNotExpectedValue)), "putc with wrong check should fail");

	tx.cancel().unwrap();
}

#[test]

fn delc_wrong_check_value_fails() {
	let db = Database::new();

	// Create key with known value
	let mut tx = db.transaction(true);

	tx.set("key", "actual_value").unwrap();

	tx.commit().unwrap();

	// Try delc with wrong check
	let mut tx = db.transaction(true);

	let result = tx.delc("key", Some("wrong_check"));

	assert!(matches!(result, Err(Error::ValNotExpectedValue)), "delc with wrong check should fail");

	tx.cancel().unwrap();
}

#[test]

fn putc_none_check_on_existing_key_fails() {
	let db = Database::new();

	// Create key
	let mut tx = db.transaction(true);

	tx.set("key", "exists").unwrap();

	tx.commit().unwrap();

	// Try putc with None check (expects key to not exist)
	let mut tx = db.transaction(true);

	let result = tx.putc::<_, _, &[u8]>("key", "new_value", None);

	assert!(
		matches!(result, Err(Error::ValNotExpectedValue)),
		"putc with None check on existing key should fail"
	);

	tx.cancel().unwrap();
}

#[test]

fn delc_none_check_on_existing_key_fails() {
	let db = Database::new();

	// Create key
	let mut tx = db.transaction(true);

	tx.set("key", "exists").unwrap();

	tx.commit().unwrap();

	// Try delc with None check (expects key to not exist)
	let mut tx = db.transaction(true);

	let result = tx.delc::<_, &[u8]>("key", None);

	assert!(
		matches!(result, Err(Error::ValNotExpectedValue)),
		"delc with None check on existing key should fail"
	);

	tx.cancel().unwrap();
}

// =============================================================================
// No Savepoint Errors
// =============================================================================

#[test]

fn rollback_without_savepoint_fails() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	let result = tx.rollback_to_savepoint();

	assert!(matches!(result, Err(Error::NoSavepoint)), "rollback without savepoint should fail");

	tx.cancel().unwrap();
}

#[test]

fn rollback_after_all_savepoints_consumed_fails() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Set one savepoint
	tx.set_savepoint().unwrap();

	tx.set("key", "value").unwrap();

	// Rollback once (consumes the savepoint)
	tx.rollback_to_savepoint().unwrap();

	// Second rollback should fail
	let result = tx.rollback_to_savepoint();

	assert!(matches!(result, Err(Error::NoSavepoint)), "second rollback should fail");

	tx.cancel().unwrap();
}

// =============================================================================
// Conflict Errors
// =============================================================================

#[test]

fn write_conflict_error() {
	let db = Database::new();

	// Two transactions writing same key
	let mut tx1 = db.transaction(true);

	let mut tx2 = db.transaction(true);

	tx1.set("key", "tx1").unwrap();

	tx2.set("key", "tx2").unwrap();

	// First commits
	tx1.commit().unwrap();

	// Second should fail with write conflict
	let result = tx2.commit();

	assert!(
		matches!(result, Err(Error::KeyWriteConflict)),
		"concurrent write to same key should conflict"
	);
}

#[test]

fn read_conflict_error_ssi() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);

	tx.set("key", "initial").unwrap();

	tx.commit().unwrap();

	// tx1 reads with SSI
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();

	let _ = tx1.get("key").unwrap();

	// tx2 modifies and commits
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

	tx2.set("key", "modified").unwrap();

	tx2.commit().unwrap();

	// tx1 tries to write and commit
	tx1.set("other", "data").unwrap();

	let result = tx1.commit();

	assert!(matches!(result, Err(Error::KeyReadConflict)), "SSI should detect read conflict");
}

// =============================================================================
// Version In Future Error
// =============================================================================

#[test]

fn get_at_future_version_fails() {
	let db = Database::new();

	// Create data
	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	tx.commit().unwrap();

	// Try to read at a future version
	let tx = db.transaction(false);

	let current_version = tx.version();

	// Future version
	let future_version = current_version + 1_000_000_000; // Way in the future

	let result = tx.get_at_version("key", future_version);

	assert!(matches!(result, Err(Error::VersionInFuture)), "reading future version should fail");
}

#[test]

fn exists_at_future_version_fails() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	tx.commit().unwrap();

	let tx = db.transaction(false);

	let future_version = tx.version() + 1_000_000_000;

	let result = tx.exists_at_version("key", future_version);

	assert!(matches!(result, Err(Error::VersionInFuture)), "exists at future version should fail");
}

// =============================================================================
// Iterator/Cursor with Closed Transaction
// =============================================================================

#[test]

fn cursor_after_cancel_fails() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	tx.cancel().unwrap();

	let result = tx.cursor("a".."z");

	assert!(matches!(result, Err(Error::TxClosed)));
}

#[test]

fn keys_iter_after_commit_fails() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	tx.commit().unwrap();

	let result = tx.keys_iter("a".."z");

	assert!(matches!(result, Err(Error::TxClosed)));
}

#[test]

fn scan_iter_after_cancel_fails() {
	let db = Database::new();

	let mut tx = db.transaction(false);

	tx.cancel().unwrap();

	let result = tx.scan_iter("a".."z");

	assert!(matches!(result, Err(Error::TxClosed)));
}

// =============================================================================
// Getm with Closed Transaction
// =============================================================================

#[test]

fn getm_after_close_fails() {
	let db = Database::new();

	let mut tx = db.transaction(false);

	tx.cancel().unwrap();

	let result = tx.getm(vec!["key1", "key2"]);

	assert!(matches!(result, Err(Error::TxClosed)));
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]

fn operations_on_dropped_transaction() {
	let db = Database::new();

	// Transaction drops without commit or cancel
	{
		let mut tx = db.transaction(true);

		tx.set("key", "value").unwrap();
		// tx drops here without commit
	}

	// Data should not be committed
	let mut tx = db.transaction(false);

	assert!(tx.get("key").unwrap().is_none(), "Dropped transaction should not commit");

	tx.cancel().unwrap();
}

#[test]

fn set_savepoint_on_closed_transaction() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.commit().unwrap();

	let result = tx.set_savepoint();

	assert!(matches!(result, Err(Error::TxClosed)));
}

#[test]

fn rollback_on_closed_transaction() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	tx.set_savepoint().unwrap();

	tx.cancel().unwrap();

	let result = tx.rollback_to_savepoint();

	assert!(matches!(result, Err(Error::TxClosed)));
}
