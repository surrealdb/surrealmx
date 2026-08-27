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

//! Transaction isolation tests for `SurrealMX`.
//!
//! Tests SSI (Serializable Snapshot Isolation) conflict detection
//! and Snapshot Isolation behavior.

use bytes::Bytes;
use surrealmx::{Database, Error};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::*;

// =============================================================================
// Snapshot Isolation Tests
// =============================================================================

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn snapshot_isolation_read_sees_consistent_snapshot() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key1", "initial1").unwrap();
	tx.set("key2", "initial2").unwrap();
	tx.commit().unwrap();

	// Start a read transaction (SI)
	let mut read_tx = db.transaction(false).with_snapshot_isolation();

	// Concurrently modify data
	let mut write_tx = db.transaction(true);
	write_tx.set("key1", "modified1").unwrap();
	write_tx.set("key2", "modified2").unwrap();
	write_tx.commit().unwrap();

	// Read transaction should still see the original values (snapshot)
	assert_eq!(
		read_tx.get("key1").unwrap(),
		Some(Bytes::from("initial1")),
		"SI read should see original value for key1"
	);
	assert_eq!(
		read_tx.get("key2").unwrap(),
		Some(Bytes::from("initial2")),
		"SI read should see original value for key2"
	);

	read_tx.cancel().unwrap();

	// New transaction should see updated values
	let mut new_tx = db.transaction(false);
	assert_eq!(
		new_tx.get("key1").unwrap(),
		Some(Bytes::from("modified1")),
		"New transaction should see modified value"
	);
	new_tx.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn snapshot_isolation_allows_concurrent_writes_to_different_keys() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key1", "initial1").unwrap();
	tx.set("key2", "initial2").unwrap();
	tx.commit().unwrap();

	// Start two concurrent write transactions with SI
	let mut tx1 = db.transaction(true).with_snapshot_isolation();
	let mut tx2 = db.transaction(true).with_snapshot_isolation();

	// tx1 writes to key1
	tx1.set("key1", "tx1_value").unwrap();

	// tx2 writes to key2 (different key)
	tx2.set("key2", "tx2_value").unwrap();

	// Both should commit successfully with SI (no conflict on different keys)
	assert!(tx1.commit().is_ok(), "tx1 should commit successfully");
	assert!(tx2.commit().is_ok(), "tx2 should commit successfully (different key)");

	// Verify final state
	let mut verify_tx = db.transaction(false);
	assert_eq!(verify_tx.get("key1").unwrap(), Some(Bytes::from("tx1_value")));
	assert_eq!(verify_tx.get("key2").unwrap(), Some(Bytes::from("tx2_value")));
	verify_tx.cancel().unwrap();
}

// =============================================================================
// Serializable Snapshot Isolation (SSI) Tests
// =============================================================================

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_detects_write_write_conflict_on_same_key() {
	let db = Database::new();

	// Start two concurrent SSI transactions
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

	// Both write to the same key
	tx1.set("key", "value1").unwrap();
	tx2.set("key", "value2").unwrap();

	// First commit succeeds
	assert!(tx1.commit().is_ok(), "First committer should succeed");

	// Second commit should fail due to write-write conflict
	assert!(tx2.commit().is_err(), "Second committer should fail due to write conflict");

	// Verify final state
	let mut verify_tx = db.transaction(false);
	assert_eq!(
		verify_tx.get("key").unwrap(),
		Some(Bytes::from("value1")),
		"Value should be from first committed transaction"
	);
	verify_tx.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_detects_read_write_conflict() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// tx1 reads the key
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	let _ = tx1.get("key").unwrap();

	// tx2 modifies the same key and commits
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
	tx2.set("key", "modified").unwrap();
	assert!(tx2.commit().is_ok(), "tx2 should commit first");

	// tx1 tries to write something (could be any key)
	tx1.set("other_key", "value").unwrap();

	// tx1 commit should fail because it read a key that was modified
	assert!(tx1.commit().is_err(), "tx1 should fail due to read-write conflict");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_allows_concurrent_writes_to_disjoint_keys() {
	let db = Database::new();

	// Start two concurrent SSI transactions
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

	// Write to completely disjoint keys (no reads)
	tx1.set("key_a", "value_a").unwrap();
	tx2.set("key_b", "value_b").unwrap();

	// Both should succeed
	assert!(tx1.commit().is_ok(), "tx1 should succeed");
	assert!(tx2.commit().is_ok(), "tx2 should succeed");

	// Verify
	let mut verify_tx = db.transaction(false);
	assert_eq!(verify_tx.get("key_a").unwrap(), Some(Bytes::from("value_a")));
	assert_eq!(verify_tx.get("key_b").unwrap(), Some(Bytes::from("value_b")));
	verify_tx.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_phantom_read_prevention_on_range_scan() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key_a", "a").unwrap();
	tx.set("key_c", "c").unwrap();
	tx.commit().unwrap();

	// tx1 scans a range (this creates a read dependency on the range)
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	let keys = tx1.keys("key_".."key_z", None, None).unwrap();
	assert_eq!(keys.len(), 2, "Should see 2 keys initially");

	// tx2 inserts a new key in that range
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
	tx2.set("key_b", "b").unwrap();
	assert!(tx2.commit().is_ok(), "tx2 should commit first");

	// tx1 tries to commit after scanning the range
	tx1.set("unrelated", "value").unwrap();

	// tx1 should fail due to phantom read conflict
	// (a key was inserted in the range it scanned)
	assert!(tx1.commit().is_err(), "tx1 should fail due to phantom read");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_read_on_non_existent_key_then_concurrent_insert() {
	let db = Database::new();

	// tx1 reads a non-existent key
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	assert!(tx1.get("new_key").unwrap().is_none(), "Key should not exist");

	// tx2 inserts that key
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
	tx2.set("new_key", "value").unwrap();
	assert!(tx2.commit().is_ok(), "tx2 should commit");

	// tx1 writes to something else and tries to commit
	tx1.set("other", "data").unwrap();

	// Should fail because tx1 read "new_key" (saw None) but it was created
	assert!(tx1.commit().is_err(), "tx1 should fail due to read-write conflict on new_key");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_exists_check_creates_read_dependency() {
	let db = Database::new();

	// tx1 checks existence of a key
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	assert!(!tx1.exists("key").unwrap(), "Key should not exist");

	// tx2 creates that key
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
	tx2.set("key", "value").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 writes something else and tries to commit
	tx1.set("other", "data").unwrap();
	assert!(tx1.commit().is_err(), "tx1 should fail due to exists check conflict");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_delete_conflict() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "value").unwrap();
	tx.commit().unwrap();

	// tx1 reads the key
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	assert!(tx1.get("key").unwrap().is_some());

	// tx2 deletes the key
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
	tx2.del("key").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 tries to modify based on what it read
	tx1.set("key", "new_value").unwrap();
	assert!(tx1.commit().is_err(), "tx1 should fail because key was deleted");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_scan_conflict_on_update() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("b", "2").unwrap();
	tx.set("c", "3").unwrap();
	tx.commit().unwrap();

	// tx1 scans all keys
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	let result = tx1.scan("a".."d", None, None).unwrap();
	assert_eq!(result.len(), 3);

	// tx2 updates one of the keys in the range
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
	tx2.set("b", "modified").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 tries to commit
	tx1.set("unrelated", "value").unwrap();
	assert!(tx1.commit().is_err(), "tx1 should fail due to scan conflict");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_multiple_readers_one_writer() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// Multiple readers start
	let mut reader1 = db.transaction(true).with_serializable_snapshot_isolation();
	let mut reader2 = db.transaction(true).with_serializable_snapshot_isolation();

	// Both read the same key
	let _ = reader1.get("key").unwrap();
	let _ = reader2.get("key").unwrap();

	// One reader becomes a writer and commits
	reader1.set("key", "modified").unwrap();
	assert!(reader1.commit().is_ok(), "First writer should succeed");

	// Second reader tries to write something else
	reader2.set("other", "value").unwrap();
	assert!(reader2.commit().is_err(), "Second reader should fail due to read conflict");
}

// =============================================================================
// Mixed Isolation Level Tests
// =============================================================================

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn default_transaction_uses_ssi() {
	let db = Database::new();

	// Default write transactions should use SSI
	let mut tx1 = db.transaction(true);
	let mut tx2 = db.transaction(true);

	// tx1 reads non-existent key
	assert!(tx1.get("key").unwrap().is_none());

	// tx2 creates the key
	tx2.set("key", "value").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 writes something else
	tx1.set("other", "data").unwrap();

	// Should fail with default SSI
	assert!(tx1.commit().is_err(), "Default SSI should detect read conflict");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn si_mode_allows_read_write_anomaly() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// tx1 with SI reads the key
	let mut tx1 = db.transaction(true).with_snapshot_isolation();
	let _ = tx1.get("key").unwrap();

	// tx2 modifies the key
	let mut tx2 = db.transaction(true);
	tx2.set("key", "modified").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 writes to a different key
	tx1.set("other", "value").unwrap();

	// SI should allow this (no read tracking for conflict detection)
	assert!(tx1.commit().is_ok(), "SI should not track read-write conflicts");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn concurrent_counter_increment_conflict() {
	let db = Database::new();

	// Create counter
	let mut tx = db.transaction(true);
	tx.set("counter", "0").unwrap();
	tx.commit().unwrap();

	// Two transactions try to increment
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

	// Both read current value
	let val1 = tx1.get("counter").unwrap().unwrap();
	let val2 = tx2.get("counter").unwrap().unwrap();

	assert_eq!(val1.as_ref(), b"0");
	assert_eq!(val2.as_ref(), b"0");

	// Both try to increment
	tx1.set("counter", "1").unwrap();
	tx2.set("counter", "1").unwrap();

	// First commits
	assert!(tx1.commit().is_ok());

	// Second should fail (lost update prevention)
	assert!(tx2.commit().is_err(), "Second increment should fail to prevent lost update");

	// Verify counter is 1, not 2
	let mut verify = db.transaction(false);
	assert_eq!(verify.get("counter").unwrap(), Some(Bytes::from("1")));
	verify.cancel().unwrap();
}

// =============================================================================
// Locked Read Tests
// =============================================================================

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn si_locked_read_detects_concurrent_write_conflict() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// tx1 with SI locks the key for update
	let mut tx1 = db.transaction(true).with_snapshot_isolation();
	assert_eq!(tx1.get_for_update("key").unwrap(), Some(Bytes::from("initial")));

	// tx2 modifies the locked key
	let mut tx2 = db.transaction(true);
	tx2.set("key", "modified").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 writes to a different key
	tx1.set("other", "value").unwrap();

	// tx1 must abort, even under plain snapshot isolation
	let result = tx1.commit();
	assert!(
		matches!(result, Err(Error::KeyReadConflict)),
		"SI should detect a conflict on a locked read, got: {result:?}"
	);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn si_locked_read_unrelated_write_commits() {
	// Negative control for `si_locked_read_detects_concurrent_write_conflict`:
	// without this, that test could pass for the wrong reason.

	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// tx1 with SI locks the key for update
	let mut tx1 = db.transaction(true).with_snapshot_isolation();
	assert_eq!(tx1.get_for_update("key").unwrap(), Some(Bytes::from("initial")));

	// tx2 modifies an unrelated key
	let mut tx2 = db.transaction(true);
	tx2.set("unrelated", "modified").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 writes to a different key
	tx1.set("other", "value").unwrap();

	// tx1 should commit cleanly
	assert!(tx1.commit().is_ok(), "SI locked read should not conflict with unrelated writes");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn si_locked_read_without_writes_detects_conflict() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// tx1 with SI locks the key for update, but writes nothing
	let mut tx1 = db.transaction(true).with_snapshot_isolation();
	assert_eq!(tx1.get_for_update("key").unwrap(), Some(Bytes::from("initial")));

	// tx2 modifies the locked key
	let mut tx2 = db.transaction(true);
	tx2.set("key", "modified").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 must abort, even though its writeset is empty
	let result = tx1.commit();
	assert!(
		matches!(result, Err(Error::KeyReadConflict)),
		"A locked read with no writes should still be validated, got: {result:?}"
	);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_locked_read_without_writes_detects_conflict() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// tx1 with SSI locks the key for update, but writes nothing
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	assert_eq!(tx1.get_for_update("key").unwrap(), Some(Bytes::from("initial")));

	// tx2 modifies the locked key
	let mut tx2 = db.transaction(true);
	tx2.set("key", "modified").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 must abort: a plain read with an empty writeset would commit
	// cleanly under SSI, but a locked read is validated in every mode
	let result = tx1.commit();
	assert!(
		matches!(result, Err(Error::KeyReadConflict)),
		"A locked read with no writes should still be validated, got: {result:?}"
	);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn locked_read_without_writes_commits_when_unchanged() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// tx1 with SI locks the key for update, but writes nothing
	let mut tx1 = db.transaction(true).with_snapshot_isolation();
	assert_eq!(tx1.get_for_update("key").unwrap(), Some(Bytes::from("initial")));

	// tx2 modifies an unrelated key
	let mut tx2 = db.transaction(true);
	tx2.set("unrelated", "modified").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 should commit cleanly
	assert!(tx1.commit().is_ok(), "An unchanged locked read should commit cleanly");

	// Subsequent transactions should be unaffected
	let mut tx3 = db.transaction(true);
	tx3.set("after", "value").unwrap();
	assert!(tx3.commit().is_ok());
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn locked_read_on_read_only_transaction_errors() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// A read-only transaction cannot lock a key for update
	let mut read_tx = db.transaction(false);
	let result = read_tx.get_for_update("key");
	assert!(
		matches!(result, Err(Error::TxNotWritable)),
		"A locked read on a read-only transaction should error, got: {result:?}"
	);
	read_tx.cancel().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn locked_read_reads_snapshot_and_own_writes() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	let mut tx1 = db.transaction(true).with_snapshot_isolation();

	// The locked read sees the snapshot value
	assert_eq!(tx1.get_for_update("key").unwrap(), Some(Bytes::from("initial")));

	// The locked read sees this transaction's own writes
	tx1.set("key", "updated").unwrap();
	assert_eq!(tx1.get_for_update("key").unwrap(), Some(Bytes::from("updated")));

	tx1.commit().unwrap();
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_locked_read_scopes_validation_to_the_locked_key() {
	// The commit guarantee of a locked read is per-key: a transaction with
	// an empty writeset validates only its locked keys, so a concurrent
	// write to a key it merely read cannot abort it.

	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("locked", "initial").unwrap();
	tx.set("plain", "initial").unwrap();
	tx.commit().unwrap();

	// tx1 with SSI locks one key and plainly reads another, writing nothing
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	assert_eq!(tx1.get_for_update("locked").unwrap(), Some(Bytes::from("initial")));
	assert_eq!(tx1.get("plain").unwrap(), Some(Bytes::from("initial")));

	// tx2 modifies only the plainly read key
	let mut tx2 = db.transaction(true);
	tx2.set("plain", "changed").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 must commit: no concurrent write touched the locked key
	assert!(tx1.commit().is_ok(), "A locked read must be validated only against the locked key");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_locked_read_keeps_plain_read_validation_for_writers() {
	// A writing SSI transaction still validates its plain reads: locking a
	// key neither widens nor narrows the read validation of a transaction
	// that publishes writes.

	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("locked", "initial").unwrap();
	tx.set("plain", "initial").unwrap();
	tx.commit().unwrap();

	// tx1 with SSI locks one key, plainly reads another, and writes
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	assert_eq!(tx1.get_for_update("locked").unwrap(), Some(Bytes::from("initial")));
	assert_eq!(tx1.get("plain").unwrap(), Some(Bytes::from("initial")));
	tx1.set("other", "value").unwrap();

	// tx2 modifies only the plainly read key
	let mut tx2 = db.transaction(true);
	tx2.set("plain", "changed").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 must abort under SSI, having written after a stale plain read
	let result = tx1.commit();
	assert!(
		matches!(result, Err(Error::KeyReadConflict)),
		"A writing SSI transaction should still validate plain reads, got: {result:?}"
	);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn si_locked_read_does_not_track_plain_reads() {
	// Under snapshot isolation plain reads are never validated: locking a
	// key must not change that, even for a transaction that writes.

	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("locked", "initial").unwrap();
	tx.set("plain", "initial").unwrap();
	tx.commit().unwrap();

	// tx1 with SI locks one key, plainly reads another, and writes
	let mut tx1 = db.transaction(true).with_snapshot_isolation();
	assert_eq!(tx1.get_for_update("locked").unwrap(), Some(Bytes::from("initial")));
	assert_eq!(tx1.get("plain").unwrap(), Some(Bytes::from("initial")));
	tx1.set("other", "value").unwrap();

	// tx2 modifies only the plainly read key
	let mut tx2 = db.transaction(true);
	tx2.set("plain", "changed").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 must commit: SI does not validate plain reads
	assert!(tx1.commit().is_ok(), "SI should not validate plain reads when a key is locked");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn ssi_locked_read_does_not_arm_scan_validation() {
	// A locked-read-only transaction validates only its locked keys: a
	// range it scanned is not validated when the writeset is empty,
	// matching the behavior of every other read-only SSI transaction.

	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("locked", "initial").unwrap();
	tx.set("scan/a", "initial").unwrap();
	tx.commit().unwrap();

	// tx1 with SSI locks one key and scans a range, writing nothing
	let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
	assert_eq!(tx1.get_for_update("locked").unwrap(), Some(Bytes::from("initial")));
	assert_eq!(tx1.scan("scan/".."scan/z", None, None).unwrap().len(), 1);

	// tx2 writes inside the scanned range
	let mut tx2 = db.transaction(true);
	tx2.set("scan/b", "changed").unwrap();
	assert!(tx2.commit().is_ok());

	// tx1 must commit: no concurrent write touched the locked key
	assert!(tx1.commit().is_ok(), "A locked read must not arm scan validation without writes");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn locked_read_does_not_leak_into_a_pooled_reuse() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);
	tx.set("key", "initial").unwrap();
	tx.commit().unwrap();

	// Lock a key, then drop the transaction without committing or
	// cancelling it, so it goes back to the pool with a populated lockset
	{
		let mut tx1 = db.transaction(true);
		assert_eq!(tx1.get_for_update("key").unwrap(), Some(Bytes::from("initial")));
	}

	// Take that pooled transaction back out. Its snapshot is established
	// here, before the concurrent write below
	let mut tx3 = db.transaction(true);

	// A concurrent transaction writes the previously locked key, and
	// commits after tx3's snapshot
	let mut tx2 = db.transaction(true);
	tx2.set("key", "modified").unwrap();
	assert!(tx2.commit().is_ok());

	// tx3 locks a different key, arming lockset validation for itself
	assert_eq!(tx3.get_for_update("other").unwrap(), None);

	// tx3 must commit: it never locked "key", so the write to it is not a
	// conflict. Only a lockset left behind by the previous use of this
	// pooled transaction could make this abort
	let result = tx3.commit();
	assert!(
		result.is_ok(),
		"A reused transaction must not inherit a stale lockset, got: {result:?}"
	);
}
