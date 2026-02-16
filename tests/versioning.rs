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

//! Point-in-time query tests for SurrealMX.
//!
//! Tests version-specific APIs like `get_at_version()`, `exists_at_version()`,
//! `scan_at_version()`, and `scan_all_versions()`.

use bytes::Bytes;
use std::time::Duration;
use surrealmx::{Database, DatabaseOptions};

// Helper to advance the oracle timestamp
fn advance_time(db: &Database) {
	std::thread::sleep(Duration::from_millis(5));

	let mut tx = db.transaction(true);

	tx.set("__time_advance__", "dummy").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));
}

// =============================================================================
// get_at_version Tests
// =============================================================================

#[test]

fn get_at_version_reads_historical_value() {
	let db = Database::new();

	// Version 1: Set initial value
	let mut tx = db.transaction(true);

	tx.set("key", "version1").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version after v1 commits
	let snapshot1 = db.transaction(false);

	let version1 = snapshot1.version();

	advance_time(&db);

	// Version 2: Update value
	let mut tx = db.transaction(true);

	tx.set("key", "version2").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version after v2 commits
	let snapshot2 = db.transaction(false);

	let version2 = snapshot2.version();

	advance_time(&db);

	// Version 3: Update again
	let mut tx = db.transaction(true);

	tx.set("key", "version3").unwrap();

	tx.commit().unwrap();

	advance_time(&db);

	// Read at different versions from a fresh transaction
	let tx = db.transaction(false);

	// Read at version1
	let val1 = tx.get_at_version("key", version1).unwrap();

	assert_eq!(val1, Some(Bytes::from("version1")), "Should see version1 value");

	// Read at version2
	let val2 = tx.get_at_version("key", version2).unwrap();

	assert_eq!(val2, Some(Bytes::from("version2")), "Should see version2 value");

	// Current read should see version3
	let val3 = tx.get("key").unwrap();

	assert_eq!(val3, Some(Bytes::from("version3")), "Current should see version3");
}

#[test]

fn get_at_version_sees_delete_as_none() {
	let db = Database::new();

	// Create initial value
	let mut tx = db.transaction(true);

	tx.set("key", "exists").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version when key exists
	let snapshot_before = db.transaction(false);

	let version_before_delete = snapshot_before.version();

	advance_time(&db);

	// Delete the key
	let mut tx = db.transaction(true);

	tx.del("key").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version after delete
	let snapshot_after = db.transaction(false);

	let version_after_delete = snapshot_after.version();

	advance_time(&db);

	// Read at different points
	let tx = db.transaction(false);

	// Before delete - should exist
	assert_eq!(
		tx.get_at_version("key", version_before_delete).unwrap(),
		Some(Bytes::from("exists")),
		"Should exist before delete"
	);

	// After delete - should be None
	assert_eq!(
		tx.get_at_version("key", version_after_delete).unwrap(),
		None,
		"Should be None after delete"
	);
}

#[test]

fn get_at_version_non_existent_key_returns_none() {
	let db = Database::new();

	// Create some data
	let mut tx = db.transaction(true);

	tx.set("other_key", "value").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version
	let snapshot = db.transaction(false);

	let version = snapshot.version();

	advance_time(&db);

	// Query non-existent key at that version
	let tx = db.transaction(false);

	let result = tx.get_at_version("non_existent", version).unwrap();

	assert_eq!(result, None, "Non-existent key should return None");
}

// =============================================================================
// exists_at_version Tests
// =============================================================================

#[test]

fn exists_at_version_tracks_existence() {
	let db = Database::new();

	// Create the key
	let mut tx = db.transaction(true);

	tx.set("key", "value").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version after creation
	let snapshot_created = db.transaction(false);

	let version_created = snapshot_created.version();

	advance_time(&db);

	// Delete the key
	let mut tx = db.transaction(true);

	tx.del("key").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version after deletion
	let snapshot_deleted = db.transaction(false);

	let version_deleted = snapshot_deleted.version();

	advance_time(&db);

	// Check existence at different points
	let tx = db.transaction(false);

	// After creation - should exist
	assert!(tx.exists_at_version("key", version_created).unwrap(), "Should exist after creation");

	// After deletion - should not exist
	assert!(
		!tx.exists_at_version("key", version_deleted).unwrap(),
		"Should not exist after deletion"
	);
}

// =============================================================================
// scan_at_version Tests
// =============================================================================

#[test]

fn scan_at_version_returns_consistent_snapshot() {
	let db = Database::new();

	// Version 1: Create initial range
	let mut tx = db.transaction(true);

	tx.set("a", "1").unwrap();

	tx.set("b", "2").unwrap();

	tx.set("c", "3").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version after v1
	let snapshot1 = db.transaction(false);

	let version1 = snapshot1.version();

	advance_time(&db);

	// Version 2: Modify the range
	let mut tx = db.transaction(true);

	tx.set("b", "modified").unwrap();

	tx.del("c").unwrap();

	tx.set("d", "4").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version after v2
	let snapshot2 = db.transaction(false);

	let version2 = snapshot2.version();

	advance_time(&db);

	// Scan at version1
	let tx = db.transaction(false);

	let scan1 = tx.scan_at_version("a".."z", None, None, version1).unwrap();

	assert_eq!(scan1.len(), 3, "Version1 should have 3 entries");

	assert_eq!(scan1[0], (Bytes::from("a"), Bytes::from("1")));

	assert_eq!(scan1[1], (Bytes::from("b"), Bytes::from("2")));

	assert_eq!(scan1[2], (Bytes::from("c"), Bytes::from("3")));

	// Scan at version2
	let scan2 = tx.scan_at_version("a".."z", None, None, version2).unwrap();

	// Should have: a=1, b=modified, d=4 (c is deleted)
	// Plus __time_advance__ from advance_time helper
	assert!(scan2.len() >= 3, "Version2 should have at least 3 entries");
}

#[test]

fn keys_at_version_returns_historical_keys() {
	let db = Database::new();

	// Create initial keys
	let mut tx = db.transaction(true);

	tx.set("key1", "v1").unwrap();

	tx.set("key2", "v1").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version1
	let snapshot1 = db.transaction(false);

	let version1 = snapshot1.version();

	advance_time(&db);

	// Modify keys
	let mut tx = db.transaction(true);

	tx.del("key1").unwrap();

	tx.set("key3", "v1").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version2
	let snapshot2 = db.transaction(false);

	let version2 = snapshot2.version();

	advance_time(&db);

	// Check keys at different versions
	let tx = db.transaction(false);

	let keys1 = tx.keys_at_version("key".."keyz", None, None, version1).unwrap();

	assert_eq!(keys1.len(), 2);

	assert!(keys1.contains(&Bytes::from("key1")));

	assert!(keys1.contains(&Bytes::from("key2")));

	let keys2 = tx.keys_at_version("key".."keyz", None, None, version2).unwrap();

	assert_eq!(keys2.len(), 2);

	assert!(keys2.contains(&Bytes::from("key2")));

	assert!(keys2.contains(&Bytes::from("key3")));
}

// =============================================================================
// scan_all_versions Tests
// =============================================================================

#[test]

fn scan_all_versions_returns_complete_history() {
	let db = Database::new();

	// Create key with multiple versions
	let mut tx = db.transaction(true);

	tx.set("key", "v1").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	let mut tx = db.transaction(true);

	tx.set("key", "v2").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	let mut tx = db.transaction(true);

	tx.set("key", "v3").unwrap();

	tx.commit().unwrap();

	// Get all versions
	let tx = db.transaction(false);

	let all_versions = tx.scan_all_versions("key".."keyz", None, None).unwrap();

	// Should have at least 3 versions
	assert!(all_versions.len() >= 3, "Should have at least 3 versions, got {}", all_versions.len());

	// All entries should be for "key"
	for (key, _version, _value) in &all_versions {
		assert_eq!(key, &Bytes::from("key"));
	}

	// Values should include v1, v2, v3
	let values: Vec<_> = all_versions.iter().filter_map(|(_, _, v)| v.clone()).collect();

	assert!(values.contains(&Bytes::from("v1")), "Should contain v1");

	assert!(values.contains(&Bytes::from("v2")), "Should contain v2");

	assert!(values.contains(&Bytes::from("v3")), "Should contain v3");
}

#[test]

fn scan_all_versions_includes_deletes() {
	let db = Database::new();

	// Create and delete key
	let mut tx = db.transaction(true);

	tx.set("key", "created").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	let mut tx = db.transaction(true);

	tx.del("key").unwrap();

	tx.commit().unwrap();

	// Get all versions
	let tx = db.transaction(false);

	let all_versions = tx.scan_all_versions("key".."keyz", None, None).unwrap();

	// Should have at least 2 versions (create and delete)
	assert!(all_versions.len() >= 2, "Should have create and delete versions");

	// Check that we have both Some (created) and None (deleted) values
	let has_some = all_versions.iter().any(|(_, _, v)| v.is_some());

	let has_none = all_versions.iter().any(|(_, _, v)| v.is_none());

	assert!(has_some, "Should have a version with value");

	assert!(has_none, "Should have a deleted version (None)");
}

// =============================================================================
// GC History Tests
// =============================================================================

#[test]

fn gc_history_preserves_versions_within_window() {
	let history_duration = Duration::from_secs(60); // Long history window

	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(100))
			.with_cleanup_interval(Duration::from_millis(50)),
	)
	.with_gc_history(history_duration);

	// Create initial version
	let mut tx = db.transaction(true);

	tx.set("key", "v1").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(10));

	// Capture version after v1
	let snapshot = db.transaction(false);

	let version1 = snapshot.version();

	std::thread::sleep(Duration::from_millis(10));

	// Update to v2
	let mut tx = db.transaction(true);

	tx.set("key", "v2").unwrap();

	tx.commit().unwrap();

	// Allow GC to run
	std::thread::sleep(Duration::from_millis(300));

	// Create a new transaction to advance time
	let mut tx = db.transaction(true);

	tx.set("dummy", "value").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(10));

	// Version1 should still be readable (within history window)
	let tx = db.transaction(false);

	let v1 = tx.get_at_version("key", version1).unwrap();

	assert_eq!(v1, Some(Bytes::from("v1")), "v1 should be preserved within history window");
}

#[test]

fn gc_mode_cleans_up_old_versions() {
	// With gc (no history), old versions should be cleaned up
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(50))
			.with_cleanup_interval(Duration::from_millis(50)),
	)
	.with_gc(); // Enable aggressive GC

	// Create many versions quickly
	for i in 0..10 {
		let mut tx = db.transaction(true);

		tx.set("key", format!("v{}", i)).unwrap();

		tx.commit().unwrap();
	}

	// Wait for GC
	std::thread::sleep(Duration::from_millis(200));

	// Current value should still be readable
	let mut tx = db.transaction(false);

	let current = tx.get("key").unwrap();

	assert!(current.is_some(), "Current value should still exist");

	tx.cancel().unwrap();
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]

fn version_query_with_uncommitted_changes() {
	let db = Database::new();

	// Create initial data
	let mut tx = db.transaction(true);

	tx.set("key", "committed").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture committed version
	let snapshot = db.transaction(false);

	let committed_version = snapshot.version();

	advance_time(&db);

	// Start a new transaction with uncommitted changes
	let mut tx = db.transaction(true);

	tx.set("key", "uncommitted").unwrap();

	// Read at committed version should see committed value
	let committed_value = tx.get_at_version("key", committed_version).unwrap();

	assert_eq!(committed_value, Some(Bytes::from("committed")));

	// Current read should see uncommitted value
	let current_value = tx.get("key").unwrap();

	assert_eq!(current_value, Some(Bytes::from("uncommitted")));

	tx.cancel().unwrap();
}

#[test]

fn version_query_on_multiple_keys() {
	let db = Database::new();

	// Create key1
	let mut tx = db.transaction(true);

	tx.set("key1", "v1").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version when only key1 exists
	let snapshot1 = db.transaction(false);

	let version1 = snapshot1.version();

	advance_time(&db);

	// Create key2 and update both
	let mut tx = db.transaction(true);

	tx.set("key2", "v1").unwrap();

	tx.commit().unwrap();

	advance_time(&db);

	let mut tx = db.transaction(true);

	tx.set("key1", "v2").unwrap();

	tx.set("key2", "v2").unwrap();

	tx.commit().unwrap();

	advance_time(&db);

	// Read both at version1
	let tx = db.transaction(false);

	let k1_v1 = tx.get_at_version("key1", version1).unwrap();

	let k2_v1 = tx.get_at_version("key2", version1).unwrap();

	assert_eq!(k1_v1, Some(Bytes::from("v1")), "key1 should be v1");

	assert_eq!(k2_v1, None, "key2 should not exist at version1");
}

#[test]

fn scan_at_version_with_skip_and_limit() {
	let db = Database::new();

	// Create range of keys
	let mut tx = db.transaction(true);

	for i in 0..10 {
		tx.set(format!("key_{:02}", i), format!("value_{}", i)).unwrap();
	}

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version
	let snapshot = db.transaction(false);

	let version = snapshot.version();

	advance_time(&db);

	// Scan with skip and limit at that version
	let tx = db.transaction(false);

	let result = tx.scan_at_version("key_".."key_z", Some(3), Some(4), version).unwrap();

	assert_eq!(result.len(), 4, "Should return 4 items");

	assert_eq!(result[0].0, Bytes::from("key_03"), "First should be key_03 (skipped 0,1,2)");

	assert_eq!(result[3].0, Bytes::from("key_06"), "Last should be key_06");
}

#[test]

fn scan_at_version_reverse() {
	let db = Database::new();

	// Create keys
	let mut tx = db.transaction(true);

	tx.set("a", "1").unwrap();

	tx.set("b", "2").unwrap();

	tx.set("c", "3").unwrap();

	tx.commit().unwrap();

	std::thread::sleep(Duration::from_millis(5));

	// Capture version
	let snapshot = db.transaction(false);

	let version = snapshot.version();

	advance_time(&db);

	// Reverse scan at version
	let tx = db.transaction(false);

	let result = tx.scan_at_version_reverse("a".."z", None, None, version).unwrap();

	assert_eq!(result.len(), 3);

	assert_eq!(result[0].0, Bytes::from("c"), "First in reverse should be 'c'");

	assert_eq!(result[2].0, Bytes::from("a"), "Last in reverse should be 'a'");
}
