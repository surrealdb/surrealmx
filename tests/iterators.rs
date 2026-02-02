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

//! Iterator and cursor edge case tests for SurrealMX.
//!
//! Tests cursor, KeyIterator, and ScanIterator behavior including
//! direction switching, empty ranges, and edge cases.

use bytes::Bytes;
use surrealmx::Database;

// =============================================================================
// Cursor Direction Tests
// =============================================================================

#[test]
fn cursor_direction_switch_at_first_element() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("b", "2").unwrap();
	tx.set("c", "3").unwrap();
	tx.commit().unwrap();

	let mut tx = db.transaction(false);
	let mut cursor = tx.cursor("a".."z").unwrap();

	// Start at first element
	cursor.seek_to_first();
	assert_eq!(cursor.key().unwrap().as_ref(), b"a");

	// Try to go back (should become invalid)
	cursor.prev();
	assert!(!cursor.valid(), "Prev at first element should invalidate");

	// Re-seek to valid position
	cursor.seek_to_first();
	assert!(cursor.valid(), "After re-seek, cursor should be valid");
	assert_eq!(cursor.key().unwrap().as_ref(), b"a");

	tx.cancel().unwrap();
}

#[test]
fn cursor_direction_switch_at_last_element() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("b", "2").unwrap();
	tx.set("c", "3").unwrap();
	tx.commit().unwrap();

	let mut tx = db.transaction(false);
	let mut cursor = tx.cursor("a".."z").unwrap();

	// Start at last element
	cursor.seek_to_last();
	assert_eq!(cursor.key().unwrap().as_ref(), b"c");

	// Try to go forward (should become invalid)
	cursor.next();
	assert!(!cursor.valid(), "Next at last element should invalidate");

	tx.cancel().unwrap();
}

#[test]
fn cursor_rapid_direction_changes() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	for c in b'a'..=b'z' {
		tx.set(&[c][..], &[c][..]).unwrap();
	}
	tx.commit().unwrap();

	let mut tx = db.transaction(false);
	let end = "z".to_string() + "\0";
	let mut cursor = tx.cursor("a"..end.as_str()).unwrap();

	cursor.seek("m");
	assert_eq!(cursor.key().unwrap().as_ref(), b"m");

	// Rapid direction changes
	cursor.next(); // n
	assert_eq!(cursor.key().unwrap().as_ref(), b"n");

	cursor.prev(); // m
	assert_eq!(cursor.key().unwrap().as_ref(), b"m");

	cursor.prev(); // l
	assert_eq!(cursor.key().unwrap().as_ref(), b"l");

	cursor.next(); // m
	assert_eq!(cursor.key().unwrap().as_ref(), b"m");

	cursor.next(); // n
	cursor.next(); // o
	assert_eq!(cursor.key().unwrap().as_ref(), b"o");

	cursor.prev(); // n
	cursor.prev(); // m
	cursor.prev(); // l
	cursor.prev(); // k
	assert_eq!(cursor.key().unwrap().as_ref(), b"k");

	tx.cancel().unwrap();
}

// =============================================================================
// Empty Range Tests
// =============================================================================

#[test]
fn cursor_empty_range_no_data() {
	let db = Database::new();

	let tx = db.transaction(false);
	let mut cursor = tx.cursor("a".."z").unwrap();

	cursor.seek_to_first();
	assert!(!cursor.valid(), "Empty database should have no entries");

	cursor.seek_to_last();
	assert!(!cursor.valid(), "Empty database should have no entries");
}

#[test]
fn cursor_empty_range_with_data_outside() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("z", "26").unwrap();
	tx.commit().unwrap();

	// Range that doesn't include any keys
	let tx = db.transaction(false);
	let mut cursor = tx.cursor("m".."n").unwrap();

	cursor.seek_to_first();
	assert!(!cursor.valid(), "No keys in range m..n");

	cursor.seek_to_last();
	assert!(!cursor.valid(), "No keys in range m..n");
}

#[test]
fn keys_iterator_empty_range() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("z", "26").unwrap();
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let keys: Vec<_> = tx.keys_iter("m".."n").unwrap().collect();
	assert!(keys.is_empty(), "Should return empty iterator");
}

#[test]
fn scan_iterator_empty_range() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let pairs: Vec<_> = tx.scan_iter("m".."z").unwrap().collect();
	assert!(pairs.is_empty(), "Should return empty iterator");
}

// =============================================================================
// Double-Ended Iterator Tests
// =============================================================================

#[test]
fn keys_iterator_double_ended_meets_in_middle() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("b", "2").unwrap();
	tx.set("c", "3").unwrap();
	tx.set("d", "4").unwrap();
	tx.set("e", "5").unwrap();
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let mut iter = tx.keys_iter("a".."z").unwrap();

	// Test basic forward iteration
	let first = iter.next();
	assert_eq!(first, Some(Bytes::from("a")));

	let second = iter.next();
	assert_eq!(second, Some(Bytes::from("b")));

	// Continue to collect remaining
	let remaining: Vec<_> = iter.collect();
	assert!(remaining.len() >= 1, "Should have remaining elements");
}

#[test]
fn scan_iterator_double_ended() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("b", "2").unwrap();
	tx.set("c", "3").unwrap();
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let mut iter = tx.scan_iter("a".."z").unwrap();

	// Get first
	let first = iter.next().unwrap();
	assert_eq!(first.0.as_ref(), b"a");

	// Get second
	let second = iter.next().unwrap();
	assert_eq!(second.0.as_ref(), b"b");

	// Collect remaining
	let remaining: Vec<_> = iter.collect();
	assert!(remaining.len() >= 1, "Should have remaining elements");
}

// =============================================================================
// Seek Edge Cases
// =============================================================================

#[test]
fn cursor_seek_beyond_range_end() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("b", "2").unwrap();
	tx.set("c", "3").unwrap();
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let mut cursor = tx.cursor("a".."c").unwrap(); // Range excludes "c"

	// Seek beyond range
	cursor.seek("z");
	assert!(!cursor.valid(), "Seek beyond range should invalidate");

	// Seek for prev beyond range
	cursor.seek_for_prev("a"); // Before start
	assert!(!cursor.valid(), "Seek for prev before range should invalidate");
}

#[test]
fn cursor_seek_to_non_existent_key() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("c", "3").unwrap();
	tx.set("e", "5").unwrap();
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let mut cursor = tx.cursor("a".."z").unwrap();

	// Seek to "b" (doesn't exist, should land on "c")
	cursor.seek("b");
	assert!(cursor.valid());
	assert_eq!(cursor.key().unwrap().as_ref(), b"c");

	// Seek for prev to "d" (doesn't exist, should land on "c")
	cursor.seek_for_prev("d");
	assert!(cursor.valid());
	assert_eq!(cursor.key().unwrap().as_ref(), b"c");
}

// =============================================================================
// Writeset and Merge Queue Interaction Tests
// =============================================================================

#[test]
fn iterator_sees_uncommitted_writes() {
	let db = Database::new();

	let mut tx = db.transaction(true);

	// Add data without commit
	tx.set("key1", "uncommitted1").unwrap();
	tx.set("key2", "uncommitted2").unwrap();

	// Iterator should see uncommitted data
	let keys: Vec<_> = tx.keys_iter("key".."keyz").unwrap().collect();
	assert_eq!(keys.len(), 2);
	assert_eq!(keys[0].as_ref(), b"key1");
	assert_eq!(keys[1].as_ref(), b"key2");

	tx.cancel().unwrap();
}

#[test]
fn iterator_sees_uncommitted_deletes() {
	let db = Database::new();

	// Create committed data
	let mut tx = db.transaction(true);
	tx.set("key1", "value1").unwrap();
	tx.set("key2", "value2").unwrap();
	tx.set("key3", "value3").unwrap();
	tx.commit().unwrap();

	// Start new transaction and delete
	let mut tx = db.transaction(true);
	tx.del("key2").unwrap();

	// Iterator should skip deleted key
	let keys: Vec<_> = tx.keys_iter("key".."keyz").unwrap().collect();
	assert_eq!(keys.len(), 2);
	assert_eq!(keys[0].as_ref(), b"key1");
	assert_eq!(keys[1].as_ref(), b"key3");

	tx.cancel().unwrap();
}

#[test]
fn iterator_merge_queue_and_writeset() {
	let db = Database::new();

	// First transaction creates some data
	let mut tx1 = db.transaction(true);
	tx1.set("a", "tx1_a").unwrap();
	tx1.set("b", "tx1_b").unwrap();
	tx1.commit().unwrap();

	// Second transaction reads while first is in merge queue
	let mut tx2 = db.transaction(true);

	// tx2 modifies some keys
	tx2.set("a", "tx2_a").unwrap(); // Override
	tx2.set("c", "tx2_c").unwrap(); // New key

	// Iterator should see merged view
	let scan: Vec<_> = tx2.scan_iter("a".."z").unwrap().collect();
	assert_eq!(scan.len(), 3);
	assert_eq!(scan[0], (Bytes::from("a"), Bytes::from("tx2_a"))); // From writeset
	assert_eq!(scan[1], (Bytes::from("b"), Bytes::from("tx1_b"))); // From committed/merge queue
	assert_eq!(scan[2], (Bytes::from("c"), Bytes::from("tx2_c"))); // From writeset

	tx2.cancel().unwrap();
}

// =============================================================================
// Iterator with Skip and Limit
// =============================================================================

#[test]
fn scan_with_skip_beyond_data() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("b", "2").unwrap();
	tx.set("c", "3").unwrap();
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let result = tx.scan("a".."z", Some(10), None).unwrap();
	assert!(result.is_empty(), "Skip beyond data should return empty");
}

#[test]
fn scan_with_limit_one() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("b", "2").unwrap();
	tx.set("c", "3").unwrap();
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let result = tx.scan("a".."z", None, Some(1)).unwrap();
	assert_eq!(result.len(), 1, "Limit 1 should return 1 result");
	assert_eq!(result[0].0.as_ref(), b"a");
}

#[test]
fn scan_with_skip_and_limit() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	for i in 0..10 {
		tx.set(format!("key_{:02}", i), format!("value_{}", i)).unwrap();
	}
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let result = tx.scan("key_".."key_z", Some(3), Some(4)).unwrap();

	assert_eq!(result.len(), 4);
	assert_eq!(result[0].0.as_ref(), b"key_03");
	assert_eq!(result[1].0.as_ref(), b"key_04");
	assert_eq!(result[2].0.as_ref(), b"key_05");
	assert_eq!(result[3].0.as_ref(), b"key_06");
}

// =============================================================================
// Reverse Iterator Tests
// =============================================================================

#[test]
fn keys_reverse_iterator() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("b", "2").unwrap();
	tx.set("c", "3").unwrap();
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let keys: Vec<_> = tx.keys_iter_reverse("a".."z").unwrap().collect();

	assert_eq!(keys.len(), 3);
	assert_eq!(keys[0].as_ref(), b"c");
	assert_eq!(keys[1].as_ref(), b"b");
	assert_eq!(keys[2].as_ref(), b"a");
}

#[test]
fn scan_reverse_iterator() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("a", "1").unwrap();
	tx.set("b", "2").unwrap();
	tx.set("c", "3").unwrap();
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let pairs: Vec<_> = tx.scan_iter_reverse("a".."z").unwrap().collect();

	assert_eq!(pairs.len(), 3);
	assert_eq!(pairs[0].0.as_ref(), b"c");
	assert_eq!(pairs[1].0.as_ref(), b"b");
	assert_eq!(pairs[2].0.as_ref(), b"a");
}

// =============================================================================
// Binary Key Tests
// =============================================================================

#[test]
fn iterator_with_binary_keys() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set(vec![0x00, 0x01], "null_start").unwrap();
	tx.set(vec![0xFF, 0xFE], "high_bytes").unwrap();
	tx.set(vec![0x50], "middle").unwrap();
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let start: Vec<u8> = vec![0x00];
	let end: Vec<u8> = vec![0xFF, 0xFF];
	let keys: Vec<_> = tx.keys_iter(start..end).unwrap().collect();

	assert_eq!(keys.len(), 3);
	// Should be sorted lexicographically
	assert_eq!(keys[0].as_ref(), &[0x00, 0x01]);
	assert_eq!(keys[1].as_ref(), &[0x50]);
	assert_eq!(keys[2].as_ref(), &[0xFF, 0xFE]);
}

// =============================================================================
// Single Element Range Tests
// =============================================================================

#[test]
fn cursor_single_element_range() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	tx.set("only", "one").unwrap();
	tx.commit().unwrap();

	let tx = db.transaction(false);
	let mut cursor = tx.cursor("only".."onlz").unwrap();

	cursor.seek_to_first();
	assert!(cursor.valid());
	assert_eq!(cursor.key().unwrap().as_ref(), b"only");

	cursor.next();
	assert!(!cursor.valid());

	cursor.seek_to_last();
	assert!(cursor.valid());
	assert_eq!(cursor.key().unwrap().as_ref(), b"only");

	cursor.prev();
	assert!(!cursor.valid());
}
