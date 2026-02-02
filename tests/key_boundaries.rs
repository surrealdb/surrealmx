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

//! Key and value boundary tests for SurrealMX.
//!
//! Tests boundary conditions including empty values, null bytes,
//! byte boundaries, and key ordering.

use bytes::Bytes;
use surrealmx::Database;

// =============================================================================
// Empty Value Tests
// =============================================================================

#[test]
fn empty_value() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	// Set a key with an empty value
	tx.set("key_with_empty_value", "").unwrap();
	tx.set("key_with_data", "some_data").unwrap();
	tx.commit().unwrap();

	// Verify empty value is retrievable and distinct from non-existent
	let tx = db.transaction(false);
	let empty_val = tx.get("key_with_empty_value").unwrap();
	assert_eq!(empty_val, Some(Bytes::from("")), "Empty value should be Some(empty)");

	let nonexistent = tx.get("nonexistent_key").unwrap();
	assert!(nonexistent.is_none(), "Non-existent key should be None");

	// Empty value should appear in scans
	let results = tx.scan("key_".."key_z", None, None).unwrap();
	assert_eq!(results.len(), 2, "Both keys should appear in scan");
}

// =============================================================================
// Null Bytes Tests
// =============================================================================

#[test]
fn null_bytes_in_key() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	// Keys with null bytes at various positions
	let key1 = b"key\x00middle";
	let key2 = b"\x00start";
	let key3 = b"end\x00";

	tx.set(key1.as_slice(), "value1").unwrap();
	tx.set(key2.as_slice(), "value2").unwrap();
	tx.set(key3.as_slice(), "value3").unwrap();
	tx.commit().unwrap();

	// Verify all keys are retrievable
	let tx = db.transaction(false);
	assert_eq!(
		tx.get(key1.as_slice()).unwrap(),
		Some(Bytes::from("value1")),
		"Key with middle null byte"
	);
	assert_eq!(
		tx.get(key2.as_slice()).unwrap(),
		Some(Bytes::from("value2")),
		"Key starting with null byte"
	);
	assert_eq!(
		tx.get(key3.as_slice()).unwrap(),
		Some(Bytes::from("value3")),
		"Key ending with null byte"
	);
}

#[test]
fn null_bytes_in_value() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	// Values with null bytes
	let value1 = b"value\x00with\x00nulls";
	let value2 = b"\x00\x00\x00";
	let value3 = b"normal";

	tx.set("key1", value1.as_slice()).unwrap();
	tx.set("key2", value2.as_slice()).unwrap();
	tx.set("key3", value3.as_slice()).unwrap();
	tx.commit().unwrap();

	// Verify values are stored correctly
	let tx = db.transaction(false);
	let v1 = tx.get("key1").unwrap().unwrap();
	assert_eq!(v1.as_ref(), value1, "Value with null bytes should be preserved");

	let v2 = tx.get("key2").unwrap().unwrap();
	assert_eq!(v2.as_ref(), value2, "All-null value should be preserved");

	let v3 = tx.get("key3").unwrap().unwrap();
	assert_eq!(v3.as_ref(), value3, "Normal value should be preserved");
}

// =============================================================================
// Byte Boundary Tests
// =============================================================================

#[test]
fn max_byte_key() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	// Keys with 0xFF bytes
	let key_all_ff = vec![0xFF; 10];
	let key_mixed = vec![0x00, 0x7F, 0xFF, 0x80];
	let key_high = vec![0xFE, 0xFF];

	tx.set(&key_all_ff, "all_ff").unwrap();
	tx.set(&key_mixed, "mixed").unwrap();
	tx.set(&key_high, "high").unwrap();
	tx.commit().unwrap();

	// Verify retrieval
	let tx = db.transaction(false);
	assert_eq!(tx.get(&key_all_ff).unwrap(), Some(Bytes::from("all_ff")), "All 0xFF key");
	assert_eq!(tx.get(&key_mixed).unwrap(), Some(Bytes::from("mixed")), "Mixed byte key");
	assert_eq!(tx.get(&key_high).unwrap(), Some(Bytes::from("high")), "High byte key");
}

#[test]
fn single_byte_keys() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	// Single byte keys covering the byte range
	for byte in [0x00u8, 0x01, 0x7F, 0x80, 0xFE, 0xFF] {
		let key = vec![byte];
		let value = format!("value_{:02X}", byte);
		tx.set(&key, value).unwrap();
	}
	tx.commit().unwrap();

	// Verify all single-byte keys
	let tx = db.transaction(false);
	for byte in [0x00u8, 0x01, 0x7F, 0x80, 0xFE, 0xFF] {
		let key = vec![byte];
		let expected = format!("value_{:02X}", byte);
		assert_eq!(
			tx.get(&key).unwrap(),
			Some(Bytes::from(expected)),
			"Single byte key 0x{:02X}",
			byte
		);
	}
}

// =============================================================================
// Unicode and UTF-8 Tests
// =============================================================================

#[test]
fn unicode_keys() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	// Various Unicode strings as keys
	let keys = [
		"hello",      // ASCII
		"héllo",      // Latin extended
		"こんにちは", // Japanese
		"🎉🎊",       // Emoji
		"مرحبا",      // Arabic
		"שלום",       // Hebrew
		"中文",       // Chinese
	];

	for (i, key) in keys.iter().enumerate() {
		tx.set(*key, format!("value_{}", i)).unwrap();
	}
	tx.commit().unwrap();

	// Verify all Unicode keys
	let tx = db.transaction(false);
	for (i, key) in keys.iter().enumerate() {
		let expected = format!("value_{}", i);
		assert_eq!(tx.get(*key).unwrap(), Some(Bytes::from(expected)), "Unicode key: {}", key);
	}

	// Verify we can retrieve all keys using a wide scan
	// Use byte slices for range with high bytes
	let start: &[u8] = &[];
	let end: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF];
	let all_results = tx.scan(start..end, None, None).unwrap();
	assert!(all_results.len() >= keys.len(), "Should have at least {} keys", keys.len());
}

// =============================================================================
// Key Ordering Tests
// =============================================================================

#[test]
fn binary_key_ordering() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	// Insert keys in non-sorted order
	let keys = [
		b"b".as_slice(),
		b"a".as_slice(),
		b"ab".as_slice(),
		b"aa".as_slice(),
		b"ba".as_slice(),
		b"\x00".as_slice(),
		b"\xFF".as_slice(),
		b"".as_slice(), // Empty key
	];

	for key in &keys {
		tx.set(*key, "value").unwrap();
	}
	tx.commit().unwrap();

	// Scan and verify ordering
	let tx = db.transaction(false);
	let start: &[u8] = &[];
	let end: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF];
	let results = tx.scan(start..end, None, None).unwrap();

	// Verify keys are in byte-lexicographic order
	let mut prev: Option<&Bytes> = None;
	for (key, _) in &results {
		if let Some(p) = prev {
			assert!(
				key.as_ref() > p.as_ref(),
				"Keys should be in ascending byte order: {:?} should be after {:?}",
				key,
				p
			);
		}
		prev = Some(key);
	}
}

#[test]
fn key_prefix_edge_cases() {
	let db = Database::new();

	let mut tx = db.transaction(true);
	// Keys that are prefixes of each other
	tx.set("prefix", "v1").unwrap();
	tx.set("prefix_extended", "v2").unwrap();
	tx.set("prefix_more", "v3").unwrap();
	tx.set("pre", "v0").unwrap();
	tx.commit().unwrap();

	// Scan with prefix
	let tx = db.transaction(false);

	// Scan for exact prefix match range
	let prefix_start = b"prefix";
	let prefix_end = b"prefix\xFF";
	let prefix_results =
		tx.scan(prefix_start.as_slice()..prefix_end.as_slice(), None, None).unwrap();
	// Should include "prefix", "prefix_extended", "prefix_more" but NOT "pre"
	assert!(prefix_results.len() >= 3, "Should have at least 3 prefix matches");

	// Verify "pre" is not included
	let has_pre = prefix_results.iter().any(|(k, _)| k.as_ref() == b"pre");
	assert!(!has_pre, "'pre' should not be included in 'prefix' scan");

	// Verify exact "prefix" is included
	let has_exact = prefix_results.iter().any(|(k, _)| k.as_ref() == b"prefix");
	assert!(has_exact, "'prefix' should be included");

	// Scan for shorter prefix
	let pre_start = b"pre";
	let pre_end = b"pre\xFF";
	let pre_results = tx.scan(pre_start.as_slice()..pre_end.as_slice(), None, None).unwrap();
	assert!(pre_results.len() >= 4, "Should have at least 4 'pre' prefix matches");
}
