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

//! Large transaction stress tests for SurrealMX.
//!
//! Tests high-volume transaction scenarios including many keys,
//! large values, and sustained write pressure.

use bytes::Bytes;
use surrealmx::Database;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::*;

// =============================================================================
// Transaction With Thousands of Keys Tests
// =============================================================================

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn transaction_with_thousands_of_keys() {
	let db = Database::new();

	let num_keys = 10_000;

	// Write many keys in a single transaction
	let mut tx = db.transaction(true);
	for i in 0..num_keys {
		let key = format!("key_{:06}", i);
		let value = format!("value_{}", i);
		tx.set(key, value).unwrap();
	}
	tx.commit().unwrap();

	// Verify all keys are present
	let tx = db.transaction(false);
	let results = tx.scan("key_".."key_z", None, None).unwrap();
	assert_eq!(results.len(), num_keys, "All {} keys should be present", num_keys);

	// Spot check some values
	assert_eq!(tx.get("key_000000").unwrap(), Some(Bytes::from("value_0")));
	assert_eq!(tx.get("key_005000").unwrap(), Some(Bytes::from("value_5000")));
	assert_eq!(tx.get("key_009999").unwrap(), Some(Bytes::from("value_9999")));
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn large_value_handling() {
	let db = Database::new();

	// Create values of increasing sizes
	let sizes = [1024, 10 * 1024, 100 * 1024, 1024 * 1024]; // 1KB, 10KB, 100KB, 1MB

	let mut tx = db.transaction(true);
	for (i, size) in sizes.iter().enumerate() {
		let key = format!("large_key_{}", i);
		// Create a value of the specified size
		let value = vec![b'x'; *size];
		tx.set(key, value).unwrap();
	}
	tx.commit().unwrap();

	// Verify all values are retrievable and correct size
	let tx = db.transaction(false);
	for (i, expected_size) in sizes.iter().enumerate() {
		let key = format!("large_key_{}", i);
		let value = tx.get(&key).unwrap().expect("Value should exist");
		assert_eq!(value.len(), *expected_size, "Value {} should be {} bytes", i, expected_size);
		// Verify content
		assert!(value.iter().all(|&b| b == b'x'), "All bytes should be 'x'");
	}
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn many_small_writes() {
	let db = Database::new();

	let num_transactions = 1000;
	let keys_per_tx = 10;

	// Perform many small transactions
	for tx_num in 0..num_transactions {
		let mut tx = db.transaction(true);
		for key_num in 0..keys_per_tx {
			let key = format!("tx{:04}_key{:02}", tx_num, key_num);
			tx.set(key, "value").unwrap();
		}
		tx.commit().unwrap();
	}

	// Verify total key count
	let tx = db.transaction(false);
	let total_keys = tx.scan("tx".."tz", None, None).unwrap().len();
	assert_eq!(
		total_keys,
		num_transactions * keys_per_tx,
		"Should have {} total keys",
		num_transactions * keys_per_tx
	);
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn large_scan_results() {
	let db = Database::new();

	let num_keys = 5000;

	// Insert many keys
	{
		let mut tx = db.transaction(true);
		for i in 0..num_keys {
			let key = format!("scan_key_{:05}", i);
			let value = format!("value_for_{}", i);
			tx.set(key, value).unwrap();
		}
		tx.commit().unwrap();
	}

	// Perform a full scan
	let tx = db.transaction(false);
	let results = tx.scan("scan_key_".."scan_key_z", None, None).unwrap();
	assert_eq!(results.len(), num_keys);

	// Verify ordering
	let mut prev_key: Option<Bytes> = None;
	for (key, _value) in &results {
		if let Some(ref prev) = prev_key {
			assert!(key > prev, "Keys should be in sorted order");
		}
		prev_key = Some(key.clone());
	}

	// Test scan with skip and limit on large dataset
	let partial_results = tx.scan("scan_key_".."scan_key_z", Some(1000), Some(500)).unwrap();
	assert_eq!(partial_results.len(), 500, "Should return exactly 500 results");
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn memory_under_write_pressure() {
	let db = Database::new();

	// Perform sustained writes to test memory behavior
	let iterations = 100;
	let keys_per_iteration = 100;
	let value_size = 1024; // 1KB values

	for iter in 0..iterations {
		let mut tx = db.transaction(true);
		for key_num in 0..keys_per_iteration {
			// Reuse key names to simulate updates (overwriting existing data)
			let key = format!("pressure_key_{:03}", key_num);
			let value = vec![(iter % 256) as u8; value_size];
			tx.set(key, value).unwrap();
		}
		tx.commit().unwrap();
	}

	// Verify final state
	let tx = db.transaction(false);
	let results = tx.scan("pressure_key_".."pressure_key_z", None, None).unwrap();
	assert_eq!(results.len(), keys_per_iteration, "Should have {} unique keys", keys_per_iteration);

	// Verify values have the content from the last iteration
	let expected_byte = ((iterations - 1) % 256) as u8;
	for (_key, value) in &results {
		assert_eq!(value.len(), value_size);
		assert!(
			value.iter().all(|&b| b == expected_byte),
			"Values should contain bytes from last iteration"
		);
	}
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[test]
fn large_batch_getm() {
	let db = Database::new();

	let num_keys = 500;

	// Insert keys
	{
		let mut tx = db.transaction(true);
		for i in 0..num_keys {
			let key = format!("batch_key_{:04}", i);
			let value = format!("batch_value_{}", i);
			tx.set(key, value).unwrap();
		}
		tx.commit().unwrap();
	}

	// Perform a large batch get
	let tx = db.transaction(false);
	let keys: Vec<String> = (0..num_keys).map(|i| format!("batch_key_{:04}", i)).collect();
	let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

	let results = tx.getm(key_refs).unwrap();
	assert_eq!(results.len(), num_keys);

	// All values should be present
	for (i, result) in results.iter().enumerate() {
		assert!(result.is_some(), "Key {} should have a value", i);
		let expected = format!("batch_value_{}", i);
		assert_eq!(result.as_ref().unwrap(), &Bytes::from(expected));
	}

	// Test with some missing keys
	let mixed_keys: Vec<String> = (0..100)
		.map(|i| {
			if i % 2 == 0 {
				format!("batch_key_{:04}", i)
			} else {
				format!("nonexistent_{:04}", i)
			}
		})
		.collect();
	let mixed_refs: Vec<&str> = mixed_keys.iter().map(|s| s.as_str()).collect();

	let mixed_results = tx.getm(mixed_refs).unwrap();
	assert_eq!(mixed_results.len(), 100);

	// Every other result should be Some/None
	for (i, result) in mixed_results.iter().enumerate() {
		if i % 2 == 0 {
			assert!(result.is_some(), "Even index {} should have value", i);
		} else {
			assert!(result.is_none(), "Odd index {} should be None", i);
		}
	}
}
