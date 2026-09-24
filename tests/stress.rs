#![cfg(not(target_arch = "wasm32"))]

use byteslice::ByteSlice;
use rand::RngExt;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread;
use surrealmx::Database;

#[test]
fn concurrent_random_transactions() {
	const KEY_COUNT: u32 = 100;
	const OPERATIONS: usize = 1000;
	const THREADS: usize = 10;
	// Create database
	let db: Arc<Database> = Arc::new(Database::new());
	// Store successful modifications
	let expected: Arc<Mutex<BTreeMap<ByteSlice, Option<ByteSlice>>>> =
		Arc::new(Mutex::new(BTreeMap::new()));
	// Spin up a number of threads
	let mut handles = vec![];
	for _ in 0..THREADS {
		let db = Arc::clone(&db);
		let expected = Arc::clone(&expected);
		handles.push(thread::spawn(move || {
			let mut rng = rand::rng();
			// Run the set of operations
			for _ in 0..OPERATIONS {
				let key_num = rng.random_range(0..KEY_COUNT);
				let key = ByteSlice::from_slice(&key_num.to_be_bytes());
				match rng.random_range(0..3) {
					0 => {
						// Read transaction
						let mut tx = db.transaction(false);
						let _ = tx.get(&key);
						let _ = tx.cancel();
					}
					1 => {
						// Set value
						let value_num = rng.random_range(0..1000u32);
						let value = ByteSlice::from_slice(&value_num.to_be_bytes());
						let mut tx = db.transaction(true);
						tx.set(key.clone(), value.clone()).unwrap();
						let mut exp = expected.lock().unwrap();
						if tx.commit().is_ok() {
							exp.insert(key, Some(value));
						}
					}
					_ => {
						// Delete value
						let mut tx = db.transaction(true);
						tx.del(key.clone()).unwrap();
						let mut exp = expected.lock().unwrap();
						if tx.commit().is_ok() {
							exp.insert(key, None);
						}
					}
				}
			}
		}));
	}
	// Wait for threads to finish
	for handle in handles {
		handle.join().unwrap();
	}
	// Verify that the final state matches
	let snapshot = expected.lock().unwrap().clone();
	let mut tx = db.transaction(false);
	for key_num in 0..KEY_COUNT {
		let key = ByteSlice::from_slice(&key_num.to_be_bytes());
		let val = tx.get(&key).unwrap();
		let expected_val = snapshot.get(&key).cloned().unwrap_or(None);
		assert_eq!(val, expected_val, "mismatch for key {key_num}");
	}
	tx.cancel().unwrap();
}
