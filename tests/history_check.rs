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

//! History serializability and conflict verification tests for `SurrealMX`.

#![allow(clippy::cast_possible_wrap, clippy::needless_continue, clippy::ignored_unit_patterns)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use surrealmx::{Database, Error};

#[test]
fn test_serializable_bank_transfers() {
	const ACCOUNTS: usize = 10;
	const INITIAL_BALANCE: i64 = 1000;
	const TOTAL_EXPECTED: i64 = (ACCOUNTS as i64) * INITIAL_BALANCE;
	const THREADS: usize = 8;
	const TRANSFERS_PER_THREAD: usize = 100;

	let db = Arc::new(Database::new());

	// Initialize accounts
	{
		let mut tx = db.transaction(true);
		for i in 0..ACCOUNTS {
			tx.set(format!("account:{i}"), INITIAL_BALANCE.to_string()).unwrap();
		}
		tx.commit().unwrap();
	}

	let stop_auditor = Arc::new(AtomicBool::new(false));

	// Background auditor thread checking invariant sum == TOTAL_EXPECTED
	let auditor = {
		let db = Arc::clone(&db);
		let stop = Arc::clone(&stop_auditor);
		thread::spawn(move || {
			while !stop.load(Ordering::Relaxed) {
				let tx = db.transaction(false);
				let mut sum = 0i64;
				for i in 0..ACCOUNTS {
					if let Ok(Some(val)) = tx.get(format!("account:{i}")) {
						if let Ok(s) = std::str::from_utf8(&val) {
							if let Ok(b) = s.parse::<i64>() {
								sum += b;
							}
						}
					}
				}
				assert_eq!(
					sum, TOTAL_EXPECTED,
					"Auditor observed broken bank invariant: sum = {sum}, expected {TOTAL_EXPECTED}"
				);
				thread::sleep(Duration::from_millis(1));
			}
		})
	};

	// Worker threads performing transfers with retry on conflict
	let mut handles = Vec::new();
	for tid in 0..THREADS {
		let db = Arc::clone(&db);
		handles.push(thread::spawn(move || {
			for iter in 0..TRANSFERS_PER_THREAD {
				let from = (tid + iter) % ACCOUNTS;
				let to = (tid + iter + 1) % ACCOUNTS;
				if from == to {
					continue;
				}
				let amount: i64 = 10;

				loop {
					let mut tx = db.transaction(true);
					let from_key = format!("account:{from}");
					let to_key = format!("account:{to}");

					let from_val = tx.get(&from_key).unwrap().unwrap();
					let from_bal: i64 = std::str::from_utf8(&from_val).unwrap().parse().unwrap();

					let to_val = tx.get(&to_key).unwrap().unwrap();
					let to_bal: i64 = std::str::from_utf8(&to_val).unwrap().parse().unwrap();

					tx.set(&from_key, (from_bal - amount).to_string()).unwrap();
					tx.set(&to_key, (to_bal + amount).to_string()).unwrap();

					match tx.commit() {
						Ok(_) => break,
						Err(Error::KeyReadConflict | Error::KeyWriteConflict) => {
							thread::yield_now();
							continue;
						}
						Err(e) => panic!("Unexpected commit error: {e}"),
					}
				}
			}
		}));
	}

	for h in handles {
		h.join().unwrap();
	}

	stop_auditor.store(true, Ordering::Relaxed);
	auditor.join().unwrap();

	// Final verification
	let tx = db.transaction(false);
	let mut final_sum = 0i64;
	for i in 0..ACCOUNTS {
		let val = tx.get(format!("account:{i}")).unwrap().unwrap();
		let bal: i64 = std::str::from_utf8(&val).unwrap().parse().unwrap();
		final_sum += bal;
	}
	assert_eq!(final_sum, TOTAL_EXPECTED);
}

#[test]
fn test_write_skew_prevention() {
	let db = Database::new();

	// Invariant: x + y >= 0
	// Initially: x = 50, y = 50 (sum = 100)
	{
		let mut tx = db.transaction(true);
		tx.set("x", "50").unwrap();
		tx.set("y", "50").unwrap();
		tx.commit().unwrap();
	}

	// Tx1 intends to withdraw 80 from x (allowed since 50 + 50 >= 80)
	let mut tx1 = db.transaction(true);
	let x1: i64 = std::str::from_utf8(&tx1.get("x").unwrap().unwrap()).unwrap().parse().unwrap();
	let y1: i64 = std::str::from_utf8(&tx1.get("y").unwrap().unwrap()).unwrap().parse().unwrap();
	assert!(x1 + y1 >= 80);
	tx1.set("x", (x1 - 80).to_string()).unwrap();

	// Tx2 concurrently intends to withdraw 80 from y (allowed since 50 + 50 >=
	// 80)
	let mut tx2 = db.transaction(true);
	let x2: i64 = std::str::from_utf8(&tx2.get("x").unwrap().unwrap()).unwrap().parse().unwrap();
	let y2: i64 = std::str::from_utf8(&tx2.get("y").unwrap().unwrap()).unwrap().parse().unwrap();
	assert!(x2 + y2 >= 80);
	tx2.set("y", (y2 - 80).to_string()).unwrap();

	// Tx1 commits first
	tx1.commit().unwrap();

	// Tx2 commit must be rejected because tx2 read x (which was modified by
	// tx1)
	match tx2.commit() {
		Err(Error::KeyReadConflict) => {}
		other => panic!("Expected KeyReadConflict preventing write skew, got: {other:?}"),
	}

	// Verify the invariant still holds in the database
	let tx = db.transaction(false);
	let final_x: i64 =
		std::str::from_utf8(&tx.get("x").unwrap().unwrap()).unwrap().parse().unwrap();
	let final_y: i64 =
		std::str::from_utf8(&tx.get("y").unwrap().unwrap()).unwrap().parse().unwrap();
	assert!(
		final_x + final_y >= 0,
		"Write skew occurred! final_x={final_x}, final_y={final_y}, sum={}",
		final_x + final_y
	);
}

#[test]
fn test_concurrent_increment_serializability() {
	const THREADS: usize = 6;
	const INCS_PER_THREAD: usize = 50;

	let db = Arc::new(Database::new());
	{
		let mut tx = db.transaction(true);
		tx.set("counter", "0").unwrap();
		tx.commit().unwrap();
	}

	let mut handles = Vec::new();
	for _ in 0..THREADS {
		let db = Arc::clone(&db);
		handles.push(thread::spawn(move || {
			for _ in 0..INCS_PER_THREAD {
				loop {
					let mut tx = db.transaction(true);
					let val = tx.get("counter").unwrap().unwrap();
					let count: i64 = std::str::from_utf8(&val).unwrap().parse().unwrap();
					tx.set("counter", (count + 1).to_string()).unwrap();
					match tx.commit() {
						Ok(_) => break,
						Err(Error::KeyReadConflict | Error::KeyWriteConflict) => {
							thread::yield_now();
							continue;
						}
						Err(e) => panic!("Unexpected error: {e}"),
					}
				}
			}
		}));
	}

	for h in handles {
		h.join().unwrap();
	}

	let tx = db.transaction(false);
	let val = tx.get("counter").unwrap().unwrap();
	let final_count: usize = std::str::from_utf8(&val).unwrap().parse().unwrap();
	assert_eq!(final_count, THREADS * INCS_PER_THREAD);
}
