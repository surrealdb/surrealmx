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
//! Read-modify-write pattern tests for SurrealMX.
//!
//! Tests common transactional patterns including increment, compare-and-swap,
//! conditional updates, and retry-on-conflict logic.

use bytes::Bytes;
use surrealmx::Database;

// =============================================================================
// Increment Pattern Tests
// =============================================================================
#[test]

fn increment_pattern_ssi() {

	let db = Database::new();

	// Initialize counter
	{

		let mut tx = db.transaction(true);

		tx.set("counter", "0").unwrap();

		tx.commit().unwrap();
	}

	// Perform several increments sequentially
	for expected_before in 0..5 {

		let mut tx = db.transaction(true).with_serializable_snapshot_isolation();

		// Read current value
		let current = tx.get("counter").unwrap().unwrap();

		let current_val: i32 = std::str::from_utf8(&current).unwrap().parse().unwrap();

		assert_eq!(current_val, expected_before, "Counter should be at expected value");

		// Increment
		let new_val = current_val + 1;

		tx.set("counter", new_val.to_string()).unwrap();

		tx.commit().unwrap();
	}

	// Verify final value
	let tx = db.transaction(false);

	let final_val: i32 =
		std::str::from_utf8(&tx.get("counter").unwrap().unwrap()).unwrap().parse().unwrap();

	assert_eq!(final_val, 5, "Counter should be 5 after 5 increments");
}

// =============================================================================
// Compare and Swap Pattern Tests
// =============================================================================
#[test]

fn compare_and_swap_pattern() {

	let db = Database::new();

	// Initialize value
	{

		let mut tx = db.transaction(true);

		tx.set("cas_key", "initial").unwrap();

		tx.commit().unwrap();
	}

	// Successful CAS: expect "initial", set to "updated"
	{

		let mut tx = db.transaction(true);

		let result = tx.putc("cas_key", "updated", Some::<&str>("initial"));

		assert!(result.is_ok(), "CAS should succeed when expected value matches");

		tx.commit().unwrap();
	}

	// Verify update
	{

		let tx = db.transaction(false);

		assert_eq!(tx.get("cas_key").unwrap(), Some(Bytes::from("updated")));
	}

	// Failed CAS: expect "initial" (but it's now "updated")
	{

		let mut tx = db.transaction(true);

		let result = tx.putc("cas_key", "should_not_be_set", Some::<&str>("initial"));

		assert!(result.is_err(), "CAS should fail when expected value doesn't match");

		tx.cancel().unwrap();
	}

	// Value should still be "updated"
	{

		let tx = db.transaction(false);

		assert_eq!(tx.get("cas_key").unwrap(), Some(Bytes::from("updated")));
	}
}

// =============================================================================
// Conditional Update Chain Tests
// =============================================================================
#[test]

fn conditional_update_chain() {

	let db = Database::new();

	// Initialize state machine value
	{

		let mut tx = db.transaction(true);

		tx.set("state", "pending").unwrap();

		tx.commit().unwrap();
	}

	// State transitions: pending -> processing -> completed
	let transitions = [("pending", "processing"), ("processing", "completed")];

	for (expected, new_state) in transitions {

		let mut tx = db.transaction(true);

		// Use putc to ensure atomic state transition
		let result = tx.putc("state", new_state, Some::<&str>(expected));

		assert!(result.is_ok(), "Transition from {} to {} should succeed", expected, new_state);

		tx.commit().unwrap();
	}

	// Verify final state
	let tx = db.transaction(false);

	assert_eq!(tx.get("state").unwrap(), Some(Bytes::from("completed")));

	// Attempting invalid transition should fail
	{

		let mut tx = db.transaction(true);

		let result = tx.putc("state", "pending", Some::<&str>("processing"));

		assert!(result.is_err(), "Invalid transition should fail");

		tx.cancel().unwrap();
	}
}

// =============================================================================
// Optimistic Locking Pattern Tests
// =============================================================================
#[test]

fn optimistic_locking_pattern() {

	let db = Database::new();

	// Initialize data with version
	{

		let mut tx = db.transaction(true);

		tx.set("data", "initial_data").unwrap();

		tx.set("data_version", "1").unwrap();

		tx.commit().unwrap();
	}

	// Optimistic update: read version, update data and version atomically
	{

		let mut tx = db.transaction(true).with_serializable_snapshot_isolation();

		// Read current version
		let version = tx.get("data_version").unwrap().unwrap();

		let version_num: i32 = std::str::from_utf8(&version).unwrap().parse().unwrap();

		// Read and modify data
		let _data = tx.get("data").unwrap().unwrap();

		// Update data and bump version
		tx.set("data", "modified_data").unwrap();

		tx.set("data_version", (version_num + 1).to_string()).unwrap();

		tx.commit().unwrap();
	}

	// Verify update
	let tx = db.transaction(false);

	assert_eq!(tx.get("data").unwrap(), Some(Bytes::from("modified_data")));

	assert_eq!(tx.get("data_version").unwrap(), Some(Bytes::from("2")));
}

// =============================================================================
// Retry on Conflict Pattern Tests
// =============================================================================
#[test]

fn retry_on_conflict() {

	let db = Database::new();

	// Initialize counter
	{

		let mut tx = db.transaction(true);

		tx.set("retry_counter", "0").unwrap();

		tx.commit().unwrap();
	}

	// Verify initial value is set
	{

		let tx = db.transaction(false);

		let val = tx.get("retry_counter").unwrap();

		assert_eq!(val, Some(Bytes::from("0")), "Counter should be initialized to 0");
	}

	// Perform sequential increments with retry-on-conflict pattern
	let num_increments = 5;

	let mut total_retries = 0;

	for _ in 0..num_increments {

		let max_retries = 10;

		let mut retries = 0;

		loop {

			let mut tx = db.transaction(true).with_serializable_snapshot_isolation();

			// Read current value
			let current = tx.get("retry_counter").unwrap().expect("Counter should exist");

			let current_val: i32 = std::str::from_utf8(&current).unwrap().parse().unwrap();

			// Increment
			tx.set("retry_counter", (current_val + 1).to_string()).unwrap();

			// Try to commit
			match tx.commit() {
				Ok(_) => break,
				Err(_) => {

					retries += 1;

					total_retries += 1;

					if retries >= max_retries {

						panic!("Exceeded max retries");
					}
				}
			}
		}
	}

	// Verify final value
	let tx = db.transaction(false);

	let final_val: i32 =
		std::str::from_utf8(&tx.get("retry_counter").unwrap().unwrap()).unwrap().parse().unwrap();

	assert_eq!(final_val, num_increments, "Counter should equal number of increments");

	// In sequential execution, there should be no retries needed
	assert_eq!(total_retries, 0, "Sequential execution should not need retries");
}
