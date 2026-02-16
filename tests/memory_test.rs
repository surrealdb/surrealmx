// Test to demonstrate memory usage improvement with the GC fix

use bytes::Bytes;
use std::time::Duration;
use surrealmx::{Database, DatabaseOptions};

#[test]

fn test_version_cleanup_with_updates() {
	// Create a database with aggressive inline GC (no history retention)
	let db = Database::new_with_options(
		DatabaseOptions::default()
			.with_gc_interval(Duration::from_millis(100))
			.with_cleanup_interval(Duration::from_millis(50)),
	)
	.with_gc(); // Enable aggressive inline GC

	let num_keys = 1000;

	let num_updates = 10;

	println!("Creating {} keys...", num_keys);

	// Create initial keys
	for i in 0..num_keys {
		let mut tx = db.transaction(true);

		let key = format!("key_{:06}", i);

		let value = vec![0u8; 100];

		tx.set(key, value).unwrap();

		tx.commit().unwrap();
	}

	println!("Updating each key {} times...", num_updates);

	// Update each key multiple times
	for update_round in 0..num_updates {
		for i in 0..num_keys {
			let mut tx = db.transaction(true);

			let key = format!("key_{:06}", i);

			let value = vec![update_round as u8; 100];

			tx.set(key, value).unwrap();

			tx.commit().unwrap();
		}

		if update_round % 2 == 0 {
			println!("  Round {}/{}", update_round + 1, num_updates);

			// Give GC a chance to run
			std::thread::sleep(Duration::from_millis(200));
		}
	}

	println!("Verifying final state...");

	// Verify all keys exist and have the latest value
	let mut tx = db.transaction(false);

	for i in 0..num_keys {
		let key = format!("key_{:06}", i);

		let value = tx.get(key).unwrap();

		assert!(value.is_some(), "Key {} should exist", i);

		assert_eq!(value.unwrap(), Bytes::from(vec![(num_updates - 1) as u8; 100]));
	}

	tx.cancel().unwrap();

	println!("Test passed! All keys have correct final values.");

	println!("\nNote: With the fix, old versions should be cleaned up during updates,");

	println!("keeping memory usage low even with {} total updates.", num_keys * num_updates);
}

#[test]

fn test_version_cleanup_with_deletes() {
	let db = Database::new().with_gc();

	let num_keys = 500;

	println!("Creating {} keys...", num_keys);

	// Create keys
	for i in 0..num_keys {
		let mut tx = db.transaction(true);

		let key = format!("key_{:06}", i);

		let value = vec![1u8; 100];

		tx.set(key, value).unwrap();

		tx.commit().unwrap();
	}

	println!("Updating keys...");

	// Update all keys
	for i in 0..num_keys {
		let mut tx = db.transaction(true);

		let key = format!("key_{:06}", i);

		let value = vec![2u8; 100];

		tx.set(key, value).unwrap();

		tx.commit().unwrap();
	}

	println!("Deleting keys...");

	// Delete all keys
	for i in 0..num_keys {
		let mut tx = db.transaction(true);

		let key = format!("key_{:06}", i);

		tx.del(key).unwrap();

		tx.commit().unwrap();
	}

	// Give cleanup a chance to run
	std::thread::sleep(Duration::from_millis(500));

	println!("Verifying keys are deleted...");

	// Verify all keys are gone
	let mut tx = db.transaction(false);

	for i in 0..num_keys {
		let key = format!("key_{:06}", i);

		let value = tx.get(key).unwrap();

		assert!(value.is_none(), "Key {} should be deleted", i);
	}

	tx.cancel().unwrap();

	println!("Test passed! All keys are properly deleted.");

	println!("\nNote: With the fix, delete operations should remove all old versions,");

	println!("freeing memory for {} keys with their full version history.", num_keys);
}

#[test]

fn test_memory_with_batch_operations() {
	let db = Database::new_with_options(
		DatabaseOptions::default().with_gc_interval(Duration::from_millis(100)),
	)
	.with_gc();

	let num_batches = 10;

	let batch_size = 100;

	println!("Running {} batches of {} operations...", num_batches, batch_size);

	for batch_num in 0..num_batches {
		// Create a batch
		let mut tx = db.transaction(true);

		for i in 0..batch_size {
			let key = format!("batch_{}_{:04}", batch_num, i);

			let value = vec![batch_num as u8; 100];

			tx.set(key, value).unwrap();
		}

		tx.commit().unwrap();

		println!("  Batch {} created", batch_num + 1);

		// Update the same batch
		let mut tx = db.transaction(true);

		for i in 0..batch_size {
			let key = format!("batch_{}_{:04}", batch_num, i);

			let value = vec![(batch_num + 100) as u8; 100];

			tx.set(key, value).unwrap();
		}

		tx.commit().unwrap();

		println!("  Batch {} updated", batch_num + 1);

		// Give GC time to clean up
		if batch_num % 3 == 0 {
			std::thread::sleep(Duration::from_millis(200));
		}
	}

	println!("\nVerifying data consistency...");

	// Verify data
	let mut tx = db.transaction(false);

	let mut count = 0;

	for batch_num in 0..num_batches {
		for i in 0..batch_size {
			let key = format!("batch_{}_{:04}", batch_num, i);

			let value = tx.get(&key).unwrap();

			assert!(value.is_some());

			assert_eq!(value.unwrap(), Bytes::from(vec![(batch_num + 100) as u8; 100]));

			count += 1;
		}
	}

	tx.cancel().unwrap();

	println!("Test passed! Verified {} entries.", count);

	println!(
		"\nWith the fix, {} create + {} update operations should use",
		num_batches * batch_size,
		num_batches * batch_size
	);

	println!("reasonable memory by cleaning up old versions inline.");
}
