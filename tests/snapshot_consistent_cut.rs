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

//! Point-in-time snapshot consistent cut and AOL truncation tests for
//! `SurrealMX`.

#![allow(clippy::items_after_statements)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use surrealmx::{AolMode, Database, DatabaseOptions, FsyncMode, PersistenceOptions, SnapshotMode};
use tempfile::TempDir;

#[test]
fn test_snapshot_point_in_time_consistent_cut() {
	let dir = TempDir::new().unwrap();
	let popts = PersistenceOptions::new(dir.path())
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	let db = Arc::new(Database::new_with_persistence(DatabaseOptions::default(), popts).unwrap());

	// Invariant: k1 == k2 at all times
	{
		let mut tx = db.transaction(true);
		tx.set("pair:A", "0").unwrap();
		tx.set("pair:B", "0").unwrap();
		tx.commit().unwrap();
	}

	let stop_writer = Arc::new(AtomicBool::new(false));

	// Writer thread constantly updates A and B in single transaction
	let writer = {
		let db = Arc::clone(&db);
		let stop = Arc::clone(&stop_writer);
		thread::spawn(move || {
			let mut seq = 1;
			while !stop.load(Ordering::Relaxed) {
				let mut tx = db.transaction(true);
				tx.set("pair:A", seq.to_string()).unwrap();
				tx.set("pair:B", seq.to_string()).unwrap();
				if tx.commit().is_ok() {
					seq += 1;
				}
			}
			seq
		})
	};

	// Allow some writes to occur, then trigger manual snapshot
	thread::sleep(Duration::from_millis(50));
	db.persistence().unwrap().snapshot().unwrap();

	stop_writer.store(true, Ordering::Relaxed);
	let max_seq = writer.join().unwrap();
	assert!(max_seq > 10, "Writer should have performed iterations");

	// Now open a fresh database purely from the snapshot file (ignoring AOL)
	// by pointing to snapshot path in a new dir, or copying snapshot file
	let snap_path = dir.path().join("snapshot.bin");
	assert!(snap_path.exists());

	let snap_only_dir = TempDir::new().unwrap();
	std::fs::copy(&snap_path, snap_only_dir.path().join("snapshot.bin")).unwrap();

	let snap_popts = PersistenceOptions::new(snap_only_dir.path())
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never);

	let recovered_db =
		Database::new_with_persistence(DatabaseOptions::default(), snap_popts).unwrap();
	let tx = recovered_db.transaction(false);
	let a_val = tx.get("pair:A").unwrap().expect("pair:A must exist in snapshot");
	let b_val = tx.get("pair:B").unwrap().expect("pair:B must exist in snapshot");

	assert_eq!(
		a_val,
		b_val,
		"Snapshot contained a torn multi-key state! pair:A={:?}, pair:B={:?}",
		std::str::from_utf8(&a_val),
		std::str::from_utf8(&b_val)
	);
}

#[test]
fn test_snapshot_truncates_aol_and_replays_tail_accurately() {
	let dir = TempDir::new().unwrap();
	let popts = PersistenceOptions::new(dir.path())
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_fsync_mode(FsyncMode::Never);

	let db = Database::new_with_persistence(DatabaseOptions::default(), popts).unwrap();

	const TOTAL_KEYS: usize = 1000;
	const CUTOFF_KEY: usize = 500;

	// Write first batch of keys
	for i in 0..CUTOFF_KEY {
		let mut tx = db.transaction(true);
		tx.set(format!("k{i:04}"), format!("v{i}")).unwrap();
		tx.commit().unwrap();
	}

	// Trigger snapshot and AOL truncation
	db.persistence().unwrap().snapshot().unwrap();

	// Verify AOL file exists and snapshot file exists
	let aol_path = dir.path().join("aol.bin");
	let snap_path = dir.path().join("snapshot.bin");
	assert!(aol_path.exists());
	assert!(snap_path.exists());

	// Write second batch of keys after the cutoff
	for i in CUTOFF_KEY..TOTAL_KEYS {
		let mut tx = db.transaction(true);
		tx.set(format!("k{i:04}"), format!("v{i}")).unwrap();
		tx.commit().unwrap();
	}

	drop(db);

	// Reopen database from disk (recovers snapshot + remaining AOL tail)
	let popts2 = PersistenceOptions::new(dir.path())
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);
	let reopened_db = Database::new_with_persistence(DatabaseOptions::default(), popts2).unwrap();

	let tx = reopened_db.transaction(false);
	for i in 0..TOTAL_KEYS {
		let key = format!("k{i:04}");
		let expected = format!("v{i}");
		let val = tx
			.get(&key)
			.unwrap_or_else(|_| panic!("Failed to read {key}"))
			.unwrap_or_else(|| panic!("Key {key} missing after snapshot + AOL tail replay"));
		assert_eq!(std::str::from_utf8(&val).unwrap(), expected);
	}
}

#[test]
fn test_repeated_snapshots_with_continuous_writes() {
	let dir = TempDir::new().unwrap();
	let popts = PersistenceOptions::new(dir.path())
		.with_aol_mode(AolMode::AsynchronousAfterCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	let db = Database::new_with_persistence(DatabaseOptions::default(), popts).unwrap();

	const CYCLES: usize = 5;
	const WRITES_PER_CYCLE: usize = 200;

	for cycle in 0..CYCLES {
		for i in 0..WRITES_PER_CYCLE {
			let idx = cycle * WRITES_PER_CYCLE + i;
			let mut tx = db.transaction(true);
			tx.set(format!("cycle_key_{idx:05}"), format!("val_{idx}")).unwrap();
			tx.commit().unwrap();
		}
		// Snapshot at each cycle
		db.persistence().unwrap().snapshot().unwrap();
	}

	drop(db);

	// Reopen and check all keys across all cycles are intact
	let popts2 = PersistenceOptions::new(dir.path())
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);
	let reopened_db = Database::new_with_persistence(DatabaseOptions::default(), popts2).unwrap();

	let tx = reopened_db.transaction(false);
	for idx in 0..(CYCLES * WRITES_PER_CYCLE) {
		let key = format!("cycle_key_{idx:05}");
		let expected = format!("val_{idx}");
		let val = tx
			.get(&key)
			.unwrap()
			.unwrap_or_else(|| panic!("Key {key} missing after repeated snapshots"));
		assert_eq!(std::str::from_utf8(&val).unwrap(), expected);
	}
}
