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

//! Integration tests using the Deterministic Simulation Testing (DST) harness
//! and `ModelDb` differential oracle.

mod sim;

use bytes::Bytes;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use sim::{ModelDb, ModelIsolation, SimRunner, WorkloadGenerator};
use std::sync::Arc;
use std::thread;
use surrealmx::{Database, DatabaseOptions};

#[test]
fn simulation_smoke_test() {
	let seed = 0x1234_5678_9ABC_DEF0;
	let mut runner = SimRunner::new_in_memory(seed);
	let gen = WorkloadGenerator::new(seed, 20);
	runner.run(gen, 500);
}

#[test]
fn simulation_multiple_seeds_in_memory() {
	// Run 10 different random seeds with 1,000 steps each
	let seeds = [
		42,
		1337,
		0xDEAD_BEEF,
		0xCAFE_BABE,
		0xFEED_FACE,
		0x0123_4567,
		0x89AB_CDEF,
		999_999,
		7_777_777,
		314_159_265,
	];

	for &seed in &seeds {
		let mut runner = SimRunner::new_in_memory(seed);
		let gen = WorkloadGenerator::new(seed, 25);
		runner.run(gen, 1000);
	}
}

#[test]
fn simulation_high_contention_small_keyspace() {
	// Only 5 distinct keys in the pool - forces extreme write conflicts,
	// read-write collisions, range overlaps, and savepoint rollbacks
	let seed = 0x5555_AAAA_5555_AAAA;
	let mut runner = SimRunner::new_in_memory(seed);
	let gen = WorkloadGenerator::new(seed, 5).with_max_concurrent_txns(12);
	runner.run(gen, 3000);
}

#[test]
fn simulation_deep_savepoint_interleavings() {
	let seed = 0x9876_5432_10FE_DCBA;
	let mut runner = SimRunner::new_in_memory(seed);
	let gen = WorkloadGenerator::new(seed, 15).with_max_concurrent_txns(6);
	runner.run(gen, 2000);
}

#[test]
fn simulation_large_scale_100k_steps() {
	// If SIM_SEED is specified in environment, use it; otherwise run 10 seeds x 10,000 steps
	if let Ok(seed_str) = std::env::var("SIM_SEED") {
		let seed: u64 = seed_str.parse().expect("SIM_SEED must be a valid u64");
		let steps: usize =
			std::env::var("SIM_STEPS").ok().and_then(|s| s.parse().ok()).unwrap_or(100_000);

		println!("Running single seed simulation: seed={seed}, steps={steps}");
		let mut runner = SimRunner::new_in_memory(seed);
		let gen = WorkloadGenerator::new(seed, 40).with_max_concurrent_txns(16);
		runner.run(gen, steps);
	} else {
		// Run 10 seeds x 10,000 steps = 100,000 steps total
		let base_seeds = [
			0x1000_0001,
			0x2000_0002,
			0x3000_0003,
			0x4000_0004,
			0x5000_0005,
			0x6000_0006,
			0x7000_0007,
			0x8000_0008,
			0x9000_0009,
			0xA000_000A,
		];

		for (i, &seed) in base_seeds.iter().enumerate() {
			let mut runner = SimRunner::new_in_memory(seed);
			let gen = WorkloadGenerator::new(seed, 30).with_max_concurrent_txns(12);
			runner.run(gen, 10_000);
			println!("Completed seed batch {}/10 (seed: {:#X})", i + 1, seed);
		}
	}
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn simulation_persistent_sync_on_commit() {
	let seed = 0xBEEF_CAFE_0123_4567;
	let mut runner = SimRunner::new_persistent(
		seed,
		surrealmx::AolMode::SynchronousOnCommit,
		surrealmx::FsyncMode::Never,
	);
	let gen = WorkloadGenerator::new(seed, 20).with_persistence_faults(true);
	runner.run(gen, 1000);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn simulation_persistent_crash_and_reload() {
	let seed = 0x7777_8888_9999_0000;
	let mut runner = SimRunner::new_persistent(
		seed,
		surrealmx::AolMode::SynchronousOnCommit,
		surrealmx::FsyncMode::Never,
	);
	let gen = WorkloadGenerator::new(seed, 15).with_persistence_faults(true);
	runner.run(gen, 1500);
}

#[test]
fn multithreaded_differential_stress_test() {
	const THREADS: usize = 8;
	const TXNS_PER_THREAD: usize = 200;
	const KEY_POOL_SIZE: usize = 20;

	// Concurrent worker threads execute randomized transactions against SurrealMX and ModelDb
	let db = Arc::new(Database::new_with_options(
		DatabaseOptions::default().with_all_workers_disabled(),
	));
	let model = Arc::new(Mutex::new(ModelDb::new()));

	let mut handles = Vec::new();

	for thread_id in 0..THREADS {
		let db = Arc::clone(&db);
		let model = Arc::clone(&model);
		let seed = 0xCAFE_0000u64 + thread_id as u64;

		handles.push(thread::spawn(move || {
			let mut rng = StdRng::seed_from_u64(seed);

			for txn_seq in 0..TXNS_PER_THREAD {
				let write = rng.random_bool(0.7);
				let mode = if rng.random_bool(0.5) {
					ModelIsolation::SerializableSnapshotIsolation
				} else {
					ModelIsolation::SnapshotIsolation
				};

				// Begin atomically in both so snapshots reflect the exact same logical point
				let (mut db_tx, mut model_tx) = {
					let model_tx = model.lock().begin(write, mode);
					let db_tx = db.transaction(write);
					let db_tx = match mode {
						ModelIsolation::SnapshotIsolation => db_tx.with_snapshot_isolation(),
						ModelIsolation::SerializableSnapshotIsolation => {
							db_tx.with_serializable_snapshot_isolation()
						}
					};
					(db_tx, model_tx)
				};

				// Perform 5-15 operations in this transaction
				let ops_count = rng.random_range(5..15);
				for _ in 0..ops_count {
					let key_id = rng.random_range(0..KEY_POOL_SIZE);
					let key = Bytes::from(format!("k_{key_id:04}"));

					if write && rng.random_bool(0.6) {
						let val = Bytes::from(format!("v_{thread_id}_{txn_seq}"));
						if rng.random_bool(0.8) {
							// Set
							let db_res = db_tx.set(&key, &val);
							let model_res = model_tx.set(key.as_ref(), val.as_ref());
							assert_eq!(db_res.is_ok(), model_res.is_ok());
						} else {
							// Del
							let db_res = db_tx.del(&key);
							let model_res = model_tx.del(key.as_ref());
							assert_eq!(db_res.is_ok(), model_res.is_ok());
						}
					} else {
						// Read
						let db_val = db_tx.get(&key).unwrap();
						let model_val = {
							let m = model.lock();
							model_tx.get(&m, key.as_ref()).unwrap()
						};
						assert_eq!(
							db_val,
							model_val,
							"Thread {thread_id} read mismatch on key {:?}",
							String::from_utf8_lossy(&key)
						);
					}
				}

				// Linearize commit against ModelDb
				let (db_res, model_res) = {
					let db_res = db_tx.commit();
					let model_res = model.lock().commit(model_tx);
					(db_res, model_res)
				};

				match (db_res.is_ok(), model_res.is_ok()) {
					(true, true) | (false, false) => {}
					(true, false) => {
						panic!(
							"Thread {thread_id} txn {txn_seq}: SurrealMX committed but ModelDb rejected with {:?}",
							model_res.err()
						);
					}
					(false, true) => {
						panic!(
							"Thread {thread_id} txn {txn_seq}: ModelDb committed but SurrealMX rejected with {:?}",
							db_res.err()
						);
					}
				}
			}
		}));
	}

	for handle in handles {
		handle.join().unwrap();
	}

	// Final verification across full keyspace
	let tx = db.transaction(false);
	let db_all =
		tx.scan(b"".as_slice()..b"\xff\xff\xff\xff".as_slice(), None, None).expect("scan failed");

	let model_all = {
		let model_guard = model.lock();
		model_guard.scan_at_version(b"", b"\xff\xff\xff\xff", model_guard.current_version)
	};

	assert_eq!(db_all, model_all, "Final multithreaded scan mismatch!");
}
