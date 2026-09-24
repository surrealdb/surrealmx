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

use sim::{SimRunner, WorkloadGenerator};
use std::thread;

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
fn multithreaded_parallel_simulation_runners() {
	// Execute multiple independent simulation runs concurrently across worker threads
	const THREADS: usize = 8;
	const STEPS_PER_THREAD: usize = 5_000;

	let mut handles = Vec::new();

	for thread_id in 0..THREADS {
		let seed = 0xCAFE_0000_0000u64 + thread_id as u64;

		handles.push(thread::spawn(move || {
			let mut runner = SimRunner::new_in_memory(seed);
			let gen = WorkloadGenerator::new(seed, 25).with_max_concurrent_txns(8);
			runner.run(gen, STEPS_PER_THREAD);
		}));
	}

	for handle in handles {
		handle.join().expect("simulation worker thread panicked");
	}
}
