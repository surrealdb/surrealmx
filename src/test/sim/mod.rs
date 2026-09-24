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

//! Deterministic simulation and differential testing framework.

pub mod generator;
pub mod harness;
pub mod model;

pub use generator::{SimAction, WorkloadGenerator};
pub use harness::SimRunner;
pub use model::{ModelDb, ModelError, ModelIsolation, ModelTxn};

#[cfg(test)]
mod tests {
	use super::generator::WorkloadGenerator;
	use super::harness::SimRunner;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::Arc;
	use std::thread;

	#[test]
	fn test_dst_differential_seeds() {
		// Read environment variables or default to a robust testing set
		let steps: usize =
			std::env::var("SURREALMX_SIM_STEPS").ok().and_then(|s| s.parse().ok()).unwrap_or(1000);

		let seed_count: usize =
			std::env::var("SURREALMX_SIM_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(500);

		let base_seeds: Vec<u64> =
			vec![1, 42, 1337, 2026, 99_999, 777_777, 1_234_567, 3_141_592, 2_718_281, 8_888_888];
		let seeds: Vec<u64> = if seed_count <= base_seeds.len() {
			base_seeds[..seed_count].to_vec()
		} else {
			let mut extended = base_seeds;
			for i in 10..seed_count {
				extended.push((i as u64).wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1));
			}
			extended
		};

		let parallelism =
			std::thread::available_parallelism().map_or(4, std::num::NonZero::get).min(64);
		let seeds = Arc::new(seeds);
		let index = Arc::new(AtomicUsize::new(0));
		let mut handles = Vec::with_capacity(parallelism);

		for _ in 0..parallelism {
			let seeds = Arc::clone(&seeds);
			let index = Arc::clone(&index);
			handles.push(std::thread::spawn(move || loop {
				let idx = index.fetch_add(1, Ordering::Relaxed);
				if idx >= seeds.len() {
					break;
				}
				let seed = seeds[idx];
				let mut runner = SimRunner::new_in_memory(seed);
				let gen = WorkloadGenerator::new(seed, 30).with_max_concurrent_txns(12);
				runner.run(gen, steps);
			}));
		}

		for h in handles {
			h.join().expect("simulation worker thread panicked");
		}
	}

	#[test]
	fn simulation_smoke_test() {
		let seed = 0x1234_5678_9ABC_DEF0;
		let mut runner = SimRunner::new_in_memory(seed);
		let gen = WorkloadGenerator::new(seed, 20);
		runner.run(gen, 500);
	}

	#[test]
	fn simulation_multiple_seeds_in_memory() {
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
		if let Ok(seed_str) =
			std::env::var("SURREALMX_SIM_SEED").or_else(|_| std::env::var("SIM_SEED"))
		{
			let seed: u64 = seed_str.parse().expect("SURREALMX_SIM_SEED must be a valid u64");
			let steps: usize = std::env::var("SURREALMX_SIM_STEPS")
				.or_else(|_| std::env::var("SIM_STEPS"))
				.ok()
				.and_then(|s| s.parse().ok())
				.unwrap_or(100_000);

			let mut runner = SimRunner::new_in_memory(seed);
			let gen = WorkloadGenerator::new(seed, 40).with_max_concurrent_txns(16);
			runner.run(gen, steps);
		} else {
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

			for &seed in &base_seeds {
				let mut runner = SimRunner::new_in_memory(seed);
				let gen = WorkloadGenerator::new(seed, 30).with_max_concurrent_txns(12);
				runner.run(gen, 10_000);
			}
		}
	}

	#[cfg(not(target_arch = "wasm32"))]
	#[test]
	fn simulation_persistent_sync_on_commit() {
		let seed = 0xBEEF_CAFE_0123_4567;
		let mut runner = SimRunner::new_persistent(
			seed,
			crate::AolMode::SynchronousOnCommit,
			crate::FsyncMode::Never,
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
			crate::AolMode::SynchronousOnCommit,
			crate::FsyncMode::Never,
		);
		let gen = WorkloadGenerator::new(seed, 15).with_persistence_faults(true);
		runner.run(gen, 1500);
	}

	#[test]
	fn multithreaded_parallel_simulation_runners() {
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
}
