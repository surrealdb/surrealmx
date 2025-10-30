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

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;
use surrealmx::{Database, DatabaseOptions};

const SEED: u64 = 42;

// Helper function to create a database with configuration from environment
fn create_database() -> Database {
	Database::new()
}

// Helper functions for generating test data
fn generate_key(rng: &mut StdRng, size: usize) -> Bytes {
	let mut key = vec![0u8; size];
	rng.fill(&mut key[..]);
	Bytes::from(key)
}

fn generate_value(rng: &mut StdRng, size: usize) -> Bytes {
	let mut val = vec![0u8; size];
	rng.fill(&mut val[..]);
	Bytes::from(val)
}

fn generate_sequential_key(index: usize) -> Bytes {
	Bytes::from(format!("key_{:08}", index).into_bytes())
}

fn generate_sequential_value(index: usize, size: usize) -> Bytes {
	let base = format!("value_{:08}", index);
	let mut val = base.into_bytes();
	val.resize(size, b'x');
	Bytes::from(val)
}

fn setup_database_with_data(count: usize, key_size: usize, value_size: usize) -> Database {
	let db = create_database();
	let mut rng = StdRng::seed_from_u64(SEED);

	{
		let mut tx = db.transaction(true);
		for _i in 0..count {
			let key = generate_key(&mut rng, key_size);
			let value = generate_value(&mut rng, value_size);
			tx.put(key, value).unwrap();
		}
		tx.commit().unwrap();
	}

	db
}

fn setup_database_with_sequential_data(count: usize, value_size: usize) -> Database {
	let db = create_database();

	{
		let mut tx = db.transaction(true);
		for i in 0..count {
			let key = generate_sequential_key(i);
			let value = generate_sequential_value(i, value_size);
			tx.put(key, value).unwrap();
		}
		tx.commit().unwrap();
	}

	db
}

// Basic Operations Benchmarks
fn bench_transaction_creation(c: &mut Criterion) {
	let db = create_database();

	c.bench_function("transaction_creation_read", |b| {
		b.iter(|| {
			let tx = db.transaction(false);
			black_box(tx);
		})
	});

	c.bench_function("transaction_creation_write", |b| {
		b.iter(|| {
			let tx = db.transaction(true);
			black_box(tx);
		})
	});
}

fn bench_put_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("put_operations");

	for data_size in [1, 100, 1000, 10_000].iter() {
		for entry_count in [100, 1000, 10_000].iter() {
			let mut rng = StdRng::seed_from_u64(SEED);

			// Pre-generate data for consistent benchmarking
			let test_data: Vec<(Bytes, Bytes)> = (0..*entry_count)
				.map(|_| (generate_key(&mut rng, 16), generate_value(&mut rng, *data_size)))
				.collect();

			group.throughput(Throughput::Elements(*entry_count as u64));
			group.bench_with_input(
				BenchmarkId::new("put", format!("{}b_{}entries", data_size, entry_count)),
				&test_data,
				|b, data| {
					b.iter_batched(
						create_database,
						|db: Database| {
							let mut tx = db.transaction(true);
							for (key, value) in data.iter() {
								tx.put(key.clone(), value.clone()).unwrap();
							}
							tx.commit().unwrap();
						},
						criterion::BatchSize::LargeInput,
					)
				},
			);
		}
	}
	group.finish();
}

fn bench_get_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("get_operations");

	for data_size in [1, 100, 1000, 10_000].iter() {
		for entry_count in [100, 1000, 10_000].iter() {
			let db = setup_database_with_data(*entry_count, 16, *data_size);
			let mut rng = StdRng::seed_from_u64(SEED);

			// Pre-generate keys for lookup
			let lookup_keys: Vec<Bytes> = (0..100).map(|_| generate_key(&mut rng, 16)).collect();

			group.throughput(Throughput::Elements(lookup_keys.len() as u64));
			group.bench_with_input(
				BenchmarkId::new("get", format!("{}b_{}entries", data_size, entry_count)),
				&lookup_keys,
				|b, keys| {
					b.iter(|| {
						let mut tx = db.transaction(false);
						for key in keys {
							black_box(tx.get(key.clone()).unwrap());
						}
						tx.cancel().unwrap();
					})
				},
			);
		}
	}
	group.finish();
}

fn bench_exists_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("exists_operations");

	for entry_count in [100, 1000, 10_000].iter() {
		let db = setup_database_with_data(*entry_count, 16, 100);
		let mut rng = StdRng::seed_from_u64(SEED);

		// Pre-generate keys for lookup
		let lookup_keys: Vec<Bytes> = (0..100).map(|_| generate_key(&mut rng, 16)).collect();

		group.throughput(Throughput::Elements(lookup_keys.len() as u64));
		group.bench_with_input(
			BenchmarkId::new("exists", format!("{}entries", entry_count)),
			&lookup_keys,
			|b, keys| {
				b.iter(|| {
					let mut tx = db.transaction(false);
					for key in keys {
						black_box(tx.exists(key.clone()).unwrap());
					}
					tx.cancel().unwrap();
				})
			},
		);
	}
	group.finish();
}

fn bench_delete_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("delete_operations");

	for entry_count in [100, 1000, 10_000].iter() {
		let mut rng = StdRng::seed_from_u64(SEED);

		// Pre-generate keys for deletion
		let delete_keys: Vec<Bytes> = (0..(*entry_count / 10)) // Delete 10% of entries
			.map(|_| generate_key(&mut rng, 16))
			.collect();

		group.throughput(Throughput::Elements(delete_keys.len() as u64));
		group.bench_with_input(
			BenchmarkId::new("del", format!("{}entries", entry_count)),
			&delete_keys,
			|b, keys| {
				b.iter_batched(
					|| setup_database_with_data(*entry_count, 16, 100),
					|db| {
						let mut tx = db.transaction(true);
						for key in keys {
							tx.del(key.clone()).unwrap();
						}
						tx.commit().unwrap();
					},
					criterion::BatchSize::LargeInput,
				)
			},
		);
	}
	group.finish();
}

// Scan Operations Benchmarks
fn bench_scan_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("scan_operations");

	for entry_count in [1000, 10_000, 100_000].iter() {
		let db = setup_database_with_sequential_data(*entry_count, 100);

		for scan_limit in [10, 100, 1000].iter() {
			let limit = std::cmp::min(*scan_limit, *entry_count);

			group.throughput(Throughput::Elements(limit as u64));
			group.bench_with_input(
				BenchmarkId::new("scan", format!("{}entries_limit{}", entry_count, limit)),
				&limit,
				|b, &limit| {
					b.iter(|| {
						let mut tx = db.transaction(false);
						let start_key = b"".to_vec();
						let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
						let result = tx.scan(start_key..end_key, None, Some(limit)).unwrap();
						black_box(result);
						tx.cancel().unwrap();
					})
				},
			);
		}
	}
	group.finish();
}

fn bench_keys_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("keys_operations");

	for entry_count in [1000, 10_000, 100_000].iter() {
		let db = setup_database_with_sequential_data(*entry_count, 100);

		for scan_limit in [10, 100, 1000].iter() {
			let limit = std::cmp::min(*scan_limit, *entry_count);

			group.throughput(Throughput::Elements(limit as u64));
			group.bench_with_input(
				BenchmarkId::new("keys", format!("{}entries_limit{}", entry_count, limit)),
				&limit,
				|b, &limit| {
					b.iter(|| {
						let mut tx = db.transaction(false);
						let start_key = b"".to_vec();
						let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
						let result = tx.keys(start_key..end_key, None, Some(limit)).unwrap();
						black_box(result);
						tx.cancel().unwrap();
					})
				},
			);
		}
	}
	group.finish();
}

fn bench_total_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("total_operations");

	for entry_count in [1000, 10_000, 100_000].iter() {
		let db = setup_database_with_sequential_data(*entry_count, 100);

		group.bench_with_input(
			BenchmarkId::new("total", format!("{}entries", entry_count)),
			entry_count,
			|b, _| {
				b.iter(|| {
					let mut tx = db.transaction(false);
					let start_key = b"".to_vec();
					let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
					let result = tx.total(start_key..end_key, None, None).unwrap();
					black_box(result);
					tx.cancel().unwrap();
				})
			},
		);
	}
	group.finish();
}

// Concurrent Operations Benchmarks
fn bench_concurrent_readers(c: &mut Criterion) {
	let mut group = c.benchmark_group("concurrent_readers");

	// Test with different thread counts: 1, 4, and CPU cores
	let cpu_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
	let thread_counts = [1, 4, cpu_cores];

	for entry_count in [1000, 10_000].iter() {
		let db = Arc::new(setup_database_with_sequential_data(*entry_count, 100));
		let mut rng = StdRng::seed_from_u64(SEED);

		// Pre-generate keys for lookup (more keys for better distribution across threads)
		let lookup_keys: Vec<Bytes> =
			(0..200).map(|_| generate_sequential_key(rng.random_range(0..*entry_count))).collect();

		for &thread_count in thread_counts.iter() {
			group.throughput(Throughput::Elements((lookup_keys.len() * thread_count) as u64));
			group.bench_with_input(
				BenchmarkId::new(
					"concurrent_read",
					format!("{}entries_{}threads", entry_count, thread_count),
				),
				&(lookup_keys.clone(), thread_count),
				|b, (keys, num_threads)| {
					b.iter(|| {
						let handles: Vec<_> = (0..*num_threads)
							.map(|_| {
								let db: Arc<Database> = Arc::clone(&db);
								let keys = keys.clone();
								std::thread::spawn(move || {
									let mut tx = db.transaction(false);
									let mut results = Vec::new();
									for key in keys {
										results.push(tx.get(key.clone()).unwrap());
									}
									tx.cancel().unwrap();
									results
								})
							})
							.collect();

						let results: Vec<_> =
							handles.into_iter().map(|h| h.join().unwrap()).collect();
						black_box(results);
					})
				},
			);
		}
	}
	group.finish();
}

// Concurrent Insert/Update Operations Benchmarks
fn bench_concurrent_writers(c: &mut Criterion) {
	let mut group = c.benchmark_group("concurrent_writers");

	// Test with different thread counts: 1, 4, and CPU cores
	let cpu_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
	let thread_counts = [1, 4, cpu_cores];

	for entry_count in [1000, 10_000].iter() {
		for &thread_count in thread_counts.iter() {
			let operations_per_thread = 50; // Each thread performs 50 operations

			group.throughput(Throughput::Elements((operations_per_thread * thread_count) as u64));
			group.bench_with_input(
				BenchmarkId::new(
					"concurrent_inserts",
					format!("{}entries_{}threads", entry_count, thread_count),
				),
				&(entry_count, thread_count, operations_per_thread),
				|b, &(entry_count, num_threads, ops_per_thread)| {
					b.iter_batched(
						|| {
							// Setup database with initial data
							let db =
								Arc::new(setup_database_with_sequential_data(*entry_count, 100));

							// Pre-generate operations for each thread to avoid RNG contention
							let mut all_operations = Vec::new();
							let mut rng = StdRng::seed_from_u64(SEED);

							for thread_id in 0..num_threads {
								let mut thread_ops = Vec::new();
								for op_id in 0..ops_per_thread {
									let operation_type = op_id % 3; // 0=insert, 1=update, 2=upsert
									let base_key_id = thread_id * 1000 + op_id; // Avoid key conflicts between threads

									match operation_type {
										0 => {
											// Insert new key
											let key = Bytes::from(
												format!("new_key_{}_{}", thread_id, op_id)
													.into_bytes(),
											);
											let value = generate_value(&mut rng, 100);
											thread_ops.push(("insert", key, value));
										}
										1 => {
											// Update existing key
											let key =
												generate_sequential_key(base_key_id % *entry_count);
											let value = generate_value(&mut rng, 100);
											thread_ops.push(("update", key, value));
										}
										2 => {
											// Upsert (put - could be insert or update)
											let key = if op_id % 2 == 0 {
												generate_sequential_key(base_key_id % *entry_count) // Existing key
											} else {
												Bytes::from(
													format!("upsert_key_{}_{}", thread_id, op_id)
														.into_bytes(),
												) // New key
											};
											let value = generate_value(&mut rng, 100);
											thread_ops.push(("upsert", key, value));
										}
										_ => unreachable!(),
									}
								}
								all_operations.push(thread_ops);
							}

							(db, all_operations)
						},
						|(db, operations_per_thread)| {
							let handles: Vec<_> = operations_per_thread
								.into_iter()
								.map(|thread_operations| {
									let db: Arc<Database> = Arc::clone(&db);
									std::thread::spawn(move || {
										let mut tx = db.transaction(true);
										let mut results = Vec::new();

										for (op_type, key, value) in thread_operations {
											match op_type {
												"insert" => {
													// For inserts, use putc to ensure we're creating new entries
													let result = tx.putc(key, value, None);
													results.push(result.is_ok());
												}
												"update" => {
													// For updates, we don't check if key exists (simpler)
													let result = tx.put(key, value);
													results.push(result.is_ok());
												}
												"upsert" => {
													// Standard put operation (insert or update)
													let result = tx.put(key, value);
													results.push(result.is_ok());
												}
												_ => unreachable!(),
											}
										}

										tx.commit().unwrap();
										results
									})
								})
								.collect();

							let results: Vec<_> =
								handles.into_iter().map(|h| h.join().unwrap()).collect();
							black_box(results);
						},
						criterion::BatchSize::LargeInput,
					)
				},
			);
		}
	}
	group.finish();
}

// Mixed Concurrent Operations (Readers + Writers)
fn bench_concurrent_mixed(c: &mut Criterion) {
	let mut group = c.benchmark_group("concurrent_mixed");

	// Test with different thread configurations
	let cpu_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
	let half_cores = (cpu_cores / 2).max(1);
	let cpu_config_name = format!("{}r_{}w", half_cores, half_cores);

	for entry_count in [1000, 10_000].iter() {
		let configurations = vec![
			("2r_2w", 2, 2),
			("4r_4w", 4, 4),
			(cpu_config_name.as_str(), half_cores, half_cores),
		];

		for (config_name, reader_threads, writer_threads) in configurations {
			let operations_per_thread = 25;
			let total_ops = (reader_threads + writer_threads) * operations_per_thread;

			group.throughput(Throughput::Elements(total_ops as u64));
			group.bench_with_input(
				BenchmarkId::new("mixed", format!("{}entries_{}", entry_count, config_name)),
				&(entry_count, reader_threads, writer_threads, operations_per_thread),
				|b, &(entry_count, num_readers, num_writers, ops_per_thread)| {
					b.iter_batched(
						|| {
							// Setup database and pre-generate operations
							let db =
								Arc::new(setup_database_with_sequential_data(*entry_count, 100));
							let mut rng = StdRng::seed_from_u64(SEED);

							// Generate read keys
							let read_keys: Vec<Bytes> = (0..ops_per_thread)
								.map(|_| generate_sequential_key(rng.random_range(0..*entry_count)))
								.collect();

							// Generate write operations
							let write_ops: Vec<(Bytes, Bytes)> = (0..ops_per_thread)
								.map(|i| {
									let key =
										Bytes::from(format!("mixed_write_{}", i).into_bytes());
									let value = generate_value(&mut rng, 100);
									(key, value)
								})
								.collect();

							(db, read_keys, write_ops)
						},
						|(db, read_keys, write_ops)| {
							let mut handles = Vec::new();

							// Spawn reader threads
							for _ in 0..num_readers {
								let db: Arc<Database> = Arc::clone(&db);
								let keys = read_keys.clone();
								let handle = std::thread::spawn(move || {
									let mut tx = db.transaction(false);
									let mut results = Vec::new();
									for key in keys {
										results.push(tx.get(key).unwrap());
									}
									tx.cancel().unwrap();
									results.len()
								});
								handles.push(handle);
							}

							// Spawn writer threads
							for writer_id in 0..num_writers {
								let db: Arc<Database> = Arc::clone(&db);
								let ops = write_ops.clone();
								let handle = std::thread::spawn(move || {
									let mut tx = db.transaction(true);
									let mut success_count = 0;
									for (key, value) in &ops {
										// Make keys unique per writer thread
										let unique_key = Bytes::from(
											format!(
												"{}_w{}",
												String::from_utf8_lossy(key),
												writer_id
											)
											.into_bytes(),
										);
										if tx.put(unique_key, value.clone()).is_ok() {
											success_count += 1;
										}
									}
									tx.commit().unwrap();
									success_count
								});
								handles.push(handle);
							}

							let results: Vec<_> =
								handles.into_iter().map(|h| h.join().unwrap()).collect();
							black_box(results);
						},
						criterion::BatchSize::LargeInput,
					)
				},
			);
		}
	}
	group.finish();
}

// Mixed Workload Benchmarks
fn bench_mixed_workload(c: &mut Criterion) {
	let mut group = c.benchmark_group("mixed_workload");

	for entry_count in [1000, 10_000].iter() {
		let mut rng = StdRng::seed_from_u64(SEED);

		// Generate operations mix: 70% reads, 20% writes, 10% deletes
		// Use existing keys for reads/deletes, new keys for writes
		let mut operations = Vec::new();
		for i in 0..70 {
			operations.push(("read", generate_sequential_key(i % *entry_count)));
		}
		for _ in 0..20 {
			operations.push(("write", generate_key(&mut rng, 16)));
		}
		for i in 0..10 {
			operations.push(("delete", generate_sequential_key(i % *entry_count)));
		}

		group.throughput(Throughput::Elements(operations.len() as u64));
		group.bench_with_input(
			BenchmarkId::new("mixed_70r_20w_10d", format!("{}entries", entry_count)),
			&operations,
			|b, ops| {
				b.iter_batched(
					|| setup_database_with_sequential_data(*entry_count, 100),
					|db| {
						let mut tx = db.transaction(true);
						for (op_type, key) in ops {
							match *op_type {
								"read" => {
									black_box(tx.get(key.clone()).unwrap());
								}
								"write" => {
									let value =
										generate_value(&mut StdRng::seed_from_u64(SEED), 100);
									tx.put(key.clone(), value).unwrap();
								}
								"delete" => {
									let _ = tx.del(key.clone()); // Ignore error if key doesn't exist
								}
								_ => unreachable!(),
							}
						}
						tx.commit().unwrap();
					},
					criterion::BatchSize::LargeInput,
				)
			},
		);
	}
	group.finish();
}

// Database Configuration Benchmarks
fn bench_database_options(c: &mut Criterion) {
	let mut group = c.benchmark_group("database_options");

	// Test different database configurations
	let configurations = vec![
		("default", DatabaseOptions::default()),
		(
			"no_gc",
			DatabaseOptions {
				enable_gc: false,
				..DatabaseOptions::default()
			},
		),
		(
			"no_cleanup",
			DatabaseOptions {
				enable_cleanup: false,
				..DatabaseOptions::default()
			},
		),
		(
			"no_bg_threads",
			DatabaseOptions {
				enable_gc: false,
				enable_cleanup: false,
				..DatabaseOptions::default()
			},
		),
	];

	for (config_name, options) in configurations {
		let mut rng = StdRng::seed_from_u64(SEED);

		// Pre-generate data for consistent benchmarking
		let test_data: Vec<(Bytes, Bytes)> = (0..1000)
			.map(|_| (generate_key(&mut rng, 16), generate_value(&mut rng, 100)))
			.collect();

		group.bench_with_input(
			BenchmarkId::new("put_1000_entries", config_name),
			&test_data,
			|b, data| {
				b.iter_batched(
					|| Database::new_with_options(options.clone()),
					|db: Database| {
						let mut tx = db.transaction(true);
						for (key, value) in data.iter() {
							tx.put(key.clone(), value.clone()).unwrap();
						}
						tx.commit().unwrap();
					},
					criterion::BatchSize::LargeInput,
				)
			},
		);
	}
	group.finish();
}

criterion_group!(
	database_benchmarks,
	bench_transaction_creation,
	bench_put_operations,
	bench_get_operations,
	bench_exists_operations,
	bench_delete_operations
);

criterion_group!(
	scan_benchmarks,
	bench_scan_operations,
	bench_keys_operations,
	bench_total_operations
);

criterion_group!(
	concurrent_benchmarks,
	bench_concurrent_readers,
	bench_concurrent_writers,
	bench_concurrent_mixed,
	bench_mixed_workload
);

criterion_group!(configuration_benchmarks, bench_database_options);

criterion_main!(
	database_benchmarks,
	scan_benchmarks,
	concurrent_benchmarks,
	configuration_benchmarks
);
