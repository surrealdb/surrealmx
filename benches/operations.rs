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
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::{rngs::StdRng, RngExt, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;
use surrealmx::bench_internals::{
	MergeQueueScenario, ReadsetConflictScenario, WatermarkScanScenario, WritesetConflictScenario,
};
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
	Bytes::from(format!("key_{index:08}").into_bytes())
}

fn generate_sequential_value(index: usize, size: usize) -> Bytes {
	let base = format!("value_{index:08}");
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
		});
	});

	c.bench_function("transaction_creation_write", |b| {
		b.iter(|| {
			let tx = db.transaction(true);
			black_box(tx);
		});
	});
}

fn bench_put_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("put_operations");

	for data_size in &[1, 100, 1000, 10_000] {
		for entry_count in &[100, 1000, 10_000] {
			let mut rng = StdRng::seed_from_u64(SEED);

			// Pre-generate data for consistent benchmarking
			let test_data: Vec<(Bytes, Bytes)> = (0..*entry_count)
				.map(|_| (generate_key(&mut rng, 16), generate_value(&mut rng, *data_size)))
				.collect();

			group.throughput(Throughput::Elements(*entry_count as u64));
			group.bench_with_input(
				BenchmarkId::new("put", format!("{data_size}b_{entry_count}entries")),
				&test_data,
				|b, data| {
					b.iter_batched(
						create_database,
						|db: Database| {
							let mut tx = db.transaction(true);
							for (key, value) in data {
								tx.put(key.clone(), value.clone()).unwrap();
							}
							tx.commit().unwrap();
						},
						criterion::BatchSize::LargeInput,
					);
				},
			);
		}
	}
	group.finish();
}

fn bench_get_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("get_operations");

	for data_size in &[1, 100, 1000, 10_000] {
		for entry_count in &[100, 1000, 10_000] {
			let db = setup_database_with_data(*entry_count, 16, *data_size);
			let mut rng = StdRng::seed_from_u64(SEED);

			// Pre-generate keys for lookup
			let lookup_keys: Vec<Bytes> = (0..100).map(|_| generate_key(&mut rng, 16)).collect();

			group.throughput(Throughput::Elements(lookup_keys.len() as u64));
			group.bench_with_input(
				BenchmarkId::new("get", format!("{data_size}b_{entry_count}entries")),
				&lookup_keys,
				|b, keys| {
					b.iter(|| {
						let mut tx = db.transaction(false);
						for key in keys {
							black_box(tx.get(key.clone()).unwrap());
						}
						tx.cancel().unwrap();
					});
				},
			);
		}
	}
	group.finish();
}

fn bench_exists_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("exists_operations");

	for entry_count in &[100, 1000, 10_000] {
		let db = setup_database_with_data(*entry_count, 16, 100);
		let mut rng = StdRng::seed_from_u64(SEED);

		// Pre-generate keys for lookup
		let lookup_keys: Vec<Bytes> = (0..100).map(|_| generate_key(&mut rng, 16)).collect();

		group.throughput(Throughput::Elements(lookup_keys.len() as u64));
		group.bench_with_input(
			BenchmarkId::new("exists", format!("{entry_count}entries")),
			&lookup_keys,
			|b, keys| {
				b.iter(|| {
					let mut tx = db.transaction(false);
					for key in keys {
						black_box(tx.exists(key.clone()).unwrap());
					}
					tx.cancel().unwrap();
				});
			},
		);
	}
	group.finish();
}

fn bench_delete_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("delete_operations");

	for entry_count in &[100, 1000, 10_000] {
		let mut rng = StdRng::seed_from_u64(SEED);

		// Pre-generate keys for deletion
		let delete_keys: Vec<Bytes> = (0..(*entry_count / 10)) // Delete 10% of entries
			.map(|_| generate_key(&mut rng, 16))
			.collect();

		group.throughput(Throughput::Elements(delete_keys.len() as u64));
		group.bench_with_input(
			BenchmarkId::new("del", format!("{entry_count}entries")),
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
				);
			},
		);
	}
	group.finish();
}

// Scan Operations Benchmarks
fn bench_scan_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("scan_operations");

	for entry_count in &[1000, 10_000, 100_000] {
		let db = setup_database_with_sequential_data(*entry_count, 100);

		for scan_limit in &[10, 100, 1000] {
			let limit = std::cmp::min(*scan_limit, *entry_count);

			group.throughput(Throughput::Elements(limit as u64));
			group.bench_with_input(
				BenchmarkId::new("scan", format!("{entry_count}entries_limit{limit}")),
				&limit,
				|b, &limit| {
					b.iter(|| {
						let mut tx = db.transaction(false);
						let start_key = b"".to_vec();
						let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
						let result = tx.scan(start_key..end_key, None, Some(limit)).unwrap();
						black_box(result);
						tx.cancel().unwrap();
					});
				},
			);
		}
	}
	group.finish();
}

fn bench_keys_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("keys_operations");

	for entry_count in &[1000, 10_000, 100_000] {
		let db = setup_database_with_sequential_data(*entry_count, 100);

		for scan_limit in &[10, 100, 1000] {
			let limit = std::cmp::min(*scan_limit, *entry_count);

			group.throughput(Throughput::Elements(limit as u64));
			group.bench_with_input(
				BenchmarkId::new("keys", format!("{entry_count}entries_limit{limit}")),
				&limit,
				|b, &limit| {
					b.iter(|| {
						let mut tx = db.transaction(false);
						let start_key = b"".to_vec();
						let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
						let result = tx.keys(start_key..end_key, None, Some(limit)).unwrap();
						black_box(result);
						tx.cancel().unwrap();
					});
				},
			);
		}
	}
	group.finish();
}

fn bench_total_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("total_operations");

	for entry_count in &[1000, 10_000, 100_000] {
		let db = setup_database_with_sequential_data(*entry_count, 100);

		group.bench_with_input(
			BenchmarkId::new("total", format!("{entry_count}entries")),
			entry_count,
			|b, _| {
				b.iter(|| {
					let mut tx = db.transaction(false);
					let start_key = b"".to_vec();
					let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
					let result = tx.total(start_key..end_key, None, None).unwrap();
					black_box(result);
					tx.cancel().unwrap();
				});
			},
		);
	}
	group.finish();
}

// Cursor-based iteration benchmarks (exercises persistent MergeIterator in
// Cursor)
fn bench_cursor_scan(c: &mut Criterion) {
	let mut group = c.benchmark_group("cursor_scan");

	for entry_count in &[1000, 10_000, 100_000] {
		let db = setup_database_with_sequential_data(*entry_count, 100);

		for scan_limit in &[10, 100, 1000] {
			let limit = std::cmp::min(*scan_limit, *entry_count);

			group.throughput(Throughput::Elements(limit as u64));

			group.bench_with_input(
				BenchmarkId::new("cursor_fwd", format!("{entry_count}entries_limit{limit}")),
				&limit,
				|b, &limit| {
					b.iter(|| {
						let tx = db.transaction(false);

						let start_key = b"".to_vec();
						let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();

						let mut cursor = tx.cursor(start_key..end_key).unwrap();
						cursor.seek_to_first();

						let mut count = 0;
						while cursor.valid() && count < limit {
							if cursor.exists() {
								black_box(cursor.key());
								black_box(cursor.value());
								count += 1;
							}
							cursor.next();
						}

						black_box(count);
					});
				},
			);
		}
	}

	group.finish();
}

// Iterator-based scan benchmarks (ScanIterator wrapping Cursor)
fn bench_scan_iter(c: &mut Criterion) {
	let mut group = c.benchmark_group("scan_iter_ops");

	for entry_count in &[1000, 10_000, 100_000] {
		let db = setup_database_with_sequential_data(*entry_count, 100);

		for scan_limit in &[10, 100, 1000] {
			let limit = std::cmp::min(*scan_limit, *entry_count);

			group.throughput(Throughput::Elements(limit as u64));

			group.bench_with_input(
				BenchmarkId::new("scan_iter", format!("{entry_count}entries_limit{limit}")),
				&limit,
				|b, &limit| {
					b.iter(|| {
						let tx = db.transaction(false);

						let start_key = b"".to_vec();
						let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();

						let iter = tx.scan_iter(start_key..end_key).unwrap();

						let result: Vec<_> = iter.take(limit).collect();

						black_box(result);
					});
				},
			);
		}
	}

	group.finish();
}

// Iterator-based keys benchmarks (KeyIterator wrapping Cursor)
fn bench_keys_iter(c: &mut Criterion) {
	let mut group = c.benchmark_group("keys_iter_ops");

	for entry_count in &[1000, 10_000, 100_000] {
		let db = setup_database_with_sequential_data(*entry_count, 100);

		for scan_limit in &[10, 100, 1000] {
			let limit = std::cmp::min(*scan_limit, *entry_count);

			group.throughput(Throughput::Elements(limit as u64));

			group.bench_with_input(
				BenchmarkId::new("keys_iter", format!("{entry_count}entries_limit{limit}")),
				&limit,
				|b, &limit| {
					b.iter(|| {
						let tx = db.transaction(false);

						let start_key = b"".to_vec();
						let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();

						let iter = tx.keys_iter(start_key..end_key).unwrap();

						let result: Vec<_> = iter.take(limit).collect();

						black_box(result);
					});
				},
			);
		}
	}

	group.finish();
}

// Concurrent Operations Benchmarks
fn bench_concurrent_readers(c: &mut Criterion) {
	let mut group = c.benchmark_group("concurrent_readers");

	// Test with different thread counts: 1, 4, and CPU cores
	let cpu_cores = std::thread::available_parallelism().map_or(8, std::num::NonZero::get);
	let thread_counts = [1, 4, cpu_cores];

	for entry_count in &[1000, 10_000] {
		let db = Arc::new(setup_database_with_sequential_data(*entry_count, 100));
		let mut rng = StdRng::seed_from_u64(SEED);

		// Pre-generate keys for lookup (more keys for better distribution across
		// threads)
		let lookup_keys: Vec<Bytes> =
			(0..200).map(|_| generate_sequential_key(rng.random_range(0..*entry_count))).collect();

		for &thread_count in &thread_counts {
			group.throughput(Throughput::Elements((lookup_keys.len() * thread_count) as u64));
			group.bench_with_input(
				BenchmarkId::new(
					"concurrent_read",
					format!("{entry_count}entries_{thread_count}threads"),
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
					});
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
	let cpu_cores = std::thread::available_parallelism().map_or(8, std::num::NonZero::get);
	let thread_counts = [1, 4, cpu_cores];

	for entry_count in &[1000, 10_000] {
		for &thread_count in &thread_counts {
			let operations_per_thread = 50; // Each thread performs 50 operations

			group.throughput(Throughput::Elements((operations_per_thread * thread_count) as u64));
			group.bench_with_input(
				BenchmarkId::new(
					"concurrent_inserts",
					format!("{entry_count}entries_{thread_count}threads"),
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
												format!("new_key_{thread_id}_{op_id}").into_bytes(),
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
													format!("upsert_key_{thread_id}_{op_id}")
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
													// For inserts, use putc to ensure we're
													// creating new entries
													let result = tx.putc(key, value, None::<&[u8]>);
													results.push(result.is_ok());
												}
												"update" => {
													// For updates, we don't check if key exists
													// (simpler)
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
					);
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
	let cpu_cores = std::thread::available_parallelism().map_or(8, std::num::NonZero::get);
	let half_cores = (cpu_cores / 2).max(1);
	let cpu_config_name = format!("{half_cores}r_{half_cores}w");

	for entry_count in &[1000, 10_000] {
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
				BenchmarkId::new("mixed", format!("{entry_count}entries_{config_name}")),
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
									let key = Bytes::from(format!("mixed_write_{i}").into_bytes());
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
					);
				},
			);
		}
	}
	group.finish();
}

// Mixed Workload Benchmarks
fn bench_mixed_workload(c: &mut Criterion) {
	let mut group = c.benchmark_group("mixed_workload");

	for entry_count in &[1000, 10_000] {
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
			BenchmarkId::new("mixed_70r_20w_10d", format!("{entry_count}entries")),
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
				);
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
						for (key, value) in data {
							tx.put(key.clone(), value.clone()).unwrap();
						}
						tx.commit().unwrap();
					},
					criterion::BatchSize::LargeInput,
				);
			},
		);
	}
	group.finish();
}

// Callback-based scan benchmarks (scan_for_each vs scan)
fn bench_scan_for_each(c: &mut Criterion) {
	let mut group = c.benchmark_group("scan_for_each_vs_vec");

	for entry_count in &[1000, 10_000, 100_000] {
		let db = setup_database_with_sequential_data(*entry_count, 100);
		let mut seen_limits = std::collections::HashSet::new();

		for scan_limit in &[100, 1000, 10_000] {
			let limit = std::cmp::min(*scan_limit, *entry_count);
			if !seen_limits.insert(limit) {
				continue;
			}
			let id = format!("{entry_count}entries_limit{limit}");

			group.throughput(Throughput::Elements(limit as u64));

			group.bench_with_input(BenchmarkId::new("scan_vec", &id), &limit, |b, &limit| {
				b.iter(|| {
					let mut tx = db.transaction(false);
					let start_key = b"".to_vec();
					let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
					let result = tx.scan(start_key..end_key, None, Some(limit)).unwrap();
					black_box(result);
					tx.cancel().unwrap();
				});
			});

			group.bench_with_input(BenchmarkId::new("scan_for_each", &id), &limit, |b, &limit| {
				b.iter(|| {
					let tx = db.transaction(false);
					let start_key = b"".to_vec();
					let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
					let mut count = 0usize;
					tx.scan_for_each(start_key..end_key, None, Some(limit), |k, v| {
						black_box((k, v));
						count += 1;
						true
					})
					.unwrap();
					black_box(count);
				});
			});

			group.bench_with_input(
				BenchmarkId::new("scan_into_reuse", &id),
				&limit,
				|b, &limit| {
					let mut buf = Vec::with_capacity(limit);
					b.iter(|| {
						let tx = db.transaction(false);
						let start_key = b"".to_vec();
						let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
						tx.scan_into(start_key..end_key, None, Some(limit), &mut buf).unwrap();
						black_box(buf.len());
					});
				},
			);
		}
	}
	group.finish();
}

// Keys callback benchmarks
fn bench_keys_for_each(c: &mut Criterion) {
	let mut group = c.benchmark_group("keys_for_each_vs_vec");

	for entry_count in &[1000, 10_000, 100_000] {
		let db = setup_database_with_sequential_data(*entry_count, 100);
		let mut seen_limits = std::collections::HashSet::new();

		for scan_limit in &[100, 1000, 10_000] {
			let limit = std::cmp::min(*scan_limit, *entry_count);
			if !seen_limits.insert(limit) {
				continue;
			}
			let id = format!("{entry_count}entries_limit{limit}");

			group.throughput(Throughput::Elements(limit as u64));

			group.bench_with_input(BenchmarkId::new("keys_vec", &id), &limit, |b, &limit| {
				b.iter(|| {
					let mut tx = db.transaction(false);
					let start_key = b"".to_vec();
					let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
					let result = tx.keys(start_key..end_key, None, Some(limit)).unwrap();
					black_box(result);
					tx.cancel().unwrap();
				});
			});

			group.bench_with_input(BenchmarkId::new("keys_for_each", &id), &limit, |b, &limit| {
				b.iter(|| {
					let tx = db.transaction(false);
					let start_key = b"".to_vec();
					let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
					let mut count = 0usize;
					tx.keys_for_each(start_key..end_key, None, Some(limit), |k| {
						black_box(k);
						count += 1;
						true
					})
					.unwrap();
					black_box(count);
				});
			});

			group.bench_with_input(
				BenchmarkId::new("keys_into_reuse", &id),
				&limit,
				|b, &limit| {
					let mut buf = Vec::with_capacity(limit);
					b.iter(|| {
						let tx = db.transaction(false);
						let start_key = b"".to_vec();
						let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
						tx.keys_into(start_key..end_key, None, Some(limit), &mut buf).unwrap();
						black_box(buf.len());
					});
				},
			);
		}
	}
	group.finish();
}

// ---------------------------------------------------------------------------
// GC benchmarks: incremental dirty-key GC vs full scan
// ---------------------------------------------------------------------------

// Safety-net full scan: large datastore with a departed reader's pinned
// garbage on a subset of keys. Measures the cost of the periodic sweep,
// which grows linearly with total keys regardless of garbage volume.
fn bench_gc_full_scan(c: &mut Criterion) {
	let mut group = c.benchmark_group("gc_full_scan");
	// Fixed number of keys carrying stale versions
	let stale_count = 100;

	for total_keys in &[1_000, 10_000, 100_000] {
		// Setup helper: create datastore, then overwrite a small subset
		// while a reader pins the old versions, then drop the reader —
		// exactly the garbage shape only a background sweep can reclaim.
		let setup = || {
			let db =
				Database::new_with_options(DatabaseOptions::default().with_all_workers_disabled());
			// Insert initial data
			{
				let mut tx = db.transaction(true);
				for i in 0..*total_keys {
					let key = generate_sequential_key(i);
					let value = generate_sequential_value(i, 100);
					tx.put(key, value).unwrap();
				}
				tx.commit().unwrap();
			}
			// Pin the initial versions with a reader while overwriting
			{
				let reader = db.transaction(false);
				let mut tx = db.transaction(true);
				for i in 0..stale_count {
					let key = generate_sequential_key(i);
					let value = Bytes::from(format!("updated_{i:08}").into_bytes());
					tx.set(key, value).unwrap();
				}
				tx.commit().unwrap();
				drop(reader);
			}
			db
		};

		// Full-scan GC: grows linearly with total keys. The database is
		// returned from the routine so its O(n) teardown is dropped
		// outside the timed region rather than polluting the measurement.
		group.bench_function(
			BenchmarkId::new("full_gc", format!("{total_keys}total_{stale_count}stale")),
			|b| {
				b.iter_batched(
					setup,
					|db| {
						db.run_gc();
						db
					},
					criterion::BatchSize::LargeInput,
				);
			},
		);

		// Tracked sweep: visits only the keys commits tracked as holding
		// garbage, so cost scales with the stale count, not total keys
		group.bench_function(
			BenchmarkId::new("tracked_gc", format!("{total_keys}total_{stale_count}stale")),
			|b| {
				b.iter_batched(
					setup,
					|db| {
						db.run_gc_tracked();
						db
					},
					criterion::BatchSize::LargeInput,
				);
			},
		);
	}
	group.finish();
}

// Inline commit-time GC overhead: the per-commit cost of the watermark
// slot scan plus the chain trim under the already-held write lock. The
// hot-key arm overwrites a single key per commit (worst-case relative
// overhead); the wide arm amortises the scan across a large writeset.
fn bench_commit_inline_gc(c: &mut Criterion) {
	let mut group = c.benchmark_group("commit_inline_gc");

	// Hot key: repeated single-key overwrite commits
	group.bench_function("hot_key_overwrite", |b| {
		let db = Database::new_with_options(DatabaseOptions::default().with_all_workers_disabled());
		let key = generate_sequential_key(0);
		let value = generate_sequential_value(0, 100);
		b.iter(|| {
			let mut tx = db.transaction(true);
			tx.set(key.clone(), value.clone()).unwrap();
			tx.commit().unwrap();
		});
	});

	// Wide writeset: one commit overwriting many keys
	group.bench_function("wide_writeset_1000", |b| {
		let db = Database::new_with_options(DatabaseOptions::default().with_all_workers_disabled());
		let keys: Vec<Bytes> = (0..1_000).map(generate_sequential_key).collect();
		let value = generate_sequential_value(0, 100);
		b.iter(|| {
			let mut tx = db.transaction(true);
			for key in &keys {
				tx.set(key.clone(), value.clone()).unwrap();
			}
			tx.commit().unwrap();
		});
	});

	group.finish();
}

// Per-commit inline-GC watermark scan: every commit walks the registered
// reader slots to find the minimum pinned snapshot. Measures how that
// walk scales with the number of live transactions, isolated from chain
// growth and every other commit cost.
fn bench_watermark_scan(c: &mut Criterion) {
	let mut group = c.benchmark_group("watermark_scan");
	for num_readers in &[0usize, 10, 100, 1_000, 10_000] {
		let scenario = WatermarkScanScenario::new(*num_readers);
		group.bench_function(BenchmarkId::new("live_readers", num_readers), |b| {
			b.iter(|| scenario.scan());
		});
	}
	group.finish();
}

// ---------------------------------------------------------------------------
// Bloom filter impact: direct with-bloom vs without-bloom comparison
// ---------------------------------------------------------------------------

// Readset conflict detection: bloom filter vs exact HashSet intersection.
// Isolates just the conflict check cost with disjoint keys (no conflict).
fn bench_readset_bloom_impact(c: &mut Criterion) {
	let mut group = c.benchmark_group("readset_bloom_impact");

	for readset_size in &[100, 1_000, 10_000] {
		// Committed transaction wrote 10 keys in the high range
		let writeset_keys: Vec<Bytes> = (90_000..90_010).map(generate_sequential_key).collect();
		// Current transaction read keys in the low range (no overlap)
		let readset_keys: Vec<Bytes> = (0..*readset_size).map(generate_sequential_key).collect();
		// Build the scenario once
		let scenario = ReadsetConflictScenario::new(&writeset_keys, &readset_keys);

		// With bloom filter pre-check
		group.bench_function(BenchmarkId::new("with_bloom", format!("{readset_size}reads")), |b| {
			b.iter(|| {
				black_box(scenario.check_with_bloom());
			});
		});

		// Without bloom filter (exact HashSet intersection)
		group.bench_function(
			BenchmarkId::new("without_bloom", format!("{readset_size}reads")),
			|b| {
				b.iter(|| {
					black_box(scenario.check_without_bloom());
				});
			},
		);
	}
	group.finish();
}

// Readset conflict detection with actual conflict present.
// The bloom filter says "maybe" and falls through to the exact check.
fn bench_readset_bloom_conflict_path(c: &mut Criterion) {
	let mut group = c.benchmark_group("readset_bloom_conflict_path");

	for readset_size in &[100, 1_000, 10_000] {
		// Committed transaction wrote 10 keys that overlap the readset
		let writeset_keys: Vec<Bytes> = (0..10).map(generate_sequential_key).collect();
		// Current transaction read keys 0..readset_size (overlap on 0..10)
		let readset_keys: Vec<Bytes> = (0..*readset_size).map(generate_sequential_key).collect();
		// Build the scenario once
		let scenario = ReadsetConflictScenario::new(&writeset_keys, &readset_keys);

		// With bloom (bloom says "maybe", falls through)
		group.bench_function(BenchmarkId::new("with_bloom", format!("{readset_size}reads")), |b| {
			b.iter(|| {
				black_box(scenario.check_with_bloom());
			});
		});

		// Without bloom (exact check directly)
		group.bench_function(
			BenchmarkId::new("without_bloom", format!("{readset_size}reads")),
			|b| {
				b.iter(|| {
					black_box(scenario.check_without_bloom());
				});
			},
		);
	}
	group.finish();
}

// Writeset conflict detection: bloom + min/max vs sorted merge.
// Isolates just the conflict check cost with disjoint keys (no conflict).
fn bench_writeset_bloom_impact(c: &mut Criterion) {
	let mut group = c.benchmark_group("writeset_bloom_impact");

	for writeset_size in &[10, 100, 1_000] {
		// Committed transaction wrote keys in the high range
		let committed_keys: Vec<Bytes> =
			(90_000..(90_000 + *writeset_size)).map(generate_sequential_key).collect();
		// Current transaction wrote keys in the low range (no overlap)
		let current_keys: Vec<Bytes> = (0..*writeset_size).map(generate_sequential_key).collect();
		// Build the scenario once
		let scenario = WritesetConflictScenario::new(&committed_keys, &current_keys);

		// With bloom filter + min/max pre-check
		group.bench_function(
			BenchmarkId::new("with_bloom", format!("{writeset_size}writes")),
			|b| {
				b.iter(|| {
					black_box(scenario.check_with_bloom());
				});
			},
		);

		// Without bloom (sorted merge only)
		group.bench_function(
			BenchmarkId::new("without_bloom", format!("{writeset_size}writes")),
			|b| {
				b.iter(|| {
					black_box(scenario.check_without_bloom());
				});
			},
		);
	}
	group.finish();
}

// Writeset conflict detection with actual conflict present.
fn bench_writeset_bloom_conflict_path(c: &mut Criterion) {
	let mut group = c.benchmark_group("writeset_bloom_conflict_path");

	for writeset_size in &[10, 100, 1_000] {
		// Both write to overlapping ranges
		let committed_keys: Vec<Bytes> = (0..10).map(generate_sequential_key).collect();
		let current_keys: Vec<Bytes> = (0..*writeset_size).map(generate_sequential_key).collect();
		// Build the scenario once
		let scenario = WritesetConflictScenario::new(&committed_keys, &current_keys);

		// With bloom (bloom says "maybe", falls through to sorted merge)
		group.bench_function(
			BenchmarkId::new("with_bloom", format!("{writeset_size}writes")),
			|b| {
				b.iter(|| {
					black_box(scenario.check_with_bloom());
				});
			},
		);

		// Without bloom (sorted merge only)
		group.bench_function(
			BenchmarkId::new("without_bloom", format!("{writeset_size}writes")),
			|b| {
				b.iter(|| {
					black_box(scenario.check_without_bloom());
				});
			},
		);
	}
	group.finish();
}

// Concurrent SSI throughput: N threads each reading and writing disjoint
// key ranges, all under SSI. The bloom filter speeds up the conflict-check
// loop over the commit queue because each thread's writeset is disjoint
// from every other thread's readset.
fn bench_bloom_concurrent_throughput(c: &mut Criterion) {
	let mut group = c.benchmark_group("bloom_concurrent_throughput");
	// Use a reasonable thread count
	let cpu_cores = std::thread::available_parallelism().map_or(8, std::num::NonZero::get);
	let thread_counts = [2, 4, cpu_cores.min(8)];

	for &thread_count in &thread_counts {
		// Each thread gets a disjoint key range of 1000 keys
		let keys_per_thread = 1000;
		let total_keys = thread_count * keys_per_thread;
		let db = Arc::new(setup_database_with_sequential_data(total_keys, 64));

		// SSI mode
		group.throughput(Throughput::Elements(thread_count as u64));
		group.bench_function(BenchmarkId::new("ssi", format!("{thread_count}threads")), |b| {
			b.iter(|| {
				let handles: Vec<_> = (0..thread_count)
					.map(|t| {
						let db = Arc::clone(&db);
						std::thread::spawn(move || {
							let range_start = t * keys_per_thread;
							let mut tx =
								db.transaction(true).with_serializable_snapshot_isolation();
							// Read 100 keys in our range
							for i in range_start..(range_start + 100) {
								tx.get(generate_sequential_key(i)).unwrap();
							}
							// Write 10 keys in our range
							for i in range_start..(range_start + 10) {
								tx.set(
									generate_sequential_key(i),
									Bytes::from("updated".as_bytes().to_vec()),
								)
								.unwrap();
							}
							tx.commit().unwrap();
						})
					})
					.collect();
				for h in handles {
					h.join().unwrap();
				}
			});
		});

		// SI baseline for comparison
		group.bench_function(
			BenchmarkId::new("si_baseline", format!("{thread_count}threads")),
			|b| {
				b.iter(|| {
					let handles: Vec<_> = (0..thread_count)
						.map(|t| {
							let db = Arc::clone(&db);
							std::thread::spawn(move || {
								let range_start = t * keys_per_thread;
								let mut tx = db.transaction(true).with_snapshot_isolation();
								// Read 100 keys in our range
								for i in range_start..(range_start + 100) {
									tx.get(generate_sequential_key(i)).unwrap();
								}
								// Write 10 keys in our range
								for i in range_start..(range_start + 10) {
									tx.set(
										generate_sequential_key(i),
										Bytes::from("updated".as_bytes().to_vec()),
									)
									.unwrap();
								}
								tx.commit().unwrap();
							})
						})
						.collect();
					for h in handles {
						h.join().unwrap();
					}
				});
			},
		);
	}
	group.finish();
}

// ---------------------------------------------------------------------------
// Lazy k-way merge over the transaction merge queue. Microbenchmark over
// the `MergeQueueIter` in isolation (built via bench_internals because the
// production merge queue drains synchronously at commit, so the only way to
// get reproducible queue depth in a single-threaded bench is to construct it
// directly).
// ---------------------------------------------------------------------------

fn bench_merge_queue_iter(c: &mut Criterion) {
	let mut group = c.benchmark_group("merge_queue_iter");

	let total_keys = 10_000;
	let keys_per_source = 50;

	for num_sources in &[1, 4, 16, 64] {
		let scenario = MergeQueueScenario::new(*num_sources, keys_per_source, total_keys);

		// Full drain: scales with merge-queue depth × keys per source.
		group.bench_function(BenchmarkId::new("drain", format!("{num_sources}sources")), |b| {
			b.iter(|| {
				black_box(scenario.iter_forward_count());
			});
		});

		// Early termination at 100 entries: highlights the lazy iterator's
		// advantage over the previous eager BTreeMap materialisation, which
		// always paid the full merge cost up front.
		group.bench_function(BenchmarkId::new("take_100", format!("{num_sources}sources")), |b| {
			b.iter(|| {
				black_box(scenario.iter_forward_take(100));
			});
		});
	}
	group.finish();
}

// Cursor pagination: open a cursor over a 10k-key range and fetch the first
// 100 keys via `next()`. Targets the cursor's `create_iterator` path which
// used to re-slice a BTreeMap on every direction change and now builds a
// fresh `MergeQueueIter` from the pre-snapshotted `Vec<Arc<Merge>>`.
fn bench_cursor_pagination(c: &mut Criterion) {
	let mut group = c.benchmark_group("cursor_pagination");

	let entry_count = 10_000;
	let db = setup_database_with_sequential_data(entry_count, 100);

	for page_size in &[10, 100, 1000] {
		group.throughput(Throughput::Elements(*page_size as u64));
		group.bench_with_input(
			BenchmarkId::new("first_n", format!("page{page_size}")),
			page_size,
			|b, &page| {
				b.iter(|| {
					let tx = db.transaction(false);
					let start_key = b"".to_vec();
					let end_key = b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF".to_vec();
					let mut cursor = tx.cursor(start_key..end_key).unwrap();
					cursor.seek_to_first();
					let mut count = 0;
					while cursor.valid() && count < page {
						if cursor.exists() {
							black_box(cursor.key());
							black_box(cursor.value());
							count += 1;
						}
						cursor.next();
					}
					black_box(count);
				});
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
	bench_total_operations,
	bench_cursor_scan,
	bench_scan_iter,
	bench_keys_iter
);

criterion_group!(
	concurrent_benchmarks,
	bench_concurrent_readers,
	bench_concurrent_writers,
	bench_concurrent_mixed,
	bench_mixed_workload
);

criterion_group!(configuration_benchmarks, bench_database_options);

criterion_group!(merge_benchmarks, bench_merge_queue_iter, bench_cursor_pagination);

criterion_group!(
	optimization_benchmarks,
	bench_scan_for_each,
	bench_keys_for_each,
	bench_gc_full_scan,
	bench_commit_inline_gc,
	bench_watermark_scan,
	bench_readset_bloom_impact,
	bench_readset_bloom_conflict_path,
	bench_writeset_bloom_impact,
	bench_writeset_bloom_conflict_path,
	bench_bloom_concurrent_throughput
);

criterion_main!(
	database_benchmarks,
	scan_benchmarks,
	concurrent_benchmarks,
	configuration_benchmarks,
	optimization_benchmarks,
	merge_benchmarks
);
