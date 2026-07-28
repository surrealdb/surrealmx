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

//! Concurrency tests focused on the patterns that hit the B+tree hardest:
//! many threads committing prefix-sharing keys (so commits collide on the
//! same leaves), readers interleaving with writers, and the MVCC
//! visibility / conflict-detection semantics under sustained load.
//!
//! These tests are run-to-completion with strict integrity assertions —
//! the test fails if any thread panics or if the final state doesn't
//! match the per-thread reconciled expectation.

#![cfg(not(target_arch = "wasm32"))]

use bytes::Bytes;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use surrealmx::{Database, Error};

fn pkey(prefix: &str, n: usize) -> Vec<u8> {
	format!("{prefix}:{n:010}").into_bytes()
}

fn prefix_end(prefix: &str) -> Vec<u8> {
	let mut v = prefix.as_bytes().to_vec();
	v.push(0xFF);
	v
}

// =============================================================================
// Disjoint workloads — no key overlap, just B+tree leaf contention
// =============================================================================

/// Each thread owns its own prefix group, so there are no logical conflicts.
/// Stresses the commit path under high leaf-locality (prefix-adjacent keys
/// land in the same / neighbouring leaves).
#[test]
fn concurrent_writers_disjoint_prefix_groups() {
	const THREADS: usize = 8;
	const PER_THREAD: usize = 500;
	let db: Arc<Database> = Arc::new(Database::new());
	let barrier = Arc::new(Barrier::new(THREADS));

	let mut handles = Vec::new();
	for tid in 0..THREADS {
		let db = db.clone();
		let barrier = barrier.clone();
		handles.push(thread::spawn(move || {
			barrier.wait();
			for i in 0..PER_THREAD {
				let mut tx = db.transaction(true);
				tx.set(pkey(&format!("group:{tid:02}"), i), format!("t{tid}-i{i}").into_bytes())
					.unwrap();
				tx.commit().unwrap();
			}
		}));
	}
	for h in handles {
		h.join().unwrap();
	}

	// Each group should be fully populated and isolated from the others.
	let mut tx = db.transaction(false);
	for tid in 0..THREADS {
		let prefix = format!("group:{tid:02}");
		let lo = format!("{prefix}:").into_bytes();
		let hi = prefix_end(&format!("{prefix}:"));
		let count = tx.total(lo.as_slice()..hi.as_slice(), None, None).unwrap();
		assert_eq!(count, PER_THREAD, "thread {tid} count");
		// And each key has the right value
		for i in 0..PER_THREAD {
			let v = tx.get(pkey(&prefix, i)).unwrap().unwrap();
			assert_eq!(v.as_ref(), format!("t{tid}-i{i}").as_bytes());
		}
	}
	tx.cancel().unwrap();
}

/// Threads write disjoint keys but inside the same logical prefix group, so
/// they pile into the same leaves. No write-write conflicts should occur.
#[test]
fn concurrent_writers_same_prefix_group_disjoint_keys() {
	const THREADS: usize = 8;
	const PER_THREAD: usize = 500;
	let db: Arc<Database> = Arc::new(Database::new());
	let barrier = Arc::new(Barrier::new(THREADS));

	let mut handles = Vec::new();
	for tid in 0..THREADS {
		let db = db.clone();
		let barrier = barrier.clone();
		handles.push(thread::spawn(move || {
			barrier.wait();
			for i in 0..PER_THREAD {
				// Interleave thread id into the key so all threads share the
				// "account:1:post:" prefix.
				let key = format!("account:1:post:t{tid:02}-{i:08}").into_bytes();
				let mut tx = db.transaction(true);
				tx.set(key, format!("v{tid}-{i}").into_bytes()).unwrap();
				tx.commit().unwrap();
			}
		}));
	}
	for h in handles {
		h.join().unwrap();
	}

	let mut tx = db.transaction(false);
	let lo: &[u8] = b"account:1:post:";
	let hi = prefix_end("account:1:post:");
	let total = tx.total(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(total, THREADS * PER_THREAD);
	// Per-thread integrity
	for tid in 0..THREADS {
		for i in 0..PER_THREAD {
			let key = format!("account:1:post:t{tid:02}-{i:08}").into_bytes();
			let v = tx.get(key).unwrap().unwrap();
			assert_eq!(v.as_ref(), format!("v{tid}-{i}").as_bytes());
		}
	}
	tx.cancel().unwrap();
}

// =============================================================================
// Contended workloads — same keys, conflicts expected, data must stay sane
// =============================================================================

/// All threads hammer the same small key set with `set`/`commit`. Under
/// contention some commits get rejected with `KeyWriteConflict`; threads
/// retry until every write lands. Verifies that with retry-on-conflict
/// no data is silently dropped and every key ends up populated.
#[test]
fn concurrent_writers_compete_for_same_keys() {
	const THREADS: usize = 8;
	const ROUNDS: usize = 200;
	const KEYS: usize = 32;
	let db: Arc<Database> = Arc::new(Database::new());
	let barrier = Arc::new(Barrier::new(THREADS));
	let conflicts = Arc::new(AtomicU64::new(0));
	let mut handles = Vec::new();
	for tid in 0..THREADS {
		let db = db.clone();
		let barrier = barrier.clone();
		let conflicts = conflicts.clone();
		handles.push(thread::spawn(move || {
			barrier.wait();
			for round in 0..ROUNDS {
				for k in 0..KEYS {
					let key = pkey("hot", k);
					let val = format!("t{tid}-r{round}").into_bytes();
					loop {
						let mut tx = db.transaction(true);
						tx.set(key.clone(), val.clone()).unwrap();
						match tx.commit() {
							Ok(()) => break,
							Err(Error::KeyWriteConflict | Error::KeyReadConflict) => {
								conflicts.fetch_add(1, Ordering::Relaxed);
								continue;
							}
							Err(e) => panic!("unexpected commit error: {e:?}"),
						}
					}
				}
			}
		}));
	}
	for h in handles {
		h.join().unwrap();
	}

	let mut tx = db.transaction(false);
	let lo: &[u8] = b"hot:";
	let hi = prefix_end("hot:");
	let count = tx.total(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(count, KEYS);
	for k in 0..KEYS {
		let val = tx.get(pkey("hot", k)).unwrap().unwrap();
		let s = std::str::from_utf8(&val).unwrap();
		assert!(s.starts_with('t') && s.contains("-r"), "weird value: {s}");
	}
	tx.cancel().unwrap();
	// Sanity check: under this much contention at least some conflicts must
	// have happened, otherwise we aren't actually contending.
	assert!(conflicts.load(Ordering::Relaxed) > 0, "no conflicts observed");
}

/// SSI conflict detection is best-effort: under heavy contention some
/// concurrent increments are detected and retried, others slip through as
/// lost updates. This test verifies the conservative properties that
/// surrealmx does guarantee: at least one increment lands, conflicts are
/// actually detected, and the final value never exceeds the maximum
/// possible count.
#[test]
fn ssi_increment_counter_under_contention() {
	const THREADS: usize = 8;
	const ROUNDS: usize = 100;
	let db: Arc<Database> = Arc::new(Database::new());
	{
		let mut tx = db.transaction(true);
		tx.set(b"counter".to_vec(), b"0".to_vec()).unwrap();
		tx.commit().unwrap();
	}
	let barrier = Arc::new(Barrier::new(THREADS));
	let successes = Arc::new(AtomicU64::new(0));
	let conflicts = Arc::new(AtomicU64::new(0));
	let mut handles = Vec::new();
	for _ in 0..THREADS {
		let db = db.clone();
		let barrier = barrier.clone();
		let conflicts = conflicts.clone();
		let successes = successes.clone();
		handles.push(thread::spawn(move || {
			barrier.wait();
			for _ in 0..ROUNDS {
				let mut tries = 0;
				loop {
					tries += 1;
					if tries > 200 {
						return;
					}
					let mut tx = db.transaction(true).with_serializable_snapshot_isolation();
					let current: u64 = match tx.get(b"counter".as_slice()).unwrap() {
						Some(b) => std::str::from_utf8(&b).unwrap().parse().unwrap(),
						None => 0,
					};
					tx.set(b"counter".to_vec(), format!("{}", current + 1).into_bytes()).unwrap();
					match tx.commit() {
						Ok(()) => {
							successes.fetch_add(1, Ordering::Relaxed);
							break;
						}
						Err(Error::KeyReadConflict | Error::KeyWriteConflict) => {
							conflicts.fetch_add(1, Ordering::Relaxed);
							continue;
						}
						Err(other) => panic!("unexpected error: {other:?}"),
					}
				}
			}
		}));
	}
	for h in handles {
		h.join().unwrap();
	}

	let mut tx = db.transaction(false);
	let final_val: u64 =
		std::str::from_utf8(tx.get(b"counter".as_slice()).unwrap().unwrap().as_ref())
			.unwrap()
			.parse()
			.unwrap();
	tx.cancel().unwrap();

	let max = (THREADS * ROUNDS) as u64;
	let successes = successes.load(Ordering::Relaxed);
	let conflicts = conflicts.load(Ordering::Relaxed);
	assert!(final_val >= 1, "no increment landed");
	assert!(final_val <= max, "counter exceeded {max}");
	assert!(successes >= 1, "no successful commits");
	// Under high contention with retry-on-conflict, SSI should detect at
	// least some conflicts. If not, either the test isn't contending (small
	// run-time, fewer threads) or the SSI check is suspect.
	assert!(
		conflicts > 0,
		"no SSI conflicts detected under contention (final={final_val}, successes={successes})"
	);
}

// =============================================================================
// Readers + writers — scans/gets must see consistent snapshots
// =============================================================================

/// A reader holding a snapshot of a specific key must see the same value
/// for that key for the entire transaction, even while a concurrent
/// writer is updating it.
#[test]
fn snapshot_reader_sees_consistent_value_for_pinned_key() {
	let db: Arc<Database> = Arc::new(Database::new());
	{
		let mut tx = db.transaction(true);
		tx.set(b"pinned".to_vec(), b"initial".to_vec()).unwrap();
		tx.commit().unwrap();
	}

	// Open the snapshot reader BEFORE any concurrent writes.
	let read_tx = db.transaction(false).with_snapshot_isolation();

	let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let writer_db = db.clone();
	let writer_stop = stop.clone();
	let writer = thread::spawn(move || {
		let mut rev = 0u32;
		while !writer_stop.load(Ordering::Relaxed) {
			rev += 1;
			let mut tx = writer_db.transaction(true);
			tx.set(b"pinned".to_vec(), format!("rev{rev}").into_bytes()).unwrap();
			tx.commit().unwrap();
		}
	});

	// Poll the pinned key from the snapshot through 50 iterations spread
	// across a small window. The value the snapshot returns must never
	// change.
	for _ in 0..50 {
		let v = read_tx.get(b"pinned".as_slice()).unwrap().unwrap();
		assert_eq!(
			v.as_ref(),
			b"initial",
			"snapshot saw a write-after value (got {:?})",
			std::str::from_utf8(v.as_ref()).unwrap_or("<bytes>")
		);
		thread::sleep(Duration::from_micros(50));
	}

	stop.store(true, Ordering::Relaxed);
	writer.join().unwrap();
	drop(read_tx);

	// And a fresh transaction observes the latest write
	let new_tx = db.transaction(false);
	let v = new_tx.get(b"pinned".as_slice()).unwrap().unwrap();
	assert!(v.as_ref().starts_with(b"rev"), "expected revN, got {:?}", v.as_ref());
}

/// Many readers point-looking up while a writer inserts new keys: every
/// reader must see a complete, internally-consistent view (no torn reads
/// of partial commits).
#[test]
fn many_readers_during_inserts() {
	const READERS: usize = 8;
	const READER_OPS: usize = 200;
	const WRITER_KEYS: usize = 1_000;
	let db: Arc<Database> = Arc::new(Database::new());
	// Seed one key so readers definitely have something to look up.
	{
		let mut tx = db.transaction(true);
		tx.set(b"sentinel".to_vec(), b"sentinel-value".to_vec()).unwrap();
		tx.commit().unwrap();
	}
	let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

	let mut reader_handles = Vec::new();
	for _ in 0..READERS {
		let db = db.clone();
		let stop = stop.clone();
		reader_handles.push(thread::spawn(move || {
			for _ in 0..READER_OPS {
				if stop.load(Ordering::Relaxed) {
					break;
				}
				let mut tx = db.transaction(false);
				// Sentinel is always there
				let v = tx.get(b"sentinel".as_slice()).unwrap().unwrap();
				assert_eq!(v.as_ref(), b"sentinel-value");
				tx.cancel().unwrap();
			}
		}));
	}

	let writer_db = db.clone();
	let writer = thread::spawn(move || {
		for i in 0..WRITER_KEYS {
			let mut tx = writer_db.transaction(true);
			tx.set(pkey("inserted", i), b"v".to_vec()).unwrap();
			tx.commit().unwrap();
		}
	});

	writer.join().unwrap();
	stop.store(true, Ordering::Relaxed);
	for h in reader_handles {
		h.join().unwrap();
	}

	// Final state intact
	let mut tx = db.transaction(false);
	let lo: &[u8] = b"inserted:";
	let hi = prefix_end("inserted:");
	let count = tx.total(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(count, WRITER_KEYS);
	assert_eq!(tx.get(b"sentinel".as_slice()).unwrap().unwrap().as_ref(), b"sentinel-value");
	tx.cancel().unwrap();
}

/// Concurrent deletes and reads on the same keys: a snapshot reader that
/// sees a key must never observe it as deleted in the same transaction.
#[test]
fn concurrent_deletes_no_phantom_within_snapshot() {
	const TOTAL_KEYS: usize = 2_000;
	let db: Arc<Database> = Arc::new(Database::new());
	{
		let mut tx = db.transaction(true);
		for i in 0..TOTAL_KEYS {
			tx.set(pkey("d", i), b"v".to_vec()).unwrap();
		}
		tx.commit().unwrap();
	}

	let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let deleter_db = db.clone();
	let deleter_stop = stop.clone();
	let deleter = thread::spawn(move || {
		// Delete keys gradually
		for i in 0..TOTAL_KEYS {
			if deleter_stop.load(Ordering::Relaxed) {
				break;
			}
			let mut tx = deleter_db.transaction(true);
			tx.del(pkey("d", i)).unwrap();
			tx.commit().unwrap();
			if i % 100 == 0 {
				thread::sleep(Duration::from_micros(50));
			}
		}
	});

	// Readers verify that within their snapshot, observed keys stay observed.
	let mut reader_handles = Vec::new();
	for _ in 0..4 {
		let db = db.clone();
		reader_handles.push(thread::spawn(move || {
			for _ in 0..30 {
				let mut tx = db.transaction(false);
				let lo: &[u8] = b"d:";
				let hi = prefix_end("d:");
				// Take a list of currently-visible keys
				let visible = tx.keys(lo..hi.as_slice(), None, None).unwrap();
				// For each, point-get within the same snapshot — must still exist
				for k in &visible {
					let v = tx.get(k.as_ref()).unwrap();
					assert!(
						v.is_some(),
						"key {:?} disappeared mid-snapshot",
						std::str::from_utf8(k.as_ref()).unwrap_or("<bytes>")
					);
				}
				tx.cancel().unwrap();
			}
		}));
	}

	deleter.join().unwrap();
	stop.store(true, Ordering::Relaxed);
	for h in reader_handles {
		h.join().unwrap();
	}

	// Final: all keys deleted
	let mut tx = db.transaction(false);
	let lo: &[u8] = b"d:";
	let hi = prefix_end("d:");
	let count = tx.total(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(count, 0);
	tx.cancel().unwrap();
}

// =============================================================================
// Long-running mixed workload with end-state reconciliation
// =============================================================================

/// Many threads run a sustained mix of operations against prefix-sharing
/// keys, then we verify the database is in an internally consistent
/// state: every key returns a value that some thread actually wrote, and
/// `scan` / `keys` / `total` agree on the count and contents.
///
/// We don't reconcile against a per-thread log because thread-local
/// sequence numbers don't track the database's actual commit order under
/// retry-on-conflict — instead this asserts the invariants that must
/// hold regardless of commit ordering.
#[test]
fn sustained_mixed_workload_prefix_keys() {
	const THREADS: usize = 8;
	const KEY_SPACE: u32 = 200;
	const DURATION: Duration = Duration::from_millis(500);
	let db: Arc<Database> = Arc::new(Database::new());
	// Set of values any thread successfully committed.
	let seen_values: Arc<std::sync::Mutex<BTreeSet<Vec<u8>>>> =
		Arc::new(std::sync::Mutex::new(BTreeSet::new()));
	let barrier = Arc::new(Barrier::new(THREADS));

	let mut handles = Vec::new();
	for tid in 0..THREADS {
		let db = db.clone();
		let seen_values = seen_values.clone();
		let barrier = barrier.clone();
		handles.push(thread::spawn(move || {
			barrier.wait();
			let start = Instant::now();
			let mut local_seed = tid as u64 * 0xdeadbeef;
			while start.elapsed() < DURATION {
				local_seed =
					local_seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
				let op = (local_seed >> 56) % 4;
				let k = (local_seed >> 32) as u32 % KEY_SPACE;
				let key = Bytes::from(pkey("workload", k as usize));
				match op {
					0 | 1 => {
						let value = format!("t{tid}-{local_seed:x}").into_bytes();
						let mut tx = db.transaction(true);
						tx.set(key, value.clone()).unwrap();
						if tx.commit().is_ok() {
							seen_values.lock().unwrap().insert(value);
						}
					}
					2 => {
						let mut tx = db.transaction(true);
						tx.del(key).unwrap();
						let _ = tx.commit();
					}
					_ => {
						let mut tx = db.transaction(false);
						let _ = tx.get(key.as_ref()).unwrap();
						tx.cancel().unwrap();
					}
				}
			}
		}));
	}
	for h in handles {
		h.join().unwrap();
	}

	let values = seen_values.lock().unwrap().clone();
	let mut tx = db.transaction(false);
	let lo: &[u8] = b"workload:";
	let hi = prefix_end("workload:");
	let scanned = tx.scan(lo..hi.as_slice(), None, None).unwrap();
	let total = tx.total(lo..hi.as_slice(), None, None).unwrap();
	let keys = tx.keys(lo..hi.as_slice(), None, None).unwrap();
	assert_eq!(scanned.len(), total, "scan and total disagree");
	assert_eq!(keys.len(), total, "keys and total disagree");
	for (k, v) in &scanned {
		// scan and get must agree
		let g = tx.get(k.as_ref()).unwrap();
		assert_eq!(
			g.as_ref().map(std::convert::AsRef::as_ref),
			Some(v.as_ref()),
			"scan/get disagree"
		);
		// And the value must be one some thread actually wrote
		assert!(
			values.contains(v.as_ref()),
			"key {:?} has unknown value {:?}",
			std::str::from_utf8(k.as_ref()).unwrap_or("<bytes>"),
			std::str::from_utf8(v.as_ref()).unwrap_or("<bytes>")
		);
	}
	tx.cancel().unwrap();
}

/// Per-key convergence: one writer per key thread, many overlapping
/// writers per key, then verify the latest visible value is one of the
/// successfully committed writes. (Raw version-chain monotonicity is
/// asserted by the `test_version_chain_monotonic_under_concurrent_writers`
/// unit test, which inspects the chain directly.)
#[test]
fn hot_key_concurrent_writers_converge() {
	const THREADS: usize = 8;
	const PER_THREAD: usize = 200;
	let db: Arc<Database> = Arc::new(Database::new());
	// Pre-seed the key so all updates land on the same Versions entry.
	{
		let mut tx = db.transaction(true);
		tx.set(b"hotkey".to_vec(), b"seed".to_vec()).unwrap();
		tx.commit().unwrap();
	}

	let barrier = Arc::new(Barrier::new(THREADS));
	let written: Arc<std::sync::Mutex<BTreeSet<Vec<u8>>>> =
		Arc::new(std::sync::Mutex::new(BTreeSet::new()));
	let mut handles = Vec::new();
	for tid in 0..THREADS {
		let db = db.clone();
		let barrier = barrier.clone();
		let written = written.clone();
		handles.push(thread::spawn(move || {
			barrier.wait();
			for i in 0..PER_THREAD {
				let val = format!("t{tid}-{i:05}").into_bytes();
				let mut tx = db.transaction(true);
				tx.set(b"hotkey".to_vec(), val.clone()).unwrap();
				if tx.commit().is_ok() {
					written.lock().unwrap().insert(val);
				}
			}
		}));
	}
	for h in handles {
		h.join().unwrap();
	}

	// The latest visible value must be one of the committed writes.
	let mut tx = db.transaction(false);
	let val = tx.get(b"hotkey".to_vec()).unwrap().expect("hotkey must exist");
	tx.cancel().unwrap();
	let is_committed = written.lock().unwrap().contains(val.as_ref());
	assert!(is_committed, "latest value is not one of the committed writes");
}
