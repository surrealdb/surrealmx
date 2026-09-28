#![cfg(not(target_arch = "wasm32"))]
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

//! Direct `Database` operations racing with transactions.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use surrealmx::{Database, Error};

/// How long each race runs for. The violations these tests guard against
/// used to reproduce within milliseconds.
const fn run_for() -> Duration {
	Duration::from_millis(if cfg!(miri) {
		50
	} else {
		1500
	})
}

/// Run `race` on a background thread until `check` has run for
/// [`run_for`], returning the first violation `check` reports.
fn race<T: Send + 'static>(
	race: impl FnOnce(Arc<AtomicBool>) -> T + Send + 'static,
	mut check: impl FnMut() -> Option<String>,
) -> Option<String> {
	let stop = Arc::new(AtomicBool::new(false));
	let handle = {
		let stop = Arc::clone(&stop);
		thread::spawn(move || race(stop))
	};
	let deadline = Instant::now() + run_for();
	let mut violation = None;
	while violation.is_none() && Instant::now() < deadline {
		violation = check();
	}
	stop.store(true, Ordering::Relaxed);
	handle.join().unwrap();
	violation
}

#[test]
fn direct_set_is_invisible_until_applied() {
	// A direct write used to publish the version clock before its value
	// reached the datastore, so a snapshot taken in between read the old
	// value first and the new one later.
	let db = Arc::new(Database::new());
	db.set("k", "0").unwrap();
	let writer = Arc::clone(&db);
	let violation = race(
		move |stop| {
			let mut i: u64 = 1;
			while !stop.load(Ordering::Relaxed) {
				writer.set("k", i.to_string()).unwrap();
				i += 1;
			}
		},
		|| {
			let tx = db.transaction(false);
			let a = tx.get("k").unwrap();
			for _ in 0..64 {
				std::hint::spin_loop();
			}
			let b = tx.get("k").unwrap();
			(a != b).then(|| format!("non-repeatable read: {a:?} then {b:?}"))
		},
	);
	assert_eq!(violation, None);
}

#[test]
fn direct_set_does_not_skip_in_flight_merges() {
	// A direct write used to advance the version clock and the merge
	// retirement watermark past a transaction's in-flight merge, so
	// snapshots skipped the merge overlay and saw that transaction's
	// writes torn, or even going backwards.
	let db = Arc::new(Database::new());
	{
		let mut tx = db.transaction(true);
		for k in 0..8 {
			tx.set(format!("a{k}"), "0").unwrap();
		}
		tx.commit().unwrap();
	}
	let writers = Arc::clone(&db);
	let violation = race(
		move |stop| {
			let direct = {
				let db = Arc::clone(&writers);
				let stop = Arc::clone(&stop);
				thread::spawn(move || {
					let mut i: u64 = 0;
					while !stop.load(Ordering::Relaxed) {
						db.set("b", i.to_string()).unwrap();
						i += 1;
					}
				})
			};
			let mut i: u64 = 1;
			while !stop.load(Ordering::Relaxed) {
				let mut tx = writers.transaction(true);
				for k in 0..8 {
					tx.set(format!("a{k}"), i.to_string()).unwrap();
				}
				tx.commit().unwrap();
				i += 1;
			}
			direct.join().unwrap();
		},
		|| {
			let tx = db.transaction(false);
			let first: Vec<_> = (0..8).map(|k| tx.get(format!("a{k}")).unwrap()).collect();
			let again: Vec<_> = (0..8).map(|k| tx.get(format!("a{k}")).unwrap()).collect();
			(first != again || first.iter().any(|v| v != &first[0]))
				.then(|| format!("torn snapshot: {first:?} then {again:?}"))
		},
	);
	assert_eq!(violation, None);
}

#[test]
fn direct_set_conflicts_with_a_concurrent_read_modify_write() {
	// First-committer-wins: a transaction that read a key must not
	// overwrite a direct write that committed after its snapshot.
	let db = Database::new();
	db.set("k", "0").unwrap();
	let mut tx = db.transaction(true);
	assert_eq!(tx.get("k").unwrap().as_deref(), Some(b"0" as &[u8]));
	db.set("k", "direct").unwrap();
	tx.set("k", "from-tx").unwrap();
	assert!(matches!(tx.commit(), Err(Error::KeyWriteConflict)));
	assert_eq!(db.get("k").unwrap().as_deref(), Some(b"direct" as &[u8]));
}

#[test]
fn direct_writes_never_report_conflicts() {
	// Direct writes read nothing, so they retry write-write conflicts
	// internally rather than surfacing them.
	let threads = if cfg!(miri) {
		2
	} else {
		4
	};
	let writes = if cfg!(miri) {
		10
	} else {
		2_000
	};
	let db = Arc::new(Database::new());
	let handles: Vec<_> = (0..threads)
		.map(|t| {
			let db = Arc::clone(&db);
			thread::spawn(move || {
				for i in 0..writes {
					db.set("hot", format!("{t}:{i}")).unwrap();
				}
			})
		})
		.collect();
	for h in handles {
		h.join().unwrap();
	}
}

#[test]
fn direct_reads_never_miss_a_live_key() {
	// Direct reads used to read at a clock value without pinning it, so
	// inline GC in a concurrent commit could reclaim the version being read
	// and a key that existed throughout was reported missing.
	let db = Arc::new(Database::new());
	db.set("k", "0").unwrap();
	let writer = Arc::clone(&db);
	let violation = race(
		move |stop| {
			let mut i: u64 = 1;
			while !stop.load(Ordering::Relaxed) {
				let mut tx = writer.transaction(true);
				tx.set("k", i.to_string()).unwrap();
				tx.commit().unwrap();
				i += 1;
			}
		},
		|| {
			if db.get("k").unwrap().is_none() {
				return Some("get missed a live key".into());
			}
			if db.with_value("k", <[u8]>::len).unwrap().is_none() {
				return Some("with_value missed a live key".into());
			}
			if !db.exists("k").unwrap() {
				return Some("exists missed a live key".into());
			}
			let mut seen = 0;
			db.scan_with("a".."z", None, None, |_, _| {
				seen += 1;
				true
			})
			.unwrap();
			if seen != 1 {
				return Some(format!("scan_with saw {seen} keys"));
			}
			let keys = db.keys("a".."z", None, None).unwrap().len();
			if keys != 1 {
				return Some(format!("keys saw {keys} keys"));
			}
			let total = db.total("a".."z", None, None).unwrap();
			(total != 1).then(|| format!("total counted {total} keys"))
		},
	);
	assert_eq!(violation, None);
}

#[test]
fn direct_scans_read_a_consistent_snapshot() {
	// Every transaction rewrites all keys with one value, so a scan at a
	// snapshot must see every key, all with the same value.
	let db = Arc::new(Database::new());
	{
		let mut tx = db.transaction(true);
		for k in 0..16 {
			tx.set(format!("k{k:02}"), "0").unwrap();
		}
		tx.commit().unwrap();
	}
	let writer = Arc::clone(&db);
	let violation = race(
		move |stop| {
			let mut i: u64 = 1;
			while !stop.load(Ordering::Relaxed) {
				let mut tx = writer.transaction(true);
				for k in 0..16 {
					tx.set(format!("k{k:02}"), i.to_string()).unwrap();
				}
				tx.commit().unwrap();
				i += 1;
			}
		},
		|| {
			let rows = db.scan("k".."l", None, None).unwrap();
			(rows.len() != 16 || rows.iter().any(|(_, v)| v != &rows[0].1))
				.then(|| format!("inconsistent scan: {rows:?}"))
		},
	);
	assert_eq!(violation, None);
}
