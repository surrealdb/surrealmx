//! Snapshot isolation loses no update: concurrent transactions that read lists and append to them
//! leave each list holding exactly the appends that committed, each over the list it read.
//!
//! The transactions are held open across yields while many others commit, so that most of the
//! commits a transaction is validated against have already been applied and retired by the time
//! it commits, as well as some that are still in flight.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use surrealmx::{Database, Error};

/// The number of lists the transactions read and append to.
const KEYS: u8 = 8;

fn key_name(key: u8) -> String {
	format!("k{key}")
}

fn decode(value: Option<surrealmx::ByteSlice>) -> Vec<u64> {
	match value {
		None => Vec::new(),
		Some(bytes) => std::str::from_utf8(&bytes)
			.expect("a list is text")
			.split(',')
			.filter(|s| !s.is_empty())
			.map(|s| s.parse().expect("an element is a number"))
			.collect(),
	}
}

fn encode(list: &[u64]) -> String {
	list.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
}

struct Rng(u64);

impl Rng {
	const fn next_u64(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}
}

/// An append that committed: the element, and the list it read first.
struct Append {
	element: u64,
	seen: Vec<u64>,
}

fn run(db: &Arc<Database>, threads: u64, per_thread: u64) -> HashMap<u8, Vec<Append>> {
	let elements = Arc::new(AtomicU64::new(1));
	let committed: Arc<Mutex<HashMap<u8, Vec<Append>>>> = Arc::default();
	let handles: Vec<_> = (0..threads)
		.map(|t| {
			let db = Arc::clone(db);
			let elements = Arc::clone(&elements);
			let committed = Arc::clone(&committed);
			thread::spawn(move || {
				let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ (t + 1).wrapping_mul(0x2545_f491));
				for _ in 0..per_thread {
					let mut tx = db.transaction(true).with_snapshot_isolation();
					// Read every list, then append to one or two of them.
					let mut seen: Vec<Vec<u64>> = (0..KEYS)
						.map(|key| decode(tx.get(key_name(key)).expect("a read succeeds")))
						.collect();
					// Stay open while other transactions commit.
					for _ in 0..(rng.next_u64() % 64) {
						thread::yield_now();
					}
					let mut appends = Vec::new();
					for _ in 0..=(rng.next_u64() % 2) {
						let key = (rng.next_u64() % u64::from(KEYS)) as u8;
						if appends.iter().any(|(k, _, _)| *k == key) {
							continue;
						}
						let element = elements.fetch_add(1, Ordering::SeqCst);
						let read = seen[usize::from(key)].clone();
						seen[usize::from(key)].push(element);
						tx.set(key_name(key), encode(&seen[usize::from(key)]))
							.expect("a write succeeds");
						appends.push((key, element, read));
					}
					match tx.commit() {
						Ok(()) => {
							let mut committed = committed.lock().unwrap();
							for (key, element, seen) in appends {
								committed.entry(key).or_default().push(Append {
									element,
									seen,
								});
							}
						}
						Err(Error::KeyWriteConflict | Error::KeyReadConflict) => {}
						Err(e) => panic!("unexpected commit error: {e:?}"),
					}
				}
			})
		})
		.collect();
	for handle in handles {
		handle.join().unwrap();
	}
	Arc::try_unwrap(committed).ok().unwrap().into_inner().unwrap()
}

#[test]
fn snapshot_isolation_loses_no_append() {
	for round in 0..5 {
		let db = Arc::new(Database::new());
		let committed = run(&db, 64, 500);
		let tx = db.transaction(false);
		for key in 0..KEYS {
			let list = decode(tx.get(key_name(key)).expect("a read succeeds"));
			let appends = committed.get(&key).map_or(&[][..], Vec::as_slice);
			assert_eq!(
				list.len(),
				appends.len(),
				"round {round}, key {key}: the list holds every committed append and nothing else"
			);
			for append in appends {
				let at = list.iter().position(|e| *e == append.element).unwrap_or_else(|| {
					panic!("round {round}, key {key}: append {} is lost", append.element)
				});
				assert_eq!(
					&list[..at],
					append.seen.as_slice(),
					"round {round}, key {key}: append {} was made over the list before it",
					append.element
				);
			}
		}
	}
}

/// A conflict with a commit that was applied and retired before this one commits is found
/// through the key's version chain.
#[test]
fn a_write_conflicts_with_a_retired_commit_to_the_same_key() {
	let db = Database::new();
	let mut a = db.transaction(true).with_snapshot_isolation();
	a.set("k", "a").unwrap();
	let mut b = db.transaction(true).with_snapshot_isolation();
	b.set("k", "b").unwrap();
	b.commit().unwrap();
	// Later commits to other keys, so the conflicting one is well below the retired watermark.
	for i in 0..8 {
		let mut c = db.transaction(true).with_snapshot_isolation();
		c.set(format!("other{i}"), "c").unwrap();
		c.commit().unwrap();
	}
	assert!(matches!(a.commit(), Err(Error::KeyWriteConflict)));
}

/// A key read with `get_for_update` conflicts with a retired commit that wrote it.
#[test]
fn a_locked_read_conflicts_with_a_retired_commit_to_the_same_key() {
	let db = Database::new();
	let mut a = db.transaction(true).with_snapshot_isolation();
	a.get_for_update("k").unwrap();
	a.set("mine", "a").unwrap();
	let mut b = db.transaction(true).with_snapshot_isolation();
	b.set("k", "b").unwrap();
	b.commit().unwrap();
	for i in 0..8 {
		let mut c = db.transaction(true).with_snapshot_isolation();
		c.set(format!("other{i}"), "c").unwrap();
		c.commit().unwrap();
	}
	assert!(matches!(a.commit(), Err(Error::KeyReadConflict)));
}

/// A commit to another key, retired before this one commits, is no conflict.
#[test]
fn a_retired_commit_to_another_key_is_no_conflict() {
	let db = Database::new();
	let mut a = db.transaction(true).with_snapshot_isolation();
	a.get_for_update("k").unwrap();
	a.set("k", "a").unwrap();
	for i in 0..8 {
		let mut c = db.transaction(true).with_snapshot_isolation();
		c.set(format!("other{i}"), "c").unwrap();
		c.commit().unwrap();
	}
	a.commit().unwrap();
}
