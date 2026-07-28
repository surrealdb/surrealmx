#![cfg(not(target_arch = "wasm32"))]
//! Reproducer for a scan-correctness bug observed in surrealdb-private's
//! `multi_index_concurrent_test_create_update_delete`.
//!
//! Symptom (in the parent project): a range scan against
//! `[/*0*0*aaa+ix!dc\0 , /*0*0*aaa+ix!dc\xff*40)` would occasionally
//! return keys with a *different* IndexId (different bytes inside the
//! shared prefix) and a *different* category byte (`!tt`, `!dl`, …).
//! The decode of the value as `DocLengthAndCount` then failed with
//! either "Invalid revision 0" (8-byte u64 doc-length value) or
//! "Reader did not have enough data" (0-byte string value).
//!
//! This isolated reproducer drives the same access pattern directly
//! against surrealmx:
//!
//! - Several writer threads insert into multiple synthetic "indexes",
//!   committing one key at a time, in tight loops. Each thread uses
//!   `with_snapshot_isolation()` because that is what the in-mem engine
//!   in surrealdb passes through.
//! - A reader thread repeatedly opens a snapshot-isolation transaction
//!   and scans a narrow range that should only ever contain keys with
//!   a specific `(ix, category)` prefix.
//! - On every scanned entry the reader verifies the returned key is
//!   within the scan bounds; out-of-range keys (or values that don't
//!   match the expected category-byte) are reported and counted.

use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use surrealmx::Database;

/// Build a key shaped like SurrealDB's index keys:
///   `/ * 00 00 00 00 * 00 00 00 00 * aaa \0 + <ix:4 BE> ! <kind:2> <doc:8 BE> <nid:16> <uid:16>`
/// `nid`/`uid` are filled with a counter so writers don't collide on the same
/// key — every put is a fresh delta-style key, like the full-text index does.
fn make_key(ix: u32, kind: [u8; 2], doc: u64, nid_uid: u128) -> Vec<u8> {
	let mut k = Vec::with_capacity(64);
	k.extend_from_slice(b"/*\0\0\0\0*\0\0\0\0*aaa\0+");
	k.extend_from_slice(&ix.to_be_bytes());
	k.push(b'!');
	k.extend_from_slice(&kind);
	k.extend_from_slice(&doc.to_be_bytes());
	let bytes = nid_uid.to_be_bytes();
	k.extend_from_slice(&bytes);
	k.extend_from_slice(&bytes);
	k
}

/// Build a scan range for a specific `(ix, kind)` pair, matching the shape
/// of `Dc::range_with_root` + `dc_delta_range` in surrealdb:
///   begin = prefix + [0]
///   end   = prefix + [0xff; 40]
fn make_range(ix: u32, kind: [u8; 2]) -> (Vec<u8>, Vec<u8>) {
	let mut prefix = Vec::with_capacity(32);
	prefix.extend_from_slice(b"/*\0\0\0\0*\0\0\0\0*aaa\0+");
	prefix.extend_from_slice(&ix.to_be_bytes());
	prefix.push(b'!');
	prefix.extend_from_slice(&kind);
	let mut beg = prefix.clone();
	beg.push(0);
	let mut end = prefix.clone();
	end.extend(std::iter::repeat_n(0xff, 40));
	(beg, end)
}

/// Run the reproducer for at most `budget` and return the number of
/// out-of-range entries observed. Zero means the bug did not reproduce
/// in this window.
fn run_once(budget: Duration) -> (usize, usize, usize) {
	const N_INDEXES: u32 = 5;
	const SCAN_IX: u32 = 3;
	const SCAN_KIND: [u8; 2] = *b"dc";

	// All the (ix, kind) combinations that the writers will hit. The mix
	// matters: full-text writes `!dc`, `!dl`, `!td`, `!tt`, `!ii`, `!tv`,
	// etc., spread over `N_INDEXES` indexes. We just pick the same letter
	// pairs from the real code.
	const KINDS: &[[u8; 2]] = &[*b"dc", *b"dl", *b"td", *b"tt", *b"ii", *b"tv"];

	let db = Arc::new(Database::new());
	let stop = Arc::new(AtomicBool::new(false));
	let writes_done = Arc::new(AtomicUsize::new(0));
	let scans_done = Arc::new(AtomicUsize::new(0));
	let oor_keys = Arc::new(AtomicUsize::new(0));
	let first_offender = Arc::new(Mutex::new(None::<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)>));

	// Writer threads — one per index, each cycling through the kinds and
	// committing one tx per key (like full-text does).
	let mut writers = Vec::new();
	for ix in 0..N_INDEXES {
		let db = Arc::clone(&db);
		let stop = Arc::clone(&stop);
		let writes_done = Arc::clone(&writes_done);
		writers.push(thread::spawn(move || {
			let mut counter: u128 = (ix as u128 + 1) << 96;
			let mut doc: u64 = 0;
			while !stop.load(Ordering::Relaxed) {
				for &kind in KINDS {
					if stop.load(Ordering::Relaxed) {
						break;
					}
					let key = make_key(ix, kind, doc, counter);
					counter = counter.wrapping_add(1);
					doc = doc.wrapping_add(1);
					let value: Vec<u8> = if kind == *b"dl" {
						// DocLength-style value: 8 bytes, starts with zero.
						doc.to_be_bytes().to_vec()
					} else if kind == *b"tt" {
						// Tt-style value: empty.
						Vec::new()
					} else {
						// Dc-style value: revisioned-ish (1 byte revision + 24 payload).
						let mut v = vec![1u8];
						v.extend_from_slice(&doc.to_be_bytes());
						v.extend_from_slice(&doc.to_be_bytes());
						v.extend_from_slice(&doc.to_be_bytes());
						v
					};
					let mut tx = db.transaction(true).with_snapshot_isolation();
					if tx.set(key, value).is_err() {
						let _ = tx.commit();
						continue;
					}
					if tx.commit().is_ok() {
						writes_done.fetch_add(1, Ordering::Relaxed);
					}
				}
			}
		}));
	}

	// Reader thread — repeatedly scans the narrow `(SCAN_IX, SCAN_KIND)`
	// range and validates every returned key is within bounds.
	let reader = {
		let db = Arc::clone(&db);
		let stop = Arc::clone(&stop);
		let scans_done = Arc::clone(&scans_done);
		let oor_keys = Arc::clone(&oor_keys);
		let first_offender = Arc::clone(&first_offender);
		thread::spawn(move || {
			let (beg, end) = make_range(SCAN_IX, SCAN_KIND);
			while !stop.load(Ordering::Relaxed) {
				let tx = db.transaction(false).with_snapshot_isolation();
				let beg_b = Bytes::copy_from_slice(&beg);
				let end_b = Bytes::copy_from_slice(&end);
				let res = match tx.scan(beg_b..end_b, None, None) {
					Ok(r) => r,
					Err(_) => continue,
				};
				scans_done.fetch_add(1, Ordering::Relaxed);
				for (k, v) in res {
					let k_slice = k.as_ref();
					let in_range = k_slice >= beg.as_slice() && k_slice < end.as_slice();
					if !in_range {
						oor_keys.fetch_add(1, Ordering::Relaxed);
						let mut g = first_offender.lock().unwrap();
						if g.is_none() {
							*g = Some((beg.clone(), end.clone(), k.to_vec(), v.to_vec()));
						}
					}
				}
			}
		})
	};

	thread::sleep(budget);
	stop.store(true, Ordering::Relaxed);
	for w in writers {
		w.join().unwrap();
	}
	reader.join().unwrap();

	let writes = writes_done.load(Ordering::Relaxed);
	let scans = scans_done.load(Ordering::Relaxed);
	let oor = oor_keys.load(Ordering::Relaxed);

	if oor > 0 {
		let g = first_offender.lock().unwrap();
		if let Some((b, e, k, v)) = g.as_ref() {
			eprintln!("=== scan_out_of_range — first offender ===");
			eprintln!("  range_start_hex = {}", hex(b));
			eprintln!("  range_end_hex   = {}", hex(e));
			eprintln!("  key_hex         = {}", hex(k));
			eprintln!("  val_hex         = {}", hex(v));
			eprintln!("  writes={writes} scans={scans} oor={oor}");
		}
	}

	(writes, scans, oor)
}

fn hex(b: &[u8]) -> String {
	let mut s = String::with_capacity(b.len() * 2);
	for c in b {
		s.push_str(&format!("{:02x}", c));
	}
	s
}

/// Drive the workload for a fixed budget and assert no out-of-range keys
/// were seen. Failure prints the first offender so the bug shape is
/// directly comparable to the surrealdb-private logs.
#[test]
fn scan_never_returns_keys_outside_requested_range() {
	let start = Instant::now();
	let budget = Duration::from_secs(5);
	let (writes, scans, oor) = run_once(budget);
	eprintln!(
		"scan_out_of_range: elapsed={:?} writes={writes} scans={scans} oor={oor}",
		start.elapsed()
	);
	assert_eq!(
		oor, 0,
		"scan returned {oor} keys outside the requested range \
		 (writes={writes} scans={scans}) — see stderr for first offender"
	);
}
