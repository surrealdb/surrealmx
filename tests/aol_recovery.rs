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

//! Append-only log recovery tests for `SurrealMX`.
//!
//! A crash can leave the log cut off anywhere, including part-way through a
//! transaction or part-way through a record. These tests cut the log at every
//! byte offset and check that recovery exposes only whole transactions, cuts
//! the damaged tail away, and keeps every commit made after recovery.

use byteslice::ByteSlice;
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use surrealmx::{
	AolMode, Database, DatabaseOptions, FsyncMode, PersistenceError, PersistenceOptions,
	SnapshotMode,
};
use tempfile::TempDir;

/// The header which opens the framed section of the log
const MAGIC: [u8; 8] = [0xFF, b'S', b'M', b'X', b'A', b'O', b'L', 1];

/// The size of the header in front of each frame
const FRAME_HEADER_LEN: usize = 8;

fn options(dir: &Path, aol_mode: AolMode, fsync_mode: FsyncMode) -> PersistenceOptions {
	PersistenceOptions::new(dir)
		.with_aol_mode(aol_mode)
		.with_fsync_mode(fsync_mode)
		.with_snapshot_mode(SnapshotMode::Never)
}

fn open(dir: &Path, aol_mode: AolMode, fsync_mode: FsyncMode) -> io::Result<Database> {
	Database::new_with_persistence(DatabaseOptions::default(), options(dir, aol_mode, fsync_mode))
}

/// Whether opening failed because the log is corrupted
fn is_corrupted<T>(result: &io::Result<T>) -> bool {
	let source = result.as_ref().err().and_then(io::Error::get_ref);
	matches!(
		source.and_then(|e| e.downcast_ref::<PersistenceError>()),
		Some(PersistenceError::Corrupted(_))
	)
}

/// Every key visible in the database, in order
fn dump(db: &Database) -> Vec<String> {
	let tx = db.transaction(false);
	let keys = tx.keys(vec![0u8]..vec![255u8], None, None).unwrap();
	keys.iter().map(|k| String::from_utf8_lossy(k).into_owned()).collect()
}

/// Commits `keys` as one transaction
fn commit(db: &Database, keys: &[&str]) {
	let mut tx = db.transaction(true);
	for key in keys {
		tx.set(*key, "value").unwrap();
	}
	tx.commit().unwrap();
}

/// The expected contents of the database after the given transactions
fn keys_of(txns: &[&[&str]]) -> Vec<String> {
	let mut keys: Vec<String> =
		txns.iter().flat_map(|txn| txn.iter().map(|k| (*k).to_owned())).collect();
	keys.sort();
	keys
}

/// The offset at which each frame of a framed log ends. Written independently
/// of the engine, so that the tests check the layout rather than assume it.
fn frame_ends(log: &[u8]) -> Vec<usize> {
	assert_eq!(&log[..MAGIC.len()], &MAGIC, "log does not open with the header");
	let mut ends = Vec::new();
	let mut pos = MAGIC.len();
	while pos < log.len() {
		let len = u32::from_le_bytes(log[pos..pos + 4].try_into().unwrap()) as usize;
		pos += FRAME_HEADER_LEN + len;
		ends.push(pos);
	}
	assert_eq!(pos, log.len(), "the last frame overruns the log");
	ends
}

/// Writes `txns` to a fresh log and returns its bytes with the end of each
/// transaction's frame
fn build_log(aol_mode: AolMode, fsync_mode: FsyncMode, txns: &[&[&str]]) -> (Vec<u8>, Vec<usize>) {
	let dir = TempDir::new().unwrap();
	{
		let db = open(dir.path(), aol_mode, fsync_mode).unwrap();
		for txn in txns {
			commit(&db, txn);
		}
	}
	let log = fs::read(dir.path().join("aol.bin")).unwrap();
	let ends = frame_ends(&log);
	assert_eq!(ends.len(), txns.len(), "expected one frame for each transaction");
	(log, ends)
}

/// Cuts the log at every byte offset, then checks that:
/// - recovery exposes exactly the transactions whose frames are complete,
/// - the damaged tail is removed from the file,
/// - a transaction committed after recovery survives the next restart.
fn sweep(aol_mode: AolMode, fsync_mode: FsyncMode) {
	let txns: [&[&str]; 4] =
		[&["g0", "g1"], &["t0", "t1", "t2", "t3", "t4"], &["u0"], &["v0", "v1", "v2"]];
	let (log, ends) = build_log(aol_mode, fsync_mode, &txns);
	for cut in 0..=log.len() {
		let context =
			format!("{aol_mode:?} with {fsync_mode:?}, log cut at {cut} of {}", log.len());
		let dir = TempDir::new().unwrap();
		let aol = dir.path().join("aol.bin");
		fs::write(&aol, &log[..cut]).unwrap();
		let complete = ends.iter().filter(|&&end| end <= cut).count();
		let expected = keys_of(&txns[..complete]);
		// Restart #1 exposes whole transactions only
		{
			let db = open(dir.path(), aol_mode, fsync_mode)
				.unwrap_or_else(|e| panic!("{context}: failed to open: {e}"));
			assert_eq!(dump(&db), expected, "{context}");
		}
		// The file is cut back to the last complete frame, or to the header
		// when no frame is complete, or to nothing when the header is torn
		let kept = match complete {
			0 if cut >= MAGIC.len() => MAGIC.len(),
			0 => 0,
			n => ends[n - 1],
		};
		assert_eq!(fs::metadata(&aol).unwrap().len(), kept as u64, "{context}: torn tail kept");
		// A commit made after recovery survives restart #2
		{
			let db = open(dir.path(), aol_mode, fsync_mode).unwrap();
			commit(&db, &["n0", "n1", "n2"]);
		}
		let mut expected = expected;
		expected.extend(["n0", "n1", "n2"].map(str::to_owned));
		expected.sort();
		let db = open(dir.path(), aol_mode, fsync_mode)
			.unwrap_or_else(|e| panic!("{context}: failed to reopen after a new commit: {e}"));
		assert_eq!(dump(&db), expected, "{context}: lost a commit made after recovery");
	}
}

#[test]
fn synchronous_log_cut_at_every_offset() {
	for fsync_mode in
		[FsyncMode::Never, FsyncMode::EveryAppend, FsyncMode::Interval(Duration::from_secs(60))]
	{
		sweep(AolMode::SynchronousOnCommit, fsync_mode);
	}
}

#[test]
fn asynchronous_log_cut_at_every_offset() {
	for fsync_mode in
		[FsyncMode::Never, FsyncMode::EveryAppend, FsyncMode::Interval(Duration::from_secs(60))]
	{
		sweep(AolMode::AsynchronousAfterCommit, fsync_mode);
	}
}

#[test]
fn log_cut_part_way_through_a_transaction_does_not_apply_it() {
	for aol_mode in [AolMode::SynchronousOnCommit, AolMode::AsynchronousAfterCommit] {
		let txns: [&[&str]; 2] = [&["g0", "g1"], &["t0", "t1", "t2", "t3", "t4"]];
		let (log, ends) = build_log(aol_mode, FsyncMode::EveryAppend, &txns);
		let dir = TempDir::new().unwrap();
		// Cut after three of the five records of the second transaction
		let records = (ends[1] - ends[0] - FRAME_HEADER_LEN) / 5;
		let cut = ends[0] + FRAME_HEADER_LEN + 3 * records;
		fs::write(dir.path().join("aol.bin"), &log[..cut]).unwrap();
		let db = open(dir.path(), aol_mode, FsyncMode::EveryAppend).unwrap();
		assert_eq!(dump(&db), ["g0", "g1"], "{aol_mode:?} applied part of a transaction");
	}
}

#[test]
fn torn_tail_does_not_poison_later_commits() {
	for aol_mode in [AolMode::SynchronousOnCommit, AolMode::AsynchronousAfterCommit] {
		let txns: [&[&str]; 2] = [&["g0", "g1"], &["t0", "t1", "t2", "t3", "t4"]];
		let (log, ends) = build_log(aol_mode, FsyncMode::EveryAppend, &txns);
		let dir = TempDir::new().unwrap();
		// Cut a few bytes into the second transaction's first record
		fs::write(dir.path().join("aol.bin"), &log[..ends[0] + FRAME_HEADER_LEN + 4]).unwrap();
		{
			let db = open(dir.path(), aol_mode, FsyncMode::EveryAppend).unwrap();
			assert_eq!(dump(&db), ["g0", "g1"]);
			commit(&db, &["n0", "n1", "n2"]);
		}
		let db = open(dir.path(), aol_mode, FsyncMode::EveryAppend)
			.unwrap_or_else(|e| panic!("{aol_mode:?} failed to reopen: {e}"));
		assert_eq!(dump(&db), ["g0", "g1", "n0", "n1", "n2"]);
	}
}

#[test]
fn garbage_after_the_last_frame_is_removed() {
	let txns: [&[&str]; 1] = [&["a", "b"]];
	let (log, ends) = build_log(AolMode::SynchronousOnCommit, FsyncMode::Never, &txns);
	let dir = TempDir::new().unwrap();
	let aol = dir.path().join("aol.bin");
	let mut damaged = log.clone();
	damaged.extend_from_slice(b"INVALID_PARTIAL_DATA");
	fs::write(&aol, &damaged).unwrap();
	{
		let db = open(dir.path(), AolMode::SynchronousOnCommit, FsyncMode::Never).unwrap();
		assert_eq!(dump(&db), ["a", "b"]);
		commit(&db, &["c"]);
	}
	let db = open(dir.path(), AolMode::SynchronousOnCommit, FsyncMode::Never).unwrap();
	assert_eq!(dump(&db), ["a", "b", "c"]);
	assert_eq!(&fs::read(&aol).unwrap()[..ends[0]], &log[..]);
}

#[test]
fn damage_followed_by_valid_data_is_an_error() {
	let txns: [&[&str]; 3] = [&["a"], &["b"], &["c"]];
	let (log, ends) = build_log(AolMode::SynchronousOnCommit, FsyncMode::Never, &txns);
	let dir = TempDir::new().unwrap();
	let aol = dir.path().join("aol.bin");
	// Flip a bit in the payload of the second frame, which the third follows
	let mut damaged = log;
	damaged[ends[0] + FRAME_HEADER_LEN] ^= 0x01;
	fs::write(&aol, &damaged).unwrap();
	let result = open(dir.path(), AolMode::SynchronousOnCommit, FsyncMode::Never);
	assert!(is_corrupted(&result), "corruption before valid data must not be silently dropped");
	// The log is left exactly as it was found
	assert_eq!(fs::read(&aol).unwrap(), damaged);
}

#[test]
fn damage_to_the_last_frame_is_a_torn_tail() {
	let txns: [&[&str]; 3] = [&["a"], &["b"], &["c"]];
	let (log, ends) = build_log(AolMode::SynchronousOnCommit, FsyncMode::Never, &txns);
	let dir = TempDir::new().unwrap();
	let aol = dir.path().join("aol.bin");
	// Flip a bit in the payload of the last frame, and zero-fill past it as
	// a file extended by a crash would be
	let mut damaged = log;
	damaged[ends[1] + FRAME_HEADER_LEN] ^= 0x01;
	damaged.extend_from_slice(&[0u8; 32]);
	fs::write(&aol, &damaged).unwrap();
	let db = open(dir.path(), AolMode::SynchronousOnCommit, FsyncMode::Never).unwrap();
	assert_eq!(dump(&db), ["a", "b"]);
	assert_eq!(fs::metadata(&aol).unwrap().len(), ends[1] as u64);
}

#[test]
fn unrecognised_log_formats_are_rejected() {
	let txns: [&[&str]; 1] = [&["a"]];
	let (log, _) = build_log(AolMode::SynchronousOnCommit, FsyncMode::Never, &txns);
	// A header from a format version this release does not know
	let mut newer = log.clone();
	newer[MAGIC.len() - 1] = 200;
	// A header which opens like ours but is not
	let mut foreign = log;
	foreign[1] = b'X';
	for damaged in [newer, foreign] {
		let dir = TempDir::new().unwrap();
		fs::write(dir.path().join("aol.bin"), &damaged).unwrap();
		let result = open(dir.path(), AolMode::SynchronousOnCommit, FsyncMode::Never);
		assert!(is_corrupted(&result));
	}
}

/// One record in the format used before transactions were framed
fn legacy_record(key: &str, version: u64, value: Option<&str>) -> Vec<u8> {
	bincode::serde::encode_to_vec(
		(ByteSlice::from(key), version, value.map(ByteSlice::from)),
		bincode::config::standard(),
	)
	.unwrap()
}

/// How a log from before frames existed ends
#[derive(Debug, Clone, Copy)]
enum LegacyTail {
	/// After the last whole record
	Clean,
	/// Part-way through a record
	TornRecord,
	/// Part-way through the header which a crashed first framed write began
	TornHeader,
}

#[test]
fn log_written_before_frames_existed_is_replayed_and_extended() {
	for tail in [LegacyTail::Clean, LegacyTail::TornRecord, LegacyTail::TornHeader] {
		for aol_mode in [AolMode::SynchronousOnCommit, AolMode::AsynchronousAfterCommit] {
			let context = format!("{tail:?}, {aol_mode:?}");
			let dir = TempDir::new().unwrap();
			let aol = dir.path().join("aol.bin");
			let mut legacy = Vec::new();
			legacy.extend(legacy_record("a", 1, Some("1")));
			legacy.extend(legacy_record("b", 1, Some("2")));
			legacy.extend(legacy_record("c", 2, Some("3")));
			legacy.extend(legacy_record("b", 3, None));
			let mut written = legacy.clone();
			match tail {
				LegacyTail::Clean => {}
				LegacyTail::TornRecord => {
					written.extend_from_slice(&legacy_record("d", 4, Some("4"))[..3]);
				}
				LegacyTail::TornHeader => written.extend_from_slice(&MAGIC[..3]),
			}
			fs::write(&aol, &written).unwrap();
			// The records are replayed, and any torn tail is cut away
			{
				let db = open(dir.path(), aol_mode, FsyncMode::EveryAppend).unwrap();
				assert_eq!(dump(&db), ["a", "c"], "{context}");
				commit(&db, &["d", "e"]);
			}
			// New commits are framed after the legacy records
			let after = fs::read(&aol).unwrap();
			assert_eq!(&after[..legacy.len()], &legacy[..], "{context}");
			assert_eq!(&after[legacy.len()..legacy.len() + MAGIC.len()], &MAGIC, "{context}");
			let ends = frame_ends(&after[legacy.len()..]);
			assert_eq!(ends.len(), 1, "{context}");
			// Everything survives another restart, and a third
			for _ in 0..2 {
				let db = open(dir.path(), aol_mode, FsyncMode::EveryAppend).unwrap();
				assert_eq!(dump(&db), ["a", "c", "d", "e"], "{context}");
			}
		}
	}
}

#[test]
fn log_written_before_frames_existed_can_be_snapshotted() {
	let dir = TempDir::new().unwrap();
	let aol = dir.path().join("aol.bin");
	let mut legacy = Vec::new();
	legacy.extend(legacy_record("a", 1, Some("1")));
	legacy.extend(legacy_record("b", 2, Some("2")));
	fs::write(&aol, &legacy).unwrap();
	{
		let db = open(dir.path(), AolMode::SynchronousOnCommit, FsyncMode::Never).unwrap();
		commit(&db, &["c"]);
		db.persistence().unwrap().snapshot().unwrap();
		commit(&db, &["d"]);
	}
	let db = open(dir.path(), AolMode::SynchronousOnCommit, FsyncMode::Never).unwrap();
	assert_eq!(dump(&db), ["a", "b", "c", "d"]);
}

#[test]
fn queued_async_commits_are_written_when_the_database_is_dropped() {
	for round in 0..100 {
		let dir = TempDir::new().unwrap();
		{
			let db = open(dir.path(), AolMode::AsynchronousAfterCommit, FsyncMode::Never).unwrap();
			commit(&db, &["k"]);
			// Dropped at once, while the commit may still be queued
		}
		let db = open(dir.path(), AolMode::AsynchronousAfterCommit, FsyncMode::Never).unwrap();
		assert_eq!(dump(&db), ["k"], "round {round}: the commit never reached the log");
	}
}

#[test]
fn large_async_backlog_is_written_when_the_database_is_dropped() {
	let dir = TempDir::new().unwrap();
	let total = 1000;
	{
		let db = open(dir.path(), AolMode::AsynchronousAfterCommit, FsyncMode::Never).unwrap();
		for i in 0..total {
			commit(&db, &[&format!("k{i:05}")]);
		}
	}
	let db = open(dir.path(), AolMode::AsynchronousAfterCommit, FsyncMode::Never).unwrap();
	assert_eq!(dump(&db).len(), total);
}

#[test]
fn snapshot_keeps_the_frames_written_after_its_cutoff() {
	for aol_mode in [AolMode::SynchronousOnCommit, AolMode::AsynchronousAfterCommit] {
		let dir = TempDir::new().unwrap();
		let db = Arc::new(open(dir.path(), aol_mode, FsyncMode::Never).unwrap());
		let stop = Arc::new(AtomicBool::new(false));
		// Commit steadily while snapshots cut the log underneath. The pace
		// keeps each snapshot's scan of the datastore short.
		let writer = {
			let db = Arc::clone(&db);
			let stop = Arc::clone(&stop);
			thread::spawn(move || {
				let mut committed = 0usize;
				while !stop.load(Ordering::Relaxed) {
					commit(&db, &[&format!("w{committed:06}")]);
					committed += 1;
					thread::sleep(Duration::from_micros(200));
				}
				committed
			})
		};
		for _ in 0..20 {
			db.persistence().unwrap().snapshot().unwrap();
			thread::sleep(Duration::from_millis(2));
		}
		stop.store(true, Ordering::Relaxed);
		let committed = writer.join().unwrap();
		drop(db);
		assert!(committed > 0);
		// Whatever the log held after the last cut is still readable
		let db = open(dir.path(), aol_mode, FsyncMode::Never)
			.unwrap_or_else(|e| panic!("{aol_mode:?} failed to reopen: {e}"));
		let keys = dump(&db);
		assert_eq!(keys.len(), committed, "{aol_mode:?} lost a commit across a snapshot");
		// And the log still opens with the header after being cut
		let log = fs::read(dir.path().join("aol.bin")).unwrap();
		if !log.is_empty() {
			frame_ends(&log);
		}
	}
}

#[test]
fn concurrent_group_commits_are_atomic_under_truncation() {
	const THREADS: usize = 6;
	const TXNS: usize = 8;
	const KEYS: usize = 3;
	let dir = TempDir::new().unwrap();
	{
		let db = Arc::new(
			open(dir.path(), AolMode::SynchronousOnCommit, FsyncMode::EveryAppend).unwrap(),
		);
		let handles: Vec<_> = (0..THREADS)
			.map(|thread| {
				let db = Arc::clone(&db);
				thread::spawn(move || {
					for txn in 0..TXNS {
						let mut tx = db.transaction(true);
						for key in 0..KEYS {
							tx.set(format!("w{thread}-{txn:02}-{key}"), "value").unwrap();
						}
						tx.commit().unwrap();
					}
				})
			})
			.collect();
		for handle in handles {
			handle.join().unwrap();
		}
	}
	let log = fs::read(dir.path().join("aol.bin")).unwrap();
	let ends = frame_ends(&log);
	assert_eq!(ends.len(), THREADS * TXNS);
	let cuts = (0..=log.len()).step_by(5).chain(ends.iter().copied()).chain([log.len()]);
	for cut in cuts {
		let dir = TempDir::new().unwrap();
		fs::write(dir.path().join("aol.bin"), &log[..cut]).unwrap();
		let db = open(dir.path(), AolMode::SynchronousOnCommit, FsyncMode::EveryAppend)
			.unwrap_or_else(|e| panic!("cut at {cut}: failed to open: {e}"));
		// Group the visible keys by transaction
		let mut by_txn: BTreeMap<String, usize> = BTreeMap::new();
		for key in dump(&db) {
			let txn = key.rsplit_once('-').unwrap().0.to_owned();
			*by_txn.entry(txn).or_default() += 1;
		}
		assert!(
			by_txn.values().all(|&count| count == KEYS),
			"cut at {cut}: a transaction is partially visible: {by_txn:?}"
		);
		let complete = ends.iter().filter(|&&end| end <= cut).count();
		assert_eq!(by_txn.len(), complete, "cut at {cut}: wrong number of transactions");
	}
}

// =============================================================================
// Killing a real process
// =============================================================================

const CRASH_DIR: &str = "SURREALMX_AOL_CRASH_DIR";
const CRASH_MODE: &str = "SURREALMX_AOL_CRASH_MODE";
const CRASH_PREFIX: &str = "SURREALMX_AOL_CRASH_PREFIX";
const CRASH_KEYS: usize = 16;

fn crash_config(name: &str) -> (AolMode, FsyncMode) {
	match name {
		"sync-never" => (AolMode::SynchronousOnCommit, FsyncMode::Never),
		"sync-every" => (AolMode::SynchronousOnCommit, FsyncMode::EveryAppend),
		"async-never" => (AolMode::AsynchronousAfterCommit, FsyncMode::Never),
		other => panic!("unknown crash configuration {other}"),
	}
}

/// Not a test of its own: the child process which `kill_during_commits` kills.
/// It commits large transactions until it is stopped, and returns at once when
/// it is run as an ordinary test.
#[test]
fn aol_crash_child() {
	let Ok(dir) = std::env::var(CRASH_DIR) else {
		return;
	};
	let (aol_mode, fsync_mode) = crash_config(&std::env::var(CRASH_MODE).unwrap());
	let prefix = std::env::var(CRASH_PREFIX).unwrap();
	let db = open(Path::new(&dir), aol_mode, fsync_mode).unwrap();
	let value = vec![b'x'; 16 * 1024];
	let mut txn = 0u64;
	loop {
		let mut tx = db.transaction(true);
		for key in 0..CRASH_KEYS {
			tx.set(format!("{prefix}-{txn:06}-{key:02}"), value.clone()).unwrap();
		}
		tx.commit().unwrap();
		txn += 1;
	}
}

/// Checks that every transaction visible in the database is complete, and that
/// the transactions of each run are the first ones that run committed
fn assert_whole_transactions(db: &Database, context: &str) {
	let mut counts: BTreeMap<String, usize> = BTreeMap::new();
	for key in dump(db) {
		if let Some((txn, _)) = key.rsplit_once('-') {
			*counts.entry(txn.to_owned()).or_default() += 1;
		}
	}
	let mut next: BTreeMap<String, u64> = BTreeMap::new();
	for (txn, count) in &counts {
		if !txn.starts_with('i') {
			continue;
		}
		assert_eq!(*count, CRASH_KEYS, "{context}: transaction {txn} is partially visible");
		let (run, n) = txn.split_once('-').unwrap();
		let n: u64 = n.parse().unwrap();
		let expected = next.entry(run.to_owned()).or_default();
		assert_eq!(n, *expected, "{context}: {run} lost transaction {expected} but kept {n}");
		*expected += 1;
	}
}

#[test]
fn kill_during_commits_recovers_whole_transactions() {
	let exe = std::env::current_exe().unwrap();
	for config in ["sync-never", "sync-every", "async-never"] {
		let (aol_mode, fsync_mode) = crash_config(config);
		let dir = TempDir::new().unwrap();
		let aol: PathBuf = dir.path().join("aol.bin");
		for run in 0..4 {
			let context = format!("{config}, run {run}");
			let before = fs::metadata(&aol).map_or(0, |m| m.len());
			let mut child = Command::new(&exe)
				.args(["--exact", "aol_crash_child", "--test-threads=1"])
				.env(CRASH_DIR, dir.path())
				.env(CRASH_MODE, config)
				.env(CRASH_PREFIX, format!("i{run}"))
				.stdout(Stdio::null())
				.stderr(Stdio::null())
				.spawn()
				.unwrap();
			// Let the child get going, then kill it at an arbitrary moment
			let start = Instant::now();
			while fs::metadata(&aol).map_or(0, |m| m.len()) <= before {
				assert!(
					start.elapsed() < Duration::from_secs(60),
					"{context}: child wrote nothing"
				);
				thread::sleep(Duration::from_millis(2));
			}
			let jitter = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
			thread::sleep(Duration::from_micros(u64::from(jitter % 150_000)));
			child.kill().unwrap();
			let status = child.wait().unwrap();
			assert!(!status.success(), "{context}: the child stopped by itself");
			#[cfg(unix)]
			{
				use std::os::unix::process::ExitStatusExt;
				assert_eq!(
					status.signal(),
					Some(9),
					"{context}: the child failed rather than being killed"
				);
			}
			// Restart #1: whole transactions only
			let seen = {
				let db = open(dir.path(), aol_mode, fsync_mode)
					.unwrap_or_else(|e| panic!("{context}: failed to open after the kill: {e}"));
				assert_whole_transactions(&db, &context);
				let seen = dump(&db);
				commit(&db, &[&format!("marker{run}")]);
				seen
			};
			// Restart #2: the same state plus the commit made after recovery
			let db = open(dir.path(), aol_mode, fsync_mode)
				.unwrap_or_else(|e| panic!("{context}: failed to reopen after recovery: {e}"));
			assert_whole_transactions(&db, &context);
			let mut expected = seen;
			expected.push(format!("marker{run}"));
			expected.sort();
			assert_eq!(dump(&db), expected, "{context}: state changed across the second restart");
		}
	}
}
