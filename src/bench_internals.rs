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

//! Internal helpers for benchmarking conflict detection with and without
//! bloom filter pre-checks. Not part of the public API.

use crate::bloom::BloomFilter;
use crate::direction::Direction;
use crate::iter::MergeQueueIter;
use crate::queue::{Commit, Merge};
use bytes::Bytes;
use papaya::HashSet;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;

/// A prepared readset conflict scenario for benchmarking
pub struct ReadsetConflictScenario {
	/// The committed transaction entry
	commit: Arc<Commit>,
	/// The transaction readset
	readset: HashSet<Bytes>,
	/// The bloom filter over the readset
	readset_bloom: BloomFilter,
}

impl ReadsetConflictScenario {
	/// Build a scenario with the given writeset and readset keys
	pub fn new(writeset_keys: &[Bytes], readset_keys: &[Bytes]) -> Self {
		// Build the sorted writeset key list (inputs may be unsorted)
		let mut ws: Vec<Bytes> = writeset_keys.to_vec();
		ws.sort();
		ws.dedup();
		let keys: Arc<[Bytes]> = ws.into();
		// Build the writeset bloom filter
		let mut writeset_bloom = BloomFilter::new();
		for k in keys.iter() {
			writeset_bloom.insert(k);
		}
		// Build the commit entry
		let commit = Arc::new(Commit {
			keys,
			writeset_bloom,
			merge_version: AtomicU64::new(0),
		});
		// Build the readset and bloom filter
		let readset = HashSet::new();
		let mut readset_bloom = BloomFilter::new();
		{
			let pin = readset.pin();
			for k in readset_keys {
				pin.insert(k.clone());
				readset_bloom.insert(k);
			}
		}
		Self {
			commit,
			readset,
			readset_bloom,
		}
	}

	/// Check readset disjointness WITH bloom filter pre-check
	pub fn check_with_bloom(&self) -> bool {
		self.commit.is_disjoint_readset_bloom(&self.readset, &self.readset_bloom)
	}

	/// Check readset disjointness WITHOUT bloom filter (exact check only)
	pub fn check_without_bloom(&self) -> bool {
		self.commit.is_disjoint_readset(&self.readset)
	}
}

/// A prepared writeset conflict scenario for benchmarking
pub struct WritesetConflictScenario {
	/// The committed transaction entry
	committed: Arc<Commit>,
	/// The current transaction entry
	current: Arc<Commit>,
}

impl WritesetConflictScenario {
	/// Build a scenario with two writesets
	pub fn new(committed_keys: &[Bytes], current_keys: &[Bytes]) -> Self {
		Self {
			committed: Arc::new(Self::build_commit(committed_keys)),
			current: Arc::new(Self::build_commit(current_keys)),
		}
	}

	/// Check writeset disjointness WITH bloom filter + min/max pre-check
	pub fn check_with_bloom(&self) -> bool {
		self.committed.is_disjoint_writeset_bloom(&self.current)
	}

	/// Check writeset disjointness WITHOUT bloom filter (sorted merge only)
	pub fn check_without_bloom(&self) -> bool {
		self.committed.is_disjoint_writeset(&self.current)
	}

	/// Build a Commit entry from a set of keys
	fn build_commit(input: &[Bytes]) -> Commit {
		// Build the sorted writeset key list (inputs may be unsorted)
		let mut ws: Vec<Bytes> = input.to_vec();
		ws.sort();
		ws.dedup();
		let keys: Arc<[Bytes]> = ws.into();
		// Build the writeset bloom filter
		let mut writeset_bloom = BloomFilter::new();
		for k in keys.iter() {
			writeset_bloom.insert(k);
		}
		// Build the commit entry
		Commit {
			keys,
			writeset_bloom,
			merge_version: AtomicU64::new(0),
		}
	}
}

/// A prepared merge-queue scenario for benchmarking the lazy k-way merge.
/// Holds a vector of `Arc<Merge>` source writesets and the iteration bounds.
pub struct MergeQueueScenario {
	sources: Vec<Arc<Merge>>,
	beg: Bytes,
	end: Bytes,
}

impl MergeQueueScenario {
	/// Build a scenario with `num_sources` writesets, each populated with
	/// `keys_per_source` keys drawn pseudo-randomly from `0..total_keys` so
	/// that sources statistically overlap and the merge has to dedup.
	pub fn new(num_sources: usize, keys_per_source: usize, total_keys: usize) -> Self {
		let mut sources = Vec::with_capacity(num_sources);
		for i in 0..num_sources {
			let mut ws = BTreeMap::new();
			for j in 0..keys_per_source {
				// Cheap deterministic spread; collisions across sources are
				// expected and exercise the dedup path.
				let key_idx = (i.wrapping_mul(31) + j.wrapping_mul(17)) % total_keys;
				let key = Bytes::from(format!("key_{key_idx:08}").into_bytes());
				ws.insert(key, Some(Bytes::from_static(b"v")));
			}
			sources.push(Arc::new(Merge {
				writeset: Arc::new(ws),
				applied: AtomicBool::new(false),
			}));
		}
		let beg = Bytes::from(b"key_00000000".to_vec());
		let end = Bytes::from(format!("key_{total_keys:08}").into_bytes());
		Self {
			sources,
			beg,
			end,
		}
	}

	/// Fully iterate the lazy merge in forward direction and return the
	/// total number of entries yielded.
	pub fn iter_forward_count(&self) -> usize {
		MergeQueueIter::new(
			self.sources.clone(),
			self.beg.clone(),
			self.end.clone(),
			Direction::Forward,
		)
		.count()
	}

	/// Iterate the lazy merge forward, taking only the first `n` entries —
	/// exercises the early-termination path that the previous eager
	/// materialisation could not benefit from.
	pub fn iter_forward_take(&self, n: usize) -> usize {
		MergeQueueIter::new(
			self.sources.clone(),
			self.beg.clone(),
			self.end.clone(),
			Direction::Forward,
		)
		.take(n)
		.count()
	}
}

/// A prepared watermark-scan scenario for benchmarking the per-commit
/// inline-GC slot scan. Registers `num_readers` pinned slots directly in
/// the readers map — the exact state a database holds with that many
/// live transactions — and exposes the watermark computation a committer
/// performs once per commit.
pub struct WatermarkScanScenario {
	db: crate::Database,
	own_slot: u64,
}

impl WatermarkScanScenario {
	/// Build a scenario with `num_readers` live registered slots plus the
	/// committer's own slot.
	pub fn new(num_readers: usize) -> Self {
		use crate::inner::Slot;
		use std::sync::atomic::Ordering;
		let db = crate::Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		// Register the reader slots with plausible snapshot values
		for i in 0..num_readers {
			let id = db.reader_slot_id.fetch_add(1, Ordering::SeqCst) + 1;
			let slot = Arc::new(Slot::pinning());
			slot.version.store(1 + (i as u64 % 16), Ordering::SeqCst);
			slot.commit.store(1 + (i as u64 % 16), Ordering::SeqCst);
			db.readers.insert(id, slot);
		}
		// Register the committer's own slot, excluded by the scan
		let own_slot = db.reader_slot_id.fetch_add(1, Ordering::SeqCst) + 1;
		let slot = Arc::new(Slot::pinning());
		slot.version.store(32, Ordering::SeqCst);
		slot.commit.store(32, Ordering::SeqCst);
		db.readers.insert(own_slot, slot);
		Self {
			db,
			own_slot,
		}
	}

	/// Perform one inline-GC watermark computation, exactly as a commit
	/// does after publishing its merge version.
	pub fn scan(&self) -> Option<u64> {
		self.db.inline_gc_watermark(self.own_slot)
	}
}
