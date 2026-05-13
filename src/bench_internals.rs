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
use crate::queue::Commit;
use bytes::Bytes;
use papaya::HashSet;
use std::collections::BTreeMap;
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
		// Build the writeset BTreeMap
		let mut ws = BTreeMap::new();
		for k in writeset_keys {
			ws.insert(k.clone(), Some(Bytes::from_static(b"v")));
		}
		// Build the writeset bloom filter
		let mut writeset_bloom = BloomFilter::new();
		for k in ws.keys() {
			writeset_bloom.insert(k);
		}
		// Extract min and max keys
		let min_key = ws.keys().next().cloned().unwrap_or_default();
		let max_key = ws.keys().next_back().cloned().unwrap_or_default();
		// Build the commit entry
		let commit = Arc::new(Commit {
			id: 1,
			writeset: Arc::new(ws),
			writeset_bloom,
			min_key,
			max_key,
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
			committed: Arc::new(Self::build_commit(committed_keys, 1)),
			current: Arc::new(Self::build_commit(current_keys, 2)),
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
	fn build_commit(keys: &[Bytes], id: u64) -> Commit {
		// Build the writeset BTreeMap
		let mut ws = BTreeMap::new();
		for k in keys {
			ws.insert(k.clone(), Some(Bytes::from_static(b"v")));
		}
		// Build the writeset bloom filter
		let mut writeset_bloom = BloomFilter::new();
		for k in ws.keys() {
			writeset_bloom.insert(k);
		}
		// Extract min and max keys
		let min_key = ws.keys().next().cloned().unwrap_or_default();
		let max_key = ws.keys().next_back().cloned().unwrap_or_default();
		// Build the commit entry
		Commit {
			id,
			writeset: Arc::new(ws),
			writeset_bloom,
			min_key,
			max_key,
		}
	}
}
