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

//! High-performance bloom filters for probabilistic set membership testing.
//!
//! Powered by SIMD-accelerated xxHash3 and lock-free atomic bit array
//! operations for zero-contention SSI readset tracking.

use std::sync::atomic::{AtomicU64, Ordering};

const BLOOM_BITS: usize = 4096;
const BLOOM_WORDS: usize = BLOOM_BITS / 64; // 64 words of u64 = 512 bytes
const NUM_HASHES: u32 = 3;

/// A lightweight 512-byte bloom filter powered by xxHash3.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BloomFilter {
	/// 64 words of 64-bit integers backing 4096 bits
	bits: [u64; BLOOM_WORDS],
	/// The number of keys inserted into the filter
	count: usize,
}

impl Default for BloomFilter {
	fn default() -> Self {
		Self::new()
	}
}

impl BloomFilter {
	/// Create a new empty bloom filter
	pub const fn new() -> Self {
		Self {
			bits: [0; BLOOM_WORDS],
			count: 0,
		}
	}

	/// Insert a key into the bloom filter using xxHash3
	#[inline]
	pub fn insert(&mut self, key: &[u8]) {
		let (h1, h2) = Self::hash(key);
		for i in 0..NUM_HASHES {
			let bit = Self::nth_hash((h1, h2), i) % (BLOOM_BITS as u64);
			let word = (bit / 64) as usize;
			let mask = 1u64 << (bit % 64);
			self.bits[word] |= mask;
		}
		self.count += 1;
	}

	/// Check whether a key may be present in the filter
	#[inline]
	pub fn may_contain(&self, key: &[u8]) -> bool {
		if self.count == 0 {
			return false;
		}
		let (h1, h2) = Self::hash(key);
		for i in 0..NUM_HASHES {
			let bit = Self::nth_hash((h1, h2), i) % (BLOOM_BITS as u64);
			let word = (bit / 64) as usize;
			let mask = 1u64 << (bit % 64);
			if (self.bits[word] & mask) == 0 {
				return false;
			}
		}
		true
	}

	/// Check whether the filter is empty
	#[inline]
	pub const fn is_empty(&self) -> bool {
		self.count == 0
	}

	/// Reset the filter to its initial empty state
	#[cfg(test)]
	pub const fn clear(&mut self) {
		self.bits = [0; BLOOM_WORDS];
		self.count = 0;
	}

	/// Compute a dual hash pair using 128-bit xxHash3
	#[inline]
	pub(crate) fn hash(key: &[u8]) -> (u64, u64) {
		let bytes = xxhash_rust::xxh3::xxh3_128(key).to_ne_bytes();
		let h1 = u64::from_ne_bytes([
			bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
		]);
		let h2 = u64::from_ne_bytes([
			bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
		]);
		(h1, h2)
	}

	/// Compute the nth hash from the dual hash pair
	#[inline]
	pub(crate) fn nth_hash((h1, h2): (u64, u64), n: u32) -> u64 {
		h1.wrapping_add(u64::from(n).wrapping_mul(h2))
	}
}

/// A lock-free, atomic 512-byte bloom filter for concurrent SSI readset
/// tracking.
///
/// Uses relaxed atomic OR operations to set bits without locking any mutex.
pub(crate) struct AtomicBloomFilter {
	bits: [AtomicU64; BLOOM_WORDS],
	count: AtomicU64,
}

impl Default for AtomicBloomFilter {
	fn default() -> Self {
		Self::new()
	}
}

impl AtomicBloomFilter {
	/// Creates a new empty atomic bloom filter
	pub fn new() -> Self {
		Self {
			bits: std::array::from_fn(|_| AtomicU64::new(0)),
			count: AtomicU64::new(0),
		}
	}

	/// Atomically sets the bloom filter bits using lock-free relaxed `fetch_or`
	#[inline]
	pub fn insert(&self, key: &[u8]) {
		let (h1, h2) = BloomFilter::hash(key);
		for i in 0..NUM_HASHES {
			let bit = BloomFilter::nth_hash((h1, h2), i) % (BLOOM_BITS as u64);
			let word = (bit / 64) as usize;
			let mask = 1u64 << (bit % 64);
			self.bits[word].fetch_or(mask, Ordering::Relaxed);
		}
		self.count.fetch_add(1, Ordering::Relaxed);
	}

	/// Checks if a key may be present in the filter without locking
	#[inline]
	pub fn may_contain(&self, key: &[u8]) -> bool {
		if self.count.load(Ordering::Relaxed) == 0 {
			return false;
		}
		let (h1, h2) = BloomFilter::hash(key);
		for i in 0..NUM_HASHES {
			let bit = BloomFilter::nth_hash((h1, h2), i) % (BLOOM_BITS as u64);
			let word = (bit / 64) as usize;
			let mask = 1u64 << (bit % 64);
			if (self.bits[word].load(Ordering::Relaxed) & mask) == 0 {
				return false;
			}
		}
		true
	}

	/// Checks whether the filter is empty
	#[inline]
	pub fn is_empty(&self) -> bool {
		self.count.load(Ordering::Relaxed) == 0
	}

	/// Clears all bits in the filter
	pub fn clear(&self) {
		if self.count.load(Ordering::Relaxed) == 0 {
			return;
		}
		for word in &self.bits {
			word.store(0, Ordering::Relaxed);
		}
		self.count.store(0, Ordering::Relaxed);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn empty_filter_contains_nothing() {
		let bf = BloomFilter::new();
		assert!(bf.is_empty());
		assert!(!bf.may_contain(b"hello"));
		assert!(!bf.may_contain(b"world"));
	}

	#[test]
	fn inserted_keys_are_found() {
		let mut bf = BloomFilter::new();
		bf.insert(b"hello");
		bf.insert(b"world");
		assert!(!bf.is_empty());
		assert!(bf.may_contain(b"hello"));
		assert!(bf.may_contain(b"world"));
	}

	#[test]
	fn missing_keys_usually_not_found() {
		let mut bf = BloomFilter::new();
		for i in 0..100u32 {
			bf.insert(&i.to_le_bytes());
		}
		let mut false_positives = 0;
		for i in 1000..2000u32 {
			if bf.may_contain(&i.to_le_bytes()) {
				false_positives += 1;
			}
		}
		assert!(false_positives < 100, "too many false positives: {false_positives}");
	}

	#[test]
	fn clear_resets_filter() {
		let mut bf = BloomFilter::new();
		bf.insert(b"hello");
		assert!(bf.may_contain(b"hello"));
		bf.clear();
		assert!(bf.is_empty());
		assert!(!bf.may_contain(b"hello"));
	}

	#[test]
	fn atomic_bloom_filter_operations() {
		let abf = AtomicBloomFilter::new();
		assert!(abf.is_empty());
		assert!(!abf.may_contain(b"test_key"));

		abf.insert(b"test_key");
		assert!(!abf.is_empty());
		assert!(abf.may_contain(b"test_key"));
		assert!(!abf.may_contain(b"other_key"));

		abf.clear();
		assert!(abf.is_empty());
		assert!(!abf.may_contain(b"test_key"));
	}
}
