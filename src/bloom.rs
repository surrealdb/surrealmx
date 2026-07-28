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

//! A lightweight bloom filter for probabilistic set membership testing.

const BLOOM_BITS: usize = 4096;
const BLOOM_BYTES: usize = BLOOM_BITS / 8;
const NUM_HASHES: u32 = 3;

pub(crate) struct BloomFilter {
	/// The bit array backing the bloom filter
	bits: [u8; BLOOM_BYTES],
	/// The number of keys inserted into the filter
	count: usize,
}

impl BloomFilter {
	/// Create a new empty bloom filter
	pub const fn new() -> Self {
		Self {
			bits: [0; BLOOM_BYTES],
			count: 0,
		}
	}

	/// Insert a key into the bloom filter
	#[inline]
	pub fn insert(&mut self, key: &[u8]) {
		// Compute the dual hash for this key
		let h = Self::hash(key);
		// Set all k hash positions in the bit array
		for i in 0..NUM_HASHES {
			let bit = Self::nth_hash(h, i) % (BLOOM_BITS as u64);
			self.bits[(bit / 8) as usize] |= 1 << (bit % 8);
		}
		// Increment the insert counter
		self.count += 1;
	}

	/// Check whether a key may be present in the filter
	#[inline]
	pub fn may_contain(&self, key: &[u8]) -> bool {
		// Fast path: empty filter contains nothing
		if self.count == 0 {
			return false;
		}
		// Compute the dual hash for this key
		let h = Self::hash(key);
		// Check all k hash positions in the bit array
		for i in 0..NUM_HASHES {
			let bit = Self::nth_hash(h, i) % (BLOOM_BITS as u64);
			if self.bits[(bit / 8) as usize] & (1 << (bit % 8)) == 0 {
				return false;
			}
		}
		// All bits are set, so the key may be present
		true
	}

	/// Check whether the filter is empty
	pub const fn is_empty(&self) -> bool {
		self.count == 0
	}

	/// Reset the filter to its initial empty state
	pub const fn clear(&mut self) {
		self.bits = [0; BLOOM_BYTES];
		self.count = 0;
	}

	/// Compute a dual FNV-1a hash for double hashing
	#[inline]
	fn hash(key: &[u8]) -> (u64, u64) {
		// Compute the primary FNV-1a hash
		let mut h1: u64 = 0xcbf2_9ce4_8422_2325;
		for &b in key {
			h1 ^= u64::from(b);
			h1 = h1.wrapping_mul(0x0100_0000_01b3);
		}
		// Derive the secondary hash via multiplication and rotation
		let h2 = h1.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(31);
		// Return the dual hash pair
		(h1, h2)
	}

	/// Compute the nth hash from the dual hash pair
	#[inline]
	fn nth_hash(hashes: (u64, u64), n: u32) -> u64 {
		hashes.0.wrapping_add(u64::from(n).wrapping_mul(hashes.1))
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
}
