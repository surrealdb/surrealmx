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

use crate::version::Version;
use byteslice::ByteSlice;
use thin_vec::{thin_vec, ThinVec};

/// A key's MVCC version chain, ordered oldest to newest.
///
/// Specialized for the steady-state single-version case: inline commit-time
/// garbage collection keeps over 95% of keys at a single live value.
/// Representing `Versions` as an enum avoids any heap allocation and eliminates
/// vector capacity overhead for single-version keys, shrinking `size_of::<Versions>()`
/// to 40 bytes. When a reader pins older versions, a key temporarily spills to
/// `Chain(ThinVec<Version>)`, which stores header and buffer in a single 8-byte pointer
/// allocation. When GC trims the chain back to 1 live version, it transitions back
/// to `Single(Version)`, immediately freeing heap memory.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Versions {
	/// No versions stored (e.g. empty chain or collapsed tombstone).
	#[default]
	Empty,
	/// Steady-state: exactly one version stored completely inline.
	Single(Version),
	/// Pinned state: multiple versions stored in a single-pointer `ThinVec`.
	Chain(ThinVec<Version>),
}

impl From<Version> for Versions {
	#[inline]
	fn from(value: Version) -> Self {
		if value.value.is_some() {
			Self::Single(value)
		} else {
			Self::Empty
		}
	}
}

impl Versions {
	/// Create a new empty versions object.
	#[inline]
	pub(crate) const fn new() -> Self {
		Self::Empty
	}

	/// Returns a borrowed slice of all versions in sorted order.
	#[allow(dead_code)]
	pub(crate) fn as_slice(&self) -> &[Version] {
		match self {
			Self::Empty => &[],
			Self::Single(ref v) => std::slice::from_ref(v),
			Self::Chain(ref chain) => chain.as_slice(),
		}
	}

	/// Returns the number of versions stored.
	#[allow(dead_code)]
	pub(crate) fn len(&self) -> usize {
		match self {
			Self::Empty => 0,
			Self::Single(_) => 1,
			Self::Chain(ref chain) => chain.len(),
		}
	}

	/// Returns true if there are no versions stored.
	#[allow(dead_code)]
	pub(crate) const fn is_empty(&self) -> bool {
		matches!(self, Self::Empty)
	}

	/// Returns a reference to the latest version, if any.
	#[inline]
	pub(crate) fn last(&self) -> Option<&Version> {
		match self {
			Self::Empty => None,
			Self::Single(ref v) => Some(v),
			Self::Chain(ref chain) => chain.last(),
		}
	}

	/// Appends or inserts an element into its sorted position.
	#[inline]
	pub(crate) fn push(&mut self, value: Version) {
		match self {
			Self::Empty => {
				if value.value.is_some() {
					*self = Self::Single(value);
				}
			}
			Self::Single(ref mut current) => {
				match value.version.cmp(&current.version) {
					std::cmp::Ordering::Greater => {
						// Transition from Single to Chain
						let first = std::mem::replace(current, value.clone());
						*self = Self::Chain(thin_vec![first, value]);
					}
					std::cmp::Ordering::Equal => {
						// Same version - update value if different
						if value.value != current.value {
							current.value = value.value;
						}
					}
					std::cmp::Ordering::Less => {
						// Out-of-order older version
						let older = value;
						let newer = current.clone();
						if older.value.is_none() {
							// Initial delete before the first value is ignored
						} else {
							*self = Self::Chain(thin_vec![older, newer]);
						}
					}
				}
			}
			Self::Chain(ref mut chain) => {
				// Fast path: check if appending to the end
				if let Some(last) = chain.last_mut() {
					match value.version.cmp(&last.version) {
						std::cmp::Ordering::Greater => {
							chain.push(value);
							return;
						}
						std::cmp::Ordering::Equal => {
							if value.value != last.value {
								last.value = value.value;
							}
							return;
						}
						std::cmp::Ordering::Less => {}
					}
				}

				// Slower path: binary search and insert in sorted position
				let idx = chain.partition_point(|v| v.version <= value.version);
				if idx == 0 {
					if value.value.is_some() {
						chain.insert(0, value);
					}
				} else if let Some(existing) = chain.get_mut(idx - 1) {
					if existing.version == value.version {
						if existing.value != value.value {
							existing.value = value.value;
						}
					} else {
						chain.insert(idx, value);
					}
				} else {
					chain.insert(idx, value);
				}
			}
		}
	}

	/// Find the index of the entry where item.version <= version.
	#[inline]
	pub(crate) fn find_index_lte_version(&self, version: u64) -> usize {
		match self {
			Self::Empty => 0,
			Self::Single(ref v) => usize::from(v.version <= version),
			Self::Chain(ref chain) => {
				if let Some(last) = chain.last() {
					if version >= last.version {
						return chain.len();
					}
				}
				if chain.len() <= 4 {
					chain.iter().rposition(|v| v.version <= version).map_or(0, |i| i + 1)
				} else {
					chain.partition_point(|v| v.version <= version)
				}
			}
		}
	}

	/// Fetch the entry at a specific version in the versions list.
	#[inline]
	pub(crate) fn fetch_version(&self, version: u64) -> Option<ByteSlice> {
		match self {
			Self::Empty => None,
			Self::Single(ref v) => {
				if v.version <= version {
					v.value.clone()
				} else {
					None
				}
			}
			Self::Chain(ref chain) => {
				let idx = self.find_index_lte_version(version);
				if idx > 0 {
					chain.get(idx - 1).and_then(|v| v.value.clone())
				} else {
					None
				}
			}
		}
	}

	/// Check if an entry at a specific version exists and is not a delete.
	#[inline]
	pub(crate) fn exists_version(&self, version: u64) -> bool {
		match self {
			Self::Empty => false,
			Self::Single(ref v) => v.version <= version && v.value.is_some(),
			Self::Chain(ref chain) => {
				let idx = self.find_index_lte_version(version);
				if idx > 0 {
					chain.get(idx - 1).is_some_and(|v| v.value.is_some())
				} else {
					false
				}
			}
		}
	}

	/// The newest committed version and value for this key, or `None` when
	/// the chain is empty or its newest entry is a delete tombstone.
	#[cfg(not(target_arch = "wasm32"))]
	#[inline]
	pub(crate) fn latest(&self) -> Option<(u64, ByteSlice)> {
		self.last().and_then(|v| v.value.clone().map(|val| (v.version, val)))
	}

	/// Whether a future garbage-collection pass could reclaim anything from this chain.
	#[inline]
	pub(crate) const fn needs_gc(&self) -> bool {
		match self {
			Self::Empty => false,
			Self::Single(ref v) => v.value.is_none(),
			Self::Chain(_) => true,
		}
	}

	/// Remove versions that no reader at a snapshot `>= version` can observe.
	#[inline]
	pub(crate) fn gc_older_versions(&mut self, version: u64) -> usize {
		match self {
			Self::Empty => 0,
			Self::Single(ref v) => {
				if v.version <= version {
					if v.value.is_none() {
						*self = Self::Empty;
						0
					} else {
						1
					}
				} else {
					1
				}
			}
			Self::Chain(ref mut chain) => {
				let lte = chain.partition_point(|v| v.version <= version);
				if lte == 0 {
					return chain.len();
				}
				let visible = lte - 1;
				if chain[visible].value.is_none() {
					chain.drain(..lte);
				} else {
					chain.drain(..visible);
				}

				if chain.len() == 1 {
					let single = chain.pop().unwrap();
					*self = Self::Single(single);
					1
				} else if chain.is_empty() {
					*self = Self::Empty;
					0
				} else {
					chain.len()
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Helper function to create a Version from a version number and optional value
	fn make_version(version: u64, value: Option<&str>) -> Version {
		Version {
			version,
			value: value.map(ByteSlice::from),
		}
	}

	/// Helper function to create a Versions instance with the given version tuples
	fn make_versions(versions: Vec<(u64, Option<&str>)>) -> Versions {
		let mut v = Versions::new();
		for (version, value) in versions {
			v.push(make_version(version, value));
		}
		v
	}

	#[test]
	fn test_find_index_lte_version_empty() {
		let versions = Versions::new();
		assert_eq!(versions.find_index_lte_version(0), 0);
		assert_eq!(versions.find_index_lte_version(1), 0);
		assert_eq!(versions.find_index_lte_version(100), 0);
	}

	#[test]
	fn test_find_index_lte_version_single_version() {
		let versions = make_versions(vec![(10, Some("value"))]);
		assert_eq!(versions.find_index_lte_version(5), 0);
		assert_eq!(versions.find_index_lte_version(9), 0);
		assert_eq!(versions.find_index_lte_version(10), 1);
		assert_eq!(versions.find_index_lte_version(11), 1);
		assert_eq!(versions.find_index_lte_version(100), 1);
	}

	#[test]
	fn test_find_index_lte_version_multiple_versions() {
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, Some("v2")),
			(30, Some("v3")),
			(40, Some("v4")),
			(50, Some("v5")),
		]);
		assert_eq!(versions.find_index_lte_version(0), 0);
		assert_eq!(versions.find_index_lte_version(5), 0);
		assert_eq!(versions.find_index_lte_version(10), 1);
		assert_eq!(versions.find_index_lte_version(15), 1);
		assert_eq!(versions.find_index_lte_version(20), 2);
		assert_eq!(versions.find_index_lte_version(25), 2);
		assert_eq!(versions.find_index_lte_version(30), 3);
		assert_eq!(versions.find_index_lte_version(35), 3);
		assert_eq!(versions.find_index_lte_version(40), 4);
		assert_eq!(versions.find_index_lte_version(45), 4);
		assert_eq!(versions.find_index_lte_version(50), 5);
		assert_eq!(versions.find_index_lte_version(51), 5);
		assert_eq!(versions.find_index_lte_version(100), 5);
	}

	#[test]
	fn test_find_index_lte_version_with_deletes() {
		let versions =
			make_versions(vec![(10, Some("v1")), (20, None), (30, Some("v3")), (40, None)]);
		assert_eq!(versions.find_index_lte_version(10), 1);
		assert_eq!(versions.find_index_lte_version(15), 1);
		assert_eq!(versions.find_index_lte_version(20), 2);
		assert_eq!(versions.find_index_lte_version(25), 2);
		assert_eq!(versions.find_index_lte_version(30), 3);
		assert_eq!(versions.find_index_lte_version(35), 3);
		assert_eq!(versions.find_index_lte_version(40), 4);
		assert_eq!(versions.find_index_lte_version(50), 4);
	}

	#[test]
	fn test_gc_keeps_version_visible_at_floor() {
		let mut v = make_versions(vec![(10, Some("v1")), (40, None)]);
		v.gc_older_versions(30);
		assert_eq!(v.fetch_version(30), Some(ByteSlice::from("v1")));
		assert_eq!(v.fetch_version(35), Some(ByteSlice::from("v1")));
		assert_eq!(v.fetch_version(40), None);
	}

	#[test]
	fn test_gc_keeps_value_before_newer_version_in_gap() {
		let mut v = make_versions(vec![(10, Some("v1")), (50, Some("v2"))]);
		v.gc_older_versions(30);
		assert_eq!(v.fetch_version(30), Some(ByteSlice::from("v1")));
		assert_eq!(v.fetch_version(49), Some(ByteSlice::from("v1")));
		assert_eq!(v.fetch_version(50), Some(ByteSlice::from("v2")));
	}

	#[test]
	fn test_gc_drops_versions_below_visible() {
		let mut v = make_versions(vec![(10, Some("v1")), (30, Some("v2"))]);
		assert_eq!(v.gc_older_versions(30), 1);
		assert_eq!(v.fetch_version(30), Some(ByteSlice::from("v2")));
		assert_eq!(v.fetch_version(35), Some(ByteSlice::from("v2")));
	}

	#[test]
	fn test_gc_collapses_fully_deleted_key() {
		let mut v = make_versions(vec![(10, Some("v1")), (30, None)]);
		assert_eq!(v.gc_older_versions(40), 0);
		assert_eq!(v.fetch_version(40), None);
	}

	#[test]
	fn test_gc_retains_all_when_floor_below_everything() {
		let mut v = make_versions(vec![(10, Some("v1")), (20, Some("v2"))]);
		assert_eq!(v.gc_older_versions(5), 2);
		assert_eq!(v.fetch_version(10), Some(ByteSlice::from("v1")));
		assert_eq!(v.fetch_version(20), Some(ByteSlice::from("v2")));
	}

	#[test]
	fn test_fetch_version_empty() {
		let versions = Versions::new();
		assert_eq!(versions.fetch_version(0), None);
		assert_eq!(versions.fetch_version(10), None);
		assert_eq!(versions.fetch_version(100), None);
	}

	#[test]
	fn test_fetch_version_single_version() {
		let versions = make_versions(vec![(10, Some("value"))]);
		assert_eq!(versions.fetch_version(5), None);
		assert_eq!(versions.fetch_version(9), None);
		assert_eq!(versions.fetch_version(10), Some(ByteSlice::from("value")));
		assert_eq!(versions.fetch_version(11), Some(ByteSlice::from("value")));
		assert_eq!(versions.fetch_version(100), Some(ByteSlice::from("value")));
	}

	#[test]
	fn test_fetch_version_multiple_versions() {
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, Some("v2")),
			(30, Some("v3")),
			(40, Some("v4")),
			(50, Some("v5")),
		]);
		assert_eq!(versions.fetch_version(5), None);
		assert_eq!(versions.fetch_version(10), Some(ByteSlice::from("v1")));
		assert_eq!(versions.fetch_version(15), Some(ByteSlice::from("v1")));
		assert_eq!(versions.fetch_version(20), Some(ByteSlice::from("v2")));
		assert_eq!(versions.fetch_version(25), Some(ByteSlice::from("v2")));
		assert_eq!(versions.fetch_version(30), Some(ByteSlice::from("v3")));
		assert_eq!(versions.fetch_version(35), Some(ByteSlice::from("v3")));
		assert_eq!(versions.fetch_version(40), Some(ByteSlice::from("v4")));
		assert_eq!(versions.fetch_version(45), Some(ByteSlice::from("v4")));
		assert_eq!(versions.fetch_version(50), Some(ByteSlice::from("v5")));
		assert_eq!(versions.fetch_version(100), Some(ByteSlice::from("v5")));
	}

	#[test]
	fn test_fetch_version_with_deletes() {
		let versions =
			make_versions(vec![(10, Some("v1")), (20, None), (30, Some("v3")), (40, None)]);
		assert_eq!(versions.fetch_version(5), None);
		assert_eq!(versions.fetch_version(10), Some(ByteSlice::from("v1")));
		assert_eq!(versions.fetch_version(15), Some(ByteSlice::from("v1")));
		assert_eq!(versions.fetch_version(20), None);
		assert_eq!(versions.fetch_version(25), None);
		assert_eq!(versions.fetch_version(30), Some(ByteSlice::from("v3")));
		assert_eq!(versions.fetch_version(35), Some(ByteSlice::from("v3")));
		assert_eq!(versions.fetch_version(40), None);
		assert_eq!(versions.fetch_version(50), None);
	}

	#[test]
	fn test_exists_version_empty() {
		let versions = Versions::new();
		assert!(!versions.exists_version(0));
		assert!(!versions.exists_version(10));
		assert!(!versions.exists_version(100));
	}

	#[test]
	fn test_exists_version_single_version() {
		let versions = make_versions(vec![(10, Some("value"))]);
		assert!(!versions.exists_version(5));
		assert!(!versions.exists_version(9));
		assert!(versions.exists_version(10));
		assert!(versions.exists_version(11));
		assert!(versions.exists_version(100));
	}

	#[test]
	fn test_exists_version_multiple_versions() {
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, Some("v2")),
			(30, Some("v3")),
			(40, Some("v4")),
			(50, Some("v5")),
		]);
		assert!(!versions.exists_version(5));
		assert!(versions.exists_version(10));
		assert!(versions.exists_version(15));
		assert!(versions.exists_version(20));
		assert!(versions.exists_version(25));
		assert!(versions.exists_version(30));
		assert!(versions.exists_version(35));
		assert!(versions.exists_version(40));
		assert!(versions.exists_version(45));
		assert!(versions.exists_version(50));
		assert!(versions.exists_version(100));
	}

	#[test]
	fn test_exists_version_with_deletes() {
		let versions =
			make_versions(vec![(10, Some("v1")), (20, None), (30, Some("v3")), (40, None)]);
		assert!(!versions.exists_version(5));
		assert!(versions.exists_version(10));
		assert!(versions.exists_version(15));
		assert!(!versions.exists_version(20));
		assert!(!versions.exists_version(25));
		assert!(versions.exists_version(30));
		assert!(versions.exists_version(35));
		assert!(!versions.exists_version(40));
		assert!(!versions.exists_version(50));
	}

	#[test]
	fn test_push_to_empty_list() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		assert_eq!(versions.len(), 1);
		assert_eq!(versions.fetch_version(10), Some(ByteSlice::from("v1")));
	}

	#[test]
	fn test_push_delete_to_empty_list() {
		let mut versions = Versions::new();
		versions.push(make_version(10, None));
		assert_eq!(versions.len(), 0);
	}

	#[test]
	fn test_push_in_order() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(30, Some("v3")));
		assert_eq!(versions.len(), 3);
		assert_eq!(versions.fetch_version(10), Some(ByteSlice::from("v1")));
		assert_eq!(versions.fetch_version(20), Some(ByteSlice::from("v2")));
		assert_eq!(versions.fetch_version(30), Some(ByteSlice::from("v3")));
	}

	#[test]
	fn test_push_duplicate_values() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		assert_eq!(versions.len(), 1);
		versions.push(make_version(20, Some("v1")));
		assert_eq!(versions.len(), 2);
		versions.push(make_version(30, Some("v2")));
		assert_eq!(versions.len(), 3);
		versions.push(make_version(40, Some("v2")));
		assert_eq!(versions.len(), 4);
	}

	#[test]
	fn test_push_out_of_order() {
		let mut versions = Versions::new();
		versions.push(make_version(30, Some("v3")));
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		assert_eq!(versions.len(), 3);
		assert_eq!(versions.as_slice()[0].version, 10);
		assert_eq!(versions.as_slice()[1].version, 20);
		assert_eq!(versions.as_slice()[2].version, 30);
	}

	#[test]
	fn test_push_with_deletes() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		assert_eq!(versions.len(), 1);
		versions.push(make_version(20, None));
		assert_eq!(versions.len(), 2);
		assert!(!versions.exists_version(20));
		versions.push(make_version(30, Some("v3")));
		assert_eq!(versions.len(), 3);
		assert!(versions.exists_version(30));
	}

	#[test]
	fn test_push_same_version_different_value() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		assert_eq!(versions.len(), 1);
		versions.push(make_version(10, Some("v2")));
		assert_eq!(versions.len(), 1);
		assert_eq!(versions.fetch_version(10), Some(ByteSlice::from("v2")));
	}

	#[test]
	fn test_push_same_version_same_value() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		assert_eq!(versions.len(), 1);
		versions.push(make_version(10, Some("v1")));
		assert_eq!(versions.len(), 1);
		assert_eq!(versions.fetch_version(10), Some(ByteSlice::from("v1")));
	}

	#[test]
	fn test_push_fast_path_append_different_value() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(30, Some("v3")));
		assert_eq!(versions.len(), 3);
		assert_eq!(versions.as_slice()[2].version, 30);
		assert_eq!(versions.fetch_version(30), Some(ByteSlice::from("v3")));
	}

	#[test]
	fn test_push_fast_path_append_same_value() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(30, Some("v2")));
		assert_eq!(versions.len(), 3);
		assert_eq!(versions.fetch_version(30), Some(ByteSlice::from("v2")));
	}

	#[test]
	fn test_push_fast_path_update_last_different_value() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(20, Some("v2_updated")));
		assert_eq!(versions.len(), 2);
		assert_eq!(versions.fetch_version(20), Some(ByteSlice::from("v2_updated")));
	}

	#[test]
	fn test_push_fast_path_update_last_same_value() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(20, Some("v2")));
		assert_eq!(versions.len(), 2);
		assert_eq!(versions.fetch_version(20), Some(ByteSlice::from("v2")));
	}

	#[test]
	fn test_push_fast_path_multiple_updates_to_last() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(10, Some("v2")));
		versions.push(make_version(10, Some("v3")));
		versions.push(make_version(10, Some("v4")));
		assert_eq!(versions.len(), 1);
		assert_eq!(versions.fetch_version(10), Some(ByteSlice::from("v4")));
	}

	#[test]
	fn test_push_fast_path_alternating_append_update() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(20, Some("v2_updated")));
		versions.push(make_version(30, Some("v3")));
		versions.push(make_version(30, Some("v3_updated")));

		assert_eq!(versions.len(), 3);
		assert_eq!(versions.fetch_version(10), Some(ByteSlice::from("v1")));
		assert_eq!(versions.fetch_version(20), Some(ByteSlice::from("v2_updated")));
		assert_eq!(versions.fetch_version(30), Some(ByteSlice::from("v3_updated")));
	}

	#[test]
	fn test_push_slow_path_insert_middle() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(30, Some("v3")));
		versions.push(make_version(20, Some("v2")));

		assert_eq!(versions.len(), 3);
		assert_eq!(versions.as_slice()[0].version, 10);
		assert_eq!(versions.as_slice()[1].version, 20);
		assert_eq!(versions.as_slice()[2].version, 30);
	}

	#[test]
	fn test_push_slow_path_insert_beginning() {
		let mut versions = Versions::new();
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(30, Some("v3")));
		versions.push(make_version(10, Some("v1")));

		assert_eq!(versions.len(), 3);
		assert_eq!(versions.as_slice()[0].version, 10);
		assert_eq!(versions.as_slice()[1].version, 20);
		assert_eq!(versions.as_slice()[2].version, 30);
	}

	#[test]
	fn test_push_slow_path_update_middle() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(30, Some("v3")));
		versions.push(make_version(20, Some("v2_updated")));

		assert_eq!(versions.len(), 3);
		assert_eq!(versions.fetch_version(20), Some(ByteSlice::from("v2_updated")));
	}

	#[test]
	fn test_push_with_delete_at_end() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(30, None));

		assert_eq!(versions.len(), 3);
		assert!(!versions.exists_version(30));
		assert_eq!(versions.fetch_version(30), None);
	}

	#[test]
	fn test_push_delete_then_value_same_version() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, None));
		assert!(!versions.exists_version(20));
		versions.push(make_version(20, Some("v2")));
		assert_eq!(versions.len(), 2);
		assert!(versions.exists_version(20));
		assert_eq!(versions.fetch_version(20), Some(ByteSlice::from("v2")));
	}

	#[test]
	fn test_push_value_then_delete_same_version() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, Some("v2")));
		versions.push(make_version(20, None));

		assert_eq!(versions.len(), 2);
		assert!(!versions.exists_version(20));
		assert_eq!(versions.fetch_version(20), None);
	}

	#[test]
	fn test_push_consecutive_deletes() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		versions.push(make_version(20, None));
		versions.push(make_version(30, None));

		assert_eq!(versions.len(), 3);
		assert!(!versions.exists_version(20));
		assert!(!versions.exists_version(30));
	}

	#[test]
	fn test_push_stress_many_appends() {
		let mut versions = Versions::new();
		for i in 0..100 {
			let value = format!("v{i}");
			versions.push(make_version(i * 10, Some(&value)));
		}
		assert_eq!(versions.len(), 100);
		assert_eq!(versions.as_slice()[0].version, 0);
		assert_eq!(versions.as_slice()[99].version, 990);
	}

	#[test]
	fn test_push_stress_many_updates() {
		let mut versions = Versions::new();
		versions.push(make_version(10, Some("v1")));
		for i in 0..100 {
			let value = format!("v{i}");
			versions.push(make_version(10, Some(&value)));
		}
		assert_eq!(versions.len(), 1);
		assert_eq!(versions.fetch_version(10), Some(ByteSlice::from("v99")));
	}

	#[test]
	fn versions_inline_footprint_is_small() {
		// Memory layout assertion: Versions must stay within 40 bytes!
		assert!(
			std::mem::size_of::<Versions>() <= 40,
			"Versions grew to {} bytes; the per-key inline footprint must stay <= 40 bytes",
			std::mem::size_of::<Versions>()
		);
	}
}
