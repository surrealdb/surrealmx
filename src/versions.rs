use crate::version::Version;
use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::RwLock;
use smallvec::SmallVec;
use std::sync::Arc;

/// Cached snapshot of the latest version for lock-free reads.

pub(crate) struct LatestSnapshot {
	pub version: u64,
	pub value: Option<Bytes>,
}

/// A versioned entry in the datastore that provides lock-free reads
/// for the common case (querying the latest version).

pub struct VersionedEntry {
	/// Cached latest version + value, readable without locking.
	/// Updated after each write.
	latest: ArcSwap<LatestSnapshot>,
	/// Full version history behind RwLock for historical queries and writes.
	pub(crate) versions: RwLock<Versions>,
}

impl VersionedEntry {
	/// Create a new entry with an initial version.

	pub fn new(version: u64, value: Option<Bytes>) -> Self {
		let snapshot = Arc::new(LatestSnapshot {
			version,
			value: value.clone(),
		});

		VersionedEntry {
			latest: ArcSwap::from(snapshot),
			versions: RwLock::new(Versions::from(Version {
				version,
				value,
			})),
		}
	}

	/// Create an entry from a pre-built Versions list (used during snapshot
	/// restore).

	pub fn from_parts(
		versions: Versions,
		latest_version: u64,
		latest_value: Option<Bytes>,
	) -> Self {
		let snapshot = Arc::new(LatestSnapshot {
			version: latest_version,
			value: latest_value,
		});

		VersionedEntry {
			latest: ArcSwap::from(snapshot),
			versions: RwLock::new(versions),
		}
	}

	/// Check if the entry exists (is not deleted) at the given version.
	/// Lock-free fast path for the common case where we're reading
	/// the latest version.
	#[inline]

	pub fn exists_version(&self, version: u64) -> bool {
		// Fast path: if the latest version is visible, check it without locking
		let latest = self.latest.load();

		if latest.version <= version {
			return latest.value.is_some();
		}

		// Slow path: acquire read lock for historical version lookup
		let guard = match self.versions.try_read() {
			Some(g) => g,
			None => self.versions.read(),
		};

		guard.exists_version(version)
	}

	/// Fetch the value at the given version.
	/// Lock-free fast path for the common case where we're reading
	/// the latest version.
	#[inline]

	pub fn fetch_version(&self, version: u64) -> Option<Bytes> {
		// Fast path: if the latest version is visible, return it without locking
		let latest = self.latest.load();

		if latest.version <= version {
			return latest.value.clone();
		}

		// Slow path: acquire read lock for historical version lookup
		let guard = match self.versions.try_read() {
			Some(g) => g,
			None => self.versions.read(),
		};

		guard.fetch_version(version)
	}

	/// Update the cached latest snapshot after a write.
	#[inline]

	pub(crate) fn update_latest(&self, version: u64, value: Option<Bytes>) {
		self.latest.store(Arc::new(LatestSnapshot {
			version,
			value,
		}));
	}
}

pub(crate) enum IndexOrUpdate<'a> {
	/// No need to insert the entry or update
	Ignore,
	/// Insert the entry at the specified index
	Index(usize),
	/// Update an entry with the specified entry
	Update(&'a mut Version),
}

pub struct Versions {
	inner: SmallVec<[Version; 4]>,
}

impl From<Version> for Versions {
	fn from(value: Version) -> Self {
		let mut inner = SmallVec::new();

		inner.push(value);

		Versions {
			inner,
		}
	}
}

impl Versions {
	/// Create a new versions object.
	#[inline]

	pub(crate) fn new() -> Self {
		Versions {
			inner: SmallVec::new(),
		}
	}

	/// Appends or inserts an element into its sorted position.
	#[inline]

	pub(crate) fn push(&mut self, value: Version) {
		// Fast path: check if appending to the end
		if let Some(last) = self.inner.last_mut() {
			// Compare the new value with the last value
			match value.version.cmp(&last.version) {
				std::cmp::Ordering::Greater => {
					// Newer version - append if value is different
					if value.value != last.value {
						self.inner.push(value);
					}

					// Same value, ignore duplicate
					return;
				}
				std::cmp::Ordering::Equal => {
					// Same version - update if value is different
					if value.value != last.value {
						last.value = value.value;
					}

					// Same value, ignore
					return;
				}
				std::cmp::Ordering::Less => {
					// Older version - fall through to slow path
				}
			}
		} else {
			// Empty list - push if not a delete
			if value.value.is_some() {
				self.inner.push(value);
			}

			// Delete on empty list, ignore
			return;
		}

		// Otherwise, use the index or update logic
		match self.fetch_index_or_update(&value) {
			// No need to insert or update the entry
			IndexOrUpdate::Ignore => {
				// Do nothing
			}
			// Insert the entry at the specified index
			IndexOrUpdate::Index(idx) => {
				self.inner.insert(idx, value);
			}
			// Update an existing entry in the list
			IndexOrUpdate::Update(entry) => {
				entry.value = value.value;
			}
		}
	}

	/// Determine if a new entry should be ignored, inserted, or update an
	/// existing entry.
	///
	/// This function works in the following way:
	/// - Return IndexOrUpdate::Ignore if:
	///   - The latest entry with a version <= value.version has the same value
	///     as the new value
	///   - The latest entry with a version <= value.version is a delete and the
	///     new value is a delete
	/// - Return IndexOrUpdate::Update(version) if:
	///   - The new value version is the same as an existing version and we
	///     should update the entry
	/// - Return IndexOrUpdate::Index(index) if:
	///   - There is no entry with a version <= value.version
	///   - The new value version is different to the latest entry with a
	///     version <= value.version
	#[inline]

	pub(crate) fn fetch_index_or_update(&mut self, value: &Version) -> IndexOrUpdate<'_> {
		// Find the index of the item where item.version <= value.version
		let idx = self.find_index_lte_version(value.version);

		// If there is no entry with a version <= value.version
		if idx == 0 {
			// If this is a delete, ignore it (no point storing initial delete)
			if value.value.is_none() {
				return IndexOrUpdate::Ignore;
			}

			// Otherwise, insert at the beginning
			return IndexOrUpdate::Index(0);
		}

		// Get the latest entry with version <= value.version
		if let Some(existing) = self.inner.get_mut(idx - 1) {
			// Check if the version is the same as an existing version
			if existing.version == value.version {
				// Check if the values are the same
				if existing.value == value.value {
					// Same version, same value - ignore
					return IndexOrUpdate::Ignore;
				}

				// Same version, different value - update
				return IndexOrUpdate::Update(existing);
			}

			// Different version - check if values are the same
			if existing.value == value.value {
				// Latest entry has the same value - ignore
				return IndexOrUpdate::Ignore;
			}

			// Different version, different value - insert
			return IndexOrUpdate::Index(idx);
		}

		// Fallback - should not reach here
		IndexOrUpdate::Index(idx)
	}

	/// An iterator that removes the items and yields them by value.
	#[inline]

	pub fn drain<R>(&mut self, range: R)
	where
		R: std::ops::RangeBounds<usize>,
	{
		// Drain the versions
		self.inner.drain(range);

		// Shrink the vec inline
		self.inner.shrink_to_fit();
	}

	/// Check if the item at a specific version is a delete.
	#[inline]

	pub(crate) fn is_delete(&self, version: usize) -> bool {
		self.inner.get(version).is_some_and(|v| v.value.is_none())
	}

	/// Find the index of the entry where item.version < version.
	#[inline]

	pub(crate) fn find_index_lt_version(&self, version: u64) -> usize {
		// Check for any existing version
		if let Some(last) = self.inner.last() {
			// Check if the version is newer
			if version > last.version {
				// Return the index of the last version
				return self.inner.len();
			}
		}

		// Check the list length for reverse iteration or binary search
		if self.inner.len() <= 4 {
			// Use linear search to find the first element where v.version > version
			self.inner.iter().rposition(|v| v.version < version).map_or(0, |i| i + 1)
		} else {
			// Find the index of the item where item.version <= version
			self.inner.partition_point(|v| v.version < version)
		}
	}

	/// Find the index of the entry where item.version <= version.
	#[inline]

	pub(crate) fn find_index_lte_version(&self, version: u64) -> usize {
		// Check for any existing version
		if let Some(last) = self.inner.last() {
			// Check if the version is newer
			if version >= last.version {
				// Return the index of the last version
				return self.inner.len();
			}
		}

		// Check the list length for reverse iteration or binary search
		if self.inner.len() <= 4 {
			// Use linear search to find the first element where v.version > version
			self.inner.iter().rposition(|v| v.version <= version).map_or(0, |i| i + 1)
		} else {
			// Use binary search to find the first element where v.version >= version
			self.inner.partition_point(|v| v.version <= version)
		}
	}

	/// Fetch the entry at a specific version in the versions list.
	#[inline]

	pub(crate) fn fetch_version(&self, version: u64) -> Option<Bytes> {
		// Find the index of the item where item.version <= version
		let idx = self.find_index_lte_version(version);

		// If there is an entry, return the value
		if idx > 0 {
			self.inner.get(idx - 1).and_then(|v| v.value.clone())
		} else {
			None
		}
	}

	/// Check if an entry at a specific version exists and is not a delete.
	#[inline]

	pub(crate) fn exists_version(&self, version: u64) -> bool {
		// Find the index of the item where item.version <= version
		let idx = self.find_index_lte_version(version);

		// If there is an entry, return the value
		if idx > 0 {
			self.inner.get(idx - 1).is_some_and(|v| v.value.is_some())
		} else {
			false
		}
	}

	/// Get all versions as a vector of (version, value) tuples.
	#[inline]

	pub(crate) fn all_versions(&self) -> Vec<(u64, Option<Bytes>)> {
		self.inner.iter().map(|v| (v.version, v.value.clone())).collect()
	}

	/// Remove all versions older than the specified version.
	#[inline]

	pub(crate) fn gc_older_versions(&mut self, version: u64) -> usize {
		// Find the index of the item where item.version < version
		let idx = self.find_index_lt_version(version);

		// Handle the case where all versions are older than the cutoff
		if idx >= self.inner.len() {
			// Check if the last version is a delete
			if let Some(last) = self.inner.last() {
				if last.value.is_none() {
					// Last version is a delete, remove everything
					self.inner.clear();
				} else if idx > 1 {
					// Last version has data, keep it and remove all others
					self.drain(..idx - 1);
				}
			}
		} else if self.is_delete(idx) {
			// Remove all versions up to and including this delete
			self.drain(..=idx);
		} else if idx > 0 {
			// Remove all versions up to this version
			self.drain(..idx);
		}

		// Return the length
		self.inner.len()
	}
}

#[cfg(test)]

mod tests {

	use super::*;
	use bytes::Bytes;

	/// Helper function to create a Version from a version number and optional
	/// value

	fn make_version(version: u64, value: Option<&str>) -> Version {
		Version {
			version,
			value: value.map(|s| Bytes::from(s.to_string())),
		}
	}

	/// Helper function to create a Versions instance with the given version
	/// tuples

	fn make_versions(versions: Vec<(u64, Option<&str>)>) -> Versions {
		let mut v = Versions::new();

		for (version, value) in versions {
			v.push(make_version(version, value));
		}

		v
	}

	// ==================== Tests for find_index_lt_version ====================

	#[test]

	fn test_find_index_lt_version_empty() {
		let versions = Versions::new();

		assert_eq!(versions.find_index_lt_version(0), 0);

		assert_eq!(versions.find_index_lt_version(1), 0);

		assert_eq!(versions.find_index_lt_version(100), 0);
	}

	#[test]

	fn test_find_index_lt_version_single_version() {
		let versions = make_versions(vec![(10, Some("value"))]);

		// Query before the version
		assert_eq!(versions.find_index_lt_version(5), 0);

		assert_eq!(versions.find_index_lt_version(9), 0);

		// Query at the version
		assert_eq!(versions.find_index_lt_version(10), 0);

		// Query after the version
		assert_eq!(versions.find_index_lt_version(11), 1);

		assert_eq!(versions.find_index_lt_version(100), 1);
	}

	#[test]

	fn test_find_index_lt_version_multiple_versions() {
		// Create a small list (≤32 elements) to trigger linear search
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, Some("v2")),
			(30, Some("v3")),
			(40, Some("v4")),
			(50, Some("v5")),
		]);

		// Query before the first version
		assert_eq!(versions.find_index_lt_version(0), 0);

		assert_eq!(versions.find_index_lt_version(5), 0);

		// Query at the first version
		assert_eq!(versions.find_index_lt_version(10), 0);

		// Query after the first version
		assert_eq!(versions.find_index_lt_version(15), 1);

		// Query at the second version
		assert_eq!(versions.find_index_lt_version(20), 1);

		// Query after the second version
		assert_eq!(versions.find_index_lt_version(25), 2);

		assert_eq!(versions.find_index_lt_version(30), 2);

		// Query at the third version
		assert_eq!(versions.find_index_lt_version(35), 3);

		assert_eq!(versions.find_index_lt_version(40), 3);

		// Query at the fourth version
		assert_eq!(versions.find_index_lt_version(45), 4);

		// Query at the fifth version
		assert_eq!(versions.find_index_lt_version(50), 4);

		// Query after the fifth version
		assert_eq!(versions.find_index_lt_version(51), 5);

		assert_eq!(versions.find_index_lt_version(100), 5);
	}

	#[test]

	fn test_find_index_lt_version_with_deletes() {
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, None), // Delete
			(30, Some("v3")),
			(40, None), // Delete
		]);

		// Query before the first version
		assert_eq!(versions.find_index_lt_version(5), 0);

		assert_eq!(versions.find_index_lt_version(9), 0);

		// Query at the first version
		assert_eq!(versions.find_index_lt_version(10), 0);

		// Query after the first version
		assert_eq!(versions.find_index_lt_version(15), 1);

		// Query at the second version
		assert_eq!(versions.find_index_lt_version(20), 1);

		// Query after the second version
		assert_eq!(versions.find_index_lt_version(25), 2);

		assert_eq!(versions.find_index_lt_version(30), 2);

		// Query at the third version
		assert_eq!(versions.find_index_lt_version(35), 3);

		assert_eq!(versions.find_index_lt_version(40), 3);

		// Query after the third version
		assert_eq!(versions.find_index_lt_version(50), 4);
	}

	// ==================== Tests for find_index_lte_version ====================

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

		// Query before the version
		assert_eq!(versions.find_index_lte_version(5), 0);

		assert_eq!(versions.find_index_lte_version(9), 0);

		// Query at the version
		assert_eq!(versions.find_index_lte_version(10), 1);

		// Query after the version
		assert_eq!(versions.find_index_lte_version(11), 1);

		assert_eq!(versions.find_index_lte_version(100), 1);
	}

	#[test]

	fn test_find_index_lte_version_multiple_versions() {
		// Create a small list (≤32 elements) to trigger linear search
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, Some("v2")),
			(30, Some("v3")),
			(40, Some("v4")),
			(50, Some("v5")),
		]);

		// Query before the first version
		assert_eq!(versions.find_index_lte_version(0), 0);

		assert_eq!(versions.find_index_lte_version(5), 0);

		// Query at the first version
		assert_eq!(versions.find_index_lte_version(10), 1);

		// Query after the first version
		assert_eq!(versions.find_index_lte_version(15), 1);

		// Query at the second version
		assert_eq!(versions.find_index_lte_version(20), 2);

		// Query after the second version
		assert_eq!(versions.find_index_lte_version(25), 2);

		// Query at the third version
		assert_eq!(versions.find_index_lte_version(30), 3);

		// Query after the third version
		assert_eq!(versions.find_index_lte_version(35), 3);

		// Query at the fourth version
		assert_eq!(versions.find_index_lte_version(40), 4);

		// Query after the fourth version
		assert_eq!(versions.find_index_lte_version(45), 4);

		// Query at the fifth version
		assert_eq!(versions.find_index_lte_version(50), 5);

		// Query after the fifth version
		assert_eq!(versions.find_index_lte_version(51), 5);

		assert_eq!(versions.find_index_lte_version(100), 5);
	}

	#[test]

	fn test_find_index_lte_version_with_deletes() {
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, None), // Delete
			(30, Some("v3")),
			(40, None), // Delete
		]);

		// Query at the first version
		assert_eq!(versions.find_index_lte_version(10), 1);

		// Query after the first version
		assert_eq!(versions.find_index_lte_version(15), 1);

		// Query at the second version
		assert_eq!(versions.find_index_lte_version(20), 2);

		// Query after the second version
		assert_eq!(versions.find_index_lte_version(25), 2);

		// Query at the third version
		assert_eq!(versions.find_index_lte_version(30), 3);

		// Query after the third version
		assert_eq!(versions.find_index_lte_version(35), 3);

		// Query at the fourth version
		assert_eq!(versions.find_index_lte_version(40), 4);

		// Query after the fourth version
		assert_eq!(versions.find_index_lte_version(50), 4);
	}

	#[test]

	fn test_find_index_lt_vs_lte_difference() {
		// This test demonstrates the key difference between < and <=
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, Some("v2")),
			(30, Some("v3")),
			(40, Some("v4")),
			(50, Some("v5")),
		]);

		// Query at the first version
		assert_eq!(versions.find_index_lt_version(10), 0);

		assert_eq!(versions.find_index_lte_version(10), 1);

		// Query after the first version
		assert_eq!(versions.find_index_lt_version(15), 1);

		assert_eq!(versions.find_index_lte_version(15), 1);

		// Query at the second version
		assert_eq!(versions.find_index_lt_version(20), 1);

		assert_eq!(versions.find_index_lte_version(20), 2);

		// Query at the third version
		assert_eq!(versions.find_index_lt_version(30), 2);

		assert_eq!(versions.find_index_lte_version(30), 3);

		// Query after the third version
		assert_eq!(versions.find_index_lt_version(35), 3);

		assert_eq!(versions.find_index_lte_version(35), 3);
	}

	// ==================== Tests for fetch_version ====================

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

		// Query before the version
		assert_eq!(versions.fetch_version(5), None);

		assert_eq!(versions.fetch_version(9), None);

		// Query at the version
		assert_eq!(versions.fetch_version(10), Some(Bytes::from("value".to_string())));

		// Query after the version
		assert_eq!(versions.fetch_version(11), Some(Bytes::from("value".to_string())));

		assert_eq!(versions.fetch_version(100), Some(Bytes::from("value".to_string())));
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

		// Query before the first version
		assert_eq!(versions.fetch_version(5), None);

		// Query at the first version
		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v1".to_string())));

		// Query after the first version
		assert_eq!(versions.fetch_version(15), Some(Bytes::from("v1".to_string())));

		// Query at the second version
		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2".to_string())));

		// Query after the second version
		assert_eq!(versions.fetch_version(25), Some(Bytes::from("v2".to_string())));

		// Query at the third version
		assert_eq!(versions.fetch_version(30), Some(Bytes::from("v3".to_string())));

		// Query after the third version
		assert_eq!(versions.fetch_version(35), Some(Bytes::from("v3".to_string())));

		// Query at the fourth version
		assert_eq!(versions.fetch_version(40), Some(Bytes::from("v4".to_string())));

		// Query after the fourth version
		assert_eq!(versions.fetch_version(45), Some(Bytes::from("v4".to_string())));

		// Query at the fifth version
		assert_eq!(versions.fetch_version(50), Some(Bytes::from("v5".to_string())));

		// Query after the fifth version
		assert_eq!(versions.fetch_version(100), Some(Bytes::from("v5".to_string())));
	}

	#[test]

	fn test_fetch_version_with_deletes() {
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, None), // Delete
			(30, Some("v3")),
			(40, None), // Delete
		]);

		// Query before the first version
		assert_eq!(versions.fetch_version(5), None);

		// Query at the first version
		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v1".to_string())));

		// Query after the first version
		assert_eq!(versions.fetch_version(15), Some(Bytes::from("v1".to_string())));

		// Query at the second version (delete)
		assert_eq!(versions.fetch_version(20), None);

		// Query after the second version (delete)
		assert_eq!(versions.fetch_version(25), None);

		// Query at the third version
		assert_eq!(versions.fetch_version(30), Some(Bytes::from("v3".to_string())));

		// Query after the third version
		assert_eq!(versions.fetch_version(35), Some(Bytes::from("v3".to_string())));

		// Query at the fourth version (delete)
		assert_eq!(versions.fetch_version(40), None);

		// Query after the fourth version (delete)
		assert_eq!(versions.fetch_version(50), None);
	}

	// ==================== Tests for exists_version ====================

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

		// Query before the version
		assert!(!versions.exists_version(5));

		assert!(!versions.exists_version(9));

		// Query at the version
		assert!(versions.exists_version(10));

		// Query after the version
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

		// Query before the first version
		assert!(!versions.exists_version(5));

		// Query at the first version
		assert!(versions.exists_version(10));

		// Query after the first version
		assert!(versions.exists_version(15));

		// Query at the second version
		assert!(versions.exists_version(20));

		// Query after the second version
		assert!(versions.exists_version(25));

		// Query at the third version
		assert!(versions.exists_version(30));

		// Query after the third version
		assert!(versions.exists_version(35));

		// Query at the fourth version
		assert!(versions.exists_version(40));

		// Query after the fourth version
		assert!(versions.exists_version(45));

		// Query at the fifth version
		assert!(versions.exists_version(50));

		// Query after the fifth version
		assert!(versions.exists_version(100));
	}

	#[test]

	fn test_exists_version_with_deletes() {
		let versions = make_versions(vec![
			(10, Some("v1")),
			(20, None), // Delete
			(30, Some("v3")),
			(40, None), // Delete
		]);

		// Query before the first version
		assert!(!versions.exists_version(5));

		// Query at the first version
		assert!(versions.exists_version(10));

		// Query after the first version
		assert!(versions.exists_version(15));

		// Query at the second version (delete)
		assert!(!versions.exists_version(20));

		// Query after the second version (delete)
		assert!(!versions.exists_version(25));

		// Query at the third version
		assert!(versions.exists_version(30));

		// Query after the third version
		assert!(versions.exists_version(35));

		// Query at the fourth version (delete)
		assert!(!versions.exists_version(40));

		// Query after the fourth version (delete)
		assert!(!versions.exists_version(50));
	}

	// ==================== Tests for push ====================

	#[test]

	fn test_push_to_empty_list() {
		let mut versions = Versions::new();

		// Push a value to empty list
		versions.push(make_version(10, Some("v1")));

		assert_eq!(versions.inner.len(), 1);

		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v1".to_string())));
	}

	#[test]

	fn test_push_delete_to_empty_list() {
		let mut versions = Versions::new();

		// Push a delete (None) to empty list - should not add
		versions.push(make_version(10, None));

		assert_eq!(versions.inner.len(), 0);
	}

	#[test]

	fn test_push_in_order() {
		let mut versions = Versions::new();

		// Push versions in increasing order
		versions.push(make_version(10, Some("v1")));

		versions.push(make_version(20, Some("v2")));

		versions.push(make_version(30, Some("v3")));

		assert_eq!(versions.inner.len(), 3);

		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v1".to_string())));

		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2".to_string())));

		assert_eq!(versions.fetch_version(30), Some(Bytes::from("v3".to_string())));
	}

	#[test]

	fn test_push_duplicate_values() {
		let mut versions = Versions::new();

		// Push first version
		versions.push(make_version(10, Some("v1")));

		assert_eq!(versions.inner.len(), 1);

		// Push same value at newer version - should be skipped
		versions.push(make_version(20, Some("v1")));

		assert_eq!(versions.inner.len(), 1);

		// Push different value - should be added
		versions.push(make_version(30, Some("v2")));

		assert_eq!(versions.inner.len(), 2);

		// Push same value again - should be skipped
		versions.push(make_version(40, Some("v2")));

		assert_eq!(versions.inner.len(), 2);
	}

	#[test]

	fn test_push_out_of_order() {
		let mut versions = Versions::new();

		// Push versions out of order
		versions.push(make_version(30, Some("v3")));

		versions.push(make_version(10, Some("v1")));

		versions.push(make_version(20, Some("v2")));

		// Should be sorted correctly
		assert_eq!(versions.inner.len(), 3);

		assert_eq!(versions.inner[0].version, 10);

		assert_eq!(versions.inner[1].version, 20);

		assert_eq!(versions.inner[2].version, 30);
	}

	#[test]

	fn test_push_with_deletes() {
		let mut versions = Versions::new();

		// Push value, then delete, then value again
		versions.push(make_version(10, Some("v1")));

		assert_eq!(versions.inner.len(), 1);

		// Push delete
		versions.push(make_version(20, None));

		assert_eq!(versions.inner.len(), 2);

		assert!(!versions.exists_version(20));

		// Push new value
		versions.push(make_version(30, Some("v3")));

		assert_eq!(versions.inner.len(), 3);

		assert!(versions.exists_version(30));
	}

	#[test]

	fn test_push_same_version_different_value() {
		let mut versions = Versions::new();

		// Push a version
		versions.push(make_version(10, Some("v1")));

		assert_eq!(versions.inner.len(), 1);

		// Push same version with different value - should update/replace
		versions.push(make_version(10, Some("v2")));

		assert_eq!(versions.inner.len(), 1);

		// The new value should have replaced the old one
		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v2".to_string())));
	}

	#[test]

	fn test_push_same_version_same_value() {
		let mut versions = Versions::new();

		// Push a version
		versions.push(make_version(10, Some("v1")));

		assert_eq!(versions.inner.len(), 1);

		// Push same version with same value - should still update (no-op)
		versions.push(make_version(10, Some("v1")));

		assert_eq!(versions.inner.len(), 1);

		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v1".to_string())));
	}

	// ==================== Fast Path Tests ====================

	#[test]

	fn test_push_fast_path_append_different_value() {
		let mut versions = Versions::new();

		versions.push(make_version(10, Some("v1")));

		versions.push(make_version(20, Some("v2")));

		// Fast path: append with different value
		versions.push(make_version(30, Some("v3")));

		assert_eq!(versions.inner.len(), 3);

		assert_eq!(versions.inner[2].version, 30);

		assert_eq!(versions.fetch_version(30), Some(Bytes::from("v3".to_string())));
	}

	#[test]

	fn test_push_fast_path_append_same_value() {
		let mut versions = Versions::new();

		versions.push(make_version(10, Some("v1")));

		versions.push(make_version(20, Some("v2")));

		// Fast path: append with same value as last - should be ignored
		versions.push(make_version(30, Some("v2")));

		assert_eq!(versions.inner.len(), 2);

		assert_eq!(versions.fetch_version(30), Some(Bytes::from("v2".to_string())));
	}

	#[test]

	fn test_push_fast_path_update_last_different_value() {
		let mut versions = Versions::new();

		versions.push(make_version(10, Some("v1")));

		versions.push(make_version(20, Some("v2")));

		// Fast path: update last version with different value
		versions.push(make_version(20, Some("v2_updated")));

		assert_eq!(versions.inner.len(), 2);

		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2_updated".to_string())));
	}

	#[test]

	fn test_push_fast_path_update_last_same_value() {
		let mut versions = Versions::new();

		versions.push(make_version(10, Some("v1")));

		versions.push(make_version(20, Some("v2")));

		// Fast path: update last version with same value - no-op
		versions.push(make_version(20, Some("v2")));

		assert_eq!(versions.inner.len(), 2);

		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2".to_string())));
	}

	#[test]

	fn test_push_fast_path_multiple_updates_to_last() {
		let mut versions = Versions::new();

		versions.push(make_version(10, Some("v1")));

		// Multiple sequential updates to the same version
		versions.push(make_version(10, Some("v2")));

		versions.push(make_version(10, Some("v3")));

		versions.push(make_version(10, Some("v4")));

		assert_eq!(versions.inner.len(), 1);

		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v4".to_string())));
	}

	#[test]

	fn test_push_fast_path_alternating_append_update() {
		let mut versions = Versions::new();

		// Append version 10
		versions.push(make_version(10, Some("v1")));

		// Append version 20
		versions.push(make_version(20, Some("v2")));

		// Update version 20
		versions.push(make_version(20, Some("v2_updated")));

		// Append version 30
		versions.push(make_version(30, Some("v3")));

		// Update version 30
		versions.push(make_version(30, Some("v3_updated")));

		assert_eq!(versions.inner.len(), 3);

		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v1".to_string())));

		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2_updated".to_string())));

		assert_eq!(versions.fetch_version(30), Some(Bytes::from("v3_updated".to_string())));
	}

	#[test]

	fn test_push_slow_path_insert_middle() {
		let mut versions = Versions::new();

		versions.push(make_version(10, Some("v1")));

		versions.push(make_version(30, Some("v3")));

		// Slow path: insert in the middle (version < last.version)
		versions.push(make_version(20, Some("v2")));

		assert_eq!(versions.inner.len(), 3);

		assert_eq!(versions.inner[0].version, 10);

		assert_eq!(versions.inner[1].version, 20);

		assert_eq!(versions.inner[2].version, 30);
	}

	#[test]

	fn test_push_slow_path_insert_beginning() {
		let mut versions = Versions::new();

		versions.push(make_version(20, Some("v2")));

		versions.push(make_version(30, Some("v3")));

		// Slow path: insert at the beginning
		versions.push(make_version(10, Some("v1")));

		assert_eq!(versions.inner.len(), 3);

		assert_eq!(versions.inner[0].version, 10);

		assert_eq!(versions.inner[1].version, 20);

		assert_eq!(versions.inner[2].version, 30);
	}

	#[test]

	fn test_push_slow_path_update_middle() {
		let mut versions = Versions::new();

		versions.push(make_version(10, Some("v1")));

		versions.push(make_version(20, Some("v2")));

		versions.push(make_version(30, Some("v3")));

		// Slow path: update a middle version
		versions.push(make_version(20, Some("v2_updated")));

		assert_eq!(versions.inner.len(), 3);

		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2_updated".to_string())));
	}

	#[test]

	fn test_push_with_delete_at_end() {
		let mut versions = Versions::new();

		versions.push(make_version(10, Some("v1")));

		versions.push(make_version(20, Some("v2")));

		// Fast path: append delete
		versions.push(make_version(30, None));

		assert_eq!(versions.inner.len(), 3);

		assert!(!versions.exists_version(30));

		assert_eq!(versions.fetch_version(30), None);
	}

	#[test]

	fn test_push_delete_then_value_same_version() {
		let mut versions = Versions::new();

		versions.push(make_version(10, Some("v1")));

		// Push delete
		versions.push(make_version(20, None));

		assert!(!versions.exists_version(20));

		// Update same version with a value
		versions.push(make_version(20, Some("v2")));

		assert_eq!(versions.inner.len(), 2);

		assert!(versions.exists_version(20));

		assert_eq!(versions.fetch_version(20), Some(Bytes::from("v2".to_string())));
	}

	#[test]

	fn test_push_value_then_delete_same_version() {
		let mut versions = Versions::new();

		versions.push(make_version(10, Some("v1")));

		versions.push(make_version(20, Some("v2")));

		// Update last version to delete
		versions.push(make_version(20, None));

		assert_eq!(versions.inner.len(), 2);

		assert!(!versions.exists_version(20));

		assert_eq!(versions.fetch_version(20), None);
	}

	#[test]

	fn test_push_consecutive_deletes() {
		let mut versions = Versions::new();

		versions.push(make_version(10, Some("v1")));

		// Push delete at version 20
		versions.push(make_version(20, None));

		// Push another delete at version 30 (different from last value which is None)
		versions.push(make_version(30, None));

		// Should only have 2 entries - version 20 and 30 deletes should be separate
		assert_eq!(versions.inner.len(), 2);

		assert!(!versions.exists_version(20));

		assert!(!versions.exists_version(30));
	}

	#[test]

	fn test_push_stress_many_appends() {
		let mut versions = Versions::new();

		// Push many versions in order (all fast path appends)
		for i in 0..100 {
			let value = format!("v{}", i);

			versions.push(make_version(i * 10, Some(&value)));
		}

		assert_eq!(versions.inner.len(), 100);

		assert_eq!(versions.inner[0].version, 0);

		assert_eq!(versions.inner[99].version, 990);
	}

	#[test]

	fn test_push_stress_many_updates() {
		let mut versions = Versions::new();

		versions.push(make_version(10, Some("v1")));

		// Update the same version many times (all fast path updates)
		for i in 0..100 {
			let value = format!("v{}", i);

			versions.push(make_version(10, Some(&value)));
		}

		assert_eq!(versions.inner.len(), 1);

		assert_eq!(versions.fetch_version(10), Some(Bytes::from("v99".to_string())));
	}
}
