use crate::version::Version;
use bytes::Bytes;
use smallvec::SmallVec;

pub(crate) enum IndexOrUpdate<'a> {
	/// No need to insert the entry or update
	Ignore,
	/// Insert the entry at the specified index
	Index(usize),
	/// Update an entry with the specified entry
	Update(&'a mut Version),
}

/// A key's MVCC version chain, ordered oldest to newest.
///
/// The inline capacity is one entry: eager commit-time garbage
/// collection keeps every chain at a single live value except while a
/// reader watermark briefly pins superseded versions, and in a large
/// dataset the overwhelming majority of keys are cold — written once
/// and holding exactly one version forever. Sizing the inline buffer
/// for the steady state keeps every cold key at ~a third of the memory
/// a four-slot buffer costs, which dominates total datastore footprint
/// by key count. Keys that do grow a chain under a pinned reader spill
/// to a small heap buffer once (SmallVec retains it thereafter), and
/// even a spilled two-entry chain occupies no more total memory than
/// the old four-slot inline layout.
pub struct Versions {
	inner: SmallVec<[Version; 1]>,
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
	/// Create a new versions object. Only the persistence loader builds
	/// chains incrementally, so this is unused on wasm targets.
	#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
	#[inline]
	pub(crate) fn new() -> Self {
		Versions {
			inner: SmallVec::new(),
		}
	}

	/// Appends or inserts an element into its sorted position.
	///
	/// Entries are NEVER deduplicated by value across different versions:
	/// versions can arrive out of order (concurrent commit applies, and
	/// append-only log replay, both race), so dropping a strictly-newer
	/// version because its value matches an older neighbour silently loses
	/// the write once an intermediate version is inserted between them.
	/// Only pushes of the SAME version are collapsed, which keeps replay
	/// idempotent.
	#[inline]
	pub(crate) fn push(&mut self, value: Version) {
		// Fast path: check if appending to the end
		if let Some(last) = self.inner.last_mut() {
			// Compare the new value with the last value
			match value.version.cmp(&last.version) {
				std::cmp::Ordering::Greater => {
					// Newer version - append
					self.inner.push(value);
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
			// Empty list - push if not a delete. An initial delete is
			// ignored: an absent prefix already reads as `None`.
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
	///   - There is no entry with a version <= value.version and the new
	///     value is a delete (an absent prefix already reads as `None`)
	/// - Return IndexOrUpdate::Update(version) if:
	///   - The new value version is the same as an existing version and we
	///     should update the entry
	/// - Return IndexOrUpdate::Index(index) if:
	///   - The entry belongs at any other sorted position
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
			// Different version - insert in sorted position. No value
			// deduplication here: see the `push` doc comment.
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
		// Only reclaim backing storage once capacity has grown well beyond
		// the live set. Shrinking on every drain would thrash allocations
		// for hot keys under the frequent background GC; the hysteresis keeps
		// steady-state churn cheap while still bounding wasted capacity.
		if self.inner.capacity() > self.inner.len().max(4).saturating_mul(2) {
			self.inner.shrink_to_fit();
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

	/// The newest committed version and value for this key, or `None` when
	/// the chain is empty or its newest entry is a delete tombstone. Used
	/// by the snapshot writers, which persist only the latest visible
	/// state.
	#[cfg(not(target_arch = "wasm32"))]
	#[inline]
	pub(crate) fn latest(&self) -> Option<(u64, Bytes)> {
		self.inner.last().and_then(|v| v.value.clone().map(|value| (v.version, value)))
	}

	/// Test-only view of the raw version chain, oldest first.
	#[cfg(test)]
	pub(crate) fn as_slice(&self) -> &[Version] {
		&self.inner
	}

	/// Whether a future garbage-collection pass could reclaim anything
	/// from this chain. A chain is terminal — and reclamation-free — only
	/// when it holds exactly one live value: superseded versions can be
	/// dropped once no reader needs them, and a newest-entry tombstone
	/// means the whole chain can collapse and the key unlink. Used by the
	/// commit path to decide whether a key needs tracking for the
	/// background sweep.
	#[inline]
	pub(crate) fn needs_gc(&self) -> bool {
		self.inner.len() > 1 || self.inner.last().is_some_and(|v| v.value.is_none())
	}

	/// Remove versions that no reader at a snapshot `>= version` can observe.
	///
	/// `version` is the GC floor: no reader exists below it, but readers may
	/// sit exactly at it or anywhere above. The earliest snapshot any surviving
	/// reader can hold is `version` itself, so the oldest entry we must retain
	/// is the one *visible at* `version` — the latest entry with
	/// `entry.version <= version` — together with every newer entry. Removing
	/// that entry would let a reader whose snapshot lands between it and the
	/// next entry observe the key vanish mid-snapshot (an SI violation).
	#[inline]
	pub(crate) fn gc_older_versions(&mut self, version: u64) -> usize {
		// Number of entries with entry.version <= version.
		let lte = self.find_index_lte_version(version);
		// No entry is <= version: every entry is newer and still required.
		if lte == 0 {
			return self.inner.len();
		}
		// The entry visible at `version`.
		let visible = lte - 1;
		if self.inner[visible].value.is_none() {
			// The visible entry is a delete tombstone. A reader at or above
			// `version` (and below the next entry) observes "absent", which is
			// identical to the entry being gone — so drop the tombstone and
			// everything before it.
			self.drain(..lte);
		} else {
			// The visible entry carries a value a surviving reader may read.
			// Keep it; drop only the strictly-older entries before it.
			self.drain(..visible);
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

	// ==================== Tests for gc_older_versions ====================

	#[test]
	fn test_gc_keeps_version_visible_at_floor() {
		// Regression: the GC floor falls in the gap between a value and a
		// later delete. A reader whose snapshot lands in [floor, delete) must
		// still observe the value, so it must survive GC.
		let mut v = make_versions(vec![(10, Some("v1")), (40, None)]);
		v.gc_older_versions(30);
		// The value visible at 30 (and at 35) must remain readable.
		assert_eq!(v.fetch_version(30), Some(Bytes::from("v1".to_string())));
		assert_eq!(v.fetch_version(35), Some(Bytes::from("v1".to_string())));
		// At/after the delete it is gone.
		assert_eq!(v.fetch_version(40), None);
	}

	#[test]
	fn test_gc_keeps_value_before_newer_version_in_gap() {
		// Floor in the gap between two values: the earlier value is visible at
		// the floor and must survive.
		let mut v = make_versions(vec![(10, Some("v1")), (50, Some("v2"))]);
		v.gc_older_versions(30);
		assert_eq!(v.fetch_version(30), Some(Bytes::from("v1".to_string())));
		assert_eq!(v.fetch_version(49), Some(Bytes::from("v1".to_string())));
		assert_eq!(v.fetch_version(50), Some(Bytes::from("v2".to_string())));
	}

	#[test]
	fn test_gc_drops_versions_below_visible() {
		// Floor exactly on a value: older versions are reclaimed, the visible
		// one is kept.
		let mut v = make_versions(vec![(10, Some("v1")), (30, Some("v2"))]);
		assert_eq!(v.gc_older_versions(30), 1);
		assert_eq!(v.fetch_version(30), Some(Bytes::from("v2".to_string())));
		assert_eq!(v.fetch_version(35), Some(Bytes::from("v2".to_string())));
	}

	#[test]
	fn test_gc_collapses_fully_deleted_key() {
		// Visible entry at the floor is a delete tombstone: the whole chain is
		// reclaimable.
		let mut v = make_versions(vec![(10, Some("v1")), (30, None)]);
		assert_eq!(v.gc_older_versions(40), 0);
		assert_eq!(v.fetch_version(40), None);
	}

	#[test]
	fn test_gc_retains_all_when_floor_below_everything() {
		// Floor below the earliest version: nothing is reclaimable.
		let mut v = make_versions(vec![(10, Some("v1")), (20, Some("v2"))]);
		assert_eq!(v.gc_older_versions(5), 2);
		assert_eq!(v.fetch_version(10), Some(Bytes::from("v1".to_string())));
		assert_eq!(v.fetch_version(20), Some(Bytes::from("v2".to_string())));
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
		// Push same value at newer version - kept: value deduplication
		// across versions is unsound under out-of-order insertion (a
		// later insert between the two would silently lose this write)
		versions.push(make_version(20, Some("v1")));
		assert_eq!(versions.inner.len(), 2);
		// Push different value - should be added
		versions.push(make_version(30, Some("v2")));
		assert_eq!(versions.inner.len(), 3);
		// Push same value again - kept
		versions.push(make_version(40, Some("v2")));
		assert_eq!(versions.inner.len(), 4);
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
		// Fast path: append with same value as last - kept as its own
		// version (cross-version value deduplication is unsound under
		// out-of-order insertion)
		versions.push(make_version(30, Some("v2")));
		assert_eq!(versions.inner.len(), 3);
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
		// Push another delete at version 30 - kept as its own version
		versions.push(make_version(30, None));

		// All three entries are retained
		assert_eq!(versions.inner.len(), 3);
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
	#[test]
	fn versions_inline_footprint_is_small() {
		// The datastore holds one `Versions` per key, so its inline size
		// directly scales total memory by key count. One inline entry plus
		// SmallVec bookkeeping must stay within 56 bytes — sizing the
		// buffer for the steady state (eager commit-time GC keeps chains
		// at a single live value) rather than for transient growth.
		assert!(
			std::mem::size_of::<Versions>() <= 56,
			"Versions grew to {} bytes; the per-key inline footprint is load-bearing for large datasets",
			std::mem::size_of::<Versions>()
		);
	}
}
