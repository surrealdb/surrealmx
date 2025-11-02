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

	/// Determine if a new entry should be ignored, inserted, or update an existing entry.
	///
	/// This function works in the following way:
	/// - Return IndexOrUpdate::Ignore if:
	///   - The latest entry with a version <= value.version has the same value as the new value
	///   - The latest entry with a version <= value.version is a delete and the new value is a delete
	/// - Return IndexOrUpdate::Update(version) if:
	///   - The new value version is the same as an existing version and we should update the entry
	/// - Return IndexOrUpdate::Index(index) if:
	///   - There is no entry with a version <= value.version
	///   - The new value version is different to the latest entry with a version <= value.version
	///
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
