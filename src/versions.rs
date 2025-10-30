use crate::version::Version;
use bytes::Bytes;
use smallvec::SmallVec;

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

	/// Insert a value into its sorted position
	#[inline]
	pub(crate) fn insert(&mut self, value: Version) {
		let pos = self.inner.binary_search(&value).unwrap_or_else(|e| e);
		self.inner.insert(pos, value);
	}

	/// Appends an element to the back of a collection.
	#[inline]
	pub(crate) fn push(&mut self, value: Version) {
		// Check for any existing version
		if let Some(last) = self.inner.last() {
			// Check if the version is newer
			if value >= *last {
				// Don't add duplicate sequential versions
				if value.value == last.value {
					return;
				}
				// Add the version to the list
				self.inner.push(value);
			} else {
				// Insert at the correct position
				self.insert(value);
			}
		} else if value.value.is_some() {
			// Add the version to the list
			self.inner.push(value);
		}
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

	/// Fetch the entry at a specific version in the versions list.
	#[inline]
	pub(crate) fn find_index_lt_version(&self, version: u64) -> usize {
		// Check the length of the list
		if self.inner.len() <= 4 {
			// Linear search for small lists
			self.inner.iter().rposition(|v| v.version < version).map_or(0, |i| i + 1)
		} else {
			// Use partition_point to find the first element where v.version > version
			self.inner.partition_point(|v| v.version < version)
		}
	}

	/// Fetch the entry at a specific version in the versions list.
	#[inline]
	pub(crate) fn find_index_lte_version(&self, version: u64) -> usize {
		// Check the length of the list
		if self.inner.len() <= 4 {
			// Linear search for small lists
			self.inner.iter().rposition(|v| v.version <= version).map_or(0, |i| i + 1)
		} else {
			// Use partition_point to find the first element where v.version > version
			self.inner.partition_point(|v| v.version <= version)
		}
	}

	/// Fetch the entry at a specific version in the versions list.
	#[inline]
	pub(crate) fn fetch_version(&self, version: u64) -> Option<Bytes> {
		// Use partition_point to find the first element where v.version > version
		let idx = self.find_index_lte_version(version);
		// We want the last element where v.version <= version
		if idx > 0 {
			self.inner.get(idx - 1).and_then(|v| v.value.clone())
		} else {
			None
		}
	}

	/// Check if an entry at a specific version exists and is not a delete.
	#[inline]
	pub(crate) fn exists_version(&self, version: u64) -> bool {
		// Use partition_point to find the first element where v.version > version
		let idx = self.find_index_lte_version(version);
		// We want the last element where v.version <= version
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
		// Use partition_point to find the first element where v.version >= version
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
