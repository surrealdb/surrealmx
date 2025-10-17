use crate::version::Version;
use smallvec::SmallVec;
use std::sync::Arc;

pub struct Versions<V>
where
	V: Eq + Clone + Sync + Send + 'static,
{
	inner: SmallVec<[Version<V>; 4]>,
}

impl<V> From<Version<V>> for Versions<V>
where
	V: Eq + Clone + Sync + Send + 'static,
{
	fn from(value: Version<V>) -> Self {
		let mut inner = SmallVec::new();
		inner.push(value);
		Versions {
			inner,
		}
	}
}

impl<V> Versions<V>
where
	V: Eq + Clone + Sync + Send + 'static,
{
	/// Create a new versions object.
	#[inline]
	pub(crate) fn new() -> Self {
		Versions {
			inner: SmallVec::new(),
		}
	}

	/// Insert a value into its sorted position
	#[inline]
	pub(crate) fn insert(&mut self, value: Version<V>) {
		let pos = self.inner.binary_search(&value).unwrap_or_else(|e| e);
		self.inner.insert(pos, value);
	}

	/// Appends an element to the back of a collection.
	#[inline]
	pub(crate) fn push(&mut self, value: Version<V>) {
		if let Some(last) = self.inner.last() {
			if value >= *last {
				self.inner.push(value);
			} else {
				self.insert(value);
			}
		} else {
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

	/// Get the index for a specific version in the versions list.
	#[inline]
	pub(crate) fn find_index(&self, version: u64) -> Option<usize> {
		// Use partition_point to find the first element where v.version >= version
		let idx = self.inner.partition_point(|v| v.version < version);
		// We want the last element where v.version < version
		if idx > 0 {
			Some(idx - 1)
		} else {
			None
		}
	}

	/// Fetch the entry at a specific version in the versions list.
	#[inline]
	pub(crate) fn fetch_version(&self, version: u64) -> Option<Arc<V>> {
		// Use partition_point to find the first element where v.version > version
		let idx = self.inner.partition_point(|v| v.version <= version);
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
		let idx = self.inner.partition_point(|v| v.version <= version);
		// We want the last element where v.version <= version
		if idx > 0 {
			self.inner.get(idx - 1).is_some_and(|v| v.value.is_some())
		} else {
			false
		}
	}

	/// Get all versions as a vector of (version, value) tuples.
	#[inline]
	pub(crate) fn all_versions(&self) -> Vec<(u64, Option<Arc<V>>)> {
		self.inner.iter().map(|v| (v.version, v.value.clone())).collect()
	}

	/// Remove all versions older than the specified version.
	#[inline]
	pub(crate) fn gc_older_versions(&mut self, version: u64) {
		// Use partition_point to find the first element where v.version >= version
		let idx = self.inner.partition_point(|v| v.version < version);
		// Remove all versions before the cutoff
		if idx > 0 {
			self.drain(0..idx);
		}
	}
}
