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

//! A fixed-size, power-of-two ring holding the merge queue, indexed by merge
//! version.
//!
//! Merge versions are dense, so the merge for `version` lives in slot
//! `version & mask`. A merge enters the ring before the clock can publish its
//! version, and leaves it when in-order retirement passes it, so the ring
//! holds exactly the published merges above the retired watermark, plus
//! claimed versions whose entry has been inserted but not yet published.
//!
//! A slot is reused one lap later, by `version + capacity`. That insert waits
//! until the slot's previous merge has been retired and removed. Retirement
//! only needs every earlier merge applied, so the wait is bounded by the
//! slowest apply in flight.
//!
//! Readers take no lock: a slot's merge is loaded atomically, and every merge
//! records its own version, so a reader always knows which lap it is holding.
//! The two writers of a slot, an insert and a removal, serialise on the
//! slot's writer lock.

use crate::queue::Merge;
use crate::sync::{backoff, ArcCell, RwLock};
use byteslice::ByteSlice;
use std::sync::Arc;

/// Default capacity of the merge ring (must be a power of two). Miri builds
/// a far smaller ring, as for the commit ring.
pub(crate) const DEFAULT_MERGE_RING_CAPACITY: usize = if cfg!(miri) {
	1024
} else {
	65536
};

/// A single pre-allocated slot in the merge ring.
struct Slot {
	/// The merge held by this slot, loaded without locking.
	entry: ArcCell<Merge>,
	/// Held by an insert or a removal while it changes the slot.
	writer: RwLock<()>,
}

/// The merge queue: committed merges awaiting retirement, by version.
pub(crate) struct MergeRing {
	/// The pre-allocated circular array of slots.
	slots: Box<[Slot]>,
	/// Bitmask for fast circular indexing: `version & mask`.
	mask: u64,
}

impl MergeRing {
	/// Creates a merge ring with a capacity rounded up to the next power of
	/// two.
	pub(crate) fn new(capacity: usize) -> Self {
		let capacity = capacity.max(1).next_power_of_two();
		let slots = (0..capacity)
			.map(|_| Slot {
				entry: ArcCell::empty(),
				writer: RwLock::new(()),
			})
			.collect();
		Self {
			slots,
			mask: capacity as u64 - 1,
		}
	}

	/// Accesses the slot for the given version in O(1).
	#[inline(always)]
	fn slot(&self, version: u64) -> &Slot {
		let idx = usize::try_from(version & self.mask).unwrap_or(0);
		&self.slots[idx]
	}

	/// Inserts the merge for its version.
	///
	/// Waits while the slot still holds the merge one lap earlier, which is
	/// removed once in-order retirement passes it.
	pub(crate) fn insert(&self, merge: Arc<Merge>) {
		let slot = self.slot(merge.version);
		let mut spins = 0;
		loop {
			let guard = slot.writer.write();
			if slot.entry.read(|e| e.is_none()) {
				slot.entry.swap(Some(merge));
				drop(guard);
				return;
			}
			drop(guard);
			backoff(spins);
			spins += 1;
		}
	}

	/// Runs `f` on the merge for `version`, if it is in the ring.
	#[inline]
	pub(crate) fn get<R>(&self, version: u64, f: impl FnOnce(&Arc<Merge>) -> R) -> Option<R> {
		self.slot(version).entry.read(|e| e.filter(|m| m.version == version).map(f))
	}

	/// Whether the merge for `version` is in the ring.
	#[inline]
	pub(crate) fn contains(&self, version: u64) -> bool {
		self.get(version, |_| ()).is_some()
	}

	/// Removes the merge for `version`, if it is in the ring.
	pub(crate) fn remove(&self, version: u64) {
		let slot = self.slot(version);
		let guard = slot.writer.write();
		if slot.entry.read(|e| e.is_some_and(|m| m.version == version)) {
			// Released once no reader can see it
			slot.entry.swap(None);
		}
		drop(guard);
	}

	/// Looks for the newest merge with a version in `(after, upto]` that
	/// writes `key`, and maps its write (`None` for a delete) with `f`.
	pub(crate) fn newest_write<R>(
		&self,
		after: u64,
		upto: u64,
		key: &[u8],
		f: impl FnOnce(&Option<ByteSlice>) -> R,
	) -> Option<R> {
		// One pin covers every slot read below
		let _pin = crate::sync::pin();
		let mut f = Some(f);
		(after.saturating_add(1)..=upto).rev().find_map(|version| {
			self.get(version, |m| {
				if !m.may_contain_key(key) {
					return None;
				}
				let write = m.writeset.get(key)?;
				f.take().map(|f| f(write))
			})
			.flatten()
		})
	}

	/// The merges with a version in `(after, upto]` whose writeset may
	/// intersect `[beg, end)`, newest first.
	pub(crate) fn overlapping(
		&self,
		after: u64,
		upto: u64,
		beg: &ByteSlice,
		end: &ByteSlice,
	) -> Vec<Arc<Merge>> {
		// One pin covers every slot read below
		let _pin = crate::sync::pin();
		(after.saturating_add(1)..=upto)
			.rev()
			.filter_map(|version| {
				self.get(version, |m| match (&m.min_key, &m.max_key) {
					(Some(min), Some(max))
						if max.as_slice() >= beg.as_slice() && min.as_slice() < end.as_slice() =>
					{
						Some(Arc::clone(m))
					}
					_ => None,
				})
				.flatten()
			})
			.collect()
	}

	/// Whether the ring holds no merges.
	#[cfg(test)]
	pub(crate) fn is_empty(&self) -> bool {
		self.slots.iter().all(|s| s.entry.read(|e| e.is_none()))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::collections::BTreeMap;
	use std::sync::mpsc;
	use std::time::Duration;

	/// A merge at `version` writing each `(key, value)`, `None` for a delete.
	fn merge(version: u64, writes: &[(&str, Option<&str>)]) -> Arc<Merge> {
		let writeset: BTreeMap<ByteSlice, Option<ByteSlice>> =
			writes.iter().map(|(k, v)| (ByteSlice::from(*k), v.map(ByteSlice::from))).collect();
		let mut merge = Merge::new(Arc::new(writeset));
		merge.version = version;
		Arc::new(merge)
	}

	fn bs(s: &str) -> ByteSlice {
		ByteSlice::from(s)
	}

	#[test]
	fn inserted_merges_are_found_by_version() {
		let ring = MergeRing::new(8);
		assert!(ring.is_empty());
		ring.insert(merge(3, &[("a", Some("1"))]));
		assert!(ring.contains(3));
		assert!(!ring.contains(4));
		// Same slot, another lap
		assert!(!ring.contains(11));
		assert_eq!(ring.get(3, |m| m.version), Some(3));
		ring.remove(3);
		assert!(!ring.contains(3));
		assert!(ring.is_empty());
	}

	#[test]
	fn removal_only_clears_its_own_lap() {
		let ring = MergeRing::new(8);
		ring.insert(merge(11, &[("a", Some("1"))]));
		ring.remove(3);
		assert!(ring.contains(11));
	}

	#[test]
	fn insert_waits_for_the_previous_lap_to_be_removed() {
		let ring = Arc::new(MergeRing::new(8));
		ring.insert(merge(3, &[("a", Some("1"))]));
		let (tx, rx) = mpsc::channel();
		let inserter = {
			let ring = Arc::clone(&ring);
			std::thread::spawn(move || {
				ring.insert(merge(11, &[("b", Some("2"))]));
				tx.send(()).unwrap();
			})
		};
		// Version 3 holds the slot until it is removed
		assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
		assert!(ring.contains(3));
		ring.remove(3);
		rx.recv_timeout(Duration::from_secs(10)).unwrap();
		inserter.join().unwrap();
		assert!(ring.contains(11));
	}

	#[test]
	fn newest_write_prefers_the_newest_merge_in_range() {
		let ring = MergeRing::new(16);
		ring.insert(merge(2, &[("a", Some("old")), ("b", Some("b"))]));
		ring.insert(merge(3, &[("c", Some("c"))]));
		ring.insert(merge(4, &[("a", None)]));
		ring.insert(merge(5, &[("a", Some("new"))]));
		let read = |after, upto, key| ring.newest_write(after, upto, key, Clone::clone);
		assert_eq!(read(1, 5, b"a".as_slice()), Some(Some(bs("new"))));
		// A delete is a write
		assert_eq!(read(1, 4, b"a".as_slice()), Some(None));
		assert_eq!(read(1, 3, b"a".as_slice()), Some(Some(bs("old"))));
		// Versions at or below `after` are not consulted
		assert_eq!(read(2, 3, b"a".as_slice()), None);
		assert_eq!(read(1, 5, b"b".as_slice()), Some(Some(bs("b"))));
		assert_eq!(read(1, 5, b"z".as_slice()), None);
	}

	#[test]
	fn overlapping_returns_intersecting_merges_newest_first() {
		let ring = MergeRing::new(16);
		ring.insert(merge(1, &[("a", Some("1")), ("c", Some("1"))]));
		ring.insert(merge(2, &[("x", Some("2"))]));
		ring.insert(merge(3, &[("b", Some("3"))]));
		let versions = |after, upto, beg, end| {
			ring.overlapping(after, upto, &bs(beg), &bs(end))
				.iter()
				.map(|m| m.version)
				.collect::<Vec<_>>()
		};
		assert_eq!(versions(0, 3, "b", "d"), vec![3, 1]);
		assert_eq!(versions(1, 3, "b", "d"), vec![3]);
		// The end bound is exclusive
		assert_eq!(versions(0, 3, "d", "x"), Vec::<u64>::new());
		assert_eq!(versions(0, 3, "d", "y"), vec![2]);
	}
}
