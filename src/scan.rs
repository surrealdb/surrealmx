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

//! Datastore walks that do not hold memory reclamation back.
//!
//! An artmap range pins the epoch for as long as it is iterated, and so does
//! the pin that covers version cell reads. Held across a walk of millions of
//! entries, either pin stops the epoch from advancing, so nothing retired in
//! the meantime, such as superseded version chains, is freed until the walk
//! ends, and whichever threads pin next pay for freeing the backlog.
//! [`scan_datastore`] instead walks in chunks of [`REPIN_ENTRIES`] entries,
//! each under pins of its own that are released before the next chunk
//! starts, resuming just after the last key the previous chunk visited.
//!
//! A pin only advances to the current epoch when the thread holds no other
//! pin, so a caller gains nothing from the chunking while it holds one.
//!
//! Each chunk sees the datastore as a fresh range would: keys inserted or
//! removed in between may or may not be visited, exactly as during a single
//! artmap range. Callers read each entry's versions at their own snapshot,
//! which the epoch does not affect.

use crate::direction::Direction;
use crate::version_cell::VersionCell;
use artmap::{ArtMap, EntryRef};
use byteslice::ByteSlice;
use std::ops::Bound;

/// Entries visited under one epoch pin before a walk re-pins. A chunk of
/// this size takes around a millisecond, short enough for memory retired
/// during a long walk to be reclaimed as it goes. Miri builds use far
/// smaller chunks, so that tests cross many chunk boundaries quickly.
pub(crate) const REPIN_ENTRIES: usize = if cfg!(miri) {
	64
} else {
	16384
};

/// Visit the entries of `datastore` between `lower` and `upper` in key
/// order, starting from the end `direction` names, until `f` returns
/// `false`.
pub(crate) fn scan_datastore(
	datastore: &ArtMap<ByteSlice, VersionCell>,
	mut lower: Bound<ByteSlice>,
	mut upper: Bound<ByteSlice>,
	direction: Direction,
	mut f: impl FnMut(&EntryRef<'_, ByteSlice, VersionCell>) -> bool,
) {
	loop {
		// Both pins belong to this chunk alone, and are released at its end
		let guard = datastore.pin();
		let _pin = crate::sync::pin();
		let range =
			datastore.range_with_guard::<_, ByteSlice>((lower.as_ref(), upper.as_ref()), &guard);
		let mut visited = 0;
		let mut resume = None;
		let mut visit = |entry: &EntryRef<'_, ByteSlice, VersionCell>| {
			if !f(entry) {
				return false;
			}
			visited += 1;
			if visited == REPIN_ENTRIES {
				resume = Some(entry.key().clone());
				return false;
			}
			true
		};
		match direction {
			Direction::Forward => {
				for entry in range {
					if !visit(&entry) {
						break;
					}
				}
			}
			Direction::Reverse => {
				for entry in range.rev() {
					if !visit(&entry) {
						break;
					}
				}
			}
		}
		// A full chunk resumes after its last key; anything else is the end
		let Some(key) = resume else {
			return;
		};
		match direction {
			Direction::Forward => lower = Bound::Excluded(key),
			Direction::Reverse => upper = Bound::Excluded(key),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::version::Version;
	use crate::versions::Versions;

	fn key(i: usize) -> ByteSlice {
		ByteSlice::from(format!("key{i:06}").as_str())
	}

	fn datastore(n: usize) -> ArtMap<ByteSlice, VersionCell> {
		let map = ArtMap::new();
		for i in 0..n {
			map.insert(
				key(i),
				VersionCell::new(Versions::from(Version {
					version: 1,
					value: Some(ByteSlice::from("v")),
				})),
			);
		}
		map
	}

	fn walk(
		map: &ArtMap<ByteSlice, VersionCell>,
		lower: Bound<ByteSlice>,
		upper: Bound<ByteSlice>,
		direction: Direction,
		limit: usize,
	) -> Vec<ByteSlice> {
		let mut keys = Vec::new();
		scan_datastore(map, lower, upper, direction, |entry| {
			keys.push(entry.key().clone());
			keys.len() < limit
		});
		keys
	}

	#[test]
	fn visits_every_entry_across_chunks() {
		let n = 3 * REPIN_ENTRIES + 7;
		let map = datastore(n);
		let all = walk(&map, Bound::Unbounded, Bound::Unbounded, Direction::Forward, usize::MAX);
		assert_eq!(all, (0..n).map(key).collect::<Vec<_>>());
		let all = walk(&map, Bound::Unbounded, Bound::Unbounded, Direction::Reverse, usize::MAX);
		assert_eq!(all, (0..n).rev().map(key).collect::<Vec<_>>());
	}

	#[test]
	fn respects_bounds_across_chunks() {
		let map = datastore(3 * REPIN_ENTRIES);
		let lower = Bound::Included(key(10));
		let upper = Bound::Excluded(key(2 * REPIN_ENTRIES + 10));
		let some = walk(&map, lower.clone(), upper.clone(), Direction::Forward, usize::MAX);
		assert_eq!(some, (10..2 * REPIN_ENTRIES + 10).map(key).collect::<Vec<_>>());
		let some = walk(&map, lower, upper, Direction::Reverse, usize::MAX);
		assert_eq!(some, (10..2 * REPIN_ENTRIES + 10).rev().map(key).collect::<Vec<_>>());
	}

	#[test]
	fn stops_when_the_visitor_does() {
		let map = datastore(3 * REPIN_ENTRIES);
		// Stopping exactly at a chunk boundary must not resume
		let some =
			walk(&map, Bound::Unbounded, Bound::Unbounded, Direction::Forward, REPIN_ENTRIES);
		assert_eq!(some, (0..REPIN_ENTRIES).map(key).collect::<Vec<_>>());
		let some = walk(&map, Bound::Unbounded, Bound::Unbounded, Direction::Forward, 5);
		assert_eq!(some.len(), 5);
	}
}
