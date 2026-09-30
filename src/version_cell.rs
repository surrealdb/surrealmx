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

//! A key's version chain, read without taking a lock.
//!
//! Readers load the current chain through an epoch-protected cell, so a read
//! writes no shared memory, however many threads read the same key. Writers
//! serialise on a per-key mutex, build the next chain from a copy of the
//! current one, and swap it in. A replaced chain is freed only once every
//! reader that could still hold it has finished.

use crate::sync::ArcCell;
use crate::versions::Versions;
use parking_lot::{Mutex, MutexGuard};
use std::sync::Arc;

/// The chain every empty cell reads as.
static EMPTY: Versions = Versions::Empty;

/// A key's version chain in the datastore.
pub(crate) struct VersionCell {
	/// Serialises the writers of this chain
	writer: Mutex<()>,
	/// The current chain, or `None` when it is empty
	chain: ArcCell<Versions>,
}

impl VersionCell {
	/// Create a cell holding `versions`.
	pub(crate) fn new(versions: Versions) -> Self {
		let cell = Self {
			writer: Mutex::new(()),
			chain: ArcCell::empty(),
		};
		cell.store(versions);
		cell
	}

	/// Runs `f` on the current chain.
	#[inline]
	pub(crate) fn read<R>(&self, f: impl FnOnce(&Versions) -> R) -> R {
		self.chain.read(|chain| f(chain.map_or(&EMPTY, |c| &**c)))
	}

	/// Lock the chain for writing.
	///
	/// Readers are not blocked: until the writer replaces the chain, they
	/// keep reading the current one.
	#[inline]
	pub(crate) fn lock(&self) -> ChainWriter<'_> {
		ChainWriter {
			_guard: self.writer.lock(),
			cell: self,
		}
	}

	/// Publish `versions` as the current chain.
	fn store(&self, versions: Versions) {
		self.chain.swap(match versions {
			Versions::Empty => None,
			chain => Some(Arc::new(chain)),
		});
	}
}

/// Exclusive write access to a key's chain.
pub(crate) struct ChainWriter<'a> {
	/// Held until the writer is dropped
	_guard: MutexGuard<'a, ()>,
	/// The cell being written
	cell: &'a VersionCell,
}

impl ChainWriter<'_> {
	/// Runs `f` on the current chain.
	#[inline]
	pub(crate) fn read<R>(&self, f: impl FnOnce(&Versions) -> R) -> R {
		self.cell.read(f)
	}

	/// Replace the chain with the result of `f` applied to a copy of it.
	#[inline]
	pub(crate) fn update<R>(&mut self, f: impl FnOnce(&mut Versions) -> R) -> R {
		let mut next = self.cell.read(Clone::clone);
		let res = f(&mut next);
		self.cell.store(next);
		res
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::version::Version;
	use byteslice::ByteSlice;

	fn version(version: u64, value: Option<&str>) -> Version {
		Version {
			version,
			value: value.map(ByteSlice::from),
		}
	}

	#[test]
	fn an_empty_chain_reads_as_empty() {
		let cell = VersionCell::new(Versions::Empty);
		assert_eq!(cell.read(Clone::clone), Versions::Empty);
		// A lone delete seeds an empty chain
		let cell = VersionCell::new(Versions::from(version(1, None)));
		assert_eq!(cell.read(Versions::len), 0);
	}

	#[test]
	fn updates_replace_the_chain_readers_see() {
		let cell = VersionCell::new(Versions::from(version(1, Some("a"))));
		let mut writer = cell.lock();
		writer.update(|v| v.push(version(2, Some("b"))));
		assert_eq!(writer.read(Versions::len), 2);
		drop(writer);
		assert_eq!(cell.read(|v| v.fetch_version(1)), Some(ByteSlice::from("a")));
		assert_eq!(cell.read(|v| v.fetch_version(2)), Some(ByteSlice::from("b")));
		// Collecting every version empties the chain
		cell.lock().update(|v| {
			v.push(version(3, None));
			v.gc_older_versions(3)
		});
		assert_eq!(cell.read(Versions::len), 0);
	}

	#[test]
	fn readers_do_not_wait_for_a_writer() {
		let cell = Arc::new(VersionCell::new(Versions::from(version(1, Some("a")))));
		let writer = cell.lock();
		let reader = {
			let cell = Arc::clone(&cell);
			std::thread::spawn(move || cell.read(|v| v.fetch_version(1)))
		};
		assert_eq!(reader.join().unwrap(), Some(ByteSlice::from("a")));
		drop(writer);
	}
}
