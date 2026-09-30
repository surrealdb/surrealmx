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

//! Snapshots pinned in the calling thread's own reader slot.
//!
//! A direct scan needs a snapshot pinned for as long as it walks the
//! datastore, or inline GC in concurrent commits could reclaim the versions
//! it reads. Each thread registers one reader slot with each database it
//! scans directly, and keeps it until the thread exits, so a direct scan
//! pins and unpins a slot that only its own thread writes. A direct scan
//! started while the thread's slot is already pinned, such as one nested in
//! another scan's callback, gets no snapshot here and falls back to a read
//! transaction.

use crate::inner::Inner;
use crate::tx::pin_slot;
use std::cell::RefCell;
use std::sync::{Arc, Weak};

/// A reader slot this thread has registered with one database.
struct ThreadSlot {
	/// The database the slot is registered with
	inner: Weak<Inner>,
	/// The index of the slot in the database's readers registry
	slot: usize,
	/// Whether a snapshot on this thread currently holds the slot
	pinned: bool,
}

impl Drop for ThreadSlot {
	fn drop(&mut self) {
		// Hand the slot back when the thread exits, if the database is open
		if let Some(inner) = self.inner.upgrade() {
			inner.readers.release(self.slot);
		}
	}
}

thread_local! {
	/// The reader slots this thread has registered, one per database
	static SLOTS: RefCell<Vec<ThreadSlot>> = const { RefCell::new(Vec::new()) };
}

/// Whether `slot` is this thread's slot for the database at `inner`.
///
/// A live `Weak` keeps its allocation, so no other database can share its
/// address while the entry exists.
fn is_for(slot: &ThreadSlot, inner: &Arc<Inner>) -> bool {
	std::ptr::eq(slot.inner.as_ptr(), Arc::as_ptr(inner))
}

/// A snapshot pinned in this thread's reader slot, unpinned on drop.
pub(crate) struct ThreadSnapshot<'a> {
	/// The database the snapshot is pinned in
	inner: &'a Arc<Inner>,
	/// The index of the pinned slot in the readers registry
	slot: usize,
	/// The pinned version snapshot
	version: u64,
}

impl<'a> ThreadSnapshot<'a> {
	/// Pin a snapshot in this thread's slot for `inner`.
	///
	/// Returns `None` when the slot is already pinned on this thread, or
	/// the thread's slots are being torn down.
	pub(crate) fn pin(inner: &'a Arc<Inner>) -> Option<Self> {
		let slot = SLOTS
			.try_with(|slots| {
				let mut slots = slots.borrow_mut();
				let entry = if let Some(i) = slots.iter().position(|s| is_for(s, inner)) {
					&mut slots[i]
				} else {
					// Forget slots registered with databases since dropped
					slots.retain(|s| s.inner.strong_count() > 0);
					slots.push(ThreadSlot {
						inner: Arc::downgrade(inner),
						slot: inner.readers.register(),
						pinned: false,
					});
					slots.last_mut()?
				};
				if entry.pinned {
					return None;
				}
				entry.pinned = true;
				Some(entry.slot)
			})
			.ok()??;
		let (_, version) = pin_slot(inner, slot);
		Some(Self {
			inner,
			slot,
			version,
		})
	}

	/// The pinned version snapshot.
	#[inline]
	pub(crate) const fn version(&self) -> u64 {
		self.version
	}
}

impl Drop for ThreadSnapshot<'_> {
	fn drop(&mut self) {
		// Release the snapshot before the slot can be pinned again
		self.inner.readers.unpin(self.slot);
		let _ = SLOTS.try_with(|slots| {
			if let Some(entry) = slots.borrow_mut().iter_mut().find(|s| is_for(s, self.inner)) {
				entry.pinned = false;
			}
		});
	}
}
