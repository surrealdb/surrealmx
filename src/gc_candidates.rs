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

//! The keys whose version chains may still hold garbage.
//!
//! A commit that cannot trim a chain to a single live value tracks the key
//! here, and the background sweep visits only the tracked keys. Each thread
//! appends to a buffer of its own, which only the sweep ever contends for,
//! so tracking a key writes no shared memory. The sweep drains every
//! thread's buffer, and buffers of threads that have exited are dropped
//! once drained, so no tracked key is lost.

use byteslice::ByteSlice;
use std::cell::RefCell;
use std::sync::{Arc, Mutex, PoisonError, Weak};

/// One thread's tracked keys, possibly with duplicates.
type Buffer = Mutex<Vec<ByteSlice>>;

/// Every registered thread buffer.
type Registry = Mutex<Vec<Arc<Buffer>>>;

/// A buffer this thread has registered with one candidate set.
struct ThreadBuffer {
	/// The registry the buffer is registered with
	registry: Weak<Registry>,
	/// The buffer itself, also held by the registry
	buffer: Arc<Buffer>,
}

thread_local! {
	/// The buffers this thread has registered, one per candidate set
	static BUFFERS: RefCell<Vec<ThreadBuffer>> = const { RefCell::new(Vec::new()) };
}

/// Locks a mutex, ignoring poisoning: the guarded vectors stay valid.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
	mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The keys whose version chains may still hold garbage.
pub(crate) struct GcCandidates {
	/// Every thread's buffer
	registry: Arc<Registry>,
	/// Keys tracked where no thread buffer is available: by the sweep
	/// itself, or by a thread whose buffers are being torn down
	shared: Buffer,
}

impl GcCandidates {
	/// Create an empty candidate set.
	pub(crate) fn new() -> Self {
		Self {
			registry: Arc::new(Mutex::new(Vec::new())),
			shared: Mutex::new(Vec::new()),
		}
	}

	/// Track `keys`, in the calling thread's buffer.
	pub(crate) fn track(&self, keys: Vec<ByteSlice>) {
		if keys.is_empty() {
			return;
		}
		let mut keys = Some(keys);
		let _ = BUFFERS.try_with(|buffers| {
			let buffer = self.thread_buffer(&mut buffers.borrow_mut());
			lock(&buffer).extend(keys.take().into_iter().flatten());
		});
		// The thread's buffers are being torn down
		if let Some(keys) = keys {
			lock(&self.shared).extend(keys);
		}
	}

	/// Track `keys` without a thread buffer, as the sweep does when it
	/// re-tracks keys whose garbage is still pinned.
	pub(crate) fn retrack(&self, keys: Vec<ByteSlice>) {
		if !keys.is_empty() {
			lock(&self.shared).extend(keys);
		}
	}

	/// This thread's buffer for this set, registered on first use.
	fn thread_buffer(&self, buffers: &mut Vec<ThreadBuffer>) -> Arc<Buffer> {
		let ptr = Arc::as_ptr(&self.registry);
		if let Some(entry) = buffers.iter().find(|b| std::ptr::eq(b.registry.as_ptr(), ptr)) {
			return Arc::clone(&entry.buffer);
		}
		// Forget buffers of candidate sets since dropped
		buffers.retain(|b| b.registry.strong_count() > 0);
		let buffer = Arc::new(Mutex::new(Vec::new()));
		lock(&self.registry).push(Arc::clone(&buffer));
		buffers.push(ThreadBuffer {
			registry: Arc::downgrade(&self.registry),
			buffer: Arc::clone(&buffer),
		});
		buffer
	}

	/// Take every tracked key, sorted and without duplicates.
	pub(crate) fn drain(&self) -> Vec<ByteSlice> {
		let mut keys = Vec::new();
		lock(&self.registry).retain(|buffer| {
			keys.append(&mut lock(buffer));
			// Keep the buffer while its thread still holds it
			Arc::strong_count(buffer) > 1
		});
		keys.append(&mut lock(&self.shared));
		keys.sort_unstable();
		keys.dedup();
		keys
	}

	/// Forget every tracked key.
	pub(crate) fn clear(&self) {
		drop(self.drain());
	}

	/// Whether no key is tracked.
	pub(crate) fn is_empty(&self) -> bool {
		lock(&self.shared).is_empty() && lock(&self.registry).iter().all(|b| lock(b).is_empty())
	}

	/// Whether `key` is tracked.
	#[cfg(test)]
	pub(crate) fn contains(&self, key: &[u8]) -> bool {
		lock(&self.shared).iter().any(|k| k.as_slice() == key)
			|| lock(&self.registry).iter().any(|b| lock(b).iter().any(|k| k.as_slice() == key))
	}

	/// The number of distinct tracked keys.
	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		let mut keys: Vec<ByteSlice> = lock(&self.shared).clone();
		for buffer in lock(&self.registry).iter() {
			keys.extend(lock(buffer).iter().cloned());
		}
		keys.sort_unstable();
		keys.dedup();
		keys.len()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn keys(names: &[&str]) -> Vec<ByteSlice> {
		names.iter().map(|k| ByteSlice::from(*k)).collect()
	}

	#[test]
	fn drained_keys_are_sorted_and_distinct() {
		let set = GcCandidates::new();
		set.track(keys(&["b", "a"]));
		set.track(keys(&["b", "c"]));
		set.retrack(keys(&["a"]));
		assert_eq!(set.len(), 3);
		assert_eq!(set.drain(), keys(&["a", "b", "c"]));
		assert!(set.is_empty());
	}

	#[test]
	fn keys_tracked_by_exited_threads_are_drained() {
		let set = Arc::new(GcCandidates::new());
		let tracker = Arc::clone(&set);
		std::thread::spawn(move || tracker.track(keys(&["x"]))).join().unwrap();
		assert!(set.contains(b"x"));
		assert_eq!(set.drain(), keys(&["x"]));
		// The exited thread's buffer is dropped once drained
		assert!(lock(&set.registry).is_empty());
	}

	#[test]
	fn sets_keep_their_own_keys() {
		let a = GcCandidates::new();
		let b = GcCandidates::new();
		a.track(keys(&["a"]));
		b.track(keys(&["b"]));
		assert_eq!(a.drain(), keys(&["a"]));
		assert_eq!(b.drain(), keys(&["b"]));
	}
}
