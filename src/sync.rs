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

//! Synchronisation primitives for the commit ring.
//!
//! Under `--cfg loom` these resolve to their `loom` equivalents so that the
//! ring protocol can be model checked; otherwise they are the primitives
//! the engine uses everywhere else. Only the ring is modelled: a database
//! cannot be constructed outside a loom model under `--cfg loom`.

#[cfg(not(loom))]
pub(crate) use parking_lot::RwLock;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::AtomicU64;

#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicU64;

/// A `loom` read-write lock with the non-poisoning `parking_lot` API.
#[cfg(loom)]
pub(crate) struct RwLock<T>(loom::sync::RwLock<T>);

#[cfg(loom)]
impl<T> RwLock<T> {
	pub(crate) fn new(value: T) -> Self {
		Self(loom::sync::RwLock::new(value))
	}

	pub(crate) fn read(&self) -> loom::sync::RwLockReadGuard<'_, T> {
		self.0.read().unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	pub(crate) fn write(&self) -> loom::sync::RwLockWriteGuard<'_, T> {
		self.0.write().unwrap_or_else(std::sync::PoisonError::into_inner)
	}
}

/// Progressive backoff strategy for contention in atomic queues.
#[cfg(not(loom))]
#[inline(always)]
pub(crate) fn backoff(spins: usize) {
	if spins < 10 {
		std::hint::spin_loop();
	} else {
		#[cfg(not(target_arch = "wasm32"))]
		if spins < 100 {
			std::thread::yield_now();
		} else {
			std::thread::park_timeout(std::time::Duration::from_micros(10));
		}
		#[cfg(target_arch = "wasm32")]
		std::hint::spin_loop();
	}
}

/// Loom only makes progress through a spin loop that yields to the
/// scheduler, so every backoff step is a yield.
#[cfg(loom)]
pub(crate) fn backoff(_spins: usize) {
	loom::thread::yield_now();
}
