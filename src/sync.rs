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

//! Synchronisation primitives, swappable for testing.
//!
//! Normal builds use `parking_lot` locks and `std` atomics. Two test
//! configurations substitute them:
//!
//! - Under `--cfg loom` the commit ring's atomics and every lock resolve to
//!   their `loom` equivalents, so the ring protocol can be model checked. Only
//!   the ring is modelled: a database cannot be constructed outside a loom
//!   model in this configuration.
//! - Under Miri the locks are `std` locks. Miri rejects the argument types
//!   `parking_lot_core` passes to the `futex` syscall whenever a lock is
//!   contended, which would otherwise stop every multi-threaded test.
//!
//! The substitutes expose the non-poisoning `parking_lot` API.

#[cfg(not(any(loom, miri)))]
pub(crate) use parking_lot::RwLock;

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::AtomicU64;

#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicU64;

#[cfg(any(loom, miri))]
pub(crate) use substitute::RwLock;

#[cfg(any(loom, miri))]
mod substitute {
	#[cfg(loom)]
	use loom::sync as imp;
	#[cfg(not(loom))]
	use std::sync as imp;
	use std::sync::{PoisonError, TryLockError};

	/// A read-write lock with the non-poisoning `parking_lot` API.
	pub(crate) struct RwLock<T>(imp::RwLock<T>);

	impl<T> RwLock<T> {
		pub(crate) fn new(value: T) -> Self {
			Self(imp::RwLock::new(value))
		}

		pub(crate) fn read(&self) -> imp::RwLockReadGuard<'_, T> {
			self.0.read().unwrap_or_else(PoisonError::into_inner)
		}

		pub(crate) fn write(&self) -> imp::RwLockWriteGuard<'_, T> {
			self.0.write().unwrap_or_else(PoisonError::into_inner)
		}

		pub(crate) fn try_read(&self) -> Option<imp::RwLockReadGuard<'_, T>> {
			match self.0.try_read() {
				Ok(guard) => Some(guard),
				Err(TryLockError::Poisoned(e)) => Some(e.into_inner()),
				Err(TryLockError::WouldBlock) => None,
			}
		}
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
