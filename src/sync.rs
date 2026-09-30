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
//! [`ArcCell`] reclaims replaced values through `crossbeam-epoch`, which loom
//! does not model, so under loom it is a lock around an `Option<Arc<T>>`.
//! Miri runs the epoch-based cell.
//!
//! The substitutes expose the non-poisoning `parking_lot` API.

#[cfg(not(any(loom, miri)))]
pub(crate) use parking_lot::{RwLock, RwLockWriteGuard};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::AtomicU64;

#[cfg(loom)]
pub(crate) use loom::sync::atomic::AtomicU64;

#[cfg(any(loom, miri))]
pub(crate) use substitute::{RwLock, RwLockWriteGuard};

#[cfg(not(loom))]
pub(crate) use epoch_cell::ArcCell;

#[cfg(loom)]
pub(crate) use substitute::ArcCell;

/// Pins the current thread for a run of [`ArcCell`] reads. Each read pins
/// on its own, but a pin taken while one is held costs no fence.
#[cfg(not(loom))]
#[inline]
pub(crate) fn pin() -> crossbeam_epoch::Guard {
	crossbeam_epoch::pin()
}

/// Pins the current thread for a run of [`ArcCell`] reads.
#[cfg(loom)]
#[inline]
pub(crate) const fn pin() {}

#[cfg(any(loom, miri))]
mod substitute {
	#[cfg(loom)]
	use loom::sync as imp;
	#[cfg(not(loom))]
	use std::sync as imp;
	#[cfg(loom)]
	use std::sync::Arc;
	use std::sync::PoisonError;

	pub(crate) use imp::RwLockWriteGuard;

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
	}

	/// An `Option<Arc<T>>` behind a lock, with the [`super::ArcCell`] API.
	#[cfg(loom)]
	pub(crate) struct ArcCell<T>(RwLock<Option<Arc<T>>>);

	#[cfg(loom)]
	impl<T> ArcCell<T> {
		pub(crate) fn empty() -> Self {
			Self(RwLock::new(None))
		}

		pub(crate) fn read<R>(&self, f: impl FnOnce(Option<&Arc<T>>) -> R) -> R {
			f(self.0.read().as_ref())
		}

		pub(crate) fn swap(&self, value: Option<Arc<T>>) {
			let old = std::mem::replace(&mut *self.0.write(), value);
			drop(old);
		}
	}
}

#[cfg(not(loom))]
#[allow(unsafe_code, reason = "the epoch-protected cell is the engine's only unsafe code")]
mod epoch_cell {
	use crossbeam_epoch as epoch;
	use std::marker::PhantomData;
	use std::mem::ManuallyDrop;
	use std::ptr;
	use std::sync::atomic::{AtomicPtr, Ordering};
	use std::sync::Arc;

	/// An atomically replaceable `Option<Arc<T>>` whose reads take no lock
	/// and write no shared memory.
	///
	/// The cell owns one strong count of the `Arc` it holds, kept as the
	/// pointer returned by `Arc::into_raw` (null for `None`). A read pins an
	/// epoch guard, loads the pointer, and lends the `Arc` without touching
	/// its count. A replacement swaps the pointer out and hands the old count
	/// to the epoch collector, which releases it only once every thread that
	/// was pinned at the time has unpinned. A read that loaded the old
	/// pointer was pinned before the swap, so the count, and with it the
	/// value, outlives the read.
	pub(crate) struct ArcCell<T> {
		ptr: AtomicPtr<T>,
		/// The cell owns an `Option<Arc<T>>`, for auto traits and drop check
		owns: PhantomData<Option<Arc<T>>>,
	}

	/// A raw `Arc` pointer moved into a deferred release.
	struct Retired<T>(*const T);

	// SAFETY: the pointer carries an `Arc<T>` strong count, and dropping an
	// `Arc<T>` on another thread is sound when `T: Send + Sync`.
	unsafe impl<T: Send + Sync> Send for Retired<T> {}

	impl<T> ArcCell<T> {
		/// Creates an empty cell.
		pub(crate) const fn empty() -> Self {
			Self {
				ptr: AtomicPtr::new(ptr::null_mut()),
				owns: PhantomData,
			}
		}
	}

	impl<T: Send + Sync + 'static> ArcCell<T> {
		/// Runs `f` on the current value.
		pub(crate) fn read<R>(&self, f: impl FnOnce(Option<&Arc<T>>) -> R) -> R {
			let _guard = epoch::pin();
			let raw = self.ptr.load(Ordering::Acquire);
			if raw.is_null() {
				return f(None);
			}
			// SAFETY: `raw` came from `Arc::into_raw` in `swap`, and the count
			// it carries is released only by the deferred function in `swap`
			// or by `drop`. The deferred function runs after every guard that
			// was active when it was deferred has been dropped, and `_guard`
			// was pinned before the load that found `raw` in the cell, so it
			// is one of them; `drop` needs `&mut self`, which excludes this
			// read. `ManuallyDrop` lends the count without releasing it.
			let arc = ManuallyDrop::new(unsafe { Arc::from_raw(raw) });
			f(Some(&arc))
		}

		/// Replaces the value. The previous value is released once no read
		/// can still observe it.
		pub(crate) fn swap(&self, value: Option<Arc<T>>) {
			let new = value.map_or(ptr::null_mut(), |v| Arc::into_raw(v).cast_mut());
			let old = self.ptr.swap(new, Ordering::AcqRel);
			if old.is_null() {
				return;
			}
			let retired = Retired(old.cast_const());
			// `old` is no longer in the cell, so only reads pinned before this
			// point can hold it, and the collector runs the function only
			// after they have all unpinned
			epoch::pin().defer(move || {
				let retired = retired;
				// SAFETY: the function owns the count `old` carries, and runs
				// exactly once, after the last read that could lend it.
				drop(unsafe { Arc::from_raw(retired.0) });
			});
		}
	}

	impl<T> Drop for ArcCell<T> {
		fn drop(&mut self) {
			let raw = *self.ptr.get_mut();
			if !raw.is_null() {
				// SAFETY: `&mut self` excludes any read, and the cell owns the
				// count `raw` carries; replaced values own their counts
				// separately.
				drop(unsafe { Arc::from_raw(raw) });
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

#[cfg(all(test, not(loom)))]
mod tests {
	use super::ArcCell;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::Arc;
	use std::time::{Duration, Instant};

	/// Counts how many times it has been dropped.
	struct Counted(Arc<AtomicUsize>);

	impl Drop for Counted {
		fn drop(&mut self) {
			self.0.fetch_add(1, Ordering::SeqCst);
		}
	}

	/// Drives the epoch collector until `count` reaches `expected`.
	fn collect_until(count: &AtomicUsize, expected: usize) {
		let deadline = Instant::now() + Duration::from_secs(10);
		while count.load(Ordering::SeqCst) < expected {
			assert!(Instant::now() < deadline, "replaced values were never released");
			crossbeam_epoch::pin().flush();
			std::thread::yield_now();
		}
	}

	#[test]
	fn an_empty_cell_reads_none() {
		let cell: ArcCell<u64> = ArcCell::empty();
		assert!(cell.read(|v| v.is_none()));
	}

	#[test]
	fn reads_lend_the_value_without_a_count() {
		let value = Arc::new(7u64);
		let cell = ArcCell::empty();
		cell.swap(Some(Arc::clone(&value)));
		cell.read(|v| {
			let v = v.expect("value");
			assert_eq!(**v, 7);
			// The test's handle and the cell's
			assert_eq!(Arc::strong_count(v), 2);
		});
		assert_eq!(Arc::strong_count(&value), 2);
	}

	#[test]
	fn replaced_values_are_released_once() {
		let drops = Arc::new(AtomicUsize::new(0));
		let cell = ArcCell::empty();
		cell.swap(Some(Arc::new(Counted(Arc::clone(&drops)))));
		cell.swap(Some(Arc::new(Counted(Arc::clone(&drops)))));
		cell.swap(None);
		collect_until(&drops, 2);
		drop(cell);
		assert_eq!(drops.load(Ordering::SeqCst), 2);
	}

	#[test]
	fn dropping_the_cell_releases_its_value() {
		let drops = Arc::new(AtomicUsize::new(0));
		let cell = ArcCell::empty();
		cell.swap(Some(Arc::new(Counted(Arc::clone(&drops)))));
		drop(cell);
		assert_eq!(drops.load(Ordering::SeqCst), 1);
	}

	#[test]
	fn reads_never_observe_a_released_value() {
		// Each value holds two copies of the same number, and is released
		// through the collector while readers keep loading the cell
		let (readers, swaps) = if cfg!(miri) {
			(2, 50)
		} else {
			(4, 20_000)
		};
		let cell = Arc::new(ArcCell::empty());
		cell.swap(Some(Arc::new((0u64, 0u64))));
		let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let handles: Vec<_> = (0..readers)
			.map(|_| {
				let cell = Arc::clone(&cell);
				let done = Arc::clone(&done);
				std::thread::spawn(move || {
					while !done.load(Ordering::SeqCst) {
						cell.read(|v| {
							let v = v.expect("never emptied");
							assert_eq!(v.0, v.1);
						});
					}
				})
			})
			.collect();
		for i in 1..=swaps {
			cell.swap(Some(Arc::new((i, i))));
		}
		done.store(true, Ordering::SeqCst);
		for handle in handles {
			handle.join().unwrap();
		}
		cell.read(|v| assert_eq!(**v.expect("value"), (swaps, swaps)));
	}
}
