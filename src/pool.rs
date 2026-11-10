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

//! This module stores the transaction pool for database transactions.

use crate::inner::Inner;
use crate::tx::Transaction;
use crate::TransactionInner;
use crossbeam_queue::ArrayQueue;
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// The default transaction pool size
pub(crate) const DEFAULT_POOL_SIZE: usize = 512;

/// The most shards a database's transaction pool is split into
const MAX_POOL_SHARDS: usize = 64;

/// Hands out each thread's pool shard index, in the order threads first
/// start a transaction
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
	/// This thread's pool shard index, before reduction modulo the number of
	/// shards, or `usize::MAX` until the thread first starts a transaction
	static SHARD: Cell<usize> = const { Cell::new(usize::MAX) };
}

/// A database's transaction pool, split into shards.
///
/// Every transaction holds a reference to the pool it returns to, so a
/// single pool would see every starting and finishing transaction write
/// the same reference count and the same queue head and tail. Each thread
/// takes its transactions from its own shard instead, so those writes stay
/// on lines that one thread mostly owns. A transaction finished on another
/// thread still returns to the shard it came from.
pub(crate) struct Pools {
	/// The pool shards, one per thread up to the number of shards
	shards: Box<[Arc<Pool>]>,
}

impl Pools {
	/// Creates a transaction pool of `size` transactions in all, split into
	/// one shard per available core, up to [`MAX_POOL_SHARDS`]
	pub(crate) fn new(inner: &Arc<Inner>, size: usize) -> Self {
		let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
		let count = cores.clamp(1, MAX_POOL_SHARDS);
		let each = size.div_ceil(count).max(1);
		Self {
			shards: (0..count).map(|_| Pool::new(Arc::clone(inner), each)).collect(),
		}
	}

	/// Get a new transaction from this thread's shard
	#[inline]
	pub(crate) fn get(&self, write: bool) -> Transaction {
		let shard = SHARD.with(|shard| {
			let mut index = shard.get();
			if index == usize::MAX {
				index = NEXT_SHARD.fetch_add(1, Ordering::Relaxed);
				shard.set(index);
			}
			index
		});
		self.shards[shard % self.shards.len()].get(write)
	}
}

/// A memory-allocated transaction pool for database transactions
pub(crate) struct Pool {
	/// The parent database for this transaction pool
	inner: Arc<Inner>,
	/// A queue for storing the allocated transactions
	pool: ArrayQueue<TransactionInner>,
}

impl Pool {
	/// Creates a new transaction pool for allocated transactions
	pub(crate) fn new(inner: Arc<Inner>, size: usize) -> Arc<Self> {
		Arc::new(Self {
			inner,
			pool: ArrayQueue::new(size),
		})
	}

	/// Put a transaction back into the pool
	pub(crate) fn put(self: &Arc<Self>, inner: TransactionInner) {
		let _ = self.pool.push(inner);
	}

	/// Get a new transaction from the pool
	pub(crate) fn get(self: &Arc<Self>, write: bool) -> Transaction {
		// Fetch a new or pooled inner transaction
		let inner = if let Some(mut tx) = self.pool.pop() {
			tx.reset(write);
			tx
		} else {
			TransactionInner::new(Arc::clone(&self.inner), write)
		};
		// Return a new enclosing transaction
		Transaction {
			inner: Some(inner),
			pool: Arc::clone(self),
		}
	}
}
