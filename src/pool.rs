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

use crate::{inner::Inner, tx::Transaction, TransactionInner};
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

/// The default transaction pool size

pub(crate) const DEFAULT_POOL_SIZE: usize = 512;

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
			TransactionInner::new(self.inner.clone(), write)
		};

		// Return a new enclosing transaction
		Transaction {
			inner: Some(inner),
			pool: Arc::clone(self),
		}
	}
}
