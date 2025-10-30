use crate::inner::Inner;
use crate::tx::Transaction;
use crate::TransactionInner;
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
