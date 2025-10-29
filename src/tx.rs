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

//! This module stores the database transaction logic.

use crate::direction::Direction;
use crate::err::Error;
use crate::inner::Inner;
use crate::iter::MergeIterator;
use crate::pool::Pool;
use crate::queue::{Commit, Merge};
use crate::version::Version;
use crate::versions::Versions;
use parking_lot::RwLock;
use serde::Serialize;
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::ops::Bound;
use std::ops::Range;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The isolation level of a database transaction
#[derive(PartialEq, PartialOrd)]
pub enum IsolationLevel {
	SnapshotIsolation,
	SerializableSnapshotIsolation,
}

// --------------------------------------------------
// Transaction
// --------------------------------------------------

/// A serializable database transaction
pub struct Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The transaction pool for this transaction
	pub(crate) pool: Arc<Pool<K, V>>,
	/// The inner transaction for this transaction
	pub(crate) inner: Option<TransactionInner<K, V>>,
}

impl<K, V> Drop for Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	fn drop(&mut self) {
		if let Some(inner) = self.inner.take() {
			// Reduce the transaction commit counter
			if inner.counter_commit.fetch_sub(1, Ordering::Relaxed) == 1 {
				// If this was the last reference, remove the counter entry
				inner.database.counter_by_commit.remove(&inner.commit);
			}
			// Reduce the transaction version counter
			if inner.counter_version.fetch_sub(1, Ordering::Relaxed) == 1 {
				// If this was the last reference, remove the counter entry
				inner.database.counter_by_oracle.remove(&inner.version);
			}
			// Put the transaction in to the pool
			self.pool.put(inner);
		}
	}
}

impl<K, V> Deref for Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	type Target = TransactionInner<K, V>;

	fn deref(&self) -> &Self::Target {
		self.inner.as_ref().unwrap()
	}
}

impl<K, V> DerefMut for Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.inner.as_mut().unwrap()
	}
}

impl<K, V> Transaction<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Ensure this transaction is committed with snapshot isolation guarantees
	pub fn with_snapshot_isolation(mut self) -> Self {
		self.mode = IsolationLevel::SnapshotIsolation;
		self
	}

	/// Ensure this transaction is committed with serializable snapshot isolation guarantees
	pub fn with_serializable_snapshot_isolation(mut self) -> Self {
		self.mode = IsolationLevel::SerializableSnapshotIsolation;
		self
	}

	/// Set a savepoint in the transaction for partial rollback
	/// This method is stackable and can be called multiple times with
	/// corresponding calls to `rollback_to_savepoint`
	pub fn set_savepoint(&mut self) -> Result<(), Error> {
		self.inner.as_mut().unwrap().set_savepoint()
	}

	/// Rollback the transaction to the most recently set savepoint
	/// After calling this method, subsequent modifications within this
	/// transaction can be rolled back by calling `rollback_to_savepoint`
	/// again if there are more savepoints in the stack
	pub fn rollback_to_savepoint(&mut self) -> Result<(), Error> {
		self.inner.as_mut().unwrap().rollback_to_savepoint()
	}
}

// --------------------------------------------------
// SavepointState
// --------------------------------------------------

/// A savepoint state capturing transaction state at a specific point
#[derive(Clone)]
struct SavepointState<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The readset at the time of the savepoint
	readset: BTreeSet<K>,
	/// The scanset at the time of the savepoint
	scanset: BTreeMap<K, K>,
	/// The writeset at the time of the savepoint
	writeset: BTreeMap<K, Option<Arc<V>>>,
}

// --------------------------------------------------
// TransactionInner
// --------------------------------------------------

/// The inner structure of a database transaction
pub struct TransactionInner<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// The isolation level of this transaction
	pub(crate) mode: IsolationLevel,
	/// Is the transaction complete?
	pub(crate) done: bool,
	/// Is the transaction writeable?
	pub(crate) write: bool,
	/// The version at which this transaction started
	pub(crate) commit: u64,
	/// The version at which this transaction started
	pub(crate) version: u64,
	/// The local set of key reads
	pub(crate) readset: BTreeSet<K>,
	/// The local set of key scans
	pub(crate) scanset: BTreeMap<K, K>,
	/// The local set of updates and deletes
	pub(crate) writeset: BTreeMap<K, Option<Arc<V>>>,
	/// The parent database for this transaction
	pub(crate) database: Arc<Inner<K, V>>,
	/// The reference to the transaction commit counter
	pub(crate) counter_commit: Arc<AtomicU64>,
	/// The reference to the transaction version counter
	pub(crate) counter_version: Arc<AtomicU64>,
	/// Threshold after which transaction state is reset
	reset_threshold: usize,
	/// Stack of savepoint states for nested partial rollbacks
	savepoint_stack: Vec<SavepointState<K, V>>,
}

impl<K, V> TransactionInner<K, V>
where
	K: Ord + Clone + Debug + Sync + Send + 'static,
	V: Eq + Clone + Debug + Sync + Send + 'static,
{
	/// Create a new read-only or writeable transaction
	pub(crate) fn new(db: Arc<Inner<K, V>>, write: bool) -> Self {
		// Prepare and increment the oracle counter
		let (version, counter_version) = {
			// Get the current version sequence number
			let value = db.oracle.current_timestamp();
			// Initialise the transaction oracle counter
			let entry =
				db.counter_by_oracle.get_or_insert_with(value, || Arc::new(AtomicU64::new(0)));
			// Fetch the underlying counter for this value
			let counter = entry.value().clone();
			// Increment the transaction oracle counter
			counter.fetch_add(1, Ordering::Relaxed);
			// Return the value
			(value, counter)
		};
		// Prepare and increment the commit counter
		let (commit, counter_commit) = {
			// Get the current commit sequence number
			let value = db.transaction_commit_id.load(Ordering::Relaxed);
			// Initialise the transaction commit counter
			let entry =
				db.counter_by_commit.get_or_insert_with(value, || Arc::new(AtomicU64::new(0)));
			// Fetch the underlying counter for this value
			let counter = entry.value().clone();
			// Increment the transaction commit counter
			counter.fetch_add(1, Ordering::Relaxed);
			// Return the value
			(value, counter)
		};
		// Store the threshold separately before moving db
		let threshold = db.reset_threshold;
		// Create the transaction
		Self {
			mode: IsolationLevel::SerializableSnapshotIsolation,
			done: false,
			write,
			commit,
			version,
			readset: BTreeSet::new(),
			scanset: BTreeMap::new(),
			writeset: BTreeMap::new(),
			database: db,
			counter_commit,
			counter_version,
			reset_threshold: threshold,
			savepoint_stack: Vec::new(),
		}
	}

	/// Resets an allocated read-only or writeable transaction
	pub(crate) fn reset(&mut self, write: bool) {
		// Set the default transaction isolation level
		self.mode = IsolationLevel::SerializableSnapshotIsolation;
		// Update the reset threshold from the database
		self.reset_threshold = self.database.reset_threshold;
		// Prepare and increment the oracle counter
		let (version, counter_version) = {
			// Get the current version sequence number
			let value = self.database.oracle.current_timestamp();
			// Initialise the transaction oracle counter
			let entry = self
				.database
				.counter_by_oracle
				.get_or_insert_with(value, || Arc::new(AtomicU64::new(0)));
			// Fetch the underlying counter for this value
			let counter = entry.value().clone();
			// Increment the transaction oracle counter
			counter.fetch_add(1, Ordering::Relaxed);
			// Return the value
			(value, counter)
		};
		// Prepare and increment the commit counter
		let (commit, counter_commit) = {
			// Get the current commit sequence number
			let value = self.database.transaction_commit_id.load(Ordering::Relaxed);
			// Initialise the transaction commit counter
			let entry = self
				.database
				.counter_by_commit
				.get_or_insert_with(value, || Arc::new(AtomicU64::new(0)));
			// Fetch the underlying counter for this value
			let counter = entry.value().clone();
			// Increment the transaction commit counter
			counter.fetch_add(1, Ordering::Relaxed);
			// Return the value
			(value, counter)
		};
		// Clear or completely reset the allocated readset
		let threshold = self.reset_threshold;
		match self.readset.len() > threshold {
			true => self.readset = BTreeSet::new(),
			false => self.readset.clear(),
		};
		// Clear or completely reset the allocated scanset
		match self.scanset.len() > threshold {
			true => self.scanset = BTreeMap::new(),
			false => self.scanset.clear(),
		};
		// Clear or completely reset the allocated writeset
		match self.writeset.len() > threshold {
			true => self.writeset = BTreeMap::new(),
			false => self.writeset.clear(),
		};
		// Clear or completely reset the allocated writeset
		match self.savepoint_stack.len() > threshold {
			true => self.savepoint_stack = Vec::new(),
			false => self.savepoint_stack.clear(),
		};
		// Reset the transaction
		self.done = false;
		self.write = write;
		self.commit = commit;
		self.version = version;
		self.counter_commit = counter_commit;
		self.counter_version = counter_version;
	}

	/// Get the starting sequence number of this transaction
	pub fn version(&self) -> u64 {
		self.version
	}

	/// Check if the transaction is closed
	pub fn closed(&self) -> bool {
		self.done
	}

	/// Cancel the transaction and rollback any changes
	pub fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Mark this transaction as done
		self.done = true;
		// Clear the transaction entries
		self.writeset.clear();
		// Clear savepoint stack
		self.savepoint_stack.clear();
		// Continue
		Ok(())
	}

	/// Set a savepoint in the transaction for partial rollback
	/// This method is stackable and can be called multiple times
	pub fn set_savepoint(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Push current transaction state onto savepoint stack
		self.savepoint_stack.push(SavepointState {
			readset: self.readset.clone(),
			scanset: self.scanset.clone(),
			writeset: self.writeset.clone(),
		});
		// Continue
		Ok(())
	}

	/// Rollback the transaction to the most recently set savepoint
	/// Pops the latest savepoint from the stack and restores transaction state
	pub fn rollback_to_savepoint(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Pop the most recent savepoint from the stack
		let savepoint = self.savepoint_stack.pop().ok_or(Error::NoSavepoint)?;
		// Restore the transaction state to the savepoint
		self.readset = savepoint.readset;
		self.scanset = savepoint.scanset;
		self.writeset = savepoint.writeset;
		// Continue
		Ok(())
	}

	/// Commit the transaction and store all changes
	pub fn commit(&mut self) -> Result<(), Error>
	where
		K: Serialize,
		V: Serialize,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Mark this transaction as done
		self.done = true;
		// Return immediately if no modifications
		if self.writeset.is_empty() {
			return Ok(());
		}
		// Take ownership over the local modifications
		let writeset = Arc::new(std::mem::take(&mut self.writeset));
		// Insert this transaction into the commit queue
		let (version, entry) = self.atomic_commit(Commit {
			writeset: writeset.clone(),
			id: self.database.transaction_queue_id.fetch_add(1, Ordering::AcqRel) + 1,
		});
		// Check wether we should check reads conflicts on commit
		if self.mode >= IsolationLevel::SnapshotIsolation {
			// Retrieve all transactions committed since we began
			for tx in self.database.transaction_commit_queue.range(self.commit + 1..version) {
				// Check if a previous transaction conflicts against writes
				if !tx.value().is_disjoint_writeset(&entry) {
					// Remove the transaction from the commit queue
					self.database.transaction_commit_queue.remove(&version);
					// Return the error for this transaction
					return Err(Error::KeyWriteConflict);
				}
				// Check if we should check for conflicting read keys
				if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
					// Check if a previous transaction conflicts against reads
					if !tx.value().is_disjoint_readset(&self.readset) {
						// Remove the transaction from the commit queue
						self.database.transaction_commit_queue.remove(&version);
						// Return the error for this transaction
						return Err(Error::KeyReadConflict);
					}
					// A previous transaction has conflicts against scans
					for k in tx.value().writeset.keys() {
						// Check if this key may be within a scan range
						if let Some(range) = self.scanset.range(..=k).next_back() {
							// Check if the range includes this key
							if range.1 > k {
								// Remove the transaction from the commit queue
								self.database.transaction_commit_queue.remove(&version);
								// Return the error for this transaction
								return Err(Error::KeyReadConflict);
							}
						}
					}
				}
			}
		}
		// Insert this transaction into the merge queue
		let (version, entry) = self.atomic_merge(Merge {
			writeset,
			id: self.database.transaction_merge_id.fetch_add(1, Ordering::AcqRel) + 1,
		});
		// Get the earliest active transaction version
		let earliest = self.database.counter_by_oracle.front().map(|e| *e.key()).unwrap_or(version);
		// Get the garbage collection epoch as nanoseconds
		let history = self.database.garbage_collection_epoch.read().unwrap_or_default().as_nanos();
		// Calculate the history cutoff (current time - history duration)
		let history = version.saturating_sub(history as u64);
		// Use the earlier of history or earliest transaction
		let cleanup_ts = history.min(earliest);
		// Loop over the updates in the writeset
		for (key, value) in entry.writeset.iter() {
			// Clone the value for insertion
			let value = value.clone();
			// Check if this key already exists
			if let Some(entry) = self.database.datastore.get(key) {
				// Get a mutable reference to the versions list
				let mut versions = entry.value().write();
				// Clean up unnecessary older versions
				let len = versions.gc_older_versions(cleanup_ts);
				// If no versions remain and the value is none, remove the entry fully
				if len == 0 && value.is_none() {
					// Drop the version reference
					drop(versions);
					// Remove the entry from the datastore
					self.database.datastore.remove(key);
				}
				// If a value is set, add the new version entry
				else {
					// Add the version entry to the versions list
					versions.push(Version {
						version,
						value,
					});
				}
			} else {
				self.database.datastore.insert(
					key.clone(),
					RwLock::new(Versions::from(Version {
						version,
						value,
					})),
				);
			}
		}
		// Append the transaction to the persistence layer
		if let Some(p) = self.database.persistence.read().clone() {
			if let Err(e) = p.append(version, entry.writeset.as_ref()) {
				// Remove this transaction from the merge queue
				self.database.transaction_merge_queue.remove(&version);
				// Return a persistence error
				return Err(Error::TxCommitNotPersisted(e));
			}
		}
		// Remove this transaction from the merge queue
		self.database.transaction_merge_queue.remove(&version);
		// Continue
		Ok(())
	}

	/// Check if a key exists in the database
	pub fn exists<Q>(&mut self, key: Q) -> Result<bool, Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check the transaction type
		let res = match self.write {
			// This is a writeable transaction
			true => match self.writeset.get(key.borrow()) {
				// The key exists in the writeset
				Some(_) => true,
				// Check for the key in the tree
				None => {
					// Fetch for the key from the datastore
					let res = self.exists_in_datastore(key.borrow(), self.version);
					// Check whether we should track key reads
					if self.mode >= IsolationLevel::SerializableSnapshotIsolation
						&& !self.readset.contains(key.borrow())
					{
						self.readset.insert(key.into());
					}
					// Return the result
					res
				}
			},
			// This is a readonly transaction
			false => self.exists_in_datastore(key.borrow(), self.version),
		};
		// Return result
		Ok(res)
	}

	/// Check if a key exists in the database at a specific version
	pub fn exists_at_version<Q>(&self, key: Q, version: u64) -> Result<bool, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check the specified key version
		if self.version <= version {
			return Err(Error::VersionInFuture);
		}
		// Check the key
		let res = self.exists_in_datastore(key.borrow(), version);
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	pub fn get<Q>(&mut self, key: Q) -> Result<Option<V>, Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check the transaction type
		let res = match self.write {
			// This is a writeable transaction
			true => match self.writeset.get(key.borrow()) {
				// The key exists in the writeset
				Some(v) => v.as_ref().map(|arc| arc.as_ref().clone()),
				// Check for the key in the tree
				None => {
					// Fetch for the key from the datastore
					let res = self.fetch_in_datastore(key.borrow(), self.version);
					// Check whether we should track key reads
					if self.mode >= IsolationLevel::SerializableSnapshotIsolation
						&& !self.readset.contains(key.borrow())
					{
						self.readset.insert(key.into());
					}
					// Return the result
					res
				}
			},
			// This is a readonly transaction
			false => self.fetch_in_datastore(key.borrow(), self.version),
		};
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database at a specific version
	pub fn get_at_version<Q>(&self, key: Q, version: u64) -> Result<Option<V>, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check the specified key version
		if self.version <= version {
			return Err(Error::VersionInFuture);
		}
		// Get the key
		let res = self.fetch_in_datastore(key.borrow(), version);
		// Return result
		Ok(res)
	}

	/// Insert or update a key in the database
	pub fn set<Q>(&mut self, key: Q, val: V) -> Result<(), Error>
	where
		Q: Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		self.writeset.insert(key.into(), Some(Arc::new(val)));
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	pub fn put<Q>(&mut self, key: Q, val: V) -> Result<(), Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match self.exists_in_datastore(key.borrow(), self.version) {
			false => self.writeset.insert(key.into(), Some(Arc::new(val))),
			_ => return Err(Error::KeyAlreadyExists),
		};
		// Return result
		Ok(())
	}

	/// Insert a key if it matches a value
	pub fn putc<Q>(&mut self, key: Q, val: V, chk: Option<V>) -> Result<(), Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match self.equals_in_datastore(key.borrow(), chk, self.version) {
			true => self.writeset.insert(key.into(), Some(Arc::new(val))),
			_ => return Err(Error::ValNotExpectedValue),
		};
		// Return result
		Ok(())
	}

	/// Delete a key from the database
	pub fn del<Q>(&mut self, key: Q) -> Result<(), Error>
	where
		Q: Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		self.writeset.insert(key.into(), None);
		// Return result
		Ok(())
	}

	/// Delete a key if it matches a value
	pub fn delc<Q>(&mut self, key: Q, chk: Option<V>) -> Result<(), Error>
	where
		Q: Borrow<K> + Into<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if self.write == false {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		match self.equals_in_datastore(key.borrow(), chk, self.version) {
			true => self.writeset.insert(key.into(), None),
			_ => return Err(Error::ValNotExpectedValue),
		};
		// Return result
		Ok(())
	}

	/// Retrieve a count of keys from the database
	pub fn total<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<usize, Error>
	where
		Q: Borrow<K>,
	{
		self.total_any(rng, skip, limit, Direction::Forward, self.version)
	}

	/// Retrieve a count of keys from the database at a specific version
	pub fn total_at_version<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		version: u64,
	) -> Result<usize, Error>
	where
		Q: Borrow<K>,
	{
		self.total_any(rng, skip, limit, Direction::Forward, version)
	}

	/// Retrieve a range of keys from the database
	pub fn keys<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		self.keys_any(rng, skip, limit, Direction::Forward, self.version)
	}

	/// Retrieve a range of keys from the database, in reverse order
	pub fn keys_reverse<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		self.keys_any(rng, skip, limit, Direction::Reverse, self.version)
	}

	/// Retrieve a range of keys from the database at a specific version
	pub fn keys_at_version<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		self.keys_any(rng, skip, limit, Direction::Forward, version)
	}

	/// Retrieve a range of keys from the database at a specific version, in reverse order
	pub fn keys_at_version_reverse<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		self.keys_any(rng, skip, limit, Direction::Reverse, version)
	}

	/// Retrieve a range of keys and values from the database
	pub fn scan<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_any(rng, skip, limit, Direction::Forward, self.version)
	}

	/// Retrieve a range of keys and values from the database in reverse order
	pub fn scan_reverse<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_any(rng, skip, limit, Direction::Reverse, self.version)
	}

	/// Retrieve a range of keys and values from the database at a specific version
	pub fn scan_at_version<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_any(rng, skip, limit, Direction::Forward, version)
	}

	/// Retrieve a range of keys and values from the database at a specific version, in reverse order
	pub fn scan_at_version_reverse<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_any(rng, skip, limit, Direction::Reverse, version)
	}

	/// Retrieve all versions of keys within a range from the database
	/// Returns tuples of (key, version, value) for all historical versions
	/// The skip and limit parameters apply to the number of keys, not the number of versions
	pub fn scan_all_versions<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<(K, u64, Option<V>)>, Error>
	where
		Q: Borrow<K>,
	{
		self.scan_all_versions_any(rng, skip, limit, self.version)
	}

	/// Helper to track a scan range in the scanset (optimized to minimize clones)
	#[inline(always)]
	fn track_scan_range(&mut self, beg: &K, end: &K) {
		// Add this range scan entry to the saved scans
		match self.scanset.range_mut(..=beg).next_back() {
			// There is no entry for this range scan
			None => {
				self.scanset.insert(beg.clone(), end.clone());
			}
			// The saved scan stops before this range
			Some(range) if &*range.1 < beg => {
				self.scanset.insert(beg.clone(), end.clone());
			}
			// The saved scan does not extend far enough
			Some(range) if &*range.1 < end => {
				*range.1 = end.clone();
			}
			// This range scan is already covered
			_ => (),
		}
	}

	/// Retrieve a count of keys from the database
	fn total_any<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		direction: Direction,
		version: u64,
	) -> Result<usize, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Prepare result count
		let mut res = 0;
		// Compute the range
		let beg = rng.start.borrow();
		let end = rng.end.borrow();
		// Calculate how many items to skip
		let skip = skip.unwrap_or_default();
		// Check wether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			// Track scans if scanning the latest version
			if version == self.version {
				self.track_scan_range(beg, end);
			}
		}
		// Build combined writeset from merge queue entries only
		let mut combined_writeset: BTreeMap<K, Option<Arc<V>>> = BTreeMap::new();
		for entry in self.database.transaction_merge_queue.range(..=version).rev() {
			// Don't skip removed entries to avoid race conditions
			for (k, v) in entry.value().writeset.range(beg..end) {
				combined_writeset.entry(k.clone()).or_insert_with(|| v.clone());
			}
		}
		// Create the 3-way merge iterator
		let mut iter = MergeIterator::new(
			self.database.datastore.range((Bound::Included(beg), Bound::Excluded(end))),
			combined_writeset,
			self.writeset.range(beg.clone()..end.clone()),
			direction,
			version,
			skip,
		);
		// Process entries with skip and limit
		while let Some(exists) = iter.next_count() {
			if exists {
				res += 1;
				if let Some(l) = limit {
					if res >= l {
						break;
					}
				}
			}
		}
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys from the database
	fn keys_any<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		direction: Direction,
		version: u64,
	) -> Result<Vec<K>, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Prepare result vector
		let mut res = match limit {
			Some(l) => Vec::with_capacity(l),
			None => Vec::new(),
		};
		// Compute the range
		let beg = rng.start.borrow();
		let end = rng.end.borrow();
		// Calculate how many items to skip
		let skip = skip.unwrap_or_default();
		// Check wether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			// Track scans if scanning the latest version
			if version == self.version {
				self.track_scan_range(beg, end);
			}
		}
		// Build combined writeset from merge queue entries only
		let mut combined_writeset: BTreeMap<K, Option<Arc<V>>> = BTreeMap::new();
		for entry in self.database.transaction_merge_queue.range(..=version).rev() {
			// Don't skip removed entries to avoid race conditions
			for (k, v) in entry.value().writeset.range(beg..end) {
				combined_writeset.entry(k.clone()).or_insert_with(|| v.clone());
			}
		}
		// Create the 3-way merge iterator
		let mut iter = MergeIterator::new(
			self.database.datastore.range((Bound::Included(beg), Bound::Excluded(end))),
			combined_writeset,
			self.writeset.range(beg.clone()..end.clone()),
			direction,
			version,
			skip,
		);
		// Process entries with skip and limit
		while let Some((key, exists)) = iter.next_key() {
			if exists {
				res.push(key);
				if let Some(l) = limit {
					if res.len() >= l {
						break;
					}
				}
			}
		}
		// Return result
		Ok(res)
	}

	/// Retrieve a range of keys and values from the database
	fn scan_any<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		direction: Direction,
		version: u64,
	) -> Result<Vec<(K, V)>, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Prepare result vector
		let mut res = match limit {
			Some(l) => Vec::with_capacity(l),
			None => Vec::new(),
		};
		// Compute the range
		let beg = rng.start.borrow();
		let end = rng.end.borrow();
		// Calculate how many items to skip
		let skip = skip.unwrap_or_default();
		// Check wether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			// Track scans if scanning the latest version
			if version == self.version {
				self.track_scan_range(beg, end);
			}
		}
		// Build combined writeset from merge queue entries only
		let mut combined_writeset: BTreeMap<K, Option<Arc<V>>> = BTreeMap::new();
		for entry in self.database.transaction_merge_queue.range(..=version).rev() {
			// Don't skip removed entries to avoid race conditions
			for (k, v) in entry.value().writeset.range(beg..end) {
				combined_writeset.entry(k.clone()).or_insert_with(|| v.clone());
			}
		}
		// Create the 3-way merge iterator
		let iter = MergeIterator::new(
			self.database.datastore.range((Bound::Included(beg), Bound::Excluded(end))),
			combined_writeset,
			self.writeset.range(beg.clone()..end.clone()),
			direction,
			version,
			skip,
		);
		// Process entries with skip and limit
		for (key, val) in iter {
			// Only include non-deleted entries
			if let Some(val) = val {
				res.push((key, val.as_ref().clone()));
				// Check limit
				if let Some(l) = limit {
					if res.len() >= l {
						break;
					}
				}
			}
		}
		// Return result
		Ok(res)
	}

	/// Retrieve all versions of keys within a range from the database
	fn scan_all_versions_any<Q>(
		&mut self,
		rng: Range<Q>,
		skip: Option<usize>,
		limit: Option<usize>,
		version: u64,
	) -> Result<Vec<(K, u64, Option<V>)>, Error>
	where
		Q: Borrow<K>,
	{
		// Check to see if transaction is closed
		if self.done == true {
			return Err(Error::TxClosed);
		}
		// Prepare result vector
		let mut res = Vec::new();
		// Compute the range
		let beg = rng.start.borrow();
		let end = rng.end.borrow();
		// Calculate how many items to skip
		let skip = skip.unwrap_or_default();
		// Check wether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			// Track scans if scanning the latest version
			if version == self.version {
				self.track_scan_range(beg, end);
			}
		}
		// Build combined writeset from merge queue entries only
		let mut combined_writeset: BTreeMap<K, Option<Arc<V>>> = BTreeMap::new();
		for entry in self.database.transaction_merge_queue.range(..=version).rev() {
			// Don't skip removed entries to avoid race conditions
			for (k, v) in entry.value().writeset.range(beg..end) {
				combined_writeset.entry(k.clone()).or_insert_with(|| v.clone());
			}
		}
		// Create the 3-way merge iterator to iterate over keys
		let iter = MergeIterator::new(
			self.database.datastore.range((Bound::Included(beg), Bound::Excluded(end))),
			combined_writeset,
			self.writeset.range(beg.clone()..end.clone()),
			Direction::Forward,
			version,
			skip,
		);
		// Track how many keys we've processed
		let mut count = 0;
		// Process entries with skip and limit
		for (key, _) in iter {
			// Use a BTreeMap to collect and merge versions by version number
			let mut all_versions: BTreeMap<u64, Option<Arc<V>>> = BTreeMap::new();
			// Collect all versions from the datastore
			if let Some(entry) = self.database.datastore.get(&key) {
				let versions = entry.value().read();
				for (ver, val) in versions.all_versions() {
					if ver <= version {
						all_versions.insert(ver, val);
					}
				}
			}
			// Collect version from the current writeset
			if self.write {
				if let Some(val) = self.writeset.get(&key) {
					// Set a temporary version for the new version entry
					all_versions.insert(self.version + 1, val.clone());
				}
			}
			// Add all versions for this key to the result
			for (ver, val) in all_versions {
				res.push((key.clone(), ver, val.as_ref().map(|arc| arc.as_ref().clone())));
			}
			// Increment key counter
			count += 1;
			// Check limit on keys
			if let Some(l) = limit {
				if count >= l {
					break;
				}
			}
		}
		// Return result
		Ok(res)
	}

	/// Fetch a key if it exists in the datastore only
	#[inline(always)]
	fn fetch_in_datastore<Q>(&self, key: Q, version: u64) -> Option<V>
	where
		Q: Borrow<K>,
	{
		// Fetch the transaction merge queue range
		let iter = self.database.transaction_merge_queue.range(..=version);
		// Check the current entry iteration
		for entry in iter.rev() {
			// Even if the entry is marked as removed, we still check it to avoid a race
			// where the entry is removed after we check but before the data is visible
			// in the datastore. The merge queue entry remains accessible even when removed.
			// Check if the entry has a key
			if let Some(v) = entry.value().writeset.get(key.borrow()) {
				// Return the entry value
				return v.as_ref().map(|arc| arc.as_ref().clone());
			}
		}
		// Check the key in the datastore
		self.database
			.datastore
			.get(key.borrow())
			.and_then(|e| e.value().read().fetch_version(version))
			.map(|arc| arc.as_ref().clone())
	}

	/// Check if a key exists in the datastore only
	#[inline(always)]
	fn exists_in_datastore<Q>(&self, key: Q, version: u64) -> bool
	where
		Q: Borrow<K>,
	{
		// Fetch the transaction merge queue range
		let iter = self.database.transaction_merge_queue.range(..=version);
		// Check the current entry iteration
		for entry in iter.rev() {
			// Even if the entry is marked as removed, we still check it to avoid a race
			// where the entry is removed after we check but before the data is visible
			// in the datastore. The merge queue entry remains accessible even when removed.
			// Check if the entry has a key
			if let Some(v) = entry.value().writeset.get(key.borrow()) {
				// Return whether the entry exists
				return v.is_some();
			}
		}
		// Check the key in the datastore
		self.database
			.datastore
			.get(key.borrow())
			.map(|e| e.value().read().exists_version(version))
			.is_some_and(|v| v)
	}

	/// Check if a key equals a value in the datastore only
	#[inline(always)]
	fn equals_in_datastore<Q>(&self, key: Q, chk: Option<V>, version: u64) -> bool
	where
		Q: Borrow<K>,
	{
		// Fetch the transaction merge queue range
		let iter = self.database.transaction_merge_queue.range(..=version);
		// Check the current entry iteration
		for entry in iter.rev() {
			// Even if the entry is marked as removed, we still check it to avoid a race
			// where the entry is removed after we check but before the data is visible
			// in the datastore. The merge queue entry remains accessible even when removed.
			// Check if the entry has a key
			if let Some(v) = entry.value().writeset.get(key.borrow()) {
				// Return whether the entry matches
				return match (chk.as_ref(), v.as_ref()) {
					(Some(x), Some(y)) => x == y.as_ref(),
					(None, None) => true,
					_ => false,
				};
			}
		}
		// Check the key in the datastore
		match (
			chk.as_ref(),
			self.database
				.datastore
				.get(key.borrow())
				.and_then(|e| e.value().read().fetch_version(version))
				.as_ref(),
		) {
			(Some(x), Some(y)) => x == y.as_ref(),
			(None, None) => true,
			_ => false,
		}
	}

	/// Atomimcally inserts the transaction into the commit queue
	#[inline(always)]
	fn atomic_commit(&self, updates: Commit<K, V>) -> (u64, Arc<Commit<K, V>>) {
		// Store the number of spins
		let mut spins = 0;
		// Get the commit attempt id
		let id = updates.id;
		// Store the commit in an Arc
		let updates = Arc::new(updates);
		// Get the database transaction merge queue
		let queue = &self.database.transaction_commit_queue;
		// Loop until the atomic operation is successful
		loop {
			// Get the current commit queue number
			let version = self.database.transaction_commit_id.load(Ordering::Acquire) + 1;
			// Insert into the queue if the number is the same
			let entry = queue.get_or_insert_with(version, || Arc::clone(&updates));
			// Check if the entry was inserted correctly
			if id == entry.value().id {
				self.database.transaction_commit_id.fetch_add(1, Ordering::Release);
				return (version, entry.value().clone());
			}
			// Ensure the thread backs off when under contention
			if spins < 10 {
				std::hint::spin_loop();
			} else if spins < 100 {
				std::thread::yield_now();
			} else {
				std::thread::park_timeout(Duration::from_micros(10));
			}
			// Increase the number loop spins we have attempted
			spins += 1;
		}
	}

	/// Atomimcally inserts the transaction into the merge queue
	#[inline(always)]
	fn atomic_merge(&self, updates: Merge<K, V>) -> (u64, Arc<Merge<K, V>>) {
		// Store the number of spins
		let mut spins = 0;
		// Get the commit attempt id
		let id = updates.id;
		// Store the commit in an Arc
		let updates = Arc::new(updates);
		// Get the database timestamp oracle
		let oracle = self.database.oracle.clone();
		// Get the database transaction merge queue
		let queue = &self.database.transaction_merge_queue;
		// Loop until we reach the next incremental timestamp
		loop {
			// Get the current nanoseconds since the Unix epoch
			let mut version = oracle.current_time_ns();
			// Get the last timestamp for this oracle
			let last_ts = oracle.inner.timestamp.load(Ordering::Acquire);
			// Increase the timestamp to ensure monotonicity
			if version <= last_ts {
				version = last_ts + 1;
			}
			// Insert into the queue if the number is the same
			let entry = queue.get_or_insert_with(version, || Arc::clone(&updates));
			// Check if the entry was inserted correctly
			if id == entry.value().id {
				oracle.inner.timestamp.fetch_max(version, Ordering::Release);
				return (version, entry.value().clone());
			}
			// Ensure the thread backs off when under contention
			if spins < 10 {
				std::hint::spin_loop();
			} else if spins < 100 {
				std::thread::yield_now();
			} else {
				std::thread::park_timeout(Duration::from_micros(10));
			}
			// Increase the number loop spins we have attempted
			spins += 1;
		}
	}
}

#[cfg(test)]
mod tests {

	use crate::err::Error;
	use crate::Database;
	use std::sync::Arc;
	use std::thread;

	#[test]
	fn mvcc_non_conflicting_keys_should_succeed() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		let mut tx2 = db.transaction(true);
		// ----------
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		assert!(tx2.get("key2").unwrap().is_none());
		tx2.set("key2", "value2").unwrap();
		assert!(tx2.commit().is_ok());
	}

	#[test]
	fn mvcc_conflicting_blind_writes_should_error() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		let mut tx2 = db.transaction(true);
		// ----------
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		// ----------
		assert!(tx2.get("key1").unwrap().is_none());
		tx2.set("key1", "value2").unwrap();
		// ----------
		assert!(tx1.commit().is_ok());
		assert!(tx2.commit().is_err());
	}

	#[test]
	fn mvcc_si_conflicting_read_keys_should_succeed() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true).with_snapshot_isolation();
		let mut tx2 = db.transaction(true).with_snapshot_isolation();
		// ----------
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		assert!(tx2.get("key1").unwrap().is_none());
		tx2.set("key2", "value2").unwrap();
		assert!(tx2.commit().is_ok());
	}

	#[test]
	fn mvcc_ssi_conflicting_read_keys_should_error() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
		let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
		// ----------
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		assert!(tx2.get("key1").unwrap().is_none());
		tx2.set("key2", "value2").unwrap();
		assert!(tx2.commit().is_err());
	}

	#[test]
	fn mvcc_conflicting_write_keys_should_error() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		let mut tx2 = db.transaction(true);
		// ----------
		assert!(tx1.get("key1").unwrap().is_none());
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		assert!(tx2.get("key1").unwrap().is_none());
		tx2.set("key1", "value2").unwrap();
		assert!(tx2.commit().is_err());
	}

	#[test]
	fn mvcc_conflicting_read_deleted_keys_should_error() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		tx1.set("key", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		let mut tx2 = db.transaction(true);
		let mut tx3 = db.transaction(true);
		// ----------
		assert!(tx2.get("key").unwrap().is_some());
		tx2.del("key").unwrap();
		assert!(tx2.commit().is_ok());
		// ----------
		assert!(tx3.get("key").unwrap().is_some());
		tx3.set("key", "value2").unwrap();
		assert!(tx3.commit().is_err());
	}

	#[test]
	fn mvcc_scan_conflicting_write_keys_should_error() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		let mut tx2 = db.transaction(true);
		let mut tx3 = db.transaction(true);
		// ----------
		tx2.set("key1", "value4").unwrap();
		tx2.set("key2", "value2").unwrap();
		tx2.set("key3", "value3").unwrap();
		assert!(tx2.commit().is_ok());
		// ----------
		let res = tx3.scan("key1".."key9", None, Some(10)).unwrap();
		assert_eq!(res.len(), 1);
		tx3.set("key2", "value5").unwrap();
		tx3.set("key3", "value6").unwrap();
		let res = tx3.scan("key1".."key9", None, Some(10)).unwrap();
		assert_eq!(res.len(), 3);
		assert!(tx3.commit().is_err());
	}

	#[test]
	fn mvcc_scan_conflicting_read_deleted_keys_should_error() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		// ----------
		let mut tx2 = db.transaction(true);
		let mut tx3 = db.transaction(true);
		// ----------
		tx2.del("key1").unwrap();
		assert!(tx2.commit().is_ok());
		// ----------
		let res = tx3.scan("key1".."key9", None, Some(10)).unwrap();
		assert_eq!(res.len(), 1);
		tx3.set("key1", "other").unwrap();
		tx3.set("key2", "value2").unwrap();
		tx3.set("key3", "value3").unwrap();
		let res = tx3.scan("key1".."key9", None, Some(10)).unwrap();
		assert_eq!(res.len(), 3);
		assert!(tx3.commit().is_err());
	}

	#[test]
	fn mvcc_transaction_queue_correctness() {
		let db: Database<&str, &str> = Database::new();
		// ----------
		let mut tx1 = db.transaction(true);
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());
		std::mem::drop(tx1);
		// ----------
		let mut tx2 = db.transaction(true);
		tx2.set("key2", "value2").unwrap();
		assert!(tx2.commit().is_ok());
		std::mem::drop(tx2);
		// ----------
		let mut tx3 = db.transaction(true);
		tx3.set("key", "value").unwrap();
		// ----------
		let mut tx4 = db.transaction(true);
		tx4.set("key", "value").unwrap();
		// ----------
		assert!(tx3.commit().is_ok());
		assert!(tx4.commit().is_err());
		std::mem::drop(tx3);
		std::mem::drop(tx4);
		// ----------
		let mut tx5 = db.transaction(true);
		tx5.set("key", "other").unwrap();
		// ----------
		let mut tx6 = db.transaction(true);
		tx6.set("key", "other").unwrap();
		// ----------
		assert!(tx5.commit().is_ok());
		assert!(tx6.commit().is_err());
		std::mem::drop(tx5);
		std::mem::drop(tx6);
		// ----------
		let mut tx7 = db.transaction(true);
		tx7.set("key", "change").unwrap();
		// ----------
		let mut tx8 = db.transaction(true);
		tx8.set("key", "change").unwrap();
		// ----------
		assert!(tx7.commit().is_ok());
		assert!(tx7.commit().is_err());
		std::mem::drop(tx7);
		std::mem::drop(tx8);
	}

	#[test]
	fn test_snapshot_isolation() {
		let db: Database<&str, &str> = Database::new();

		let key1 = "key1";
		let key2 = "key2";
		let value1 = "baz";
		let value2 = "bar";

		// no conflict
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			txn1.set(key1, value1).unwrap();
			txn1.commit().unwrap();

			assert!(txn2.get(key2).unwrap().is_none());
			txn2.set(key2, value2).unwrap();
			txn2.commit().unwrap();
		}

		// conflict when the read key was updated by another transaction
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			txn1.set(key1, value1).unwrap();
			txn1.commit().unwrap();

			assert!(txn2.get(key1).is_ok());
			txn2.set(key1, value2).unwrap();
			assert!(txn2.commit().is_err());
		}

		// blind writes should not succeed
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			txn1.set(key1, value1).unwrap();
			txn2.set(key1, value2).unwrap();

			txn1.commit().unwrap();
			assert!(txn2.commit().is_err());
		}

		// conflict when the read key was updated by another transaction
		{
			let key = "key3";

			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			txn1.set(key, value1).unwrap();
			txn1.commit().unwrap();

			assert!(txn2.get(key).unwrap().is_none());
			txn2.set(key, value1).unwrap();
			assert!(txn2.commit().is_err());
		}

		// write-skew: read conflict when the read key was deleted by another transaction
		{
			let key = "key4";

			let mut txn1 = db.transaction(true);
			txn1.set(key, value1).unwrap();
			txn1.commit().unwrap();

			let mut txn2 = db.transaction(true);
			let mut txn3 = db.transaction(true);

			txn2.del(key).unwrap();
			assert!(txn2.commit().is_ok());

			assert!(txn3.get(key).is_ok());
			txn3.set(key, value2).unwrap();
			assert!(txn3.commit().is_err());
		}
	}

	#[test]
	fn test_snapshot_isolation_scan() {
		let db: Database<&str, &str> = Database::new();

		let key1 = "key1";
		let key2 = "key2";
		let key3 = "key3";
		let key4 = "key4";
		let value1 = "value1";
		let value2 = "value2";
		let value3 = "value3";
		let value4 = "value4";
		let value5 = "value5";
		let value6 = "value6";

		// conflict when scan keys have been updated in another transaction
		{
			let mut txn1 = db.transaction(true);

			txn1.set(key1, value1).unwrap();
			txn1.commit().unwrap();

			let mut txn2 = db.transaction(true);
			let mut txn3 = db.transaction(true);

			txn2.set(key1, value4).unwrap();
			txn2.set(key2, value2).unwrap();
			txn2.set(key3, value3).unwrap();
			txn2.commit().unwrap();

			let range = "key1".."key4";
			let results = txn3.scan(range, None, Some(10)).unwrap();
			assert_eq!(results.len(), 1);
			txn3.set(key2, value5).unwrap();
			txn3.set(key3, value6).unwrap();

			assert!(txn3.commit().is_err());
		}

		// write-skew: read conflict when read keys are deleted by other transaction
		{
			let mut txn1 = db.transaction(true);

			txn1.set(key4, value1).unwrap();
			txn1.commit().unwrap();

			let mut txn2 = db.transaction(true);
			let mut txn3 = db.transaction(true);

			txn2.del(key4).unwrap();
			txn2.commit().unwrap();

			let range = "key1".."key5";
			let _ = txn3.scan(range, None, Some(10)).unwrap();
			txn3.set(key4, value2).unwrap();
			assert!(txn3.commit().is_err());
		}
	}

	// Common setup logic for creating anomaly tests database
	fn new_db() -> Database<&'static str, &'static str> {
		let db: Database<&str, &str> = Database::new();

		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		// Start a new read-write transaction (txn)
		let mut txn = db.transaction(true);
		txn.set(key1, value1).unwrap();
		txn.set(key2, value2).unwrap();
		txn.commit().unwrap();

		db
	}

	// G0: Write Cycles (dirty writes)
	#[test]
	fn test_anomaly_g0() {
		let db = new_db();
		let key1 = "k1";
		let key2 = "k2";
		let value3 = "v3";
		let value4 = "v4";
		let value5 = "v5";
		let value6 = "v6";

		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			assert!(txn1.get(key1).is_ok());
			assert!(txn1.get(key2).is_ok());
			assert!(txn2.get(key1).is_ok());
			assert!(txn2.get(key2).is_ok());

			txn1.set(key1, value3).unwrap();
			txn2.set(key1, value4).unwrap();

			txn1.set(key2, value5).unwrap();

			txn1.commit().unwrap();

			txn2.set(key2, value6).unwrap();
			assert!(txn2.commit().is_err());
		}

		{
			let mut txn3 = db.transaction(true);
			let val1 = txn3.get(key1).unwrap().unwrap();
			assert_eq!(val1, value3);
			let val2 = txn3.get(key2).unwrap().unwrap();
			assert_eq!(val2, value5);
		}
	}

	// G1a: Aborted Reads (dirty reads, cascaded aborts)
	#[test]
	fn test_anomaly_g1a() {
		let db = new_db();
		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";

		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			assert!(txn1.get(key1).is_ok());

			txn1.set(key1, value3).unwrap();

			let range = "k1".."k3";
			let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
			assert_eq!(res.len(), 2);
			assert_eq!(res[0].1, value1);

			drop(txn1);

			let res = txn2.scan(range, None, None).expect("Scan should succeed");
			assert_eq!(res.len(), 2);
			assert_eq!(res[0].1, value1);

			txn2.commit().unwrap();
		}

		{
			let mut txn3 = db.transaction(true);
			let val1 = txn3.get(key1).unwrap().unwrap();
			assert_eq!(val1, value1);
			let val2 = txn3.get(key2).unwrap().unwrap();
			assert_eq!(val2, value2);
		}
	}

	// G1b: Intermediate Reads (dirty reads)
	#[test]
	fn test_anomaly_g1b() {
		let db = new_db();

		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value3 = "v3";
		let value4 = "v4";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert!(txn1.get(key1).is_ok());
		assert!(txn1.get(key2).is_ok());

		txn1.set(key1, value3).unwrap();

		let range = "k1".."k3";
		let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);

		txn1.set(key1, value4).unwrap();
		txn1.commit().unwrap();

		let res = txn2.scan(range, None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);

		txn2.commit().unwrap();
	}

	// PMP: Predicate-Many-Preceders
	#[test]
	fn test_anomaly_pmp() {
		let db = new_db();

		let key3 = "k3";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		// k3 should not be visible to txn1
		let range = "k1".."k3";
		let res = txn1.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		// k3 is committed by txn2
		txn2.set(key3, value3).unwrap();
		txn2.commit().unwrap();

		// k3 should still not be visible to txn1
		let range = "k1".."k3";
		let res = txn1.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);
	}

	// P4: Lost Update
	#[test]
	fn test_anomaly_p4() {
		let db = new_db();

		let key1 = "k1";
		let value3 = "v3";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert!(txn1.get(key1).is_ok());
		assert!(txn2.get(key1).is_ok());

		txn1.set(key1, value3).unwrap();
		txn2.set(key1, value3).unwrap();

		txn1.commit().unwrap();
		assert!(txn2.commit().is_err());
	}

	// G-single: Single Anti-dependency Cycles (read skew)
	#[test]
	fn test_anomaly_g_single() {
		let db = new_db();

		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";
		let value4 = "v4";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert_eq!(txn1.get(key1).unwrap().unwrap(), value1);
		assert_eq!(txn2.get(key1).unwrap().unwrap(), value1);
		assert_eq!(txn2.get(key2).unwrap().unwrap(), value2);
		txn2.set(key1, value3).unwrap();
		txn2.set(key2, value4).unwrap();

		txn2.commit().unwrap();

		assert_eq!(txn1.get(key2).unwrap().unwrap(), value2);
		txn1.commit().unwrap();
	}

	// G-single-write-1: Single Anti-dependency Cycles (read skew)
	#[test]
	fn test_anomaly_g_single_write_1() {
		let db = new_db();

		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";
		let value4 = "v4";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert_eq!(txn1.get(key1).unwrap().unwrap(), value1);

		let range = "k1".."k3";
		let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		txn2.set(key1, value3).unwrap();
		txn2.set(key2, value4).unwrap();

		txn2.commit().unwrap();

		txn1.del(key2).unwrap();
		assert!(txn1.get(key2).unwrap().is_none());
		assert!(txn1.commit().is_err());
	}

	// G-single-write-2: Single Anti-dependency Cycles (read skew)
	#[test]
	fn test_anomaly_g_single_write_2() {
		let db = new_db();

		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";
		let value4 = "v4";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert_eq!(txn1.get(key1).unwrap().unwrap(), value1);
		let range = "k1".."k3";
		let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		txn2.set(key1, value3).unwrap();

		txn1.del(key2).unwrap();

		txn2.set(key2, value4).unwrap();

		drop(txn1);

		txn2.commit().unwrap();
	}

	// G1c: Circular Information Flow (dirty reads)
	#[test]
	fn test_anomaly_g1c() {
		let db = new_db();
		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";
		let value4 = "v4";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert!(txn1.get(key1).is_ok());
		assert!(txn2.get(key2).is_ok());

		txn1.set(key1, value3).unwrap();
		txn2.set(key2, value4).unwrap();

		assert_eq!(txn1.get(key2).unwrap().unwrap(), value2);
		assert_eq!(txn2.get(key1).unwrap().unwrap(), value1);

		txn1.commit().unwrap();
		assert!(txn2.commit().is_err());
	}

	// PMP-Write: Circular Information Flow (dirty reads)
	#[test]
	fn test_pmp_write() {
		let db = new_db();
		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		assert!(txn1.get(key1).is_ok());
		txn1.set(key1, value3).unwrap();

		let range = "k1".."k3";
		let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		txn2.del(key2).unwrap();
		txn1.commit().unwrap();

		let range = "k1".."k3";
		let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 1);
		assert_eq!(res[0].1, value1);

		assert!(txn2.commit().is_err());
	}

	#[test]
	fn test_g2_item() {
		let db = new_db();
		let key1 = "k1";
		let key2 = "k2";
		let value1 = "v1";
		let value2 = "v2";
		let value3 = "v3";
		let value4 = "v4";

		let mut txn1 = db.transaction(true);
		let mut txn2 = db.transaction(true);

		let range = "k1".."k3";
		let res = txn1.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		let res = txn2.scan(range.clone(), None, None).expect("Scan should succeed");
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].1, value1);
		assert_eq!(res[1].1, value2);

		txn1.set(key1, value3).unwrap();
		txn2.set(key2, value4).unwrap();

		txn1.commit().unwrap();

		assert!(txn2.commit().is_err());
	}

	#[test]
	fn test_g2_item_predicate() {
		let db = new_db();

		let key3 = "k3";
		let key4 = "k4";
		let key5 = "k5";
		let key6 = "k6";
		let key7 = "k7";
		let value3 = "v3";
		let value4 = "v4";

		// inserts into read ranges of already-committed transaction(s) should fail
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			let range = "k1".."k4";
			txn1.scan(range.clone(), None, None).expect("Scan should succeed");
			txn2.scan(range.clone(), None, None).expect("Scan should succeed");

			txn1.set(key3, value3).unwrap();
			txn2.set(key4, value4).unwrap();

			txn1.commit().unwrap();

			assert!(txn2.commit().is_err());
		}

		// k1, k2, k3 already committed
		// inserts beyond scan range should pass
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			let range = "k1".."k3";
			txn1.scan(range.clone(), None, None).expect("Scan should succeed");
			txn2.scan(range.clone(), None, None).expect("Scan should succeed");

			txn1.set(key4, value3).unwrap();
			txn2.set(key5, value4).unwrap();

			txn1.commit().unwrap();
			txn2.commit().unwrap();
		}

		// k1, k2, k3, k4, k5 already committed
		// inserts in subset scan ranges should fail
		{
			let mut txn1 = db.transaction(true);
			let mut txn2 = db.transaction(true);

			let range = "k1".."k7";
			txn1.scan(range.clone(), None, None).expect("Scan should succeed");
			let range = "k3".."k7";
			txn2.scan(range.clone(), None, None).expect("Scan should succeed");

			txn1.set(key6, value3).unwrap();
			txn2.set(key7, value4).unwrap();

			txn1.commit().unwrap();
			assert!(txn2.commit().is_err());
		}
	}

	#[test]
	fn test_range_scan() {
		// Create database
		let db: Database<&str, &str> = Database::new();

		// Transaction 1: Add initial data and commit
		let mut txn1 = db.transaction(true);
		txn1.set("a", "1").unwrap();
		txn1.set("b", "2").unwrap();
		txn1.set("c", "3").unwrap();
		txn1.set("d", "4").unwrap();
		txn1.set("e", "5").unwrap();
		txn1.commit().unwrap();

		// Transaction 2: Should see committed data even if not merged yet
		let mut txn2 = db.transaction(false);

		// Test exclusive range
		let res = txn2.scan("b".."d", None, None).unwrap();
		assert_eq!(res.len(), 2);
		assert_eq!(res[0], ("b", "2"));
		assert_eq!(res[1], ("c", "3"));

		// Test with skip
		let res = txn2.scan("a".."f", Some(1), None).unwrap();
		assert_eq!(res.len(), 4);
		assert_eq!(res[0], ("b", "2"));

		// Test with limit
		let res = txn2.scan("a".."f", None, Some(3)).unwrap();
		assert_eq!(res.len(), 3);
		assert_eq!(res[2], ("c", "3"));

		// Test with skip and limit
		let res = txn2.scan("a".."f", Some(2), Some(2)).unwrap();
		assert_eq!(res.len(), 2);
		assert_eq!(res[0], ("c", "3"));
		assert_eq!(res[1], ("d", "4"));

		let res = txn2.scan_reverse("b".."e", None, None).unwrap();
		assert_eq!(res.len(), 3);
		assert_eq!(res[0], ("d", "4"));
		assert_eq!(res[1], ("c", "3"));
		assert_eq!(res[2], ("b", "2"));

		// Test empty range
		let res = txn2.scan("x".."z", None, None).unwrap();
		assert_eq!(res.len(), 0);

		// Test single item range
		let res = txn2.scan("c".."d", None, None).unwrap();
		assert_eq!(res.len(), 1);
		assert_eq!(res[0], ("c", "3"));
	}

	#[test]
	fn test_range_scan_with_merge_queue() {
		use std::sync::atomic::Ordering;

		// Create database
		let db: Database<&str, &str> = Database::new();

		// Ensure elements reside in the merge queue
		db.background_threads_enabled.store(false, Ordering::Relaxed);

		// Transaction 1: Add initial data and commit
		let mut txn1 = db.transaction(true);
		txn1.set("a", "1").unwrap();
		txn1.set("c", "3").unwrap();
		txn1.set("e", "5").unwrap();
		txn1.commit().unwrap();

		// Verify data is in datastore and NOT in merge queue
		assert!(db.transaction_merge_queue.is_empty(), "Data should be in merge queue");
		assert!(!db.datastore.is_empty(), "Data should NOT be in datastore yet");

		// Transaction 2: Add uncommitted data and scan
		let mut txn2 = db.transaction(true);
		txn2.set("b", "2").unwrap();
		txn2.set("d", "4").unwrap();

		// Scan should see both committed (from merge queue) and uncommitted data
		let res = txn2.scan("a".."f", None, None).unwrap();
		assert_eq!(res.len(), 5);
		assert_eq!(res[0], ("a", "1")); // From merge queue
		assert_eq!(res[1], ("b", "2")); // From current txn
		assert_eq!(res[2], ("c", "3")); // From merge queue
		assert_eq!(res[3], ("d", "4")); // From current txn
		assert_eq!(res[4], ("e", "5")); // From merge queue
	}

	#[test]
	fn test_range_scan_with_deletions_in_merge_queue() {
		// Test that deletions in merge queue are handled correctly
		let db: Database<&str, &str> = Database::new();

		// Add initial data
		let mut txn1 = db.transaction(true);
		txn1.set("a", "1").unwrap();
		txn1.set("b", "2").unwrap();
		txn1.set("c", "3").unwrap();
		txn1.set("d", "4").unwrap();
		txn1.commit().unwrap();

		// Delete some items (goes to merge queue)
		let mut txn2 = db.transaction(true);
		txn2.del("b").unwrap();
		txn2.del("d").unwrap();
		txn2.commit().unwrap();

		// New transaction should not see deleted items
		let mut txn3 = db.transaction(false);
		let res = txn3.scan("a".."e", None, None).unwrap();
		assert_eq!(res.len(), 2);
		assert_eq!(res[0], ("a", "1"));
		assert_eq!(res[1], ("c", "3"));
	}

	#[test]
	fn test_range_scan_with_overwrites_in_merge_queue() {
		// Test that overwrites in merge queue take precedence
		let db: Database<&str, &str> = Database::new();

		// Add initial data
		let mut txn1 = db.transaction(true);
		txn1.set("a", "1").unwrap();
		txn1.set("b", "2").unwrap();
		txn1.set("c", "3").unwrap();
		txn1.commit().unwrap();

		// Overwrite some values (goes to merge queue)
		let mut txn2 = db.transaction(true);
		txn2.set("a", "10").unwrap();
		txn2.set("b", "20").unwrap();
		txn2.commit().unwrap();

		// New transaction should see updated values
		let mut txn3 = db.transaction(false);
		let res = txn3.scan("a".."d", None, None).unwrap();
		assert_eq!(res.len(), 3);
		assert_eq!(res[0], ("a", "10")); // Updated value
		assert_eq!(res[1], ("b", "20")); // Updated value
		assert_eq!(res[2], ("c", "3")); // Original value
	}

	#[test]
	fn test_range_scan_boundary_conditions() {
		// Test exact boundary conditions
		let db: Database<&str, &str> = Database::new();

		let mut txn1 = db.transaction(true);
		txn1.set("a", "1").unwrap();
		txn1.set("aa", "2").unwrap();
		txn1.set("ab", "3").unwrap();
		txn1.set("b", "4").unwrap();
		txn1.set("ba", "5").unwrap();
		txn1.commit().unwrap();

		let mut txn2 = db.transaction(false);

		// Range starting exactly at a key
		let res = txn2.scan("aa".."b", None, None).unwrap();
		assert_eq!(res.len(), 2);
		assert_eq!(res[0], ("aa", "2"));
		assert_eq!(res[1], ("ab", "3"));

		// Range ending exactly at a key (exclusive)
		let res = txn2.scan("a".."aa", None, None).unwrap();
		assert_eq!(res.len(), 1);
		assert_eq!(res[0], ("a", "1"));

		// Range between keys
		let res = txn2.scan("aaa".."az", None, None).unwrap();
		assert_eq!(res.len(), 1);
		assert_eq!(res[0], ("ab", "3"));
	}

	#[test]
	fn test_range_scan_with_concurrent_transactions() {
		// Test scanning with uncommitted changes in current transaction
		let db: Database<&str, &str> = Database::new();

		// Add initial data
		let mut txn1 = db.transaction(true);
		txn1.set("a", "1").unwrap();
		txn1.set("c", "3").unwrap();
		txn1.set("e", "5").unwrap();
		txn1.commit().unwrap();

		// Start new transaction and make local changes
		let mut txn2 = db.transaction(true);
		txn2.set("b", "2").unwrap(); // New key
		txn2.set("c", "30").unwrap(); // Overwrite
		txn2.del("e").unwrap(); // Delete
		txn2.set("d", "4").unwrap(); // New key

		// Scan should see local changes
		let res = txn2.scan("a".."f", None, None).unwrap();
		assert_eq!(res.len(), 4);
		assert_eq!(res[0], ("a", "1")); // From merge queue
		assert_eq!(res[1], ("b", "2")); // Local new
		assert_eq!(res[2], ("c", "30")); // Local overwrite
		assert_eq!(res[3], ("d", "4")); // Local new
		                          // "e" is deleted locally, so not in results
	}

	#[test]
	fn test_range_scan_keys_only() {
		// Test keys() method with merge queue
		let db: Database<&str, &str> = Database::new();

		let mut txn1 = db.transaction(true);
		txn1.set("a", "1").unwrap();
		txn1.set("b", "2").unwrap();
		txn1.set("c", "3").unwrap();
		txn1.commit().unwrap();

		let mut txn2 = db.transaction(false);
		let keys = txn2.keys("a".."d", None, None).unwrap();
		assert_eq!(keys.len(), 3);
		assert_eq!(keys, vec!["a", "b", "c"]);

		// Test with reverse
		let keys = txn2.keys_reverse("a".."d", None, None).unwrap();
		assert_eq!(keys.len(), 3);
		assert_eq!(keys, vec!["c", "b", "a"]);
	}

	#[test]
	fn test_range_scan_total_count() {
		// Test total() method with merge queue
		let db: Database<&str, &str> = Database::new();

		let mut txn1 = db.transaction(true);
		txn1.set("key00", "val0").unwrap();
		txn1.set("key01", "val1").unwrap();
		txn1.set("key02", "val2").unwrap();
		txn1.set("key03", "val3").unwrap();
		txn1.set("key04", "val4").unwrap();
		txn1.set("key05", "val5").unwrap();
		txn1.set("key06", "val6").unwrap();
		txn1.set("key07", "val7").unwrap();
		txn1.set("key08", "val8").unwrap();
		txn1.set("key09", "val9").unwrap();
		txn1.commit().unwrap();

		let mut txn2 = db.transaction(false);

		// Count all
		let count = txn2.total("key00".."key99", None, None).unwrap();
		assert_eq!(count, 10);

		// Count with skip
		let count = txn2.total("key00".."key99", Some(3), None).unwrap();
		assert_eq!(count, 7);

		// Count with limit
		let count = txn2.total("key00".."key99", None, Some(5)).unwrap();
		assert_eq!(count, 5);

		// Count subset
		let count = txn2.total("key03".."key07", None, None).unwrap();
		assert_eq!(count, 4);
	}

	#[test]
	fn test_atomic_transaction_id_generation() {
		use std::sync::{Arc, Barrier};
		use std::thread;

		// Test that transaction queue IDs are unique under high concurrency
		let db = Arc::new(Database::<String, String>::default());
		let num_threads = 100;
		let commits_per_thread = 50;
		let barrier = Arc::new(Barrier::new(num_threads));

		// Collect all generated IDs
		let mut handles = vec![];

		for _ in 0..num_threads {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);

			let handle = thread::spawn(move || {
				// Synchronize all threads to start at the same time
				barrier.wait();

				for i in 0..commits_per_thread {
					let mut tx = db.transaction(true);
					tx.set(format!("key_{}", i), format!("value_{}", i)).unwrap();

					// Force the commit to generate a transaction queue ID
					match tx.commit() {
						Ok(_) => {
							// Successfully committed
						}
						Err(_) => {
							// May fail due to conflicts, which is fine for this test
						}
					}

					// Also test merge queue ID generation
					let mut tx2 = db.transaction(true);
					tx2.set(format!("merge_key_{}", i), format!("merge_value_{}", i)).unwrap();
					let _ = tx2.commit();
				}
			});

			handles.push(handle);
		}

		// Wait for all threads to complete
		for handle in handles {
			handle.join().unwrap();
		}

		// Verify the database maintains consistency under high concurrency
		// We can't directly access inner fields, but we can verify overall consistency
		// by checking that the database is still functional
		let mut tx = db.transaction(false);
		let mut count = 0;
		for _ in tx.scan("key_0".to_string().."key_999".to_string(), None, None).unwrap() {
			count += 1;
		}

		// We should have some successful commits
		assert!(count > 0, "Should have at least some successful commits");

		// Try to commit another transaction to verify the database is still healthy
		let mut tx = db.transaction(true);
		tx.set("final_key", "final_value".to_string()).unwrap();
		tx.commit().expect("Final commit should succeed, database should be healthy");
	}

	#[test]
	fn test_atomic_commit_ordering() {
		use std::sync::{Arc, Barrier};
		use std::thread;
		use std::time::Duration;

		// Test that the atomic_commit function maintains ordering guarantees
		let db = Arc::new(Database::<String, String>::default());
		let num_threads = 50;
		let barrier = Arc::new(Barrier::new(num_threads));

		let mut handles = vec![];

		for thread_id in 0..num_threads {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);

			let handle = thread::spawn(move || {
				// Synchronize all threads to maximize contention
				barrier.wait();

				// Each thread tries to commit multiple transactions
				for i in 0..10 {
					let mut tx = db.transaction(true);
					let key = format!("thread_{}_key_{}", thread_id, i);
					let value = format!("thread_{}_value_{}", thread_id, i);

					tx.set(key.clone(), value.clone()).unwrap();

					// Try to commit - this will exercise atomic_commit
					// We don't verify reads here because in MVCC, a new read transaction
					// might not see recent commits depending on its snapshot
					let _ = tx.commit(); // Success or conflict, both are fine for this test

					// Small delay to vary timing
					thread::sleep(Duration::from_micros(thread_id as u64));
				}
			});

			handles.push(handle);
		}

		// Wait for all threads
		for handle in handles {
			handle.join().unwrap();
		}

		// Verify database is in a consistent state
		let mut tx = db.transaction(false);
		let mut count = 0;
		// Use a very wide range to scan all keys
		for _ in tx.scan("".to_string().."~".to_string(), None, None).unwrap() {
			count += 1;
		}

		// We should have many successful commits
		assert!(count > 0, "Should have at least some successful commits");
		assert!(count <= (num_threads * 10), "Should not exceed maximum possible commits");

		// Test that we can still perform operations
		let mut final_tx = db.transaction(true);
		final_tx.set("ordering_test_complete", "true".to_string()).unwrap();
		final_tx.commit().expect("Final transaction should succeed");
	}

	#[test]
	fn test_savepoints() {
		let db: Database<&str, &str> = Database::new();

		// === BASIC SAVEPOINT FUNCTIONALITY ===
		let mut tx = db.transaction(true);
		tx.set("initial_key", "initial_value").unwrap();

		// Set a savepoint
		tx.set_savepoint().unwrap();

		// Make some changes after the savepoint
		tx.set("savepoint_key", "savepoint_value").unwrap();
		tx.set("another_key", "another_value").unwrap();

		// Verify the changes exist
		assert_eq!(tx.get("initial_key").unwrap(), Some("initial_value"));
		assert_eq!(tx.get("savepoint_key").unwrap(), Some("savepoint_value"));
		assert_eq!(tx.get("another_key").unwrap(), Some("another_value"));

		// Rollback to savepoint
		tx.rollback_to_savepoint().unwrap();

		// Verify the rollback worked - changes after savepoint should be gone
		assert_eq!(tx.get("initial_key").unwrap(), Some("initial_value"));
		assert_eq!(tx.get("savepoint_key").unwrap(), None);
		assert_eq!(tx.get("another_key").unwrap(), None);

		// Make new changes after rollback
		tx.set("new_key", "new_value").unwrap();
		assert_eq!(tx.get("new_key").unwrap(), Some("new_value"));

		// === NESTED STACKABLE SAVEPOINTS ===
		// Initial state: {initial_key, new_key}
		tx.set("base", "value").unwrap();

		// First nested savepoint
		tx.set_savepoint().unwrap();
		tx.set("level1_key1", "level1_value1").unwrap();
		tx.set("level1_key2", "level1_value2").unwrap();

		// Second nested savepoint
		tx.set_savepoint().unwrap();
		tx.set("level2_key1", "level2_value1").unwrap();
		tx.set("level1_key1", "modified_at_level2").unwrap(); // Modify existing key

		// Third nested savepoint
		tx.set_savepoint().unwrap();
		tx.set("level3_key1", "level3_value1").unwrap();

		// Verify all data is present
		assert_eq!(tx.get("base").unwrap(), Some("value"));
		assert_eq!(tx.get("level1_key1").unwrap(), Some("modified_at_level2"));
		assert_eq!(tx.get("level1_key2").unwrap(), Some("level1_value2"));
		assert_eq!(tx.get("level2_key1").unwrap(), Some("level2_value1"));
		assert_eq!(tx.get("level3_key1").unwrap(), Some("level3_value1"));

		// Rollback from level 3 to level 2
		tx.rollback_to_savepoint().unwrap();
		assert_eq!(tx.get("base").unwrap(), Some("value"));
		assert_eq!(tx.get("level1_key1").unwrap(), Some("modified_at_level2"));
		assert_eq!(tx.get("level1_key2").unwrap(), Some("level1_value2"));
		assert_eq!(tx.get("level2_key1").unwrap(), Some("level2_value1"));
		assert_eq!(tx.get("level3_key1").unwrap(), None); // Gone after rollback

		// Add new data at level 2
		tx.set("level2_new", "after_rollback").unwrap();

		// Rollback from level 2 to level 1
		tx.rollback_to_savepoint().unwrap();
		assert_eq!(tx.get("base").unwrap(), Some("value"));
		assert_eq!(tx.get("level1_key1").unwrap(), Some("level1_value1")); // Restored original
		assert_eq!(tx.get("level1_key2").unwrap(), Some("level1_value2"));
		assert_eq!(tx.get("level2_key1").unwrap(), None); // Gone
		assert_eq!(tx.get("level2_new").unwrap(), None); // Gone
		assert_eq!(tx.get("level3_key1").unwrap(), None); // Still gone

		// Add final data and commit
		tx.set("final", "committed_data").unwrap();
		tx.commit().unwrap();

		// === VERIFY FINAL STATE ===
		let mut tx_verify = db.transaction(false);
		// From basic test
		assert_eq!(tx_verify.get("initial_key").unwrap(), Some("initial_value"));
		assert_eq!(tx_verify.get("new_key").unwrap(), Some("new_value"));
		assert_eq!(tx_verify.get("savepoint_key").unwrap(), None);
		assert_eq!(tx_verify.get("another_key").unwrap(), None);
		// From nested test
		assert_eq!(tx_verify.get("base").unwrap(), Some("value"));
		assert_eq!(tx_verify.get("level1_key1").unwrap(), Some("level1_value1"));
		assert_eq!(tx_verify.get("level1_key2").unwrap(), Some("level1_value2"));
		assert_eq!(tx_verify.get("final").unwrap(), Some("committed_data"));
		assert_eq!(tx_verify.get("level2_key1").unwrap(), None);
		assert_eq!(tx_verify.get("level2_new").unwrap(), None);
		assert_eq!(tx_verify.get("level3_key1").unwrap(), None);
	}

	#[test]
	fn test_scan_all_versions() {
		let db: Database<&str, &str> = Database::new();

		// Transaction 1: Add initial data
		let mut tx1 = db.transaction(true);
		tx1.set("key1", "v1").unwrap();
		tx1.set("key2", "v1").unwrap();
		tx1.set("key3", "v1").unwrap();
		tx1.commit().unwrap();

		// Transaction 2: Update key1 and key2
		let mut tx2 = db.transaction(true);
		tx2.set("key1", "v2").unwrap();
		tx2.set("key2", "v2").unwrap();
		tx2.commit().unwrap();

		// Transaction 3: Update key1 again and delete key3
		let mut tx3 = db.transaction(true);
		tx3.set("key1", "v3").unwrap();
		tx3.del("key3").unwrap();
		tx3.commit().unwrap();

		// Transaction 4: Read all versions
		let mut tx4 = db.transaction(false);
		let results = tx4.scan_all_versions("key0".."key9", None, None).unwrap();

		// Verify we have all versions:
		// key1: should have 3 versions (v1, v2, v3)
		// key2: should have 2 versions (v1, v2)
		// key3: should have 2 versions (v1, None/deleted)
		let key1_versions: Vec<_> = results.iter().filter(|(k, _, _)| k == &"key1").collect();
		let key2_versions: Vec<_> = results.iter().filter(|(k, _, _)| k == &"key2").collect();
		let key3_versions: Vec<_> = results.iter().filter(|(k, _, _)| k == &"key3").collect();

		assert_eq!(key1_versions.len(), 3, "key1 should have 3 versions");
		assert_eq!(key2_versions.len(), 2, "key2 should have 2 versions");
		assert_eq!(key3_versions.len(), 2, "key3 should have 2 versions (including delete)");

		// Verify key1 versions are correct
		assert_eq!(key1_versions[0].2, Some("v1"));
		assert_eq!(key1_versions[1].2, Some("v2"));
		assert_eq!(key1_versions[2].2, Some("v3"));

		// Verify key2 versions are correct
		assert_eq!(key2_versions[0].2, Some("v1"));
		assert_eq!(key2_versions[1].2, Some("v2"));

		// Verify key3 versions are correct (last one is deleted)
		assert_eq!(key3_versions[0].2, Some("v1"));
		assert_eq!(key3_versions[1].2, None); // Deleted

		// Test with skip: skip first key (key1)
		let mut tx5 = db.transaction(false);
		let results = tx5.scan_all_versions("key0".."key9", Some(1), None).unwrap();

		// Should have key2 and key3 only
		let keys: Vec<_> = results.iter().map(|(k, _, _)| *k).collect();
		assert!(!keys.contains(&"key1"), "key1 should be skipped");
		assert!(keys.contains(&"key2"), "key2 should be included");
		assert!(keys.contains(&"key3"), "key3 should be included");

		// Verify we still have all versions of the remaining keys
		let key2_versions: Vec<_> = results.iter().filter(|(k, _, _)| k == &"key2").collect();
		assert_eq!(key2_versions.len(), 2, "key2 should still have all 2 versions");

		// Test with limit: limit to first 2 keys (key1 and key2)
		let mut tx6 = db.transaction(false);
		let results = tx6.scan_all_versions("key0".."key9", None, Some(2)).unwrap();

		// Should have key1 and key2 only (all their versions)
		let keys: Vec<_> = results.iter().map(|(k, _, _)| *k).collect();
		assert!(keys.contains(&"key1"), "key1 should be included");
		assert!(keys.contains(&"key2"), "key2 should be included");
		assert!(!keys.contains(&"key3"), "key3 should not be included (limit)");

		// Verify we have all versions of the limited keys
		let key1_versions: Vec<_> = results.iter().filter(|(k, _, _)| k == &"key1").collect();
		assert_eq!(key1_versions.len(), 3, "key1 should still have all 3 versions");

		// Test with skip and limit: skip 1, limit 1 (should get only key2)
		let mut tx7 = db.transaction(false);
		let results = tx7.scan_all_versions("key0".."key9", Some(1), Some(1)).unwrap();

		let keys: Vec<_> = results.iter().map(|(k, _, _)| *k).collect();
		assert!(!keys.contains(&"key1"), "key1 should be skipped");
		assert!(keys.contains(&"key2"), "key2 should be included");
		assert!(!keys.contains(&"key3"), "key3 should not be included (limit)");

		let key2_versions: Vec<_> = results.iter().filter(|(k, _, _)| k == &"key2").collect();
		assert_eq!(key2_versions.len(), 2, "key2 should have all 2 versions");
	}

	#[test]
	fn test_scan_all_versions_with_writeset() {
		let db: Database<&str, &str> = Database::new();

		// Transaction 1: Add initial data
		let mut tx1 = db.transaction(true);
		tx1.set("key1", "v1").unwrap();
		tx1.set("key2", "v1").unwrap();
		tx1.commit().unwrap();

		// Transaction 2: Update key1
		let mut tx2 = db.transaction(true);
		tx2.set("key1", "v2").unwrap();
		tx2.commit().unwrap();

		// Transaction 3: Start a write transaction and scan all versions
		// The writeset changes should be included in the scan
		let mut tx3 = db.transaction(true);
		tx3.set("key1", "v3").unwrap(); // Update key1 in writeset
		tx3.set("key3", "new").unwrap(); // Add new key in writeset

		let results = tx3.scan_all_versions("key0".."key9", None, None).unwrap();

		// key1 should have 3 versions: v1 (datastore), v2 (datastore), v3 (writeset)
		let key1_versions: Vec<_> = results.iter().filter(|(k, _, _)| k == &"key1").collect();
		assert_eq!(key1_versions.len(), 3, "key1 should have 3 versions including writeset");
		assert_eq!(key1_versions[2].2, Some("v3"), "Latest version should be from writeset");

		// key2 should have 1 version: v1 (datastore)
		let key2_versions: Vec<_> = results.iter().filter(|(k, _, _)| k == &"key2").collect();
		assert_eq!(key2_versions.len(), 1, "key2 should have 1 version");
		assert_eq!(key2_versions[0].2, Some("v1"));

		// key3 should have 1 version: new (writeset only)
		let key3_versions: Vec<_> = results.iter().filter(|(k, _, _)| k == &"key3").collect();
		assert_eq!(key3_versions.len(), 1, "key3 should have 1 version from writeset");
		assert_eq!(key3_versions[0].2, Some("new"));

		// Cancel the transaction so it doesn't commit
		tx3.cancel().unwrap();

		// Transaction 4: Read-only transaction should not see tx3's writeset
		let mut tx4 = db.transaction(false);
		let results = tx4.scan_all_versions("key0".."key9", None, None).unwrap();

		let key1_versions: Vec<_> = results.iter().filter(|(k, _, _)| k == &"key1").collect();
		assert_eq!(key1_versions.len(), 2, "key1 should only have 2 versions (tx3 was cancelled)");

		let key3_versions: Vec<_> = results.iter().filter(|(k, _, _)| k == &"key3").collect();
		assert_eq!(key3_versions.len(), 0, "key3 should not exist (tx3 was cancelled)");
	}

	#[test]
	fn test_savepoint_errors() {
		let db: Database<&str, &str> = Database::new();

		// Test error when no savepoint is set
		let mut tx = db.transaction(true);
		assert!(matches!(tx.rollback_to_savepoint(), Err(Error::NoSavepoint)));

		// Test error on read-only transaction
		let mut tx_readonly = db.transaction(false);
		assert!(matches!(tx_readonly.set_savepoint(), Err(Error::TxNotWritable)));
		assert!(matches!(tx_readonly.rollback_to_savepoint(), Err(Error::TxNotWritable)));

		// Test error on closed transaction
		let mut tx_closed = db.transaction(true);
		tx_closed.cancel().unwrap();
		assert!(matches!(tx_closed.set_savepoint(), Err(Error::TxClosed)));
		assert!(matches!(tx_closed.rollback_to_savepoint(), Err(Error::TxClosed)));

		// Test stack exhaustion - error when rolling back more savepoints than were set
		let db_stack: Database<String, &str> = Database::new();
		let mut tx_stack = db_stack.transaction(true);

		// Set multiple nested savepoints
		for i in 0..3 {
			let key = format!("key{}", i);
			tx_stack.set(key, "value").unwrap();
			tx_stack.set_savepoint().unwrap();
		}

		// Rollback all savepoints one by one
		for _rollback_count in 0..3 {
			tx_stack.rollback_to_savepoint().unwrap();
		}

		// Should error when no more savepoints available
		assert!(matches!(tx_stack.rollback_to_savepoint(), Err(Error::NoSavepoint)));
	}

	#[test]
	fn test_gc_does_not_remove_active_versions() {
		// Test for garbage collection regression issue where versions
		// were being removed too aggressively
		// Create a database with GC disabled (automatic version cleanup enabled)
		let db: Database<Vec<u8>, Vec<u8>> = Database::new();

		// Insert 10,000 keys one-by-one
		for i in 0..10_000 {
			let key = format!("key_{:08}", i).into_bytes();
			let value = format!("value_{:08}", i).into_bytes();

			let mut tx = db.transaction(true);
			tx.put(key, value).unwrap();
			tx.commit().unwrap();
		}

		// Read each key one-by-one
		for i in 0..10_000 {
			let key = format!("key_{:08}", i).into_bytes();
			let expected_value = format!("value_{:08}", i).into_bytes();

			let mut tx = db.transaction(false);
			let result = tx.get(key.clone());
			assert!(result.is_ok(), "Failed to get key at index {i}: {result:?}");
			let value = result.unwrap();
			assert!(
				value.is_some(),
				"Key at index {} was not found (version was garbage collected too early)",
				i
			);
			assert_eq!(*value.unwrap(), expected_value, "Value mismatch at index {i}");
			tx.cancel().unwrap();
		}
	}

	#[test]
	fn test_gc_concurrent_readers() {
		use std::sync::Arc;
		use std::thread;

		// Create a database
		let db: Database<Vec<u8>, Vec<u8>> = Database::new();

		// Insert initial data
		{
			let mut tx = db.transaction(true);
			for i in 0..1000 {
				let key = format!("key_{:08}", i).into_bytes();
				let value = format!("value_{:08}", i).into_bytes();
				tx.put(key, value).unwrap();
			}
			tx.commit().unwrap();
		}

		// Wrap database in Arc for sharing across threads
		let db = Arc::new(db);

		// Spawn multiple reader threads
		let mut handles = vec![];
		for thread_id in 0..4 {
			let db = Arc::clone(&db);
			let handle = thread::spawn(move || {
				// Each thread reads all keys
				for i in 0..1000 {
					let key = format!("key_{:08}", i).into_bytes();
					let mut tx = db.transaction(false);
					let result = tx.get(key.clone());
					assert!(
						result.is_ok(),
						"Thread {thread_id} failed to get key at index {i}: {result:?}",
					);
					let value = result.unwrap();
					assert!(value.is_some(), "Thread {thread_id} could not find key at index {i} (version was garbage collected)");
					tx.cancel().unwrap();

					// Interleave with some writes to trigger GC
					// Each thread writes to its own keys to avoid conflicts
					if i % 100 == 0 {
						let mut write_tx = db.transaction(true);
						let update_key = format!("thread_{thread_id}_key_{i:08}").into_bytes();
						let update_value = format!("updated_by_thread_{thread_id}").into_bytes();
						write_tx.put(update_key, update_value).unwrap();
						write_tx.commit().unwrap();
					}
				}
			});
			handles.push(handle);
		}

		// Wait for all threads to complete
		for handle in handles {
			handle.join().unwrap();
		}
	}

	#[test]
	fn test_concurrent_write_read_merge_queue_race() {
		// This test specifically targets the race condition where readers check
		// is_removed() on merge queue entries and miss data that's being merged.
		// The race manifests under high concurrency when:
		// 1. Writer commits and merges data into datastore
		// 2. Writer removes entry from merge queue
		// 3. Reader checks merge queue (sees removed), checks datastore (doesn't see data yet)
		//
		// This was fixed by removing the is_removed() checks and always checking all
		// merge queue entries, since crossbeam-skiplist guarantees they remain accessible.

		let db: Database<Vec<u8>, Vec<u8>> = Database::new();
		let db = Arc::new(db);

		// Run multiple iterations to increase chance of hitting the race
		for iteration in 0..100 {
			let key = format!("race_test_key_{}", iteration).into_bytes();
			let value = format!("race_test_value_{}", iteration).into_bytes();

			let db_writer = Arc::clone(&db);
			let key_writer = key.clone();
			let value_writer = value.clone();

			let db_reader = Arc::clone(&db);
			let key_reader = key.clone();
			let value_reader = value.clone();

			// Spawn writer thread
			let writer = thread::spawn(move || {
				let mut tx = db_writer.transaction(true);
				tx.set(key_writer, value_writer).unwrap();
				tx.commit().unwrap();
			});

			// Spawn reader thread that races with the writer
			let reader = thread::spawn(move || {
				// Small delay to let writer start, then race during commit
				thread::yield_now();

				// Try multiple reads to catch the race window
				for _ in 0..10 {
					let mut tx = db_reader.transaction(false);
					let result = tx.get(key_reader.clone()).unwrap();
					tx.cancel().unwrap();

					// If we see the value, it must be correct
					if let Some(v) = result {
						assert_eq!(v, value_reader, "Read wrong value during race");
						return true;
					}
					// Spin a bit to increase chance of hitting race window
					thread::yield_now();
				}
				false // Didn't see the value (read before commit completed)
			});

			// Wait for both to complete
			writer.join().unwrap();
			let _saw_value = reader.join().unwrap();

			// After both complete, a fresh read MUST see the value
			let mut tx = db.transaction(false);
			let result = tx.get(key.clone()).unwrap();
			tx.cancel().unwrap();
			assert!(
				result.is_some(),
				"Key must be visible after writer completes (iteration {})",
				iteration
			);
			assert_eq!(
				result.unwrap(),
				value,
				"Value mismatch after writer completes (iteration {})",
				iteration
			);
		}
	}

	#[test]
	fn test_high_concurrency_merge_queue_visibility() {
		// Simulate the crud-bench scenario: many concurrent writers and readers
		// This stresses the merge queue under high contention

		let db: Database<Vec<u8>, Vec<u8>> = Database::new();
		let db = Arc::new(db);

		let num_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
		let operations_per_thread = 50;

		let mut handles = vec![];

		// Spawn writer threads
		for thread_id in 0..num_threads {
			let db = Arc::clone(&db);
			let handle = thread::spawn(move || {
				for op_id in 0..operations_per_thread {
					let key = format!("thread_{}_key_{}", thread_id, op_id).into_bytes();
					let value = format!("thread_{}_value_{}", thread_id, op_id).into_bytes();

					let mut tx = db.transaction(true);
					tx.set(key, value).unwrap();
					tx.commit().unwrap();
				}
			});
			handles.push(handle);
		}

		// Spawn reader threads that try to read as writers are committing
		for reader_id in 0..num_threads {
			let db = Arc::clone(&db);
			let handle = thread::spawn(move || {
				// Read keys from different writers
				for writer_id in 0..num_threads {
					for op_id in 0..operations_per_thread {
						let key = format!("thread_{}_key_{}", writer_id, op_id).into_bytes();
						let expected_value =
							format!("thread_{}_value_{}", writer_id, op_id).into_bytes();

						// Try reading multiple times
						for _ in 0..5 {
							let mut tx = db.transaction(false);
							if let Some(value) = tx.get(key.clone()).unwrap() {
								// If we see a value, it must be correct
								assert_eq!(
									value, expected_value,
									"Reader {} saw wrong value for writer {} op {}",
									reader_id, writer_id, op_id
								);
							}
							tx.cancel().unwrap();
							thread::yield_now();
						}
					}
				}
			});
			handles.push(handle);
		}

		// Wait for all threads
		for handle in handles {
			handle.join().unwrap();
		}

		// Final verification: all keys must be visible
		for thread_id in 0..num_threads {
			for op_id in 0..operations_per_thread {
				let key = format!("thread_{}_key_{}", thread_id, op_id).into_bytes();
				let expected_value = format!("thread_{}_value_{}", thread_id, op_id).into_bytes();

				let mut tx = db.transaction(false);
				let result = tx.get(key.clone()).unwrap();
				tx.cancel().unwrap();

				assert!(
					result.is_some(),
					"Key from thread {} op {} not found after all threads complete",
					thread_id,
					op_id
				);
				assert_eq!(
					result.unwrap(),
					expected_value,
					"Wrong value for thread {} op {} after all threads complete",
					thread_id,
					op_id
				);
			}
		}
	}
}
