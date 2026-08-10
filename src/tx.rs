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

use crate::bloom::BloomFilter;
use crate::cursor::{Cursor, KeyIterator, ScanIterator};
use crate::direction::Direction;
use crate::err::Error;
use crate::inner::{Inner, Slot, COMMIT_ABORTED, SLOT_PINNING};
use crate::iter::{MergeIterator, MergeQueueIter};
use crate::kv::IntoBytes;
use crate::pool::Pool;
use crate::queue::{Commit, Merge};
use crate::version::Version;
use crate::versions::Versions;
#[cfg(debug_assertions)]
use crate::LOG_TARGET_CONFLICTS;
use arc_swap::ArcSwap;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use papaya::HashSet;
use parking_lot::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::ops::Range;
use std::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(debug_assertions)]
use tracing::debug;

/// The isolation level of a database transaction
#[derive(PartialEq, PartialOrd)]
pub enum IsolationLevel {
	/// Reads see a consistent snapshot, and commits conflict only on
	/// write-write overlap
	SnapshotIsolation,
	/// As snapshot isolation, and additionally tracks the read set so that
	/// read-write conflicts are detected at commit time
	SerializableSnapshotIsolation,
}

// --------------------------------------------------
// Transaction
// --------------------------------------------------

/// A serializable database transaction
pub struct Transaction {
	/// The transaction pool for this transaction
	pub(crate) pool: Arc<Pool>,
	/// The inner transaction for this transaction
	pub(crate) inner: Option<TransactionInner>,
}

/// Panic message for the `Transaction::inner` accessors below.
///
/// `inner` is only an `Option` so that `Drop` can move the inner transaction
/// out and return it to the pool. It is `Some` for the entire lifetime of a
/// `Transaction`, and the only `take` is in `Drop`, so no method on a live
/// `Transaction` can observe `None`.
const INNER_TAKEN: &str = "Transaction::inner is only taken in Drop";

// Transaction must be Send + Sync so downstream callers can place it
// behind shared locks like `tokio::sync::RwLock<Transaction>`.
const _: fn() = || {
	const fn assert_send_sync<T: Send + Sync>() {}
	assert_send_sync::<Transaction>();
};

impl Drop for Transaction {
	fn drop(&mut self) {
		if let Some(inner) = self.inner.take() {
			// Unpin: remove this transaction's slot from the readers map.
			// A sweeper holding the entry mid-scan reads our final
			// snapshot values, which is only ever conservative. The slot
			// allocation itself is retained on the pooled transaction and
			// re-pinned on reuse.
			inner.database.readers.remove(&inner.slot_id);
			// Put the transaction in to the pool
			self.pool.put(inner);
		}
	}
}

impl Transaction {
	/// Ensure this transaction is committed with snapshot isolation guarantees
	pub const fn with_snapshot_isolation(mut self) -> Self {
		self.inner.as_mut().expect(INNER_TAKEN).mode = IsolationLevel::SnapshotIsolation;
		self
	}

	/// Ensure this transaction is committed with serializable snapshot
	/// isolation guarantees
	pub const fn with_serializable_snapshot_isolation(mut self) -> Self {
		self.inner.as_mut().expect(INNER_TAKEN).mode =
			IsolationLevel::SerializableSnapshotIsolation;
		self
	}

	/// Set a savepoint in the transaction for partial rollback
	/// This method is stackable and can be called multiple times with
	/// corresponding calls to `rollback_to_savepoint`
	pub fn set_savepoint(&mut self) -> Result<(), Error> {
		self.inner.as_mut().expect(INNER_TAKEN).set_savepoint()
	}

	/// Rollback the transaction to the most recently set savepoint
	/// After calling this method, subsequent modifications within this
	/// transaction can be rolled back by calling `rollback_to_savepoint`
	/// again if there are more savepoints in the stack
	pub fn rollback_to_savepoint(&mut self) -> Result<(), Error> {
		self.inner.as_mut().expect(INNER_TAKEN).rollback_to_savepoint()
	}

	/// Get the starting sequence number of this transaction
	pub const fn version(&self) -> u64 {
		self.inner.as_ref().expect(INNER_TAKEN).version()
	}

	/// Check if the transaction is closed
	pub const fn closed(&self) -> bool {
		self.inner.as_ref().expect(INNER_TAKEN).closed()
	}

	/// Cancel the transaction and rollback any changes
	pub fn cancel(&mut self) -> Result<(), Error> {
		self.inner.as_mut().expect(INNER_TAKEN).cancel()
	}

	/// Commit the transaction and store all changes
	pub fn commit(&mut self) -> Result<(), Error> {
		self.inner.as_mut().expect(INNER_TAKEN).commit()
	}

	/// Check if a key exists in the database
	pub fn exists<K>(&self, key: K) -> Result<bool, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).exists(key)
	}

	/// Fetch a key from the database
	pub fn get<K>(&self, key: K) -> Result<Option<Bytes>, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).get(key)
	}

	/// Fetch multiple keys from the database
	pub fn getm<K>(&self, keys: Vec<K>) -> Result<Vec<Option<Bytes>>, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).getm(keys)
	}

	/// Insert or update a key in the database
	pub fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: IntoBytes,
		V: IntoBytes,
	{
		self.inner.as_mut().expect(INNER_TAKEN).set(key, val)
	}

	/// Insert a key if it doesn't exist in the database
	pub fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: IntoBytes,
		V: IntoBytes,
	{
		self.inner.as_mut().expect(INNER_TAKEN).put(key, val)
	}

	/// Insert a key if it matches a value
	pub fn putc<K, V, C>(&mut self, key: K, val: V, chk: Option<C>) -> Result<(), Error>
	where
		K: IntoBytes,
		V: IntoBytes,
		C: IntoBytes,
	{
		self.inner.as_mut().expect(INNER_TAKEN).putc(key, val, chk)
	}

	/// Delete a key from the database
	pub fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: IntoBytes,
	{
		self.inner.as_mut().expect(INNER_TAKEN).del(key)
	}

	/// Delete a key if it matches a value
	pub fn delc<K, C>(&mut self, key: K, chk: Option<C>) -> Result<(), Error>
	where
		K: IntoBytes,
		C: IntoBytes,
	{
		self.inner.as_mut().expect(INNER_TAKEN).delc(key, chk)
	}

	/// Retrieve a count of keys from the database
	pub fn total<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<usize, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).total(rng, skip, limit)
	}

	/// Retrieve a range of keys from the database
	pub fn keys<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<Bytes>, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).keys(rng, skip, limit)
	}

	/// Retrieve a range of keys from the database, in reverse order
	pub fn keys_reverse<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<Bytes>, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).keys_reverse(rng, skip, limit)
	}

	/// Retrieve a range of keys and values from the database
	pub fn scan<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<(Bytes, Bytes)>, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).scan(rng, skip, limit)
	}

	/// Retrieve a range of keys and values from the database in reverse order
	pub fn scan_reverse<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<(Bytes, Bytes)>, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).scan_reverse(rng, skip, limit)
	}

	// --------------------------------------------------
	// Callback and buffer-based scan API
	// --------------------------------------------------

	/// Call a closure for each key-value pair in a range, avoiding intermediate
	/// Vec allocation. Return `false` from the closure to stop iteration early.
	pub fn scan_for_each<K, F>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
		f: F,
	) -> Result<usize, Error>
	where
		K: IntoBytes,
		F: FnMut(&Bytes, &Bytes) -> bool,
	{
		self.inner.as_ref().expect(INNER_TAKEN).scan_for_each(rng, skip, limit, f)
	}

	/// Call a closure for each key in a range, avoiding intermediate Vec
	/// allocation. Return `false` from the closure to stop iteration early.
	pub fn keys_for_each<K, F>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
		f: F,
	) -> Result<usize, Error>
	where
		K: IntoBytes,
		F: FnMut(&Bytes) -> bool,
	{
		self.inner.as_ref().expect(INNER_TAKEN).keys_for_each(rng, skip, limit, f)
	}

	/// Scan key-value pairs into a caller-provided Vec, reusing its capacity.
	pub fn scan_into<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
		buf: &mut Vec<(Bytes, Bytes)>,
	) -> Result<(), Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).scan_into(rng, skip, limit, buf)
	}

	/// Scan keys into a caller-provided Vec, reusing its capacity.
	pub fn keys_into<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
		buf: &mut Vec<Bytes>,
	) -> Result<(), Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).keys_into(rng, skip, limit, buf)
	}

	// --------------------------------------------------
	// Cursor and Iterator API
	// --------------------------------------------------

	/// Create a cursor over a range of keys.
	///
	/// The cursor provides low-level bidirectional iteration with seek support.
	/// Use this when you need fine-grained control over iteration direction
	/// and position.
	///
	/// # Example
	///
	/// ```ignore
	/// let mut cursor = tx.cursor("a".."z")?;
	/// cursor.seek_to_first();
	/// while cursor.valid() {
	///     println!("Key: {:?}", cursor.key());
	///     cursor.next();
	/// }
	/// ```
	pub fn cursor<K>(&self, rng: Range<K>) -> Result<Cursor<'_>, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).cursor(rng)
	}

	/// Iterate over keys in a range.
	///
	/// Returns an iterator that yields keys in forward order.
	/// Use `.take(n)` to limit results and `.skip(n)` to skip entries.
	///
	/// # Example
	///
	/// ```ignore
	/// for key in tx.keys_iter("a".."z")?.take(10) {
	///     println!("Key: {:?}", key);
	/// }
	/// ```
	pub fn keys_iter<K>(&self, rng: Range<K>) -> Result<KeyIterator<'_>, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).keys_iter(rng)
	}

	/// Iterate over keys in a range, in reverse order.
	pub fn keys_iter_reverse<K>(&self, rng: Range<K>) -> Result<KeyIterator<'_>, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).keys_iter_reverse(rng)
	}

	/// Iterate over key-value pairs in a range.
	///
	/// Returns an iterator that yields (key, value) pairs in forward order.
	/// Use `.take(n)` to limit results and `.skip(n)` to skip entries.
	///
	/// # Example
	///
	/// ```ignore
	/// for (key, value) in tx.scan_iter("a".."z")?.take(10) {
	///     println!("Key: {:?}, Value: {:?}", key, value);
	/// }
	/// ```
	pub fn scan_iter<K>(&self, rng: Range<K>) -> Result<ScanIterator<'_>, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).scan_iter(rng)
	}

	/// Iterate over key-value pairs in a range, in reverse order.
	pub fn scan_iter_reverse<K>(&self, rng: Range<K>) -> Result<ScanIterator<'_>, Error>
	where
		K: IntoBytes,
	{
		self.inner.as_ref().expect(INNER_TAKEN).scan_iter_reverse(rng)
	}
}

// --------------------------------------------------
// SavepointState
// --------------------------------------------------

/// A savepoint state capturing transaction state at a specific point
struct SavepointState {
	/// The readset at the time of the savepoint
	readset: HashSet<Bytes>,
	/// The scanset at the time of the savepoint
	scanset: SkipMap<Bytes, ArcSwap<Bytes>>,
	/// The writeset at the time of the savepoint
	writeset: BTreeMap<Bytes, Option<Bytes>>,
}

// --------------------------------------------------
// TransactionInner
// --------------------------------------------------

/// The inner structure of a database transaction
pub(crate) struct TransactionInner {
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
	pub(crate) readset: HashSet<Bytes>,
	/// Bloom filter over the readset for fast conflict pre-checks
	pub(crate) readset_bloom: Mutex<BloomFilter>,
	/// The local set of key scans
	pub(crate) scanset: SkipMap<Bytes, ArcSwap<Bytes>>,
	/// The local set of updates and deletes
	pub(crate) writeset: BTreeMap<Bytes, Option<Bytes>>,
	/// The parent database for this transaction
	pub(crate) database: Arc<Inner>,
	/// The reference to this transaction's pinned reader slot
	pub(crate) slot: Arc<Slot>,
	/// The key of this transaction's slot in the readers map
	pub(crate) slot_id: u64,
	/// Threshold after which transaction state is reset
	reset_threshold: usize,
	/// Stack of savepoint states for nested partial rollbacks
	savepoint_stack: Vec<SavepointState>,
}

/// Register a transaction in the readers map and choose its snapshot.
///
/// Pin-then-read: the slot is published (in the pinning state) BEFORE
/// the snapshot values are loaded, so every watermark scan computed
/// after our insert either sees the pinning sentinel (and skips
/// reclamation for that pass) or sees our final snapshot values (and is
/// bounded by them). A scan computed before our insert loaded its
/// bounds before our snapshot loads in the `SeqCst` total order, so our
/// snapshot is at or above every bound it used — and reclamation always
/// retains the entry visible at its watermark. This closes the historic
/// load-then-register race structurally, with no validate/rollback
/// retry loop and no reclamation floor.
///
/// The commit snapshot is loaded BEFORE the version snapshot, and from
/// the contiguous completed watermark rather than the raw commit id: a
/// commit id becomes visible before its merge version is published, so
/// snapshotting the raw id could exclude a commit from our conflict
/// window while its writes are invisible at our version snapshot — a
/// lost update. Every commit at or below the watermark has published
/// its merge version, so the version snapshot (loaded after) always
/// covers the excluded prefix; any commit landing between the two
/// loads simply falls inside our conflict window, which is at worst a
/// spurious conflict, never a missed one.
#[inline]
fn pin_slot(db: &Inner, slot: &Arc<Slot>) -> (u64, u64, u64) {
	// Publish the pinning sentinels before the slot becomes visible
	slot.version.store(SLOT_PINNING, Ordering::SeqCst);
	slot.commit.store(SLOT_PINNING, Ordering::SeqCst);
	// Insert the slot into the readers map under a fresh id
	let slot_id = db.reader_slot_id.fetch_add(1, Ordering::Relaxed) + 1;
	db.readers.insert(slot_id, Arc::clone(slot));
	// Pair with the fence in every watermark scan
	fence(Ordering::SeqCst);
	// Load the commit snapshot, then the version snapshot
	let commit = db.commit_watermark.load(Ordering::SeqCst);
	let version = db.oracle.timestamp.load(Ordering::SeqCst);
	// Publish the chosen snapshot into the slot
	slot.commit.store(commit, Ordering::SeqCst);
	slot.version.store(version, Ordering::SeqCst);
	// Return the slot id and the snapshot
	(slot_id, commit, version)
}

impl TransactionInner {
	/// Create a new read-only or writeable transaction
	pub(crate) fn new(db: Arc<Inner>, write: bool) -> Self {
		// Allocate this transaction's slot and pin it
		let slot = Arc::new(Slot::pinning());
		let (slot_id, commit, version) = pin_slot(&db, &slot);
		// Store the threshold separately before moving db
		let threshold = db.reset_threshold;
		// Create the transaction
		Self {
			mode: IsolationLevel::SerializableSnapshotIsolation,
			done: false,
			write,
			commit,
			version,
			readset: HashSet::new(),
			readset_bloom: Mutex::new(BloomFilter::new()),
			scanset: SkipMap::new(),
			writeset: BTreeMap::new(),
			database: db,
			slot,
			slot_id,
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
		// Re-pin the retained slot allocation under a fresh id
		let (slot_id, commit, version) = pin_slot(&self.database, &self.slot);
		// Clear savepoint stack
		self.savepoint_stack.clear();
		// Clear or completely reset the allocated readset
		let threshold = self.reset_threshold;
		// Clear the transaction scanset
		self.scanset.clear();
		// Clear the transaction readset
		self.readset.pin().clear();
		// Clear the readset bloom filter
		self.readset_bloom.lock().clear();
		// Clear or completely reset the allocated writeset
		if self.writeset.len() > threshold {
			self.writeset = BTreeMap::new();
		} else {
			self.writeset.clear();
		}
		// Clear or completely reset the allocated savepoints
		if self.savepoint_stack.len() > threshold {
			self.savepoint_stack = Vec::new();
		}
		// Reset the transaction
		self.done = false;
		self.write = write;
		self.commit = commit;
		self.version = version;
		self.slot_id = slot_id;
	}

	/// Get the starting sequence number of this transaction
	pub const fn version(&self) -> u64 {
		self.version
	}

	/// Check if the transaction is closed
	pub const fn closed(&self) -> bool {
		self.done
	}

	/// Cancel the transaction and rollback any changes
	pub fn cancel(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Mark this transaction as done
		self.done = true;
		// Clear the transaction state
		if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			self.readset.pin().clear();
			self.readset_bloom.lock().clear();
		}
		self.scanset.clear();
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
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Create a new readset for the savepoint
		let readset = HashSet::new();
		// Store the readset for the savepoint
		{
			// Pin the readset for access
			let pin = readset.pin();
			// Clone the readset for the savepoint
			for key in &self.readset.pin() {
				pin.insert(key.clone());
			}
		};
		// Create a new scanset for the savepoint
		let scanset = SkipMap::new();
		// Clone the scanset for the savepoint
		for entry in &self.scanset {
			// Load the Arc from ArcSwap and clone it for the new savepoint
			let value = Arc::clone(&entry.value().load());
			scanset.insert(entry.key().clone(), ArcSwap::new(value));
		}
		// Create the savepoint state
		self.savepoint_stack.push(SavepointState {
			readset,
			scanset,
			writeset: self.writeset.clone(),
		});
		// Continue
		Ok(())
	}

	/// Rollback the transaction to the most recently set savepoint
	/// Pops the latest savepoint from the stack and restores transaction state
	pub fn rollback_to_savepoint(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Pop the most recent savepoint from the stack
		let savepoint = self.savepoint_stack.pop().ok_or(Error::NoSavepoint)?;
		// Pin the readset for access
		let readset = self.readset.pin();
		// Clear the readset
		readset.clear();
		// Clone the readset from the savepoint
		for key in &savepoint.readset.pin() {
			readset.insert(key.clone());
		}
		// Restore the scanset to the savepoint
		self.scanset.clear();
		// Clone the scanset from the savepoint
		for entry in &savepoint.scanset {
			// Load the Arc from ArcSwap and clone it for the restored scanset
			let value = Arc::clone(&entry.value().load());
			self.scanset.insert(entry.key().clone(), ArcSwap::new(value));
		}
		// Restore the other transaction state
		self.writeset = savepoint.writeset;
		// Continue
		Ok(())
	}

	/// Commit the transaction and store all changes
	pub fn commit(&mut self) -> Result<(), Error> {
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Mark this transaction as done
		self.done = true;
		// Return immediately if no modifications
		if self.writeset.is_empty() {
			// Clear the transaction state
			self.scanset.clear();
			if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
				self.readset.pin().clear();
				self.readset_bloom.lock().clear();
			}
			// Clear savepoint stack
			self.savepoint_stack.clear();
			// Continue
			return Ok(());
		}
		// Take ownership over the local modifications
		let writeset = Arc::new(std::mem::take(&mut self.writeset));
		// Collect the sorted writeset keys for conflict detection. The
		// commit queue entry retains only these keys: conflict detection
		// never reads values, and the entry outlives the merge queue's
		// value-bearing writeset by the whole cleanup window.
		let keys: Arc<[Bytes]> = writeset.keys().cloned().collect();
		// Build a bloom filter over the writeset keys
		let mut writeset_bloom = BloomFilter::new();
		for key in keys.iter() {
			writeset_bloom.insert(key);
		}
		// Insert this transaction into the commit queue
		let (commit_slot, commit_entry) = self.atomic_commit(Commit {
			keys,
			writeset_bloom,
			merge_version: AtomicU64::new(0),
		});
		// Unwind guard: if this transaction panics after publishing its
		// commit slot, mark the entry aborted and advance the completed
		// watermark, so an abandoned in-flight entry can never wedge the
		// contiguous prefix that every future reader's conflict window is
		// based on. The abort-on-conflict paths below also rely on this
		// guard for their watermark advance after removing the entry.
		let mut commit_guard = OnUnwind {
			armed: true,
			action: {
				let database = Arc::clone(&self.database);
				let entry = Arc::clone(&commit_entry);
				move || {
					entry.merge_version.store(COMMIT_ABORTED, Ordering::SeqCst);
					database.advance_commit_watermark();
				}
			},
		};
		// Check wether we should check reads conflicts on commit
		if self.mode >= IsolationLevel::SnapshotIsolation {
			// Retrieve all transactions committed since we began. The
			// window base is the completed watermark our snapshot was
			// taken from, which over-approximates concurrency: it can
			// include commits our version snapshot already sees.
			for tx in self.database.transaction_commit_queue.range(self.commit + 1..commit_slot) {
				// Skip aborted commits: their writes will never publish
				let merge_version = tx.value().merge_version.load(Ordering::SeqCst);
				if merge_version == COMMIT_ABORTED {
					continue;
				}
				// Skip commits whose merge version is visible in our
				// snapshot: a visible commit happened strictly before our
				// snapshot in version order, so it is not concurrent with
				// this transaction and first-committer-wins does not
				// apply — we read its effects rather than raced with it.
				if merge_version != 0 && merge_version <= self.version {
					continue;
				}
				// Check if a previous transaction conflicts against writes
				if !tx.value().is_disjoint_writeset_bloom(&commit_entry) {
					// Do NOT remove the entry here: the commit-queue
					// insertion-order watermark is advanced opportunistically
					// (see `Inner::try_advance_commit_prefix`) rather than
					// synchronously by this slot's own claim, so an entry
					// must stay visible until that watermark has confirmably
					// passed it — otherwise a concurrent advance could
					// mistake "removed early" for "never inserted" and stall
					// forever. The unwind guard (dropped below, on return)
					// marks the entry aborted and advances the completed
					// watermark; `cleanup_commit_queue` removes it later,
					// once it is safely below every reader's visibility.
					// Clear the transaction state
					self.scanset.clear();
					if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
						self.readset.pin().clear();
						self.readset_bloom.lock().clear();
					}
					self.writeset.clear();
					// Clear savepoint stack
					self.savepoint_stack.clear();
					// Return the error for this transaction
					return Err(Error::KeyWriteConflict);
				}
				// Check if we should check for conflicting read keys
				if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
					// Check if a previous transaction conflicts against reads
					if !tx
						.value()
						.is_disjoint_readset_bloom(&self.readset, &self.readset_bloom.lock())
					{
						// Do not remove the entry here — see the comment
						// on the writeset-conflict branch above. The
						// unwind guard marks it aborted on return.
						// Clear the transaction state
						self.scanset.clear();
						self.readset.pin().clear();
						self.readset_bloom.lock().clear();
						self.writeset.clear();
						// Clear savepoint stack
						self.savepoint_stack.clear();
						// Return the error for this transaction
						return Err(Error::KeyReadConflict);
					}
					// Check if the committed writeset may overlap any scan range
					let scan_overlap = if let Some(scan_front) = self.scanset.front() {
						// Get the upper bound of the last scan range
						if let Some(scan_back) = self.scanset.back() {
							let scan_max_end = Arc::clone(&scan_back.value().load());
							tx.value().may_overlap_range(scan_front.key(), &scan_max_end)
						} else {
							false
						}
					} else {
						false
					};
					// Only iterate writeset keys if ranges may overlap
					if scan_overlap {
						// A previous transaction has conflicts against scans
						for k in tx.value().keys.iter() {
							// Check if this key may be within a scan range
							if let Some(entry) = self.scanset.range::<Bytes, _>(..=k).next_back() {
								// Check if the range includes this key (load from ArcSwap)
								if **entry.value().load() > *k {
									// Do not remove the entry here — see the
									// comment on the writeset-conflict branch
									// above. The unwind guard marks it
									// aborted on return.
									// Clear the transaction state
									self.readset.pin().clear();
									self.readset_bloom.lock().clear();
									self.scanset.clear();
									self.writeset.clear();
									// Clear savepoint stack
									self.savepoint_stack.clear();
									// Log the error for debug purposes
									#[cfg(debug_assertions)]
									debug!(target: LOG_TARGET_CONFLICTS, "KeyReadConflict involving {:?}", k);
									// Return the error for this transaction
									return Err(Error::KeyReadConflict);
								}
							}
						}
					}
				}
			}
		}
		// Insert this transaction into the merge queue
		let (version, entry) = self.atomic_merge(Merge {
			writeset,
			applied: AtomicBool::new(false),
		});
		// Our merge version is now published, so record it on the
		// commit-queue entry, disarm the abort guard, and advance the
		// contiguous completed watermark: readers snapshotting the
		// watermark from here on may exclude this commit from their
		// conflict windows, because their version snapshot (loaded after
		// the watermark) is guaranteed to cover our merge version — in
		// the queue overlay or applied below.
		commit_entry.merge_version.store(version, Ordering::SeqCst);
		commit_guard.armed = false;
		self.database.advance_commit_watermark();
		// Unwind guard for the apply phase: a panic mid-apply leaves the
		// chains partially updated (a pre-existing atomicity limitation),
		// but must not wedge the in-order merge retirement — mark the
		// entry applied and advance so later merges can still retire.
		let mut merge_guard = OnUnwind {
			armed: true,
			action: {
				let database = Arc::clone(&self.database);
				let entry = Arc::clone(&entry);
				move || {
					entry.applied.store(true, Ordering::SeqCst);
					database.advance_merge_retirement();
				}
			},
		};
		// Compute the inline-GC watermark ONCE for this commit, after our
		// merge version is published and before the apply loop. The
		// pin-then-read slot protocol makes this safe where it once was
		// not (the PR #77 race): every current transaction is visible in
		// the slot scan or forces a skip, and every future transaction
		// snapshots at or above the clock bound loaded inside — while our
		// own excluded slot is pinned at our start version, so the
		// watermark can never sit above the version we push.
		let watermark = self.database.inline_gc_watermark(self.slot_id);
		// Keys whose chains could not be trimmed to a single live value,
		// collected here and tracked in one batch after the apply loop so
		// no hash-set work happens inside a chain write-lock critical
		// section. Empty in the steady state, so no allocation occurs.
		let mut tracked: Vec<Bytes> = Vec::new();
		// Apply each writeset entry, reclaiming superseded versions
		// inline while the chain write lock is already held.
		for (key, value) in entry.writeset.iter() {
			// Clone the value for insertion
			let value = value.clone();
			// Publish the new version into the datastore, guarding against a
			// concurrent gc sweep that may be unlinking this key: sweeps
			// call `Entry::remove` while holding the entry's write
			// lock, so once we hold that lock `is_removed()` tells us whether
			// the node is still live. `get_or_insert_with` (unlike `insert`)
			// never replaces a live node, so a concurrent writer's entry can
			// never be clobbered; if a sweep unlinked the node we landed
			// on, we retry and get a fresh one. The closure seeds the chain
			// with this version so a freshly-inserted node is never an empty,
			// immediately-gc-reclaimable node mid-insert.
			loop {
				let entry = self.database.datastore.get_or_insert_with(key.clone(), || {
					RwLock::new(Versions::from(Version {
						version,
						value: value.clone(),
					}))
				});
				let mut versions = entry.value().write();
				// A sweep unlinked this node between lookup and lock; retry
				// onto a fresh one rather than writing into a detached node.
				if entry.is_removed() {
					continue;
				}
				// A no-op when the node was just seeded with this version.
				versions.push(Version {
					version,
					value,
				});
				// Reclaim versions no live or future transaction can see,
				// under the write lock we already hold. When the chain
				// collapses entirely — the entry visible at the watermark
				// is a delete tombstone with nothing newer, e.g. our own
				// delete with no readers below — unlink the node while
				// still holding the lock, so a concurrent committer
				// observes `is_removed()` and re-inserts. When the chain
				// could NOT be trimmed to a single live value (a reader
				// watermark pins older versions, our newest entry is a
				// tombstone awaiting collapse, or the watermark scan was
				// skipped mid-registration), collect the key for tracking
				// so the background sweep revisits exactly this chain once
				// the pin clears — the sweep never scans the datastore.
				match watermark {
					Some(w) => {
						if versions.gc_older_versions(w) == 0 {
							entry.remove();
						} else if versions.needs_gc() {
							tracked.push(key.clone());
						}
					}
					None => {
						if versions.needs_gc() {
							tracked.push(key.clone());
						}
					}
				}
				break;
			}
		}
		// Track the collected keys in one batch, outside every chain
		// write lock and under a single map guard. Insert-after-unlock
		// is race-free against the sweep's remove-then-trim ordering:
		// the garbage these keys refer to was pushed before this insert,
		// so a sweep that untracks a key here either trims that garbage
		// in the same pass (and re-tracks it if a reader still pins it)
		// or ran entirely before it existed — in which case this insert
		// lands after the removal and the key stays tracked. At worst a
		// key is tracked for an already-terminal chain, which the next
		// sweep simply untracks.
		if !tracked.is_empty() {
			let candidates = self.database.gc_candidates.pin();
			for key in tracked {
				candidates.insert(key);
			}
		}
		// Append the transaction to the persistence layer. Clone the `Arc` out
		// first so the `persistence` read guard is not held across `append`,
		// which performs file I/O and may fsync.
		#[cfg(not(target_arch = "wasm32"))]
		let persistence = self.database.persistence.read().clone();
		#[cfg(not(target_arch = "wasm32"))]
		if let Some(p) = persistence {
			if let Err(e) = p.append(version, entry.writeset.as_ref()) {
				// Do NOT remove the entry here: the merge clock's
				// insertion-order advance is opportunistic (see
				// `Inner::try_advance_merge_clock`), so an entry must stay
				// visible until the clock has confirmably passed it —
				// removing it early would be indistinguishable from "never
				// inserted" to a concurrent advance and could stall it
				// forever. The unwind guard (dropped below, on return)
				// marks this entry applied and lets retirement remove it
				// once the clock naturally reaches this version — the
				// datastore chains already hold this data regardless (the
				// pre-existing applied-but-unpersisted limitation of this
				// error path).
				// Clear the transaction state
				self.scanset.clear();
				if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
					self.readset.pin().clear();
					self.readset_bloom.lock().clear();
				}
				self.writeset.clear();
				// Clear savepoint stack
				self.savepoint_stack.clear();
				// Return a persistence error
				return Err(Error::TxCommitNotPersisted(e));
			}
		}
		// Mark this merge as applied and retire the contiguous applied
		// prefix of the merge queue. Entries are NEVER removed directly
		// here: removal out of version order would let a reader find an
		// older in-flight version's surviving entry in the overlay while
		// a newer version already sits applied in the chain, returning a
		// stale value for a snapshot that should see the newer write.
		entry.applied.store(true, Ordering::SeqCst);
		merge_guard.armed = false;
		self.database.advance_merge_retirement();
		// Clear the transaction state
		self.scanset.clear();
		if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			self.readset.pin().clear();
			self.readset_bloom.lock().clear();
		}
		self.writeset.clear();
		// Clear savepoint stack
		self.savepoint_stack.clear();
		// Continue
		Ok(())
	}

	/// Check if a key exists in the database
	pub fn exists<K>(&self, key: K) -> Result<bool, Error>
	where
		K: IntoBytes,
	{
		// Get the key reference
		let lookup = key.as_slice();
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check the transaction type
		let res = if self.write {
			// The key exists in the writeset
			if self.writeset.contains_key(lookup) {
				true
			} else {
				// Check for the key in the tree
				// Fetch for the key from the datastore
				let res = self.exists_in_datastore(lookup, self.version);
				// Check whether we should track key reads
				if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
					self.readset_bloom.lock().insert(lookup);
					self.readset.pin().insert(key.into_bytes());
				}
				// Return the result
				res
			}
		} else {
			self.exists_in_datastore(lookup, self.version)
		};
		// Return result
		Ok(res)
	}

	/// Fetch a key from the database
	pub fn get<K>(&self, key: K) -> Result<Option<Bytes>, Error>
	where
		K: IntoBytes,
	{
		// Get the key reference
		let lookup = key.as_slice();
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check the transaction type
		let res = if self.write {
			// The key exists in the writeset
			if let Some(v) = self.writeset.get(lookup) {
				v.clone()
			} else {
				// Check for the key in the tree
				// Fetch for the key from the datastore
				let res = self.fetch_in_datastore(lookup, self.version);
				// Check whether we should track key reads
				if self.mode >= IsolationLevel::SerializableSnapshotIsolation {
					let guard = self.readset.pin();
					if !guard.contains(lookup) {
						self.readset_bloom.lock().insert(lookup);
						guard.insert(key.into_bytes());
					}
				}
				// Return the result
				res
			}
		} else {
			self.fetch_in_datastore(lookup, self.version)
		};
		// Return result
		Ok(res)
	}

	/// Fetch multiple keys from the database
	pub fn getm<K>(&self, keys: Vec<K>) -> Result<Vec<Option<Bytes>>, Error>
	where
		K: IntoBytes,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Prepare result vector with the same capacity
		let mut results = Vec::with_capacity(keys.len());
		// Check the transaction type
		if self.write {
			for key in keys {
				// Get the key reference
				let lookup = key.as_slice();
				// Check the writeset first
				// The key exists in the writeset
				let res = if let Some(v) = self.writeset.get(lookup) {
					v.clone()
				} else {
					// Check for the key in the tree
					// Fetch for the key from the datastore
					let res = self.fetch_in_datastore(lookup, self.version);
					// Check whether we should track key reads
					if self.mode >= IsolationLevel::SerializableSnapshotIsolation
						&& !self.readset.pin().contains(lookup)
					{
						self.readset_bloom.lock().insert(lookup);
						self.readset.pin().insert(key.into_bytes());
					}
					// Return the result
					res
				};
				results.push(res);
			}
		} else {
			for key in keys {
				// Get the key reference
				let lookup = key.as_slice();
				// Fetch from datastore
				let res = self.fetch_in_datastore(lookup, self.version);
				results.push(res);
			}
		}
		// Return results
		Ok(results)
	}

	/// Insert or update a key in the database
	pub fn set<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: IntoBytes,
		V: IntoBytes,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		self.writeset.insert(key.into_bytes(), Some(val.into_bytes()));
		// Return result
		Ok(())
	}

	/// Insert a key if it doesn't exist in the database
	pub fn put<K, V>(&mut self, key: K, val: V) -> Result<(), Error>
	where
		K: IntoBytes,
		V: IntoBytes,
	{
		// Get the key reference
		let lookup = key.as_slice();
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Set the key. The key must not already exist, either in the
		// writeset or in the tree.
		if self.writeset.contains_key(lookup) || self.exists_in_datastore(lookup, self.version) {
			return Err(Error::KeyAlreadyExists);
		}
		self.writeset.insert(key.into_bytes(), Some(val.into_bytes()));
		// Return result
		Ok(())
	}

	/// Insert a key if it matches a value
	pub fn putc<K, V, C>(&mut self, key: K, val: V, chk: Option<C>) -> Result<(), Error>
	where
		K: IntoBytes,
		V: IntoBytes,
		C: IntoBytes,
	{
		// Get the key reference
		let lookup = key.as_slice();
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Set the key
		match (chk.as_ref(), self.writeset.get(lookup)) {
			// The key exists in the writeset, check if it matches
			(Some(x), Some(Some(y))) if x.as_slice() == y.as_slice() => {
				self.writeset.insert(key.into_bytes(), Some(val.into_bytes()));
			}
			// The key does not exist in the writeset, check if it matches
			(None, Some(None)) => {
				self.writeset.insert(key.into_bytes(), Some(val.into_bytes()));
			}
			// The key exists in the writeset, but does not match
			(_, Some(_)) => return Err(Error::ValNotExpectedValue),
			// Check for the key in the tree
			_ => {
				if self.equals_in_datastore(lookup, chk, self.version) {
					self.writeset.insert(key.into_bytes(), Some(val.into_bytes()));
				} else {
					return Err(Error::ValNotExpectedValue);
				}
			}
		}
		// Return result
		Ok(())
	}

	/// Delete a key from the database
	pub fn del<K>(&mut self, key: K) -> Result<(), Error>
	where
		K: IntoBytes,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		self.writeset.insert(key.into_bytes(), None);
		// Return result
		Ok(())
	}

	/// Delete a key if it matches a value
	pub fn delc<K, C>(&mut self, key: K, chk: Option<C>) -> Result<(), Error>
	where
		K: IntoBytes,
		C: IntoBytes,
	{
		// Get the key reference
		let lookup = key.as_slice();
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Check to see if transaction is writable
		if !self.write {
			return Err(Error::TxNotWritable);
		}
		// Remove the key
		match (chk.as_ref(), self.writeset.get(lookup)) {
			// The key exists in the writeset, check if it matches
			(Some(x), Some(Some(y))) if x.as_slice() == y.as_slice() => {
				self.writeset.insert(key.into_bytes(), None);
			}
			// The key does not exist in the writeset, check if it matches
			(None, Some(None)) => {
				self.writeset.insert(key.into_bytes(), None);
			}
			// The key exists in the writeset, but does not match
			(_, Some(_)) => return Err(Error::ValNotExpectedValue),
			// Check for the key in the tree
			_ => {
				if self.equals_in_datastore(lookup, chk, self.version) {
					self.writeset.insert(key.into_bytes(), None);
				} else {
					return Err(Error::ValNotExpectedValue);
				}
			}
		}
		// Return result
		Ok(())
	}

	/// Retrieve a count of keys from the database
	pub fn total<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<usize, Error>
	where
		K: IntoBytes,
	{
		self.total_any(rng, skip, limit, Direction::Forward, self.version)
	}

	/// Retrieve a range of keys from the database
	pub fn keys<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<Bytes>, Error>
	where
		K: IntoBytes,
	{
		self.keys_any(rng, skip, limit, Direction::Forward, self.version)
	}

	/// Retrieve a range of keys from the database, in reverse order
	pub fn keys_reverse<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<Bytes>, Error>
	where
		K: IntoBytes,
	{
		self.keys_any(rng, skip, limit, Direction::Reverse, self.version)
	}

	/// Retrieve a range of keys and values from the database
	pub fn scan<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<(Bytes, Bytes)>, Error>
	where
		K: IntoBytes,
	{
		self.scan_any(rng, skip, limit, Direction::Forward, self.version)
	}

	/// Retrieve a range of keys and values from the database in reverse order
	pub fn scan_reverse<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<(Bytes, Bytes)>, Error>
	where
		K: IntoBytes,
	{
		self.scan_any(rng, skip, limit, Direction::Reverse, self.version)
	}

	/// Call a closure for each key-value pair in a range
	pub fn scan_for_each<K, F>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
		mut f: F,
	) -> Result<usize, Error>
	where
		K: IntoBytes,
		F: FnMut(&Bytes, &Bytes) -> bool,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Initialise the entry counter
		let mut count = 0;
		// Compute the range
		let beg = &rng.start.into_bytes();
		let end = &rng.end.into_bytes();
		// Calculate how many items to skip
		let mut skip = skip.unwrap_or_default();
		// Check whether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			self.track_scan_range(beg, end);
		}
		// Fast path: no merge queue entries and no transaction writes in this
		// range — walk the datastore directly.
		if self.database.transaction_merge_queue.is_empty()
			&& self.writeset.range::<Bytes, _>(beg..end).next().is_none()
		{
			let datastore_range = self
				.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone())));
			for entry in datastore_range {
				let value = match entry.value().try_read() {
					Some(g) => g.fetch_version(self.version),
					None => entry.value().read().fetch_version(self.version),
				};
				let Some(value) = value else {
					continue;
				};
				if skip > 0 {
					skip -= 1;
					continue;
				}
				count += 1;
				if !f(entry.key(), &value) {
					break;
				}
				if let Some(l) = limit {
					if count >= l {
						break;
					}
				}
			}
			return Ok(count);
		}
		// Lazy k-way merge over the merge-queue writesets.
		let join_iter = Box::new(MergeQueueIter::new(
			self.snapshot_merge_sources(self.version),
			beg.clone(),
			end.clone(),
			Direction::Forward,
		));
		// Create the 3-way merge iterator
		let iter = MergeIterator::new(
			self.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone()))),
			join_iter,
			self.writeset.range::<Bytes, _>(beg..end),
			Direction::Forward,
			self.version,
			skip,
		);
		// Process entries, calling the closure for each pair
		for (key, val) in iter {
			// Only include non-deleted entries
			if let Some(val) = &val {
				count += 1;
				// Call the closure and stop if it returns false
				if !f(&key, val) {
					break;
				}
				// Check limit
				if let Some(l) = limit {
					if count >= l {
						break;
					}
				}
			}
		}
		// Return the number of entries processed
		Ok(count)
	}

	/// Call a closure for each key in a range
	pub fn keys_for_each<K, F>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
		mut f: F,
	) -> Result<usize, Error>
	where
		K: IntoBytes,
		F: FnMut(&Bytes) -> bool,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Initialise the entry counter
		let mut count = 0;
		// Compute the range
		let beg = &rng.start.into_bytes();
		let end = &rng.end.into_bytes();
		// Calculate how many items to skip
		let mut skip = skip.unwrap_or_default();
		// Check whether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			self.track_scan_range(beg, end);
		}
		// Fast path: no merge queue entries and no transaction writes in this
		// range — walk the datastore directly.
		if self.database.transaction_merge_queue.is_empty()
			&& self.writeset.range::<Bytes, _>(beg..end).next().is_none()
		{
			let datastore_range = self
				.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone())));
			for entry in datastore_range {
				let exists = match entry.value().try_read() {
					Some(g) => g.exists_version(self.version),
					None => entry.value().read().exists_version(self.version),
				};
				if !exists {
					continue;
				}
				if skip > 0 {
					skip -= 1;
					continue;
				}
				count += 1;
				if !f(entry.key()) {
					break;
				}
				if let Some(l) = limit {
					if count >= l {
						break;
					}
				}
			}
			return Ok(count);
		}
		// Lazy k-way merge over the merge-queue writesets.
		let join_iter = Box::new(MergeQueueIter::new(
			self.snapshot_merge_sources(self.version),
			beg.clone(),
			end.clone(),
			Direction::Forward,
		));
		// Create the 3-way merge iterator
		let mut iter = MergeIterator::new(
			self.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone()))),
			join_iter,
			self.writeset.range::<Bytes, _>(beg..end),
			Direction::Forward,
			self.version,
			skip,
		);
		// Process entries, calling the closure for each key
		while let Some((key, exists)) = iter.next_key() {
			if exists {
				count += 1;
				// Call the closure and stop if it returns false
				if !f(&key) {
					break;
				}
				// Check limit
				if let Some(l) = limit {
					if count >= l {
						break;
					}
				}
			}
		}
		// Return the number of entries processed
		Ok(count)
	}

	/// Scan key-value pairs into a caller-provided Vec
	pub fn scan_into<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
		buf: &mut Vec<(Bytes, Bytes)>,
	) -> Result<(), Error>
	where
		K: IntoBytes,
	{
		// Clear the output buffer, reusing its capacity
		buf.clear();
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Compute the range
		let beg = &rng.start.into_bytes();
		let end = &rng.end.into_bytes();
		// Calculate how many items to skip
		let mut skip = skip.unwrap_or_default();
		// Check whether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			self.track_scan_range(beg, end);
		}
		// Fast path: no merge queue entries and no transaction writes in this
		// range — walk the datastore directly into the buffer.
		if self.database.transaction_merge_queue.is_empty()
			&& self.writeset.range::<Bytes, _>(beg..end).next().is_none()
		{
			let datastore_range = self
				.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone())));
			for entry in datastore_range {
				let value = match entry.value().try_read() {
					Some(g) => g.fetch_version(self.version),
					None => entry.value().read().fetch_version(self.version),
				};
				let Some(value) = value else {
					continue;
				};
				if skip > 0 {
					skip -= 1;
					continue;
				}
				buf.push((entry.key().clone(), value));
				if let Some(l) = limit {
					if buf.len() >= l {
						break;
					}
				}
			}
			return Ok(());
		}
		// Lazy k-way merge over the merge-queue writesets.
		let join_iter = Box::new(MergeQueueIter::new(
			self.snapshot_merge_sources(self.version),
			beg.clone(),
			end.clone(),
			Direction::Forward,
		));
		// Create the 3-way merge iterator
		let iter = MergeIterator::new(
			self.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone()))),
			join_iter,
			self.writeset.range::<Bytes, _>(beg..end),
			Direction::Forward,
			self.version,
			skip,
		);
		// Process entries into the output buffer
		for (key, val) in iter {
			// Only include non-deleted entries
			if let Some(val) = val {
				buf.push((key, val));
				// Check limit
				if let Some(l) = limit {
					if buf.len() >= l {
						break;
					}
				}
			}
		}
		// Continue
		Ok(())
	}

	/// Scan keys into a caller-provided Vec
	pub fn keys_into<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
		buf: &mut Vec<Bytes>,
	) -> Result<(), Error>
	where
		K: IntoBytes,
	{
		// Clear the output buffer, reusing its capacity
		buf.clear();
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Compute the range
		let beg = &rng.start.into_bytes();
		let end = &rng.end.into_bytes();
		// Calculate how many items to skip
		let mut skip = skip.unwrap_or_default();
		// Check whether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			self.track_scan_range(beg, end);
		}
		// Fast path: no merge queue entries and no transaction writes in this
		// range — walk the datastore directly into the buffer.
		if self.database.transaction_merge_queue.is_empty()
			&& self.writeset.range::<Bytes, _>(beg..end).next().is_none()
		{
			let datastore_range = self
				.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone())));
			for entry in datastore_range {
				let exists = match entry.value().try_read() {
					Some(g) => g.exists_version(self.version),
					None => entry.value().read().exists_version(self.version),
				};
				if !exists {
					continue;
				}
				if skip > 0 {
					skip -= 1;
					continue;
				}
				buf.push(entry.key().clone());
				if let Some(l) = limit {
					if buf.len() >= l {
						break;
					}
				}
			}
			return Ok(());
		}
		// Lazy k-way merge over the merge-queue writesets.
		let join_iter = Box::new(MergeQueueIter::new(
			self.snapshot_merge_sources(self.version),
			beg.clone(),
			end.clone(),
			Direction::Forward,
		));
		// Create the 3-way merge iterator
		let mut iter = MergeIterator::new(
			self.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone()))),
			join_iter,
			self.writeset.range::<Bytes, _>(beg..end),
			Direction::Forward,
			self.version,
			skip,
		);
		// Process entries into the output buffer
		while let Some((key, exists)) = iter.next_key() {
			if exists {
				buf.push(key);
				// Check limit
				if let Some(l) = limit {
					if buf.len() >= l {
						break;
					}
				}
			}
		}
		// Continue
		Ok(())
	}

	/// Helper to track a scan range in the scanset (optimized to minimize
	/// clones)
	#[inline(always)]
	fn track_scan_range(&self, beg: &Bytes, end: &Bytes) {
		// Add this range scan entry to the saved scans
		match self.scanset.range::<Bytes, _>(..=beg).next_back() {
			// There is no entry for this range scan
			None => {
				self.scanset.insert(beg.clone(), ArcSwap::from_pointee(end.clone()));
			}
			// The saved scan stops before this range
			Some(entry) if **entry.value().load() < *beg => {
				self.scanset.insert(beg.clone(), ArcSwap::from_pointee(end.clone()));
			}
			// The saved scan does not extend far enough - atomically update the end
			Some(entry) if **entry.value().load() < *end => {
				entry.value().store(Arc::new(end.clone()));
			}
			// This range scan is already covered
			_ => (),
		}
	}

	/// Snapshot the merge-queue writesets visible at `version` as a vector of
	/// `Arc<Merge>` ordered newest-first. Cheap — each element is an Arc bump.
	#[inline]
	fn snapshot_merge_sources(&self, version: u64) -> Vec<Arc<Merge>> {
		self.database
			.transaction_merge_queue
			.range(..=version)
			.rev()
			.map(|e| Arc::clone(e.value()))
			.collect()
	}

	/// Retrieve a count of keys from the database
	fn total_any<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
		direction: Direction,
		version: u64,
	) -> Result<usize, Error>
	where
		K: IntoBytes,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Prepare result count
		let mut res = 0;
		// Compute the range
		let beg = &rng.start.into_bytes();
		let end = &rng.end.into_bytes();
		// Calculate how many items to skip
		let mut skip = skip.unwrap_or_default();
		// Check wether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			// Track scans if scanning the latest version
			if version == self.version {
				self.track_scan_range(beg, end);
			}
		}
		// Fast path: when there are no in-flight committed transactions and no
		// uncommitted writes in this range, the merge iterator has nothing to
		// merge — read straight from the datastore range.
		if self.database.transaction_merge_queue.is_empty()
			&& self.writeset.range::<Bytes, _>(beg..end).next().is_none()
		{
			let datastore_range = self
				.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone())));
			macro_rules! consume_fast_path {
				($iter:expr) => {
					for entry in $iter {
						let exists = match entry.value().try_read() {
							Some(g) => g.exists_version(version),
							None => entry.value().read().exists_version(version),
						};
						if !exists {
							continue;
						}
						if skip > 0 {
							skip -= 1;
							continue;
						}
						res += 1;
						if let Some(l) = limit {
							if res >= l {
								break;
							}
						}
					}
				};
			}
			match direction {
				Direction::Forward => consume_fast_path!(datastore_range),
				Direction::Reverse => consume_fast_path!(datastore_range.rev()),
			}
			return Ok(res);
		}
		// Lazy k-way merge over the merge-queue writesets.
		let join_iter = Box::new(MergeQueueIter::new(
			self.snapshot_merge_sources(version),
			beg.clone(),
			end.clone(),
			direction,
		));
		// Create the 3-way merge iterator
		let mut iter = MergeIterator::new(
			self.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone()))),
			join_iter,
			self.writeset.range::<Bytes, _>(beg..end),
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
	fn keys_any<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
		direction: Direction,
		version: u64,
	) -> Result<Vec<Bytes>, Error>
	where
		K: IntoBytes,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Prepare result vector
		let mut res = match limit {
			Some(l) => Vec::with_capacity(l.min(10_000)),
			None => Vec::new(),
		};
		// Compute the range
		let beg = &rng.start.into_bytes();
		let end = &rng.end.into_bytes();
		// Calculate how many items to skip
		let mut skip = skip.unwrap_or_default();
		// Check wether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			// Track scans if scanning the latest version
			if version == self.version {
				self.track_scan_range(beg, end);
			}
		}
		// Fast path: no merge queue entries and no transaction writes in this
		// range — read keys straight from the datastore range.
		if self.database.transaction_merge_queue.is_empty()
			&& self.writeset.range::<Bytes, _>(beg..end).next().is_none()
		{
			let datastore_range = self
				.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone())));
			macro_rules! consume_fast_path {
				($iter:expr) => {
					for entry in $iter {
						let exists = match entry.value().try_read() {
							Some(g) => g.exists_version(version),
							None => entry.value().read().exists_version(version),
						};
						if !exists {
							continue;
						}
						if skip > 0 {
							skip -= 1;
							continue;
						}
						res.push(entry.key().clone());
						if let Some(l) = limit {
							if res.len() >= l {
								break;
							}
						}
					}
				};
			}
			match direction {
				Direction::Forward => consume_fast_path!(datastore_range),
				Direction::Reverse => consume_fast_path!(datastore_range.rev()),
			}
			return Ok(res);
		}
		// Lazy k-way merge over the merge-queue writesets.
		let join_iter = Box::new(MergeQueueIter::new(
			self.snapshot_merge_sources(version),
			beg.clone(),
			end.clone(),
			direction,
		));
		// Create the 3-way merge iterator
		let mut iter = MergeIterator::new(
			self.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone()))),
			join_iter,
			self.writeset.range::<Bytes, _>(beg..end),
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
	fn scan_any<K>(
		&self,
		rng: Range<K>,
		skip: Option<usize>,
		limit: Option<usize>,
		direction: Direction,
		version: u64,
	) -> Result<Vec<(Bytes, Bytes)>, Error>
	where
		K: IntoBytes,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Prepare result vector
		let mut res = match limit {
			Some(l) => Vec::with_capacity(l.min(10_000)),
			None => Vec::new(),
		};
		// Compute the range
		let beg = &rng.start.into_bytes();
		let end = &rng.end.into_bytes();
		// Calculate how many items to skip
		let mut skip = skip.unwrap_or_default();
		// Check wether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			// Track scans if scanning the latest version
			if version == self.version {
				self.track_scan_range(beg, end);
			}
		}
		// Fast path: no merge queue entries and no transaction writes in this
		// range — read key/value pairs straight from the datastore range.
		if self.database.transaction_merge_queue.is_empty()
			&& self.writeset.range::<Bytes, _>(beg..end).next().is_none()
		{
			let datastore_range = self
				.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone())));
			macro_rules! consume_fast_path {
				($iter:expr) => {
					for entry in $iter {
						let value = match entry.value().try_read() {
							Some(g) => g.fetch_version(version),
							None => entry.value().read().fetch_version(version),
						};
						let Some(value) = value else {
							continue;
						};
						if skip > 0 {
							skip -= 1;
							continue;
						}
						res.push((entry.key().clone(), value));
						if let Some(l) = limit {
							if res.len() >= l {
								break;
							}
						}
					}
				};
			}
			match direction {
				Direction::Forward => consume_fast_path!(datastore_range),
				Direction::Reverse => consume_fast_path!(datastore_range.rev()),
			}
			return Ok(res);
		}
		// Lazy k-way merge over the merge-queue writesets.
		let join_iter = Box::new(MergeQueueIter::new(
			self.snapshot_merge_sources(version),
			beg.clone(),
			end.clone(),
			direction,
		));
		// Create the 3-way merge iterator
		let iter = MergeIterator::new(
			self.database
				.datastore
				.range((Bound::Included(beg.clone()), Bound::Excluded(end.clone()))),
			join_iter,
			self.writeset.range::<Bytes, _>(beg..end),
			direction,
			version,
			skip,
		);
		// Process entries with skip and limit
		for (key, val) in iter {
			// Only include non-deleted entries
			if let Some(val) = val {
				res.push((key, val));
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

	// --------------------------------------------------
	// Cursor and Iterator API
	// --------------------------------------------------

	/// Create a cursor over a range of keys.
	pub fn cursor<K>(&self, rng: Range<K>) -> Result<Cursor<'_>, Error>
	where
		K: IntoBytes,
	{
		// Check to see if transaction is closed
		if self.done {
			return Err(Error::TxClosed);
		}
		// Compute the range
		let beg = rng.start.into_bytes();
		let end = rng.end.into_bytes();
		// Check whether we should track range scan reads
		if self.write && self.mode >= IsolationLevel::SerializableSnapshotIsolation {
			self.track_scan_range(&beg, &end);
		}
		// Create and return the cursor
		Ok(Cursor::new(&self.database, &self.writeset, beg, end, self.version))
	}

	/// Iterate over keys in a range.
	pub fn keys_iter<K>(&self, rng: Range<K>) -> Result<KeyIterator<'_>, Error>
	where
		K: IntoBytes,
	{
		let cursor = self.cursor(rng)?;
		Ok(KeyIterator::new(cursor))
	}

	/// Iterate over keys in a range, in reverse order.
	pub fn keys_iter_reverse<K>(&self, rng: Range<K>) -> Result<KeyIterator<'_>, Error>
	where
		K: IntoBytes,
	{
		let cursor = self.cursor(rng)?;
		Ok(KeyIterator::new_reverse(cursor))
	}

	/// Iterate over key-value pairs in a range.
	pub fn scan_iter<K>(&self, rng: Range<K>) -> Result<ScanIterator<'_>, Error>
	where
		K: IntoBytes,
	{
		let cursor = self.cursor(rng)?;
		Ok(ScanIterator::new(cursor))
	}

	/// Iterate over key-value pairs in a range, in reverse order.
	pub fn scan_iter_reverse<K>(&self, rng: Range<K>) -> Result<ScanIterator<'_>, Error>
	where
		K: IntoBytes,
	{
		let cursor = self.cursor(rng)?;
		Ok(ScanIterator::new_reverse(cursor))
	}

	// --------------------------------------------------
	// Datastore helpers
	// --------------------------------------------------

	/// Fetch a key if it exists in the datastore only
	#[inline(always)]
	fn fetch_in_datastore<K>(&self, key: K, version: u64) -> Option<Bytes>
	where
		K: IntoBytes,
	{
		// Get the key reference
		let key = key.as_slice();
		// Fetch the transaction merge queue range
		let iter = self.database.transaction_merge_queue.range(..=version);
		// Check the current entry iteration
		for entry in iter.rev() {
			if !entry.is_removed() {
				// Check for the key in the merge queue
				if let Some(v) = entry.value().writeset.get(key) {
					// Return the entry value
					return v.clone();
				}
			}
		}
		// Check the key in the datastore
		self.database.datastore.get(key).and_then(|e| match e.value().try_read() {
			Some(guard) => guard.fetch_version(version),
			None => e.value().read().fetch_version(version),
		})
	}

	/// Check if a key exists in the datastore only
	#[inline(always)]
	fn exists_in_datastore<K>(&self, key: K, version: u64) -> bool
	where
		K: IntoBytes,
	{
		// Get the key reference
		let key = key.as_slice();
		// Fetch the transaction merge queue range
		let iter = self.database.transaction_merge_queue.range(..=version);
		// Check the current entry iteration
		for entry in iter.rev() {
			if !entry.is_removed() {
				// Check for the key in the merge queue
				if let Some(v) = entry.value().writeset.get(key) {
					// Return whether the entry exists
					return v.is_some();
				}
			}
		}
		// Check the key in the datastore
		self.database
			.datastore
			.get(key)
			.map(|e| match e.value().try_read() {
				Some(guard) => guard.exists_version(version),
				None => e.value().read().exists_version(version),
			})
			.is_some_and(|v| v)
	}

	/// Check if a key equals a value in the datastore only
	#[inline(always)]
	fn equals_in_datastore<C, K>(&self, key: K, chk: Option<C>, version: u64) -> bool
	where
		K: IntoBytes,
		C: IntoBytes,
	{
		// Get the key reference
		let key = key.as_slice();
		// Fetch the transaction merge queue range
		let iter = self.database.transaction_merge_queue.range(..=version);
		// Check the current entry iteration
		for entry in iter.rev() {
			if !entry.is_removed() {
				// Check for the key in the merge queue
				if let Some(v) = entry.value().writeset.get(key) {
					// Return whether the entry matches
					return match (chk.as_ref(), v.as_ref()) {
						(Some(x), Some(y)) => x.as_slice() == y,
						(None, None) => true,
						_ => false,
					};
				}
			}
		}
		// Check the key in the datastore
		match (
			chk.as_ref(),
			self.database
				.datastore
				.get(key)
				.and_then(|e| match e.value().try_read() {
					Some(guard) => guard.fetch_version(version),
					None => e.value().read().fetch_version(version),
				})
				.as_ref(),
		) {
			(Some(x), Some(y)) => x.as_slice() == y,
			(None, None) => true,
			_ => false,
		}
	}

	/// Atomimcally inserts the transaction into the commit queue
	#[inline(always)]
	fn atomic_commit(&self, updates: Commit) -> (u64, Arc<Commit>) {
		// Store the number of spins
		let mut spins = 0;
		// Store the commit in an Arc
		let updates = Arc::new(updates);
		// Get the database transaction commit queue
		let queue = &self.database.transaction_commit_queue;
		// Claim a unique commit slot from the allocation counter. A dense
		// claim is always unique, so no collision retry is needed, unlike
		// probing a queue whose entries can be removed and (if probed by
		// value) re-claimed out from under a slow concurrent committer.
		let slot = self.database.transaction_queue_id.fetch_add(1, Ordering::SeqCst) + 1;
		// Insert the commit entry at the claimed slot
		let entry = queue.get_or_insert_with(slot, || Arc::clone(&updates));
		// Publish strictly in claim order: wait until every lower slot
		// has been published, then advance the inserted-prefix bound to
		// cover our own. This is NOT purely a courtesy to others — a
		// subsequent transaction on this same thread relies on being
		// able to see this commit's effects via the published bound
		// alone (it has no other source of "how recent" information),
		// so our own slot must be reflected before we return.
		loop {
			// Reload the current bound rather than retrying a fixed CAS:
			// a concurrent opportunistic helper (`try_advance_commit_prefix`,
			// used by the cleanup/GC paths) can advance this same bound on
			// our behalf once our entry is inserted, using only presence
			// in the queue as its criterion. If we kept retrying a stale
			// `compare_exchange(slot - 1, slot)` after that happens, the
			// bound can never return to `slot - 1` (it is monotonic), so
			// our CAS would never succeed again — a livelock. Treat the
			// bound already having reached our slot as success.
			let cur = self.database.transaction_commit_id.load(Ordering::SeqCst);
			if cur >= slot {
				return (slot, Arc::clone(entry.value()));
			}
			if cur == slot - 1
				&& self
					.database
					.transaction_commit_id
					.compare_exchange_weak(cur, slot, Ordering::SeqCst, Ordering::Acquire)
					.is_ok()
			{
				return (slot, Arc::clone(entry.value()));
			}
			// Ensure the thread backs off when under contention
			backoff(spins);
			// Increase the number loop spins we have attempted
			spins += 1;
		}
	}

	/// Atomimcally inserts the transaction into the merge queue
	#[inline(always)]
	fn atomic_merge(&self, updates: Merge) -> (u64, Arc<Merge>) {
		// Store the number of spins
		let mut spins = 0;
		// Store the commit in an Arc
		let updates = Arc::new(updates);
		// Get the database logical clock
		let oracle = Arc::clone(&self.database.oracle);
		// Get the database transaction merge queue
		let queue = &self.database.transaction_merge_queue;
		// Claim a unique merge version from the allocation counter. See
		// the equivalent comment in `atomic_commit`: a dense claim is
		// always unique, so no collision retry is needed.
		let version = oracle.alloc.fetch_add(1, Ordering::SeqCst) + 1;
		// Insert the merge entry at the claimed version
		let entry = queue.get_or_insert_with(version, || Arc::clone(&updates));
		// Publish strictly in claim order — see the equivalent comment
		// in `atomic_commit` for why this must cover our own version
		// before we return, and why we reload rather than retry a fixed
		// CAS (a concurrent `try_advance_merge_clock` call can publish
		// our version on our behalf).
		loop {
			let cur = oracle.timestamp.load(Ordering::SeqCst);
			if cur >= version {
				return (version, Arc::clone(entry.value()));
			}
			if cur == version - 1
				&& oracle
					.timestamp
					.compare_exchange_weak(cur, version, Ordering::SeqCst, Ordering::Acquire)
					.is_ok()
			{
				return (version, Arc::clone(entry.value()));
			}
			// Ensure the thread backs off when under contention
			backoff(spins);
			// Increase the number loop spins we have attempted
			spins += 1;
		}
	}
}

/// Progressive backoff strategy for contention in atomic queues.
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

/// Runs a completion action when dropped while still armed.
///
/// Used by the commit path to guarantee forward progress of the
/// contiguous commit and merge watermarks when a transaction unwinds
/// mid-commit: an abandoned in-flight queue entry would otherwise wedge
/// the prefix forever. Disarmed on the success path once the completion
/// has been performed explicitly.
struct OnUnwind<F: FnMut()> {
	/// The completion action to run on an armed drop
	action: F,
	/// Whether the action still needs to run
	armed: bool,
}

impl<F: FnMut()> Drop for OnUnwind<F> {
	fn drop(&mut self) {
		if self.armed {
			(self.action)();
		}
	}
}

#[cfg(test)]
#[allow(
	clippy::significant_drop_tightening,
	reason = "lock contention is irrelevant in single-threaded assertions"
)]
mod tests {

	use crate::err::Error;
	use crate::Database;
	use std::sync::Arc;
	use std::thread;

	#[test]
	fn mvcc_non_conflicting_keys_should_succeed() {
		let db = Database::new();
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
		let db = Database::new();
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
		let db = Database::new();
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
		let db = Database::new();
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
		let db = Database::new();
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
		let db = Database::new();
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
		let db = Database::new();
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
		let db = Database::new();
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
		let db = Database::new();
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
		let db = Database::new();

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

		// write-skew: read conflict when the read key was deleted by another
		// transaction
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
		let db = Database::new();

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
	fn new_db() -> Database {
		let db = Database::new();

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
			let txn3 = db.transaction(true);
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
			let txn3 = db.transaction(true);
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

		let txn1 = db.transaction(true);
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
		let db = Database::new();

		// Transaction 1: Add initial data and commit
		let mut txn1 = db.transaction(true);
		txn1.set("a", "1").unwrap();
		txn1.set("b", "2").unwrap();
		txn1.set("c", "3").unwrap();
		txn1.set("d", "4").unwrap();
		txn1.set("e", "5").unwrap();
		txn1.commit().unwrap();

		// Transaction 2: Should see committed data even if not merged yet
		let txn2 = db.transaction(false);

		// Test exclusive range
		let res = txn2.scan("b".."d", None, None).unwrap();
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].0.as_ref(), b"b");
		assert_eq!(res[1].0.as_ref(), b"c");

		// Test with skip
		let res = txn2.scan("a".."f", Some(1), None).unwrap();
		assert_eq!(res.len(), 4);
		assert_eq!(res[0].0.as_ref(), b"b");

		// Test with limit
		let res = txn2.scan("a".."f", None, Some(3)).unwrap();
		assert_eq!(res.len(), 3);
		assert_eq!(res[2].0.as_ref(), b"c");

		// Test with skip and limit
		let res = txn2.scan("a".."f", Some(2), Some(2)).unwrap();
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].0.as_ref(), b"c");
		assert_eq!(res[1].0.as_ref(), b"d");

		let res = txn2.scan_reverse("b".."e", None, None).unwrap();
		assert_eq!(res.len(), 3);
		assert_eq!(res[0].0.as_ref(), b"d");
		assert_eq!(res[1].0.as_ref(), b"c");
		assert_eq!(res[2].0.as_ref(), b"b");

		// Test empty range
		let res = txn2.scan("x".."z", None, None).unwrap();
		assert_eq!(res.len(), 0);

		// Test single item range
		let res = txn2.scan("c".."d", None, None).unwrap();
		assert_eq!(res.len(), 1);
		assert_eq!(res[0].0.as_ref(), b"c");
	}

	#[test]
	fn test_range_scan_with_merge_queue() {
		use std::sync::atomic::Ordering;

		// Create database
		let db = Database::new();

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
		assert_eq!(res[0].0.as_ref(), b"a"); // From merge queue
		assert_eq!(res[1].0.as_ref(), b"b"); // From current txn
		assert_eq!(res[2].0.as_ref(), b"c"); // From merge queue
		assert_eq!(res[3].0.as_ref(), b"d"); // From current txn
		assert_eq!(res[4].0.as_ref(), b"e"); // From merge queue
	}

	#[test]
	fn test_range_scan_with_deletions_in_merge_queue() {
		// Test that deletions in merge queue are handled correctly
		let db = Database::new();

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
		let txn3 = db.transaction(false);
		let res = txn3.scan("a".."e", None, None).unwrap();
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].0.as_ref(), b"a");
		assert_eq!(res[1].0.as_ref(), b"c");
	}

	#[test]
	fn test_range_scan_with_overwrites_in_merge_queue() {
		// Test that overwrites in merge queue take precedence
		let db = Database::new();

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
		let txn3 = db.transaction(false);
		let res = txn3.scan("a".."d", None, None).unwrap();
		assert_eq!(res.len(), 3);
		assert_eq!(res[0].0.as_ref(), b"a"); // Updated value
		assert_eq!(res[0].1.as_ref(), b"10");
		assert_eq!(res[1].0.as_ref(), b"b"); // Updated value
		assert_eq!(res[1].1.as_ref(), b"20");
		assert_eq!(res[2].0.as_ref(), b"c"); // Original value
	}

	#[test]
	fn test_range_scan_boundary_conditions() {
		// Test exact boundary conditions
		let db = Database::new();

		let mut txn1 = db.transaction(true);
		txn1.set("a", "1").unwrap();
		txn1.set("aa", "2").unwrap();
		txn1.set("ab", "3").unwrap();
		txn1.set("b", "4").unwrap();
		txn1.set("ba", "5").unwrap();
		txn1.commit().unwrap();

		let txn2 = db.transaction(false);

		// Range starting exactly at a key
		let res = txn2.scan("aa".."b", None, None).unwrap();
		assert_eq!(res.len(), 2);
		assert_eq!(res[0].0.as_ref(), b"aa");
		assert_eq!(res[0].1.as_ref(), b"2");
		assert_eq!(res[1].0.as_ref(), b"ab");
		assert_eq!(res[1].1.as_ref(), b"3");

		// Range ending exactly at a key (exclusive)
		let res = txn2.scan("a".."aa", None, None).unwrap();
		assert_eq!(res.len(), 1);
		assert_eq!(res[0].0.as_ref(), b"a");

		// Range between keys
		let res = txn2.scan("aaa".."az", None, None).unwrap();
		assert_eq!(res.len(), 1);
		assert_eq!(res[0].0.as_ref(), b"ab");
		assert_eq!(res[0].1.as_ref(), b"3");
	}

	#[test]
	fn test_range_scan_with_concurrent_transactions() {
		// Test scanning with uncommitted changes in current transaction
		let db = Database::new();

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
		assert_eq!(res[0].0.as_ref(), b"a"); // From merge queue
		assert_eq!(res[1].0.as_ref(), b"b"); // Local new
		assert_eq!(res[2].0.as_ref(), b"c"); // Local overwrite
		assert_eq!(res[2].1.as_ref(), b"30");
		assert_eq!(res[3].0.as_ref(), b"d"); // Local new
		                               // "e" is deleted locally, so not in
		                               // results
	}

	#[test]
	fn test_range_scan_keys_only() {
		// Test keys() method with merge queue
		let db = Database::new();

		let mut txn1 = db.transaction(true);
		txn1.set("a", "1").unwrap();
		txn1.set("b", "2").unwrap();
		txn1.set("c", "3").unwrap();
		txn1.commit().unwrap();

		let txn2 = db.transaction(false);
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
		let db = Database::new();

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

		let txn2 = db.transaction(false);

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
		let db = Arc::new(Database::default());
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
					tx.set(format!("key_{i}"), format!("value_{i}")).unwrap();

					// Force the commit to generate a transaction queue ID
					if let Ok(()) = tx.commit() {
						// Successfully committed
					} else {
						// May fail due to conflicts, which is fine for this
						// test
					}

					// Also test merge queue ID generation
					let mut tx2 = db.transaction(true);
					tx2.set(format!("merge_key_{i}"), format!("merge_value_{i}")).unwrap();
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
		let tx = db.transaction(false);
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
		let db = Arc::new(Database::default());
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
					let key = format!("thread_{thread_id}_key_{i}");
					let value = format!("thread_{thread_id}_value_{i}");

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
		let tx = db.transaction(false);
		let mut count = 0;
		// Use a very wide range to scan all keys
		for _ in tx.scan(String::new().."~".to_string(), None, None).unwrap() {
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
		let db = Database::new();

		// === BASIC SAVEPOINT FUNCTIONALITY ===
		let mut tx = db.transaction(true);
		tx.set("initial_key", "initial_value").unwrap();

		// Set a savepoint
		tx.set_savepoint().unwrap();

		// Make some changes after the savepoint
		tx.set("savepoint_key", "savepoint_value").unwrap();
		tx.set("another_key", "another_value").unwrap();

		// Verify the changes exist
		assert_eq!(tx.get("initial_key").unwrap().as_deref(), Some(b"initial_value" as &[u8]));
		assert_eq!(tx.get("savepoint_key").unwrap().as_deref(), Some(b"savepoint_value" as &[u8]));
		assert_eq!(tx.get("another_key").unwrap().as_deref(), Some(b"another_value" as &[u8]));

		// Rollback to savepoint
		tx.rollback_to_savepoint().unwrap();

		// Verify the rollback worked - changes after savepoint should be gone
		assert_eq!(tx.get("initial_key").unwrap().as_deref(), Some(b"initial_value" as &[u8]));
		assert_eq!(tx.get("savepoint_key").unwrap(), None);
		assert_eq!(tx.get("another_key").unwrap(), None);

		// Make new changes after rollback
		tx.set("new_key", "new_value").unwrap();
		assert_eq!(tx.get("new_key").unwrap().as_deref(), Some(b"new_value" as &[u8]));

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
		assert_eq!(tx.get("base").unwrap().as_deref(), Some(b"value" as &[u8]));
		assert_eq!(tx.get("level1_key1").unwrap().as_deref(), Some(b"modified_at_level2" as &[u8]));
		assert_eq!(tx.get("level1_key2").unwrap().as_deref(), Some(b"level1_value2" as &[u8]));
		assert_eq!(tx.get("level2_key1").unwrap().as_deref(), Some(b"level2_value1" as &[u8]));
		assert_eq!(tx.get("level3_key1").unwrap().as_deref(), Some(b"level3_value1" as &[u8]));

		// Rollback from level 3 to level 2
		tx.rollback_to_savepoint().unwrap();
		assert_eq!(tx.get("base").unwrap().as_deref(), Some(b"value" as &[u8]));
		assert_eq!(tx.get("level1_key1").unwrap().as_deref(), Some(b"modified_at_level2" as &[u8]));
		assert_eq!(tx.get("level1_key2").unwrap().as_deref(), Some(b"level1_value2" as &[u8]));
		assert_eq!(tx.get("level2_key1").unwrap().as_deref(), Some(b"level2_value1" as &[u8]));
		assert_eq!(tx.get("level3_key1").unwrap(), None); // Gone after rollback

		// Add new data at level 2
		tx.set("level2_new", "after_rollback").unwrap();

		// Rollback from level 2 to level 1
		tx.rollback_to_savepoint().unwrap();
		assert_eq!(tx.get("base").unwrap().as_deref(), Some(b"value" as &[u8]));
		assert_eq!(tx.get("level1_key1").unwrap().as_deref(), Some(b"level1_value1" as &[u8])); // Restored original
		assert_eq!(tx.get("level1_key2").unwrap().as_deref(), Some(b"level1_value2" as &[u8]));
		assert_eq!(tx.get("level2_key1").unwrap(), None); // Gone
		assert_eq!(tx.get("level2_new").unwrap(), None); // Gone
		assert_eq!(tx.get("level3_key1").unwrap(), None); // Still gone

		// Add final data and commit
		tx.set("final", "committed_data").unwrap();
		tx.commit().unwrap();

		// === VERIFY FINAL STATE ===
		let tx_verify = db.transaction(false);
		// From basic test
		assert_eq!(
			tx_verify.get("initial_key").unwrap().as_deref(),
			Some(b"initial_value" as &[u8])
		);
		assert_eq!(tx_verify.get("new_key").unwrap().as_deref(), Some(b"new_value" as &[u8]));
		assert_eq!(tx_verify.get("savepoint_key").unwrap(), None);
		assert_eq!(tx_verify.get("another_key").unwrap(), None);
		// From nested test
		assert_eq!(tx_verify.get("base").unwrap().as_deref(), Some(b"value" as &[u8]));
		assert_eq!(
			tx_verify.get("level1_key1").unwrap().as_deref(),
			Some(b"level1_value1" as &[u8])
		);
		assert_eq!(
			tx_verify.get("level1_key2").unwrap().as_deref(),
			Some(b"level1_value2" as &[u8])
		);
		assert_eq!(tx_verify.get("final").unwrap().as_deref(), Some(b"committed_data" as &[u8]));
		assert_eq!(tx_verify.get("level2_key1").unwrap(), None);
		assert_eq!(tx_verify.get("level2_new").unwrap(), None);
		assert_eq!(tx_verify.get("level3_key1").unwrap(), None);
	}

	#[test]
	fn test_savepoint_errors() {
		let db = Database::new();

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
		let db_stack = Database::new();
		let mut tx_stack = db_stack.transaction(true);

		// Set multiple nested savepoints
		for i in 0..3 {
			let key = format!("key{i}");
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
	fn test_savepoint_stack_cleared_on_transaction_reuse() {
		// Verify that savepoint stacks are properly cleared when transactions
		// are returned to the pool and reused for new operations.

		let db = Database::new();

		// Transaction 1: Create savepoints and commit
		let mut tx1 = db.transaction(true);
		tx1.set("key1", "value1").unwrap();
		tx1.set_savepoint().unwrap();
		tx1.set("key2", "value2").unwrap();
		tx1.set_savepoint().unwrap();
		tx1.set("key3", "value3").unwrap();

		// Verify savepoints exist
		assert!(!tx1.inner.as_ref().unwrap().savepoint_stack.is_empty());

		// Commit the transaction (returns to pool)
		tx1.commit().unwrap();

		// Transaction 2: Get a new transaction (likely reuses tx1's pooled object)
		let mut tx2 = db.transaction(true);

		// CRITICAL: The savepoint stack should be empty for the new transaction
		assert!(
			tx2.inner.as_ref().unwrap().savepoint_stack.is_empty(),
			"Savepoint stack should be cleared when transaction is reused from pool"
		);

		// Attempting to rollback should fail with NoSavepoint
		let result = tx2.rollback_to_savepoint();
		assert!(
			matches!(result, Err(Error::NoSavepoint)),
			"Should get NoSavepoint error on fresh transaction, got: {result:?}"
		);

		tx2.cancel().unwrap();

		// Transaction 3: Test with cancel instead of commit
		let mut tx3 = db.transaction(true);
		tx3.set("key4", "value4").unwrap();
		tx3.set_savepoint().unwrap();
		tx3.set_savepoint().unwrap();
		tx3.set_savepoint().unwrap();

		// Verify savepoints exist
		assert_eq!(tx3.inner.as_ref().unwrap().savepoint_stack.len(), 3);

		// Cancel the transaction (returns to pool)
		tx3.cancel().unwrap();

		// Transaction 4: Get another transaction
		let mut tx4 = db.transaction(true);

		// Savepoint stack should be empty
		assert!(
			tx4.inner.as_ref().unwrap().savepoint_stack.is_empty(),
			"Savepoint stack should be cleared after cancel"
		);

		// Readset and scanset should also be cleared
		assert!(
			tx4.inner.as_ref().unwrap().readset.pin().is_empty(),
			"Readset should be cleared after cancel"
		);
		assert!(
			tx4.inner.as_ref().unwrap().scanset.is_empty(),
			"Scanset should be cleared after cancel"
		);

		tx4.cancel().unwrap();
	}

	#[test]
	fn test_savepoints_with_scans_and_writes() {
		// Verify that savepoints correctly interact with scans and subsequent writes.
		// This pattern is common in SurrealDB queries with IF-ELSE, SELECT, and UPSERT.

		let db = Database::new();

		// Setup initial data
		let mut tx_setup = db.transaction(true);
		tx_setup.set("person:test", "initial").unwrap();
		tx_setup.set("person:other", "other_value").unwrap();
		tx_setup.commit().unwrap();

		// Transaction that scans, sets savepoint, then writes
		let mut tx1 = db.transaction(true);

		// Scan first (like SELECT count() - scans the whole range)
		let result = tx1.scan("person:".."person;", None, None).unwrap();
		assert_eq!(result.len(), 2);

		// Set savepoint (SurrealDB might use these internally)
		tx1.set_savepoint().unwrap();

		// Concurrent transaction modifies data in scanned range
		// but a DIFFERENT key than what tx1 will write
		let mut tx2 = db.transaction(true);
		tx2.set("person:other", "modified").unwrap();
		tx2.commit().unwrap();

		// Now write to a different key (like UPSERT based on the scan result)
		tx1.set("person:test", "updated").unwrap();

		// Commit should detect the SCAN conflict (tx2 wrote to a key tx1 scanned)
		let result = tx1.commit();
		assert!(
			matches!(result, Err(Error::KeyReadConflict)),
			"Should detect scan conflict even with savepoint, got: {result:?}"
		);
	}

	#[test]
	fn test_savepoint_rollback_preserves_earlier_scans() {
		// Verify that rolling back a savepoint doesn't lose scans from before the
		// savepoint

		let db = Database::new();

		// Setup
		let mut tx_setup = db.transaction(true);
		tx_setup.set("key1", "value1").unwrap();
		tx_setup.set("key2", "value2").unwrap();
		tx_setup.commit().unwrap();

		let mut tx = db.transaction(true);

		// Scan before savepoint
		let _ = tx.scan("key0".."key5", None, None).unwrap();

		// Verify scanset is populated
		assert!(!tx.inner.as_ref().unwrap().scanset.is_empty());
		let scanset_before_len = tx.inner.as_ref().unwrap().scanset.len();

		// Set savepoint
		tx.set_savepoint().unwrap();

		// Another scan after savepoint
		let _ = tx.scan("key5".."key9", None, None).unwrap();

		// Scanset should now have both ranges
		assert!(tx.inner.as_ref().unwrap().scanset.len() >= scanset_before_len);

		// Rollback to savepoint
		tx.rollback_to_savepoint().unwrap();

		// First scan should still be there (check length matches)
		assert_eq!(
			tx.inner.as_ref().unwrap().scanset.len(),
			scanset_before_len,
			"Scanset should be restored to state before savepoint"
		);

		// Add a write
		tx.set("key1", "updated").unwrap();

		// Concurrent write in the original scan range
		let mut tx2 = db.transaction(true);
		tx2.set("key2", "concurrent").unwrap();
		tx2.commit().unwrap();

		// Should still detect conflict from the preserved scan
		let result = tx.commit();
		assert!(
			matches!(result, Err(Error::KeyReadConflict)),
			"Should detect conflict from scan before savepoint, got: {result:?}"
		);
	}

	#[test]
	fn test_gc_does_not_remove_active_versions() {
		// Test for garbage collection regression issue where versions
		// were being removed too aggressively
		// Create a database with GC disabled (automatic version cleanup enabled)
		let db = Database::new();

		// Insert 10,000 keys one-by-one
		for i in 0..10_000 {
			let key = format!("key_{i:08}").into_bytes();
			let value = format!("value_{i:08}").into_bytes();

			let mut tx = db.transaction(true);
			tx.put(key, value).unwrap();
			tx.commit().unwrap();
		}

		// Read each key one-by-one
		for i in 0..10_000 {
			let key = format!("key_{i:08}").into_bytes();
			let expected_value = format!("value_{i:08}").into_bytes();

			let mut tx = db.transaction(false);
			let result = tx.get(key.clone());
			assert!(result.is_ok(), "Failed to get key at index {i}: {result:?}");
			let value = result.unwrap();
			assert!(
				value.is_some(),
				"Key at index {i} was not found (version was garbage collected too early)"
			);
			assert_eq!(value.unwrap(), expected_value, "Value mismatch at index {i}");
			tx.cancel().unwrap();
		}
	}

	#[test]
	fn test_gc_concurrent_readers() {
		use std::sync::Arc;
		use std::thread;

		// Create a database
		let db = Database::new();

		// Insert initial data
		{
			let mut tx = db.transaction(true);
			for i in 0..1000 {
				let key = format!("key_{i:08}").into_bytes();
				let value = format!("value_{i:08}").into_bytes();
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
					let key = format!("key_{i:08}").into_bytes();
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
	fn test_cleanup_trims_commit_queue_when_idle() {
		use crate::DatabaseOptions;

		// Disable all background workers so the commit queue is only
		// ever trimmed by the manual cleanup calls in this test
		let db = Database::new_with_options(DatabaseOptions::default().with_all_workers_disabled());

		// Commit a number of transactions
		for i in 0..10 {
			let mut tx = db.transaction(true);
			tx.set(format!("key_{i}"), "value").unwrap();
			tx.commit().unwrap();
		}

		// Every commit is still queued for conflict detection
		assert_eq!(db.transaction_commit_queue.len(), 10);

		// With no transaction registered, cleanup trims everything below
		// the current commit id, leaving only the most recent entry
		db.run_cleanup();
		assert_eq!(db.transaction_commit_queue.len(), 1);

		// The datastore itself is untouched by the queue trim. Scope the
		// reader so it is dropped: counters are released on Drop, not on
		// cancel, and a live reader pins the cleanup watermark below.
		{
			let mut tx = db.transaction(false);
			for i in 0..10 {
				assert!(tx.exists(format!("key_{i}")).unwrap());
			}
			tx.cancel().unwrap();
		}

		// An active transaction pins the queue at its own commit
		// snapshot, so entries in its conflict window must survive
		let pin = db.transaction(false);
		for i in 0..5 {
			let mut tx = db.transaction(true);
			tx.set(format!("extra_{i}"), "value").unwrap();
			tx.commit().unwrap();
		}
		assert_eq!(db.transaction_commit_queue.len(), 6);
		db.run_cleanup();
		assert_eq!(db.transaction_commit_queue.len(), 6);

		// Once the pinning transaction is dropped, cleanup trims fully
		drop(pin);
		db.run_cleanup();
		assert_eq!(db.transaction_commit_queue.len(), 1);
	}

	#[test]
	fn test_concurrent_write_read_merge_queue_race() {
		// This test specifically targets the race condition where readers check
		// is_removed() on merge queue entries and miss data that's being merged.
		// The race manifests under high concurrency when:
		// 1. Writer commits and merges data into datastore
		// 2. Writer removes entry from merge queue
		// 3. Reader checks merge queue (sees removed), checks datastore (doesn't see
		//    data yet)
		//
		// This was fixed by removing the is_removed() checks and always checking all
		// merge queue entries, since crossbeam-skiplist guarantees they remain
		// accessible.

		let db = Database::new();
		let db = Arc::new(db);

		// Run multiple iterations to increase chance of hitting the race
		for iteration in 0..100 {
			let key = format!("race_test_key_{iteration}").into_bytes();
			let value = format!("race_test_value_{iteration}").into_bytes();

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
				"Key must be visible after writer completes (iteration {iteration})"
			);
			assert_eq!(
				result.unwrap(),
				value,
				"Value mismatch after writer completes (iteration {iteration})"
			);
		}
	}

	#[test]
	fn test_high_concurrency_merge_queue_visibility() {
		// Simulate the crud-bench scenario: many concurrent writers and readers
		// This stresses the merge queue under high contention

		let db = Database::new();
		let db = Arc::new(db);

		let num_threads = std::thread::available_parallelism().map_or(8, std::num::NonZero::get);
		let operations_per_thread = 50;

		let mut handles = vec![];

		// Spawn writer threads
		for thread_id in 0..num_threads {
			let db = Arc::clone(&db);
			let handle = thread::spawn(move || {
				for op_id in 0..operations_per_thread {
					let key = format!("thread_{thread_id}_key_{op_id}").into_bytes();
					let value = format!("thread_{thread_id}_value_{op_id}").into_bytes();

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
						let key = format!("thread_{writer_id}_key_{op_id}").into_bytes();
						let expected_value =
							format!("thread_{writer_id}_value_{op_id}").into_bytes();

						// Try reading multiple times
						for _ in 0..5 {
							let mut tx = db.transaction(false);
							if let Some(value) = tx.get(key.clone()).unwrap() {
								// If we see a value, it must be correct
								assert_eq!(
									value, expected_value,
									"Reader {reader_id} saw wrong value for writer {writer_id} op {op_id}"
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
				let key = format!("thread_{thread_id}_key_{op_id}").into_bytes();
				let expected_value = format!("thread_{thread_id}_value_{op_id}").into_bytes();

				let mut tx = db.transaction(false);
				let result = tx.get(key.clone()).unwrap();
				tx.cancel().unwrap();

				assert!(
					result.is_some(),
					"Key from thread {thread_id} op {op_id} not found after all threads complete"
				);
				assert_eq!(
					result.unwrap(),
					expected_value,
					"Wrong value for thread {thread_id} op {op_id} after all threads complete"
				);
			}
		}
	}

	#[test]
	fn test_writeset_checked_before_datastore() {
		// Test that put, putc, and delc correctly check the writeset
		// before checking the datastore
		let db = Database::new();

		// === Test 1: put() checks writeset ===
		{
			let mut tx = db.transaction(true);

			// Set a key in the writeset (not yet committed)
			tx.set("key1", "value1").unwrap();

			// Try to put the same key - should fail even though it's not in datastore yet
			let result = tx.put("key1", "value2");
			assert!(
				matches!(result, Err(Error::KeyAlreadyExists)),
				"put() should check writeset and fail when key exists there"
			);

			tx.cancel().unwrap();
		}

		// === Test 2: put() allows insert when key only exists in datastore ===
		{
			let mut tx1 = db.transaction(true);
			tx1.put("key2", "initial").unwrap();
			tx1.commit().unwrap();

			let mut tx2 = db.transaction(true);
			let result = tx2.put("key2", "should_fail");
			assert!(
				matches!(result, Err(Error::KeyAlreadyExists)),
				"put() should fail when key exists in datastore"
			);
		}

		// === Test 3: putc() checks writeset with matching value ===
		{
			let mut tx = db.transaction(true);

			// Set a key in the writeset
			tx.set("key3", "value3").unwrap();

			// putc with matching value should succeed
			tx.putc("key3", "new_value", Some("value3")).unwrap();
			assert_eq!(tx.get("key3").unwrap().as_deref(), Some(b"new_value" as &[u8]));

			tx.cancel().unwrap();
		}

		// === Test 4: putc() checks writeset with non-matching value ===
		{
			let mut tx = db.transaction(true);

			// Set a key in the writeset
			tx.set("key4", "value4").unwrap();

			// putc with non-matching value should fail
			let result = tx.putc("key4", "new_value", Some("wrong_value"));
			assert!(
				matches!(result, Err(Error::ValNotExpectedValue)),
				"putc() should check writeset and fail when value doesn't match"
			);

			tx.cancel().unwrap();
		}

		// === Test 5: putc() with None checks writeset for deleted key ===
		{
			let mut tx = db.transaction(true);

			// Delete a key in the writeset
			tx.del("key5").unwrap();

			// putc with None (expecting non-existent) should succeed on deleted key
			tx.putc("key5", "resurrected", None::<&str>).unwrap();
			assert_eq!(tx.get("key5").unwrap().as_deref(), Some(b"resurrected" as &[u8]));

			tx.cancel().unwrap();
		}

		// === Test 6: delc() checks writeset with matching value ===
		{
			let mut tx = db.transaction(true);

			// Set a key in the writeset
			tx.set("key6", "value6").unwrap();

			// delc with matching value should succeed
			tx.delc("key6", Some("value6")).unwrap();
			assert_eq!(tx.get("key6").unwrap(), None);

			tx.cancel().unwrap();
		}

		// === Test 7: delc() checks writeset with non-matching value ===
		{
			let mut tx = db.transaction(true);

			// Set a key in the writeset
			tx.set("key7", "value7").unwrap();

			// delc with non-matching value should fail
			let result = tx.delc("key7", Some("wrong_value"));
			assert!(
				matches!(result, Err(Error::ValNotExpectedValue)),
				"delc() should check writeset and fail when value doesn't match"
			);

			tx.cancel().unwrap();
		}

		// === Test 8: delc() with None checks writeset for non-existent key ===
		{
			let mut tx = db.transaction(true);

			// Delete a key in the writeset (making it non-existent)
			tx.del("key8").unwrap();

			// delc with None (expecting non-existent) should succeed
			tx.delc("key8", None::<&str>).unwrap();
			assert_eq!(tx.get("key8").unwrap(), None);

			tx.cancel().unwrap();
		}

		// === Test 9: Complex scenario with multiple operations ===
		{
			let mut tx = db.transaction(true);

			// Set a key
			tx.set("complex", "v1").unwrap();

			// Update it
			tx.set("complex", "v2").unwrap();

			// Try to put (should fail - key exists in writeset)
			assert!(matches!(tx.put("complex", "v3"), Err(Error::KeyAlreadyExists)));

			// putc with correct value should work
			tx.putc("complex", "v3", Some("v2")).unwrap();
			assert_eq!(tx.get("complex").unwrap().as_deref(), Some(b"v3" as &[u8]));

			// delc with correct value should work
			tx.delc("complex", Some("v3")).unwrap();
			assert_eq!(tx.get("complex").unwrap(), None);

			// Now putc with None should work (key deleted)
			tx.putc("complex", "v4", None::<&str>).unwrap();
			assert_eq!(tx.get("complex").unwrap().as_deref(), Some(b"v4" as &[u8]));

			tx.cancel().unwrap();
		}
	}

	#[test]
	fn test_getm_basic() {
		let db = Database::new();
		// Set up some initial data
		let mut tx = db.transaction(true);
		tx.set("key1", "value1").unwrap();
		tx.set("key2", "value2").unwrap();
		tx.set("key3", "value3").unwrap();
		tx.commit().unwrap();

		// Test getm with all existing keys
		let tx = db.transaction(false);
		let keys = vec!["key1", "key2", "key3"];
		let results = tx.getm(keys).unwrap();
		assert_eq!(results.len(), 3);
		assert_eq!(results[0].as_deref(), Some(b"value1" as &[u8]));
		assert_eq!(results[1].as_deref(), Some(b"value2" as &[u8]));
		assert_eq!(results[2].as_deref(), Some(b"value3" as &[u8]));
	}

	#[test]
	fn test_getm_with_missing_keys() {
		let db = Database::new();
		// Set up some initial data
		let mut tx = db.transaction(true);
		tx.set("key1", "value1").unwrap();
		tx.set("key3", "value3").unwrap();
		tx.commit().unwrap();

		// Test getm with some missing keys
		let tx = db.transaction(false);
		let keys = vec!["key1", "key2", "key3", "key4"];
		let results = tx.getm(keys).unwrap();
		assert_eq!(results.len(), 4);
		assert_eq!(results[0].as_deref(), Some(b"value1" as &[u8]));
		assert_eq!(results[1], None); // key2 doesn't exist
		assert_eq!(results[2].as_deref(), Some(b"value3" as &[u8]));
		assert_eq!(results[3], None); // key4 doesn't exist
	}

	#[test]
	fn test_getm_empty_vector() {
		let db = Database::new();
		let tx = db.transaction(false);
		let keys: Vec<&str> = vec![];
		let results = tx.getm(keys).unwrap();
		assert_eq!(results.len(), 0);
	}

	#[test]
	fn test_getm_with_writeset() {
		let db = Database::new();
		// Set up some initial data
		let mut tx = db.transaction(true);
		tx.set("key1", "value1").unwrap();
		tx.set("key2", "value2").unwrap();
		tx.commit().unwrap();

		// Test getm with writeset modifications
		let mut tx = db.transaction(true);
		tx.set("key2", "updated2").unwrap(); // Update existing
		tx.set("key3", "value3").unwrap(); // New key
		tx.del("key1").unwrap(); // Delete existing

		let keys = vec!["key1", "key2", "key3", "key4"];
		let results = tx.getm(keys).unwrap();
		assert_eq!(results.len(), 4);
		assert_eq!(results[0], None); // key1 was deleted
		assert_eq!(results[1].as_deref(), Some(b"updated2" as &[u8])); // key2 was updated
		assert_eq!(results[2].as_deref(), Some(b"value3" as &[u8])); // key3 is new
		assert_eq!(results[3], None); // key4 doesn't exist
	}

	#[test]
	fn test_getm_maintains_order() {
		let db = Database::new();
		let mut tx = db.transaction(true);
		tx.set("a", "1").unwrap();
		tx.set("b", "2").unwrap();
		tx.set("c", "3").unwrap();
		tx.commit().unwrap();

		// Test that results maintain the order of input keys
		let tx = db.transaction(false);
		let keys = vec!["c", "a", "b"];
		let results = tx.getm(keys).unwrap();
		assert_eq!(results[0].as_deref(), Some(b"3" as &[u8])); // c
		assert_eq!(results[1].as_deref(), Some(b"1" as &[u8])); // a
		assert_eq!(results[2].as_deref(), Some(b"2" as &[u8])); // b
	}

	#[test]
	fn test_getm_duplicate_keys() {
		let db = Database::new();
		let mut tx = db.transaction(true);
		tx.set("key1", "value1").unwrap();
		tx.commit().unwrap();

		// Test with duplicate keys in input
		let tx = db.transaction(false);
		let keys = vec!["key1", "key1", "key1"];
		let results = tx.getm(keys).unwrap();
		assert_eq!(results.len(), 3);
		assert_eq!(results[0].as_deref(), Some(b"value1" as &[u8]));
		assert_eq!(results[1].as_deref(), Some(b"value1" as &[u8]));
		assert_eq!(results[2].as_deref(), Some(b"value1" as &[u8]));
	}

	#[test]
	fn test_getm_closed_transaction() {
		let db = Database::new();
		let mut tx = db.transaction(false);
		tx.cancel().unwrap();

		let keys = vec!["key1"];
		let result = tx.getm(keys);
		assert!(matches!(result, Err(Error::TxClosed)));
	}

	#[test]
	fn test_getm_ssi_read_tracking() {
		let db = Database::new();

		// Test that getm tracks reads for SSI
		let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
		let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

		// tx1 reads keys using getm (both don't exist yet)
		let keys = vec!["key1", "key2"];
		let results = tx1.getm(keys).unwrap();
		assert_eq!(results[0], None);
		assert_eq!(results[1], None);

		// tx1 writes to one of the keys it read
		tx1.set("key1", "value1").unwrap();
		assert!(tx1.commit().is_ok());

		// tx2 reads the same key (sees None due to snapshot isolation)
		assert!(tx2.get("key1").unwrap().is_none());
		// tx2 writes to a different key
		tx2.set("key2", "value2").unwrap();
		// tx2 should fail to commit due to read-write conflict on key1
		assert!(tx2.commit().is_err());
	}

	#[test]
	fn test_getm_concurrent_reads() {
		let db = Arc::new(Database::new());

		// Set up initial data
		let mut tx = db.transaction(true);
		for i in 0..100 {
			tx.set(format!("key{i}"), format!("value{i}")).unwrap();
		}
		tx.commit().unwrap();

		// Spawn multiple threads doing concurrent getm operations
		let mut handles = vec![];
		for thread_id in 0..10 {
			let db_clone = Arc::clone(&db);
			let handle = thread::spawn(move || {
				let tx = db_clone.transaction(false);
				let keys: Vec<String> = (0..100).map(|i| format!("key{i}")).collect();
				let results = tx.getm(keys).unwrap();

				// Verify all results
				for (i, result) in results.iter().enumerate() {
					assert_eq!(
						result.as_deref(),
						Some(format!("value{i}").as_bytes()),
						"Thread {thread_id} failed at index {i}"
					);
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
	fn snapshot_isolation_reader_registration_race_does_not_lose_versions() {
		// Historic regression test for a snapshot-isolation registration
		// race: a transaction loaded its snapshot version BEFORE becoming
		// visible to sweepers, so a concurrently computed `cleanup_ts`
		// could miss the registering reader and sweep the versions its
		// snapshot needed — the reader then observed `None` for a key
		// that was committed before its snapshot.
		//
		// The pin-then-read slot protocol closes this structurally: a
		// transaction publishes its pinning slot before loading the
		// clock, so every sweep either sees the slot (as a sentinel or a
		// bounding value) or precedes the snapshot load in the SeqCst
		// total order, in which case the snapshot is at or above the
		// sweep's clock bound — and reclamation always retains the entry
		// visible at its watermark. This test remains as the empirical
		// validator for that protocol.
		//
		// In the SurrealDB embedded `memory` backend the original bug
		// surfaced as a flake on tests that poll `INFO FOR INDEX` while a
		// concurrent index builder is committing rapid heartbeat updates
		// to the build-state key.
		use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
		use std::sync::Arc;
		use std::thread;
		use std::time::{Duration, Instant};

		let db = Arc::new(Database::new());

		// Seed the key. Every subsequent reader has a committed value at
		// or before its snapshot timestamp, so `get` must return `Some(_)`
		// on every iteration.
		{
			let mut tx = db.transaction(true);
			tx.set("key", "v0").unwrap();
			tx.commit().unwrap();
		}

		let stop = Arc::new(AtomicBool::new(false));
		let none_reads = Arc::new(AtomicUsize::new(0));
		let total_reads = Arc::new(AtomicUsize::new(0));

		// Single writer thread keeps overwriting the same key.
		let writer = {
			let db = Arc::clone(&db);
			let stop = Arc::clone(&stop);
			thread::spawn(move || {
				let mut counter: u64 = 0;
				while !stop.load(Ordering::Relaxed) {
					let mut tx = db.transaction(true);
					tx.set("key", format!("v{counter}")).unwrap();
					// Sequential writes from one thread never conflict;
					// any conflict here would be a test-environment issue,
					// not the race we're exercising.
					tx.commit().unwrap();
					counter = counter.wrapping_add(1);
				}
			})
		};

		// Reader threads open many short-lived snapshots. The original
		// bug triggered when a reader entered `TransactionInner::new` or
		// `reset` and was descheduled between choosing its snapshot and
		// becoming visible to sweepers. More readers means more chances
		// to land in that window.
		let mut readers = Vec::new();
		for _ in 0..6 {
			let db = Arc::clone(&db);
			let stop = Arc::clone(&stop);
			let none_reads = Arc::clone(&none_reads);
			let total_reads = Arc::clone(&total_reads);
			readers.push(thread::spawn(move || {
				while !stop.load(Ordering::Relaxed) {
					let tx = db.transaction(false);
					let value = tx.get("key").unwrap();
					total_reads.fetch_add(1, Ordering::Relaxed);
					if value.is_none() {
						none_reads.fetch_add(1, Ordering::Relaxed);
					}
					drop(tx);
				}
			}));
		}

		let started = Instant::now();
		while started.elapsed() < Duration::from_millis(500) {
			thread::sleep(Duration::from_millis(10));
		}
		stop.store(true, Ordering::Relaxed);

		writer.join().unwrap();
		for r in readers {
			r.join().unwrap();
		}

		let nones = none_reads.load(Ordering::Relaxed);
		let total = total_reads.load(Ordering::Relaxed);
		assert_eq!(
			nones, 0,
			"reader observed `None` for a key that was seeded with a value \
			 and never deleted ({nones} of {total} reads returned `None`). \
			 This is a snapshot-isolation registration race: a transaction \
			 chose its snapshot before its slot was visible to sweepers, \
			 so a concurrently computed `cleanup_ts` swept the versions \
			 its snapshot needed. The pin-then-read slot protocol in \
			 `pin_slot` is supposed to make this impossible."
		);
	}

	/// Per-key version chain: many overlapping writers on one key, then
	/// verify the raw chain is strictly monotonic and holds every committed
	/// value exactly once. A reader pinned at the seed version holds every
	/// commit's inline-GC watermark below the whole test's writes, so the
	/// chain retains the full history for inspection; background workers
	/// are disabled so no background sweep runs either.
	#[test]
	fn test_version_chain_monotonic_under_concurrent_writers() {
		const THREADS: usize = 8;
		const PER_THREAD: usize = 200;
		let db: Arc<Database> = Arc::new(Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		));
		// Pre-seed the key so all updates land on the same Versions entry.
		{
			let mut tx = db.transaction(true);
			tx.set(b"hotkey".to_vec(), b"seed".to_vec()).unwrap();
			tx.commit().unwrap();
		}
		// Pin a reader at the seed version so inline commit-time GC keeps
		// the seed and every later version alive for the whole test.
		let pin = db.transaction(false);
		let barrier = Arc::new(std::sync::Barrier::new(THREADS));
		let written: Arc<std::sync::Mutex<std::collections::BTreeSet<Vec<u8>>>> =
			Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::new()));
		let mut handles = Vec::new();
		for tid in 0..THREADS {
			let db = Arc::clone(&db);
			let barrier = Arc::clone(&barrier);
			let written = Arc::clone(&written);
			handles.push(thread::spawn(move || {
				barrier.wait();
				for i in 0..PER_THREAD {
					let val = format!("t{tid}-{i:05}").into_bytes();
					let mut tx = db.transaction(true);
					tx.set(b"hotkey".to_vec(), val.clone()).unwrap();
					if tx.commit().is_ok() {
						written.lock().unwrap().insert(val);
					}
				}
			}));
		}
		for h in handles {
			h.join().unwrap();
		}
		// Inspect the raw version chain directly.
		let entry = db.datastore.get(b"hotkey".as_slice()).expect("hotkey entry missing");
		let guard = entry.value().read();
		let chain = guard.as_slice();
		// Versions are strictly increasing: every commit is minted a unique
		// version, and every written value is unique, so no push dedups.
		for w in chain.windows(2) {
			assert!(
				w[0].version < w[1].version,
				"versions not strictly monotonic: {} then {}",
				w[0].version,
				w[1].version
			);
		}
		// No tombstones: this key was never deleted.
		assert!(chain.iter().all(|v| v.value.is_some()), "unexpected tombstone in chain");
		// Lossless and duplicate-free: the chain holds the seed plus every
		// successfully committed value, and nothing else.
		let committed = written.lock().unwrap().clone();
		assert_eq!(chain.len(), 1 + committed.len(), "chain length diverges from committed writes");
		let mut got: std::collections::BTreeSet<Vec<u8>> =
			chain.iter().filter_map(|v| v.value.as_ref().map(|b| b.to_vec())).collect();
		got.remove(b"seed".as_slice());
		assert_eq!(got, committed, "chain values diverge from committed writes");
		// Release the pinned reader
		drop(pin);
	}

	/// Repeated sequential writes to the same keys: each key's raw version
	/// chain accumulates every committed round in order. A reader pinned
	/// before the first round holds every commit's inline-GC watermark
	/// below the whole test's writes; background workers are disabled so
	/// no background sweep runs either.
	#[test]
	fn test_version_chain_accumulates_repeated_writes() {
		let db = Database::new_with_options(
			crate::DatabaseOptions::default().with_all_workers_disabled(),
		);
		// Pin a reader so inline commit-time GC retains every round
		let pin = db.transaction(false);
		// Five writes per key, on 200 keys
		for round in 0..5 {
			let mut tx = db.transaction(true);
			for i in 0..200 {
				tx.set(
					format!("doc:{i:010}").into_bytes(),
					format!("round{round}-i{i}").into_bytes(),
				)
				.unwrap();
			}
			tx.commit().unwrap();
		}
		// Each key's chain holds all five rounds, in commit order.
		for i in 0..200 {
			let key = format!("doc:{i:010}").into_bytes();
			let entry = db.datastore.get(key.as_slice()).expect("doc entry missing");
			let guard = entry.value().read();
			let chain = guard.as_slice();
			assert_eq!(chain.len(), 5, "chain should hold all five rounds");
			for w in chain.windows(2) {
				assert!(w[0].version < w[1].version, "versions not strictly monotonic");
			}
			for (round, v) in chain.iter().enumerate() {
				assert_eq!(
					v.value.as_deref(),
					Some(format!("round{round}-i{i}").as_bytes()),
					"unexpected value at round {round} for key {i}"
				);
			}
		}
		// Release the pinned reader
		drop(pin);
	}
}
