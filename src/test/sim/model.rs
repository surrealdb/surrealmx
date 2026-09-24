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

//! The reference model oracle (`ModelDb`).
//!
//! Maintains a simple, unoptimized, and mathematically correct reference model
//! of a Multi-Version Concurrency Control (MVCC) key-value store using standard
//! `BTreeMap` structures.
//!
//! Used for differential testing against `SurrealMX`: every point read, conditional
//! write, range scan, savepoint rollback, and commit conflict is verified against
//! `ModelDb` for exact equivalence.

use byteslice::ByteSlice;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use crate::Error;

/// Isolation level supported by the reference model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelIsolation {
	SnapshotIsolation,
	SerializableSnapshotIsolation,
}

/// Errors returned by the reference model matching `SurrealMX` error variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelError {
	TxClosed,
	TxNotWritable,
	KeyAlreadyExists,
	ValNotExpectedValue,
	NoSavepoint,
	KeyWriteConflict,
	KeyReadConflict,
}

impl From<ModelError> for Error {
	fn from(e: ModelError) -> Self {
		match e {
			ModelError::TxClosed => Self::TxClosed,
			ModelError::TxNotWritable => Self::TxNotWritable,
			ModelError::KeyAlreadyExists => Self::KeyAlreadyExists,
			ModelError::ValNotExpectedValue => Self::ValNotExpectedValue,
			ModelError::NoSavepoint => Self::NoSavepoint,
			ModelError::KeyWriteConflict => Self::KeyWriteConflict,
			ModelError::KeyReadConflict => Self::KeyReadConflict,
		}
	}
}

/// A committed transaction record stored in the model history.
#[derive(Debug, Clone)]
pub struct ModelCommitRecord {
	pub commit_id: u64,
	pub version: u64,
	pub writeset: BTreeMap<ByteSlice, Option<ByteSlice>>,
}

/// The in-memory reference model representing ground truth.
#[derive(Debug, Default, Clone)]
pub struct ModelDb {
	/// Monotonically increasing commit sequence ID.
	pub current_commit_id: u64,
	/// Monotonically increasing merge version / logical clock.
	pub current_version: u64,
	/// Full MVCC version history per key: sorted vector of (version, Option<value>).
	pub key_history: BTreeMap<ByteSlice, Vec<(u64, Option<ByteSlice>)>>,
	/// Ordered history of all committed transactions.
	pub commit_history: Vec<ModelCommitRecord>,
}

impl ModelDb {
	/// Creates a new empty reference model.
	pub const fn new() -> Self {
		Self {
			current_commit_id: 0,
			current_version: 0,
			key_history: BTreeMap::new(),
			commit_history: Vec::new(),
		}
	}

	/// Begins a new transaction in the reference model.
	pub const fn begin(&self, write: bool, mode: ModelIsolation) -> ModelTxn {
		ModelTxn {
			write,
			mode,
			start_commit_id: self.current_commit_id,
			start_version: self.current_version,
			writeset: BTreeMap::new(),
			readset: BTreeSet::new(),
			lockset: BTreeSet::new(),
			scanset: Vec::new(),
			savepoints: Vec::new(),
			done: false,
		}
	}

	/// Retrieves the value of a key as of a specific version snapshot.
	pub fn get_at_version(&self, key: &[u8], version: u64) -> Option<ByteSlice> {
		let history = self.key_history.get(key)?;
		let idx = history.partition_point(|(v, _)| *v <= version);
		if idx > 0 {
			history[idx - 1].1.clone()
		} else {
			None
		}
	}

	/// Checks whether a key exists (and is not deleted) at a specific version snapshot.
	pub fn exists_at_version(&self, key: &[u8], version: u64) -> bool {
		self.get_at_version(key, version).is_some()
	}

	/// Scans a range of keys as of a specific version snapshot.
	pub fn scan_at_version(
		&self,
		start: &[u8],
		end: &[u8],
		version: u64,
	) -> Vec<(ByteSlice, ByteSlice)> {
		let mut results = Vec::new();
		for (k, _) in
			self.key_history.range::<[u8], _>((Bound::Included(start), Bound::Excluded(end)))
		{
			if let Some(val) = self.get_at_version(k, version) {
				results.push((k.clone(), val));
			}
		}
		results
	}

	/// Commits a model transaction using strict first-committer-wins validation.
	pub fn commit(&mut self, mut txn: ModelTxn) -> Result<(), ModelError> {
		if txn.done {
			return Err(ModelError::TxClosed);
		}
		txn.done = true;

		// Read-only transactions with no locked reads succeed trivially
		if txn.writeset.is_empty() && txn.lockset.is_empty() {
			return Ok(());
		}

		let has_writes = !txn.writeset.is_empty();

		// Validate against all transactions committed concurrently since this transaction's snapshot
		for record in &self.commit_history {
			if record.commit_id <= txn.start_commit_id {
				continue;
			}
			// Skip commits whose merge version is visible in our snapshot
			if record.version != 0 && record.version <= txn.start_version {
				continue;
			}

			// 1. Write-write conflict: first-committer-wins on writeset overlap
			if has_writes {
				for write_key in txn.writeset.keys() {
					if record.writeset.contains_key(write_key) {
						return Err(ModelError::KeyWriteConflict);
					}
				}
			}

			// 2. Locked reads conflict: validated in all isolation modes
			if !txn.lockset.is_empty() {
				for locked_key in &txn.lockset {
					if record.writeset.contains_key(locked_key) {
						return Err(ModelError::KeyReadConflict);
					}
				}
			}

			// 3. Plain reads and scans conflict: validated only under SSI for transactions that write
			if has_writes && txn.mode == ModelIsolation::SerializableSnapshotIsolation {
				for read_key in &txn.readset {
					if record.writeset.contains_key(read_key) {
						return Err(ModelError::KeyReadConflict);
					}
				}

				for (scan_beg, scan_end) in &txn.scanset {
					for write_key in record.writeset.keys() {
						if write_key >= scan_beg && write_key < scan_end {
							return Err(ModelError::KeyReadConflict);
						}
					}
				}
			}
		}

		// Validation passed! Apply modifications to model history
		if has_writes {
			self.current_commit_id += 1;
			self.current_version += 1;

			for (key, val) in &txn.writeset {
				self.key_history
					.entry(key.clone())
					.or_default()
					.push((self.current_version, val.clone()));
			}

			self.commit_history.push(ModelCommitRecord {
				commit_id: self.current_commit_id,
				version: self.current_version,
				writeset: txn.writeset,
			});
		}

		Ok(())
	}
}

/// A transaction running against the reference model.
#[derive(Debug, Clone)]
pub struct ModelTxn {
	pub write: bool,
	pub mode: ModelIsolation,
	pub start_commit_id: u64,
	pub start_version: u64,
	pub writeset: BTreeMap<ByteSlice, Option<ByteSlice>>,
	pub readset: BTreeSet<ByteSlice>,
	pub lockset: BTreeSet<ByteSlice>,
	pub scanset: Vec<(ByteSlice, ByteSlice)>,
	pub savepoints: Vec<BTreeMap<ByteSlice, Option<ByteSlice>>>,
	pub done: bool,
}

impl ModelTxn {
	/// Point read of a key.
	pub fn get(&mut self, model: &ModelDb, key: &[u8]) -> Result<Option<ByteSlice>, ModelError> {
		if self.done {
			return Err(ModelError::TxClosed);
		}

		let key_bytes = ByteSlice::from_slice(key);
		let res = if let Some(local_val) = self.writeset.get(key) {
			local_val.clone()
		} else {
			let val = model.get_at_version(key, self.start_version);
			if self.mode == ModelIsolation::SerializableSnapshotIsolation {
				self.readset.insert(key_bytes);
			}
			val
		};

		Ok(res)
	}

	/// Point read of a key with update lock.
	pub fn get_for_update(
		&mut self,
		model: &ModelDb,
		key: &[u8],
	) -> Result<Option<ByteSlice>, ModelError> {
		if self.done {
			return Err(ModelError::TxClosed);
		}
		if !self.write {
			return Err(ModelError::TxNotWritable);
		}

		let key_bytes = ByteSlice::from_slice(key);
		self.lockset.insert(key_bytes);

		let res = if let Some(local_val) = self.writeset.get(key) {
			local_val.clone()
		} else {
			model.get_at_version(key, self.start_version)
		};

		Ok(res)
	}

	/// Checks existence of a key.
	pub fn exists(&mut self, model: &ModelDb, key: &[u8]) -> Result<bool, ModelError> {
		Ok(self.get(model, key)?.is_some())
	}

	/// Unconditionally inserts or updates a key.
	pub fn set(&mut self, key: &[u8], val: &[u8]) -> Result<(), ModelError> {
		if self.done {
			return Err(ModelError::TxClosed);
		}
		if !self.write {
			return Err(ModelError::TxNotWritable);
		}

		self.writeset.insert(ByteSlice::from_slice(key), Some(ByteSlice::from_slice(val)));
		Ok(())
	}

	/// Inserts a key only if it does not already exist.
	pub fn put(&mut self, model: &ModelDb, key: &[u8], val: &[u8]) -> Result<(), ModelError> {
		if self.done {
			return Err(ModelError::TxClosed);
		}
		if !self.write {
			return Err(ModelError::TxNotWritable);
		}

		if self.writeset.contains_key(key) || model.exists_at_version(key, self.start_version) {
			return Err(ModelError::KeyAlreadyExists);
		}

		self.writeset.insert(ByteSlice::from_slice(key), Some(ByteSlice::from_slice(val)));
		Ok(())
	}

	/// Inserts a key if its current value matches an expected check value.
	pub fn putc(
		&mut self,
		model: &ModelDb,
		key: &[u8],
		val: &[u8],
		chk: Option<&[u8]>,
	) -> Result<(), ModelError> {
		if self.done {
			return Err(ModelError::TxClosed);
		}
		if !self.write {
			return Err(ModelError::TxNotWritable);
		}

		let current_val = match self.writeset.get(key) {
			Some(Some(v)) => Some(v.clone()),
			Some(None) => None,
			None => model.get_at_version(key, self.start_version),
		};

		let matches = match (chk, current_val.as_ref()) {
			(Some(expected), Some(actual)) => expected == actual.as_ref(),
			(None, None) => true,
			_ => false,
		};

		if !matches {
			return Err(ModelError::ValNotExpectedValue);
		}

		self.writeset.insert(ByteSlice::from_slice(key), Some(ByteSlice::from_slice(val)));
		Ok(())
	}

	/// Deletes a key.
	pub fn del(&mut self, key: &[u8]) -> Result<(), ModelError> {
		if self.done {
			return Err(ModelError::TxClosed);
		}
		if !self.write {
			return Err(ModelError::TxNotWritable);
		}

		self.writeset.insert(ByteSlice::from_slice(key), None);
		Ok(())
	}

	/// Deletes a key if its current value matches an expected check value.
	pub fn delc(
		&mut self,
		model: &ModelDb,
		key: &[u8],
		chk: Option<&[u8]>,
	) -> Result<(), ModelError> {
		if self.done {
			return Err(ModelError::TxClosed);
		}
		if !self.write {
			return Err(ModelError::TxNotWritable);
		}

		let current_val = match self.writeset.get(key) {
			Some(Some(v)) => Some(v.clone()),
			Some(None) => None,
			None => model.get_at_version(key, self.start_version),
		};

		let matches = match (chk, current_val.as_ref()) {
			(Some(expected), Some(actual)) => expected == actual.as_ref(),
			(None, None) => true,
			_ => false,
		};

		if !matches {
			return Err(ModelError::ValNotExpectedValue);
		}

		self.writeset.insert(ByteSlice::from_slice(key), None);
		Ok(())
	}

	/// Sets a savepoint.
	pub fn set_savepoint(&mut self) -> Result<(), ModelError> {
		if self.done {
			return Err(ModelError::TxClosed);
		}
		if !self.write {
			return Err(ModelError::TxNotWritable);
		}
		self.savepoints.push(self.writeset.clone());
		Ok(())
	}

	/// Rolls back to the most recent savepoint.
	pub fn rollback_to_savepoint(&mut self) -> Result<(), ModelError> {
		if self.done {
			return Err(ModelError::TxClosed);
		}
		if !self.write {
			return Err(ModelError::TxNotWritable);
		}
		self.writeset = self.savepoints.pop().ok_or(ModelError::NoSavepoint)?;
		Ok(())
	}

	/// Releases the most recent savepoint.
	pub fn release_savepoint(&mut self) -> Result<(), ModelError> {
		if self.done {
			return Err(ModelError::TxClosed);
		}
		if !self.write {
			return Err(ModelError::TxNotWritable);
		}
		self.savepoints.pop().ok_or(ModelError::NoSavepoint)?;
		Ok(())
	}

	/// Scans a range of keys, merging snapshot state with local modifications.
	pub fn scan(
		&mut self,
		model: &ModelDb,
		start: &[u8],
		end: &[u8],
		skip: Option<usize>,
		limit: Option<usize>,
		reverse: bool,
	) -> Result<Vec<(ByteSlice, ByteSlice)>, ModelError> {
		if self.done {
			return Err(ModelError::TxClosed);
		}

		let start_bytes = ByteSlice::from_slice(start);
		let end_bytes = ByteSlice::from_slice(end);

		if self.write && self.mode == ModelIsolation::SerializableSnapshotIsolation {
			self.scanset.push((start_bytes.clone(), end_bytes.clone()));
		}

		// Gather keys present in snapshot and local writeset in range
		let mut merged: BTreeMap<ByteSlice, Option<ByteSlice>> = BTreeMap::new();

		// Add snapshot state
		for (k, v) in model.scan_at_version(start, end, self.start_version) {
			merged.insert(k, Some(v));
		}

		// Overlay local writeset
		for (k, v) in self
			.writeset
			.range::<ByteSlice, _>((Bound::Included(&start_bytes), Bound::Excluded(&end_bytes)))
		{
			merged.insert(k.clone(), v.clone());
		}

		// Filter out deleted entries
		let mut pairs: Vec<(ByteSlice, ByteSlice)> =
			merged.into_iter().filter_map(|(k, v)| v.map(|val| (k, val))).collect();

		if reverse {
			pairs.reverse();
		}

		let skip_n = skip.unwrap_or(0);
		let limit_n = limit.unwrap_or(usize::MAX);

		let result = pairs.into_iter().skip(skip_n).take(limit_n).collect();
		Ok(result)
	}

	/// Counts keys in a range.
	#[allow(dead_code)]
	pub fn total(
		&mut self,
		model: &ModelDb,
		start: &[u8],
		end: &[u8],
		skip: Option<usize>,
		limit: Option<usize>,
		reverse: bool,
	) -> Result<usize, ModelError> {
		Ok(self.scan(model, start, end, skip, limit, reverse)?.len())
	}
}
