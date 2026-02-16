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

//! This module stores the database error types.

use bincode::error::DecodeError as BincodeDecodeError;
use bincode::error::EncodeError as BincodeEncodeError;
use std::io::Error as IoError;
use std::sync::PoisonError;
use thiserror::Error;

/// The errors which can be emitted from a database.
#[derive(Error, Debug)]
pub enum Error {
	/// A transaction is closed.
	#[error("Transaction is closed")]
	TxClosed,

	/// A transaction is not writable.
	#[error("Transaction is not writable")]
	TxNotWritable,

	/// A key being inserted already exists.
	#[error("Key being inserted already exists")]
	KeyAlreadyExists,

	/// A value being checked was not correct.
	#[error("Value being checked was not correct")]
	ValNotExpectedValue,

	/// A read conflict, retry the transaction.
	#[error("Read conflict, retry the transaction")]
	KeyReadConflict,

	/// A write conflict, retry the transaction.
	#[error("Write conflict, retry the transaction")]
	KeyWriteConflict,

	/// Can not fetch value at a future version.
	#[error("Can not fetch value at a future version")]
	VersionInFuture,

	/// No savepoint has been set.
	#[error("No savepoint has been set")]
	NoSavepoint,

	/// A transaction is not persistent.
	#[error("Transaction is not persistent")]
	TxCommitNotPersisted(PersistenceError),
}

/// The errors that can occur during persistence operations.
#[derive(Error, Debug)]
pub enum PersistenceError {
	/// An IO error occurred.
	#[error("IO error: {0}")]
	Io(#[from] IoError),

	/// A serialization error occurred.
	#[error("Serialization error: {0}")]
	Serialization(#[from] BincodeEncodeError),

	/// A deserialization error occurred.
	#[error("Deserialization error: {0}")]
	Deserialization(#[from] BincodeDecodeError),

	/// A lock acquisition failed.
	#[error("Lock acquisition failed")]
	LockFailed(String),

	/// A snapshot creation failed.
	#[error("Snapshot creation failed: {0}")]
	SnapshotFailed(String),

	/// An AOL append failed.
	#[error("AOL append failed: {0}")]
	AppendFailed(String),
}

impl<T> From<PoisonError<std::sync::MutexGuard<'_, T>>> for PersistenceError {
	fn from(error: PoisonError<std::sync::MutexGuard<'_, T>>) -> Self {
		PersistenceError::LockFailed(error.to_string())
	}
}
