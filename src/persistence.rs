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

//! This module stores the database persistence logic.

use crate::compression::CompressedReader;
use crate::compression::CompressedWriter;
use crate::compression::CompressionMode;
use crate::err::PersistenceError;
use crate::inner::Inner;
use crate::version::Version;
use crate::versions::Versions;
use bincode::config;
use bytes::Bytes;
use crossbeam_deque::{Injector, Steal};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Represents a pending asynchronous append operation
#[derive(Debug, Clone)]
pub(crate) struct AsyncAppendOperation {
	pub version: u64,
	pub writeset: BTreeMap<Bytes, Option<Bytes>>,
}

/// Configuration for AOL (Append-Only Log) behavior
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum AolMode {
	/// Never use AOL
	#[default]
	Never,
	/// Write immediatelyto AOL on every commit
	SynchronousOnCommit,
	/// Write asynchronously to AOL on every commit
	AsynchronousAfterCommit,
}

/// Configuration for snapshot behavior
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMode {
	/// Never use snapshots
	#[default]
	Never,
	/// Periodically snapshot at the given interval
	Interval(Duration),
}

/// Configuration for fsync behavior
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
	/// Never call fsync (fastest, least durable)
	#[default]
	Never,
	/// Call fsync after every append operation (slowest, most durable)
	EveryAppend,
	/// Call fsync at most once per interval
	Interval(Duration),
}

/// Configuration options for persistence
#[derive(Debug, Clone)]
pub struct PersistenceOptions {
	/// Base path for persistence files
	pub base_path: PathBuf,
	/// AOL (append-only log) behavior mode
	pub aol_mode: AolMode,
	/// Snapshot behavior mode
	pub snapshot_mode: SnapshotMode,
	/// Configuration for fsync behavior
	pub fsync_mode: FsyncMode,
	/// Path to the append-only log file (relative to base path or absolute)
	pub aol_path: Option<PathBuf>,
	/// Path to the snapshot file (relative to base path or absolute)
	pub snapshot_path: Option<PathBuf>,
	/// Compression mode for snapshots
	pub compression_mode: CompressionMode,
}

impl Default for PersistenceOptions {
	fn default() -> Self {
		Self {
			base_path: PathBuf::from("./data"),
			aol_mode: AolMode::default(),
			snapshot_mode: SnapshotMode::default(),
			fsync_mode: FsyncMode::default(),
			aol_path: None,
			snapshot_path: None,
			compression_mode: CompressionMode::default(),
		}
	}
}

impl PersistenceOptions {
	/// Create new persistence options with the given base path
	pub fn new<P: Into<PathBuf>>(base_path: P) -> Self {
		Self {
			base_path: base_path.into(),
			..Self::default()
		}
	}

	/// Set the base path for persistence files
	pub fn with_base_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
		self.base_path = path.into();
		self
	}

	/// Set the AOL (append-only log) behavior mode
	pub fn with_aol_mode(mut self, mode: AolMode) -> Self {
		self.aol_mode = mode;
		self
	}

	/// Set the snapshot behavior mode
	pub fn with_snapshot_mode(mut self, mode: SnapshotMode) -> Self {
		self.snapshot_mode = mode;
		self
	}

	/// Set the fsync mode
	pub fn with_fsync_mode(mut self, mode: FsyncMode) -> Self {
		self.fsync_mode = mode;
		self
	}

	/// Set a custom AOL file path
	pub fn with_aol_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
		self.aol_path = Some(path.into());
		self
	}

	/// Set a custom snapshot file path
	pub fn with_snapshot_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
		self.snapshot_path = Some(path.into());
		self
	}

	/// Set the compression mode for snapshots
	pub fn with_compression(mut self, mode: CompressionMode) -> Self {
		self.compression_mode = mode;
		self
	}
}

/// A persistence layer for storing and loading database state
///
/// This struct handles the persistence of database state through:
/// - Append-only log (AOL) for recording changes
/// - Periodic snapshots for efficient recovery
/// - Background worker for automatic snapshot creation
#[derive(Clone)]
pub struct Persistence {
	/// Reference to the inner database state
	pub(crate) inner: Arc<Inner>,
	/// File handle for the append-only log (None if AOL is disabled)
	pub(crate) aol: Option<Arc<Mutex<File>>>,
	/// Path to the append-only log file (None if AOL is disabled)
	pub(crate) aol_path: PathBuf,
	/// Path to the snapshot file
	pub(crate) snapshot_path: PathBuf,
	/// AOL (append-only log) behavior mode
	pub(crate) aol_mode: AolMode,
	/// Snapshot behavior mode
	pub(crate) snapshot_mode: SnapshotMode,
	/// Fsync configuration mode
	pub(crate) fsync_mode: FsyncMode,
	/// Compression mode for snapshots
	pub(crate) compression_mode: CompressionMode,
	/// Specifies whether background worker threads are enabled
	pub(crate) background_threads_enabled: Arc<AtomicBool>,
	/// Handle to the background fsync worker thread (for interval mode)
	pub(crate) fsync_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
	/// Handle to the background snapshot worker thread
	pub(crate) snapshot_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
	/// Handle to the background async append worker thread
	pub(crate) appender_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
	/// Last fsync timestamp for interval mode
	pub(crate) last_fsync: Arc<Mutex<Instant>>,
	/// Counter for AOL appends since last fsync
	pub(crate) pending_syncs: Arc<AtomicU64>,
	/// Queue for asynchronous append operations
	pub(crate) async_append_injector: Arc<Injector<AsyncAppendOperation>>,
}

impl Persistence {
	/// Creates a new persistence layer with custom options
	///
	/// # Arguments
	/// * `options` - Configuration options for persistence
	/// * `inner` - Reference to the database state
	///
	/// # Returns
	/// * `Result<Self, PersistenceError>` - The created persistence layer or an error
	pub(crate) fn new_with_options(
		options: PersistenceOptions,
		inner: Arc<Inner>,
	) -> Result<Self, PersistenceError> {
		// Get the base path from options
		let base_path = &options.base_path;
		// Ensure the directory exists
		fs::create_dir_all(base_path)?;
		// Determine the specified AOL file path
		let aol_path = if let Some(path) = options.aol_path {
			if path.is_absolute() {
				path
			} else {
				base_path.join(path)
			}
		} else {
			base_path.join("aol.bin")
		};
		// Determine the specified snapshot file path
		let snapshot_path = if let Some(path) = options.snapshot_path {
			if path.is_absolute() {
				path
			} else {
				base_path.join(path)
			}
		} else {
			base_path.join("snapshot.bin")
		};
		// Initialize AOL components if enabled
		let aol = if !matches!(options.aol_mode, AolMode::Never) {
			// Ensure parent directories exist for AOL path
			if let Some(parent) = aol_path.parent() {
				fs::create_dir_all(parent)?;
			}
			// Open the AOL file with append mode
			let file = OpenOptions::new().create(true).append(true).read(true).open(&aol_path)?;
			Some(Arc::new(Mutex::new(file)))
		} else {
			None
		};
		// Ensure parent directories exist for snapshot path
		if let Some(parent) = snapshot_path.parent() {
			fs::create_dir_all(parent)?;
		}
		// Create the persistence instance
		let this = Self {
			inner,
			aol,
			aol_path,
			snapshot_path,
			aol_mode: options.aol_mode,
			snapshot_mode: options.snapshot_mode,
			fsync_mode: options.fsync_mode,
			compression_mode: options.compression_mode,
			background_threads_enabled: Arc::new(AtomicBool::new(true)),
			fsync_handle: Arc::new(RwLock::new(None)),
			snapshot_handle: Arc::new(RwLock::new(None)),
			appender_handle: Arc::new(RwLock::new(None)),
			last_fsync: Arc::new(Mutex::new(Instant::now())),
			pending_syncs: Arc::new(AtomicU64::new(0)),
			async_append_injector: Arc::new(Injector::new()),
		};
		// Load existing data from disk
		this.load()?;
		// Start the background snapshot worker if snapshots are enabled
		this.spawn_snapshot_worker();
		// Start the fsync worker if needed (only when AOL is enabled)
		this.spawn_fsync_worker();
		// Start the async append worker if asynchronous mode is enabled
		this.spawn_appender_worker();
		// Return the persistence layer
		Ok(this)
	}

	/// Creates a new snapshot of the current database state
	///
	/// This function:
	/// 1. Captures the current AOL file position as a cutoff point
	/// 2. Creates a new snapshot file atomically using a temporary file
	/// 3. Streams data to reduce memory usage
	/// 4. Truncates AOL only up to the cutoff position, preserving newer entries
	///
	/// # Returns
	/// * `Result<(), PersistenceError>` - Success or an error
	pub fn snapshot(&self) -> Result<(), PersistenceError> {
		// Create temporary file for atomic swap
		let temp_path = self.snapshot_path.with_extension("tmp");
		// Execute snapshot operation in closure for clean error handling
		let result = (|| -> Result<(), PersistenceError> {
			// Create temporary file
			let file = File::create(&temp_path)?;
			// Create compressed writer (handles buffering internally)
			let mut writer = CompressedWriter::new(file, self.compression_mode)?;
			// Get the current position in the AOL file (if AOL is enabled)
			let aol_cutoff_position = if let Some(ref aol) = self.aol {
				aol.lock()?.metadata()?.len()
			} else {
				0
			};
			// Stream write each key-value pair to reduce memory usage
			for entry in self.inner.datastore.iter() {
				// Get all versions for this key
				let versions = entry.value().read().all_versions();
				// Ensure that there are version entries
				if !versions.is_empty() {
					// Serialize and write this single entry
					bincode::serde::encode_into_std_write(
						&(entry.key().clone(), versions),
						&mut writer,
						config::standard(),
					)?;
				}
			}
			// Flush the compressed writer
			writer.flush()?;
			// Finish compression (finalizes LZ4 stream)
			writer.finish()?;
			// Atomically rename temporary file to actual snapshot
			fs::rename(&temp_path, &self.snapshot_path)?;
			// Sync the renamed file to disk for durability
			{
				let final_file = File::open(&self.snapshot_path)?;
				final_file.sync_all()?;
			}
			// Truncate AOL only up to the cutoff position
			Self::truncate(&self.aol, aol_cutoff_position, &self.pending_syncs)?;
			// All ok
			Ok(())
		})();
		// Clean up temporary file if operation failed
		if result.is_err() {
			// Ignore removal errors
			let _ = fs::remove_file(&temp_path);
		}
		// Return the operation result
		result
	}

	/// Loads the database state from disk
	///
	/// This function:
	/// 1. Loads the latest snapshot if it exists
	/// 2. Applies any changes from the append-only log
	fn load(&self) -> Result<(), PersistenceError> {
		// Check if snapshot file exists
		if self.snapshot_path.exists() {
			// Read and deserialize the snapshot data
			let file = File::open(&self.snapshot_path)?;
			// Get the metadata of the snapshot file
			let metadata = file.metadata()?;
			// Check if the snapshot file is empty
			if metadata.len() > 0 {
				// Create compressed reader that auto-detects compression mode
				let mut reader = CompressedReader::new(file)?;
				// Initialize counters for tracking loaded entries
				let mut count = 0;
				// Stream reading the snapshot to reduce memory usage
				loop {
					// Increment the counter
					count += 1;
					// Trace the loading of the snapshot entry
					tracing::trace!("Loading snapshot entry: {count}");
					// Type alias for the entry
					type Entry = (Bytes, Vec<(u64, Option<Bytes>)>);
					// Attempt to decode the next entry, handling EOF gracefully
					let result: Result<Entry, _> =
						bincode::serde::decode_from_std_read(&mut reader, config::standard());
					// Detech any end of file errors
					match result {
						Ok((k, versions)) => {
							// Ensure that there are version entries
							if !versions.is_empty() {
								// Create a new versions entry
								let mut entries = Versions::new();
								// Add all of the version entries
								for (version, value) in versions.into_iter() {
									entries.push(Version {
										version,
										value,
									});
								}
								// Insert the entry into the datastore
								self.inner.datastore.insert(k, RwLock::new(entries));
							}
						}
						Err(e) => match e {
							// Handle bincode decode errors that indicate EOF
							bincode::error::DecodeError::Io {
								inner,
								..
							} if inner.kind() == std::io::ErrorKind::UnexpectedEof => {
								break;
							}
							e => return Err(PersistenceError::Deserialization(e)),
						},
					}
				}
			}
		}
		// Check if append-only file exists
		if self.aol_path.exists() {
			// Open and read the AOL file
			let file = File::open(&self.aol_path)?;
			// Get the metadata of the append-only file
			let metadata = file.metadata()?;
			// Check if the append-only file is empty
			if metadata.len() > 0 {
				// Create buffered reader for efficient reading
				let mut reader = BufReader::new(file);
				// Initialize counters for tracking loaded entries
				let mut count = 0;
				// Read and apply each change from the AOL
				loop {
					// Increment the counter
					count += 1;
					// Trace the loading of the append-only entry
					tracing::trace!("Loading AOL entry: {count}");
					// Type alias for the entry
					type Entry = (Bytes, u64, Option<Bytes>);
					// Explicitly type the result to help type inference
					let result: Result<Entry, _> =
						bincode::serde::decode_from_std_read(&mut reader, config::standard());
					// Detech any end of file errors
					match result {
						Ok((k, version, val)) => {
							// Check if the key already exists
							if let Some(entry) = self.inner.datastore.get(&k) {
								// Update existing key with stored version
								entry.value().write().push(Version {
									version,
									value: val,
								});
							} else {
								// Insert new key with stored version
								self.inner.datastore.insert(
									k.clone(),
									RwLock::new(Versions::from(Version {
										version,
										value: val,
									})),
								);
							}
						}
						Err(e) => match e {
							// Handle bincode decode errors that indicate EOF
							bincode::error::DecodeError::Io {
								inner,
								..
							} if inner.kind() == std::io::ErrorKind::UnexpectedEof => {
								break;
							}
							e => return Err(PersistenceError::Deserialization(e)),
						},
					}
				}
			}
		}
		// Return success
		Ok(())
	}

	/// Truncate the AOL file up to the specified position, preserving any data after.
	fn truncate(
		aol: &Option<Arc<Mutex<File>>>,
		position: u64,
		pending_syncs: &Arc<AtomicU64>,
	) -> Result<(), PersistenceError> {
		// Check that we have a AOL file handle
		if let Some(ref aol) = aol {
			// Lock the AOL file
			let mut file = aol.lock()?;
			// Get the current file length
			let file_len = file.metadata()?.len();
			// Check if there is remaining data
			if file_len > position {
				// Generate a unique name for the temporary file
				let name = format!("aol_truncate_{}.tmp", std::process::id());
				// Generate the path for the temporary file
				let path = std::env::temp_dir().join(name);
				// Execute truncation in a closure for clean error handling
				let result = (|| -> Result<(), PersistenceError> {
					// Create temporary file and copy remaining data
					{
						file.seek(SeekFrom::Start(position))?;
						// Create the temporary file
						let mut temp = File::create(&path)?;
						// Copy the remaining data to the temporary file
						std::io::copy(&mut *file, &mut temp)?;
						// Sync the temporary file
						temp.sync_all()?;
					}
					// Go to the beginning of the file
					file.seek(SeekFrom::Start(0))?;
					// Truncate the AOL file
					file.set_len(0)?;
					// Copy data from temporary file
					{
						let mut temp = File::open(&path)?;
						std::io::copy(&mut temp, &mut *file)?;
					}
					// Flush the file contents
					file.flush()?;
					// All ok
					Ok(())
				})();
				// Delete the temporary file
				let _ = fs::remove_file(&path);
				// Return the result
				result?;
			} else {
				// Truncate the AOL file
				file.set_len(0)?;
				// Flush the file contents
				file.flush()?;
			}
			// Reset pending syncs if we truncated to beginning
			if position == 0 {
				pending_syncs.store(0, Ordering::Release);
			}
		}
		// All ok
		Ok(())
	}

	/// Spawns a background worker thread for periodic fsync
	fn spawn_fsync_worker(&self) {
		// Check if AOL is enabled
		if self.aol_mode == AolMode::Never {
			return;
		}
		// Get the specified fsync interval
		let FsyncMode::Interval(interval) = self.fsync_mode else {
			return;
		};
		// Check if AOL is enabled
		if let Some(ref aol) = self.aol {
			// Check if a background thread is already running
			if self.fsync_handle.read().is_none() {
				// Clone necessary fields for the worker thread
				let aol = aol.clone();
				let pending_syncs = self.pending_syncs.clone();
				let enabled = self.background_threads_enabled.clone();
				// Spawn the background worker thread
				let handle = thread::spawn(move || {
					// Check whether the persistence process is enabled
					while enabled.load(Ordering::Acquire) {
						// Sleep for the configured interval
						thread::park_timeout(interval);
						// Check shutdown flag again after waking
						if !enabled.load(Ordering::Acquire) {
							break;
						}
						// Check if there are pending syncs
						if pending_syncs.load(Ordering::Acquire) > 0 {
							if let Ok(file) = aol.lock() {
								if let Err(e) = file.sync_all() {
									tracing::error!("Fsync worker error: {e}");
								} else {
									pending_syncs.store(0, Ordering::Release);
								}
							}
						}
					}
				});
				// Store and track the thread handle
				*self.fsync_handle.write() = Some(handle);
			}
		}
	}

	/// Spawns a background worker thread for periodic snapshots
	///
	/// The worker thread:
	/// 1. Sleeps for the configured interval
	/// 2. Captures the current AOL file position
	/// 3. Creates a new snapshot
	/// 4. Truncates AOL up to the cutoff, preserving newer entries
	fn spawn_snapshot_worker(&self) {
		// Check if snapshots are enabled
		if self.snapshot_mode == SnapshotMode::Never {
			return;
		}
		// Only spawn if snapshot interval is configured
		let SnapshotMode::Interval(interval) = self.snapshot_mode else {
			return;
		};
		// Check if a background thread is already running
		if self.snapshot_handle.read().is_none() {
			// Clone necessary fields for the worker thread
			let db = self.inner.clone();
			let aol = self.aol.clone();
			let snapshot_path = self.snapshot_path.clone();
			let pending_syncs = self.pending_syncs.clone();
			let enabled = self.background_threads_enabled.clone();
			let compression = self.compression_mode;
			// Spawn the background worker thread
			let handle = thread::spawn(move || {
				// Check whether the persistence process is enabled
				while enabled.load(Ordering::Acquire) {
					// Sleep for the configured interval
					thread::park_timeout(interval);
					// Check shutdown flag again after waking
					if !enabled.load(Ordering::Acquire) {
						break;
					}
					// Create temporary file for atomic swap
					let temp_path = snapshot_path.with_extension("tmp");
					// Ensure clean error handling in closure
					let result = (|| -> Result<(), PersistenceError> {
						// Create temporary file
						let file = File::create(&temp_path)?;
						// Create compressed writer (handles buffering internally)
						let mut writer = CompressedWriter::new(file, compression)?;
						// Get the current position in the AOL file before snapshotting (if AOL enabled)
						let aol_cutoff_position = if let Some(ref aol) = aol {
							aol.lock()?.metadata()?.len()
						} else {
							0
						};
						// Stream write each entry to reduce memory usage
						for entry in db.datastore.iter() {
							// Get all versions for this key
							let versions = entry.value().read().all_versions();
							// Ensure that there are version entries
							if !versions.is_empty() {
								// Serialize and write this single entry
								bincode::serde::encode_into_std_write(
									&(entry.key().clone(), versions),
									&mut writer,
									config::standard(),
								)?;
							}
						}
						// Flush the compressed writer
						writer.flush()?;
						// Finish compression (finalizes LZ4 stream)
						writer.finish()?;
						// Atomically rename temporary file
						fs::rename(&temp_path, &snapshot_path)?;
						// Sync the renamed file to disk for durability
						{
							let final_file = File::open(&snapshot_path)?;
							final_file.sync_all()?;
						}
						// Truncate AOL to the cutoff position
						Self::truncate(&aol, aol_cutoff_position, &pending_syncs)?;
						// All ok
						Ok(())
					})();
					// Check if the snapshot operation failed
					if let Err(e) = result {
						// Trace the snapshot worker error
						tracing::error!("Snapshot worker error: {e}");
						// Clean up temporary file if it exists
						let _ = fs::remove_file(&temp_path);
					}
				}
			});
			// Store the worker thread handle
			*self.snapshot_handle.write() = Some(handle);
		}
	}

	/// Spawn the background worker thread for processing async append operations
	fn spawn_appender_worker(&self) {
		// Check if asynchronous append mode is enabled
		if self.aol_mode != AolMode::AsynchronousAfterCommit {
			return;
		}
		// Check if AOL is enabled
		if let Some(ref aol) = self.aol {
			// Check if a background thread is already running
			if self.appender_handle.read().is_none() {
				// Clone necessary fields for the worker thread
				let injector = self.async_append_injector.clone();
				let aol = aol.clone();
				let fsync_mode = self.fsync_mode;
				let enabled = self.background_threads_enabled.clone();
				let pending_syncs = self.pending_syncs.clone();
				let last_fsync = self.last_fsync.clone();
				// Spawn the background worker thread
				let handle = thread::spawn(move || {
					// Set the batch size and timeout
					const BATCH_SIZE: usize = 100;
					const TIMEOUT_MS: u64 = 10;
					// Initialize the batch vector
					let mut batch = Vec::with_capacity(BATCH_SIZE);
					// Check whether the persistence process is enabled
					while enabled.load(Ordering::Acquire) {
						// Check shutdown flag again after waking
						if !enabled.load(Ordering::Acquire) {
							break;
						}
						// Clear the batch
						batch.clear();
						// Collect operations into a batch
						loop {
							// Check shutdown flag in the inner loop
							if !enabled.load(Ordering::Acquire) {
								break;
							}
							match injector.steal() {
								Steal::Retry => {
									std::thread::yield_now();
									continue;
								}
								Steal::Success(op) => {
									batch.push(op);
									if batch.len() == BATCH_SIZE {
										break;
									}
								}
								Steal::Empty => {
									// If we have items to append, break
									if !batch.is_empty() {
										break;
									}
									// Park the thread to wait for work
									thread::park_timeout(Duration::from_millis(TIMEOUT_MS));
								}
							}
						}
						// Process the batch if we have operations
						if !batch.is_empty() {
							// Ensure clean error handling in closure
							let result = (|| -> Result<(), PersistenceError> {
								// Lock the AOL file for writing
								if let Ok(mut file) = aol.lock() {
									// Create a new buffer for the AOL file
									let mut writer = BufWriter::new(&mut *file);
									// Write all operations in the batch
									for op in &batch {
										for (k, v) in &op.writeset {
											bincode::serde::encode_into_std_write(
												(k, op.version, v),
												&mut writer,
												config::standard(),
											)?;
										}
									}
									// Flush the buffer to the file on the operating system
									writer.flush()?;
									// Drop the writer to release the mutable borrow
									drop(writer);
									// Handle fsync based on mode
									match fsync_mode {
										// Let the operating system handle syncing to disk
										FsyncMode::Never => {
											// No fsync, just increment pending counter
											pending_syncs.fetch_add(1, Ordering::Release);
										}
										// Sync immediately to diskafter every append
										FsyncMode::EveryAppend => {
											// Sync immediately
											file.sync_all()?;
										}
										// Force sync to disk at a specified interval
										FsyncMode::Interval(duration) => {
											// Check if we should sync based on time
											let now = Instant::now();
											// Check if we should sync based on time
											let should_sync = {
												// Get the last fsync time
												let mut last_fsync = last_fsync.lock()?;
												// Check if the last fsync time is greater than the duration
												if now.duration_since(*last_fsync) >= duration {
													// Update the last fsync time
													*last_fsync = now;
													true
												} else {
													false
												}
											};
											// Check if we should sync
											if should_sync {
												// Force sync the AOL file to disk
												file.sync_all()?;
												// Reset the pending syncs counter
												pending_syncs.store(0, Ordering::Release);
											} else {
												// Increment the pending syncs counter
												pending_syncs.fetch_add(1, Ordering::Release);
											}
										}
									}
								}
								// All ok
								Ok(())
							})();
							// Check if the async append operation failed
							if let Err(e) = result {
								// Trace the snapshot worker error
								tracing::error!("Async append worker error: {e}");
							}
						}
					}
				});
				// Store the thread handle
				*self.appender_handle.write() = Some(handle);
			}
		}
	}

	/// Appends a set of changes to the append-only log
	///
	/// # Arguments
	/// * `version` - The version (timestamp) for these changes
	/// * `writeset` - Map of key-value changes to append
	///
	/// # Returns
	/// * `Result<(), PersistenceError>` - Success or an error
	pub(crate) fn append(
		&self,
		version: u64,
		writeset: &BTreeMap<Bytes, Option<Bytes>>,
	) -> Result<(), PersistenceError> {
		// Skip AOL writing if AOL is disabled
		if self.aol_mode == AolMode::Never {
			return Ok(());
		}
		// AOL is enabled, proceed with append logic
		if let Some(ref aol) = self.aol {
			// Handle asynchronous AOL mode by queuing the operation
			if self.aol_mode == AolMode::AsynchronousAfterCommit {
				// Queue the append operation
				self.async_append_injector.push(AsyncAppendOperation {
					version,
					writeset: writeset.clone(),
				});
				// Wake up the async append worker if available
				if let Some(handle) = self.appender_handle.read().as_ref() {
					handle.thread().unpark();
				}
			}
			if self.aol_mode == AolMode::SynchronousOnCommit {
				// Lock the AOL file for writing
				let mut file = aol.lock()?;
				// Create a new buffer for the AOL file
				let mut writer = BufWriter::new(&mut *file);
				// Serialize and write each change with version
				for (k, v) in writeset {
					bincode::serde::encode_into_std_write(
						(k, version, v),
						&mut writer,
						config::standard(),
					)?;
				}
				// Flush the buffer to the file on the operating system
				writer.flush()?;
				// Drop the writer to release the mutable borrow
				drop(writer);
				// Handle fsync based on mode
				match self.fsync_mode {
					// Let the operating system handle syncing to disk
					FsyncMode::Never => {
						// No fsync, just increment pending counter
						self.pending_syncs.fetch_add(1, Ordering::Release);
					}
					// Sync immediately to diskafter every append
					FsyncMode::EveryAppend => {
						// Sync immediately
						file.sync_all()?;
					}
					// Force sync to disk at a specified interval
					FsyncMode::Interval(duration) => {
						// Check if we should sync based on time
						let now = Instant::now();
						// Check if we should sync based on time
						let should_sync = {
							// Get the last fsync time
							let mut last_fsync = self.last_fsync.lock()?;
							// Check if the last fsync time is greater than the duration
							if now.duration_since(*last_fsync) >= duration {
								// Update the last fsync time
								*last_fsync = now;
								true
							} else {
								false
							}
						};
						// Check if we should sync
						if should_sync {
							// Force sync the AOL file to disk
							file.sync_all()?;
							// Reset the pending syncs counter
							self.pending_syncs.store(0, Ordering::Release);
						} else {
							// Increment the pending syncs counter
							self.pending_syncs.fetch_add(1, Ordering::Release);
						}
					}
				}
			}
		}
		// All ok
		Ok(())
	}
}

impl Drop for Persistence {
	/// Cleans up resources when the persistence layer is dropped
	fn drop(&mut self) {
		// Signal shutdown to the worker threads
		self.background_threads_enabled.store(false, Ordering::Release);
		// Stop the fsync worker if it exists
		if let Some(handle) = self.fsync_handle.write().take() {
			handle.thread().unpark();
			let _ = handle.join();
		}
		// Stop the snapshot worker if it exists
		if let Some(handle) = self.snapshot_handle.write().take() {
			handle.thread().unpark();
			let _ = handle.join();
		}
		// Stop the async append worker if it exists
		if let Some(handle) = self.appender_handle.write().take() {
			handle.thread().unpark();
			let _ = handle.join();
		}
		// Perform final fsync if there are pending syncs
		if self.aol_mode != AolMode::Never && self.pending_syncs.load(Ordering::Acquire) > 0 {
			// Try to acquire lock on AOL file
			if let Some(ref aol) = self.aol {
				// Lock the AOL file
				if let Ok(file) = aol.lock() {
					// Sync file contents to disk
					let _ = file.sync_all();
				}
			}
		}
	}
}
