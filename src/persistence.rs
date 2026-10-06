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

#![cfg(not(target_arch = "wasm32"))]

use crate::compression::CompressedReader;
use crate::compression::CompressedWriter;
use crate::compression::CompressionMode;
use crate::err::PersistenceError;
use crate::inner::Inner;
use crate::sync::RwLock;
use crate::version::Version;
use crate::version_cell::VersionCell;
use crate::versions::Versions;
use bincode::config;
use byteslice::ByteSlice;
use crossbeam_deque::{Injector, Steal};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, JoinHandle};
use web_time::{Duration, Instant};
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// Opens the framed section of an append-only log file.
///
/// A log file is a run of legacy records (each a bare `(key, version, value)`
/// tuple, with nothing to say where a transaction ends) followed by this
/// header and then a run of frames. A file written from scratch has no legacy
/// run and starts with the header. The first byte is `0xFF`, which the
/// variable-length integer encoding of a legacy record's leading key length
/// never produces, so the header cannot be mistaken for the start of a legacy
/// record. The last byte is the format version.
const AOL_MAGIC: [u8; 8] = [0xFF, b'S', b'M', b'X', b'A', b'O', b'L', 1];

/// The size of the header in front of each frame: the payload length as a
/// little-endian `u32`, then its checksum as a little-endian `u32`.
const FRAME_HEADER_LEN: usize = 8;

/// A frame holds every record of one committed transaction, so that a
/// transaction is replayed whole or not at all. Its payload is a run of
/// `(key, version, value)` records, all carrying the transaction's version.
fn encode_frame(
	buf: &mut Vec<u8>,
	version: u64,
	writeset: &BTreeMap<ByteSlice, Option<ByteSlice>>,
) -> Result<(), PersistenceError> {
	// Reserve the header, which is filled in once the payload length is known
	let start = buf.len();
	let payload_start = start + FRAME_HEADER_LEN;
	buf.resize(payload_start, 0);
	for (k, v) in writeset {
		bincode::serde::encode_into_std_write((k, version, v), &mut *buf, config::standard())?;
	}
	let payload = &buf[payload_start..];
	let Ok(len) = u32::try_from(payload.len()) else {
		buf.truncate(start);
		return Err(PersistenceError::AppendFailed(
			"transaction is too large for a single append-only log frame".to_owned(),
		));
	};
	let checksum = frame_checksum(payload);
	buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
	buf[start + 4..payload_start].copy_from_slice(&checksum.to_le_bytes());
	Ok(())
}

/// Checksums a frame payload. The payload length seeds the hash, so a frame
/// whose length field was damaged cannot pass as another valid frame.
fn frame_checksum(payload: &[u8]) -> u32 {
	let [a, b, c, d, ..] = xxh3_64_with_seed(payload, payload.len() as u64).to_le_bytes();
	u32::from_le_bytes([a, b, c, d])
}

/// Appends whole frames to the end of the log, opening a file that is still
/// empty with the header. A failed write is undone by cutting the file back to
/// its previous length, so that a partial frame is never left in front of the
/// frames that follow it.
fn append_to_log<'a>(
	file: &mut File,
	chunks: impl IntoIterator<Item = &'a [u8]>,
) -> io::Result<()> {
	let start = file.seek(SeekFrom::End(0))?;
	let result = (|| {
		if start == 0 {
			file.write_all(&AOL_MAGIC)?;
		}
		for chunk in chunks {
			file.write_all(chunk)?;
		}
		file.flush()
	})();
	if result.is_err() {
		let _ = file.set_len(start);
	}
	result
}

/// Fills `buf` as far as the reader allows, returning the number of bytes read,
/// which is short only at the end of the file.
fn read_up_to<R: Read>(reader: &mut R, buf: &mut [u8]) -> io::Result<usize> {
	let mut filled = 0;
	while filled < buf.len() {
		match reader.read(&mut buf[filled..]) {
			Ok(0) => break,
			Ok(n) => filled += n,
			Err(e) if e.kind() == ErrorKind::Interrupted => {}
			Err(e) => return Err(e),
		}
	}
	Ok(filled)
}

/// A buffered reader over the log which tracks how many bytes have been
/// consumed, so that replay knows the offset at which each record ends.
struct AolReader {
	inner: BufReader<File>,
	position: u64,
}

impl AolReader {
	fn new(file: File) -> Self {
		Self {
			inner: BufReader::new(file),
			position: 0,
		}
	}

	/// The next byte, without consuming it. `None` at the end of the file.
	fn peek(&mut self) -> io::Result<Option<u8>> {
		Ok(self.inner.fill_buf()?.first().copied())
	}
}

impl Read for AolReader {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let n = self.inner.read(buf)?;
		self.position += n as u64;
		Ok(n)
	}
}

/// The outcome of reading one frame
enum FrameRead {
	/// The file ended cleanly before this frame
	End,
	/// A complete frame whose checksum matches, with its payload in the buffer
	Valid,
	/// The file ends inside this frame
	Incomplete,
	/// A complete frame which is empty or whose checksum does not match
	Invalid,
}

/// Reads the next frame, leaving its payload in `payload`. The reader is left
/// at the end of the frame for `Valid` and `Invalid`.
fn read_frame(
	reader: &mut AolReader,
	file_len: u64,
	payload: &mut Vec<u8>,
) -> io::Result<FrameRead> {
	let mut header = [0u8; FRAME_HEADER_LEN];
	let read = read_up_to(reader, &mut header)?;
	if read == 0 {
		return Ok(FrameRead::End);
	}
	if read < FRAME_HEADER_LEN {
		return Ok(FrameRead::Incomplete);
	}
	let [l0, l1, l2, l3, c0, c1, c2, c3] = header;
	let len = u32::from_le_bytes([l0, l1, l2, l3]);
	let checksum = u32::from_le_bytes([c0, c1, c2, c3]);
	// A length beyond the end of the file is never allocated for
	if u64::from(len) > file_len.saturating_sub(reader.position) {
		return Ok(FrameRead::Incomplete);
	}
	payload.resize(len as usize, 0);
	if read_up_to(reader, payload)? < payload.len() {
		return Ok(FrameRead::Incomplete);
	}
	if len == 0 || frame_checksum(payload) != checksum {
		return Ok(FrameRead::Invalid);
	}
	Ok(FrameRead::Valid)
}

/// A decoded append-only log record: a key, the version it was written at, and
/// its value, which is `None` for a delete.
type AolEntry = (ByteSlice, u64, Option<ByteSlice>);

/// What replaying the append-only log found
struct AolReplay {
	/// The length of the prefix of the file made up only of complete, valid
	/// records and frames
	valid_len: u64,
	/// Whether the framed section's header lies within that prefix
	framed: bool,
}

/// Represents a pending asynchronous append operation
#[derive(Debug, Clone)]
pub(crate) struct AsyncAppendOperation {
	pub version: u64,
	pub writeset: BTreeMap<ByteSlice, Option<ByteSlice>>,
}

/// A slot representing a synchronous commit request waiting in a group commit
/// batch.
struct SyncCommitSlot {
	data: Vec<u8>,
	done: AtomicBool,
	thread: thread::Thread,
	error: Mutex<Option<PersistenceError>>,
}

/// Coordinates group commits across concurrent threads for AOL synchronous
/// persistence.
pub(crate) struct GroupCommitter {
	queue: Mutex<Vec<Arc<SyncCommitSlot>>>,
	flushing: AtomicBool,
	pending_syncs: Arc<AtomicU64>,
}

thread_local! {
	static ENCODE_BUF: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

impl GroupCommitter {
	pub const fn new(pending_syncs: Arc<AtomicU64>) -> Self {
		Self {
			queue: Mutex::new(Vec::new()),
			flushing: AtomicBool::new(false),
			pending_syncs,
		}
	}

	pub fn commit(&self, aol: &Mutex<File>, data: Vec<u8>) -> Result<(), PersistenceError> {
		let current_slot = Arc::new(SyncCommitSlot {
			data,
			done: AtomicBool::new(false),
			thread: thread::current(),
			error: Mutex::new(None),
		});

		{
			let mut q = self.queue.lock()?;
			q.push(Arc::clone(&current_slot));
		}

		while !current_slot.done.load(Ordering::Acquire) {
			if self
				.flushing
				.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
				.is_ok()
			{
				self.run_flusher(aol);
				break;
			}
			if !current_slot.done.load(Ordering::Acquire) {
				thread::park();
			}
		}

		let err = current_slot.error.lock()?.take();
		if let Some(err) = err {
			return Err(err);
		}

		Ok(())
	}

	fn run_flusher(&self, aol: &Mutex<File>) {
		loop {
			let batch: Vec<Arc<SyncCommitSlot>> = {
				let Ok(mut q) = self.queue.lock() else {
					self.flushing.store(false, Ordering::Release);
					break;
				};
				if q.is_empty() {
					self.flushing.store(false, Ordering::Release);
					break;
				}
				std::mem::take(&mut *q)
			};

			let flush_result = (|| -> Result<(), PersistenceError> {
				let mut file = aol.lock()?;
				append_to_log(&mut file, batch.iter().map(|slot| slot.data.as_slice()))?;
				file.sync_all()?;
				drop(file);
				self.pending_syncs.store(0, Ordering::Release);
				Ok(())
			})();

			match flush_result {
				Ok(()) => {
					for slot in batch {
						slot.done.store(true, Ordering::Release);
						slot.thread.unpark();
					}
				}
				Err(err) => {
					let err_msg = err.to_string();
					for slot in batch {
						if let Ok(mut slot_err) = slot.error.lock() {
							*slot_err = Some(PersistenceError::AppendFailed(err_msg.clone()));
						}
						slot.done.store(true, Ordering::Release);
						slot.thread.unpark();
					}
				}
			}
		}
	}
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
	pub const fn with_aol_mode(mut self, mode: AolMode) -> Self {
		self.aol_mode = mode;
		self
	}

	/// Set the snapshot behavior mode
	pub const fn with_snapshot_mode(mut self, mode: SnapshotMode) -> Self {
		self.snapshot_mode = mode;
		self
	}

	/// Set the fsync mode
	pub const fn with_fsync_mode(mut self, mode: FsyncMode) -> Self {
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
	pub const fn with_compression(mut self, mode: CompressionMode) -> Self {
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
	/// Weak reference to the inner database state. The database state owns
	/// this layer through [`Inner::persistence`], so a strong reference here
	/// would form a cycle that keeps both alive after the database is dropped.
	pub(crate) inner: Weak<Inner>,
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
	/// Group commit coordinator for synchronous AOL appends
	pub(crate) group_committer: Arc<GroupCommitter>,
}

impl Persistence {
	/// Creates a new persistence layer with custom options
	///
	/// # Arguments
	/// * `options` - Configuration options for persistence
	/// * `inner` - Reference to the database state, which is loaded into here
	///   and which the layer then tracks only weakly
	///
	/// # Returns
	/// * `Result<Self, PersistenceError>` - The created persistence layer or an
	///   error
	pub(crate) fn new_with_options(
		options: PersistenceOptions,
		inner: &Arc<Inner>,
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
		let aol = if matches!(options.aol_mode, AolMode::Never) {
			None
		} else {
			// Ensure parent directories exist for AOL path
			if let Some(parent) = aol_path.parent() {
				fs::create_dir_all(parent)?;
			}
			// Open the AOL file with read and write access (avoid append(true)
			// so set_len works on Windows)
			let mut file = OpenOptions::new()
				.create(true)
				.read(true)
				.write(true)
				.truncate(false)
				.open(&aol_path)?;
			file.seek(SeekFrom::End(0))?;
			Some(Arc::new(Mutex::new(file)))
		};
		// Ensure parent directories exist for snapshot path
		if let Some(parent) = snapshot_path.parent() {
			fs::create_dir_all(parent)?;
		}
		let pending_syncs = Arc::new(AtomicU64::new(0));
		let group_committer = Arc::new(GroupCommitter::new(Arc::clone(&pending_syncs)));
		// Create the persistence instance
		let this = Self {
			inner: Arc::downgrade(inner),
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
			pending_syncs,
			async_append_injector: Arc::new(Injector::new()),
			group_committer,
		};
		// Load existing data from disk
		this.load(inner)?;
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
	/// 4. Truncates AOL only up to the cutoff position, preserving newer
	///    entries
	///
	/// # Returns
	/// * `Result<(), PersistenceError>` - Success or an error
	pub fn snapshot(&self) -> Result<(), PersistenceError> {
		// Hold the database state for the duration of the snapshot. It is
		// gone once the database and all of its transactions are dropped.
		let inner = self.inner.upgrade().ok_or_else(|| {
			PersistenceError::SnapshotFailed("the database has been dropped".to_string())
		})?;
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
			for entry in &inner.datastore {
				// Persist only the latest committed version of each key. Keys
				// whose newest entry is a delete tombstone are omitted: on
				// reload the key is simply absent, which is the same
				// observable state. The encoded element type is unchanged, so
				// snapshot files remain readable across releases in both
				// directions.
				// `latest` returns owned values, so the per-key version read
				// guard is released at the end of this
				// statement rather than being held across
				// the encode and write below.
				let latest = entry.value().read(Versions::latest);
				if let Some((version, value)) = latest {
					// Serialize and write this single entry
					bincode::serde::encode_into_std_write(
						&(entry.key().clone(), vec![(version, Some(value))]),
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
			// Sync the renamed file to disk for durability (write access
			// required on Windows for FlushFileBuffers)
			{
				let final_file = OpenOptions::new().write(true).open(&self.snapshot_path)?;
				final_file.sync_all()?;
			}
			// Truncate AOL only up to the cutoff position
			Self::truncate(self.aol.as_ref(), aol_cutoff_position, &self.pending_syncs)?;
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
	fn load(&self, inner: &Inner) -> Result<(), PersistenceError> {
		// Decoded record shape of the snapshot file
		type SnapshotEntry = (ByteSlice, Vec<(u64, Option<ByteSlice>)>);
		// Track the maximum version seen across EVERY decoded record —
		// snapshot entries (including keys skipped as tombstone-topped)
		// and every append-only log record (including deletes). The
		// logical clock is seeded from this so newly minted merge
		// versions continue strictly above every persisted version.
		// Seeding from anything less (the last record, or only entries
		// carrying values) would let a new write mint a version below a
		// stale persisted tombstone and silently vanish once the clock
		// crosses it.
		let mut max_version: u64 = 0;
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
					// Attempt to decode the next entry, handling EOF gracefully
					let result: Result<SnapshotEntry, _> =
						bincode::serde::decode_from_std_read(&mut reader, config::standard());
					// Detech any end of file errors
					match result {
						Ok((k, versions)) => {
							// Load only the newest version of each key: older
							// versions in files written by previous releases
							// are unreadable by construction (there is no
							// historical read API), so materializing them
							// would only bloat startup memory. A key whose
							// newest entry is a delete tombstone is skipped
							// entirely - absent and deleted are the same
							// observable state.
							if let Some((version, value)) = versions.into_iter().last() {
								// Count the version towards the clock seed
								// even when the key itself is skipped
								max_version = max_version.max(version);
								// Skip keys which were deleted
								if value.is_some() {
									// Create a new versions entry
									let mut entries = Versions::default();
									// Add the latest version entry
									entries.push(Version {
										version,
										value,
									});
									// Insert the entry into the datastore
									inner.datastore.insert(k, VersionCell::new(entries));
								}
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
		// Replay the append-only log on top of the snapshot
		let replay = self.replay_aol(inner, &mut max_version)?;
		// Guard against corrupted or pathological persisted versions: the
		// slot protocol reserves values near u64::MAX as sentinels, and
		// version minting adds one to the clock. Legitimate versions from
		// wall-clock releases (~1.7e18 nanoseconds) sit six orders of
		// magnitude below this bound.
		if max_version > u64::MAX / 2 {
			return Err(PersistenceError::SnapshotFailed(format!(
				"persisted version {max_version} exceeds the maximum supported version"
			)));
		}
		// Cut away any torn tail, so that the first record appended from here
		// on follows the last valid one rather than the damaged bytes
		self.repair_aol(&replay)?;
		// Seed the logical clock so newly minted merge versions continue
		// strictly above every persisted version. Both the allocation
		// counter and the published clock are seeded: the next claim
		// takes max_version + 1, and its in-order publication advances
		// the clock from max_version. The merge retirement watermark is
		// seeded identically so its in-order advance starts at the first
		// live merge version rather than walking the persisted range.
		// This runs before any transaction or background worker exists;
		// fetch_max is used for safety under refactoring rather than
		// necessity.
		inner.oracle.alloc.fetch_max(max_version, Ordering::SeqCst);
		inner.oracle.timestamp.fetch_max(max_version, Ordering::SeqCst);
		inner.merge_retire_id.fetch_max(max_version, Ordering::SeqCst);
		// Collapse the multi-version chains that append-only-log replay
		// builds up: a key updated N times across the log holds N chain
		// entries here, and no future commit or tracked sweep would ever
		// visit the ones on keys that are never written again. This runs
		// before any transaction exists, so the cleanup bound is simply
		// the seeded clock and every chain trims to its latest version.
		if let Some(cleanup_ts) = inner.compute_cleanup_ts() {
			inner.run_gc_full(cleanup_ts);
		}
		// Return success
		Ok(())
	}

	/// Replays the append-only log into the datastore.
	///
	/// A transaction is applied only once its whole frame has been read and its
	/// checksum verified, so a log which ends part-way through a transaction
	/// replays as though that transaction never committed. Replay stops at the
	/// first damaged frame; the returned [`AolReplay`] says where the valid
	/// part of the file ends, so that the damage can be cut away.
	///
	/// Damage is only treated as a torn tail when nothing valid follows it. A
	/// complete frame which fails its check but is followed by an intact one
	/// has been corrupted in place, and is reported as an error rather than
	/// being dropped together with the commits that came after it.
	fn replay_aol(
		&self,
		inner: &Inner,
		max_version: &mut u64,
	) -> Result<AolReplay, PersistenceError> {
		let mut replay = AolReplay {
			valid_len: 0,
			framed: false,
		};
		// Check if append-only file exists
		if !self.aol_path.exists() {
			return Ok(replay);
		}
		// Open and read the AOL file
		let file = File::open(&self.aol_path)?;
		// Get the length of the append-only file
		let file_len = file.metadata()?.len();
		// Check if the append-only file is empty
		if file_len == 0 {
			return Ok(replay);
		}
		// Create buffered reader which tracks the offset it has reached
		let mut reader = AolReader::new(file);
		// Initialize counters for tracking loaded entries
		let mut count = 0;
		// Read the legacy records which precede the framed section, if any.
		// These carry no framing, so each is applied as soon as it is decoded.
		loop {
			match reader.peek()? {
				// The log ends cleanly, with no framed section
				None => return Ok(replay),
				// The framed section begins
				Some(byte) if byte == AOL_MAGIC[0] => break,
				Some(_) => {}
			}
			// Increment the counter
			count += 1;
			// Trace the loading of the append-only entry
			tracing::trace!("Loading AOL entry: {count}");
			// Explicitly type the result to help type inference
			let result: Result<AolEntry, _> =
				bincode::serde::decode_from_std_read(&mut reader, config::standard());
			// Detect any end of file errors
			match result {
				Ok((k, version, val)) => {
					Self::apply_aol_entry(inner, k, version, val, max_version);
					replay.valid_len = reader.position;
				}
				Err(e) => match e {
					// A record cut short by the end of the file is a torn tail
					bincode::error::DecodeError::Io {
						inner,
						..
					} if inner.kind() == ErrorKind::UnexpectedEof => {
						return Ok(replay);
					}
					e => return Err(PersistenceError::Deserialization(e)),
				},
			}
		}
		// Read the header opening the framed section. A header cut short by the
		// end of the file is a torn tail.
		let mut magic = [0u8; AOL_MAGIC.len()];
		if read_up_to(&mut reader, &mut magic)? < magic.len() {
			return Ok(replay);
		}
		if magic[..AOL_MAGIC.len() - 1] != AOL_MAGIC[..AOL_MAGIC.len() - 1] {
			return Err(PersistenceError::Corrupted(
				"the append-only log does not have a recognised header".to_owned(),
			));
		}
		if magic[AOL_MAGIC.len() - 1] != AOL_MAGIC[AOL_MAGIC.len() - 1] {
			return Err(PersistenceError::Corrupted(format!(
				"the append-only log is of format version {}, which is not supported",
				magic[AOL_MAGIC.len() - 1]
			)));
		}
		replay.framed = true;
		replay.valid_len = reader.position;
		// Read and apply each frame in the AOL
		let mut payload = Vec::new();
		let mut entries: Vec<AolEntry> = Vec::new();
		loop {
			match read_frame(&mut reader, file_len, &mut payload)? {
				// The log ends cleanly after the last frame, or part-way through
				// one, which is a torn tail
				FrameRead::End | FrameRead::Incomplete => break,
				// A damaged frame is a torn tail unless valid data follows it
				FrameRead::Invalid => {
					if reader.position < file_len
						&& matches!(
							read_frame(&mut reader, file_len, &mut payload)?,
							FrameRead::Valid
						) {
						return Err(PersistenceError::Corrupted(format!(
							"the append-only log has a damaged frame at offset {} which is followed by valid data",
							replay.valid_len
						)));
					}
					break;
				}
				FrameRead::Valid => {
					// Increment the counter
					count += 1;
					// Trace the loading of the append-only frame
					tracing::trace!("Loading AOL frame: {count}");
					// Decode every record of the frame before applying any
					// of them, so a frame is never partially applied
					entries.clear();
					let mut offset = 0;
					while offset < payload.len() {
						let (entry, read): (AolEntry, usize) = bincode::serde::decode_from_slice(
							&payload[offset..],
							config::standard(),
						)?;
						offset += read;
						entries.push(entry);
					}
					for (k, version, val) in entries.drain(..) {
						Self::apply_aol_entry(inner, k, version, val, max_version);
					}
					replay.valid_len = reader.position;
				}
			}
		}
		Ok(replay)
	}

	/// Applies one record of the append-only log to the datastore
	fn apply_aol_entry(
		inner: &Inner,
		k: ByteSlice,
		version: u64,
		val: Option<ByteSlice>,
		max_version: &mut u64,
	) {
		// Count the version towards the clock seed. The append-only log is
		// not version-ordered (async appends race), so the maximum must be
		// tracked over every record, not taken from the last.
		*max_version = (*max_version).max(version);
		// Check if the key already exists
		if let Some(entry) = inner.datastore.get(&k) {
			// Update existing key with stored version
			entry.value().lock().update(|v| {
				v.push(Version {
					version,
					value: val,
				});
			});
		} else {
			// Insert new key with stored version
			inner.datastore.insert(
				k.clone(),
				VersionCell::new(Versions::from(Version {
					version,
					value: val,
				})),
			);
		}
	}

	/// Cuts the append-only log back to the end of the last valid record or
	/// frame, and makes sure that what follows it is the framed section.
	///
	/// The log is opened for appending from the end of the file, so unless the
	/// damaged bytes of a torn tail are removed first, the next commit lands
	/// directly after them and is unreadable on the following restart. The cut
	/// is synced before anything is appended.
	fn repair_aol(&self, replay: &AolReplay) -> Result<(), PersistenceError> {
		// There is nothing to repair if the log is not in use
		let Some(ref aol) = self.aol else {
			return Ok(());
		};
		let mut file = aol.lock()?;
		let file_len = file.metadata()?.len();
		let mut changed = false;
		// Cut away the torn tail
		if file_len > replay.valid_len {
			tracing::warn!(
				"Removing {} bytes of incomplete data from the end of the append-only log {}",
				file_len - replay.valid_len,
				self.aol_path.display()
			);
			file.set_len(replay.valid_len)?;
			changed = true;
		}
		// Records written before frames existed are followed by the header, so
		// that everything appended from here on is framed
		file.seek(SeekFrom::Start(replay.valid_len))?;
		if replay.valid_len > 0 && !replay.framed {
			file.write_all(&AOL_MAGIC)?;
			changed = true;
		}
		if changed {
			file.flush()?;
			file.sync_all()?;
		}
		file.seek(SeekFrom::End(0))?;
		drop(file);
		Ok(())
	}

	/// Truncate the AOL file up to the specified position, preserving any data
	/// after.
	#[expect(
		clippy::significant_drop_tightening,
		reason = "the file guard must cover the pending_syncs store, or a concurrent append's increment is silently discarded along with its required fsync"
	)]
	fn truncate(
		aol: Option<&Arc<Mutex<File>>>,
		position: u64,
		pending_syncs: &Arc<AtomicU64>,
	) -> Result<(), PersistenceError> {
		// Check that we have a AOL file handle
		if let Some(aol) = aol {
			// Lock the AOL file. The mutex is deliberately held past its last
			// file use so that it still covers the `pending_syncs` store below.
			let mut file = aol.lock()?;
			// Get the current file length
			let file_len = file.metadata()?.len();
			// Check if there is remaining data
			if file_len <= position {
				// Truncate the AOL file
				file.set_len(0)?;
				// Flush the file contents
				file.flush()?;
			} else if position > 0 {
				// Everything after the cutoff is kept. With nothing before the
				// cutoff there is nothing to cut, and the data already opens
				// with the header.
				static TRUNCATE_COUNTER: AtomicU64 = AtomicU64::new(0);
				let id = TRUNCATE_COUNTER.fetch_add(1, Ordering::Relaxed);
				// Generate a unique name for the temporary file
				let name = format!("aol_truncate_{}_{id}.tmp", std::process::id());
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
					// The remaining data starts on a frame boundary, so the
					// file is reopened with the header in
					// front of it
					file.write_all(&AOL_MAGIC)?;
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
				let aol = Arc::clone(aol);
				let pending_syncs = Arc::clone(&self.pending_syncs);
				let enabled = Arc::clone(&self.background_threads_enabled);
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
			// Clone necessary fields for the worker thread. The database
			// state is held weakly and only upgraded for the duration of a
			// snapshot, so an idle worker never keeps it alive.
			let inner = Weak::clone(&self.inner);
			let aol = self.aol.clone();
			let snapshot_path = self.snapshot_path.clone();
			let pending_syncs = Arc::clone(&self.pending_syncs);
			let enabled = Arc::clone(&self.background_threads_enabled);
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
					// Hold the database state for this snapshot, or stop if it
					// has already been dropped
					let Some(db) = inner.upgrade() else {
						break;
					};
					// Create temporary file for atomic swap
					let temp_path = snapshot_path.with_extension("tmp");
					// Ensure clean error handling in closure
					let result = (|| -> Result<(), PersistenceError> {
						// Create temporary file
						let file = File::create(&temp_path)?;
						// Create compressed writer (handles buffering
						// internally)
						let mut writer = CompressedWriter::new(file, compression)?;
						// Get the current position in the AOL file before
						// snapshotting (if AOL enabled)
						let aol_cutoff_position = if let Some(ref aol) = aol {
							aol.lock()?.metadata()?.len()
						} else {
							0
						};
						// Stream write each entry to reduce memory usage
						for entry in &db.datastore {
							// Persist only the latest committed version of
							// each key, omitting keys whose newest entry is
							// a delete tombstone. See `snapshot()` above.
							// `latest` returns owned values, so the per-key
							// version read guard is
							// released at the end of this statement rather than
							// being held across the
							// encode and write below.
							let latest = entry.value().read(Versions::latest);
							if let Some((version, value)) = latest {
								// Serialize and write this single entry
								bincode::serde::encode_into_std_write(
									&(entry.key().clone(), vec![(version, Some(value))]),
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
						fs::rename(&temp_path, &snapshot_path)?;
						// Sync the renamed file to disk for durability (write
						// access required on Windows for FlushFileBuffers)
						{
							let final_file = OpenOptions::new().write(true).open(&snapshot_path)?;
							final_file.sync_all()?;
						}
						// Truncate AOL to the cutoff position
						Self::truncate(aol.as_ref(), aol_cutoff_position, &pending_syncs)?;
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

	/// Spawn the background worker thread for processing async append
	/// operations
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
				let injector = Arc::clone(&self.async_append_injector);
				let aol = Arc::clone(aol);
				let fsync_mode = self.fsync_mode;
				let enabled = Arc::clone(&self.background_threads_enabled);
				let pending_syncs = Arc::clone(&self.pending_syncs);
				let last_fsync = Arc::clone(&self.last_fsync);
				// Spawn the background worker thread
				let handle = thread::spawn(move || {
					// Set the batch size
					const BATCH_SIZE: usize = 100;
					// Initialize the batch vector and reusable scratch buffer
					let mut batch = Vec::with_capacity(BATCH_SIZE);
					let mut scratch = Vec::with_capacity(8192);
					loop {
						// Clear the batch
						batch.clear();
						// Collect operations into a batch
						loop {
							// Read the shutdown flag before looking at the
							// queue. Every commit
							// acknowledged before shutdown began was
							// queued before the flag flipped, so an empty queue
							// seen after the flag was observed is a drained
							// one.
							let shutting_down = !enabled.load(Ordering::Acquire);
							match injector.steal() {
								Steal::Retry => {
									std::thread::yield_now();
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
									// Stop only once the queue has been
									// drained,
									// so that shutdown never discards a commit
									if shutting_down {
										return;
									}
									// Park the thread to wait for work event
									// notification
									thread::park();
								}
							}
						}
						// Process the batch if we have operations
						if !batch.is_empty() {
							// Ensure clean error handling in closure
							let result = (|| -> Result<(), PersistenceError> {
								// Lock the AOL file for writing
								if let Ok(mut file) = aol.lock() {
									scratch.clear();
									// Write all operations in the batch into
									// reusable scratch buffer, one frame
									// for each transaction
									for op in &batch {
										encode_frame(&mut scratch, op.version, &op.writeset)?;
									}
									// Write encoded batch in a single operation
									append_to_log(&mut file, [scratch.as_slice()])?;
									// Handle fsync based on mode
									match fsync_mode {
										// Let the operating system handle syncing to disk
										FsyncMode::Never => {
											// No fsync, just increment pending
											// counter
											pending_syncs.fetch_add(1, Ordering::Release);
										}
										// Sync immediately to disk after every append
										FsyncMode::EveryAppend => {
											// Sync immediately
											file.sync_all()?;
										}
										// Force sync to disk at a specified interval
										FsyncMode::Interval(duration) => {
											// Check if we should sync based on
											// time
											let now = Instant::now();
											// Check if we should sync based on
											// time
											let should_sync = {
												// Get the last fsync time
												let mut last_fsync = last_fsync.lock()?;
												// Check if the last fsync time
												// is greater than the
												// duration
												if now.duration_since(*last_fsync) >= duration {
													// Update the last fsync
													// time
													*last_fsync = now;
													true
												} else {
													false
												}
											};
											// Check if we should sync
											if should_sync {
												// Force sync the AOL file to
												// disk
												file.sync_all()?;
												// Reset the pending syncs
												// counter
												pending_syncs.store(0, Ordering::Release);
											} else {
												// Increment the pending syncs
												// counter
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
	#[expect(
		clippy::significant_drop_tightening,
		reason = "the file guard must cover the pending_syncs store, or a concurrent append's increment is silently discarded along with its required fsync"
	)]
	pub(crate) fn append(
		&self,
		version: u64,
		writeset: &BTreeMap<ByteSlice, Option<ByteSlice>>,
	) -> Result<(), PersistenceError> {
		// Skip AOL writing if AOL is disabled or writeset is empty
		if self.aol_mode == AolMode::Never || writeset.is_empty() {
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
				return Ok(());
			}
			if self.aol_mode == AolMode::SynchronousOnCommit {
				// Pre-encode the writeset as a single frame in a reusable
				// thread-local scratch buffer
				let data = ENCODE_BUF.with(|buf| {
					let mut b = buf.borrow_mut();
					b.clear();
					encode_frame(&mut b, version, writeset)?;
					Ok::<_, PersistenceError>(b.clone())
				})?;

				// If fsync mode is EveryAppend, use the GroupCommitter to
				// coalesce concurrent commits
				if self.fsync_mode == FsyncMode::EveryAppend {
					self.group_committer.commit(aol, data)?;
					return Ok(());
				}

				// Lock the AOL file for writing without group fsync
				let mut file = aol.lock()?;
				append_to_log(&mut file, [data.as_slice()])?;

				// Handle fsync based on mode
				match self.fsync_mode {
					// Let the operating system handle syncing to disk
					FsyncMode::Never => {
						// No fsync, just increment pending counter
						self.pending_syncs.fetch_add(1, Ordering::Release);
					}
					FsyncMode::EveryAppend => unreachable!(),
					// Force sync to disk at a specified interval
					FsyncMode::Interval(duration) => {
						// Check if we should sync based on time
						let now = Instant::now();
						// Check if we should sync based on time
						let should_sync = {
							// Get the last fsync time
							let mut last_fsync = self.last_fsync.lock()?;
							// Check if the last fsync time is greater than the
							// duration
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
		// Stop the fsync worker if it exists. The `take` is hoisted into its
		// own statement so the write guard is released before the blocking
		// `join`, instead of being held across it.
		let fsync = self.fsync_handle.write().take();
		if let Some(handle) = fsync {
			handle.thread().unpark();
			let _ = handle.join();
		}
		// Stop the snapshot worker if it exists
		let snapshot = self.snapshot_handle.write().take();
		if let Some(handle) = snapshot {
			handle.thread().unpark();
			let _ = handle.join();
		}
		// Stop the async append worker if it exists
		let appender = self.appender_handle.write().take();
		if let Some(handle) = appender {
			handle.thread().unpark();
			let _ = handle.join();
		}
		// Perform final fsync if there are pending syncs
		if self.aol_mode != AolMode::Never && self.pending_syncs.load(Ordering::Acquire) > 0 {
			// Try to acquire lock on AOL file
			if let Some(ref aol) = self.aol {
				// Lock the AOL file
				if let Ok(file) = aol.lock() {
					// Sync file contents to disk, then clear the counter
					// shared by every clone of this layer so the next one
					// to drop does not sync the same writes again
					if file.sync_all().is_ok() {
						self.pending_syncs.store(0, Ordering::Release);
					}
				}
			}
		}
	}
}
