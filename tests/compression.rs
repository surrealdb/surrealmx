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
//! Compression tests for SurrealMX.
//!
//! Tests LZ4 compression for snapshots and compression round-trips.

use bytes::Bytes;
use surrealmx::{
	AolMode, CompressionMode, Database, DatabaseOptions, PersistenceOptions, SnapshotMode,
};
use tempfile::TempDir;

// =============================================================================
// LZ4 Snapshot Tests
// =============================================================================
#[test]

fn lz4_snapshot_round_trip() {

	let temp_dir = TempDir::new().unwrap();

	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();

	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_compression(CompressionMode::Lz4);

	// Create database and add data
	{

		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);

		for i in 0..100 {

			tx.set(format!("key_{:04}", i), format!("value_{}", i)).unwrap();
		}

		tx.commit().unwrap();

		// Create LZ4-compressed snapshot
		if let Some(persistence) = db.persistence() {

			persistence.snapshot().unwrap();
		}
	}

	// Verify LZ4 magic bytes in snapshot file
	let snapshot_path = temp_path.join("snapshot.bin");

	assert!(snapshot_path.exists(), "Snapshot file should exist");

	let snapshot_bytes = std::fs::read(&snapshot_path).unwrap();

	// LZ4 magic number: 0x04 0x22 0x4D 0x18 (little endian)
	assert!(snapshot_bytes.len() >= 4, "Snapshot should have at least 4 bytes for magic number");

	assert_eq!(&snapshot_bytes[0..4], &[0x04, 0x22, 0x4D, 0x18], "Should have LZ4 magic bytes");

	// Recover and verify data
	{

		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);

		for i in 0..100 {

			let expected = Bytes::from(format!("value_{}", i));

			let actual = tx.get(format!("key_{:04}", i)).unwrap();

			assert_eq!(actual, Some(expected), "Key {} should be recovered", i);
		}

		tx.cancel().unwrap();
	}
}

#[test]

fn uncompressed_snapshot_round_trip() {

	let temp_dir = TempDir::new().unwrap();

	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();

	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_compression(CompressionMode::None);

	// Create database and add data
	{

		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);

		for i in 0..100 {

			tx.set(format!("key_{:04}", i), format!("value_{}", i)).unwrap();
		}

		tx.commit().unwrap();

		// Create uncompressed snapshot
		if let Some(persistence) = db.persistence() {

			persistence.snapshot().unwrap();
		}
	}

	// Verify NOT LZ4 (no magic bytes)
	let snapshot_path = temp_path.join("snapshot.bin");

	let snapshot_bytes = std::fs::read(&snapshot_path).unwrap();

	if snapshot_bytes.len() >= 4 {

		assert_ne!(
			&snapshot_bytes[0..4],
			&[0x04, 0x22, 0x4D, 0x18],
			"Should NOT have LZ4 magic bytes"
		);
	}

	// Recover and verify data
	{

		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);

		for i in 0..100 {

			let expected = Bytes::from(format!("value_{}", i));

			let actual = tx.get(format!("key_{:04}", i)).unwrap();

			assert_eq!(actual, Some(expected));
		}

		tx.cancel().unwrap();
	}
}

// =============================================================================
// Large Data Compression Tests
// =============================================================================
#[test]

fn lz4_compression_with_large_values() {

	let temp_dir = TempDir::new().unwrap();

	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();

	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_compression(CompressionMode::Lz4);

	// Large, compressible data (repeated pattern)
	let large_value: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

	// Create database with large values
	{

		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);

		for i in 0..50 {

			tx.set(format!("key_{}", i), large_value.clone()).unwrap();
		}

		tx.commit().unwrap();

		if let Some(persistence) = db.persistence() {

			persistence.snapshot().unwrap();
		}
	}

	// Check that snapshot is smaller than uncompressed would be
	let snapshot_path = temp_path.join("snapshot.bin");

	let snapshot_size = std::fs::metadata(&snapshot_path).unwrap().len();

	// Uncompressed would be at least 50 * 10000 = 500,000 bytes
	// LZ4 should compress repeated data significantly
	println!("Snapshot size with LZ4: {} bytes", snapshot_size);

	assert!(
		snapshot_size < 300_000,
		"LZ4 should compress repeated data well, got {} bytes",
		snapshot_size
	);

	// Verify recovery
	{

		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);

		for i in 0..50 {

			let actual = tx.get(format!("key_{}", i)).unwrap();

			assert_eq!(actual, Some(Bytes::from(large_value.clone())));
		}

		tx.cancel().unwrap();
	}
}

#[test]

fn lz4_compression_with_random_data() {

	let temp_dir = TempDir::new().unwrap();

	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();

	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_compression(CompressionMode::Lz4);

	// Less compressible data (pseudo-random using a simple pattern)
	let random_value: Vec<u8> = (0..1000).map(|i| ((i * 17 + 31) % 256) as u8).collect();

	// Create database
	{

		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);

		for i in 0..100 {

			// Mix the base value to make each entry somewhat unique
			let mut val = random_value.clone();

			val[0] = (i % 256) as u8;

			val[1] = ((i / 256) % 256) as u8;

			tx.set(format!("key_{:04}", i), val).unwrap();
		}

		tx.commit().unwrap();

		if let Some(persistence) = db.persistence() {

			persistence.snapshot().unwrap();
		}
	}

	// Verify recovery (compression ratio doesn't matter, integrity does)
	{

		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);

		for i in 0..100 {

			let mut expected = random_value.clone();

			expected[0] = (i % 256) as u8;

			expected[1] = ((i / 256) % 256) as u8;

			let actual = tx.get(format!("key_{:04}", i)).unwrap();

			assert_eq!(actual, Some(Bytes::from(expected)));
		}

		tx.cancel().unwrap();
	}
}

// =============================================================================
// Auto-Detection Tests
// =============================================================================
#[test]

fn auto_detect_lz4_compressed_snapshot() {

	let temp_dir = TempDir::new().unwrap();

	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();

	// Write with LZ4
	let persistence_opts_lz4 = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_compression(CompressionMode::Lz4);

	{

		let db =
			Database::new_with_persistence(db_opts.clone(), persistence_opts_lz4.clone()).unwrap();

		let mut tx = db.transaction(true);

		tx.set("key", "compressed_value").unwrap();

		tx.commit().unwrap();

		if let Some(persistence) = db.persistence() {

			persistence.snapshot().unwrap();
		}
	}

	// Read with default compression setting (should auto-detect LZ4)
	let persistence_opts_default = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never);

	// Note: CompressionMode for reading is auto-detected, write mode defaults to
	// None
	{

		let db = Database::new_with_persistence(db_opts, persistence_opts_default).unwrap();

		let mut tx = db.transaction(false);

		let val = tx.get("key").unwrap();

		assert_eq!(
			val,
			Some(Bytes::from("compressed_value")),
			"Should auto-detect and read LZ4 snapshot"
		);

		tx.cancel().unwrap();
	}
}

#[test]

fn auto_detect_uncompressed_snapshot() {

	let temp_dir = TempDir::new().unwrap();

	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();

	// Write uncompressed
	let persistence_opts_none = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_compression(CompressionMode::None);

	{

		let db =
			Database::new_with_persistence(db_opts.clone(), persistence_opts_none.clone()).unwrap();

		let mut tx = db.transaction(true);

		tx.set("key", "uncompressed_value").unwrap();

		tx.commit().unwrap();

		if let Some(persistence) = db.persistence() {

			persistence.snapshot().unwrap();
		}
	}

	// Read with LZ4 compression setting (should auto-detect uncompressed)
	let persistence_opts_lz4 = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_compression(CompressionMode::Lz4);

	{

		let db = Database::new_with_persistence(db_opts, persistence_opts_lz4).unwrap();

		let mut tx = db.transaction(false);

		let val = tx.get("key").unwrap();

		assert_eq!(
			val,
			Some(Bytes::from("uncompressed_value")),
			"Should auto-detect and read uncompressed snapshot"
		);

		tx.cancel().unwrap();
	}
}

// =============================================================================
// Edge Cases
// =============================================================================
#[test]

fn lz4_empty_snapshot() {

	let temp_dir = TempDir::new().unwrap();

	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();

	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_compression(CompressionMode::Lz4);

	// Create empty database snapshot
	{

		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		// No data added - empty snapshot
		if let Some(persistence) = db.persistence() {

			persistence.snapshot().unwrap();
		}
	}

	// Should recover empty database
	{

		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);

		let keys = tx.keys("".."z", None, None).unwrap();

		assert!(keys.is_empty(), "Should recover empty database");

		tx.cancel().unwrap();
	}
}

#[test]

fn lz4_with_binary_data() {

	let temp_dir = TempDir::new().unwrap();

	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();

	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_compression(CompressionMode::Lz4);

	// Binary data including null bytes
	let binary_key: Vec<u8> = vec![0x00, 0x01, 0x02, 0xFF, 0xFE];

	let binary_value: Vec<u8> = vec![0xFF, 0x00, 0xFF, 0x00, 0xAA, 0xBB];

	{

		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);

		tx.set(binary_key.clone(), binary_value.clone()).unwrap();

		tx.commit().unwrap();

		if let Some(persistence) = db.persistence() {

			persistence.snapshot().unwrap();
		}
	}

	// Verify binary data preserved
	{

		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);

		let val = tx.get(binary_key).unwrap();

		assert_eq!(val, Some(Bytes::from(binary_value)));

		tx.cancel().unwrap();
	}
}

#[test]

fn lz4_multiple_snapshots() {

	let temp_dir = TempDir::new().unwrap();

	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();

	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_compression(CompressionMode::Lz4);

	{

		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		// First snapshot
		let mut tx = db.transaction(true);

		tx.set("key1", "value1").unwrap();

		tx.commit().unwrap();

		if let Some(persistence) = db.persistence() {

			persistence.snapshot().unwrap();
		}

		// More data
		let mut tx = db.transaction(true);

		tx.set("key2", "value2").unwrap();

		tx.del("key1").unwrap();

		tx.commit().unwrap();

		// Second snapshot (overwrites first)
		if let Some(persistence) = db.persistence() {

			persistence.snapshot().unwrap();
		}
	}

	// Should recover from latest snapshot
	{

		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);

		assert!(tx.get("key1").unwrap().is_none(), "key1 should be deleted");

		assert_eq!(tx.get("key2").unwrap(), Some(Bytes::from("value2")));

		tx.cancel().unwrap();
	}
}
