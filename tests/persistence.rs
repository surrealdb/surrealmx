#![cfg(not(target_arch = "wasm32"))]

use bytes::Bytes;
use std::time::Duration;
use surrealmx::{AolMode, Database, DatabaseOptions, FsyncMode, PersistenceOptions, SnapshotMode};
use tempfile::TempDir;

#[test]
fn test_aol_synchronous_basic() {
	// Create a temporary directory for testing
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	// Create database options
	let db_opts = DatabaseOptions::default();

	// Configure synchronous AOL persistence (no snapshots)
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_fsync_mode(FsyncMode::EveryAppend);

	// Create persistent database with AOL mode
	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Add some data
	{
		let mut tx = db.transaction(true);
		tx.set("key1", "value1").unwrap();
		tx.set("key2", "value2").unwrap();
		tx.commit().unwrap();
	}

	// Add more data in a separate transaction
	{
		let mut tx = db.transaction(true);
		tx.set("key3", "value3").unwrap();
		tx.del("key1").unwrap(); // Delete key1
		tx.commit().unwrap();
	}

	// Verify data is accessible in current session
	{
		let mut tx = db.transaction(false);
		assert_eq!(tx.get("key1").unwrap(), None); // Should be deleted
		assert_eq!(tx.get("key2").unwrap(), Some(Bytes::from("value2")));
		assert_eq!(tx.get("key3").unwrap(), Some(Bytes::from("value3")));
		tx.cancel().unwrap();
	}

	// Verify AOL file exists and snapshot doesn't
	let aol_path = temp_path.join("aol.bin");
	let snapshot_path = temp_path.join("snapshot.bin");

	assert!(aol_path.exists(), "AOL file should exist");
	assert!(!snapshot_path.exists(), "Snapshot file should not exist in AOL-only mode");

	std::thread::sleep(Duration::from_millis(250));

	// Verify the AOL file has content
	let aol_metadata = std::fs::metadata(&aol_path).unwrap();
	assert!(aol_metadata.len() > 0, "AOL file should not be empty");
}

#[test]
fn test_aol_asynchronous_basic() {
	// Create a temporary directory for testing
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	// Create database options
	let db_opts = DatabaseOptions::default();

	// Configure asynchronous AOL persistence (no snapshots)
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::AsynchronousAfterCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_fsync_mode(FsyncMode::Never);

	// Create persistent database with AOL mode
	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Add some data
	{
		let mut tx = db.transaction(true);
		tx.set("key1", "value1").unwrap();
		tx.set("key2", "value2").unwrap();
		tx.commit().unwrap();
	}

	// Add more data in a separate transaction
	{
		let mut tx = db.transaction(true);
		tx.set("key3", "value3").unwrap();
		tx.del("key1").unwrap(); // Delete key1
		tx.commit().unwrap();
	}

	// Verify data is accessible in current session
	{
		let mut tx = db.transaction(false);
		assert_eq!(tx.get("key1").unwrap(), None); // Should be deleted
		assert_eq!(tx.get("key2").unwrap(), Some(Bytes::from("value2")));
		assert_eq!(tx.get("key3").unwrap(), Some(Bytes::from("value3")));
		tx.cancel().unwrap();
	}

	// Verify AOL file exists and snapshot doesn't
	let aol_path = temp_path.join("aol.bin");
	let snapshot_path = temp_path.join("snapshot.bin");

	assert!(aol_path.exists(), "AOL file should exist");
	assert!(!snapshot_path.exists(), "Snapshot file should not exist in AOL-only mode");

	std::thread::sleep(Duration::from_millis(250));

	// Verify the AOL file has content
	let aol_metadata = std::fs::metadata(&aol_path).unwrap();
	assert!(aol_metadata.len() > 0, "AOL file should not be empty");
}

#[test]
fn test_aol_recovery() {
	// Create a temporary directory for testing
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	// Create database options
	let db_opts = DatabaseOptions::default();

	// Configure AOL persistence
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_fsync_mode(FsyncMode::EveryAppend);

	// Create first database instance and add data
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		let mut tx = db.transaction(true);
		tx.set("recover_key1".to_string(), "recover_value1".to_string()).unwrap();
		tx.set("recover_key2".to_string(), "recover_value2".to_string()).unwrap();
		tx.commit().unwrap();

		// Update a key
		let mut tx = db.transaction(true);
		tx.set("recover_key1".to_string(), "updated_value1".to_string()).unwrap();
		tx.set("recover_key3".to_string(), "recover_value3".to_string()).unwrap();
		tx.commit().unwrap();

		// Delete a key
		let mut tx = db.transaction(true);
		tx.del("recover_key2".to_string()).unwrap();
		tx.commit().unwrap();
	} // Database drops here, releasing all resources

	// Create second database instance from the same directory (simulates restart)
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		// Verify data was recovered from AOL
		let mut tx = db.transaction(false);
		assert_eq!(tx.get("recover_key1").unwrap(), Some(Bytes::from("updated_value1")));
		assert_eq!(tx.get("recover_key2").unwrap(), None); // Should be deleted
		assert_eq!(tx.get("recover_key3").unwrap(), Some(Bytes::from("recover_value3")));
		tx.cancel().unwrap();
	}
}

#[test]
fn test_aol_fsync_modes() {
	// Test FsyncMode::EveryAppend
	{
		let temp_dir = TempDir::new().unwrap();
		let temp_path = temp_dir.path();

		let db_opts = DatabaseOptions::default();
		let persistence_opts = PersistenceOptions::new(temp_path)
			.with_aol_mode(AolMode::SynchronousOnCommit)
			.with_fsync_mode(FsyncMode::EveryAppend);

		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(true);
		tx.set(&b"key_1"[..], "fsync_every_append").unwrap();
		tx.commit().unwrap(); // Should fsync immediately
	}

	// Test FsyncMode::Interval
	{
		let temp_dir = TempDir::new().unwrap();
		let temp_path = temp_dir.path();

		let db_opts = DatabaseOptions::default();
		let persistence_opts = PersistenceOptions::new(temp_path)
			.with_aol_mode(AolMode::SynchronousOnCommit)
			.with_fsync_mode(FsyncMode::Interval(Duration::from_millis(100)));

		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(true);
		tx.set(&b"key_1"[..], "fsync_interval").unwrap();
		tx.commit().unwrap(); // Should not fsync immediately

		// Wait for interval to pass
		std::thread::sleep(Duration::from_millis(200));
	}

	// Test FsyncMode::Never
	{
		let temp_dir = TempDir::new().unwrap();
		let temp_path = temp_dir.path();

		let db_opts = DatabaseOptions::default();
		let persistence_opts = PersistenceOptions::new(temp_path)
			.with_aol_mode(AolMode::SynchronousOnCommit)
			.with_fsync_mode(FsyncMode::Never);

		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(true);
		tx.set(&b"key_1"[..], "fsync_never").unwrap();
		tx.commit().unwrap(); // Should never fsync
	}
}

#[test]
fn test_snapshot_manual_creation() {
	// Create a temporary directory for testing
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	// Create database options
	let db_opts = DatabaseOptions::default();

	// Configure manual snapshot persistence (no AOL, no automatic snapshots)
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never);

	// Create persistent database
	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Add some data
	{
		let mut tx = db.transaction(true);
		tx.set("snap_key1".to_string(), "snap_value1".to_string()).unwrap();
		tx.set("snap_key2".to_string(), "snap_value2".to_string()).unwrap();
		tx.commit().unwrap();
	}

	// Manually create a snapshot
	if let Some(persistence) = db.persistence() {
		persistence.snapshot().unwrap();
	}

	// Verify snapshot file exists and AOL doesn't
	let snapshot_path = temp_path.join("snapshot.bin");
	let aol_path = temp_path.join("aol.bin");

	assert!(snapshot_path.exists(), "Snapshot file should exist");
	assert!(!aol_path.exists(), "AOL file should not exist in snapshot-only mode");

	// Verify the snapshot file has content
	let snapshot_metadata = std::fs::metadata(&snapshot_path).unwrap();
	assert!(snapshot_metadata.len() > 0, "Snapshot file should not be empty");
}

#[test]
fn test_snapshot_only_persistence_basic() {
	// Create a temporary directory for testing
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	// Create database options
	let db_opts = DatabaseOptions::default();

	// Configure snapshot-only persistence (no AOL)
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Interval(Duration::from_secs(60)));

	// Create persistent database with snapshot-only mode
	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Add some data
	{
		let mut tx = db.transaction(true);
		tx.put("key1".to_string(), "value1".to_string()).unwrap();
		tx.put("key2".to_string(), "value2".to_string()).unwrap();
		tx.commit().unwrap();
	}

	// Verify data is accessible in current session
	{
		let mut tx = db.transaction(false);
		assert_eq!(tx.get("key1").unwrap(), Some(Bytes::from("value1")));
		assert_eq!(tx.get("key2").unwrap(), Some(Bytes::from("value2")));
		tx.cancel().unwrap();
	}

	// Trigger a manual snapshot
	if let Some(persistence) = db.persistence() {
		persistence.snapshot().unwrap();
	}

	// Verify snapshot file exists but AOL file doesn't
	let snapshot_path = temp_path.join("snapshot.bin");
	let aol_path = temp_path.join("aol.bin");

	assert!(snapshot_path.exists(), "Snapshot file should exist");
	assert!(!aol_path.exists(), "AOL file should not exist in snapshot-only mode");

	// Verify the snapshot file has content
	let snapshot_metadata = std::fs::metadata(&snapshot_path).unwrap();
	assert!(snapshot_metadata.len() > 0, "Snapshot file should not be empty");

	println!("Successfully created snapshot file with {} bytes", snapshot_metadata.len());
}

#[test]
fn test_snapshot_basic() {
	// Test basic snapshot creation
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never);

	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Add test data
	{
		let mut tx = db.transaction(true);
		for i in 0..100 {
			tx.set(format!("key_{i}"), format!("value_{i}_with_some_data")).unwrap();
		}
		tx.commit().unwrap();
	}

	// Create snapshot
	if let Some(persistence) = db.persistence() {
		persistence.snapshot().unwrap();
	}

	// Verify snapshot file exists and has content
	let snapshot_path = temp_path.join("snapshot.bin");
	assert!(snapshot_path.exists(), "Snapshot file should exist");

	let metadata = std::fs::metadata(&snapshot_path).unwrap();
	assert!(metadata.len() > 0, "Snapshot file should not be empty");

	println!("Snapshot size = {} bytes", metadata.len());
}

#[test]
fn test_snapshot_recovery() {
	// Create a temporary directory for testing
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Never);

	// Create first database instance and add data
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		// Add initial data
		{
			let mut tx = db.transaction(true);
			tx.set("snapshot_key1".to_string(), "snapshot_value1".to_string()).unwrap();
			tx.set("snapshot_key2".to_string(), "snapshot_value2".to_string()).unwrap();
			tx.commit().unwrap();
		}

		// Update data
		{
			let mut tx = db.transaction(true);
			tx.set("snapshot_key1".to_string(), "updated_snapshot_value1".to_string()).unwrap();
			tx.set("snapshot_key3".to_string(), "snapshot_value3".to_string()).unwrap();
			tx.commit().unwrap();
		}

		// Delete data
		{
			let mut tx = db.transaction(true);
			tx.del("snapshot_key2".to_string()).unwrap();
			tx.commit().unwrap();
		}

		// Create snapshot before closing
		if let Some(persistence) = db.persistence() {
			persistence.snapshot().unwrap();
		}
	} // Database drops here

	// Create second database instance from the same directory (simulates restart)
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		// Verify data was recovered from snapshot
		let mut tx = db.transaction(false);
		assert_eq!(
			tx.get("snapshot_key1").unwrap().as_deref(),
			Some(b"updated_snapshot_value1" as &[u8])
		);
		assert_eq!(tx.get("snapshot_key2").unwrap(), None); // Should be deleted
		assert_eq!(tx.get("snapshot_key3").unwrap().as_deref(), Some(b"snapshot_value3" as &[u8]));
		tx.cancel().unwrap();
	}
}

#[test]
fn test_snapshot_interval() {
	// Create a temporary directory for testing
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::Never)
		.with_snapshot_mode(SnapshotMode::Interval(Duration::from_millis(100)));

	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Add some data
	{
		let mut tx = db.transaction(true);
		tx.set("interval_key1".to_string(), "interval_value1".to_string()).unwrap();
		tx.commit().unwrap();
	}

	// Wait for snapshot to be created automatically
	std::thread::sleep(Duration::from_millis(200));

	// Add more data
	{
		let mut tx = db.transaction(true);
		tx.set("interval_key2".to_string(), "interval_value2".to_string()).unwrap();
		tx.commit().unwrap();
	}

	// Wait for another snapshot
	std::thread::sleep(Duration::from_millis(200));

	// Verify snapshot file exists
	let snapshot_path = temp_path.join("snapshot.bin");
	assert!(snapshot_path.exists(), "Snapshot file should exist with interval snapshots");
}

#[test]
fn test_combined_aol_and_snapshot() {
	// Create a temporary directory for testing
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never) // Manual snapshots
		.with_fsync_mode(FsyncMode::EveryAppend);

	// Create first database instance
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		// Add initial data (will go to AOL)
		{
			let mut tx = db.transaction(true);
			tx.set("combined_key1".to_string(), "combined_value1".to_string()).unwrap();
			tx.set("combined_key2".to_string(), "combined_value2".to_string()).unwrap();
			tx.commit().unwrap();
		}

		// Create a snapshot (should truncate AOL)
		if let Some(persistence) = db.persistence() {
			persistence.snapshot().unwrap();
		}

		// Add more data after snapshot (will go to AOL)
		{
			let mut tx = db.transaction(true);
			tx.set("combined_key3".to_string(), "combined_value3".to_string()).unwrap();
			tx.set("combined_key1".to_string(), "updated_combined_value1".to_string()).unwrap();
			tx.commit().unwrap();
		}
	} // Database drops here

	// Verify both snapshot and AOL files exist
	let snapshot_path = temp_path.join("snapshot.bin");
	let aol_path = temp_path.join("aol.bin");

	assert!(snapshot_path.exists(), "Snapshot file should exist");
	assert!(aol_path.exists(), "AOL file should exist");

	// Create second database instance (simulates restart)
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		// Verify data was recovered from both snapshot and AOL
		let mut tx = db.transaction(false);
		assert_eq!(
			tx.get("combined_key1").unwrap().as_deref(),
			Some(b"updated_combined_value1" as &[u8])
		);
		assert_eq!(tx.get("combined_key2").unwrap().as_deref(), Some(b"combined_value2" as &[u8]));
		assert_eq!(tx.get("combined_key3").unwrap().as_deref(), Some(b"combined_value3" as &[u8]));
		tx.cancel().unwrap();
	}
}

#[test]
fn test_aol_snapshot_with_truncation() {
	// Create a temporary directory for testing
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_fsync_mode(FsyncMode::EveryAppend);

	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Add data that will go to AOL
	{
		let mut tx = db.transaction(true);
		for i in 0..10 {
			tx.set(format!("key_{}", i), format!("value_{}", i)).unwrap();
		}
		tx.commit().unwrap();
	}

	// Check AOL file size before snapshot
	let aol_path = temp_path.join("aol.bin");
	let aol_size_before = std::fs::metadata(&aol_path).unwrap().len();
	assert!(aol_size_before > 0, "AOL should have content before snapshot");

	// Create snapshot (should truncate AOL)
	if let Some(persistence) = db.persistence() {
		persistence.snapshot().unwrap();
	}

	// Check AOL file size after snapshot (should be much smaller or empty)
	let aol_size_after = std::fs::metadata(&aol_path).unwrap().len();
	assert!(aol_size_after < aol_size_before, "AOL should be truncated after snapshot");

	// Add more data after snapshot
	{
		let mut tx = db.transaction(true);
		tx.set("post_snapshot_key".to_string(), "post_snapshot_value".to_string()).unwrap();
		tx.commit().unwrap();
	}

	// Verify snapshot exists
	let snapshot_path = temp_path.join("snapshot.bin");
	assert!(snapshot_path.exists(), "Snapshot file should exist");

	// Verify all data is still accessible
	{
		let mut tx = db.transaction(false);
		for i in 0..10 {
			assert_eq!(
				tx.get(format!("key_{}", i)).unwrap(),
				Some(Bytes::from(format!("value_{}", i)))
			);
		}
		assert_eq!(tx.get("post_snapshot_key").unwrap(), Some(Bytes::from("post_snapshot_value")));
		tx.cancel().unwrap();
	}
}

#[test]
fn test_combined_recovery_complex() {
	// Create a temporary directory for testing
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_fsync_mode(FsyncMode::EveryAppend);

	// First session: Add data, snapshot, add more data
	{
		let db = Database::new_with_persistence(db_opts.clone(), persistence_opts.clone()).unwrap();

		// Phase 1: Add initial data
		{
			let mut tx = db.transaction(true);
			tx.set("phase1_key1".to_string(), "phase1_value1".to_string()).unwrap();
			tx.set("phase1_key2".to_string(), "phase1_value2".to_string()).unwrap();
			tx.commit().unwrap();
		}

		// Phase 2: Update and delete some data
		{
			let mut tx = db.transaction(true);
			tx.set("phase1_key1".to_string(), "updated_phase1_value1".to_string()).unwrap();
			tx.del("phase1_key2".to_string()).unwrap();
			tx.set("phase2_key1".to_string(), "phase2_value1".to_string()).unwrap();
			tx.commit().unwrap();
		}

		// Create snapshot
		if let Some(persistence) = db.persistence() {
			persistence.snapshot().unwrap();
		}

		// Phase 3: Add data after snapshot
		{
			let mut tx = db.transaction(true);
			tx.set("phase3_key1".to_string(), "phase3_value1".to_string()).unwrap();
			tx.set("phase1_key1".to_string(), "final_phase1_value1".to_string()).unwrap();
			tx.commit().unwrap();
		}
	} // First session ends

	// Second session: Verify all data is recovered correctly
	{
		let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

		let mut tx = db.transaction(false);
		assert_eq!(
			tx.get("phase1_key1").unwrap().as_deref(),
			Some(b"final_phase1_value1" as &[u8])
		);
		assert_eq!(tx.get("phase1_key2").unwrap(), None); // Should be deleted
		assert_eq!(tx.get("phase2_key1").unwrap(), Some(Bytes::from("phase2_value1")));
		assert_eq!(tx.get("phase3_key1").unwrap(), Some(Bytes::from("phase3_value1")));
		tx.cancel().unwrap();
	}
}

#[test]
fn test_custom_file_paths() {
	// Create a temporary directory for testing
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	// Create custom subdirectories
	let aol_dir = temp_path.join("logs");
	let snapshot_dir = temp_path.join("snapshots");
	std::fs::create_dir_all(&aol_dir).unwrap();
	std::fs::create_dir_all(&snapshot_dir).unwrap();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never)
		.with_aol_path(aol_dir.join("custom.aol"))
		.with_snapshot_path(snapshot_dir.join("custom.snapshot"));

	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Add some data
	{
		let mut tx = db.transaction(true);
		tx.set("custom_key".to_string(), "custom_value".to_string()).unwrap();
		tx.commit().unwrap();
	}

	// Create snapshot
	if let Some(persistence) = db.persistence() {
		persistence.snapshot().unwrap();
	}

	// Verify files exist at custom paths
	let custom_aol_path = aol_dir.join("custom.aol");
	let custom_snapshot_path = snapshot_dir.join("custom.snapshot");

	assert!(custom_aol_path.exists(), "Custom AOL file should exist");
	assert!(custom_snapshot_path.exists(), "Custom snapshot file should exist");
}

#[test]
fn test_persistence_options_builder() {
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	// Test fluent builder pattern
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::AsynchronousAfterCommit)
		.with_snapshot_mode(SnapshotMode::Interval(Duration::from_secs(1)))
		.with_fsync_mode(FsyncMode::Interval(Duration::from_millis(500)));

	// Verify the options were set correctly
	assert_eq!(persistence_opts.aol_mode, AolMode::AsynchronousAfterCommit);
	assert_eq!(persistence_opts.snapshot_mode, SnapshotMode::Interval(Duration::from_secs(1)));
	assert_eq!(persistence_opts.fsync_mode, FsyncMode::Interval(Duration::from_millis(500)));

	// Test that database can be created with these options
	let db_opts = DatabaseOptions::default();
	let _db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();
}

#[test]
fn test_readonly_operations_no_persistence() {
	// Test that read-only operations work even with persistence configured
	let temp_dir = TempDir::new().unwrap();
	let temp_path = temp_dir.path();

	let db_opts = DatabaseOptions::default();
	let persistence_opts = PersistenceOptions::new(temp_path)
		.with_aol_mode(AolMode::SynchronousOnCommit)
		.with_snapshot_mode(SnapshotMode::Never);

	let db = Database::new_with_persistence(db_opts, persistence_opts).unwrap();

	// Perform read-only operations (should not trigger persistence)
	{
		let mut tx = db.transaction(false);
		assert_eq!(tx.get("non_existent_key").unwrap(), None);
		assert!(!tx.exists("non_existent_key").unwrap());
		tx.cancel().unwrap();
	}

	// Verify no persistence files were created for read-only operations
	let aol_path = temp_path.join("aol.bin");
	let snapshot_path = temp_path.join("snapshot.bin");

	// AOL file might exist but should be empty, snapshot should not exist
	if aol_path.exists() {
		let metadata = std::fs::metadata(&aol_path).unwrap();
		assert_eq!(metadata.len(), 0, "AOL file should be empty after read-only operations");
	}
	assert!(!snapshot_path.exists(), "Snapshot file should not exist after read-only operations");
}
