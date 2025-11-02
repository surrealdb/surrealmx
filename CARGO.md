<br>

<p align="center">
    <a href="https://github.com/surrealdb/surrealmx" target="_blank">
        <img width="200" src="https://raw.githubusercontent.com/surrealdb/surrealmx/master/img/logo.svg" alt="SurrealMX Logo">
    </a>
</p>

<p align="center">An embedded, in-memory, lock-free, transaction-based, key-value database engine.</p>

<br>

<p align="center">
	<a href="https://github.com/surrealdb/surrealmx"><img src="https://img.shields.io/badge/status-stable-ff00bb.svg?style=flat-square"></a>
	&nbsp;
	<a href="https://docs.rs/surrealmx/"><img src="https://img.shields.io/docsrs/surrealmx?style=flat-square"></a>
	&nbsp;
	<a href="https://crates.io/crates/surrealmx"><img src="https://img.shields.io/crates/v/surrealmx?style=flat-square"></a>
	&nbsp;
	<a href="https://github.com/surrealdb/surrealmx"><img src="https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square"></a>
</p>

#### Features

- In-memory database
- Multi-version concurrency control
- Rich transaction support with rollbacks
- Multiple concurrent readers without locking
- Multiple concurrent writers without locking
- Support for serializable, snapshot isolated transactions
- Atomicity, Consistency, Isolation, and optional Durability from ACID
- Optional persistence with configurable modes:
  - Support for synchronous and asynchronous append-only logging
  - Support for periodic full-datastore snapshots
  - Support for fsync on every commit, or periodically in the background
  - Support for LZ4 snapshot file compression

#### Quick start

```rust
use surrealmx::{Database, DatabaseOptions};

fn main() {
    // Create a database with custom settings
    let opts = DatabaseOptions { pool_size: 128, ..Default::default() };
    let db = Database::new_with_options(opts);

    // Start a write transaction
    let mut tx = db.transaction(true);
    tx.put("key", "value").unwrap();
    tx.commit().unwrap();

    // Read the value back
    let mut tx = db.transaction(false);
    assert_eq!(tx.get("key").unwrap(), Some("value".into()));
    tx.cancel().unwrap();
}
```

#### Manual cleanup and garbage collection

Background worker threads perform cleanup and garbage collection at regular
intervals. These workers can be disabled through `DatabaseOptions` by setting
`enable_cleanup` or `enable_gc` to `false`. When disabled, the tasks can be
triggered manually using the `run_cleanup` and `run_gc` methods.

```rust
use surrealmx::{Database, DatabaseOptions};

fn main() {
    // Create a database with custom settings
    let opts = DatabaseOptions { enable_gc: false, enable_cleanup: false, ..Default::default() };
    let db = Database::new_with_options(opts);

    // Start a write transaction
    let mut tx = db.transaction(true);
    tx.put("key", "value1").unwrap();
    tx.commit().unwrap();

	// Start a write transaction
    let mut tx = db.transaction(true);
    tx.put("key", "value2").unwrap();
    tx.commit().unwrap();

	// Manually remove unused transaction stale versions
    db.run_cleanup();
	
	// Manually remove old queue entries
    db.run_gc();
}
```

#### Persistence modes

SurrealMX supports optional persistence with two modes:

##### Full persistence (AOL + Snapshots) - Default

Provides maximum durability by logging every change to an append-only log and taking periodic snapshots.

```rust
use surrealmx::{Database, DatabaseOptions, PersistenceOptions, AolMode, SnapshotMode};
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let db_opts = DatabaseOptions::default();
    let persistence_opts = PersistenceOptions::new("./data")
        .with_aol_mode(AolMode::SynchronousOnCommit)
        .with_snapshot_mode(SnapshotMode::Interval(Duration::from_secs(60)));
    
    let db = Database::new_with_persistence(db_opts, persistence_opts)?;
    
    let mut tx = db.transaction(true);
    tx.put("key", "value")?;
    tx.commit()?; // Changes immediately written to AOL
    
    Ok(())
}
```

##### Snapshot-only persistence

Provides good performance with periodic durability by taking snapshots without logging individual changes.

```rust
use surrealmx::{Database, DatabaseOptions, PersistenceOptions, AolMode, SnapshotMode};
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let db_opts = DatabaseOptions::default();
    let persistence_opts = PersistenceOptions::new("./snapshot_data")
        .with_aol_mode(AolMode::Never) // Disable AOL, use only snapshots
        .with_snapshot_mode(SnapshotMode::Interval(Duration::from_secs(30)));
    
    let db = Database::new_with_persistence(db_opts, persistence_opts)?;
    
    let mut tx = db.transaction(true);
    tx.put("key", "value")?;
    tx.commit()?; // Changes only persisted during snapshots
    
    Ok(())
}
```

##### Configuration Options

###### AOL Modes
- **`AolMode::Never`**: Disables append-only logging entirely (default)
- **`AolMode::SynchronousOnCommit`**: Writes changes to AOL immediately on every commit (maximum durability)
- **`AolMode::AsynchronousAfterCommit`**: Writes changes to AOL asynchronously after every commit (better performance)

###### Snapshot Modes
- **`SnapshotMode::Never`**: Disables snapshots entirely (default)
- **`SnapshotMode::Interval(Duration)`**: Takes snapshots at the specified interval

###### Fsync Modes
- **`FsyncMode::Never`**: Never calls fsync - fastest but least durable (default)
- **`FsyncMode::EveryAppend`**: Calls fsync after every AOL append - slowest but most durable
- **`FsyncMode::Interval(Duration)`**: Calls fsync at most once per interval - balanced approach

###### Compression Support
- **`CompressionMode::None`**: No compression applied to snapshots (default)
- **`CompressionMode::Lz4`**: Fast LZ4 compression for snapshots (reduces storage size)

##### Advanced Configuration Example

```rust
use surrealmx::{Database, DatabaseOptions, PersistenceOptions, AolMode, SnapshotMode, FsyncMode, CompressionMode};
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let db_opts = DatabaseOptions::default();
    let persistence_opts = PersistenceOptions::new("./advanced_data")
        .with_aol_mode(AolMode::AsynchronousAfterCommit) // Async AOL writes
        .with_snapshot_mode(SnapshotMode::Interval(Duration::from_secs(300))) // Snapshot every 5 minutes
        .with_fsync_mode(FsyncMode::Interval(Duration::from_secs(1))) // Fsync every second
        .with_compression(CompressionMode::Lz4); // Enable LZ4 compression
    
    let db = Database::new_with_persistence(db_opts, persistence_opts)?;
    
    let mut tx = db.transaction(true);
    tx.put("key", "value")?;
    tx.commit()?; // Changes written asynchronously to AOL, fsync'd every second
    
    Ok(())
}
```

**Trade-offs:**
- **AOL + Snapshots**: Maximum durability, slower writes, larger storage
- **Snapshot-only**: Better performance, risk of data loss between snapshots, smaller storage
- **Synchronous AOL**: Immediate durability, slower commit times
- **Asynchronous AOL**: Better performance, small risk of data loss on system crash
- **Frequent fsync**: Higher durability, reduced performance
- **LZ4 Compression**: Smaller storage footprint, slight CPU overhead

See the [Durability guarantees](#durability-guarantees) section for detailed information about ACID durability levels.

#### Durability guarantees

SurrealMX provides different levels of durability (the "D" in ACID) depending on the persistence configuration:

##### In-memory only mode (No persistence)

When persistence is disabled (the default), SurrealMX provides **no durability guarantees**. All data is lost when the process terminates, crashes, or the system shuts down. This mode is ideal for:
- Caching and temporary data storage
- Development and testing
- Scenarios where data can be reconstructed from other sources

##### Maximum durability (AOL with fsync)

For maximum durability that survives system crashes and power failures, use synchronous AOL with fsync on every append:

```rust
use surrealmx::{Database, DatabaseOptions, PersistenceOptions, AolMode, FsyncMode};

fn main() -> std::io::Result<()> {
    let db_opts = DatabaseOptions::default();
    let persistence_opts = PersistenceOptions::new("./data")
        .with_aol_mode(AolMode::SynchronousOnCommit)
        .with_fsync_mode(FsyncMode::EveryAppend);
    
    let db = Database::new_with_persistence(db_opts, persistence_opts)?;
    
    let mut tx = db.transaction(true);
    tx.put("key", "value")?;
    tx.commit()?; // Guaranteed to be durable after this returns
    
    Ok(())
}
```

With this configuration:
- Changes are written to the AOL immediately on commit
- `fsync()` is called to ensure data reaches physical storage
- Transactions are fully durable once `commit()` returns successfully
- Data survives process crashes, system crashes, and power failures

##### Configurable durability levels

Different persistence configurations provide different durability guarantees:

**AOL Modes:**
- **`AolMode::SynchronousOnCommit`**: Changes written to AOL immediately on commit. Durable after commit returns (if combined with appropriate fsync mode).
- **`AolMode::AsynchronousAfterCommit`**: Changes written to AOL asynchronously. Small window where recent commits may be lost on sudden system crash.
- **`AolMode::Never`**: No AOL logging. Changes only persisted via snapshots.

**Fsync Modes:**
- **`FsyncMode::EveryAppend`**: Calls `fsync()` after every AOL write. Maximum durability but slowest performance.
- **`FsyncMode::Interval(Duration)`**: Calls `fsync()` periodically. Durability guaranteed after the interval passes.
- **`FsyncMode::Never`**: Never calls `fsync()`. Relies on OS to flush data. Risk of data loss if OS crashes before flush.

**Snapshot Modes:**
- **`SnapshotMode::Interval(Duration)`**: Takes periodic snapshots. Without AOL, only data from the last snapshot is durable.
- **`SnapshotMode::Never`**: No snapshots. Must use AOL for any durability.

**Durability guarantees summary:**

| Configuration | Survives Process Crash | Survives System Crash | Performance |
|--------------|----------------------|---------------------|-------------|
| No persistence | ❌ | ❌ | Fastest |
| Snapshot-only | ⚠️ (last snapshot) | ⚠️ (last snapshot) | Fast |
| Async AOL + No fsync | ⚠️ (mostly) | ❌ | Good |
| Async AOL + Interval fsync | ⚠️ (mostly) | ⚠️ (after fsync) | Moderate |
| Sync AOL + Every fsync | ✅ | ✅ | Slowest |

Choose the configuration that best balances your durability requirements against performance needs.

#### Historical reads

SurrealMX's MVCC (Multi-Version Concurrency Control) design allows you to read data as it existed at any point in time. This enables powerful use cases like:

- **Audit trails**: See what data looked like at specific timestamps
- **Time-travel debugging**: Examine application state at the time of an issue
- **Consistent reporting**: Generate reports based on a snapshot of data from a specific point in time
- **Conflict resolution**: Compare different versions of data to understand changes

```rust
use surrealmx::Database;

fn main() {
    let db = Database::new();
    
    // Insert some initial data
    let mut tx = db.transaction(true);
    tx.put("user:1", "Alice").unwrap();
    tx.commit().unwrap();
    
    // Capture timestamp after first commit
    let version_1 = db.oracle.current_timestamp();
    
    // Wait a moment to ensure different timestamps
    std::thread::sleep(std::time::Duration::from_millis(1));
    
    // Make some changes
    let mut tx = db.transaction(true);
    tx.set("user:1", "Alice Smith").unwrap(); // Update name
    tx.put("user:2", "Bob").unwrap();         // Add new user
    tx.commit().unwrap();
    
    // Read historical data
    let mut tx = db.transaction(false);
    
    // Read current state
    assert_eq!(tx.get("user:1").unwrap().as_deref(), Some(b"Alice Smith" as &[u8]));
    assert_eq!(tx.get("user:2").unwrap().as_deref(), Some(b"Bob" as &[u8]));
    
    // Read state as it was at version_1 (before changes)
    assert_eq!(tx.get_at_version("user:1", version_1).unwrap().as_deref(), Some(b"Alice" as &[u8]));
    assert_eq!(tx.get_at_version("user:2", version_1).unwrap(), None);
    
    // Range operations also support historical reads
    let historical_keys = tx.keys_at_version("user:0".."user:9", None, None, version_1).unwrap();
    assert_eq!(historical_keys.len(), 1);
    assert_eq!(historical_keys[0].as_ref(), b"user:1");
    
    tx.cancel().unwrap();
}
```

**Available historical read methods:**
- `get_at_version(key, version)`: Read a single key's value at a specific version
- `keys_at_version(range, skip, limit, version)`: Get keys in range at a specific version
- `scan_at_version(range, skip, limit, version)`: Get key-value pairs at a specific version
- `total_at_version(range, skip, limit, version)`: Count keys at a specific version

#### Isolation levels

SurrealMX supports two isolation levels to balance between performance and consistency guarantees:

##### Snapshot Isolation (Default)

Provides excellent performance with strong consistency guarantees. Transactions see a consistent snapshot of the database as it existed when the transaction began.

- **Read consistency**: All reads within a transaction see the same consistent view
- **Write isolation**: Changes from other transactions are not visible until they commit
- **No dirty reads**: Never see uncommitted changes from other transactions
- **No non-repeatable reads**: Reading the same key multiple times returns the same value

```rust
use surrealmx::Database;

fn main() {
    let db = Database::new();
    
    // Snapshot isolation (default behavior)
    let mut tx1 = db.transaction(true);
    let mut tx2 = db.transaction(false); // Start tx2 before tx1 commits
    
    tx1.put("counter", "1").unwrap();
    tx1.commit().unwrap();
    
    // tx2 started before tx1 committed, so it doesn't see the change
    assert_eq!(tx2.get("counter").unwrap(), None);
    tx2.cancel().unwrap();
}
```

##### Serializable Snapshot Isolation

Provides the strongest consistency guarantee by detecting read-write conflicts and aborting transactions that would violate serializability.

- **All Snapshot Isolation guarantees**: Plus additional conflict detection
- **Read-write conflict detection**: Prevents phantom reads and write skew
- **Serializable execution**: Equivalent to running transactions one at a time
- **Higher abort rate**: More transactions may need to retry due to conflicts

```rust
use surrealmx::{Database, Error};

fn main() {
    let db = Database::new();
    
    // Initialize data
    let mut tx = db.transaction(true);
    tx.put("x", "0").unwrap();
    tx.put("y", "0").unwrap();
    tx.commit().unwrap();
    
    // Two concurrent transactions that would cause write skew
    let mut tx1 = db.transaction(true); // Uses SerializableSnapshotIsolation internally
    let mut tx2 = db.transaction(true);
    
    // tx1 reads x and writes to y
    tx1.get("x").unwrap();
    tx1.set("y", "modified_by_tx1").unwrap();
    
    // tx2 reads y and writes to x  
    tx2.get("y").unwrap();
    tx2.set("x", "modified_by_tx2").unwrap();
    
    // First transaction commits successfully
    tx1.commit().unwrap();
    
    // Second transaction detects conflict and aborts
    match tx2.commit() {
        Err(Error::KeyReadConflict) => {
            // Transaction must be retried
            println!("Transaction aborted due to read conflict, retrying...");
        }
        _ => panic!("Expected read conflict"),
    }
}
```

**When to use each isolation level:**
- **Snapshot Isolation**: Most applications, high-performance scenarios, read-heavy workloads
- **Serializable Snapshot Isolation**: Financial applications, inventory management, any scenario requiring strict serializability

#### Range operations

SurrealMX provides powerful range-based operations for scanning, counting, and iterating over keys. All range operations support:

- **Forward and reverse iteration**
- **Skip and limit parameters** for pagination  
- **Historical versions** for time-travel queries
- **Efficient range scans** using the underlying B+ tree structure

##### Basic range scanning

```rust
use surrealmx::Database;

fn main() {
    let db = Database::new();
    
    // Insert test data
    let mut tx = db.transaction(true);
    for i in 1..=10 {
        tx.put(&format!("key:{:02}", i), &format!("value:{}", i)).unwrap();
    }
    tx.commit().unwrap();
    
    let mut tx = db.transaction(false);
    
    // Get all keys in range
    let keys = tx.keys("key:03".."key:08", None, None).unwrap();
    assert_eq!(keys.len(), 5);
    assert_eq!(keys[0].as_ref(), b"key:03");
    assert_eq!(keys[4].as_ref(), b"key:07");
    
    // Get key-value pairs in range
    let pairs = tx.scan("key:03".."key:06", None, None).unwrap();
    assert_eq!(pairs.len(), 3);
    assert_eq!(pairs[0].0.as_ref(), b"key:03");
    assert_eq!(pairs[0].1.as_ref(), b"value:3");
    
    // Count keys in range
    let count = tx.total("key:00".."key:99", None, None).unwrap();
    assert_eq!(count, 10);
    
    tx.cancel().unwrap();
}
```

##### Pagination and reverse iteration

```rust
use surrealmx::Database;

fn main() {
    let db = Database::new();
    
    // Insert test data
    let mut tx = db.transaction(true);
    for i in 1..=100 {
        tx.put(format!("item:{:03}", i), format!("value_{}", i)).unwrap();
    }
    tx.commit().unwrap();
    
    let mut tx = db.transaction(false);
    
    // Paginated forward scan: skip 10, take 5
    let page1 = tx.scan("item:000".."item:999", Some(10), Some(5)).unwrap();
    assert_eq!(page1.len(), 5);
    assert_eq!(page1[0].0.as_ref(), b"item:011");
    assert_eq!(page1[4].0.as_ref(), b"item:015");
    
    // Reverse iteration: get last 3 items
    let last_items = tx.scan_reverse("item:000".."item:999", None, Some(3)).unwrap();
    assert_eq!(last_items.len(), 3);
    assert_eq!(last_items[0].0.as_ref(), b"item:100"); // First item is the highest key
    assert_eq!(last_items[2].0.as_ref(), b"item:098"); // Last item is lower
    
    tx.cancel().unwrap();
}
```

##### Historical range operations

```rust
use surrealmx::Database;

fn main() {
    let db = Database::new();
    
    // Insert initial data
    let mut tx = db.transaction(true);
    tx.put("a", "1").unwrap();
    tx.put("b", "2").unwrap();
    tx.commit().unwrap();
    let version_1 = db.oracle.current_timestamp();
    
    // Wait a moment to ensure different timestamps
    std::thread::sleep(std::time::Duration::from_millis(1));
    
    // Add more data
    let mut tx = db.transaction(true);
    tx.put("c", "3").unwrap();
    tx.put("d", "4").unwrap();
    tx.commit().unwrap();
    
    let mut tx = db.transaction(false);
    
    // Current state: all 4 keys
    let current_keys = tx.keys("a".."z", None, None).unwrap();
    assert_eq!(current_keys.len(), 4);
    assert_eq!(current_keys[0].as_ref(), b"a");
    assert_eq!(current_keys[3].as_ref(), b"d");
    
    // Historical state: only first 2 keys  
    let historical_keys = tx.keys_at_version("a".."z", None, None, version_1).unwrap();
    assert_eq!(historical_keys.len(), 2);
    assert_eq!(historical_keys[0].as_ref(), b"a");
    assert_eq!(historical_keys[1].as_ref(), b"b");
    
    // Count at different versions
    let current_count = tx.total("a".."z", None, None).unwrap();
    let historical_count = tx.total_at_version("a".."z", None, None, version_1).unwrap();
    assert_eq!(current_count, 4);
    assert_eq!(historical_count, 2);
    
    tx.cancel().unwrap();
}
```

**Available range operation methods:**

**Current version:**
- `keys(range, skip, limit)` / `keys_reverse(...)`: Get keys in range
- `scan(range, skip, limit)` / `scan_reverse(...)`: Get key-value pairs in range
- `total(range, skip, limit)`: Count keys in range

**Historical versions:**
- `keys_at_version(range, skip, limit, version)` / `keys_at_version_reverse(...)`
- `scan_at_version(range, skip, limit, version)` / `scan_at_version_reverse(...)`
- `total_at_version(range, skip, limit, version)`

**Range parameters:**
- `range`: Rust range syntax (`"start".."end"`) - start inclusive, end exclusive
- `skip`: Optional number of items to skip (for pagination)
- `limit`: Optional maximum number of items to return
- `version`: Specific version timestamp for historical operations

#### Project History

**Note:** This project was originally developed under the name `memodb`. It has been renamed to `surrealmx` to better reflect its evolution and alignment with the SurrealDB ecosystem.
