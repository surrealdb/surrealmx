<br>

<p align="center">
    <a href="https://surrealdb.com" target="_blank">
        <img width="200" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/black/logo.svg" alt="SurrealMX Logo">
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

SurrealMX is an ultra-high-throughput, in-memory, lock-free key-value database engine built on multi-version concurrency control (MVCC) over a concurrent adaptive radix tree, with a circular commit ring buffer.

It is designed as an independent, standalone embedded storage engine and caching layer suitable for microsecond-latency workloads, high-concurrency server applications, and browser WebAssembly environments.

---

## Performance

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM**, 15,000,000 keys across 48 concurrent worker threads with 128 clients, and 5,000,000 iterations of each range scan, via [`crud-bench`](https://github.com/surrealdb/crud-bench); SurrealMX figures are the best of three runs):

| Engine | Point&nbsp;Read&nbsp;(OPS) | &nbsp;&nbsp;&nbsp;Create&nbsp;(OPS) | Update&nbsp;(OPS) | &nbsp;&nbsp;&nbsp;Delete&nbsp;(OPS) | &nbsp;&nbsp;&nbsp;Scan&nbsp;(OPS) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **SurrealMX** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/rocket.png" alt="🚀">&nbsp;**22,921,015** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/rocket.png" alt="🚀">&nbsp;**2,037,150** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/rocket.png" alt="🚀">&nbsp;**1,704,945** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/rocket.png" alt="🚀">&nbsp;**2,496,510** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/rocket.png" alt="🚀">&nbsp;**17,741,053** |
| LMDB<br><sup>(In-Memory)</sup> | 1,196,173 | 224,909 | 243,045 | 176,188 | 1,186,447 |
| RocksDB<br><sup>(In-Memory)</sup> | 750,476 | 627,071 | 599,129 | 681,996 | 839,268 |

- **Point Reads**: Over **22,900,000 OPS** sustained (654ms for 15,000,000 lookups) using zero-copy borrowed slice inspection (`db.with_value`): over **19× faster** than LMDB and **30× faster** than RocksDB.
- **Writes & Creates**: Over **2,030,000 OPS** (7.36s for 15,000,000 creates) via the concurrent ART index and lock-free circular commit ring buffer: **3.2× faster** than RocksDB and **9.1× faster** than LMDB.
- **Updates**: Over **1,700,000 OPS** (8.80s for 15,000,000 updates): **2.8× faster** than RocksDB and **7.0× faster** than LMDB.
- **Deletes**: Over **2,490,000 OPS** (6.01s for 15,000,000 deletions) via optimized inline tombstone collapse and pointer-identity unlinking: **3.7× faster** than RocksDB and **14× faster** than LMDB.
- **Range Scans**: Over **17,700,000 OPS** (282ms for 5,000,000 `select(*) limit(100)` scans) using lock-free version reads and zero-allocation cursor traversal (`scan_with` / `keys_for_each`): **21× faster** than RocksDB and **15× faster** than LMDB. See [Scan Profile](#scan-profile) for other kinds of range scan.

### Scan Profile

Range scans over the same 15,000,000-key dataset, as scans per second: 5,000,000 iterations of each limit and skip scan, and 1,000 counts over the full key range.

| Engine | Limit&nbsp;(OPS) | Keys,&nbsp;Limit&nbsp;(OPS) | Skip&nbsp;+&nbsp;Limit&nbsp;(OPS) | Keys,&nbsp;Skip&nbsp;+&nbsp;Limit&nbsp;(OPS) | Full&nbsp;Count&nbsp;(OPS) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **SurrealMX** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/rocket.png" alt="🚀">&nbsp;**17,741,053** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/rocket.png" alt="🚀">&nbsp;**17,754,720** | 257,840 | 302,465 | 18.9 |
| LMDB<br><sup>(In-Memory)</sup> | 1,186,447 | 1,188,970 | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/rocket.png" alt="🚀">&nbsp;**549,029** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/rocket.png" alt="🚀">&nbsp;**549,634** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/rocket.png" alt="🚀">&nbsp;**24.1** |
| RocksDB<br><sup>(In-Memory)</sup> | 839,268 | 836,419 | 43,231 | 43,490 | 5.3 |

- **Limit**: `select(*) limit(100)` returns the first 100 key-value pairs of the range: **15× faster** than LMDB and **21× faster** than RocksDB. **Keys, Limit** (`select(id) limit(100)`) returns only the keys, at the same rate.
- **Skip + Limit**: `select(*) start(5000) limit(100)` walks and discards 5,000 entries, checking each one's visibility at the scan's snapshot, before returning 100: **6.0× faster** than RocksDB, while LMDB is **2.1× faster** than SurrealMX. Returning only the keys narrows LMDB's lead to 1.8×.
- **Full Count**: `count()` visits every one of the 15,000,000 keys: **3.6× faster** than RocksDB, while LMDB is **1.3× faster** than SurrealMX.

### Memory Profile

| Engine | Resting&nbsp;Memory | Peak&nbsp;Memory |
| :--- | ---: | ---: |
| **SurrealMX** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/rocket.png" alt="🚀">&nbsp;**16.0 GiB** | 31.5 GiB |
| LMDB<br><sup>(In-Memory)</sup> | 19.9 GiB | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealmx/main/img/rocket.png" alt="🚀">&nbsp;**20.3 GiB** |
| RocksDB<br><sup>(In-Memory)</sup> | 19.3 GiB | 95.3 GiB |

- **In-Memory Dataset**: True in-memory database holding all 15,000,000 documents resident at **16.0 GiB** resting memory (~1.1KB per full multi-version document).
- **Concurrent Scaling**: Peak memory expands dynamically under high-throughput 48-worker commits before settling back to resting baseline.

---

## Features

- In-memory database
- Multi-version concurrency control (MVCC)
- Lock-free circular commit ring buffer with $O(1)$ atomic slot claiming
- Sharded active reader registry eliminating atomic cache-line contention across multi-core CPUs
- Rich transaction support with rollbacks and stackable savepoints
- Direct auto-committing point operations (`db.get`, `db.set`, `db.del`, `db.put`, `db.exists`)
- True zero-allocation range scanning (`scan_with`, `scan_into`, `keys_for_each`, `keys_into`)
- Multiple concurrent readers and writers without global locks
- Support for serializable and snapshot-isolated transactions
- Atomicity, Consistency, Isolation, and optional Durability from ACID
- Deterministic Simulation Tested (DST) against an in-memory linearizable model oracle across 25,000,000 operations with zero divergences
- Optional persistence with configurable modes:
  - High-throughput Group Commit flusher for synchronous append-only logging
  - Asynchronous background logging and periodic full-datastore snapshots
  - Configurable fsync modes and fast LZ4 snapshot file compression

## Quick Start

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

## Direct Database Operations

For single-key reads, writes, and scans, `Database` exposes direct methods. Point reads (`get`, `exists`, `with_value`) bypass transaction pool checkout and reader slot registration entirely, and are linearizable: each returns the latest committed value. Writes commit as single-key auto-committed transactions with full snapshot isolation, retrying write-write conflicts internally since they read nothing. Scans read a consistent snapshot pinned by a pooled read transaction:

```rust
use surrealmx::Database;

fn main() {
    let db = Database::new();

    // Direct auto-committed writes
    db.set("user:01", "alice").unwrap();
    db.put("user:02", "bob").unwrap(); // Fails if key already exists

    // Direct point reads
    assert!(db.exists("user:01").unwrap());
    assert_eq!(db.get("user:01").unwrap(), Some("alice".into()));

    // Zero-copy value inspection via closure without cloning
    db.with_value("user:01", |bytes| {
        assert_eq!(bytes, b"alice");
    }).unwrap();

    // Direct deletion
    db.del("user:02").unwrap();
    assert!(!db.exists("user:02").unwrap());
}
```

## Manual Cleanup and Garbage Collection

Background worker threads perform cleanup and garbage collection at regular
intervals. These workers can be disabled through `DatabaseOptions` by setting
`enable_cleanup` or `enable_gc` to `false`. When disabled, trigger the tasks
manually: `run_cleanup` trims the transaction commit queue, `run_gc_tracked`
sweeps just the keys tracked as holding reclaimable version garbage (cheap,
its cost scales with the amount of pinned garbage, not the dataset size), and
`run_gc` performs a full datastore scan, useful as an occasional deep sweep.

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

	// Manually trim the transaction commit queue
    db.run_cleanup();
	
	// Manually sweep the tracked garbage-candidate keys
    db.run_gc_tracked();
	
	// Occasionally, perform a full-scan sweep of the datastore
    db.run_gc();
}
```

## Persistence Modes

SurrealMX supports optional persistence with two modes:

### Full persistence (AOL + Snapshots) - Default

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

### Snapshot-only persistence

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

### Configuration Options

#### AOL Modes
- **`AolMode::Never`**: Disables append-only logging entirely (default)
- **`AolMode::SynchronousOnCommit`**: Writes changes to AOL immediately on every commit (maximum durability)
- **`AolMode::AsynchronousAfterCommit`**: Writes changes to AOL asynchronously after every commit (better performance)

#### Snapshot Modes
- **`SnapshotMode::Never`**: Disables snapshots entirely (default)
- **`SnapshotMode::Interval(Duration)`**: Takes snapshots at the specified interval

#### Fsync Modes
- **`FsyncMode::Never`**: Never calls fsync - fastest but least durable (default)
- **`FsyncMode::EveryAppend`**: Calls fsync after every AOL append - slowest but most durable
- **`FsyncMode::Interval(Duration)`**: Calls fsync at most once per interval - balanced approach

#### Compression Support
- **`CompressionMode::None`**: No compression applied to snapshots (default)
- **`CompressionMode::Lz4`**: Fast LZ4 compression for snapshots (reduces storage size)

### Advanced Configuration Example

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

## Durability Guarantees

SurrealMX provides different levels of durability (the "D" in ACID) depending on the persistence configuration:

### In-memory only mode (No persistence)

When persistence is disabled (the default), SurrealMX provides **no durability guarantees**. All data is lost when the process terminates, crashes, or the system shuts down. This mode is ideal for:
- Caching and temporary data storage
- Development and testing
- Scenarios where data can be reconstructed from other sources

### Maximum durability (AOL with fsync)

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

### Configurable durability levels

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
| Snapshot-only | ⚠️ (last snapshot) | ⚠️ (last snapshot) | Fastest |
| Async AOL + No fsync | ⚠️ (mostly) | ⚠️ (mostly + OS buffers) | Very fast |
| Async AOL + Interval fsync | ⚠️ (mostly) | ⚠️ (mostly + since last fsync) | Very fast |
| Async AOL + Every fsync | ⚠️ (mostly) | ⚠️ (mostly) | Very fast |
| Sync AOL + No fsync | ✅ | ⚠️ (OS buffers) | Fast |
| Sync AOL + Interval fsync | ✅ | ⚠️ (since last fsync) | Fast |
| Sync AOL + Every fsync | ✅ | ✅ | Slow |

Choose the configuration that best balances your durability requirements against performance needs.

## Isolation Levels

SurrealMX supports two isolation levels to balance between performance and consistency guarantees:

### Snapshot Isolation (Default)

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

### Serializable Snapshot Isolation

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

### Conflict diagnostics

When a transaction is aborted due to a conflict (`KeyReadConflict` or `KeyWriteConflict`), SurrealMX can log the conflicting key for debugging purposes. These logs are emitted at the `DEBUG` level using the [`tracing`](https://docs.rs/tracing) crate, and are only included in debug builds (`#[cfg(debug_assertions)]`).

All conflict logs are emitted under a dedicated tracing target, exposed as the public constant [`LOG_TARGET_CONFLICTS`]:

```rust
use surrealmx::LOG_TARGET_CONFLICTS;
// Value: "surrealmx::conflicts"
```

This allows you to selectively enable conflict diagnostics with a [`tracing_subscriber`](https://docs.rs/tracing-subscriber) filter, without enabling all `DEBUG` output from the crate:

```rust
use tracing_subscriber::EnvFilter;

tracing_subscriber::fmt()
    .with_env_filter(
        EnvFilter::builder()
            .parse("info,surrealmx::conflicts=debug")
            .unwrap(),
    )
    .init();
```

Or via the `RUST_LOG` environment variable:

```sh
RUST_LOG="info,surrealmx::conflicts=debug" cargo run
```

## Savepoints

Savepoints mark a point inside a transaction that later writes can be undone back to, without discarding the whole transaction. They are stackable, so scopes can nest.

```rust
let db = Database::new();
let mut tx = db.transaction(true);

tx.set("user:1", "alice").unwrap();

// Mark a point, then make some changes
tx.set_savepoint().unwrap();
tx.set("user:1", "bob").unwrap();
tx.set("user:2", "carol").unwrap();

// Undo everything back to the savepoint
tx.rollback_to_savepoint().unwrap();

assert_eq!(tx.get("user:1").unwrap(), Some("alice".into()));
assert_eq!(tx.get("user:2").unwrap(), None);

tx.commit().unwrap();
```

Every `set_savepoint` should be paired with exactly one of:

- `rollback_to_savepoint`: undo every write made since the savepoint, and discard it.
- `release_savepoint`: keep every write made since the savepoint, and discard it.

Releasing is what a nested scope does when it succeeds. Its writes join the enclosing scope, so a later rollback of that enclosing scope still undoes them:

```rust
tx.set_savepoint().unwrap();          // outer scope
tx.set("a", "1").unwrap();

tx.set_savepoint().unwrap();          // inner scope
tx.set("b", "2").unwrap();
tx.release_savepoint().unwrap();      // inner scope succeeded, so keep "b"

assert_eq!(tx.get("b").unwrap(), Some("2".into()));

// Unwinding the outer scope still undoes the released scope's write
tx.rollback_to_savepoint().unwrap();
assert_eq!(tx.get("a").unwrap(), None);
assert_eq!(tx.get("b").unwrap(), None);
```

Both methods return `Error::NoSavepoint` when no savepoint is set, and `Error::TxNotWritable` on a read-only transaction. Savepoints are anonymous, so releasing more savepoints than were set cannot be detected once the stack is non-empty again: a later rollback will silently unwind further than intended.

Note that a rollback rewinds writes only. Keys read and ranges scanned inside a rolled back scope stay tracked for conflict detection, because a write that survives the rollback may have been derived from a value that scope read. This keeps serializable transactions conservative: it can produce a retryable conflict error, never a missed one.

## Range Operations

SurrealMX provides powerful range-based operations for scanning, counting, and iterating over keys. All range operations support:

- **Forward and reverse iteration**
- **Skip and limit parameters** for pagination  
- **Efficient range scans** over the ordered adaptive radix tree that holds the datastore
- **Zero-allocation closure scans** (`scan_with`, `keys_for_each`) inspecting borrowed slices directly without vector allocations or value clones
- **Buffer-reusing scans** (`scan_into`, `keys_into`) avoiding repeated vector reallocations across query loops

### Basic range scanning

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

### Pagination and reverse iteration

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

### Zero-allocation range scanning

```rust
use surrealmx::Database;

fn main() {
    let db = Database::new();

    for i in 0..100 {
        db.set(format!("doc:{i:04}"), format!("val_{i}")).unwrap();
    }

    // Inspect borrowed key (&ByteSlice) and value (&[u8]) directly with zero allocations
    db.scan_with("doc:0000".."doc:9999", None, Some(10), |key, val| {
        println!("{key:?} => {:?}", std::str::from_utf8(val).unwrap());
        true // Return false to stop iteration early
    }).unwrap();

    // Scan keys with zero allocation
    db.keys_for_each("doc:0000".."doc:9999", None, Some(10), |key| {
        println!("Key: {key:?}");
        true
    }).unwrap();

    // Reuse a pre-allocated vector buffer across repeated scans
    let mut buffer = Vec::new();
    db.scan_into("doc:0000".."doc:0050", None, None, &mut buffer).unwrap();
    assert_eq!(buffer.len(), 50);
}
```

**Available range operation methods:**

- `scan_with(range, skip, limit, closure)`: Zero-allocation closure inspection over borrowed `(&ByteSlice, &[u8])`
- `keys_for_each(range, skip, limit, closure)`: Zero-allocation closure iteration over borrowed `&ByteSlice` keys
- `scan_into(range, skip, limit, &mut buffer)`: Reuses pre-allocated buffer for key-value pairs
- `keys_into(range, skip, limit, &mut buffer)`: Reuses pre-allocated buffer for keys
- `keys(range, skip, limit)` / `keys_reverse(...)`: Get keys in range as `Vec<ByteSlice>`
- `scan(range, skip, limit)` / `scan_reverse(...)`: Get key-value pairs in range as `Vec<(ByteSlice, ByteSlice)>`
- `total(range, skip, limit)`: Count keys in range

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/surrealdb/surrealmx/blob/main/LICENSE) for details.
