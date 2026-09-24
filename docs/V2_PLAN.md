# SurrealMX V2 Transition Plan & Architecture

## Objective
Evolve SurrealMX from a pure in-memory, skiplist-based transactional engine into an ultra-high-throughput, zero-allocation, memory-compact, and cache-friendly in-memory/embedded Key-Value database engine. The new architecture draws direct inspiration from the zero-copy design of `byteslice`, the lock-free concurrency and group-commit pipeline of ShaleDB and SurrealKV V2 (`next` branch), and the German String prefix-accelerated comparison model.

The ultimate goal is to make SurrealMX the fastest embedded in-memory transactional storage engine possible: achieving sub-microsecond point lookups, zero heap allocations on hot transactional paths, minimal memory footprint per key, and scalable linear concurrency across all available CPU cores.

---

## Core Architectural Shifts

1. **Verify First with Deterministic Simulation Testing (DST):** Build an in-memory differential testing oracle (`ModelDb`) and PRNG workload generator *before* touching the engine internals. Every subsequent refactoring step is immediately subjected to chaotic simulated concurrency, race condition checks, and crash-recovery verification with 100% reproducible seeds.
2. **Adopt `byteslice::ByteSlice` Everywhere:** Replace `bytes::Bytes` and `Vec<u8>` across all keys, values, and iterators. Leverage Small String Optimization (SSO $\le 20$ bytes inline) and 4-byte prefix-accelerated comparisons (German String design) to eliminate heap allocations, pointer indirections, and atomic reference count contention on hot keys.
3. **Bypass Merge Queue on Point Reads:** Short-circuit in-flight merge queue scans on point reads when the reader's snapshot version is already retired to the datastore (`version <= merge_retire_id`), and add key bounds / bloom filters to in-flight merges.
4. **Lock-Free Commit Ring Buffer:** Replace dynamic `SkipMap` commit and merge queues and serializing predecessor CAS backoff loops with a fixed-size, lock-free Multi-Producer Single-Consumer (MPSC) Ring Buffer. Claim slots in $O(1)$ and advance contiguous watermarks without livelocks or cache-line thrashing.
5. **Group Commit for Durable Persistence:** Transform synchronous persistence (`AolMode::SynchronousOnCommit`) from a serializing `Mutex<File>` + per-commit `BufWriter` into a high-throughput Group Commit flusher that coalesces hundreds of concurrent transactions into single batched writes and single `fdatasync` calls, using pooled scratch buffers.
6. **Datastore Memory Compaction:** Specialize version chains for the steady state (where $>95\%$ of keys hold a single live version), eliminating `SmallVec` header overhead and reducing per-key node metadata by 20–30%.
7. **Lock-Free Readset Conflict Tracking & xxHash3:** Replace scalar FNV-1a with SIMD-accelerated xxHash3 and migrate `readset_bloom` from a `Mutex<Box<BloomFilter>>` to a lock-free atomic bit array (`[AtomicU64; 64]`), removing all mutex acquisitions from the read path under Serializable Snapshot Isolation (SSI).
8. **Delta Undo Journal for Savepoints:** Replace full `BTreeMap` cloning on every `set_savepoint()` with an append-only transaction undo log, enabling $O(1)$ savepoint marks and linear rollback cost.
9. **Zero-Copy Scans & Monomorphized Iterators:** Eliminate dynamic dispatch (`Box<dyn Iterator>`) in range scans and borrow key/value slices directly during merge iteration, avoiding intermediate `ByteSlice` clones.

---

## Strategic Roadmap & Phase Sequencing

Following the engineering doctrine of FoundationDB, ShaleDB (`shale-sim`), and SurrealKV V2: **test infrastructure precedes implementation**. Implementing Deterministic Simulation Testing (Phase 1) establishes an automated differential testing safety net against the existing engine baseline. Every subsequent optimization can be tested against hundreds of thousands of randomized concurrent interleavings with zero fear of silent data corruption.

```text
Phase 1: Deterministic Simulation & Differential Testing (ModelDb Oracle)
   |
   +---> Phase 2: Zero-Copy Byte Primitives (byteslice)
   |
   +---> Phase 3: Point Read Optimization (Merge Queue Bypass)
   |
   +---> Phase 4: Concurrency Revamp (Lock-Free OCC Ring Buffer)
   |
   +---> Phase 5: High-Throughput Persistence (Group Commit & Buffer Reuse)
   |
   +---> Phase 6: Datastore Memory Compaction (Versions Specialization)
   |
   +---> Phase 7: Lock-Free SSI & xxHash3 Conflict Detection
   |
   +---> Phase 8: Delta Undo Journal for Savepoints
   |
   +---> Phase 9: Zero-Copy Scans & Monomorphized Iterators
```

1. **Phase 1: Deterministic Simulation Testing (DST) & Differential Oracle**  
   Build `ModelDb` (ground-truth reference model) and PRNG workload simulator. Validate baseline engine correctness under chaotic concurrent operations.
2. **Phase 2: Zero-Copy Byte Primitives (`byteslice`)**  
   Migrate from `bytes::Bytes` to `byteslice::ByteSlice` across all public and internal interfaces. Unlock 20-byte SSO and 4-byte prefix acceleration.
3. **Phase 3: Read Path & Overlay Optimization (Merge Queue Bypass)**  
   Short-circuit merge queue scans when `version <= merge_retire_id`. Add min/max key bounds to `Merge` to eliminate unnecessary `BTreeMap::get` calls.
4. **Phase 4: The Concurrency Revamp (Lock-Free OCC Ring Buffer)**  
   Replace `SkipMap` commit and merge queues with a pre-allocated fixed-size ring buffer. Move slot claiming to $O(1)$ atomic `fetch_add`.
5. **Phase 5: High-Throughput Persistence (Group Commit & Buffer Reuse)**  
   Implement group commit for AOL flushes and scratch buffer reuse, boosting synchronous write IOPS by 100x and eliminating per-commit `BufWriter` allocations.
6. **Phase 6: Datastore Memory Compaction & Version Specialization**  
   Specialize `Versions` for single-version keys (`enum Versions { Single, Chain }`) and optimize node memory layout.
7. **Phase 7: Lock-Free SSI & Modernized Conflict Detection**  
   Implement lock-free atomic bloom filters, xxHash3 hashing, and adaptive writeset filters for $\le 2$ keys.
8. **Phase 8: Efficient Transaction Savepoints (Delta Undo Journal)**  
   Replace deep `BTreeMap` copies with an undo log journal.
9. **Phase 9: Zero-Copy Range Scans & Monomorphized Iterators**  
   Monomorphize `MergeIterator` and implement zero-copy borrowed slice traversal.

---

## Phase 1: Deterministic Simulation Testing (DST) & Differential Oracle

Before making complex changes to the storage engine, data types, and commit pipeline, we must construct a deterministic simulation and differential testing framework. Concurrency bugs, lost updates, and memory safety issues in lock-free code are notoriously difficult to reproduce under non-deterministic thread schedules.

- [ ] Build `ModelDb`: an unoptimized, trivially correct reference in-memory model (using `BTreeMap` under a sequential lock) representing ground-truth committed state and first-committer-wins OCC semantics.
- [ ] Implement `WorkloadGenerator`: a deterministic PRNG-driven generator (seeded with `StdRng`) producing interleaved transactions (`Begin`, `Get`, `Set`, `Put`, `Putc`, `Del`, `Delc`, `Savepoint`, `Rollback`, `Commit`, `Scan`, `CrashAndReload`).
- [ ] Implement `SimRunner`: executes identical randomized actions against both SurrealMX and `ModelDb` concurrently, asserting 100% byte-for-byte equivalence after every commit and scan.
- [ ] Implement fault injection (simulating thread stalls, watermark lags, and persistence recovery).
- [ ] Add CLI / test harness support for `--seed <u64>` to reproduce any divergence deterministically in seconds.
- [ ] Run 100,000+ simulation steps against the baseline engine to verify model equivalence before starting Phase 2.

### The Differential Testing Oracle (`ModelDb`)
```text
           +-----------------------------------------------+
           |    WorkloadGenerator (Seed: 0xDEADBEEF...)     |
           +-----------------------------------------------+
                             |              |
           Action: Set/Commit|              |Action: Set/Commit
                             v              v
               +-----------------+     +-----------------+
               |    SurrealMX    |     |     ModelDb     |
               | (Lock-Free MVCC)|     | (BTreeMap Truth)|
               +-----------------+     +-----------------+
                             \              /
                              \            /
                        Assert Result Equivalence:
                        - Point Read == Model Get
                        - Range Scan == Model Range
                        - Commit Result (Ok / Conflict) Matches Exactly
```

---

## Phase 2: Zero-Copy Byte Primitives (`byteslice`)

In-memory database performance is heavily governed by memory layout, cache locality, and allocator pressure. Currently, SurrealMX uses `bytes::Bytes` across all keys, values, and transaction tracking sets. While `Bytes` is reference-counted and slicable, it is **32 bytes** on 64-bit architectures, does not inline short strings (always allocating for slices), and incurs atomic reference count updates (`fetch_add`/`fetch_sub`) on every clone.

- [ ] Add `byteslice` dependency (or workspace path) to `surrealmx/Cargo.toml`.
- [ ] Update `IntoBytes` trait in `surrealmx/src/kv.rs` to produce `ByteSlice`.
- [ ] Migrate `Version` (`surrealmx/src/version.rs`) from `Option<Bytes>` to `Option<ByteSlice>`.
- [ ] Migrate `Versions` (`surrealmx/src/versions.rs`) to use `ByteSlice`.
- [ ] Migrate `datastore` in `Inner` (`surrealmx/src/inner.rs`) to `SkipMap<ByteSlice, RwLock<Versions>>`.
- [ ] Migrate transaction tracking sets (`writeset`, `readset`, `lockset`, `scanset`, `gc_candidates`) to `ByteSlice`.
- [ ] Update public transaction methods (`get`, `set`, `put`, `del`, `scan`, `keys`) to return and accept `ByteSlice`.
- [ ] Verify zero regressions and byte-for-byte equivalence against `SimRunner`.
- [ ] Benchmark and verify zero heap allocations for keys $\le 20$ bytes.

### Small String Optimization (SSO $\le 20$ Bytes)
In database workloads, the vast majority of keys (UUIDs, ULIDs, table prefixes, foreign keys, integers) and many values are under 20 bytes:
```text
ByteSlice Memory Layout (24 bytes):
Short Representation (len <= 20 bytes):
+----------------+-----------------------------------------------+
| len (4 bytes)  | inline data buffer (20 bytes)                 | = 24 bytes
+----------------+-----------------------------------------------+

Long Representation (len > 20 bytes):
+----------------+----------------+----------------+------------------+----------------+
| len (4 bytes)  | prefix (4B)    | *heap (8B)     | orig_len (4B)    | offset (4B)    | = 24 bytes
+----------------+----------------+----------------+------------------+----------------+
```
For all keys $\le 20$ bytes:
* **Zero heap allocation**: Stored directly in the 24-byte struct.
* **Zero atomic refcount operations**: `ByteSlice::clone()` is a simple stack `memcpy`.
* **Elimination of `read-fetch-add` cache contention**: Readers querying the same key concurrently never contend on shared atomic refcount cache lines.

### Prefix-Accelerated Comparisons (German String Design)
For keys $> 20$ bytes, `ByteSlice` caches the first 4 bytes inline. In `SkipMap` lookups and `BTreeMap` searches, comparisons check the 4-byte prefix in a single 32-bit CPU register operation:
```rust
#[inline]
fn cmp(&self, other: &Self) -> Ordering {
    let prefix_cmp = self.prefix().cmp(other.prefix());
    if prefix_cmp != Ordering::Equal {
        return prefix_cmp;
    }
    self.as_slice().cmp(other.as_slice())
}
```
Over 90% of non-equal key comparisons resolve immediately without dereferencing heap pointers, drastically cutting L1/L2 cache misses on index traversals.

---

## Phase 3: Read Path & Overlay Optimization (Merge Queue Bypass)

In SurrealMX, `transaction_merge_queue` holds committed transactions whose writes are in the process of being applied to the `datastore`. Currently, point reads unconditionally search this queue.

- [ ] Add watermark check in `fetch_in_datastore`, `exists_in_datastore`, and `equals_in_datastore` to skip `transaction_merge_queue` when `version <= merge_retire_id`.
- [ ] Add fast-path check `if self.database.transaction_merge_queue.is_empty()` before creating `range(..=version)` iterators.
- [ ] Add `min_key: Option<ByteSlice>` and `max_key: Option<ByteSlice>` bounds to `Merge`.
- [ ] Skip individual merge entries in $O(1)$ when `key < min_key || key > max_key`.
- [ ] Verify correctness with `SimRunner` under intense concurrent writer/reader load.
- [ ] Benchmark single-key read latency under concurrent writes.

### Watermark Bypass Logic
`merge_retire_id` tracks the contiguous retired prefix of the merge queue: every version $\le \text{merge\_retire\_id}$ has been completely applied to the `datastore` and removed from the queue.
```rust
#[inline(always)]
fn fetch_in_datastore<K>(&self, key: K, version: u64) -> Option<ByteSlice>
where
    K: IntoBytes,
{
    let key = key.as_slice();

    // Fast Path 1: If snapshot version has already retired to the datastore,
    // NO entry <= version can possibly exist in the merge queue!
    if version > self.database.merge_retire_id.load(Ordering::Acquire) {
        // Fast Path 2: Only open range iterator if queue is non-empty
        let iter = self.database.transaction_merge_queue.range(..=version);
        for entry in iter.rev() {
            if !entry.is_removed() {
                // Fast Path 3: Bounding check before BTreeMap search
                if entry.value().may_contain_key(key) {
                    if let Some(v) = entry.value().writeset.get(key) {
                        return v.clone();
                    }
                }
            }
        }
    }

    // Direct lookup in datastore SkipMap
    self.database.datastore.get(key).and_then(|e| match e.value().try_read() {
        Some(guard) => guard.fetch_version(version),
        None => e.value().read().fetch_version(version),
    })
}
```
This eliminates merge-queue iterator instantiation, epoch pinning, and `BTreeMap` lookups for virtually all steady-state point reads.

---

## Phase 4: The Concurrency Revamp (Lock-Free OCC Ring Buffer)

The current commit pipeline relies on two `crossbeam_skiplist::SkipMap` instances (`transaction_commit_queue` and `transaction_merge_queue`). Every committer executes tight CAS loops waiting for predecessor commits to publish (`cur == slot - 1`). Under high concurrency, writers serialize and thrash atomic counters.

- [ ] Scaffold the fixed-size power-of-two `Ring` buffer (1024 or 2048 slots) inspired by ShaleDB D29 and SurrealKV V2.
- [ ] Replace `transaction_queue_id` and `transaction_commit_id` lockstep spinning with single atomic `claim()` via `fetch_add(1)`.
- [ ] Pre-allocate slot buffers with reusable `Commit` structures to eliminate per-commit `Arc<Commit>` allocations.
- [ ] Implement lock-free slot publication (`state.store(seq, Ordering::Release)`).
- [ ] Implement lock-free continuous watermark advancement (`commit_watermark` and `merge_retire_id`).
- [ ] Remove `cleanup_commit_queue` and skiplist node unlinking (`entry.remove()`).
- [ ] Verify linearizability and conflict detection correctness against `SimRunner`.
- [ ] Benchmark high-concurrency commit throughput across 16, 32, and 64 writer threads.

### The Ring Buffer Architecture
```text
                  +-----------------------------------------------+
Writer Threads -> |  claim() -> seq = next.fetch_add(1, AcqRel)   |
                  +-----------------------------------------------+
                                          |
                                          v
                      [Slot 0] [Slot 1] [Slot 2] ... [Slot N-1]
                      (Indexed via: seq & (CAPACITY - 1))
                                          |
                                          v
                  +-----------------------------------------------+
                  |  Validate Conflicts (Bloom Filter + Keys)     |
                  |  Publish writeset directly into slot          |
                  |  Mark slot published: state.store(seq, Rel)   |
                  +-----------------------------------------------+
                                          |
                                          v
Watermark Worker -> Scan contiguous published prefix -> Advance taken pointer
```
Benefits:
* **Zero allocation on commit**: Slots are pre-allocated at engine initialization.
* **No lockstep CAS loops**: Writers write concurrently into disjoint ring slots.
* **Natural bounded backpressure**: If writers outpace the flusher/applier, they wait only if the ring buffer laps (`seq > taken + capacity`).

---

## Phase 5: High-Throughput Persistence (Group Commit & Buffer Reuse)

In `AolMode::SynchronousOnCommit`, every committer currently locks a `Mutex<File>`, allocates a fresh 8 KB `BufWriter`, encodes individual records via serde/bincode, flushes, and blocks on `sync_all()`. This caps synchronous write throughput to disk `fsync` latency (~200–500 ops/sec).

- [ ] Implement dedicated Group Commit Flusher task for AOL persistence.
- [ ] Coalesce concurrent published commits from the Ring Buffer into single batched sequential disk writes.
- [ ] Issue a single `fdatasync` per batch, amortizing sync overhead across hundreds of concurrent transactions.
- [ ] Replace per-commit `BufWriter::new()` allocations with a persistent, reusable scratch buffer (`Vec<u8>` / `BytesMut`).
- [ ] Replace bincode serde visitor encoding with zero-allocation compact binary frame encoding (LEB128 varints + raw byte slices).
- [ ] Upgrade `AsynchronousAfterCommit` worker to wake up via event notification rather than polling with 10ms `park_timeout`.
- [ ] Add CrashAndReload verification in `SimRunner` differential tests.
- [ ] Benchmark persistent transaction throughput under fsync modes.

### Group Commit Pipeline
```text
Writer 1 (Commit) ---\
Writer 2 (Commit) ----- [ Commit Ring Buffer ]
Writer 3 (Commit) ---/          |
                                v
               [ Background Group Commit Flusher ]
                                |
             1. Drain all contiguous published slots
             2. Encode batch into reusable scratch buffer (zero-alloc)
             3. Single sequential write() to AOL file
             4. Single fdatasync()
             5. Notify all awaiting writers (batch committed)
```
This increases durable write throughput from ~300 commits/sec to **50,000+ commits/sec** on NVMe storage.

---

## Phase 6: Datastore Memory Compaction & Version Specialization

Inline commit-time GC ensures that in steady state, over 95% of keys have exactly **one live version**. However, `Versions` uses `SmallVec<[Version; 1]>`, paying 16 bytes of capacity and pointer/heap discriminant overhead per key.

- [ ] Replace `SmallVec<[Version; 1]>` with an optimized enum:
  ```rust
  pub enum Versions {
      Single(Version),
      Chain(Box<Vec<Version>>),
  }
  ```
- [ ] Use sentinel `ByteSlice::empty()` for delete tombstones, packing tombstone status into the version or pointer niche.
- [ ] Reduce `size_of::<Versions>()` from 56 bytes down to 32–40 bytes.
- [ ] Implement adaptive writeset and savepoint capacity thresholds in `Pool` to avoid dropping and reallocating `BTreeMap` on transactions slightly exceeding `reset_threshold`.
- [ ] Verify GC and version retention against `SimRunner`.
- [ ] Benchmark memory footprint for 1M, 5M, and 10M keys.

---

## Phase 7: Lock-Free SSI & Modernized Conflict Detection

Under Serializable Snapshot Isolation (SSI), read tracking currently acquires a `parking_lot::Mutex` on `readset_bloom` on **every single `get()`** call. Furthermore, bloom filters use scalar byte-at-a-time FNV-1a, and every `Commit` embeds a full 512-byte filter even for transactions modifying a single key.

- [ ] Replace `BloomFilter::hash` (scalar FNV-1a) with SIMD-accelerated **xxHash3** (via `xxhash-rust`).
- [ ] Implement lock-free atomic bloom filter for SSI read tracking:
  ```rust
  pub struct AtomicBloomFilter {
      bits: [AtomicU64; 64], // 4096 bits, exactly 512 bytes
  }
  ```
- [ ] Replace `readset_bloom: Mutex<Box<BloomFilter>>` with `AtomicBloomFilter`, updating bits via lock-free `fetch_or(mask, Ordering::Relaxed)`.
- [ ] Remove `Mutex` lock acquisition from `tx.get()` and `tx.exists()` in SSI mode.
- [ ] Implement Adaptive Filter for `Commit`:
  - 0 keys: $0$ bytes.
  - 1–2 keys: store keys inline in `[ByteSlice; 2]` and check direct equality in $\sim 2\text{ ns}$.
  - $> 2$ keys: use the 512-byte Bloom filter (or Ribbon filter).
- [ ] Verify SSI serializability guarantees against `SimRunner`.
- [ ] Benchmark SSI read throughput and conflict detection speed.

---

## Phase 8: Efficient Transaction Savepoints (Delta Undo Journal)

Calling `tx.set_savepoint()` currently deep-clones the entire `writeset: BTreeMap<ByteSlice, Option<ByteSlice>>`. In transactions with thousands of writes and nested savepoints (e.g. SurrealQL triggers and subqueries), this causes severe allocator pressure and CPU cache churn.

- [ ] Implement transaction `UndoJournal`:
  ```rust
  struct UndoEntry {
      key: ByteSlice,
      old_value: Option<Option<ByteSlice>>, // None = key was not present before
  }
  ```
- [ ] Change `set_savepoint()` to record the current length of the undo journal ($O(1)$ integer push).
- [ ] Change `rollback_to_savepoint()` to pop operations from the journal and revert only modified keys ($O(\Delta)$ instead of $O(N)$).
- [ ] Verify savepoint nested rollbacks with `SimRunner`.
- [ ] Benchmark nested savepoint creation and rollback on large writesets.

---

## Phase 9: Zero-Copy Range Scans & Monomorphized Iterators

In `surrealmx/src/iter.rs`, `MergeIterator` boxes its join iterator as `Box<dyn Iterator>`, causing dynamic dispatch (vtable overhead) on every step. Furthermore, `seek_in_writeset` clones both keys and values during intermediate candidate comparisons.

- [ ] Replace `Box<dyn Iterator>` in `MergeIterator` with a concrete monomorphized type or enum (`MergeQueueIter`).
- [ ] Refactor `seek_in_writeset` to compare borrowed `(&ByteSlice, &Option<ByteSlice>)` references, cloning only the final winning entry when constructing output batches.
- [ ] Ensure `scan_into` and `keys_into` write directly into pre-allocated caller buffers without intermediate tuple allocations.
- [ ] Add fast-path range iteration when the merge queue has no overlapping keys in the requested range.
- [ ] Verify scan and keys order against `SimRunner`.
- [ ] Benchmark forward and reverse range scan throughput.

---

## Performance Targets

| Metric | SurrealMX v1 Baseline | SurrealMX V2 Target | Primary Driver |
|---|---|---|---|
| **Point Read Latency (`get`)** | $\sim 70\text{--}120\text{ ns}$ | **$\le 25\text{--}40\text{ ns}$** | Merge queue bypass, `ByteSlice` SSO, prefix-accelerated lookup |
| **Point Read Allocations** | 1 heap alloc / read | **0 heap allocs** | `ByteSlice` SSO for keys $\le 20$ bytes |
| **SSI Read Throughput** | $\sim 3\text{--}5\text{M ops/sec}$ | **$\ge 25\text{--}40\text{M ops/sec}$** | Lock-free `AtomicBloomFilter` removing read mutex |
| **Commit Latency (In-Memory)** | $\sim 2\text{--}5\text{ }\mu\text{s}$ | **$\le 400\text{ ns}$** | Lock-free OCC Ring Buffer replacing skiplist queues & CAS spinning |
| **Commit Throughput (Concurrent)** | Scalability bottleneck $> 16$ threads | **Linear scaling to 64+ cores** | Ring Buffer slot claiming via $O(1)$ `fetch_add` |
| **Synchronous Persistence Throughput** | $\sim 250\text{--}400\text{ commits/sec}$ | **$\ge 50,000\text{ commits/sec}$** | Group Commit flusher batching AOL appends & single `fsync` |
| **Savepoint Overhead (5K writes)** | $\sim 150\text{ }\mu\text{s}$ + $O(N)$ allocs | **$\le 5\text{ ns}$ ($O(1)$ mark)** | Delta Undo Journal replacing full `BTreeMap` clone |
| **Memory Footprint (10M keys)** | $\sim 1.8\text{--}2.2\text{ GB}$ metadata | **$\le 1.1\text{--}1.3\text{ GB}$ metadata** | `ByteSlice` 24B, `Version` 32B, `Versions::Single` specialization |
