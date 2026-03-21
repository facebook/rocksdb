# RocksDB Write Path

## Overview

The RocksDB write path transforms user write operations (Put, Delete, Merge, etc.) into durable, queryable state. The path involves serializing operations into a `WriteBatch`, coordinating concurrent writers via `WriteThread`, persisting data to the Write-Ahead Log (WAL), and inserting into the active `MemTable`. Back-pressure mechanisms (`WriteController`, `WriteBufferManager`) throttle writes when compaction or memory usage falls behind.

### High-Level Write Flow

```
User API (Put/Delete/Merge)
    |
    v
WriteBatch (serialize operations into binary format)
    |
    v
DBImpl::WriteImpl (entry point, validation, mode selection)
    |
    v
WriteThread::JoinBatchGroup (enqueue writer, elect leader)
    |
    +---> Leader elected
    |
    v
PreprocessWrite (check errors, flush triggers, stall/delay)
    |
    v
EnterAsBatchGroupLeader (form write group from pending writers)
    |
    v
WriteGroupToWAL (leader writes all batches to WAL)
    |
    v
Assign sequence numbers
    |
    v
InsertInto MemTable (serial or parallel)
    |
    v
ExitAsBatchGroupLeader (wake followers, advance sequence)
```

### Write Modes

| Mode | WAL | MemTable | Use Case |
|------|-----|----------|----------|
| **Default** | Leader writes group | Serial or parallel | Standard writes |
| **Pipelined** | WAL and memtable writes overlap across groups | Separate memtable writer group | Higher throughput, higher latency |
| **Unordered** | Through write thread | Independent per writer | Prepared transactions (WritePrepared) |
| **WAL-only** | Through nonmem_write_thread_ | Skipped | 2PC prepare phase |

---

## 1. WriteBatch

**Files:** `include/rocksdb/write_batch.h`, `db/write_batch_internal.h`, `db/write_batch.cc`

### What It Does

WriteBatch is the fundamental unit of atomicity in RocksDB. It collects multiple write operations (Put, Delete, Merge, SingleDelete, DeleteRange, PutEntity, TimedPut) into a single serialized buffer (`rep_`) that is written atomically to the WAL and MemTable.

### Binary Encoding Format

```
WriteBatch::rep_ :=
    sequence: fixed64       (8 bytes, starting sequence number)
    count:    fixed32       (4 bytes, number of records)
    data:     record[count]

Header: 12 bytes total (WriteBatchInternal::kHeader)
```

Each record is encoded as:

```
record :=
    kTypeValue                          varstring varstring
    kTypeDeletion                       varstring
    kTypeSingleDeletion                 varstring
    kTypeRangeDeletion                  varstring varstring
    kTypeMerge                          varstring varstring
    kTypeColumnFamilyValue              varint32 varstring varstring
    kTypeColumnFamilyDeletion           varint32 varstring
    kTypeColumnFamilySingleDeletion     varint32 varstring
    kTypeColumnFamilyRangeDeletion      varint32 varstring varstring
    kTypeColumnFamilyMerge             varint32 varstring varstring
    kTypeWideColumnEntity               varstring varstring
    kTypeColumnFamilyWideColumnEntity   varint32 varstring varstring
    kTypeBeginPrepareXID                (no payload)
    kTypeEndPrepareXID                  varstring
    kTypeCommitXID                      varstring
    kTypeCommitXIDAndTimestamp           varstring varstring
    kTypeRollbackXID                    varstring
    kTypeBeginPersistedPrepareXID       (no payload)
    kTypeBeginUnprepareXID              (no payload)
    kTypeNoop                           (no payload)

varstring :=
    len:  varint32
    data: uint8[len]
```

Operations targeting the default column family (CF id 0) use shorter type codes without the `varint32` CF id prefix.

### Key Data Structures

- **`rep_`** (`std::string`): The serialized binary buffer. All operations append to this buffer.
- **`content_flags_`** (`std::atomic<uint32_t>`): Lazily-computed bitmask tracking which operation types are present (HasPut, HasDelete, etc.). Mutable to allow lazy computation on const methods.
- **`save_points_`**: Stack of SavePoint structs for transactional rollback. Each SavePoint records `(size, count, content_flags)` at the time of `SetSavePoint()`.
- **`wal_term_point_`**: Marks the boundary between data that goes to WAL vs memtable-only data.
- **`prot_info_`**: Optional per-key-value checksums (8 bytes per key when enabled) stored in `ProtectionInfo` using `ProtectionInfoKVOC64`.

### Invariants

- The header is always 12 bytes. `sequence` is set by `WriteBatchInternal::SetSequence()` before WAL write.
- `count` reflects the number of user-visible operations (excludes LogData, Noop, BeginPrepare, etc.).
- Operations are applied in insertion order during iteration and memtable insertion.
- Thread safety: multiple threads may call const methods concurrently, but non-const methods require external synchronization.
- `max_bytes_` enforces a size limit. When exceeded, `LocalSavePoint::commit()` rolls back and returns `Status::MemoryLimit()`.

### Handler/Visitor Pattern

`WriteBatch::Iterate(Handler*)` walks through `rep_` and dispatches each record to the appropriate `Handler` virtual method (`PutCF`, `DeleteCF`, `MergeCF`, etc.). This pattern is used for:
- Inserting into MemTable (`MemTableInserter` in `write_batch.cc`)
- Writing to WAL
- Timestamp updates
- Content flag computation

### SavePoint Mechanism

```
SetSavePoint()     -> push (size, count, content_flags) onto save_points_ stack
RollbackToSavePoint() -> truncate rep_ to saved size, restore count and flags
PopSavePoint()     -> discard the most recent save point without rollback
```

### Interactions

- **WAL**: The entire `rep_` is written as a single WAL record via `log::Writer::AddRecord()`.
- **MemTable**: `WriteBatchInternal::InsertInto()` iterates the batch and calls `MemTable::Add()` for each record.
- **Sequence Numbers**: Set by `WriteBatchInternal::SetSequence()` before WAL write. Each key consumes one sequence number (unless `seq_per_batch` mode).
- **Two-Phase Commit (2PC)**: Special record types (`BeginPrepare`, `EndPrepare`, `Commit`, `Rollback`) bracket transaction boundaries within the batch.

---

## 2. WriteThread

**Files:** `db/write_thread.h`, `db/write_thread.cc`

### What It Does

WriteThread coordinates concurrent writers into batch groups to amortize WAL write costs. It implements a lock-free linked list where writers enqueue themselves, one is elected leader, and the leader performs WAL writes on behalf of the group. It supports serial memtable writes, parallel memtable writes, and pipelined WAL/memtable writes.

### Writer State Machine

```
STATE_INIT (1)
    |
    +---> STATE_GROUP_LEADER (2)        -- elected as batch group leader
    |         |
    |         +---> (do WAL + memtable work as leader)
    |         +---> STATE_COMPLETED (16)
    |
    +---> STATE_MEMTABLE_WRITER_LEADER (4) -- pipelined: lead memtable group
    |         |
    |         +---> STATE_COMPLETED (16)
    |
    +---> STATE_PARALLEL_MEMTABLE_CALLER (64) -- help set states for parallel writers
    |         |
    |         +---> STATE_PARALLEL_MEMTABLE_WRITER (8)
    |
    +---> STATE_PARALLEL_MEMTABLE_WRITER (8)  -- write own batch to memtable
    |         |
    |         +---> STATE_COMPLETED (16)
    |
    +---> STATE_COMPLETED (16)           -- leader wrote on behalf of follower
    |
    +---> STATE_LOCKED_WAITING (32)      -- blocked waiting for state change
```

### Key Data Structures

- **`Writer`**: Per-writer state containing the `WriteBatch*`, sync/no_slowdown/disable_wal flags, sequence number, status, callback pointers, and linked list pointers (`link_older`, `link_newer`).
- **`WriteGroup`**: A batch group with `leader`, `last_writer`, `last_sequence`, `status`, and an atomic `running` counter for parallel writes. Iterable via `begin()`/`end()`.
- **`newest_writer_`** (`std::atomic<Writer*>`): Lock-free singly-linked list head. New writers CAS onto this. Only the leader can remove elements.
- **`newest_memtable_writer_`** (`std::atomic<Writer*>`): Separate queue for pipelined memtable writes.

### How It Works

#### JoinBatchGroup

1. Writer calls `LinkOne()` to atomically CAS itself onto `newest_writer_`.
2. If it's the only writer (was directly linked as head), it becomes the leader immediately.
3. Otherwise, it enters `AwaitState()` which adaptively spins then blocks on a mutex/condvar, waiting to be woken as leader, follower-done, or parallel writer.

#### EnterAsBatchGroupLeader

1. Leader traverses the linked list from `newest_writer_` backward via `link_older` to find all pending writers.
2. Fills in `link_newer` pointers (they are lazily computed via `CreateMissingNewerLinks()`).
3. Builds the `WriteGroup` respecting size limits (`max_write_batch_group_size_bytes`): the limit is applied when the leader's own batch exceeds 1/8 of the max.
4. Writers with incompatible flags (e.g., mixed sync/no-sync) can still be grouped; the leader handles sync for the whole group.
5. Writers that return `false` from `CheckCallback()` have `CallbackFailed()` set but remain in the group.

#### ExitAsBatchGroupLeader

1. Sets status on all group members.
2. Wakes followers by setting their state to `STATE_COMPLETED`.
3. Finds the next leader among remaining writers and sets its state to `STATE_GROUP_LEADER`.

#### Parallel MemTable Writes

When `allow_concurrent_memtable_write` is true and the group has no Merge operations:
1. Leader calls `LaunchParallelMemTableWriters()` which sets each follower's state to `STATE_PARALLEL_MEMTABLE_WRITER`.
2. For large groups, `sqrt(N)` callers are designated as `STATE_PARALLEL_MEMTABLE_CALLER` to help wake writers in parallel (cost bounded to `2*sqrt(N)` sequential SetState calls).
3. Each writer inserts its own batch into the memtable concurrently.
4. Each writer calls `CompleteParallelMemTableWriter()` which decrements `running`. The last to finish returns `true` and is responsible for advancing the sequence number and exiting the group.

### Stall Mechanism

- `BeginWriteStall()`: Inserts a dummy `write_stall_dummy_` writer at the tail. New writers with `no_slowdown=true` fail immediately. Others block on `stall_cv_`.
- `EndWriteStall()`: Removes the dummy writer and signals blocked writers via `stall_cv_`.
- Stall counts (`stall_begun_count_`, `stall_ended_count_`) allow waiters to detect when their specific stall has cleared.

### Adaptive Waiting (AwaitState)

Uses an `AdaptationContext` to decide between spinning (`std::this_thread::yield()`) and blocking on mutex/condvar:
- Controlled by `max_yield_usec_` and `slow_yield_usec_` from `ImmutableDBOptions`.
- Tracks whether yields are productive and adapts the strategy per call site.

### Interactions

- **DBImpl**: `WriteImpl()` creates a `Writer`, calls `JoinBatchGroup()`, then performs WAL and memtable operations based on returned state.
- **WriteBatch**: Each `Writer` holds a pointer to its `WriteBatch`.
- **WAL**: Only the leader writes to WAL on behalf of the group.

---

## 3. Write-Ahead Log (WAL)

**Files:** `db/log_format.h`, `db/log_writer.h`, `db/log_writer.cc`, `db/log_reader.h`, `db/log_reader.cc`, `db/wal_manager.h`, `db/wal_edit.h`

### What It Does

The WAL provides crash recovery by persisting WriteBatch contents before they are applied to MemTable. The WAL uses a block-based format inherited from LevelDB with extensions for recycling, compression, and user-defined timestamps.

### WAL Record Format

The WAL is divided into fixed-size blocks of `kBlockSize = 32768` bytes (32 KB).

#### Legacy Record Header (7 bytes)

```
+---------+-----------+-----------+--- ... ---+
|CRC (4B) | Size (2B) | Type (1B) | Payload   |
+---------+-----------+-----------+--- ... ---+
```

- **CRC**: CRC32c over type + payload.
- **Size**: Length of payload in bytes (max ~32KB per fragment).
- **Type**: Record type determining how fragments combine.

#### Recyclable Record Header (11 bytes)

```
+---------+-----------+-----------+----------------+--- ... ---+
|CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
+---------+-----------+-----------+----------------+--- ... ---+
```

Adds a 4-byte log number to distinguish records from the current log vs. a previous (recycled) log file.

#### Record Types

| Type | Value | Purpose |
|------|-------|---------|
| `kZeroType` | 0 | Reserved for preallocated files |
| `kFullType` | 1 | Complete record fits in one fragment |
| `kFirstType` | 2 | First fragment of a multi-fragment record |
| `kMiddleType` | 3 | Middle fragment(s) |
| `kLastType` | 4 | Last fragment of a multi-fragment record |
| `kRecyclableFullType` | 5 | Full record in recycled log |
| `kRecyclableFirstType` | 6 | First fragment in recycled log |
| `kRecyclableMiddleType` | 7 | Middle fragment in recycled log |
| `kRecyclableLastType` | 8 | Last fragment in recycled log |
| `kSetCompressionType` | 9 | Sets compression for subsequent records |
| `kUserDefinedTimestampSizeType` | 10 | Records CF-to-timestamp-size mapping |
| `kRecyclableUserDefinedTimestampSizeType` | 11 | Same for recycled logs |
| `kPredecessorWALInfoType` | 130 | WAL verification info |
| `kRecyclePredecessorWALInfoType` | 131 | WAL verification info (recycled) |

Records with bit 7 set (`kRecordTypeSafeIgnoreMask = 0x80`) in the type can be safely ignored by readers that don't understand them, enabling forward compatibility.

### How Records Span Blocks

When a WriteBatch payload exceeds the remaining space in the current block:

1. Write a `kFirstType` record with as much payload as fits.
2. Continue with `kMiddleType` records for full blocks.
3. End with `kLastType` record for the final fragment.
4. If fewer than `kHeaderSize` bytes remain in a block, pad with zeros.

### log::Writer

- **`block_offset_`**: Current write position within the 32KB block.
- **`EmitPhysicalRecord()`**: Writes a single physical record (header + payload). Handles padding when insufficient space remains in the block.
- **`AddRecord()`**: Splits a logical record across blocks as needed.
- **`manual_flush_`**: When true, caller must explicitly call `WriteBuffer()` to flush. Used for batching WAL writes across multiple WriteBatches.
- **Compression**: Optional streaming compression via `StreamingCompress`. Set with `kSetCompressionType` record.
- **Pre-computed CRC**: `type_crc_[]` array pre-computes CRC of each record type byte to reduce per-record overhead.

### log::Reader

- Reads physical records from the WAL file.
- Reassembles fragmented records into complete logical records.
- Reports corruption via a `Reporter` callback.
- Handles recycled logs by checking the embedded log number.
- Supports reading compressed WALs by decompressing on the fly.

### WalManager

- Manages WAL file lifecycle: listing, sorting, purging, and archiving.
- `GetSortedWalFiles()`: Returns all WAL files sorted by log number.
- `PurgeObsoleteWALFiles()`: Deletes WALs older than configured TTL or size limit.
- `GetUpdatesSince()`: Returns an iterator over WAL entries newer than a given sequence number (used for replication).
- Caches first-record sequence numbers in `read_first_record_cache_` to avoid re-reading WAL headers.

### WalEdit (WAL Tracking in MANIFEST)

- **`WalAddition`**: Records creation or sync of a WAL. Contains `WalNumber` and optional `WalMetadata` (synced size in bytes).
- **`WalDeletion`**: Records deletion of WALs with number less than a threshold.
- **`WalSet`**: In-memory set of active WALs maintained in `VersionSet`. Updated via `AddWal()`/`DeleteWalsBefore()`. Used for crash recovery verification via `CheckWals()`.
- WAL additions/deletions are persisted to MANIFEST as part of `VersionEdit`.

### Interactions

- **WriteBatch**: Each WriteBatch `rep_` is written as one logical WAL record.
- **DBImpl**: `WriteGroupToWAL()` concatenates all batches in a write group and writes them as a single WAL record (or one per batch if using `WriteOptions::sync`).
- **Recovery**: `DBImpl::RecoverLogFiles()` replays WAL entries by iterating records and inserting into MemTable.

---

## 4. MemTable

**Files:** `db/memtable.h`, `db/memtable.cc`, `include/rocksdb/memtablerep.h`

### What It Does

MemTable is the in-memory write buffer for a single column family. It receives writes from WriteBatch, supports concurrent reads, and is eventually flushed to an SST file. Each column family has one active (mutable) MemTable and zero or more immutable MemTables pending flush.

### Internal Key Format in MemTable

Entries are stored as length-prefixed keys in the MemTableRep:

```
+------------------+------------------+------------------+------------------+
| internal_key_len | user_key         | seq (7B) + type  | value_len        |
| (varint32)       | (variable)       | (1B) = 8 bytes   | (varint32)       |
+------------------+------------------+------------------+------------------+
| value            | [checksum (opt)] |
| (variable)       | (4B if enabled)  |
+------------------+------------------+
```

- `internal_key_len` = `user_key.size() + 8` (includes 7-byte sequence + 1-byte type packed as `(seq << 8) | type`)
- Optional 4-byte per-entry checksum when `protection_bytes_per_key` is set.

### Key Data Structures

- **`table_`** (`std::unique_ptr<MemTableRep>`): The primary data structure (default: concurrent skip list). Stores point operations.
- **`range_del_table_`** (`std::unique_ptr<MemTableRep>`): Separate skip list for range deletions.
- **`arena_`** (`ConcurrentArena`): Thread-safe arena allocator for all MemTable memory. Enables efficient concurrent allocation.
- **`bloom_filter_`** (`std::unique_ptr<DynamicBloom>`): Optional prefix bloom filter for faster negative lookups.
- **`comparator_`** (`KeyComparator`): Wraps `InternalKeyComparator` for ordering entries by (user_key ASC, sequence DESC, type DESC).
- **`fragmented_range_tombstone_list_`**: Cached fragmented range tombstones, constructed when memtable becomes immutable.

### MemTableRep Interface

`MemTableRep` is the pluggable interface for the underlying data structure:

- **SkipListRep** (default): Lock-free concurrent skip list. Supports concurrent reads and a single writer, or concurrent inserts with `InsertConcurrently()`.
- **HashSkipListRep**: Hash map of skip lists, optimized for prefix-scoped iteration.
- **VectorRep**: Unordered vector, sorted lazily on first iteration. Good for bulk loading.

Key requirements for any MemTableRep:
1. No duplicate items.
2. Uses `KeyComparator` for ordering.
3. Supports concurrent reads during writes.
4. Items are never deleted (append-only).

### Memory Allocation

- All MemTable memory comes from the `ConcurrentArena`, which allocates from large blocks (`arena_block_size`, default 1MB) with per-core sharding to reduce contention.
- `AllocTracker` (`mem_tracker_`) reports allocation to `WriteBufferManager`.

### How Writes Work

`MemTable::Add(seq, type, key, value, kv_prot_info, allow_concurrent, post_process_info, hint)`:

1. Encode the entry: `[internal_key_len][user_key][seq+type][value_len][value][checksum]`
2. Allocate space from `MemTableRep` via `Allocate()`.
3. Copy encoded entry into allocated space.
4. Insert into `table_` (or `range_del_table_` for range deletions).
5. Update counters (`num_entries_`, `data_size_`, `num_deletes_`, `num_range_deletes_`).
6. If concurrent: use atomic counters via `MemTablePostProcessInfo`, batch-update later via `BatchPostProcess()`.
7. Check `UpdateFlushState()` to see if flush should be triggered.

### Flush Triggers

The memtable tracks a `flush_state_` enum:
- `FLUSH_NOT_REQUESTED`: Normal state.
- `FLUSH_REQUESTED`: Set when `ShouldFlushNow()` returns true (memory usage approaching `write_buffer_size`).
- `FLUSH_SCHEDULED`: Set when `MarkFlushScheduled()` is called by the flush scheduler.

`ShouldFlushNow()` returns true when:
- `ApproximateMemoryUsage()` >= `write_buffer_size`, OR
- The arena's allocated bytes significantly exceed the data size (fragmentation), OR
- `memtable_max_range_deletions_` is exceeded.

### Reference Counting

- `Ref()` / `Unref()`: Simple integer ref count (not atomic -- requires external synchronization).
- Memtable is deleted when ref count drops to zero.
- Refs are held by: active SuperVersion, immutable MemTableList, iterators.

### Concurrent Insert Support

When `allow_concurrent_memtable_write` is true:
- Multiple threads call `Add()` with `allow_concurrent=true`.
- Each thread uses `InsertConcurrently()` on the MemTableRep (lock-free skip list CAS).
- Counters are batched per-writer in `MemTablePostProcessInfo` and applied atomically via `BatchPostProcess()`.
- Arena uses per-core sharding (`ConcurrentArena`) to reduce allocation contention.

### Interactions

- **WriteBatch**: `WriteBatchInternal::InsertInto()` iterates the batch and calls `MemTable::Add()` for each record.
- **MemTableList**: When switched to immutable, the memtable is moved to `MemTableList`.
- **WriteBufferManager**: Memory is charged/freed via `AllocTracker`.
- **FlushJob**: Reads all entries via `NewIterator()` and writes them to an SST file.

---

## 5. MemTableList

**Files:** `db/memtable_list.h`, `db/memtable_list.cc`

### What It Does

MemTableList manages the immutable memtables for a single column family. When the active memtable is full, it is "switched" -- marked immutable and added to this list. Background flush jobs pick memtables from this list, write them to SST files, and remove them. Reads must traverse all immutable memtables (newest first) in addition to the active memtable.

### Key Data Structures

#### MemTableListVersion

A snapshot of the immutable memtable list, used for lock-free reads:

- **`memlist_`** (`std::list<ReadOnlyMemTable*>`): Immutable memtables not yet flushed, ordered newest-first.
- **`memlist_history_`** (`std::list<ReadOnlyMemTable*>`): Already-flushed memtables kept for transaction conflict checking.
- **`max_write_buffer_size_to_maintain_`**: Controls how much history to retain.
- Reference counted (`Ref()`/`Unref()`). Stored in SuperVersion for lock-free read access.

#### MemTableList

The mutable manager (requires DB mutex):

- **`current_`** (`MemTableListVersion*`): Current version, replaced on each structural change.
- **`num_flush_not_started_`**: Count of memtables pending flush.
- **`flush_requested_`**: Overrides `min_write_buffer_number_to_merge` to force flush.
- **`min_write_buffer_number_to_merge_`**: Minimum number of memtables to accumulate before flushing.
- **`imm_flush_needed`** (`std::atomic<bool>`): Signal for background threads to check for pending flushes.

### How It Works

#### Adding Immutable MemTables

`MemTableList::Add(m, to_delete)`:
1. Creates a new `MemTableListVersion` by copying `current_`.
2. Adds `m` to the front of `memlist_`.
3. Increments `num_flush_not_started_`.
4. Sets `imm_flush_needed` if there are enough memtables to merge.
5. Trims history if `max_write_buffer_size_to_maintain_` is exceeded.
6. Old version is unref'd, deleted memtables collected in `to_delete`.

#### Picking MemTables for Flush

`PickMemtablesToFlush(max_memtable_id, mems)`:
1. Scans `memlist_` from oldest to newest.
2. Selects memtables with ID <= `max_memtable_id` that haven't started flushing.
3. Marks selected memtables as `flush_in_progress_`.
4. Decrements `num_flush_not_started_` for each picked memtable.

#### Installing Flush Results

`TryInstallMemtableFlushResults()`:
1. Called after a flush job completes. Checks if this flush's memtables are the oldest pending ones.
2. If not (because an older flush is still in progress), defers installation -- FIFO ordering is required for correctness.
3. When ready: removes flushed memtables from `memlist_`, moves them to `memlist_history_`, writes to MANIFEST, and installs a new `MemTableListVersion`.

#### Atomic Flush

For `atomic_flush` mode, all column families are flushed together to ensure a consistent cut point:
- `AssignAtomicFlushSeq()`: Assigns a sequence number to all unflushed immutable memtables.
- `InstallMemtableAtomicFlushResults()`: Atomically installs flush results across multiple column families.

### Reads Through Immutable MemTables

`MemTableListVersion::Get()`:
1. Iterates `memlist_` from newest to oldest.
2. Calls `Get()` on each memtable.
3. Returns as soon as a definitive result is found (value, deletion, or merge result).

### Invariants

- Memtables in `memlist_` are ordered by creation time (newest first).
- Flush results must be committed to MANIFEST in FIFO order (oldest first).
- `num_flush_not_started_` is always consistent with the actual count.
- History trimming respects `max_write_buffer_size_to_maintain_`.

### Interactions

- **MemTable**: Receives immutable memtables via `Add()`.
- **FlushJob**: Picks memtables via `PickMemtablesToFlush()`, installs results via `TryInstallMemtableFlushResults()`.
- **SuperVersion**: Holds a ref to the current `MemTableListVersion` for lock-free reads.
- **VersionSet**: Flush results are recorded in `VersionEdit` and persisted to MANIFEST.

---

## 6. WriteBufferManager

**Files:** `include/rocksdb/write_buffer_manager.h`, `memtable/write_buffer_manager.cc`

### What It Does

WriteBufferManager tracks and limits memory usage across all MemTables, potentially across multiple DB instances sharing the same manager. It provides three mechanisms: flush triggering, write stalling, and optional block cache charging.

### Key State

| Field | Type | Purpose |
|-------|------|---------|
| `buffer_size_` | `atomic<size_t>` | Memory budget (0 = unlimited) |
| `mutable_limit_` | `atomic<size_t>` | 7/8 of buffer_size_, threshold for flush |
| `memory_used_` | `atomic<size_t>` | Total memory across all memtables |
| `memory_active_` | `atomic<size_t>` | Memory in mutable (active) memtables only |
| `allow_stall_` | `atomic<bool>` | Whether to stall writes when over budget |
| `stall_active_` | `atomic<bool>` | Whether a stall is currently in effect |
| `queue_` | `list<StallInterface*>` | DB instances to block/unblock during stalls |

### Flush Triggering (ShouldFlush)

Called by the write path leader in `PreprocessWrite()`:

```
ShouldFlush() returns true when:
  1. mutable_memtable_memory_usage() > mutable_limit_ (7/8 of buffer_size), OR
  2. memory_usage() >= buffer_size AND mutable >= buffer_size/2
```

The second condition prevents triggering more flushes when most memory is already in immutable memtables being flushed.

### Write Stalling (ShouldStall)

When `allow_stall = true`:
- `ShouldStall()` returns true when `stall_active_` is set OR `memory_usage() >= buffer_size_`.
- `BeginWriteStall()`: Adds the DB's `StallInterface` to `queue_` and blocks the caller.
- `MaybeEndWriteStall()`: Called after memory is freed. If `memory_usage() < buffer_size_`, unblocks all queued DBs by calling `Signal()`.

### Cache-Based Memory Management

When constructed with a `Cache`:
- `ReserveMem()` / `FreeMem()` insert/remove dummy entries in the block cache.
- This makes MemTable memory compete with block cache for the same memory budget.
- Managed through `CacheReservationManager`.

### Memory Tracking Functions

- **`ReserveMem(size_t mem)`**: Called when allocating memory for a memtable. Atomically adds to `memory_used_` and `memory_active_`.
- **`ScheduleFreeMem(size_t mem)`**: Called when a memtable becomes immutable. Subtracts from `memory_active_` but NOT from `memory_used_`.
- **`FreeMem(size_t mem)`**: Called when a memtable is actually freed. Subtracts from `memory_used_` and potentially ends write stall.

### Interactions

- **MemTable**: Each MemTable's `AllocTracker` calls `ReserveMem()`/`ScheduleFreeMem()`/`FreeMem()`.
- **DBImpl::PreprocessWrite()**: Checks `ShouldFlush()` to trigger memtable switches.
- **DBImpl**: Checks `ShouldStall()` and calls `BeginWriteStall()`/`MaybeEndWriteStall()`.
- **Block Cache**: When enabled, memory competes with cached blocks.

---

## 7. WriteController

**Files:** `db/write_controller.h`, `db/write_controller.cc`

### What It Does

WriteController manages write throttling in response to compaction pressure. When compaction cannot keep up with the write rate, it either delays writes (rate-limiting) or stops them entirely. All methods require holding the DB mutex.

### States and Tokens

The controller uses a token-based system with three token types:

| Token Type | Effect | Trigger |
|------------|--------|---------|
| `StopWriteToken` | Completely stops all writes | L0 file count >= `level0_stop_writes_trigger` |
| `DelayWriteToken` | Rate-limits writes | L0 file count >= `level0_slowdown_writes_trigger` or too many compaction bytes pending |
| `CompactionPressureToken` | Speeds up compaction threads | Moderate compaction pressure |

State is tracked by atomic counters:
- `total_stopped_`: Number of active stop tokens (any > 0 means writes are stopped).
- `total_delayed_`: Number of active delay tokens.
- `total_compaction_pressure_`: Number of compaction pressure tokens.

Tokens are RAII: the destructor decrements the corresponding counter.

### Rate Limiting (GetDelay)

`GetDelay(clock, num_bytes)` computes microseconds to sleep:

1. Uses a credit-based system: `credit_in_bytes_` accumulates available write budget.
2. Credits are refilled periodically based on `delayed_write_rate_` (bytes/second).
3. If `num_bytes <= credit_in_bytes_`, returns 0 (no delay needed).
4. Otherwise, computes delay = `(num_bytes - credit) / rate * 1,000,000` microseconds.
5. Caps single delays at 100ms to avoid long stalls.

### Rate Adjustment

- `delayed_write_rate_`: Current effective write rate, dynamically adjusted.
- `max_delayed_write_rate_`: Upper bound, set at initialization or via `SetDBOptions()`.
- When multiple column families request delays with different rates, the slowest rate wins.
- Rate is halved or doubled by `RecalculateWriteStallConditions()` based on compaction progress.

### Low-Priority Rate Limiter

- `low_pri_rate_limiter_`: A `GenericRateLimiter` for throttling low-priority writes (e.g., compaction output).
- Default: 1 MB/s.

### Interactions

- **DBImpl::RecalculateWriteStallConditions()**: Called after compaction/flush. Acquires or releases tokens based on L0 file count and pending compaction bytes.
- **DBImpl::PreprocessWrite()**: Checks `IsStopped()` and `NeedsDelay()`, calls `DelayWrite()` which uses `GetDelay()`.
- **WriteThread**: `BeginWriteStall()` / `EndWriteStall()` on the write thread coordinate with `WriteController` state.

---

## 8. DBImpl Write Orchestration

**Files:** `db/db_impl/db_impl_write.cc`

### What It Does

`DBImpl::WriteImpl()` is the central write orchestration function. It coordinates all write path components: input validation, pre-processing (flush/stall checks), write group formation, WAL writing, memtable insertion, and post-processing.

### Entry Points

| Method | Purpose |
|--------|---------|
| `DB::Put/Delete/Merge/...` | Create a single-op WriteBatch, call `Write()` |
| `DBImpl::Write()` | Add protection info if needed, call `WriteImpl()` |
| `DBImpl::WriteWithCallback()` | Write with optimistic transaction callback |
| `DBImpl::WriteImpl()` | Main implementation |
| `DBImpl::PipelinedWriteImpl()` | Pipelined write mode |
| `DBImpl::WriteImplWALOnly()` | WAL-only writes (2PC, unordered) |
| `DBImpl::UnorderedWriteMemtable()` | Unordered memtable writes |

### WriteImpl Step-by-Step (Default Mode)

#### 1. Validation
- Check batch is non-null.
- Validate `protection_bytes_per_key` (0 or 8).
- Check timestamp requirements.
- Check compatibility flags (pipelined vs 2PC, unordered, etc.).

#### 2. Mode Selection
- If `two_write_queues_ && disable_memtable`: route to `WriteImplWALOnly()`.
- If `unordered_write`: route to `WriteImplWALOnly()` then `UnorderedWriteMemtable()`.
- If `enable_pipelined_write`: route to `PipelinedWriteImpl()`.
- Otherwise: continue with default path.

#### 3. JoinBatchGroup
- Create `WriteThread::Writer` with the batch and options.
- Call `write_thread_.JoinBatchGroup(&w)`.
- If returned as `STATE_COMPLETED`: another leader handled this write, return.
- If returned as `STATE_PARALLEL_MEMTABLE_WRITER`: insert into memtable, call `CompleteParallelMemTableWriter()`.
- If returned as `STATE_GROUP_LEADER`: continue as leader.

#### 4. PreprocessWrite (Leader Only)
Performed with the DB mutex held as needed:

```
1. Check error_handler_ (is DB stopped?)
2. If WAL total size > max_total_wal_size: SwitchWAL()
3. If WriteBufferManager::ShouldFlush(): HandleWriteBufferManagerFlush()
4. If trim_history_scheduler_ not empty: TrimMemtableHistory()
5. If flush_scheduler_ not empty: ScheduleFlushes()
6. If WriteController::IsStopped() or NeedsDelay(): DelayWrite()
7. If WriteBufferManager::ShouldStall(): WriteBufferManagerStallWrites()
```

#### 5. EnterAsBatchGroupLeader
- Build write group from pending writers.
- Count total keys, byte size, check for merges (affects parallel eligibility).
- Run callbacks for each writer.

#### 6. Write to WAL
- If WAL enabled: `WriteGroupToWAL()` serializes all batches and writes to log.
- Assigns sequence numbers: `current_sequence = last_sequence + 1`.
- If sync requested: `MarkLogsSynced()` and persist WAL metadata to MANIFEST.

#### 7. Write to MemTable
Two paths:

**Serial** (default, or when merges present):
```
WriteBatchInternal::InsertInto(write_group, sequence, memtables, ...)
```
Leader iterates all batches in the group and inserts into memtable sequentially.

**Parallel** (`allow_concurrent_memtable_write && group_size > 1 && no merges`):
```
write_thread_.LaunchParallelMemTableWriters(&write_group)
// Each writer (including leader) inserts own batch concurrently
WriteBatchInternal::InsertInto(&w, w.sequence, ..., concurrent=true)
// Last to finish advances sequence
```

#### 8. Post-Processing
- Call `post_memtable_callback` for each writer if set.
- Advance `versions_->SetLastSequence(last_sequence)`.
- `ExitAsBatchGroupLeader()`: wake followers, find next leader.
- On failure: `HandleMemTableInsertFailure()` sets background error.

### Pipelined Write Mode

Separates WAL writing and memtable writing into two overlapping stages:

```
Group 1: [WAL write] ----> [MemTable write]
Group 2:              [WAL write] ----> [MemTable write]
```

1. Leader forms a write group and writes to WAL.
2. Leader calls `ExitAsBatchGroupLeader()` for the WAL group, which links writers to `newest_memtable_writer_`.
3. A new WAL leader can begin immediately.
4. The memtable leader enters via `EnterAsMemTableWriter()`, writes to memtable (serial or parallel).
5. Calls `ExitAsMemTableWriter()` when done.

Key difference: uses `WriteThread::UpdateLastSequence()` to coordinate sequence numbers between WAL and memtable groups.

### Unordered Write Mode

For WritePrepared/WriteUnprepared transactions:

1. First pass: `WriteImplWALOnly()` writes batch to WAL through the write thread and assigns a sequence number.
2. Second pass: `UnorderedWriteMemtable()` inserts into memtable independently (no write group).
3. Uses `pending_memtable_writes_` atomic counter. `SwitchMemtable()` waits for this counter to reach zero before switching.

### Sequence Number Assignment

- In default mode: `last_sequence = versions_->LastSequence()`. New batch gets `current_sequence = last_sequence + 1`. Each key in the batch uses one sequence number.
- `seq_per_batch_` mode: Each sub-batch (transaction) gets one sequence number.
- With `two_write_queues_`: `FetchAddLastAllocatedSequence()` for lock-free allocation.
- `SetLastSequence()` is only called after successful memtable insertion to make writes visible to readers.

### Write Stall Coordination

`DelayWrite()` in PreprocessWrite:
1. If `WriteController::IsStopped()`: call `WriteThread::BeginWriteStall()`, wait until not stopped, then `EndWriteStall()`.
2. If `WriteController::NeedsDelay()`: compute delay via `GetDelay()`, sleep for that duration.
3. If `no_slowdown` is set on WriteOptions: return `Status::Incomplete` instead of blocking.
4. `WriteBufferManager` stalls are handled separately via `WriteBufferManagerStallWrites()` which calls `BeginWriteStall()` on the WBM.

---

## Data Flow Summary

### Write Data Flow (Default Mode)

```
User: db->Put(key, value)
  |
  v
DB::Put() -> WriteBatch::Put() -> WriteBatch::rep_ appended
  |
  v
DBImpl::Write() -> WriteImpl()
  |
  v
PreprocessWrite()
  |  Check: error state, WAL size, WBM flush, flush scheduler, write stall
  |  May trigger: SwitchWAL, SwitchMemtable, DelayWrite
  |
  v
WriteThread::JoinBatchGroup()
  |  Writer enqueues self on lock-free list
  |  Elected as leader or waits as follower
  |
  v
[Leader] EnterAsBatchGroupLeader()
  |  Collect pending writers into WriteGroup
  |  Validate callbacks, count keys, check parallel eligibility
  |
  v
[Leader] WriteGroupToWAL()
  |  Concatenate all WriteBatch::rep_ into one buffer
  |  log::Writer::AddRecord() -> physical records across 32KB blocks
  |  Optional: sync WAL, record sync in MANIFEST
  |
  v
Assign sequence numbers
  |  current_sequence = last_sequence + 1
  |  Each writer gets: writer->sequence = next_sequence
  |  next_sequence += count_of_keys_in_batch
  |
  v
WriteBatchInternal::InsertInto()
  |  Serial: leader iterates all batches
  |  Parallel: each writer inserts own batch concurrently
  |    For each record in batch:
  |      MemTable::Add(seq, type, key, value)
  |        -> encode entry
  |        -> allocate from ConcurrentArena
  |        -> insert into SkipList
  |
  v
[Leader] ExitAsBatchGroupLeader()
  |  versions_->SetLastSequence(last_sequence)  // makes writes visible
  |  Wake followers with STATE_COMPLETED
  |  Set next leader to STATE_GROUP_LEADER
  |
  v
Return Status to each writer
```

### Memory Flow

```
WriteBatch created (user heap)
  |
  v
MemTable::Add()
  |  ConcurrentArena::Allocate() -> large block from malloc/mmap
  |  AllocTracker -> WriteBufferManager::ReserveMem()
  |    -> memory_used_ += size
  |    -> memory_active_ += size
  |    -> (optional) charge to block cache
  |
  v
MemTable marked immutable
  |  WriteBufferManager::ScheduleFreeMem()
  |    -> memory_active_ -= size  (still counted in memory_used_)
  |  MemTable added to MemTableList
  |
  v
Flush to SST
  |  MemTable deleted
  |  WriteBufferManager::FreeMem()
  |    -> memory_used_ -= size
  |    -> MaybeEndWriteStall()
```

### Back-Pressure Flow

```
Compaction falls behind
  |
  v
RecalculateWriteStallConditions()
  |  L0 files >= slowdown_trigger:
  |    WriteController::GetDelayToken(rate)
  |  L0 files >= stop_trigger:
  |    WriteController::GetStopToken()
  |
  v
PreprocessWrite() [leader only]
  |  WriteController::IsStopped()?
  |    -> WriteThread::BeginWriteStall()
  |    -> All writers block (no_slowdown writers fail immediately)
  |    -> Wait until token released
  |    -> WriteThread::EndWriteStall()
  |  WriteController::NeedsDelay()?
  |    -> GetDelay(bytes) -> sleep N microseconds
  |  WriteBufferManager::ShouldFlush()?
  |    -> SwitchMemtable, schedule flush
  |  WriteBufferManager::ShouldStall()?
  |    -> BeginWriteStall on WBM
  |    -> Block until memory freed
```
