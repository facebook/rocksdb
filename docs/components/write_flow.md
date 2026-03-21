# RocksDB Write Flow

## Overview

This document traces the complete lifecycle of a write operation from application API call through WAL persistence, MemTable insertion, flush to L0 SST files, and eventual compaction into sorted levels. It describes the **full end-to-end write path** including deletion handling, flow control, and crash recovery.

### High-Level Write Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  APPLICATION LAYER                                              │
│  Put/Delete/SingleDelete/DeleteRange/Merge/WriteBatch          │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  v
┌─────────────────────────────────────────────────────────────────┐
│  WRITE COORDINATION (db/db_impl/db_impl_write.cc)              │
│  ┌──────────────────────────────────────────────────┐          │
│  │ DBImpl::WriteImpl                                │          │
│  │  ├─ PreprocessWrite (flow control, flush trigger)│          │
│  │  ├─ WriteThread (leader election, batching)      │          │
│  │  ├─ WAL write (log::Writer::AddRecord)           │          │
│  │  ├─ Sequence number assignment                   │          │
│  │  ├─ MemTable insertion                           │          │
│  │  └─ Publish sequence (SetLastSequence)           │          │
│  └──────────────────────────────────────────────────┘          │
└─────────────────┬───────────────────────────────────────────────┘
                  │
    ┌─────────────┴─────────────┐
    │                           │
    v                           v
┌───────────────────┐   ┌──────────────────────────────┐
│  WAL (PERSISTENT) │   │  MEMTABLE (IN-MEMORY BUFFER) │
│  32KB blocks      │   │  SkipList, sorted by         │
│  CRC'd records    │   │  (user_key, seq_desc)        │
└───────────────────┘   └──────────┬───────────────────┘
                                   │
                    MemTable full? │
                                   v
                        ┌──────────────────┐
                        │  FLUSH TO L0 SST │
                        │  (FlushJob)      │
                        └──────────┬───────┘
                                   │
                                   v
                        ┌──────────────────────────┐
                        │  L0 SST FILES (SORTED)   │
                        │  Overlapping key ranges  │
                        └──────────┬───────────────┘
                                   │
                    Compaction trigger?
                                   v
                        ┌──────────────────────────┐
                        │  COMPACTION              │
                        │  (CompactionJob)         │
                        │  └─ Merge sorted runs    │
                        │  └─ Drop tombstones      │
                        │  └─ Apply merges         │
                        └──────────┬───────────────┘
                                   │
                                   v
                        ┌──────────────────────────┐
                        │  L1+ SST FILES           │
                        │  Non-overlapping ranges  │
                        │  Tiered by level         │
                        └──────────────────────────┘
```

---

## 1. Application Write APIs

**Files:** `db/db_impl/db_impl_write.cc:23-144`

### Entry Points

RocksDB provides multiple write operations, all funneling through `DBImpl::WriteImpl`:

| API | ValueType | Description |
|-----|-----------|-------------|
| `Put(key, value)` | `kTypeValue` | Insert or overwrite key |
| `Delete(key)` | `kTypeDeletion` | Tombstone (covers all older versions) |
| `SingleDelete(key)` | `kTypeSingleDeletion` | Tombstone pairing with exactly one `Put` |
| `DeleteRange(start, end)` | `kTypeRangeDeletion` | Tombstone covering `[start, end)` |
| `Merge(key, operand)` | `kTypeMerge` | Apply merge operator |
| `PutEntity(key, columns)` | `kTypeWideColumnEntity` | Wide-column insert |
| `Write(WriteBatch)` | Mixed | Atomic batch of operations |

All single-operation APIs (`Put`, `Delete`, etc.) construct a `WriteBatch` internally and call `DB::Write()` → `DBImpl::WriteImpl()`.

### WriteBatch: The Atomic Unit

**Files:** `include/rocksdb/write_batch.h`, `db/write_batch.cc`

A `WriteBatch` serializes multiple operations into a single binary buffer (`rep_`), ensuring atomicity: all operations succeed or all fail together.

**Binary format:**

```
┌──────────────────────────────────────────────────────────┐
│  HEADER (12 bytes)                                       │
│  ├─ sequence:    fixed64  (placeholder, filled later)    │
│  └─ count:       fixed32  (number of ops)                │
├──────────────────────────────────────────────────────────┤
│  OPERATIONS (variable length)                            │
│  ├─ tag:         uint8     (ValueType)                   │
│  ├─ [cf_id]:     varint32  (only for CF-prefixed ops)    │
│  ├─ key:         varstring (varint32 length + bytes)     │
│  └─ value:       varstring (for Put/Merge/Range ops)     │
│  ... (repeat for each operation)                         │
└──────────────────────────────────────────────────────────┘

varstring := length: varint32 || data: uint8[length]
```

**Key fields** (`db/write_batch.cc`):

| Field | Type | Purpose |
|-------|------|---------|
| `rep_` | `std::string` | Binary buffer |
| `content_flags_` | `atomic<uint32_t>` | Bitmask of operation types present |
| `prot_info_` | `unique_ptr<ProtectionInfo>` | Optional per-entry checksums (8 bytes/key) |
| `save_points_` | `unique_ptr<SavePoints>` | Rollback snapshots |

**⚠️ INVARIANT:** The sequence number field in the header is set to 0 initially and **only populated by the write leader** during `DBImpl::WriteImpl()` after WAL write but before memtable insertion. This ensures sequence numbers are assigned in WAL order.

### Write Options Validation

Before entering the write path, `DBImpl::WriteImpl` validates options (`db/db_impl/db_impl_write.cc:370-493`):

- **Batch non-null**: `my_batch != nullptr`
- **Timestamps set**: If writing to memtable, `!WriteBatchInternal::TimestampsUpdateNeeded(*my_batch)`
- **Rate limiter priority**: Only `Env::IO_TOTAL` or `Env::IO_USER`
- **Protection bytes**: Must be 0 or 8
- **Incompatible combinations**:
  - `sync && disableWAL` ❌ (cannot sync without WAL)
  - `two_write_queues && enable_pipelined_write` ❌
  - `unordered_write && enable_pipelined_write` ❌
  - `seq_per_batch && enable_pipelined_write` ❌
  - `disableWAL && recycle_log_file_num > 0` ❌ (recycled WALs need sequential seqnos)

---

## 2. Write Thread & Group Commit

**Files:** `db/write_thread.h`, `db/write_thread.cc`

### What It Does

`WriteThread` coordinates concurrent writers using **lock-free leader election** and **write batching**. One thread (the leader) performs WAL and memtable writes on behalf of a group, amortizing synchronization overhead.

### Writer State Machine

```
STATE_INIT (1)
    │
    ├─→ STATE_GROUP_LEADER (2)              ← Leader: drives group
    ├─→ STATE_COMPLETED (16)                ← Follower: done
    ├─→ STATE_PARALLEL_MEMTABLE_WRITER (8)  ← Parallel memtable insert
    ├─→ STATE_MEMTABLE_WRITER_LEADER (4)    ← Pipelined memtable leader
    └─→ STATE_LOCKED_WAITING (32)           ← Blocked on condvar
```

**Writer struct** (`db/write_thread.h:124-288`):

| Field | Type | Purpose |
|-------|------|---------|
| `batch` | `WriteBatch*` | The batch to write |
| `sync` | `bool` | Requires `fsync()` after WAL |
| `no_slowdown` | `bool` | Fail immediately if stalled |
| `disable_wal` | `bool` | Skip WAL (non-durable) |
| `disable_memtable` | `bool` | WAL-only write (2PC prepare) |
| `state` | `atomic<uint8_t>` | Current state |
| `sequence` | `SequenceNumber` | Assigned sequence number |
| `status` | `Status` | Result |
| `link_older` / `link_newer` | `Writer*` | Intrusive linked list |

### Lock-Free Enqueue

**`WriteThread::LinkOne(Writer* w)`** (`db/write_thread.cc`):

```cpp
bool WriteThread::LinkOne(Writer* w, std::atomic<Writer*>* newest_writer) {
  Writer* writers = newest_writer->load(std::memory_order_relaxed);
  while (true) {
    w->link_older = writers;
    if (newest_writer->compare_exchange_weak(writers, w)) {
      return (writers == nullptr);  // true = became leader
    }
  }
}
```

CAS loop on `newest_writer_`. If the queue was empty (`writers == nullptr`), the caller becomes the leader immediately.

### Leader Election

**`WriteThread::JoinBatchGroup(Writer* w)`** (`db/write_thread.cc`):

1. Call `LinkOne(w, &newest_writer_)`.
2. If `LinkOne` returns `true`, `w` is the leader → return immediately.
3. Otherwise, wait via `AwaitState(w, STATE_GROUP_LEADER | STATE_MEMTABLE_WRITER_LEADER | STATE_PARALLEL_MEMTABLE_WRITER | STATE_COMPLETED)`.

### Group Formation

**`WriteThread::EnterAsBatchGroupLeader(WriteGroup* write_group)`** (`db/write_thread.cc`):

The leader walks from itself (oldest enqueued) to `newest_writer_` (snapshot at entry), collecting compatible writers into a `WriteGroup`.

**Compatibility criteria:**
- Same `sync`, `disable_wal`, `disable_memtable`, `protection_bytes_per_key`
- No callback that disallows batching (`AllowWriteBatching() == false`)
- Not an unbatched `WriteBatchWithIndex` ingest
- Total size does not exceed `max_write_batch_group_size_bytes`

**Size limiting heuristic**: If the leader's batch is small (`<= max_size/8`), cap the group at `leader_size + max_size/8` to avoid penalizing small writes.

Incompatible writers are spliced out and re-linked after the group for the next leader.

### Adaptive Wait

**`WriteThread::AwaitState(Writer* w, uint8_t goal_mask)`** (`db/write_thread.cc`):

Three-phase wait to minimize latency:

1. **Spin (200 iterations)**: `AsmVolatilePause()` (CPU pause instruction, ~1.4 µs total)
2. **Yield (adaptive)**: `std::this_thread::yield()` for up to `max_yield_usec_` (default 3000 µs). `AdaptationContext` tracks yield effectiveness; after 3 slow yields, escalates to blocking.
3. **Block**: Create mutex+condvar, CAS state to `STATE_LOCKED_WAITING`, wait on condvar.

**⚠️ INVARIANT:** `SetState(w, state)` checks if `w->state == STATE_LOCKED_WAITING` and uses mutex+notify. This prevents lost wakeups.

### Parallel Memtable Writes

**`WriteThread::LaunchParallelMemTableWriters(WriteGroup* write_group)`** (`db/write_thread.cc`):

When `parallel_memtable_writes` is enabled:

1. Set `write_group->running = group_size`.
2. Transition each writer to `STATE_PARALLEL_MEMTABLE_WRITER`.
3. For large groups (≥20), use **O(√n) two-level scheme**:
   - Select `sqrt(group_size)` stride leaders.
   - Each stride leader is set to `STATE_PARALLEL_MEMTABLE_CALLER` and wakes its stride of workers.

Each worker inserts its own batch concurrently. `CompleteParallelMemTableWriter(write_group)` atomically decrements `running`; the **last** worker (when `running` reaches 0) publishes the sequence and exits the group.

**⚠️ INVARIANT:** Exactly one thread observes `running == 0` and returns `true` from `CompleteParallelMemTableWriter`.

### Write Modes

| Mode | Config | WAL Queue | MemTable Queue | Concurrency |
|------|--------|-----------|----------------|-------------|
| **Normal (batched)** | default | `write_thread_` | same thread | Leader does WAL + memtable for entire group |
| **Pipelined** | `enable_pipelined_write` | `write_thread_` | `newest_memtable_writer_` | WAL and memtable groups overlap |
| **Two-queue** | `two_write_queues` | `nonmem_write_thread_` | `write_thread_` | WAL-only vs memtable threads decoupled |
| **Unordered** | `unordered_write` | `write_thread_` | per-writer | No ordering between writers |

**Pipelined write:** After WAL write, the leader links eligible writers into `newest_memtable_writer_` queue and releases WAL lock. A separate leader election drives the memtable phase, allowing the next WAL group to proceed concurrently.

---

## 3. Write-Ahead Log (WAL)

**Files:** `db/log_format.h`, `db/log_writer.h`, `db/log_reader.h`

### Purpose

The WAL provides **crash recovery** by persisting `WriteBatch` data to disk **before** memtable insertion. On recovery, WAL records are replayed to reconstruct memtable state.

**⚠️ INVARIANT:** **WAL must be written before MemTable**. This ensures that if a crash occurs after memtable insertion but before the next flush, the WAL contains all un-flushed writes.

### Record Format

WAL files are divided into **32 KB blocks** (`kBlockSize = 32768`). Each block contains one or more physical records. Logical records (WriteBatch data) that don't fit in one block are fragmented.

**Legacy header (7 bytes):**

```
┌───────────┬──────────┬──────────┐
│ CRC (4B)  │ Size (2B)│ Type (1B)│
└───────────┴──────────┴──────────┘
CRC = crc32c(type || payload)
```

**Recyclable header (11 bytes):**

```
┌───────────┬──────────┬──────────┬────────────────┐
│ CRC (4B)  │ Size (2B)│ Type (1B)│ Log Number (4B)│
└───────────┴──────────┴──────────┴────────────────┘
```

The log number in recyclable headers detects stale data from previous file incarnations when `recycle_log_file_num > 0`.

**Record types** (`db/log_format.h:22-49`):

| Type | Value | Meaning |
|------|-------|---------|
| `kZeroType` | 0 | Preallocated padding (never written) |
| `kFullType` | 1 | Complete logical record |
| `kFirstType` | 2 | First fragment of multi-block record |
| `kMiddleType` | 3 | Interior fragment |
| `kLastType` | 4 | Last fragment |
| `kRecyclable{Full,First,Middle,Last}Type` | 5-8 | Same, but for recycled files |
| `kSetCompressionType` | 9 | Meta-record: compression algorithm |
| `kUserDefinedTimestampSizeType` | 10 | Meta-record: CF timestamp sizes |
| `kPredecessorWALInfoType` | 130 | WAL chain verification |

Types ≥ 10 use bit 0 to distinguish recyclable (odd) from non-recyclable (even). Types with bit 7 set (`kRecordTypeSafeIgnoreMask = 0x80`) may be safely skipped by older readers.

### log::Writer

**`log::Writer::AddRecord(const Slice& slice)`** (`db/log_writer.cc`):

1. **Compression** (if enabled): compress payload via `compression_ctx_->Compress()`.
2. **Fragmentation**: split into blocks of `kBlockSize - header_size`.
3. **Per-fragment**:
   - Determine type (`kFull` / `kFirst` / `kMiddle` / `kLast`).
   - Compute CRC: `crc32c::Extend(type_crc_[type], fragment.data(), fragment.size())`.
   - Write header + payload via `EmitPhysicalRecord()`.
4. **Padding**: zero-fill remaining block space if next header won't fit.

**Pre-computed `type_crc_[kMaxRecordType+1]`** avoids recomputing CRC of the type byte on every write.

### log::Reader

**`log::Reader::ReadRecord(Slice* record, std::string* scratch)`** (`db/log_reader.cc`):

1. Loop: `ReadPhysicalRecord()` reads from a 32 KB buffer (`backing_store_`).
2. Verify CRC. If recyclable header, verify log number matches.
3. Reassemble fragments:
   - `kFull` → return immediately.
   - `kFirst` → start accumulating into `scratch`.
   - `kMiddle` → append to `scratch`.
   - `kLast` → append to `scratch` and return.
4. Decompress if needed.
5. Meta-records (`kSetCompressionType`, `kUserDefinedTimestampSizeType`) are consumed internally.

**⚠️ INVARIANT:** A logical record is always `kFull` or `kFirst [kMiddle...] kLast`. Corruption is reported if fragments are out of order.

### WAL File Lifecycle

```
Created → Written (AddRecord) → Synced (fsync)
    ↓
  Memtable flushed → WAL archived or recycled
    ↓
  Purged (TTL/size limit exceeded)
```

**WalSet** (`db/version_edit.h`, tracked in `VersionSet`):
- Tracks live WALs via MANIFEST: `WalAddition` (created/synced size) and `WalDeletion` (obsolete).
- **⚠️ INVARIANT:** At most one WAL may be open (unknown synced size) at a time.
- WALs transition from unknown-size to known-size after flush; never reverse.

### Sync Modes

| Mode | `WriteOptions::sync` | `manual_wal_flush_` | Behavior |
|------|---------------------|---------------------|----------|
| Auto-sync | `true` | `false` | `fsync()` after each write group |
| Auto-flush | `false` | `false` | Flush to OS buffer cache (no fsync) |
| Manual flush | `false` | `true` | App calls `FlushWAL()` explicitly |

---

## 4. MemTable Insert

**Files:** `db/memtable.h`, `db/memtable.cc`

### What It Does

`MemTable` is an in-memory sorted data structure (default: lock-free skiplist) that receives writes before flush to SST files. It supports concurrent reads during writes.

### Internal Key Format

Every memtable entry is encoded as:

```
┌──────────────────────────────────────────────────────────┐
│  internal_key_size:  varint32   (user_key.size() + 8)   │
├──────────────────────────────────────────────────────────┤
│  user_key:           uint8[user_key.size()]              │
│                      (may include user timestamp suffix) │
├──────────────────────────────────────────────────────────┤
│  packed_tag:         fixed64                             │
│                      (sequence_number << 8) | value_type │
├──────────────────────────────────────────────────────────┤
│  value_size:         varint32                            │
├──────────────────────────────────────────────────────────┤
│  value:              uint8[value_size]                   │
├──────────────────────────────────────────────────────────┤
│  checksum:           uint8[0 or 8]                       │
│                      (if protection_bytes_per_key == 8)  │
└──────────────────────────────────────────────────────────┘
```

**⚠️ INVARIANT:** Within the same `user_key`, entries are sorted by **descending sequence number** (newest first). This ensures point lookups find the latest version immediately.

### Data Structures

**Storage** (`db/memtable.h`):

| Field | Type | Purpose |
|-------|------|---------|
| `arena_` | `ConcurrentArena` | All memtable memory (shared by point + range-del tables) |
| `table_` | `unique_ptr<MemTableRep>` | Point key storage (skiplist/hash-skiplist/vector) |
| `range_del_table_` | `unique_ptr<MemTableRep>` | Always skiplist; stores range tombstones |
| `bloom_filter_` | `unique_ptr<DynamicBloom>` | Prefix/whole-key bloom (null if disabled) |

**Tracking**:

| Field | Type | Purpose |
|-------|------|---------|
| `first_seqno_` | `atomic<SequenceNumber>` | Seqno of first inserted key (0 if empty) |
| `earliest_seqno_` | `atomic<SequenceNumber>` | Lower bound on all seqnos |
| `data_size_` | `RelaxedAtomic<size_t>` | Total encoded bytes |
| `num_entries_` | `RelaxedAtomic<size_t>` | Entry count |
| `flush_state_` | `atomic<FlushStateEnum>` | NOT_REQUESTED → REQUESTED → SCHEDULED |

### Insertion

**`MemTable::Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value, ...)`** (`db/memtable.cc`):

1. **Compute size**: `encoded_len = varint(ikey_size) + ikey_size + varint(val_size) + val_size + protection_bytes`.
2. **Allocate**: `table_->Allocate(encoded_len, &buf)` (from `arena_`).
3. **Encode**: Write length-prefixed internal key (user_key + packed tag), length-prefixed value, optional checksum.
4. **Bloom update**: Add prefix and/or whole key (timestamp stripped) to bloom filter.
5. **Route**: `kTypeRangeDeletion` → `range_del_table_`, others → `table_`.
6. **Insert**: `table_->InsertKey(handle)` (skiplist insert, O(log n) expected).

**Serial vs. concurrent**:
- **Serial**: Direct `InsertKey()` + atomic counter updates.
- **Concurrent** (`allow_concurrent_memtable_write`): `InsertKeyConcurrently()` + thread-local `MemTablePostProcessInfo` batching. After the entire write batch completes, `BatchPostProcess()` drains accumulated counts into atomic counters.

**⚠️ INVARIANT:** Entries are **never removed**. The entire memtable is discarded atomically on flush.

### Point Lookup

**`MemTable::Get(const LookupKey& key, std::string* value, ...)`** (`db/memtable.cc`):

1. If `num_entries_ == 0`, return `false`.
2. **Bloom filter check** (prefix or whole-key). On miss: return `false` immediately.
3. **Range tombstone check**: `MaxCoveringTombstoneSeqnum(user_key)` from `range_del_table_`.
4. **Table lookup**: `table_->Get(key, &saver, SaveValue)`.
   - `SaveValue` callback is invoked for each entry with matching `user_key`, newest first.
   - Dispatch on `ValueType`:
     - `kTypeValue`: copy value, return `true`.
     - `kTypeDeletion`: return `NotFound`.
     - `kTypeMerge`: accumulate operands via `MergeHelper`.
     - `kTypeRangeDeletion`: covered by range tombstone, return `NotFound`.

### Flush Trigger

**`MemTable::ShouldFlushNow()`** (`db/memtable.cc`):

Returns `true` if:
- External signal (`IsMarkedForFlush()`), or
- Range deletion count ≥ limit, or
- Arena allocation ≥ `write_buffer_size` **and** last allocated block > 75% used.

Called at the end of every `Add()` or `BatchPostProcess()`. On `true`, transitions `flush_state_` from `FLUSH_NOT_REQUESTED` → `FLUSH_REQUESTED`.

---

## 5. Sequence Number Assignment

**Files:** `db/version_set.h`, `db/db_impl/db_impl_write.cc`

### Sequence Number Invariants

**⚠️ INVARIANT:** Sequence numbers are **monotonically increasing** per column family. They serve multiple purposes:
1. **Snapshot isolation**: Readers see consistent point-in-time views.
2. **Write ordering**: Determines which version of a key is visible.
3. **Merge ordering**: Merge operands are applied newest-to-oldest by sequence number.

### Sequence Number Space

| Concept | Variable | Range | Visibility |
|---------|----------|-------|------------|
| `LastSequence` | `versions_->LastSequence()` | `[0, kMaxSequenceNumber]` | Highest sequence visible to **readers** |
| `LastAllocatedSequence` | `versions_->LastAllocatedSequence()` | `[LastSequence, ∞)` | Highest allocated (may be ahead of `LastSequence`) |

**Normal write path** (`enable_pipelined_write == false`, `two_write_queues == false`):

```cpp
// db/db_impl/db_impl_write.cc
SequenceNumber current_sequence = versions_->LastSequence() + 1;
for (Writer* w : write_group) {
  w->sequence = current_sequence;
  current_sequence += WriteBatchInternal::Count(w->batch);
  WriteBatchInternal::SetSequence(w->batch, w->sequence);
}
versions_->SetLastSequence(current_sequence - 1);
```

**Two-queue / unordered paths**: Use `versions_->FetchAddLastAllocatedSequence(total_count)` atomically under `wal_write_mutex_`.

**⚠️ INVARIANT:** Sequence numbers are assigned **after WAL write** but **before memtable insertion**. This ensures WAL records can be replayed with correct sequence numbers during recovery.

### Sequence Per Batch vs. Sequence Per Key

| Mode | `seq_per_batch_` | Sequence Consumption |
|------|------------------|----------------------|
| **Sequence per key** | `false` | Each key in batch consumes one sequence |
| **Sequence per batch** | `true` | Entire batch shares one sequence |

Sequence-per-batch mode is used for optimistic transactions to detect write conflicts at batch granularity.

---

## 6. Flush: MemTable → L0 SST

**Files:** `db/flush_job.h`, `db/flush_job.cc`, `db/memtable_list.h`

### Overview

When a memtable fills (`ShouldFlushNow() == true`), it transitions from mutable to **immutable** and is added to the `MemTableList`. A background thread runs `FlushJob` to convert immutable memtables into an L0 SST file.

### FlushJob Lifecycle

**Three-phase execution** (`db/flush_job.cc`):

| Phase | Lock | Description |
|-------|------|-------------|
| `PickMemTable()` | Mutex held | Select memtables with `id <= max_memtable_id_`, allocate file number, ref `Version` |
| `Run()` | Mutex held (released during I/O) | Attempt MemPurge or `WriteLevel0Table()`, install results |
| `Cancel()` | Mutex held | Abandon flush, unref `Version` |

### Flush Flow

```
MemTable::ShouldFlushNow() == true
    │
    v
MemTable::UpdateFlushState() → FLUSH_REQUESTED
    │
    v
DBImpl::ScheduleFlushes() → enqueue flush job
    │
    v
MemTableList::PickMemtablesToFlush(max_id, mems)
    │  ├─ Select oldest memtables not yet flushing
    │  └─ Set flush_in_progress_ = true
    │
    v
FlushJob::PickMemTable()
    │  ├─ Allocate file number via versions_->NewFileNumber()
    │  └─ Ref base_ = cfd->current() (Version at flush time)
    │
    v
FlushJob::Run()
    │  ├─ Attempt MemPurge? (experimental in-memory GC)
    │  │    └─ If success: replace immutable memtable, done
    │  └─ Fall through: WriteLevel0Table()
    │
    v
FlushJob::WriteLevel0Table()
    │  1. Release db_mutex_
    │  2. Create iterators over picked memtables (point + range-del)
    │  3. MergingIterator(mem_iters, range_del_iters)
    │  4. BuildTable() → write L0 SST
    │  5. Re-acquire db_mutex_
    │  6. Update VersionEdit (add L0 file metadata)
    │
    v
MemTableList::TryInstallMemtableFlushResults()
    │  ├─ Single-writer gate (commit_in_progress_)
    │  ├─ FIFO ordering: wait until oldest memtable completes
    │  ├─ Collect contiguous completed memtables
    │  ├─ LogAndApply() → commit to MANIFEST
    │  └─ Remove from memlist_ (move to history or unref)
    │
    v
WriteBufferManager::FreeMem(size)
    │  └─ MaybeEndWriteStall() if memory usage drops below threshold
```

### FIFO Flush Ordering

**⚠️ INVARIANT:** Flush commit order **must match** memtable creation order. This prevents data loss: older data cannot overwrite newer data.

**`MemTableList::TryInstallMemtableFlushResults()`** (`db/memtable_list.cc`):

```cpp
// Wait until the oldest memtable (back of memlist_) is flush_completed_
while (!memlist_.empty() && !memlist_.back()->flush_completed_) {
  return false;  // Retry later
}

// Collect contiguous completed memtables from back
while (!memlist_.empty() && memlist_.back()->flush_completed_) {
  mems_to_commit.push_back(memlist_.back());
  memlist_.pop_back();
}

// Apply VersionEdit atomically
Status s = vset_->LogAndApply(cfd, mutable_cf_options, edit_list, ...);
```

Even if memtable N+1 finishes flushing before memtable N, its result is **not installed** until N completes.

### Atomic Flush

When `atomic_flush == true`, multiple column families are flushed atomically. `InstallMemtableAtomicFlushResults()` (free function in `db/memtable_list.cc`) commits flush results across CFs via a **single `LogAndApply()`** call with one `VersionEdit` per CF.

**⚠️ INVARIANT:** Atomic flush: either all CF flushes are visible in `MANIFEST` or none are.

---

## 7. Compaction: L0 → L1+ (High-Level)

**Files:** `db/compaction/compaction_picker.h`, `db/compaction/compaction_job.h`

### What It Does

Compaction merges SST files across levels to:
1. **Reclaim space**: Drop tombstones and overwritten keys.
2. **Reduce read amplification**: Merge L0 overlapping files into non-overlapping L1+.
3. **Enforce level size targets**: `target_file_size_base`, `max_bytes_for_level_base`.

See [compaction.md](compaction.md) for detailed documentation.

### Trigger Points

| Trigger | Score | Action |
|---------|-------|--------|
| **L0 file count** | `num_L0_files / l0_file_num_compaction_trigger` | Compact L0 → L1 |
| **Level size** | `level_bytes / target_bytes_for_level` | Compact Ln → Ln+1 |
| **Manual compaction** | User calls `CompactRange()` | Compact specified range |
| **Deletion compaction** | Many tombstones detected | Compact to drop tombstones |

### Compaction Data Flow

```
CompactionPicker::PickCompaction()
    │  ├─ Examine VersionStorageInfo::CompactionScore()
    │  └─ Return Compaction* (input files, levels, options)
    │
    v
CompactionJob::Prepare()
    │  └─ Divide key range into subcompaction boundaries
    │
    v
CompactionJob::Run()
    │  For each subcompaction (parallel):
    │      MergingIterator(input SST files)
    │          │
    │          v
    │      CompactionIterator (dedup, delete, merge, filter)
    │          │
    │          v
    │      CompactionOutputs (split into output SST files)
    │
    v
CompactionJob::Install()
    │  └─ VersionEdit (delete inputs, add outputs) → LogAndApply()
```

**⚠️ INVARIANT:** Compaction **never moves data to a higher (lower-numbered) level**. `output_level >= start_level` always.

---

## 8. Delete Path: Tombstone Lifecycle

### Overview

RocksDB uses **tombstones** to represent deletions. Tombstones flow through the write path like normal keys, but are eventually dropped during compaction when safe.

### Tombstone Types

| Type | API | Encoding | Scope |
|------|-----|----------|-------|
| `kTypeDeletion` | `Delete(key)` | point key | Covers all versions of `key` with `seq < tombstone_seq` |
| `kTypeSingleDeletion` | `SingleDelete(key)` | point key | Pairs with exactly one `Put` of same key |
| `kTypeRangeDeletion` | `DeleteRange(start, end)` | range `[start, end)` | Covers all keys in range with `seq < tombstone_seq` |

### Tombstone Write Path

1. **API** → **WriteBatch** (encoded as `kTypeDeletion` / `kTypeSingleDeletion` / `kTypeRangeDeletion`).
2. **WAL** → persisted as regular record.
3. **MemTable** → inserted into `table_` (point tombstones) or `range_del_table_` (range tombstones).
4. **Flush** → written to L0 SST:
   - Point tombstones: data blocks.
   - Range tombstones: meta-block (`FragmentedRangeTombstoneList`).

### Tombstone Visibility During Reads

**Point lookup** (`db/db_impl/db_impl.cc:GetImpl`):

```cpp
// Check range tombstones first
SequenceNumber max_covering_tombstone_seq =
    range_del_agg.MaxCoveringTombstoneSeqnum(user_key);

// Memtable lookup
if (mem->Get(lkey, &value, &seq, &type)) {
  if (type == kTypeDeletion) return NotFound;
  if (seq < max_covering_tombstone_seq) return NotFound;
  return value;
}
```

**Iterator** (`db/db_iter.cc:DBIter`):

`DBIter` wraps a `MergingIterator` (which interleaves range tombstone sentinel keys) and resolves deletions:
- `kTypeDeletion`: skip past current key's older versions.
- `kTypeRangeDeletion`: integrated via `RangeDelAggregator::ShouldDelete()`.

### Tombstone Cleanup: When Can We Drop Them?

**CompactionIterator** (`db/compaction/compaction_iterator.cc`) drops tombstones when **all three conditions** hold:

1. **Bottommost level**: No data exists below `output_level`.
2. **No snapshots need it**: `sequence < earliest_snapshot`.
3. **Key doesn't exist in lower levels**: Verified via `KeyNotExistsBeyondOutputLevel()`.

**For `kTypeSingleDeletion`:**

Additional constraint: must pair with exactly one `Put`. Violation (multiple `Put`s or zero `Put`s) is a **corruption** (or user error). CompactionIterator verifies this and reports error.

**For `kTypeRangeDeletion`:**

Range tombstones are dropped per-level during compaction:
1. **Flush** (L0): Range tombstones covering flushed memtable seqno range are written to SST meta-block.
2. **Compaction** (Ln → Ln+1): Range tombstones are fragmented at SST file boundaries (`TruncatedRangeDelIterator`) and merged. Those fully consumed by compaction are dropped.
3. **Bottommost compaction**: Range tombstones with `seq < earliest_snapshot` and no overlapping keys below are dropped.

**⚠️ INVARIANT:** Range tombstones are **truncated at SST file boundaries** via `TruncatedRangeDelIterator` to prevent leaking beyond file scope.

### Delete Flow Summary

```
Delete(key) → WriteBatch(kTypeDeletion)
    │
    v
WAL (persisted tombstone)
    │
    v
MemTable (tombstone entry with seqno)
    │
    v
Flush → L0 SST (data block contains tombstone)
    │
    v
Compaction (Ln → Ln+1)
    │  ├─ If older Put/Merge covered by tombstone: drop both
    │  └─ If bottommost + no snapshots + key not below: drop tombstone
    │
    v
Key fully reclaimed (space recovered)
```

---

## 9. WriteController & Flow Control

**Files:** `db/write_controller.h`, `db/write_controller.cc`, `include/rocksdb/write_buffer_manager.h`

### Overview

RocksDB uses **back-pressure mechanisms** to prevent write amplification from exceeding compaction capacity. Two subsystems coordinate flow control:

1. **WriteController**: Per-DB, triggered by compaction lag (L0 files, pending compaction bytes).
2. **WriteBufferManager**: Shared across DBs, triggered by memtable memory usage.

### WriteController

**`WriteController`** (`db/write_controller.h`) manages **token-based** rate limiting or full stops.

**Token types:**

| Token | Counter | Effect |
|-------|---------|--------|
| `StopWriteToken` | `total_stopped_` | Full write stop; writers wait on `bg_cv_` |
| `DelayWriteToken` | `total_delayed_` | Rate-limited writes via token bucket |
| `CompactionPressureToken` | `total_compaction_pressure_` | Increases compaction parallelism |

**Token lifecycle:**

```cpp
// ColumnFamilyData detects L0 threshold exceeded
std::unique_ptr<WriteControllerToken> token =
    write_controller_.GetDelayToken(delayed_write_rate);

// Token held in ColumnFamilyData
// Compaction catches up, token destroyed (RAII)
~DelayWriteToken() { controller_->total_delayed_--; }
```

**Token bucket algorithm** (`WriteController::GetDelay`):

```cpp
uint64_t WriteController::GetDelay(SystemClock* clock, uint64_t num_bytes) {
  if (IsStopped()) return 0;  // Stop handled separately
  if (!NeedsDelay()) return 0;

  // Fast path: enough credit
  if (credit_in_bytes_ >= num_bytes) {
    credit_in_bytes_ -= num_bytes;
    return 0;
  }

  // Refill credit based on elapsed time
  uint64_t now = clock->NowMicrosMonotonic();
  if (now >= next_refill_time_) {
    uint64_t elapsed_us = now - next_refill_time_ + 1000;  // +1ms
    credit_in_bytes_ += elapsed_us * delayed_write_rate_ / 1000000;
    next_refill_time_ = now + 1000;
  }

  // Still not enough? Compute delay
  if (credit_in_bytes_ >= num_bytes) {
    credit_in_bytes_ -= num_bytes;
    return 0;
  }

  uint64_t needed = num_bytes - credit_in_bytes_;
  return std::max(1000ULL, needed * 1000000 / delayed_write_rate_);
}
```

**⚠️ INVARIANT:** `GetDelay()` is called under `db_mutex_`. The leader sleeps outside the mutex to avoid blocking followers.

### WriteBufferManager

**`WriteBufferManager`** (`include/rocksdb/write_buffer_manager.h`) tracks total memtable memory across all DB instances.

**Memory tracking:**

| Counter | Updated By | Meaning |
|---------|-----------|---------|
| `memory_used_` | `ReserveMem` / `FreeMem` | Total bytes (active + being-flushed) |
| `memory_active_` | `ReserveMem` / `ScheduleFreeMem` | Bytes in mutable memtables only |

**Flush trigger** (`ShouldFlush()`):

```cpp
bool WriteBufferManager::ShouldFlush() const {
  return (memory_active_ > buffer_size_ * 7 / 8) ||
         (memory_used_ >= buffer_size_ && memory_active_ >= buffer_size_ / 2);
}
```

**Stall trigger** (`ShouldStall()`):

```cpp
bool WriteBufferManager::ShouldStall() const {
  return enabled() && allow_stall_ && memory_used_ >= buffer_size_;
}
```

When stall triggers:

1. `PreprocessWrite()` calls `BeginWriteStall(&write_thread_)`.
2. Writers block on `stall_interface->Block()`.
3. `FreeMem()` drops `memory_used_` below threshold → `MaybeEndWriteStall()` signals blocked writers.

### Flow Control Data Flow

```
Compaction falls behind (L0 files ≥ threshold)
    │
    v
ColumnFamilyData::RecalculateWriteStallConditions()
    │  └─ WriteController::GetDelayToken() or GetStopToken()
    │
    v
PreprocessWrite() checks IsStopped() / NeedsDelay()
    │  ├─ Stopped? BeginWriteStall() + wait on bg_cv_
    │  └─ Delayed? GetDelay(num_bytes) → sleep outside mutex
    │
    v
Compaction catches up
    │  └─ Token destroyed (RAII) → total_delayed_-- → writers resume


MemTable memory exceeds buffer_size
    │
    v
WriteBufferManager::ShouldFlush() → HandleWriteBufferManagerFlush()
    │  └─ SwitchMemtable() on oldest CF
    │
    v
WriteBufferManager::ShouldStall() → BeginWriteStall()
    │  └─ Writers block
    │
    v
FlushJob completes → FreeMem() → MaybeEndWriteStall()
    │  └─ Writers resume
```

---

## 10. Crash Recovery

**Files:** `db/db_impl/db_impl_open.cc`, `db/wal_manager.h`

### Recovery Flow

```
DB::Open()
    │
    v
Read MANIFEST → recover VersionSet
    │  └─ WalSet (live WAL list from VersionEdit)
    │
    v
For each WAL in WalSet (sorted by log_number):
    │  1. log::Reader::ReadRecord() → reassemble WriteBatch
    │  2. Verify CRC, handle recyclable headers
    │  3. Decompress if needed
    │  4. WriteBatchInternal::InsertInto(batch, memtable)
    │      └─ Replay into fresh MemTable
    │
    v
Flush recovered memtables → SST files
    │
    v
Delete obsolete WALs (flushed to SST)
```

**⚠️ INVARIANT:** WAL order = sequence number order. Recovery replays WALs in `log_number` order, which matches write order, ensuring correct sequence assignment.

### WAL Truncation on Corruption

If memtable insertion fails during recovery, `SetAttemptTruncateSize()` records the pre-write WAL file size. On next recovery attempt, the WAL is truncated to this size, discarding the corrupted tail.

---

## Key Invariants Summary

| Invariant | Why It Matters | Enforced By |
|-----------|----------------|-------------|
| **WAL before MemTable** | Crash recovery can replay WAL to restore un-flushed writes | `WriteToWAL()` called before `WriteBatchInternal::InsertInto()` |
| **Sequence number monotonicity** | Snapshot isolation, merge ordering | `versions_->SetLastSequence()` increments atomically |
| **Flush FIFO ordering** | Prevents data loss (older data cannot overwrite newer) | `MemTableList::TryInstallMemtableFlushResults()` waits for oldest |
| **mutex_ before wal_write_mutex_** | Deadlock avoidance | Lock order documented in `DBImpl` |
| **LogAndApply serialized per CF** | MANIFEST consistency | `commit_in_progress_` gate in `MemTableList` |
| **pending_outputs_ tracks live files** | Prevents deletion of files in use by compaction | `RegisterCompaction()` / `UnregisterCompaction()` |
| **Sequence assignment after WAL** | WAL contains correct seqnos for recovery | Leader assigns sequences between WAL write and memtable insert |
| **Range tombstone truncation at file boundaries** | Tombstones don't leak beyond file scope | `TruncatedRangeDelIterator` |
| **Bottommost compaction for tombstone cleanup** | Space reclamation | `CompactionIterator` checks `bottommost_level_` |

---

## Performance Considerations

### Hot Path Optimizations

**WriteThread**:
- Lock-free enqueue (`LinkOne` CAS loop).
- Adaptive wait (spin → yield → block) minimizes latency.
- Group commit amortizes fsync cost.

**MemTable**:
- Lock-free skiplist (default `MemTableRep`).
- Concurrent inserts when `allow_concurrent_memtable_write` enabled.
- Bloom filter reduces memtable lookups on misses.

**WAL**:
- Pre-computed `type_crc_[kMaxRecordType+1]` avoids CRC recomputation.
- Optional compression (`kSnappyCompression`, `kZSTD`).
- Recyclable headers detect stale data in recycled files.

### Write Amplification

Write amplification = `(bytes written to storage) / (bytes written by app)`.

**Sources:**
1. WAL: 1.0x (every byte written once).
2. Flush: ~1.0x (memtable → L0 SST).
3. Compaction: `(L0→L1 amplification) * (leveled amplification)`.
   - L0→L1: `O(num_L0_files)` (all L0 files merge with L1).
   - L1→L2→...→Ln: `O(fanout)` per level.

**Total WA**: Typically **10-30x** for leveled compaction, **2-10x** for universal compaction.

**Mitigation:**
- `enable_pipelined_write`: Overlap WAL and memtable phases.
- `unordered_write`: Skip WAL ordering overhead (non-durable until flush).
- `level_compaction_dynamic_level_bytes`: Dynamically adjust level sizes.
- Larger memtable (`write_buffer_size`): Fewer L0 flushes.

---

## Cross-Component Interactions

| From | To | Interaction |
|------|-----|-------------|
| **WriteImpl** | **WriteThread** | Leader election, group batching |
| **WriteImpl** | **WAL** | Serialize WriteBatch via `log::Writer::AddRecord()` |
| **WriteImpl** | **MemTable** | Insert via `WriteBatchInternal::InsertInto()` |
| **WriteImpl** | **WriteController** | Check `IsStopped()` / `NeedsDelay()` in `PreprocessWrite()` |
| **WriteImpl** | **WriteBufferManager** | Check `ShouldFlush()` / `ShouldStall()` |
| **MemTable** | **FlushJob** | Flush immutable memtables to L0 SST |
| **FlushJob** | **VersionSet** | `LogAndApply(VersionEdit)` commits flush to MANIFEST |
| **FlushJob** | **WriteBufferManager** | `FreeMem()` releases memory, may end stall |
| **FlushJob** | **CompactionPicker** | New L0 file may trigger compaction |
| **CompactionJob** | **CompactionIterator** | Dedup, delete, merge logic |
| **CompactionJob** | **VersionSet** | `LogAndApply(VersionEdit)` commits compaction to MANIFEST |
| **RangeDelAggregator** | **DBIter** | `ShouldDelete()` hides keys covered by range tombstones |
| **RangeDelAggregator** | **CompactionIterator** | Drop keys covered by range tombstones |

---

## Code References

| Component | Key Files |
|-----------|-----------|
| **Write APIs** | `db/db_impl/db_impl_write.cc:23-163` |
| **WriteBatch** | `include/rocksdb/write_batch.h`, `db/write_batch.cc` |
| **WriteThread** | `db/write_thread.h`, `db/write_thread.cc` |
| **WriteImpl** | `db/db_impl/db_impl_write.cc:370-end` |
| **WAL format** | `db/log_format.h`, `db/log_writer.cc`, `db/log_reader.cc` |
| **MemTable** | `db/memtable.h`, `db/memtable.cc` |
| **MemTableList** | `db/memtable_list.h`, `db/memtable_list.cc` |
| **FlushJob** | `db/flush_job.h`, `db/flush_job.cc` |
| **WriteController** | `db/write_controller.h`, `db/write_controller.cc` |
| **WriteBufferManager** | `include/rocksdb/write_buffer_manager.h` |
| **CompactionPicker** | `db/compaction/compaction_picker.h` |
| **CompactionJob** | `db/compaction/compaction_job.h`, `db/compaction/compaction_job.cc` |
| **CompactionIterator** | `db/compaction/compaction_iterator.cc` |
| **RangeDelAggregator** | `db/range_del_aggregator.h`, `db/range_del_aggregator.cc` |
| **DBFormat** | `db/dbformat.h` (ValueType, InternalKey encoding) |

---

## Related Documentation

- **[ARCHITECTURE.md](../../ARCHITECTURE.md)**: High-level RocksDB architecture overview
- **[write_path.md](write_path.md)**: Write path components (WriteBatch, WriteThread, WAL, MemTable, WriteController) — superseded by this document for write flow
- **[version_management.md](version_management.md)**: VersionEdit, VersionSet, MANIFEST, LogAndApply
- **[flush_and_read_path.md](flush_and_read_path.md)**: FlushJob details and read path (Get, DBIter, RangeDelAggregator)
- **[compaction.md](compaction.md)**: Compaction strategies, CompactionPicker, CompactionJob, CompactionIterator
- **[sst_table_format.md](sst_table_format.md)**: SST file format, BlockBasedTable, filters, indexes
- **[db_impl.md](db_impl.md)**: DBImpl, DB open, background threads, error handling
