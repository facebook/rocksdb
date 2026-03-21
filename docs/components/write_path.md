# RocksDB Write Path

## Overview

The RocksDB write path transforms user write operations (Put, Delete, Merge,
etc.) into durable, queryable state. The path involves serializing operations
into a `WriteBatch`, coordinating concurrent writers via `WriteThread`,
persisting data to the Write-Ahead Log (WAL), and inserting into the active
`MemTable`. Back-pressure mechanisms (`WriteController`, `WriteBufferManager`)
throttle writes when compaction or memory usage falls behind.

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
MemTable insertion (sequential or parallel)
    |
    v
SetLastSequence (publish: readers can now see new data)
    |
    v
ExitAsBatchGroupLeader (wake followers with STATE_COMPLETED)
```

### Write Path Variants

| Mode | WAL | MemTable | Use Case |
|------|-----|----------|----------|
| Default group commit | Leader writes group | Sequential or parallel | Most workloads |
| Pipelined write | WAL group, then separate memtable group | Decoupled pipeline stages | High-throughput ordered writes |
| Unordered write | Serialized via `wal_write_mutex_` | Concurrent, unordered | WritePrepared transactions |
| Two write queues | Separate WAL-only queue | Main queue for memtable | 2PC prepare/commit |

---

## 1. WriteBatch

**Files:** `include/rocksdb/write_batch.h`, `db/write_batch_internal.h`,
`db/write_batch.cc`

### What It Does

WriteBatch is the unit of atomicity for writes. It serializes one or more
key-value operations into a contiguous binary buffer (`rep_`), which is
written atomically to the WAL and applied to memtables. All operations in a
single WriteBatch share a starting sequence number and are visible atomically.

### Wire Format

```
rep_ :=
    sequence : fixed64          // bytes [0,7]  — little-endian
    count    : fixed32          // bytes [8,11] — little-endian
    data     : record[count]    // bytes [12, ...)

record (default CF) :=
    kTypeValue              varstring(key) varstring(value)
    kTypeDeletion           varstring(key)
    kTypeSingleDeletion     varstring(key)
    kTypeRangeDeletion      varstring(begin_key) varstring(end_key)
    kTypeMerge              varstring(key) varstring(value)
    kTypeWideColumnEntity   varstring(key) varstring(serialized_entity)
    kTypeValuePreferredSeqno varstring(key) varstring(packed_value_+_write_time)
    kTypeLogData            varstring(blob)

record (non-default CF) :=
    kTypeColumnFamilyValue  varint32(cf_id) varstring(key) varstring(value)
    ...  (same pattern: CF-variant tag + varint32 CF ID + fields)

Transaction markers (WAL-only, do not increment count):
    kTypeNoop
    kTypeBeginPrepareXID / kTypeBeginPersistedPrepareXID / kTypeBeginUnprepareXID
    kTypeEndPrepareXID   varstring(xid)
    kTypeCommitXID       varstring(xid)
    kTypeRollbackXID     varstring(xid)

varstring := varint32(len) uint8[len]
```

The header is always exactly 12 bytes (`WriteBatchInternal::kHeader`).
`kTypeLogData` and transaction markers do not increment the count and do not
consume sequence numbers.

### Class Members

| Member | Type | Purpose |
|--------|------|---------|
| `rep_` | `std::string` | Serialized batch (header + records). Protected, not private. |
| `content_flags_` | `atomic<uint32_t>` | Lazy bitmask of op types present (HAS_PUT, HAS_DELETE, etc.) |
| `save_points_` | `unique_ptr<SavePoints>` | Stack of SavePoints, allocated on demand |
| `wal_term_point_` | `SavePoint` | Where WAL writing should stop (partial batch) |
| `prot_info_` | `unique_ptr<ProtectionInfo>` | Per-key 64-bit checksums. Null if disabled. |
| `max_bytes_` | `size_t` | Max allowed `rep_` size |
| `needs_in_place_update_ts_` | `bool` | True if timestamp placeholder must be updated before apply |
| `default_cf_ts_sz_` | `size_t` | Timestamp size for default CF |

### Operation Encoding Pattern

Every Put/Delete/Merge follows this invariant:

1. Create `LocalSavePoint` (RAII rollback on `max_bytes_` overflow).
2. Increment count: `SetCount(b, Count(b) + 1)`.
3. Append tag byte (default CF variant, or CF-specific variant + varint32 CF ID).
4. Append key as length-prefixed varstring.
5. Append value (if applicable) as length-prefixed varstring.
6. Atomically OR the corresponding ContentFlags bit.
7. If `prot_info_` is non-null, append a `ProtectionInfoKVOC64` entry covering
   key, value, op type, and CF ID.
8. `LocalSavePoint::commit()` checks `rep_.size() > max_bytes_` and reverts
   if exceeded.

### SavePoint Mechanics

`SetSavePoint()` pushes `{rep_.size(), Count(), content_flags_}` onto a stack.
`RollbackToSavePoint()` pops and restores: `rep_.resize(saved_size)`, reset
count, reset content_flags. Also resizes `prot_info_->entries_` to the saved
count. `PopSavePoint()` pops without rolling back (commits the span).

### Iteration (WriteBatch::Iterate)

The `Handler` visitor pattern drives iteration. `ReadRecordFromWriteBatch`
parses each record and dispatches to the appropriate handler method (PutCF,
DeleteCF, MergeCF, etc.). `handler->Continue()` is checked each iteration;
returning false halts without error. `Status::TryAgain` from a handler causes
the same record to be re-processed (used by concurrent memtable insertion
when a sequence conflict requires retry).

### MemTableInserter

The core `Handler` implementation that applies a WriteBatch to memtables:

| Field | Role |
|-------|------|
| `sequence_` | Current sequence number; incremented per key or per batch |
| `cf_mems_` | Maps CF ID to MemTable* |
| `flush_scheduler_` | Notified when memtable is full |
| `concurrent_memtable_writes_` | If true, calls Add() with concurrent flag |
| `seq_per_batch_` | If true (WritePrepared), advance seq only on batch boundaries |

**PutCFImpl hot path:**
1. `SeekToColumnFamily(cf_id)` to find the right MemTable.
2. `mem->Add(sequence_, value_type, key, value, ...)` (or `Update`/`UpdateCallback` for in-place updates).
3. `MaybeAdvanceSeq()` — increments sequence per key (WriteCommitted) or per batch boundary (WritePrepared).
4. `CheckMemtableFull()` — schedules flush if memtable should switch.

### Invariants

- `rep_.size() >= 12` always after construction.
- `Count()` at bytes [8,11] equals the number of sequence-consuming records.
- `prot_info_->entries_.size() == Count()` when protection is enabled.
- Iteration validates `found == Count()` at the end of a full-batch iteration.
- Timestamp placeholders (`needs_in_place_update_ts_`) must be resolved by
  `UpdateTimestamps()` before the batch is applied.

### Interactions

| Component | Interaction |
|-----------|------------|
| WAL | WriteBatch `rep_` is the WAL record payload |
| MemTable | `MemTableInserter` handler applies batch to memtables |
| WriteThread | Each Writer carries a WriteBatch pointer |
| Transactions | 2PC markers (Begin/End/Commit/Rollback) embedded in the batch |

---

## 2. WriteThread

**Files:** `db/write_thread.h`, `db/write_thread.cc`

### What It Does

WriteThread implements lock-free batch grouping and write pipelining. It
funnels concurrent writers through a single WAL write while maximizing
throughput via group commits, concurrent memtable writes, and optional
pipelining. Zero db-mutex usage is an explicit design goal for all methods
except `JoinBatchGroup` and `EnterUnbatched`.

### Data Structures

**Writer** (stack-allocated per write thread):

| Field | Type | Role |
|-------|------|------|
| `state` | `atomic<uint8_t>` | Current state machine position |
| `link_older` | `Writer*` | Points toward queue head (older writers) |
| `link_newer` | `Writer*` | Reverse link, lazily constructed by leader |
| `write_group` | `WriteGroup*` | Which group this writer belongs to |
| `sequence` | `SequenceNumber` | Assigned sequence number |
| `batch` | `WriteBatch*` | The batch to write (nullptr = EnterUnbatched) |
| `state_mutex_bytes` | aligned storage | Lazily constructed mutex for blocking |
| `state_cv_bytes` | aligned storage | Lazily constructed condvar for blocking |

**WriteGroup**: `leader`, `last_writer`, `last_sequence`, `status`,
`running` (atomic countdown latch for parallel writes), `size`.

### State Machine

```
STATE_INIT                       = 1   // waiting in JoinBatchGroup
STATE_GROUP_LEADER               = 2   // won leadership of write batch queue
STATE_MEMTABLE_WRITER_LEADER     = 4   // won leadership of memtable queue (pipelined)
STATE_PARALLEL_MEMTABLE_WRITER   = 8   // parallel memtable writer
STATE_COMPLETED                  = 16  // done, terminal
STATE_LOCKED_WAITING             = 32  // blocking on condvar
STATE_PARALLEL_MEMTABLE_CALLER   = 64  // helper that broadcasts to parallel writers
```

States are bit flags so `AwaitState` can wait for any subset via bitmask.

### Queue and Linking Protocol

The write queue is an intrusive singly-linked list:

```
newest_writer_ -> W4 -> W3 -> W2 -> W1 -> nullptr
                (newest)              (oldest = leader)
```

**LinkOne** (lock-free CAS enqueue): Sets `w->link_older = current_head`,
then CAS `newest_writer_` from `current_head` to `w`. Returns true if the
writer became the leader (queue was empty).

**CreateMissingNewerLinks**: Leader walks from newest toward oldest, setting
`link_newer` pointers. Only the leader traverses in this direction, so no
synchronization is needed.

### JoinBatchGroup

```
LinkOne(w, &newest_writer_)
  leader? -> SetState(w, STATE_GROUP_LEADER), return immediately
  follower? -> AwaitState(w, STATE_GROUP_LEADER | STATE_MEMTABLE_WRITER_LEADER
                           | STATE_PARALLEL_MEMTABLE_WRITER | STATE_COMPLETED)
```

Critical: if `LinkOne` returns true (leader), no waiting occurs at all.

### EnterAsBatchGroupLeader — Group Formation

The leader harvests followers, traversing oldest-to-newest via `link_newer`.
Each follower is included only if ALL conditions are met:

| Condition | Reason |
|-----------|--------|
| Matching `sync` / `no_slowdown` / `disable_wal` | Cannot mix incompatible options |
| Matching `protection_bytes_per_key` | Integrity level must match |
| `batch != nullptr` | nullptr = EnterUnbatched, must be alone |
| Total size <= `max_write_batch_group_size_bytes` | Group size cap (default 1MB) |
| No WBWI ingestion on either side | WBWI must be solo |

If the leader's batch is small (<= 1/8 of max), the max size is clamped to
`2 * (1/8 of max)` to prevent a tiny leader from accumulating excessive
latency.

Rejected writers are placed in a temporary r-list and grafted back after the
group's `last_writer`, preserving queue ordering for the next leader.

### LaunchParallelMemTableWriters

Sets `write_group.running = group_size` as a countdown latch.

- **Small groups (< 20):** Leader sets each writer to
  `STATE_PARALLEL_MEMTABLE_WRITER` sequentially. O(n) latency.
- **Large groups (>= 20):** Two-level broadcast using
  `STATE_PARALLEL_MEMTABLE_CALLER` helpers. Leader wakes sqrt(n) helpers,
  each wakes sqrt(n) writers. Max latency: O(2*sqrt(n)).

### CompleteParallelMemTableWriter

Atomically decrements `running`. The last writer to finish (decrement reaches 1)
returns true and takes responsibility for completing the group. Error status
is propagated to `write_group.status` under the leader's `StateMutex()`.

### ExitAsBatchGroupLeader

**Non-pipelined:** CAS `newest_writer_` from `last_writer` to nullptr (or wake
next leader if new writers arrived). Walk followers setting
`STATE_COMPLETED`. Leader must not complete itself until all followers are done
(leader owns the `WriteGroup`).

**Pipelined:** Inserts a dummy Writer to hold the queue position, links the
group into `newest_memtable_writer_` for the memtable pipeline stage, then
removes the dummy and wakes the next WAL leader. The dummy prevents the next
WAL leader from racing ahead in the memtable queue.

### AwaitState — Adaptive Spinning

Three-tier waiting:

| Tier | Mechanism | Duration |
|------|-----------|----------|
| 1 | `AsmVolatilePause` busy loop | ~1 us (200 iterations) |
| 2 | `std::this_thread::yield()` loop | Up to `max_yield_usec_` (default 100 us) |
| 3 | `BlockingAwaitState` — condvar | Unbounded |

Tier 2 uses adaptive `yield_credit` with exponential decay. If recent yields
succeeded, credit stays positive (keep spinning). If yields consistently fail
(CPU contention), credit goes negative and falls through to blocking.

### Write Stall Protocol

`BeginWriteStall()` pushes `write_stall_dummy_` sentinel onto the queue.
Writers with `no_slowdown=true` that find the sentinel are immediately
completed with `Incomplete` status. Writers with `no_slowdown=false` block on
`stall_cv_`. `EndWriteStall()` removes the sentinel and signals all blocked
writers.

### Invariants

- Only the leader modifies the queue (removes writers, creates `link_newer`).
- Parallel error aggregation is protected by the leader's `StateMutex()`.
- The leader completes last (it owns the `WriteGroup` referenced by followers).
- Pipelined queue ordering is enforced by the dummy Writer during group transfer.
- `STATE_LOCKED_WAITING` CAS in `SetState` forces the mutex path when the waiter
  is blocking on a condvar.

---

## 3. WAL (Write-Ahead Log)

**Files:** `db/log_format.h`, `db/log_writer.h`, `db/log_reader.h`,
`db/wal_manager.h`, `db/wal_edit.h`

### What It Does

The WAL provides crash recovery by durably logging WriteBatch payloads before
they are applied to memtables. It uses a block-structured physical format with
CRC32c checksums, supports record fragmentation across blocks, streaming
compression, log recycling, and predecessor-chain verification.

### Physical Record Format

Two wire formats, selected at Writer construction based on `recycle_log_files`:

| Format | Layout | Header Size |
|--------|--------|-------------|
| Legacy | CRC(4) Length(2) Type(1) Payload | 7 bytes |
| Recyclable | CRC(4) Length(2) Type(1) LogNum(4) Payload | 11 bytes |

Block size is fixed at `kBlockSize = 32768` bytes. Records never leave a
partial header at a block boundary; if remaining space < header size, the
writer zero-pads the trailer and advances to the next block.

### Record Types

| Type | Value | Purpose |
|------|-------|---------|
| `kZeroType` | 0 | Reserved for preallocated zero regions; silently skipped |
| `kFullType` / `kRecyclableFullType` | 1 / 5 | Entire record in one fragment |
| `kFirstType` / `kRecyclableFirstType` | 2 / 6 | First fragment of multi-block record |
| `kMiddleType` / `kRecyclableMiddleType` | 3 / 7 | Interior fragment |
| `kLastType` / `kRecyclableLastType` | 4 / 8 | Final fragment |
| `kSetCompressionType` | 9 | First record only; declares streaming compression |
| `kUserDefinedTimestampSizeType` | 10 / 11 | Per-CF UDT size for subsequent records |
| `kPredecessorWALInfoType` | 130 / 131 | Chain integrity (predecessor log#, seqno, size) |

Unknown types with bit 7 set (`kRecordTypeSafeIgnoreMask = 1 << 7`) are
silently ignored for forward compatibility.

### Checksum Mechanics

**Writer:** CRC32c is seeded from a pre-computed `type_crc_[t]` table
(eliminates per-write overhead on the type byte). For recyclable records, CRC
is extended over the 4-byte log number. Payload CRC is computed separately
and combined via `crc32c::Crc32cCombine`. Final CRC is masked before storage.

**Reader:** Verifies `crc32c::Value(header+6, length + header_size - 6)` in
one pass over type byte, optional log number, and payload. On mismatch, the
entire remaining buffer is dropped.

### Record Fragmentation (Writer)

```
AddRecord():
  while data remaining:
    leftover = kBlockSize - block_offset_
    if leftover < header_size_: zero-pad trailer, advance to next block
    avail = kBlockSize - block_offset_ - header_size_
    fragment_length = min(remaining, avail)
    type = begin && end ? Full : begin ? First : end ? Last : Middle
    EmitPhysicalRecord(type, ptr, fragment_length)
```

With `manual_flush_ = false`, every `AddRecord()` flushes. With
`manual_flush_ = true`, the caller batches flushes via `WriteBuffer()`.

### WAL Recycling

When enabled, recyclable record types embed the 4-byte log number in every
header. On recovery, the reader compares the embedded log number against the
expected log number. Mismatches return `kOldRecord` (stale data from the
previous file occupant), which terminates replay.

### WAL Compression

`kSetCompressionType` must be the first record in the file. Reader enforces
this. All subsequent records are streaming-compressed/decompressed. The
reader maintains a separate XXH3-64 hash state to verify decompressed logical
record integrity.

### WAL Lifecycle (WalManager)

| Method | Purpose |
|--------|---------|
| `GetSortedWalFiles()` | Enumerate live and archived WALs |
| `PurgeObsoleteWALFiles()` | Delete/archive WALs beyond TTL/size limits |
| `GetUpdatesSince()` | TransactionLogIterator entry point |

Uses `read_first_record_cache_` to avoid re-reading WAL headers.

### MANIFEST Tracking (WalEdit)

**WalAddition:** Records a WAL becoming known. Carries `WalNumber` and
`WalMetadata` (synced_size_bytes, kUnknownWalSize until synced).

**WalDeletion:** Records that all WALs with number < N are obsolete. A
"delete before" marker, not per-file.

**WalSet** (in-memory, held by VersionSet): `std::map<WalNumber, WalMetadata>`
ordered by log number. `AddWal()` validates the WAL state machine.
`CheckWals()` cross-references against on-disk files to detect data loss.

### Predecessor Chain Verification

When `track_and_verify_wals` is enabled, each new WAL writes a
`kPredecessorWALInfoType` record containing the previous WAL's log number,
last sequence number, and file size. The reader verifies all three fields
against what was observed reading the previous WAL.

### Interactions

| Component | Interaction |
|-----------|------------|
| WriteBatch | WAL record payload is `WriteBatch::rep_` |
| WriteThread | Leader calls `WriteToWAL()` for the merged group |
| VersionSet | WalAddition/WalDeletion tracked in MANIFEST |
| Recovery | Reader replays WAL records through MemTableInserter |

---

## 4. MemTable

**Files:** `db/memtable.h`, `db/memtable.cc`, `include/rocksdb/memtablerep.h`

### What It Does

MemTable is an in-memory sorted data structure that receives writes after WAL
persistence. It holds key-value pairs indexed by internal key (user key +
sequence number + type). Once full, it becomes immutable and is flushed to an
SST file. Supports concurrent reads and (optionally) concurrent writes.

### Class Hierarchy

```
ReadOnlyMemTable      (abstract, reference-counted)
    +-- MemTable      (final, adds Add/Update/UpdateCallback)

MemTableRep           (abstract backing store)
    +-- SkipListRep   (default, concurrent-safe)
    +-- VectorRep     (write-optimized, sort on iteration)
    +-- HashSkipListRep (prefix-bucketed skip lists)
    +-- HashLinkListRep (prefix-bucketed linked lists)
```

### Internal Storage

| Field | Type | Purpose |
|-------|------|---------|
| `table_` | `unique_ptr<MemTableRep>` | Point key entries |
| `range_del_table_` | `unique_ptr<MemTableRep>` | Range tombstones (always SkipListRep) |
| `arena_` | `ConcurrentArena` | Shared allocator for both tables |
| `bloom_filter_` | `unique_ptr<DynamicBloom>` | Optional prefix/whole-key bloom |

### Entry Encoding

Each entry is a single flat buffer allocated from the arena:

```
[varint32: internal_key_size]     // key.size() + 8
[char[key_size]: user_key]
[uint64_t: packed(seq, type)]     // top 56 bits = seq, low 8 bits = ValueType
[varint32: value_size]
[char[value_size]: value]
[char[N]: checksum]               // only if protection_bytes_per_key > 0
```

Internal key ordering: user key ascending, sequence number descending
(newest first).

### Add() Method — Step by Step

1. **Route to correct table:** Range tombstones go to `range_del_table_`,
   everything else to `table_`.
2. **Allocate and encode:** Allocates via `table->Allocate()` and encodes
   the entry in-place.
3. **Checksum:** If `protection_bytes_per_key > 0`, writes a 64-bit checksum
   at the tail.
4. **Insert:**
   - Non-concurrent: `InsertKey(handle)` or `InsertKeyWithHint(handle, &hint)`.
   - Concurrent: `InsertKeyConcurrently(handle)` or
     `InsertKeyWithHintConcurrently(handle, hint)`.
5. **Update counters:**
   - Non-concurrent: `StoreRelaxed` (no locked instructions).
   - Concurrent: Accumulate in `MemTablePostProcessInfo`, flush atomically
     in `BatchPostProcess()`.
6. **Bloom filter:** Add prefix and/or whole-key to `DynamicBloom`.
7. **Sequence tracking:** Update `first_seqno_` and `earliest_seqno_`.
8. **Range tombstone cache invalidation:** Any `kTypeRangeDeletion` invalidates
   the per-core `FragmentedRangeTombstoneListCache`.
9. **Flush state check:** `UpdateFlushState()` calls `ShouldFlushNow()` and
   CAS-transitions `flush_state_` from `FLUSH_NOT_REQUESTED` to
   `FLUSH_REQUESTED`.

### Concurrent Insert Support

`SkipListFactory::IsInsertConcurrentlySupported()` returns true. The skip list
uses lock-free CAS-based insertion. Counters are deferred to
`BatchPostProcess()` to avoid atomic increments on every key. Bloom updates
use `DynamicBloom::AddConcurrently` (atomic fetch-or).

`Status::TryAgain` from `Add()` signals a duplicate `<key, seq>` to the
write path, which retries with an incremented sequence number.

### Bloom Filter

- Type: `DynamicBloom` with 6 probes.
- Size: `write_buffer_size * memtable_prefix_bloom_size_ratio * 8` bits.
- Two modes: prefix bloom (`prefix_extractor_->Transform(key)`) and whole-key
  bloom. Both can be active simultaneously.
- MultiGet: batch-checks all keys via `bloom_filter_->MayContain(num_keys, ...)`.
- Keys are stripped of user-defined timestamps before hashing.

### Range Tombstones

Stored in `range_del_table_` (always a SkipListRep). Uses per-core cached
`FragmentedRangeTombstoneListCache` for mutable memtable reads, invalidated on
every new range tombstone insertion. Immutable memtable uses a one-time
constructed `fragmented_range_tombstone_list_`.

### Flush Scheduling State Machine

```
FLUSH_NOT_REQUESTED  --(ShouldFlushNow())--> FLUSH_REQUESTED
FLUSH_REQUESTED      --(MarkFlushScheduled() CAS)--> FLUSH_SCHEDULED
```

`ShouldFlushNow()` triggers when:
- `IsMarkedForFlush()` (external force-flush).
- `num_range_deletes_ >= memtable_max_range_deletions_`.
- Arena allocated memory exceeds `write_buffer_size + kArenaBlockSize * 0.6`.
- Last arena block is 75% full.

### Memory Tracking

`AllocTracker mem_tracker_` ties arena allocations to `WriteBufferManager`.
`ApproximateMemoryUsage()` sums arena, table rep, range_del_table, and hint
map usage. `MarkImmutable()` calls `mem_tracker_.DoneAllocating()` to notify
`WriteBufferManager`.

### MemTableRep Interface

Key properties:
- No duplicate items.
- Uses `KeyComparator` for ordering.
- Concurrent read-safe; write concurrency is implementation-defined.
- Items are never deleted.

Insert method matrix:

| Method | Concurrent | Hint | Returns duplicate? |
|--------|-----------|------|--------------------|
| `InsertKey` | No | No | Yes |
| `InsertKeyWithHint` | No | Yes | Yes |
| `InsertKeyConcurrently` | Yes | No | Yes |
| `InsertKeyWithHintConcurrently` | Yes | Yes | Yes |

`bool` return = false means duplicate `<key, seq>` detected (only when
`CanHandleDuplicatedKey()` returns true). SkipListFactory is the only
built-in factory where this returns true.

### Interactions

| Component | Interaction |
|-----------|------------|
| WriteBatch | `MemTableInserter` handler calls `Add()` per key |
| MemTableList | Receives immutable MemTable via `Add()` |
| WriteBufferManager | Memory accounting via `AllocTracker` |
| FlushJob | Reads memtable entries to produce SST |
| SuperVersion | References current MemTable for reads |

---

## 5. MemTableList

**Files:** `db/memtable_list.h`, `db/memtable_list.cc`

### What It Does

MemTableList manages the ordered set of immutable memtables for a single
column family. It coordinates flush lifecycle (pick, execute, commit), version
management via copy-on-write `MemTableListVersion`, and history trimming for
transaction conflict checking.

### Class Structure

| Class | Role | Thread Safety |
|-------|------|---------------|
| `MemTableList` | Owner/coordinator; flush state machine | Requires DB mutex mostly; `imm_flush_needed` and `imm_trim_needed` are atomic |
| `MemTableListVersion` | Immutable snapshot; refcounted; two lists | Not thread-safe; external sync required |

### MemTableListVersion Layout

```
memlist_          front -> [newest imm] -> ... -> [oldest imm] -> back
memlist_history_  front -> [newest flushed] -> ... -> [oldest flushed] -> back
```

- `memlist_`: Unflushed immutable memtables. `back()` is flushed first (FIFO).
- `memlist_history_`: Already-flushed memtables retained for transaction
  conflict checking (controlled by `max_write_buffer_size_to_maintain`).

Ordering invariant: IDs are monotonically increasing from back to front
(newest at front).

### Copy-on-Write Protocol (InstallNewVersion)

```
if current_->refs_ == 1:
    mutate in place (sole owner)
else:
    clone the version, bump ID, Ref new, Unref old
```

Called before every mutation: Add, TrimHistory, RemoveMemTablesOrRestoreFlags.
The old version survives as long as a SuperVersion holds a reference.

### Add Path (mutable to immutable transition)

```
MemTableList::Add(m):
    InstallNewVersion()
    current_->AddMemTable(m)          // push_front, update memory_usage
    current_->TrimHistory(to_delete)  // trim if over budget
    m->MarkImmutable()
    num_flush_not_started_++
    if num_flush_not_started_ == 1:
        imm_flush_needed.store(true)
```

### Flush Lifecycle

**PickMemtablesToFlush:** Iterates `memlist_` from oldest to newest, marking
each as `flush_in_progress_`, decrementing `num_flush_not_started_`. Stops at
`max_memtable_id`. Prevents picking non-consecutive memtables (required for
correct FIFO MANIFEST ordering).

**TryInstallMemtableFlushResults:** The serialization protocol for committing
flush results to MANIFEST:

1. Mark completion: `flush_completed_ = true`, `file_number_ = N`.
2. Single-committer election via `commit_in_progress_` bool.
3. Batch scan: collect all contiguous `flush_completed_` memtables from the
   oldest end.
4. Compute WAL GC edit (new `min_log_number_to_keep`).
5. `vset->LogAndApply()` — write edits to MANIFEST (releases/reacquires mutex).
6. On success: `Remove()` each flushed memtable (moves to history or unrefs).
   On failure: reset all flags, re-arm `imm_flush_needed`.
7. Retry loop: re-check if another thread completed a flush while this thread
   held `commit_in_progress_`.

**FIFO ordering invariant:** No younger memtable can be committed until all
older ones are committed, regardless of which finished flushing first.

### TrimHistory

Budget: `MemoryAllocatedBytesExcludingLast() + usage >= max_write_buffer_size_to_maintain_`.
Trims from the oldest end of `memlist_history_` until under budget.

### Atomic Flush (InstallMemtableAtomicFlushResults)

For multi-CF atomic flush: all CFs must be committed atomically to MANIFEST.
Marks all edits with `MarkAtomicGroup` so the MANIFEST reader treats them as
a single atomic record. Uses `vset->LogAndApply(cfds, ...)` (multi-CF overload).

### Interactions

| Component | Interaction |
|-----------|------------|
| SuperVersion | Calls `current_->Ref()` on construction, `Unref()` on cleanup |
| VersionSet | `TryInstallMemtableFlushResults` calls `LogAndApply` |
| FlushJob | Calls `PickMemtablesToFlush`, then `TryInstallMemtableFlushResults` |
| DBImpl | Calls `Add()` during memtable switch; monitors `imm_flush_needed` |

---

## 6. WriteBufferManager

**File:** `include/rocksdb/write_buffer_manager.h`

### What It Does

WriteBufferManager tracks and limits total memory used by memtables across
all column families and (optionally) across multiple DB instances. It provides
flush triggers (soft limit) and write stall (hard limit) to prevent unbounded
memory growth.

### Memory Tracking

| Atomic | Meaning |
|--------|---------|
| `memory_used_` | Total bytes charged (active + immutable memtables) |
| `memory_active_` | Active (mutable) memtable bytes only |
| `buffer_size_` | User-configured hard limit. `enabled()` iff > 0 |
| `mutable_limit_` | `buffer_size_ * 7/8` (soft threshold for mutable pressure) |

### Two-Phase Accounting

```
ReserveMem(mem):   memory_used_ += mem;  memory_active_ += mem
ScheduleFreeMem(mem):                    memory_active_ -= mem   // going immutable
FreeMem(mem):      memory_used_ -= mem;  MaybeEndWriteStall()    // actually freed
```

`ScheduleFreeMem` is called when a memtable becomes immutable (bytes still
resident but no longer growing). `FreeMem` is called after flush completes.
This two-phase approach lets `ShouldFlush` distinguish between "memory is high
but being drained" vs "memory is high and nothing is draining."

### ShouldFlush Logic

```
ShouldFlush():
  if memory_active_ > mutable_limit_:           return true   // mutable pressure
  if memory_used_ >= buffer_size_
     && memory_active_ >= buffer_size_ / 2:      return true   // global pressure
  return false
```

Condition 2 suppresses flush when `memory_active_ < buffer_size_ / 2` (more
than half is already queued for flush — triggering more flushes won't help).

### Stall Mechanism

`ShouldStall()` returns true when `memory_used_ >= buffer_size_` and
`allow_stall_` is enabled. Uses a latching `stall_active_` flag: once set by
`BeginWriteStall`, `ShouldStall()` returns true even if `memory_used_`
momentarily drops below `buffer_size_`. Only cleared by `MaybeEndWriteStall()`.

`BeginWriteStall(StallInterface*)` registers the caller in a queue.
`MaybeEndWriteStall()` (called from `FreeMem()`) signals all queued
`StallInterface*` when the stall condition clears.

### Cache Integration (cost_to_cache)

When a `Cache` is provided, `CacheReservationManagerImpl` inserts dummy cache
entries to make memtable memory compete with block cache for the same capacity
pool. This causes block cache eviction pressure proportional to memtable usage.
Protected by `cache_res_mgr_mu_` (potential bottleneck with many concurrent
writers).

### Interactions

| Component | Interaction |
|-----------|------------|
| MemTable | `AllocTracker` calls `ReserveMem`/`ScheduleFreeMem`/`FreeMem` |
| DBImpl | Checks `ShouldFlush()` in `PreprocessWrite()`; blocks on `ShouldStall()` |
| Block Cache | Dummy entries compete for cache capacity when `cost_to_cache` enabled |

---

## 7. WriteController

**Files:** `db/write_controller.h`, `db/write_controller.cc`

### What It Does

WriteController provides per-DB write rate control with three states: normal,
delayed (token-bucket rate limiting), and stopped (complete write halt). It
is driven by compaction back-pressure (L0 file count, pending compaction bytes).

### State Model

Three independent counter-based states, each managed by RAII tokens:

| Counter | Token | Meaning |
|---------|-------|---------|
| `total_stopped_` | `StopWriteToken` | Complete write halt |
| `total_delayed_` | `DelayWriteToken` | Rate-limited writes via token bucket |
| `total_compaction_pressure_` | `CompactionPressureToken` | Signal for compaction speedup |

Multiple CFs can each hold tokens. The counts are additive.

- `IsStopped()`: `total_stopped_ > 0`
- `NeedsDelay()`: `total_delayed_ > 0`
- `NeedSpeedupCompaction()`: any counter > 0

### GetDelay — Token Bucket Rate Limiting

Called per write batch with `num_bytes`. Returns microseconds to sleep.

```
if credit_in_bytes_ >= num_bytes:
    credit -= num_bytes; return 0               // fast path, no clock call

time_now = NowMicrosMonotonic()
if next_refill_time_ <= time_now:               // refill bucket
    credit += elapsed * delayed_write_rate_ / 1s
    next_refill_time_ = time_now + 1ms

if credit >= num_bytes: consume; return 0       // refill covered it

bytes_over_budget = num_bytes - credit
sleep = bytes_over_budget / delayed_write_rate_ * 1s
next_refill_time_ += sleep                      // track debt in time
return max(next_refill_time_ - time_now, 1ms)   // minimum 1ms delay
```

Design points:
- Refill granularity is 1ms (`kMicrosPerRefill = 1000`).
- Fast path never calls the clock (common case: credit available).
- Token bucket tracks debt in time rather than bytes.
- Minimum 1ms return to amortize mutex release/reacquire overhead.

### Interactions

| Component | Interaction |
|-----------|------------|
| DBImpl | `DelayWrite()` calls `GetDelay()` and sleeps; checks `IsStopped()` |
| WriteThread | `BeginWriteStall()`/`EndWriteStall()` manage the stall barrier |
| Compaction | Creates/destroys Stop/Delay/CompactionPressure tokens based on L0 count and compaction debt |

---

## 8. DBImpl Write Orchestration

**File:** `db/db_impl/db_impl_write.cc`

### What It Does

`DBImpl::WriteImpl()` is the central orchestrator of the write path. It
validates inputs, dispatches to the appropriate write mode, coordinates all
components (WriteBatch, WriteThread, WAL, MemTable, WriteController,
WriteBufferManager), and manages error escalation.

### Entry Point Chain

```
DB::Put(key, value)
  -> WriteBatch.Put(key, value)     // serialize to binary
  -> DBImpl::Write(write_options, &batch)
     -> UpdateProtectionInfo()      // optional per-key checksums
     -> DBImpl::WriteImpl()         // the real entry point
```

### Input Validation

WriteImpl rejects invalid combinations before any work:

| Condition | Result |
|-----------|--------|
| `sync=true` + `disableWAL=true` | InvalidArgument |
| `two_write_queues_` + `enable_pipelined_write` | NotSupported |
| `HasDeleteRange()` + `row_cache` | NotSupported |
| WBWI ingestion + `unordered_write` | NotSupported |

### Write Mode Dispatch

```
two_write_queues_ && disable_memtable  ->  WriteImplWALOnly(nonmem_write_thread_)
unordered_write                        ->  WriteImplWALOnly(write_thread_) + UnorderedWriteMemtable()
enable_pipelined_write                 ->  PipelinedWriteImpl()
default                                ->  group commit (inline)
```

### PreprocessWrite — The Gatekeeping Phase

Called by the group leader before forming a batch group. Checks proceed in
strict priority order under `mutex_`:

1. **DB stopped check:** Return immediately with background error.
2. **WAL size limit:** `wals_total_size_ > GetMaxTotalWalSize()` triggers
   `SwitchWAL()`, which flushes CFs whose data is in the oldest alive WAL.
3. **WriteBufferManager flush:** `ShouldFlush()` triggers
   `HandleWriteBufferManagerFlush()` — picks the CF with the oldest memtable
   and switches it.
4. **Trim history:** Drains `trim_history_scheduler_`.
5. **Flush scheduler:** Per-CF memtable full; calls `SwitchMemtable()`.
6. **WriteController delay/stop:** `DelayWrite()` — sleeps or waits.
7. **WriteBufferManager stall:** `WriteBufferManagerStallWrites()` — blocks
   until global memory drops.

### SwitchMemtable

The most complex single function. Atomically transitions a CF from its active
memtable to a new one, optionally creating a new WAL:

**Phase 1 (mutex held):** Flush recoverable state, decide on new WAL, allocate
file numbers, snapshot CF options.

**Phase 2 (mutex released):** Create new WAL file, construct new MemTable,
fragment old memtable's range tombstones.

**Phase 3 (mutex reacquired):** Flush old WAL write buffer, push new WAL onto
`logs_`, call `cfd->imm()->Add(old_mem)`, call `cfd->SetMemtable(new_mem)`,
install new SuperVersion.

### Default Group Commit Protocol

**Step 1 — JoinBatchGroup:** Writer enqueues into `newest_writer_` linked list.
Spins/waits until it becomes leader or is completed by the leader.

**Step 2 — Parallel memtable writers (non-leaders):** If woken as
`STATE_PARALLEL_MEMTABLE_WRITER`, directly call `InsertInto()` with
`concurrent=true`, then `CompleteParallelMemTableWriter()`.

**Step 3 — Leader proceeds:**
1. `PreprocessWrite()` — all stall/flush logic.
2. `EnterAsBatchGroupLeader()` — absorb compatible pending writers.
3. `CheckCallback()` per writer — optimistic transaction conflict checking.
4. Compute `seq_inc` (number of sequence numbers to consume).
5. `WriteGroupToWAL()` — merge batches, write to WAL.
6. Optional WAL sync.
7. `PreReleaseCallback()` per writer — 2PC sequence registration.
8. `InsertInto()` — sequential or parallel memtable insertion.
9. `SetLastSequence()` — publish to readers.
10. `ExitAsBatchGroupLeader()` — wake followers with `STATE_COMPLETED`.

### MergeBatch

For single-writer groups: uses the batch directly (no copy). For multi-writer
groups: appends all batches into `tmp_batch_` using WAL-only append. The
merged batch gets a single starting sequence number.

### WriteToWAL

1. Verify batch checksum if protection bytes are set.
2. Optionally write UDT size record.
3. `log_writer->AddRecord(log_entry, sequence)`.
4. Update `wals_total_size_`.

### Pipelined Write (PipelinedWriteImpl)

Decouples WAL from memtable using two pipeline stages:

**WAL pass:** Leader forms `wal_write_group`, assigns sequences, writes WAL,
exits. Followers transition to memtable states.

**Memtable pass:** `STATE_MEMTABLE_WRITER_LEADER` forms a new
`memtable_write_group` from WAL-completed writers. Writes sequentially or
launches parallel writers. While the memtable leader writes, new WAL writers
can proceed in parallel.

### Error Handling

| Function | Trigger | Severity |
|----------|---------|----------|
| `WALIOStatusCheck()` | WAL I/O error | Sets bg_error; fatal if IO-fenced |
| `WriteStatusCheck()` | Pre-release callback failure | Sets bg_error if paranoid_checks |
| `HandleMemTableInsertFailure()` | Memtable insert failed after WAL | Always fatal (WAL/memtable divergence) |

On memtable failure, `SetAttemptTruncateSize()` attempts WAL truncation back
to before the failed write.

### Sequence Number Management

| Mode | Allocation | Publication |
|------|-----------|-------------|
| Single queue | `LastSequence()` read before WAL write | `SetLastSequence()` after memtable write |
| Dual queue | `FetchAddLastAllocatedSequence()` under `wal_write_mutex_` | `SetLastPublishedSequence()` after memtable write |

The gap between allocation and publication is the snapshot window used by
WritePrepared transactions.

### Key Architectural Properties

- `wal_write_mutex_` protects only WAL writer state; `mutex_` protects full
  DB state. This allows WAL writes without blocking flush/compaction.
- `last_batch_group_size_` from the previous group is used for the current
  delay calculation — introduces slight fairness skew.
- Parallel memtable writes require: `allow_concurrent_memtable_write` &&
  group size > 1 && no Merge operations.

---

## Data Flow Summary

### Complete Write Path: Put() to Durable State

```
1. User calls DB::Put(key, value)
       |
2. WriteBatch serializes key/value into binary rep_
       |
3. DBImpl::WriteImpl() validates inputs, selects write mode
       |
4. WriteThread::JoinBatchGroup() — CAS-enqueue into lock-free linked list
       |
       +--- Follower path: wait for leader, then either:
       |    a) Do nothing (leader wrote everything) -> STATE_COMPLETED
       |    b) Write own batch to memtable concurrently -> STATE_PARALLEL_MEMTABLE_WRITER
       |
       +--- Leader path:
            |
5. PreprocessWrite() under mutex_:
   - Check DB health, WAL size, WriteBufferManager pressure
   - SwitchMemtable() if any CF memtable is full
   - DelayWrite() / WriteBufferManagerStallWrites() if back-pressure
            |
6. EnterAsBatchGroupLeader() — absorb compatible pending writers
            |
7. WriteGroupToWAL():
   - MergeBatch() — combine group into single WAL record
   - log::Writer::AddRecord() — fragment, CRC, write to WAL file
   - Optional fsync for sync=true writes
            |
8. MemTable insertion:
   - Sequential: leader iterates all batches via MemTableInserter
   - Parallel: LaunchParallelMemTableWriters(), each thread calls
     MemTable::Add() concurrently via InsertKeyConcurrently()
            |
9. versions_->SetLastSequence() — readers can now see new data
            |
10. ExitAsBatchGroupLeader() — wake all followers with STATE_COMPLETED
```

### Back-Pressure Flow

```
Compaction falling behind
    |
    v
WriteController creates StopWriteToken or DelayWriteToken
    |
    v
PreprocessWrite() detects IsStopped() or NeedsDelay()
    |
    v
DelayWrite(): BeginWriteStall() + sleep(GetDelay()) or wait(bg_cv_)
    |
    v
Compaction completes, releases tokens
    |
    v
EndWriteStall() + bg_cv_.Signal() -> writers resume

Global memory over limit (WriteBufferManager)
    |
    v
ShouldFlush() -> HandleWriteBufferManagerFlush() -> SwitchMemtable()
    |
    v  (if still over limit)
ShouldStall() -> WriteBufferManagerStallWrites() -> Block()
    |
    v
FreeMem() after flush -> MaybeEndWriteStall() -> Signal() -> writers resume
```

### MemTable Lifecycle

```
MemTable created (mutable)
    |  <-- writes go here via MemTable::Add()
    |  <-- WriteBufferManager::ReserveMem() tracks memory
    v
ShouldFlushNow() returns true (arena full)
    |
    v
SwitchMemtable(): old memtable -> MemTableList::Add() (becomes immutable)
    |  <-- WriteBufferManager::ScheduleFreeMem() (active -= size)
    |  <-- MarkImmutable() + ConstructFragmentedRangeTombstones()
    v
FlushJob: PickMemtablesToFlush() -> write SST file
    |
    v
TryInstallMemtableFlushResults():
    |  <-- LogAndApply() writes VersionEdit to MANIFEST
    |  <-- Remove() moves to memlist_history_ (or unrefs)
    |  <-- WriteBufferManager::FreeMem() (used -= size)
    v
History trimming (if max_write_buffer_size_to_maintain > 0)
    |
    v
Final Unref() -> delete MemTable
```
