# RocksDB Write Path

## Overview

The RocksDB write path transforms user write operations (Put, Delete, Merge, etc.) into durable, queryable state. The path involves serializing operations into a `WriteBatch`, coordinating concurrent writers via `WriteThread`, persisting data to the Write-Ahead Log (WAL), and inserting into the active `MemTable`. Back-pressure mechanisms (`WriteController`, `WriteBufferManager`) throttle writes when compaction or memory usage falls behind.

### Write Path Flow

```
User API (Put/Delete/Merge/DeleteRange/PutEntity/TimedPut)
    |
    v
WriteBatch  ---- serialize operations into binary rep_
    |
    v
DBImpl::WriteImpl  ---- main orchestrator
    |
    +---> WriteThread::JoinBatchGroup  ---- leader election
    |         |
    |         +---> Leader elected (one thread drives the group)
    |         +---> Followers wait or do parallel memtable writes
    |
    +---> PreprocessWrite  ---- stall/delay checks, flush scheduling
    |         |
    |         +---> WriteController: rate limit or stop writes
    |         +---> WriteBufferManager: stall if memory exceeded
    |         +---> SwitchMemtable/SwitchWAL if needed
    |
    +---> EnterAsBatchGroupLeader  ---- collect compatible writers
    |
    +---> WriteToWAL  ---- serialize merged batch to log::Writer
    |         |
    |         +---> log::Writer::AddRecord  ---- block-framed, CRC'd
    |         +---> Optional fsync
    |
    +---> Assign sequence numbers to each writer
    |
    +---> MemTable insertion
    |         |
    |         +---> Serial: leader inserts all batches
    |         +---> Parallel: LaunchParallelMemTableWriters
    |
    +---> SetLastSequence  ---- publish to readers
    |
    +---> ExitAsBatchGroupLeader  ---- wake followers
```

### Write Modes

| Mode | Config | WAL Queue | MemTable Queue | Key Property |
|------|--------|-----------|----------------|--------------|
| Normal (batched) | default | `write_thread_` | same thread | Leader does WAL + memtable for group |
| Pipelined | `enable_pipelined_write` | `write_thread_` | `newest_memtable_writer_` | WAL and memtable overlap across groups |
| Two-queue | `two_write_queues` | `nonmem_write_thread_` (WAL-only) + `write_thread_` | `write_thread_` | WAL writes decoupled from memtable |
| Unordered | `unordered_write` | `write_thread_` | per-writer (concurrent) | No ordering between writers |

---

## 1. WriteBatch

**Files:** `include/rocksdb/write_batch.h`, `db/write_batch_internal.h`, `db/write_batch.cc`

### What It Does

`WriteBatch` is a container that serializes a sequence of write operations into a compact binary format (`rep_` string). It is the unit of atomicity: all operations in a batch are applied together or not at all. A single `WriteBatch` is written as a single WAL record.

### Binary Encoding Format

```
rep_ :=
    sequence: fixed64          // 8 bytes, little-endian sequence number
    count:    fixed32          // 4 bytes, little-endian entry count
    data:     record[count]    // variable-length records

Header size: WriteBatchInternal::kHeader = 12 bytes
```

Each record is encoded as:

```
record :=
    kTypeValue                            varstring varstring
    kTypeDeletion                         varstring
    kTypeSingleDeletion                   varstring
    kTypeRangeDeletion                    varstring varstring   // begin_key, end_key
    kTypeMerge                            varstring varstring
    kTypeColumnFamilyValue                varint32 varstring varstring
    kTypeColumnFamilyDeletion             varint32 varstring
    kTypeColumnFamilySingleDeletion       varint32 varstring
    kTypeColumnFamilyRangeDeletion        varint32 varstring varstring
    kTypeColumnFamilyMerge                varint32 varstring varstring
    kTypeWideColumnEntity                 varstring varstring
    kTypeColumnFamilyWideColumnEntity     varint32 varstring varstring
    kTypeValuePreferredSeqno              varstring varstring   // TimedPut
    kTypeColumnFamilyValuePreferredSeqno  varint32 varstring varstring
    kTypeBeginPrepareXID                  (no payload)
    kTypeEndPrepareXID                    varstring             // xid
    kTypeCommitXID                        varstring             // xid
    kTypeRollbackXID                      varstring             // xid
    kTypeNoop                             (no payload)
    kTypeLogData                          varstring             // NOT counted, NOT sequenced

varstring := len: varint32, data: uint8[len]
```

Non-CF variants are for column family 0. CF variants encode the CF id as a varint32 after the tag byte, before the key. `kTypeLogData` does not increment the entry count and does not consume a sequence number.

For `TimedPut`: the value field packs `value || fixed64(write_unix_time)`. If `write_unix_time == UINT64_MAX`, a plain `kTypeValue` is written instead.

### Data Structures

**Private fields of `WriteBatch`:**

| Field | Type | Purpose |
|-------|------|---------|
| `rep_` | `std::string` | Serialized binary data |
| `save_points_` | `unique_ptr<SavePoints>` | Stack of save points for rollback |
| `content_flags_` | `atomic<uint32_t>` | Lazy-computed bitmask of operation types present |
| `max_bytes_` | `size_t` | Size limit enforced by `LocalSavePoint::commit()` |
| `prot_info_` | `unique_ptr<ProtectionInfo>` | Optional per-entry checksums (0 or 8 bytes/key) |
| `default_cf_ts_sz_` | `size_t` | Timestamp size for CF 0 |
| `needs_in_place_update_ts_` | `bool` | True if timestamps need to be filled via `UpdateTimestamps()` |

**`ProtectionInfo`:** Contains `autovector<ProtectionInfoKVOC64>` with one entry per counted operation. Each entry protects: Key, Value, Operation type (normalized to non-CF variant), Column family id.

### How It Works

**Writing operations:** Each `Put`/`Delete`/`Merge`/etc. call appends the encoded record to `rep_`, increments the count in bytes 8-11, and sets the corresponding content flag atomically. A `LocalSavePoint` RAII guard snapshots the batch state before each append and enforces `max_bytes_` on commit.

**Timestamp handling:** When a CF has `ts_sz > 0` and the non-timestamp overload is called, a zero-filled timestamp is appended to the key and `needs_in_place_update_ts_` is set. `UpdateTimestamps()` later overwrites these placeholders in-place.

**Iteration (Handler pattern):** `Iterate(Handler*)` walks `rep_` from offset 12, decoding each record and dispatching to the handler's virtual methods (`PutCF`, `DeleteCF`, `MergeCF`, etc.). Supports `TryAgain` for concurrent memtable retry (two consecutive `TryAgain` = corruption).

**Save points:** `SetSavePoint()` snapshots `{rep_.size(), Count(), content_flags_}`. `RollbackToSavePoint()` truncates `rep_` and restores count/flags.

### Invariants

1. `rep_.size() >= 12` always (header).
2. `Count()` in the header equals the number of sequenced records.
3. Keys and values are limited to `< UINT32_MAX` bytes.
4. `protection_bytes_per_key` must be 0 or 8.
5. `TimedPut` is incompatible with user-defined timestamps.
6. `Merge` with explicit timestamp returns `NotSupported`.

### Interactions

- **WriteThread:** Each `Writer` holds a `WriteBatch*`. The leader merges batches via `WriteBatchInternal::Append` for WAL writes.
- **WAL:** The entire `rep_` is written as a single logical record via `log::Writer::AddRecord`.
- **MemTable:** `WriteBatchInternal::InsertInto` creates a `MemTableInserter` handler and iterates the batch, inserting each operation into the appropriate memtable.

---

## 2. WriteThread

**Files:** `db/write_thread.h`, `db/write_thread.cc`

### What It Does

`WriteThread` coordinates concurrent writer threads using a lock-free queue and leader election. One thread (the leader) performs WAL and/or memtable writes on behalf of the entire group, avoiding per-writer mutex contention.

### State Machine

```
STATE_INIT (1)
  -> STATE_GROUP_LEADER (2)          -- elected as batch group leader
  -> STATE_COMPLETED (16)            -- leader finished on this writer's behalf
  -> STATE_PARALLEL_MEMTABLE_WRITER (8)  -- must write own memtable
  -> STATE_MEMTABLE_WRITER_LEADER (4)    -- pipelined: leads memtable group
  -> STATE_PARALLEL_MEMTABLE_CALLER (64) -- helper for large parallel groups

STATE_LOCKED_WAITING (32)  -- transient: thread sleeping on condvar
```

States are powers of 2 for bitmask operations in `AwaitState`.

### Data Structures

**`Writer` struct** (stack-allocated by each calling thread):

| Field | Type | Purpose |
|-------|------|---------|
| `batch` | `WriteBatch*` | The write batch (`nullptr` for unbatched) |
| `sync` | `bool` | WAL sync required |
| `no_slowdown` | `bool` | Fail instead of waiting during stalls |
| `disable_wal` | `bool` | Skip WAL write |
| `disable_memtable` | `bool` | Skip memtable insertion |
| `state` | `atomic<uint8_t>` | Current state machine state |
| `sequence` | `SequenceNumber` | Assigned sequence number |
| `status` | `Status` | Result of write |
| `link_older` / `link_newer` | `Writer*` | Intrusive doubly-linked list |
| `write_group` | `WriteGroup*` | Group this writer belongs to |
| `callback` | `WriteCallback*` | Optional pre-write callback |

Mutex/condvar for blocking waits are lazily constructed in `state_mutex_bytes` / `state_cv_bytes` via `CreateMutex()`.

**`WriteGroup` struct:**

| Field | Type | Purpose |
|-------|------|---------|
| `leader` | `Writer*` | Head of group (oldest writer) |
| `last_writer` | `Writer*` | Tail of group (newest writer) |
| `last_sequence` | `SequenceNumber` | Last sequence consumed |
| `running` | `atomic<size_t>` | Parallel workers still executing |
| `size` | `size_t` | Number of writers in group |

### Algorithms

**Lock-free enqueue (`LinkOne`):** CAS loop on `newest_writer_`. Sets `w->link_older = current_head`, then swaps the head. Returns `true` if the queue was empty (this writer is the leader).

**`CreateMissingNewerLinks`:** Fills in `link_newer` back-pointers by walking `link_older` from the newest writer. Only called by the leader.

**Leader election (`JoinBatchGroup`):** `LinkOne` enqueues the writer. If it returns true, the writer is immediately the leader. Otherwise, `AwaitState` blocks until the state changes.

**Group formation (`EnterAsBatchGroupLeader`):** The leader walks from itself (oldest) to `newest_writer_` (snapshot), collecting compatible writers. Incompatible writers (different sync/wal/protection settings, callbacks that disallow batching, would exceed max size, `batch == nullptr`, `ingest_wbwi`) are spliced out and re-linked after the group.

Max group size: if the leader's batch is small (`<= max_size/8`), the group is capped at `leader_size + max_size/8` to avoid penalizing small writes with large follower batches.

**Group completion (`ExitAsBatchGroupLeader`):** Propagates status to all followers, sets each to `STATE_COMPLETED`. If new writers arrived during processing, hands off leadership to the next writer via `SetState(next, STATE_GROUP_LEADER)`.

### Adaptive Wait (`AwaitState`)

Three-level wait to minimize latency:

1. **Pause loop** (always): 200 iterations of `AsmVolatilePause` (~1.4 us).
2. **Yield loop** (adaptive): `std::this_thread::yield()` up to `max_yield_usec_`. Controlled by per-call-site `AdaptationContext::yield_credit`. After 3 slow yields (> `slow_yield_usec_`), falls through to blocking.
3. **Blocking wait:** Constructs mutex/condvar, CAS state to `STATE_LOCKED_WAITING`, blocks on condvar.

`SetState` checks for `STATE_LOCKED_WAITING` and uses mutex+notify if the waiter is sleeping.

### Parallel Memtable Writes

`LaunchParallelMemTableWriters` sets `running = group_size` and transitions each writer to `STATE_PARALLEL_MEMTABLE_WRITER`. For large groups (>=20), uses a two-level O(sqrt(n)) scheme: leaders set `STATE_PARALLEL_MEMTABLE_CALLER` on stride helpers, who each wake their stride of workers.

`CompleteParallelMemTableWriter` atomically decrements `running`. The last writer (running reaches 0) returns `true` and is responsible for publishing the sequence and exiting the group.

### Stall Mechanism

`BeginWriteStall` inserts a sentinel `write_stall_dummy_` at the head of `newest_writer_`. All subsequent `LinkOne` calls see this sentinel and block on `stall_cv_`. Writers with `no_slowdown=true` are spliced out and completed with `Status::Incomplete`.

`EndWriteStall` removes the sentinel and signals `stall_cv_` to wake all blocked writers.

### Pipelined Write Support

Two queues: `newest_writer_` (WAL) and `newest_memtable_writer_` (memtable). After WAL write, the leader links memtable-eligible writers into `newest_memtable_writer_` via `LinkGroup`. A separate leader election (`EnterAsMemTableWriter`) handles the memtable phase, allowing the next WAL group to proceed concurrently.

### Invariants

1. Only one thread observes `newest_writer_ == nullptr` (exactly one leader per epoch).
2. `link_newer` is only modified by the current leader or before linking.
3. `stall_ended_count_ <= stall_begun_count_ <= stall_ended_count_ + 1` (at most one outstanding stall).
4. `CompleteParallelMemTableWriter` returns `true` for exactly one thread.

---

## 3. Write-Ahead Log (WAL)

**Files:** `db/log_format.h`, `db/log_writer.h`, `db/log_reader.h`, `db/wal_manager.h`, `db/wal_edit.h`

### What It Does

The WAL provides crash recovery by persisting `WriteBatch` data to a sequential log file before memtable insertion. On recovery, WAL records are replayed to reconstruct memtable state.

### Record Format

WAL files are divided into 32 KB blocks (`kBlockSize = 32768`). Each block contains one or more physical records. Logical records (WriteBatch data) that don't fit in a single block are fragmented.

**Legacy header (7 bytes):**

```
+----------+-----------+-----------+
| CRC (4B) | Size (2B) | Type (1B) |
+----------+-----------+-----------+
```

**Recyclable header (11 bytes):**

```
+----------+-----------+-----------+------------------+
| CRC (4B) | Size (2B) | Type (1B) | Log Number (4B)  |
+----------+-----------+-----------+------------------+
```

CRC is `crc32c(type_byte || payload_bytes)`. The log number in recyclable headers detects stale records from previous incarnations of recycled files.

**Record types:**

| Type | Value | Meaning |
|------|-------|---------|
| `kZeroType` | 0 | Preallocated/unwritten (never written by writer) |
| `kFullType` | 1 | Complete logical record |
| `kFirstType` | 2 | First fragment of multi-block record |
| `kMiddleType` | 3 | Interior fragment |
| `kLastType` | 4 | Last fragment |
| `kRecyclable{Full,First,Middle,Last}Type` | 5-8 | Same but for recycled files |
| `kSetCompressionType` | 9 | Meta-record: compression algorithm announcement |
| `kUserDefinedTimestampSizeType` | 10 | Meta-record: CF timestamp sizes |
| `kPredecessorWALInfoType` | 130 | Predecessor WAL chain verification |

Types >= 10 use bit 0 to distinguish recyclable (odd) from non-recyclable (even). Types with bit 7 set (`kRecordTypeSafeIgnoreMask = 0x80`) may be safely skipped by older readers.

### log::Writer

Appends logical records to a WAL file. Manages block-level framing, fragmentation, CRC computation, and optional compression.

**Write flow:**
1. `AddRecord(slice)` called with the serialized `WriteBatch::rep_`.
2. If compression enabled, compress the payload.
3. Fragment into physical records that fit within 32 KB blocks.
4. Each fragment: compute CRC, serialize header, write header + payload via `EmitPhysicalRecord`.
5. Pad any remaining block space with zeros if a header won't fit.

Pre-computed `type_crc_[kMaxRecordType+1]` avoids recomputing CRC of the type byte on every write.

### log::Reader

Sequential reader that reassembles logical records from physical fragments. Verifies CRCs, handles recycled file validation, decompresses payloads, and extracts metadata records.

**Read flow:**
1. `ReadRecord` calls `ReadPhysicalRecord` in a loop.
2. Physical records are read from a 32 KB buffer (`backing_store_`).
3. Fragment types drive reassembly into `scratch` buffer.
4. Meta-records (`kSetCompressionType`, `kUserDefinedTimestampSizeType`, `kPredecessorWALInfoType`) are consumed internally.
5. Returns `false` at EOF.

`FragmentBufferedReader` is a non-blocking subclass that buffers incomplete multi-fragment records, returning `false` until the complete record is assembled. Used for live tailing.

### WalManager

High-level interface over the collection of WAL files. Provides:
- `GetSortedWalFiles`: lists live + archived WALs sorted by log number.
- `GetUpdatesSince(seq)`: iterator over write batches >= sequence number (for replication).
- `PurgeObsoleteWALFiles`: deletes archived WALs exceeding TTL or size limits (rate-limited to once per 600 seconds).
- `ArchiveWALFile`: moves WAL to `<wal_dir>/archive/`.

Caches first-record sequence numbers in `read_first_record_cache_` to avoid re-reading files.

### WalEdit (MANIFEST Tracking)

**`WalAddition`:** Records WAL creation or sync-size update in `VersionEdit`. Encoded as `varint64(wal_number) [kSyncedSize varint64(bytes)] kTerminate`.

**`WalDeletion`:** Records deletion of all WALs with number < threshold. Encoded as `varint64(number)`.

**`WalSet`:** In-memory ordered map of live WALs (`map<WalNumber, WalMetadata>`). Maintained by `VersionSet` under DB mutex. Enforces: WALs transition from unknown-size to known-size (never reverse); `CheckWals` validates on-disk files match MANIFEST records.

### WAL File Lifecycle

```
Created (empty) -> Written (AddRecord) -> Flushed+Synced (memtable flushed)
    -> Archived (moved to archive/) -> Purged (TTL/size limit)
    -> Recycled (reused with new log number, if recycle_log_files enabled)
    -> Deleted (immediately, if no archival/recycling)
```

### Invariants

1. Within a block, records are contiguous; unused space is zero-padded.
2. A logical record is always written as `kFull`, or `kFirst [kMiddle...] kLast`.
3. Recyclable headers include log number; reader rejects records with wrong log number.
4. `WalSet`: at most one WAL may be open (unknown synced size) at a time.
5. WAL order = sequence number order (enforced by holding `wal_write_mutex_` during concurrent writes).

---

## 4. MemTable

**Files:** `db/memtable.h`, `db/memtable.cc`, `include/rocksdb/memtablerep.h`

### What It Does

`MemTable` is an in-memory sorted data structure that receives writes before they are flushed to SST files. It supports point lookups, range scans, and concurrent reads during writes.

### Internal Key Format

Every memtable entry is stored as a flat buffer:

```
[varint32: internal_key_size]       // user_key.size() + 8
[user_key bytes]                    // may include UDT suffix
[uint64: packed_tag]                // (sequence_number << 8) | value_type
[varint32: value_size]
[value bytes]
[checksum: 0-8 bytes]              // controlled by protection_bytes_per_key
```

Within the same user key, entries are sorted by descending sequence number (newest first).

### Data Structures

**Storage:**

| Field | Type | Purpose |
|-------|------|---------|
| `arena_` | `ConcurrentArena` | All memtable memory; shared by point and range-del tables |
| `table_` | `unique_ptr<MemTableRep>` | Point key storage (factory-selected: SkipList, HashSkipList, Vector, etc.) |
| `range_del_table_` | `unique_ptr<MemTableRep>` | Always SkipList; stores range tombstones |
| `bloom_filter_` | `unique_ptr<DynamicBloom>` | Prefix/whole-key bloom filter (null if disabled) |

**Tracking:**

| Field | Type | Purpose |
|-------|------|---------|
| `first_seqno_` | `atomic<SequenceNumber>` | Seqno of first inserted key; 0 if empty |
| `earliest_seqno_` | `atomic<SequenceNumber>` | Lower bound on all inserted seqnos |
| `data_size_` | `RelaxedAtomic<size_t>` | Total encoded entry bytes |
| `num_entries_` | `RelaxedAtomic<size_t>` | Total entry count |
| `flush_state_` | `atomic<FlushStateEnum>` | NOT_REQUESTED -> REQUESTED -> SCHEDULED |

### Insertion (`Add`)

1. Compute `encoded_len = varint(ikey_size) + ikey_size + varint(val_size) + val_size + protection_bytes`.
2. Allocate from `arena_` via `table->Allocate(encoded_len, &buf)`.
3. Encode: length-prefixed internal key (user_key + packed tag), length-prefixed value, checksum.
4. Verify checksum if `kv_prot_info` provided.
5. Update bloom filter with prefix and/or whole key (timestamp stripped).
6. Route: `kTypeRangeDeletion` goes to `range_del_table_`; all others to `table_`.

**Serial path:** `InsertKey(handle)` + plain counter updates.
**Concurrent path:** `InsertKeyConcurrently(handle)` + `MemTablePostProcessInfo` thread-local batching. After the write batch completes, `BatchPostProcess` drains accumulated counts into atomic counters.

Range tombstone cache is invalidated on every `kTypeRangeDeletion` insert by swapping per-CPU `cached_range_tombstone_` slots.

### Point Lookup (`Get`)

1. If empty, return false.
2. Check range tombstone coverage (`MaxCoveringTombstoneSeqnum`).
3. Bloom filter check (prefix or whole-key). On miss: return false immediately.
4. `table_->Get(key, &saver, SaveValue)` -- callback for each matching entry, newest first.
5. `SaveValue` dispatches on `ValueType`: `kTypeValue` fills result, `kTypeDeletion` returns NotFound, `kTypeMerge` accumulates operands.

### Batch Lookup (`MultiGet`)

Bloom filter batch-check for all keys. Then either batch path (`table_->MultiGet` for single-pass sweep) or per-key path (individual `GetFromTable` calls). Range tombstone coverage is pre-computed per key.

### Flush State Machine

```
FLUSH_NOT_REQUESTED  --(ShouldFlushNow())-->  FLUSH_REQUESTED
FLUSH_REQUESTED      --(MarkFlushScheduled() CAS)-->  FLUSH_SCHEDULED
```

`ShouldFlushNow()` heuristic:
- Triggers on external signal (`IsMarkedForFlush()`), range-del count limit, or arena allocation exceeding `write_buffer_size` with the last block > 75% used.
- Called at the end of every `Add()` or `BatchPostProcess()`.

### MemTableRep Interface

Abstract interface for pluggable memtable implementations:

| Implementation | Backing Structure | Concurrent Insert | Best For |
|----------------|-------------------|-------------------|----------|
| `SkipListFactory` (default) | Lock-free skip list | Yes | General purpose |
| `VectorRepFactory` | `std::vector` (sorted on read) | Yes (batched) | Random-write-heavy |
| `HashSkipListRep` | Hash buckets -> skip lists | No | Prefix-structured keys |
| `HashLinkListRep` | Hash buckets -> linked lists/skip lists | No | Prefix keys with few entries per prefix |

All implementations: never delete items, support concurrent reads during writes (MRSW minimum), use `KeyComparator` for ordering.

### Memory Tracking

`AllocTracker` (`mem_tracker_`) charges arena allocations to the `WriteBufferManager`. On `MarkImmutable()`, calls `DoneAllocating()`. On destruction, calls `FreeMem()`.

### Invariants

1. Entries are never removed; the entire memtable is discarded atomically on flush.
2. `(internal_key, seq)` must be unique; violations return `Status::TryAgain`.
3. All entry memory lives in `arena_`; `MemTableRep` stores only pointers.
4. Bloom filter: false positives possible, false negatives impossible.
5. `first_seqno_ == 0` iff the memtable is empty.

---

## 5. MemTableList

**Files:** `db/memtable_list.h`, `db/memtable_list.cc`

### What It Does

`MemTableList` manages the list of immutable memtables for a column family. It drives the flush lifecycle: picking memtables to flush, installing flush results into the MANIFEST, and maintaining flushed-memtable history for transaction conflict detection.

### Data Structures

**`MemTableListVersion`** -- a snapshot of the immutable memtable list. Immutable once shared (refcount > 1). Used by `SuperVersion` for reads.

| Field | Type | Purpose |
|-------|------|---------|
| `memlist_` | `list<ReadOnlyMemTable*>` | Not-yet-flushed memtables (front = newest) |
| `memlist_history_` | `list<ReadOnlyMemTable*>` | Flushed memtables kept for conflict detection |
| `refs_` | `int` | Reference count; mutable only when `refs_ == 1` |
| `id_` | `uint64_t` | Monotonically increasing version ID |

**`MemTableList`** -- the owner/manager, one per column family.

| Field | Type | Purpose |
|-------|------|---------|
| `current_` | `MemTableListVersion*` | Current version (refs >= 1) |
| `num_flush_not_started_` | `int` | Memtables eligible for flush but not yet picked |
| `commit_in_progress_` | `bool` | Guards concurrent manifest writes |
| `flush_requested_` | `bool` | Explicit flush requested |
| `imm_flush_needed` | `atomic<bool>` | Polled by background threads |
| `current_memory_usage_` | `size_t` | Running total of all memtable memory |

### How It Works

**Adding immutable memtables (`Add`):**
1. Copy-on-write `InstallNewVersion` if current version is shared.
2. Push memtable to front of `memlist_`.
3. Call `MarkImmutable()` on the memtable.
4. Increment `num_flush_not_started_`; set `imm_flush_needed = true`.
5. Trim history if memory budget exceeded.

**Flush lifecycle:**

```
PickMemtablesToFlush
  -> selects contiguous oldest-to-newest memtables not yet in-progress
  -> sets flush_in_progress_ = true, decrements num_flush_not_started_

FlushJob runs (produces SST file)
  -> sets flush_completed_ = true, file_number_ = sst_number

TryInstallMemtableFlushResults
  -> single-writer gate (commit_in_progress_)
  -> checks oldest memtable in memlist_ has flush_completed_
  -> collects contiguous completed memtables
  -> LogAndApply to MANIFEST
  -> Remove from memlist_ (move to history or unref)
```

FIFO ordering: the commit loop only starts when the back (oldest) of `memlist_` is complete, ensuring memtables are committed to MANIFEST in creation order even when concurrent flushes complete out of order.

**Rollback (`RollbackMemtableFlush`):** Resets `flush_in_progress_`, `flush_completed_`, increments `num_flush_not_started_`, sets `imm_flush_needed`.

**Reads:** `Get` / `MultiGet` iterate `memlist_` front-to-back (newest first). `GetFromHistory` searches `memlist_history_` for transaction conflict detection.

### Version/Refcount Mechanism

`SuperVersion` holds a `Ref()` on the `MemTableListVersion`. When a new version is installed (copy-on-write), the old version lives until all `SuperVersion` references are released. The copy constructor `Ref()`s every memtable in both lists.

### Invariants

1. `MemTableListVersion` is mutable only when `refs_ == 1` (asserted in `Add`/`Remove`).
2. Memtables are committed to MANIFEST in creation order.
3. Picked memtables are contiguous (oldest-to-newest).
4. `num_flush_not_started_ <= memlist_.size()`.
5. History trimmed whenever memory budget (`max_write_buffer_size_to_maintain_`) exceeded.

---

## 6. WriteBufferManager

**File:** `include/rocksdb/write_buffer_manager.h`

### What It Does

`WriteBufferManager` (WBM) tracks total memory consumed by memtables across all DB instances sharing it. It triggers flushes when memory approaches the limit and stalls writers when the limit is exceeded.

### Memory Tracking

| Counter | Updated By | Meaning |
|---------|-----------|---------|
| `memory_used_` | `ReserveMem` / `FreeMem` | Total bytes reserved (active + being-flushed) |
| `memory_active_` | `ReserveMem` / `ScheduleFreeMem` | Bytes in mutable memtables only |

Lifecycle:
```
Memtable created       -> ReserveMem(size)        : memory_used_++, memory_active_++
Memtable flush started -> ScheduleFreeMem(size)   : memory_active_-- only
Memtable freed         -> FreeMem(size)           : memory_used_--, MaybeEndWriteStall()
```

### Flush Trigger (`ShouldFlush`)

```
if mutable_memtable_memory_usage > buffer_size * 7/8:
    -> flush (active memtables too large)
if memory_usage >= buffer_size AND mutable_memory >= buffer_size / 2:
    -> flush (total exceeded, enough is still mutable to benefit from flushing)
```

The second condition prevents triggering more flushes when most memory is already being flushed.

### Stall Mechanism

Hard block (not rate limit). When `allow_stall` is enabled and `memory_usage >= buffer_size`:
1. `ShouldStall()` returns true.
2. Writer calls `BeginWriteStall(stall_interface)` -- appends to `queue_`, sets `stall_active_`.
3. Writer blocks on `stall_interface->Block()`.
4. When `FreeMem()` drops usage below threshold, `MaybeEndWriteStall()` signals all blocked writers.

### Cache-Based Accounting

When a `Cache` is provided, WBM inserts dummy entries into the block cache equal to `memory_used_`. This makes memtable memory compete with cached blocks for the same capacity, applying LRU eviction pressure.

### Features Supported

- Cross-DB memory management (single WBM shared by multiple `DBImpl` instances).
- Dynamic buffer size adjustment (`SetBufferSize`).
- Dynamic stall enable/disable (`SetAllowStall`).
- Graceful DB removal from stall queue (`RemoveDBFromQueue`).

---

## 7. WriteController

**Files:** `db/write_controller.h`, `db/write_controller.cc`

### What It Does

`WriteController` rate-limits or completely stops writes within a single DB instance when compaction falls behind. It uses a token-based system with RAII tokens.

### Token Types

| Token | Counter | Effect |
|-------|---------|--------|
| `StopWriteToken` | `total_stopped_` | Full write stop; writers wait on `bg_cv_` |
| `DelayWriteToken` | `total_delayed_` | Rate-limited writes via `GetDelay()` |
| `CompactionPressureToken` | `total_compaction_pressure_` | Triggers higher compaction parallelism |

Tokens are RAII: increment counter on construction, decrement on destruction. Created by `ColumnFamilyData` when L0 file count or pending compaction bytes exceed thresholds. Destroyed when conditions resolve.

### Token Bucket Algorithm (`GetDelay`)

Operates on 1ms refill intervals. Called under DB mutex; returns microseconds to sleep (caller sleeps outside mutex).

```
1. If stopped: return 0 (stop handled separately)
2. If not delayed: return 0
3. Fast path: if credit >= num_bytes, consume and return 0
4. Refill: credit += elapsed_time * delayed_write_rate
5. If credit >= num_bytes after refill: consume and return 0
6. Compute needed_delay = (num_bytes - credit) / rate * 1,000,000
7. Return max(needed_delay, 1ms)
```

Minimum return value is 1ms to batch writes and reduce mutex churn.

### Key Fields

| Field | Type | Purpose |
|-------|------|---------|
| `delayed_write_rate_` | `uint64_t` | Current rate limit (bytes/sec) |
| `max_delayed_write_rate_` | `uint64_t` | Ceiling on rate |
| `credit_in_bytes_` | `uint64_t` | Token bucket credit |
| `next_refill_time_` | `uint64_t` | Monotonic us when next refill is due |

### WriteController vs WriteBufferManager

| Dimension | WriteController | WriteBufferManager |
|-----------|-----------------|---------------------|
| Scope | Per-DB | Shared across DBs |
| Trigger | Compaction lag (L0 files, pending bytes) | Memtable memory usage |
| Mechanism | Token bucket rate limit or full stop | Hard block (condvar) |
| Granularity | Per-write-batch (bytes) | Per-DB (all writers blocked) |
| Recovery | Compaction catches up, token deleted | `FreeMem()` drops below threshold |

---

## 8. DBImpl Write Orchestration

**File:** `db/db_impl/db_impl_write.cc`

### What It Does

`DBImpl::WriteImpl` is the main orchestrator that ties all components together. It coordinates the write thread, WAL, memtable, sequence numbers, and back-pressure mechanisms.

### Entry Flow

```
DB::Put/Delete/Merge -> DB::Write -> DBImpl::WriteImpl
```

### Early Validation

Before joining any write thread, validates:
- Batch is non-null, timestamps are set, rate limiter priority is valid.
- Incompatible option combinations (sync + disableWAL, two_write_queues + pipelined, etc.).
- Low-priority write throttling if `NeedSpeedupCompaction()`.

### PreprocessWrite (Leader Only)

Called by the leader before taking the WAL lock. Checks in order:

| Check | Action |
|-------|--------|
| Background error | Return error immediately |
| WAL size overflow | `SwitchWAL()` -- flush oldest CF, create new WAL |
| WBM should flush | `HandleWriteBufferManagerFlush()` -- switch memtable for oldest CF |
| Trim history needed | `TrimMemtableHistory()` |
| Flush scheduled | `ScheduleFlushes()` |
| WriteController stopped/delayed | `DelayWrite()` -- sleep or wait on condvar |
| WBM should stall | `WriteBufferManagerStallWrites()` -- block thread |

### Normal (Batched) Write Path

1. `JoinBatchGroup(&w)` -- leader election.
2. If follower: wait for `STATE_COMPLETED` or `STATE_PARALLEL_MEMTABLE_WRITER`.
3. Leader calls `PreprocessWrite`.
4. `EnterAsBatchGroupLeader` -- collect compatible writers into group.
5. **WAL write:** `MergeBatch` combines batches, `WriteGroupToWAL` writes + optional sync.
6. **Sequence assignment:** Each writer gets `sequence = next_sequence`, advancing by key count or batch count.
7. **MemTable insert:** Serial (leader does all) or parallel (`LaunchParallelMemTableWriters`).
8. `SetLastSequence(last_sequence)` -- publish to readers.
9. `ExitAsBatchGroupLeader` -- wake followers with `STATE_COMPLETED`.

### Pipelined Write Path

Separates WAL and memtable into two phases with separate leader elections:

**Phase 1 (WAL):** Leader writes WAL, assigns sequences, exits WAL group. Followers are released to join the memtable queue.

**Phase 2 (Memtable):** New leader election in `newest_memtable_writer_`. Leader inserts (serial or parallel), publishes sequence, exits memtable group.

Key property: WAL and memtable phases overlap across different groups.

### Two-Queue Write Path

`WriteImplWALOnly` uses `nonmem_write_thread_` for WAL-only writes (e.g., WritePrepared 2PC prepares). Atomically allocates sequences under `wal_write_mutex_` via `FetchAddLastAllocatedSequence`. Memtable writes happen separately through `write_thread_`.

### Unordered Write Path

`WriteImplWALOnly` through `write_thread_` allocates sequences and writes WAL. `UnorderedWriteMemtable` inserts each writer's batch independently with `concurrent_memtable_writes=true`. No ordering constraint between writers.

### Sequence Number Assignment

| Concept | Variable | Description |
|---------|----------|-------------|
| `LastSequence` | `versions_->LastSequence()` | Highest seqno visible to readers |
| `LastAllocatedSequence` | `versions_->LastAllocatedSequence()` | Highest allocated (may be ahead) |

Normal path: `LastSequence` read before group, incremented by total key count, published after memtable insert.
Two-queue/unordered: `FetchAddLastAllocatedSequence` atomically allocates under `wal_write_mutex_`.

### SwitchMemtable

Creates a new WAL file and rotates the memtable. Called from `PreprocessWrite` when WBM or WAL size triggers a flush.

1. `WriteRecoverableState` -- flush cached 2PC state to current memtable.
2. Allocate new log number (optionally recycle an old WAL file).
3. Release mutex, create new `log::Writer` and `MemTable`.
4. Reacquire mutex, install new WAL under `wal_write_mutex_`.
5. `ConstructFragmentedRangeTombstones()` on old memtable.
6. Move old memtable to `MemTableList::Add()`.
7. Install new memtable and `SuperVersion`.

### Error Handling

| Error Source | Handler | Effect |
|-------------|---------|--------|
| WAL I/O error | `WALIOStatusCheck` | Sets bg error; IO-fenced = always fatal |
| MemTable insert failure | `HandleMemTableInsertFailure` | Sets bg error, stops background work |
| SwitchMemtable failure | `SetBGError` | Always fatal (WAL buffer potentially lost) |
| Callback failure | `WriteStatusCheck` | Sets bg error if `paranoid_checks` |

On memtable insertion failure, the pre-write WAL file size is recorded (`SetAttemptTruncateSize`) to enable WAL truncation for recovery.

### DelayWrite

Two stall modes:

**Timed delay:** `write_controller_.GetDelay(clock, num_bytes)` returns microseconds. Leader calls `BeginWriteStall()` on `write_thread_`, sleeps in 1ms intervals, then `EndWriteStall()`.

**Hard stop:** Waits on `bg_cv_` until `!IsStopped()` or error/shutdown.

Both modes use `BeginWriteStall/EndWriteStall` to bar new writers (or complete them with `Incomplete` if `no_slowdown`).

---

## Data Flow Summary

### Write Data Flow

```
User API
  |
  +--[1]--> WriteBatch::Put/Delete/Merge
  |           Serialize into binary rep_ (12-byte header + records)
  |
  +--[2]--> WriteThread::JoinBatchGroup
  |           Lock-free enqueue, leader election
  |
  +--[3]--> PreprocessWrite (leader only)
  |           +-- WriteController check -> delay/stop if compaction behind
  |           +-- WriteBufferManager check -> stall if memory exceeded
  |           +-- ShouldFlush check -> SwitchMemtable if needed
  |           +-- WAL size check -> SwitchWAL if needed
  |
  +--[4]--> EnterAsBatchGroupLeader
  |           Collect compatible writers into write group
  |
  +--[5]--> MergeBatch + WriteToWAL
  |           Merge group batches -> log::Writer::AddRecord
  |           Block-frame, CRC, fragment across 32KB blocks
  |           Optional fsync
  |
  +--[6]--> Assign sequence numbers
  |           writer->sequence = next_sequence
  |           next_sequence += key_count (or batch_count for seq_per_batch)
  |
  +--[7]--> MemTable::Add (via WriteBatchInternal::InsertInto)
  |           Encode internal key + value into arena
  |           Insert into SkipList (or other MemTableRep)
  |           Update bloom filter
  |           Serial or parallel (LaunchParallelMemTableWriters)
  |
  +--[8]--> SetLastSequence
  |           Publish sequence to readers (snapshot visibility)
  |
  +--[9]--> ExitAsBatchGroupLeader
              Wake followers with STATE_COMPLETED
```

### Flush Data Flow

```
MemTable::ShouldFlushNow() == true
  |
  +-- MemTable::UpdateFlushState() -> FLUSH_REQUESTED
  |
  +-- DBImpl::ScheduleFlushes() -> enqueue flush job
  |
  +-- MemTableList::PickMemtablesToFlush()
  |     Select contiguous oldest memtables, set flush_in_progress_
  |
  +-- FlushJob::Run()
  |     Read memtable via iterator -> write SST file
  |
  +-- MemTableList::TryInstallMemtableFlushResults()
  |     FIFO ordering: wait for oldest to complete
  |     LogAndApply to MANIFEST
  |     Remove from memlist_ (optionally to history)
  |
  +-- WriteBufferManager::FreeMem()
        Decrement memory_used_, MaybeEndWriteStall()
```

### Back-Pressure Data Flow

```
Compaction falls behind
  |
  +-- ColumnFamilyData detects L0/pending bytes threshold
  |     -> WriteController::GetDelayToken or GetStopToken
  |
  +-- PreprocessWrite checks IsStopped()/NeedsDelay()
  |     -> DelayWrite: sleep (rate limit) or wait (full stop)
  |     -> BeginWriteStall bars new writers
  |
  +-- Compaction catches up
  |     -> Token destroyed -> total_stopped_/total_delayed_ decremented
  |     -> EndWriteStall wakes blocked writers


MemTable memory exceeds buffer_size
  |
  +-- WriteBufferManager::ShouldFlush() -> HandleWriteBufferManagerFlush
  |     -> SwitchMemtable on oldest CF
  |
  +-- WriteBufferManager::ShouldStall() -> WriteBufferManagerStallWrites
  |     -> BeginWriteStall blocks thread
  |     -> FreeMem -> MaybeEndWriteStall signals blocked writers
```

### Recovery Data Flow

```
DB::Open
  |
  +-- Read MANIFEST -> WalSet (live WAL list)
  |
  +-- For each WAL in order:
  |     log::Reader::ReadRecord
  |       -> Reassemble logical records from 32KB block fragments
  |       -> Verify CRC, handle recycled headers
  |       -> Decompress if needed
  |     WriteBatchInternal::InsertInto(batch, memtable)
  |       -> Replay into fresh MemTable
  |
  +-- Flush recovered memtable -> SST
```
