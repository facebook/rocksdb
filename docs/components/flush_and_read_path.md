# Flush and Read Path

## Overview

This document covers the flush process (converting immutable memtables to L0 SST files) and all read operations (point lookups, iteration, range deletion handling). These paths share key infrastructure: SuperVersion provides consistent snapshots, MergingIterator merges multiple sorted sources, and DBIter resolves internal key semantics into user-visible results.

### Read Path Summary

```
DB::Get(key)
    |
    v
Acquire SuperVersion (mem + imm + Version)
    |
    v
1. Mutable MemTable    -> found? done
2. Immutable MemTables -> found? done (newest to oldest)
3. SST files (Version) -> L0 (all, newest first), then L1..Ln (binary search per level)
    |
    v
Resolve merges, check range tombstones, return result
```

---

## 1. FlushJob

**Files:** `db/flush_job.h`, `db/flush_job.cc`

### What It Does

FlushJob converts immutable memtables into a single L0 SST file. One FlushJob instance handles one column family.

### Three-Phase Lifecycle

| Phase | Lock | Description |
|-------|------|-------------|
| `PickMemTable()` | Mutex held | Select immutable memtables with ID <= `max_memtable_id_`, allocate file number, Ref current Version |
| `Run()` | Mutex held (released during I/O) | Attempt MemPurge or WriteLevel0Table, then install results |
| `Cancel()` | Mutex held | Abandon flush, unref Version |

### WriteLevel0Table Flow

```
PickMemTable()
    |
    v
Run()
    |
    +-- MemPurge? (if threshold > 0, reason == kWriteBufferFull,
    |       |      !mems_.empty(), MemPurgeDecider approves, and !atomic_flush)
    |       |
    |     success? -> done
    |       |
    |     fail -> fall through
    |
    +-- WriteLevel0Table()
    |     1. Release db_mutex
    |     2. Create iterators over picked memtables (point + range tombstones)
    |     3. Merge via MergingIterator
    |     4. BuildTable() -> L0 SST file
    |     5. Re-acquire db_mutex
    |     6. Update VersionEdit with new file metadata
    |
    +-- TryInstallMemtableFlushResults()
          |
          Commit to MANIFEST, remove flushed memtables
```

### Key Fields

| Field | Description |
|-------|-------------|
| `mems_` | `autovector<ReadOnlyMemTable*>` -- memtables selected for flush, oldest first |
| `meta_` | `FileMetaData` for the output SST file |
| `edit_` | `VersionEdit*` from first memtable, records the new L0 file |
| `base_` | `Version*` at flush time (ref'd for duration) |
| `max_memtable_id_` | Upper bound for memtable selection |

### MemPurge

Experimental in-memory GC. Triggered when `mempurge_threshold > 0`, flush reason is `kWriteBufferFull`, `mems_` is non-empty, `MemPurgeDecider` heuristic approves (based on useful payload ratio vs. threshold), and atomic flush is disabled. Iterates memtable with a compaction-like iterator, filters outdated entries, and inserts survivors into a new memtable. If the output fits in one memtable, it replaces the original on the immutable list without producing an SST file. Falls through to `WriteLevel0Table()` on failure.

---

## 2. MemTableList

**Files:** `db/memtable_list.h`, `db/memtable_list.cc`

### What It Does

Manages the list of immutable memtables for a column family. `MemTableListVersion` is the immutable snapshot; `MemTableList` is the mutable controller.

### MemTableListVersion

Immutable, ref-counted snapshot of the immutable memtable list:

| Field | Description |
|-------|-------------|
| `memlist_` | `std::list<ReadOnlyMemTable*>` -- unflushed immutable memtables, newest first |
| `memlist_history_` | Flushed memtables kept for transaction conflict checking |

Key methods:
- `Get(key)` -- searches all immutable memtables newest to oldest
- `MultiGet(range)` -- batched version
- `AddIterators()` -- creates iterators for MergeIteratorBuilder
- `Add(m)` / `Remove(m)` -- add/remove memtables (creates new version)

### MemTableList

Mutable controller (requires DB mutex):

| Method | Description |
|--------|-------------|
| `PickMemtablesToFlush(max_id, mems)` | Select oldest memtables not yet flushing, up to `max_id` |
| `TryInstallMemtableFlushResults()` | Commit flush results. Enforces FIFO ordering: waits for older memtables to complete first |
| `RollbackMemtableFlush()` | Reset memtables to pending on failure |
| `Add(m)` | Add new immutable memtable, signal `imm_flush_needed` |

### FIFO Flush Ordering Invariant

`TryInstallMemtableFlushResults()` ensures that even if memtable N+1 finishes flushing before memtable N, the result is not installed until N completes. This preserves the invariant that L0 files are ordered by memtable creation time.

### Atomic Flush

`InstallMemtableAtomicFlushResults()` (free function) commits flush results across multiple column families atomically. Either all CF flushes are visible or none are.

---

## 3. Point Lookup (Get / MultiGet)

**Files:** `db/db_impl/db_impl.h`, `db/db_impl/db_impl.cc`

### GetImpl Lookup Order

```cpp
Status DBImpl::GetImpl(const ReadOptions& options, const Slice& key,
                       GetImplOptions& get_impl_options);
```

1. **Acquire SuperVersion**: `GetAndRefSuperVersion(cfd)` -- provides consistent snapshot of memtable + immutable memtables + SST files
2. **Determine snapshot**: from `ReadOptions::snapshot` or `GetLastPublishedSequence()`
3. **Construct LookupKey**: `LookupKey(user_key, snapshot_seqno)`
4. **Mutable MemTable**: `sv->mem->Get(...)` -- if found, `done = true` (MEMTABLE_HIT)
5. **Immutable MemTables**: `sv->imm->Get(...)` -- newest to oldest (MEMTABLE_HIT)
6. **SST Files**: `sv->current->Get(...)` -- L0 files (all, newest first), L1+ (binary search per level) (MEMTABLE_MISS)
7. **Post-processing**: merge operand resolution, blob value retrieval
8. **Release SuperVersion**: `ReturnAndCleanupSuperVersion(sv)`

### MultiGetImpl

```cpp
Status DBImpl::MultiGetImpl(const ReadOptions&, size_t start_key, size_t num_keys,
                            autovector<KeyContext*>* sorted_keys,
                            SuperVersion* sv, SequenceNumber snap_seqnum, ...);
```

Same lookup order as GetImpl but operates on batches. Pre-sorts keys, shares a single SuperVersion reference across the batch, and exploits spatial locality for SST reads (batch I/O, shared filter checks).

### Version::Get (SST Lookup)

`Version::Get()` (`db/version_set.h:936`) searches SST files:

1. **L0**: Check all L0 files (they may overlap). Files are checked newest-first by epoch_number.
2. **L1+**: Binary search using `FindFile()` on `LevelFilesBrief` to find the file containing the key. Only one file per level can contain the key (non-overlapping guarantee).
3. For each file: `TableCache::Get()` applies bloom filter, then seeks within the data block.

---

## 4. DBIter (User-Facing Iterator)

**Files:** `db/db_iter.h`, `db/db_iter.cc`

### What It Does

`DBIter` wraps an `InternalIterator` (typically a `MergingIterator`) and resolves internal key semantics into user-visible key-value pairs. It handles:
- **Version deduplication**: same user key with multiple sequence numbers -> latest visible version
- **Deletion resolution**: tombstones hide earlier Puts
- **Merge operator**: accumulates operands, applies merge
- **Range tombstones**: integrated via the underlying MergingIterator's tombstone interleaving

### Direction Model

```
enum Direction { kForward, kReverse };
```

- **Forward**: internal iterator positioned at (or just past) the yielded entry
- **Reverse**: internal iterator positioned just before all entries with `user_key == current key`

Direction changes require `ReverseToForward()` or `ReverseToBackward()` transitions.

### Key Methods

| Method | Description |
|--------|-------------|
| `Seek(target)` | Seek internal iterator, then `FindNextUserEntry()` to skip tombstones and older versions |
| `Next()` | Advance forward, skip past current key's remaining versions |
| `Prev()` | Move backward via `PrevInternal()`, collecting merge operands |
| `SeekForPrev(target)` | Position at last key <= target |

### Merge Resolution

`FindNextUserEntry()` encounters a kTypeMerge:
1. Collects all merge operands (newer to older) via `MergeValuesNewToOld()`
2. Continues until a Put/Delete base is found or no more versions exist
3. Applies merge operator to produce final value

### Memtable Flush Trigger

During iteration, if too many hidden (overwritten) entries are scanned within a memtable, DBIter calls `MarkForFlush()` on the active memtable. This is a heuristic to flush memtables with high overwrite ratios.

### Iterator Construction

`DBImpl::NewInternalIterator()` creates a `MergingIterator` over:
1. Mutable memtable iterator + range tombstone iterator
2. Immutable memtable iterators (via `imm->AddIterators()`)
3. SST file iterators (via `current->AddIterators()`)

This is wrapped by `DBIter` for user-facing iteration.

---

## 5. ForwardIterator

**Files:** `db/forward_iterator.h`, `db/forward_iterator.cc`

### What It Does

An `InternalIterator` optimized for forward-only traversal. Used as the underlying iterator for tailing iterators. Unlike `DBIter`, it is not a user-facing iterator -- it exposes raw internal keys.

### Key Differences from Standard Path

| Aspect | Standard (MergingIterator + DBIter) | ForwardIterator |
|--------|-------------------------------------|-----------------|
| Direction | Bidirectional | Forward only (`Prev()` not supported) |
| SuperVersion | Fixed at creation | Dynamically rebuilt via `RebuildIterators()` / `RenewIterators()` |
| Immutable seek | Always re-seek all | `NeedToSeekImmutable()` skips when key is in previously covered range |

### Architecture

ForwardIterator directly manages child iterators:
- `mutable_iter_` -- mutable memtable iterator
- `imm_iters_` -- one per immutable memtable
- `l0_iters_` -- one per L0 SST file
- `level_iters_` -- one `ForwardLevelIterator` per L1+ level
- `immutable_min_heap_` -- min-heap over immutable sources
- `current_` -- child iterator at the smallest key

### NeedToSeekImmutable Optimization

Tracks `prev_key_` (left endpoint of covered range). When `Seek()` is called within this range, only the mutable memtable iterator is re-seeked. Immutable sources are unchanged since their data is frozen.

---

## 6. RangeDelAggregator

**Files:** `db/range_del_aggregator.h`, `db/range_del_aggregator.cc`

### What It Does

Aggregates range tombstones from multiple sources (memtables, SST files) and answers "is this key deleted by a range tombstone?" queries. Two subclasses serve different use cases.

### Class Hierarchy

```
RangeDelAggregator (abstract)
    +-- ReadRangeDelAggregator     (reads/iterators)
    +-- CompactionRangeDelAggregator (compaction)
```

### TruncatedRangeDelIterator

Wraps a `FragmentedRangeTombstoneIterator` and truncates tombstone boundaries to the `[smallest, largest]` range of an SST file. This ensures tombstones don't leak beyond file boundaries.

### ReadRangeDelAggregator

Used during point lookups and iteration. Contains a single `StripeRep` with sequence range `[0, snapshot_seqno]`. All tombstones visible at the read snapshot are considered.

`ShouldDelete(parsed_key, mode)`:
- Fast path: if `IsEmpty()`, return false (no range tombstones exist)
- Otherwise: check if any active tombstone covers the key with a higher sequence number

### CompactionRangeDelAggregator

Used during compaction. Maintains multiple `StripeRep`s partitioned by snapshot sequence numbers. This enables per-snapshot visibility when deciding whether to drop keys during compaction.

Additional features:
- Timestamp filtering via `full_history_ts_low`
- `NewIterator()` produces tombstone iterator for compaction output

### ShouldDelete Algorithm (ForwardRangeDelIterator)

1. Move inactive tombstones that now cover the key into the active set
2. Remove active tombstones whose range no longer covers the key
3. If the highest-sequence active tombstone has `seq > key.sequence`, the key is deleted

Active tombstones are stored in:
- A `multiset` ordered by sequence number (for max-seq lookup)
- A `BinaryHeap` ordered by end key (for efficient boundary tracking)

---

## Key Invariants

| Invariant | Details |
|-----------|---------|
| Flush FIFO ordering | Memtable N must be installed before N+1, even if N+1 finishes first |
| SuperVersion consistency | Readers see a consistent snapshot of mem + imm + Version |
| Lookup order: mem -> imm -> SST | Ensures newest data is found first |
| L0 files checked newest-first | Higher epoch_number = newer data |
| L1+ binary search valid | Non-overlapping key ranges per level |
| Range tombstone truncation at file boundaries | Tombstones don't leak beyond their source file |
| Merge operands accumulated newest-to-oldest | Correct merge operator semantics |
| Atomic flush across CFs | Either all CF flushes are visible or none |

## Interactions With Other Components

- **Write Path** (see [write_path.md](write_path.md)): Write path switches active memtable to immutable, triggering flush. `WriteController` stalls writes when flush/compaction falls behind.
- **Version Management** (see [version_management.md](version_management.md)): FlushJob creates VersionEdit, calls `LogAndApply()`. Readers acquire SuperVersion for consistent view.
- **SST Format** (see [sst_table_format.md](sst_table_format.md)): FlushJob uses `BuildTable()` (which uses `BlockBasedTableBuilder`). Reads use `TableCache` to access `BlockBasedTable`.
- **Compaction** (see [compaction.md](compaction.md)): Flush produces L0 files that trigger compaction. CompactionIterator uses similar merge/delete logic as DBIter.
- **Cache** (see [cache.md](cache.md)): Block cache used during SST reads. TableCache manages open table readers.
