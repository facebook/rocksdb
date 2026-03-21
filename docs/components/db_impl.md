# DBImpl

## Overview

`DBImpl` is the concrete implementation of the `DB` interface -- the entry point to the core RocksDB engine. All other DB implementations (TransactionDB, BlobDB, etc.) wrap a `DBImpl` internally. Due to its size, the implementation is split across multiple files:

| File | Content |
|------|---------|
| `db/db_impl/db_impl.h` | Class declaration (~3300 lines) |
| `db/db_impl/db_impl.cc` | Core logic |
| `db/db_impl/db_impl_write.cc` | Write path (WriteImpl, etc.) |
| `db/db_impl/db_impl_open.cc` | DB::Open, recovery |
| `db/db_impl/db_impl_compaction_flush.cc` | Background flush/compaction scheduling |
| `db/db_impl/db_impl_files.cc` | File management, obsolete file purging |
| `db/db_impl/db_impl_debug.cc` | Debug/test utilities |
| `db/db_impl/db_impl_readonly.h` | Read-only mode |
| `db/db_impl/db_impl_secondary.h` | Secondary instance (tailing primary) |
| `db/db_impl/db_impl_follower.h` | Follower mode |
| `db/db_impl/compacted_db_impl.h` | Compacted/read-optimized DB |

---

## 1. DB Open and Recovery

**Files:** `db/db_impl/db_impl_open.cc`

### Open Flow

```
DB::Open(options, path, column_families, handles, db)
    |
    v
SanitizeOptions() -- validate and adjust options
    |
    v
new DBImpl(options, path)
    |
    v
DBImpl::Recover()
    |
    +-- Read CURRENT file -> identify MANIFEST
    +-- VersionSet::Recover() -> replay MANIFEST, reconstruct Versions
    +-- Recover WAL files -> replay WAL entries into memtables
    +-- If needed: flush recovered memtables
    |
    v
Install SuperVersions for all column families
    |
    v
MaybeScheduleFlushOrCompaction()
    |
    v
Return DB handle
```

### Recovery Process

1. **MANIFEST replay**: `VersionSet::Recover()` reads the MANIFEST file and applies VersionEdits to reconstruct the current Version for each column family (see [version_management.md](version_management.md)).
2. **WAL replay**: For each WAL file with number >= `min_log_number_to_keep`, replay write batches into memtables. This restores data written after the last flush.
3. **Consistency check**: Verify that all SST files referenced by the recovered Versions exist on disk.
4. **Post-recovery flush**: If `avoid_flush_during_recovery` is false, flush recovered memtables to L0.

### Open Variants

| Method | Description |
|--------|-------------|
| `DB::Open()` | Standard read-write open |
| `DB::OpenForReadOnly()` | Read-only mode (no background threads, no WAL replay) |
| `DB::OpenAsSecondary()` | Secondary instance that tails the primary's MANIFEST |
| `DB::OpenAndCompact()` | Open, compact, close (one-shot compaction) |

---

## 2. Background Operations

**Files:** `db/db_impl/db_impl_compaction_flush.cc`

### Work State Machine

Background work transitions through four states:

```
Pending/Unscheduled -> Scheduled -> Running -> Finished
```

| State | Counter | Description |
|-------|---------|-------------|
| Pending | `unscheduled_flushes_`, `unscheduled_compactions_` | Enqueued in flush_queue_ or compaction_queue_ |
| Scheduled | `bg_flush_scheduled_`, `bg_compaction_scheduled_` | Thread assigned from thread pool |
| Running | `num_running_flushes_`, `num_running_compactions_` | Thread executing |
| Finished | -- | Work complete, counters decremented |

### Scheduling

`MaybeScheduleFlushOrCompaction()` is the central scheduler, called whenever new work may be available:

```
MaybeScheduleFlushOrCompaction()
    |
    +-- While unscheduled_flushes_ > 0 and thread pool has capacity:
    |     Schedule BGWorkFlush in HIGH priority pool
    |     bg_flush_scheduled_++, unscheduled_flushes_--
    |
    +-- While unscheduled_compactions_ > 0 and thread pool has capacity:
          Schedule BGWorkCompaction in LOW priority pool
          bg_compaction_scheduled_++, unscheduled_compactions_--
```

### Flush Execution

```
BGWorkFlush() -> BackgroundCallFlush()
    |
    +-- Acquire mutex
    +-- Dequeue from flush_queue_
    +-- BackgroundFlush()
    |     +-- Create FlushJob
    |     +-- FlushJob::PickMemTable()
    |     +-- FlushJob::Run()
    |     +-- TryInstallMemtableFlushResults()
    +-- MaybeScheduleFlushOrCompaction() (more work may be available)
    +-- bg_cv_.SignalAll()
```

### Compaction Execution

```
BGWorkCompaction() -> BackgroundCallCompaction()
    |
    +-- Acquire mutex
    +-- Dequeue from compaction_queue_
    +-- BackgroundCompaction()
    |     +-- CompactionPicker::PickCompaction() (or use prepicked)
    |     +-- CompactionJob::Prepare()
    |     +-- Release mutex
    |     +-- CompactionJob::Run()
    |     +-- Acquire mutex
    |     +-- CompactionJob::Install()
    +-- MaybeScheduleFlushOrCompaction()
    +-- bg_cv_.SignalAll()
```

### Thread Pools

| Pool | Priority | Used For |
|------|----------|----------|
| HIGH | `Env::Priority::HIGH` | Flush jobs, file purging |
| LOW | `Env::Priority::LOW` | Standard compaction |
| BOTTOM | `Env::Priority::BOTTOM` | Bottom-level universal compactions |

### Work Queues

| Queue | Type | Content |
|-------|------|---------|
| `flush_queue_` | `deque<FlushRequest>` | Column families needing flush |
| `compaction_queue_` | `deque<ColumnFamilyData*>` | Column families needing compaction |
| `manual_compaction_dequeue_` | `deque<ManualCompactionState*>` | Manual compaction requests |

---

## 3. Key State Variables

### Synchronization

| Variable | Type | Purpose |
|----------|------|---------|
| `mutex_` | `InstrumentedMutex` | Primary DB mutex. Protects most mutable state |
| `wal_write_mutex_` | `InstrumentedMutex` | Protects WAL writes. Must acquire `mutex_` before `wal_write_mutex_` |
| `options_mutex_` | `InstrumentedMutex` | Guards option changes. Acquired before `mutex_` |
| `bg_cv_` | `InstrumentedCondVar` | Signaled when background work finishes, errors occur, or state changes |
| `db_lock_` | `FileLock*` | Process-level file lock on `LOCK` file |

### Core Components

| Variable | Type | Purpose |
|----------|------|---------|
| `versions_` | `unique_ptr<VersionSet>` | All Versions across all CFs, MANIFEST management |
| `column_family_memtables_` | `unique_ptr<ColumnFamilyMemTablesImpl>` | MemTable insertion routing for WriteBatch |
| `default_cf_handle_` | `ColumnFamilyHandleImpl*` | Default column family handle |
| `snapshots_` | `SnapshotList` | Doubly-linked list of live snapshots |
| `write_thread_` | `WriteThread` | Write group leader/follower coordination |
| `write_controller_` | `WriteController` | Write stall/slowdown control |
| `flush_scheduler_` | `FlushScheduler` | Tracks CFs needing flush |
| `error_handler_` | `ErrorHandler` | Background error tracking and recovery |

### File Management

| Variable | Purpose |
|----------|---------|
| `pending_outputs_` | `list<uint64_t>` -- file numbers protected from deletion during background jobs |
| `purge_files_` | Files scheduled for deletion |
| `files_grabbed_for_purge_` | File numbers claimed by a JobContext |
| `disable_delete_obsolete_files_` | Counter; >0 disables file deletion |

### Background Job Counters

| Counter | Purpose |
|---------|---------|
| `unscheduled_flushes_` | Pending flush work not yet assigned to a thread |
| `unscheduled_compactions_` | Pending compaction work not yet assigned |
| `bg_flush_scheduled_` | Flush threads scheduled/running |
| `bg_compaction_scheduled_` | Compaction threads scheduled/running |
| `bg_bottom_compaction_scheduled_` | Bottom-pool compaction threads |
| `num_running_flushes_` | Currently executing flush jobs |
| `num_running_compactions_` | Currently executing compaction jobs |
| `bg_purge_scheduled_` | File purge jobs scheduled |

---

## 4. ErrorHandler

**Files:** `db/error_handler.h`, `db/error_handler.cc`

### What It Does

Tracks and recovers from background errors (I/O failures, corruption, disk full). Supports automatic recovery for certain error types.

### Error Severity Model

| Severity | Effect | Recovery |
|----------|--------|----------|
| `kSoftError` | Background work may continue if auto-recovery enabled | Auto-recovery attempts to clear |
| `kHardError` | Background work stops | Auto-recovery via flush/compaction |
| `kFatalError` | DB operations stop | Manual intervention required |
| `kUnrecoverableError` | DB is stopped permanently | Cannot recover |

### Key Methods

| Method | Description |
|--------|-------------|
| `SetBGError(status, reason)` | Record a background error with its reason |
| `GetBGError()` | Get current background error |
| `ClearBGError()` | Clear error (REQUIRES: mutex held) |
| `RecoverFromBGError(is_manual)` | Attempt recovery |
| `IsDBStopped()` | True if error severity prevents operations |
| `IsBGWorkStopped()` | True if background work cannot proceed |
| `EnableAutoRecovery()` | Enable automatic error recovery |

### File Quarantine

When a `VersionEdit` fails to commit to MANIFEST, newly created files are "quarantined" -- protected from deletion to avoid data loss if the files were actually recorded:
- `AddFilesToQuarantine(files)` -- add file numbers to quarantine list
- `ClearFilesToQuarantine()` -- release quarantined files

---

## 5. Snapshot Management

**Files:** `db/snapshot_impl.h`

### SnapshotImpl

Each snapshot corresponds to a sequence number. Stored in a doubly-linked circular list:

```cpp
class SnapshotImpl : public Snapshot {
    SequenceNumber number_;           // const after creation
    SequenceNumber min_uncommitted_;  // for WritePrepared txn
    int64_t unix_time_;
    uint64_t timestamp_;
    bool is_write_conflict_boundary_;
    SnapshotImpl* prev_;
    SnapshotImpl* next_;
    SnapshotList* list_;
};
```

### SnapshotList

Doubly-linked circular list with a dummy head node. Snapshots are ordered by sequence number (oldest at `list_.next_`, newest at `list_.prev_`).

| Method | Description |
|--------|-------------|
| `New(s, seq, unix_time, is_conflict_boundary)` | Insert at tail (newest) |
| `Delete(s)` | Remove from list |
| `oldest()` | Get oldest snapshot |
| `newest()` | Get newest snapshot |
| `GetAll(max_seq)` | Get all snapshot seqnums up to max_seq, sorted ascending |
| `count()` | Number of live snapshots |

### Snapshot Role in Compaction

The oldest snapshot's sequence number determines which key versions can be garbage-collected during compaction. `CompactionIterator` preserves key versions visible to any snapshot in the list (see [compaction.md](compaction.md)).

### TimestampedSnapshotList

Extends snapshot management with user-defined timestamp tracking. Supports `GetTimestampedSnapshots(ts_lb, ts_ub)` for range queries.

---

## 6. InternalStats

**Files:** `db/internal_stats.h`, `db/internal_stats.cc`

### What It Does

Collects and exposes internal statistics for monitoring. Provides the data backing `DB::GetProperty()` and `DB::GetMapProperty()`.

### Per-Level Compaction Stats

```cpp
struct CompactionStats {
    uint64_t micros;            // Total time in microseconds
    uint64_t cpu_micros;        // CPU time
    uint64_t bytes_read;        // Total bytes read
    uint64_t bytes_written;     // Total bytes written
    uint64_t bytes_moved;       // Trivial move bytes
    int num_input_files_at_output_level;
    int num_output_files;
    uint64_t num_input_records;
    uint64_t num_dropped_records;
    uint64_t num_output_records;
    int count;                  // Number of compactions at this level
    // ... and more
};
```

### DB Properties

InternalStats provides data for properties like:
- `rocksdb.num-files-at-levelN` -- file count per level
- `rocksdb.stats` -- comprehensive statistics dump
- `rocksdb.cfstats` -- per-CF statistics
- `rocksdb.dbstats` -- DB-wide statistics
- `rocksdb.levelstats` -- per-level statistics
- `rocksdb.compaction-stats` -- compaction statistics
- `rocksdb.num-immutable-mem-table` -- immutable memtable count
- `rocksdb.mem-table-flush-pending` -- whether flush is pending
- `rocksdb.compaction-pending` -- whether compaction is pending
- `rocksdb.estimate-num-keys` -- estimated key count
- `rocksdb.estimate-table-readers-mem` -- table reader memory usage

---

## bg_cv_ Signal Conditions

The condition variable `bg_cv_` is signaled when:
- `bg_compaction_scheduled_` goes to 0
- Any compaction finishes (during manual compaction)
- Any compaction makes progress
- `bg_flush_scheduled_` or `bg_purge_scheduled_` decreases
- Background error occurs
- `num_running_ingest_file_` goes to 0
- `pending_purge_obsolete_files_` goes to 0
- `disable_delete_obsolete_files_` goes to 0
- Options updated successfully
- Column family dropped

---

## Key Invariants

| Invariant | Details |
|-----------|---------|
| CF in flush_queue iff pending_flush_ is true | Queue membership tracked by boolean flag |
| CF in compaction_queue iff pending_compaction_ is true | Same pattern |
| mutex_ before wal_write_mutex_ | Lock ordering to avoid deadlock |
| options_mutex_ before mutex_ | Lock ordering for option changes |
| pending_outputs_ protects files from deletion | Files with numbers >= any pending output are never deleted |
| Snapshot list is ordered by sequence number | Oldest at head, newest at tail |
| bg_cv_ signaled on all state changes | Ensures waiters are woken promptly |

## Interactions With Other Components

- **Write Path** (see [write_path.md](write_path.md)): `DBImpl::WriteImpl()` is the main entry point. Uses `WriteThread` for leader election, `WriteController` for stall management.
- **Read Path** (see [flush_and_read_path.md](flush_and_read_path.md)): `DBImpl::GetImpl()` acquires SuperVersion for consistent reads. `DBImpl::NewIterator()` creates the iterator stack.
- **Flush** (see [flush_and_read_path.md](flush_and_read_path.md)): `BackgroundFlush()` creates and runs `FlushJob`. `MaybeScheduleFlushOrCompaction()` schedules work.
- **Compaction** (see [compaction.md](compaction.md)): `BackgroundCompaction()` creates `CompactionJob`. Work is queued via `compaction_queue_`.
- **Version Management** (see [version_management.md](version_management.md)): `DBImpl` owns `VersionSet`, calls `LogAndApply()` to install new Versions.
- **Cache** (see [cache.md](cache.md)): Block cache is shared across the DB; configured at open time.
- **File I/O** (see [file_io_and_blob.md](file_io_and_blob.md)): `DBImpl` uses `Env`/`FileSystem` for all file operations. `SstFileManager` controls deletion rate.
