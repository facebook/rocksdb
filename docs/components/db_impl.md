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
| `db/db_impl/db_impl_experimental.cc` | Experimental features |
| `db/db_impl/db_impl_readonly.h` | Read-only mode |
| `db/db_impl/db_impl_secondary.h` | Secondary instance (tailing primary) |
| `db/db_impl/db_impl_follower.h` | Follower mode |
| `db/db_impl/compacted_db_impl.h` | Compacted/read-optimized DB |

---

## 1. DB Open and Recovery

**Files:** `db/db_impl/db_impl_open.cc`, `db/db_impl/db_impl_readonly.cc`, `db/db_impl/db_impl_secondary.cc`, `db/db_impl/db_impl_follower.cc`

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
    +-- Normal path: Read CURRENT file -> identify MANIFEST
    |   VersionSet::Recover() -> replay MANIFEST, reconstruct Versions
    +-- Best-efforts recovery path (if best_efforts_recovery=true):
    |   Scan DB directory for any non-empty MANIFEST (ignores CURRENT)
    |   VersionSet::TryRecover() -> tolerates missing SST files
    +-- Recover WAL files -> replay WAL entries into memtables
    +-- If needed: flush recovered memtables
    |
    v
Post-recovery steps:
    +-- Create new WAL file
    +-- Write dummy recovery marker (preserves sequence continuity)
    +-- LogAndApplyForRecovery (persist version edits to MANIFEST)
    +-- Persist OPTIONS file
    +-- Delete obsolete files, clean .trash directories
    +-- If open_files_async: schedule async file opening (HIGH pool)
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

1. **MANIFEST replay**: In the normal path, `VersionSet::Recover()` reads the CURRENT file, finds the MANIFEST, and applies VersionEdits to reconstruct the current Version for each column family. In best-efforts recovery mode (`best_efforts_recovery=true`), the CURRENT file is ignored; the code scans the DB directory for any non-empty MANIFEST file and uses `VersionSet::TryRecover()`, which tolerates missing SST files (see [version_management.md](version_management.md)).
2. **WAL replay**: For each WAL file with number >= `min_wal_number`, replay write batches into memtables. In non-2PC mode, `min_wal_number` is `max(MinLogNumberToKeep(), MinLogNumberWithUnflushedData())`. In 2PC mode, it is simply `MinLogNumberToKeep()`. Best-efforts recovery skips WAL replay entirely.
3. **Consistency check**: Verify that all SST files referenced by the recovered Versions exist on disk.
4. **Post-recovery flush**: If `avoid_flush_during_recovery` is false, flush recovered memtables to L0.

### Open Variants

| Method | File | Description |
|--------|------|-------------|
| `DB::Open()` | `db_impl_open.cc` | Standard read-write open |
| `DB::OpenForReadOnly()` | `db_impl_readonly.cc` | Read-only mode. Replays WALs into memtables (but does not flush them). No write-path background threads. Can schedule async file opening if `open_files_async=true` |
| `DB::OpenAsSecondary()` | `db_impl_secondary.cc` | Secondary instance. Performs MANIFEST recovery then WAL replay at open time. Catch-up with primary requires explicit `TryCatchUpWithPrimary()` calls (replays new MANIFEST edits and WALs) |
| `DB::OpenAsFollower()` | `db_impl_follower.cc` | Follower instance with its own directory; auto-tails leader's MANIFEST via background thread |
| `DB::OpenAndCompact()` | `db_impl_secondary.cc` | Remote compaction worker entry point. Opens as secondary (skips WAL recovery), runs a single compaction without installation, writes results to `output_directory` |
| `DB::OpenAndTrimHistory()` | `db_impl_open.cc` | Open and remove data with timestamp above a cutoff |

### DB Close / Shutdown

**Files:** `db/db_impl/db_impl.cc`

`DBImpl::Close()` coordinates a graceful shutdown:

1. **Snapshot check**: Releases timestamped snapshots, then aborts with `Status::Aborted` if any unreleased snapshots remain.
2. **Cancel background work**: Sets `shutting_down_` flag. Optionally flushes all column families (if `CancelAllBackgroundWork(flush=true)`). Disables manual compaction.
3. **Unschedule thread pool tasks**: Unschedules all pending work from BOTTOM, LOW, and HIGH thread pools.
4. **Wait for in-flight work**: Blocks until all background counters reach zero (`bg_bottom_compaction_scheduled_`, `bg_compaction_scheduled_`, `bg_flush_scheduled_`, `bg_purge_scheduled_`, async file open, error recovery, `pending_purge_obsolete_files_`).
5. **Drain queues**: Drains `flush_queue_` and `compaction_queue_`, unrefs column family data.
6. **Clean up files**: Purges obsolete files if the DB was opened successfully.
7. **Release resources**: Closes WAL writers, erases table cache entries, closes MANIFEST, resets VersionSet, unlocks the DB file lock, closes SstFileManager and directories.

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
    +-- If HIGH pool has threads:
    |     While unscheduled_flushes_ > 0 and thread pool has capacity:
    |       Schedule BGWorkFlush in HIGH priority pool
    |       bg_flush_scheduled_++, unscheduled_flushes_--
    +-- Else (HIGH pool empty):
    |     Schedule flushes in LOW priority pool (shared with compactions)
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
    |     +-- If output goes to max output level and BOTTOM pool available:
    |     |     Forward to BOTTOM pool instead
    |     +-- CompactionJob::Prepare()
    |     +-- Release mutex
    |     +-- CompactionJob::Run()
    |     +-- Acquire mutex
    |     +-- CompactionJob::Install()
    +-- MaybeScheduleFlushOrCompaction()
    +-- bg_cv_.SignalAll() (conditional: only if progress was made,
         no compactions remain, manual compaction pending,
         or unscheduled_compactions_ == 0)
```

### Thread Pools

| Pool | Priority | Used For |
|------|----------|----------|
| HIGH | `Env::Priority::HIGH` | Flush jobs, file purging, async file opening |
| LOW | `Env::Priority::LOW` | Standard compaction (also flushes when HIGH pool has 0 threads) |
| BOTTOM | `Env::Priority::BOTTOM` | Compactions whose output reaches the max output level (any compaction style, not just universal) |

### Work Queues

| Queue | Type | Content |
|-------|------|---------|
| `flush_queue_` | `deque<FlushRequest>` | Flush requests. Each `FlushRequest` contains a `cfd_to_max_mem_id_to_persist` map. In non-atomic flush mode, each request covers one CF. In atomic flush mode, a single request can cover multiple CFs |
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
| `bg_cv_` | `InstrumentedCondVar` | Signaled on specific conditions (see below), not on every state change |
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
| `kNoError` | No error | N/A |
| `kSoftError` | Background work may continue if auto-recovery enabled; some soft errors (e.g., retryable IO during flush-without-WAL) set `soft_error_no_bg_work_` which stops background work except recovery | Auto-recovery attempts to clear |
| `kHardError` | Background work stops | Auto-recovery via flush/compaction |
| `kFatalError` | DB operations stop | Manual intervention required |
| `kUnrecoverableError` | DB is stopped permanently | Cannot recover |

Note: The actual error handling behavior depends on multiple factors beyond severity, including `BackgroundErrorReason`, status code/subcode, `paranoid_checks`, and retryability. For example, retryable compaction IO errors do not set `bg_error_` at all -- the compaction self-reschedules.

### Key Methods

| Method | Description |
|--------|-------------|
| `SetBGError(status, reason, wal_related=false)` | Record a background error with its reason and optional WAL flag |
| `GetBGError()` | Get current background error |
| `ClearBGError()` | Clear error (REQUIRES: mutex held) |
| `RecoverFromBGError(is_manual)` | Attempt recovery |
| `IsDBStopped()` | True if error severity prevents operations |
| `IsBGWorkStopped()` | True if background work cannot proceed (REQUIRES: mutex held) |
| `EnableAutoRecovery()` | Enable SstFileManager-based no-space recovery. Does NOT control auto-resume for retryable IO errors (controlled by `max_bgerror_resume_count`) |

### File Quarantine

When a `VersionEdit` fails to commit to MANIFEST, newly created files are "quarantined" -- protected from deletion to avoid data loss if the files were actually recorded:
- `AddFilesToQuarantine(autovector<const autovector<uint64_t>*>)` -- add file numbers to quarantine list
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
| `New(s, seq, unix_time, is_conflict_boundary, ts=MAX)` | Insert at tail (newest). Takes optional timestamp parameter |
| `Delete(s)` | Remove from list |
| `oldest()` | Get oldest snapshot |
| `newest()` | Get newest snapshot |
| `GetAll(oldest_write_conflict_snapshot, max_seq)` | Get all snapshot seqnums up to max_seq; optionally returns oldest write-conflict snapshot. Has both return-by-value and output-parameter overloads |
| `count()` | Number of live snapshots |

### Snapshot Role in Compaction

The oldest snapshot's sequence number determines which key versions can be garbage-collected during compaction. `CompactionIterator` preserves key versions visible to any snapshot in the list (see [compaction.md](compaction.md)).

### TimestampedSnapshotList

Extends snapshot management with user-defined timestamp tracking. Key methods: `GetSnapshot(ts)` for single-timestamp lookup, `GetSnapshots(ts_lb, ts_ub, snapshots)` for range queries, `AddSnapshot(snapshot)` to insert, and `ReleaseSnapshotsOlderThan(ts, snapshots_to_release)` for cleanup.

---

## 6. InternalStats

**Files:** `db/internal_stats.h`, `db/internal_stats.cc`

### What It Does

Collects and exposes internal statistics for monitoring. Provides the data backing `DB::GetProperty()` and `DB::GetMapProperty()`.

### Per-Level Compaction Stats

```cpp
struct CompactionStats {
    uint64_t micros;                          // Total time in microseconds
    uint64_t cpu_micros;                      // CPU time
    uint64_t bytes_read_non_output_levels;    // Bytes read from non-output levels
    uint64_t bytes_read_output_level;         // Bytes read from output level
    uint64_t bytes_read_blob;                 // Bytes read from blob files
    uint64_t bytes_written;                   // Total bytes written (table files)
    uint64_t bytes_written_blob;              // Bytes written to blob files
    uint64_t bytes_moved;                     // Trivial move bytes
    int num_input_files_in_non_output_levels;
    int num_input_files_in_output_level;
    int num_output_files;
    int num_output_files_blob;
    uint64_t num_input_records;
    uint64_t num_dropped_records;
    uint64_t num_output_records;
    int count;                                // Number of compactions at this level
    int counts[static_cast<int>(CompactionReason::kNumOfReasons)];
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
| CF in flush_queue iff queued_for_flush_ is true | Queue membership tracked by boolean flag |
| CF in compaction_queue iff queued_for_compaction_ is true | Same pattern |
| mutex_ before wal_write_mutex_ | Lock ordering to avoid deadlock |
| options_mutex_ before mutex_ | Lock ordering for option changes |
| pending_outputs_ protects files from deletion | Files with numbers >= any pending output are never deleted |
| Snapshot list is ordered by sequence number | Oldest at head, newest at tail |
| bg_cv_ signaled on specific conditions | See signal conditions list above; notably NOT signaled by EnqueuePendingFlush or every state change |

## Interactions With Other Components

- **Write Path** (see [write_flow.md](write_flow.md)): `DBImpl::WriteImpl()` is the main entry point. Uses `WriteThread` for leader election, `WriteController` for stall management.
- **Read Path** (see [read_flow.md](read_flow.md)): `DBImpl::GetImpl()` acquires SuperVersion for consistent reads. `DBImpl::NewIterator()` creates the iterator stack.
- **Flush** (see [flush.md](flush.md)): `BackgroundFlush()` creates and runs `FlushJob`. `MaybeScheduleFlushOrCompaction()` schedules work.
- **Compaction** (see [compaction.md](compaction.md)): `BackgroundCompaction()` creates `CompactionJob`. Work is queued via `compaction_queue_`.
- **Version Management** (see [version_management.md](version_management.md)): `DBImpl` owns `VersionSet`, calls `LogAndApply()` to install new Versions.
- **Cache** (see [cache.md](cache.md)): Block cache is shared across the DB; configured at open time.
- **File I/O** (see [file_io.md](file_io.md)): `DBImpl` uses `Env`/`FileSystem` for all file operations. `SstFileManager` controls deletion rate.
- **Blob DB** (see [blob_db.md](blob_db.md)): Blob storage for large values, integrated with compaction and flush paths.
