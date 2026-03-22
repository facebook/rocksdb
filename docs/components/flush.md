# RocksDB Flush Subsystem

## Overview

The flush subsystem converts in-memory write buffers (MemTables) into persistent L0 SST files (and optionally blob files). This document describes flush triggers, memtable selection, the FlushJob lifecycle, atomic flush, pipelined write-flush concurrency, scheduling, commit protocol, and error handling.

### High-Level Flush Flow

```
┌────────────────────────────────────────────────────────────┐
│  FLUSH TRIGGERS                                            │
│  • write_buffer_size exceeded                              │
│  • max_write_buffer_number limit reached                   │
│  • WriteBufferManager global memory limit                  │
│  • WAL size exceeds max_total_wal_size                     │
│  • Manual flush (DB::Flush)                                │
│  • Shutdown, error recovery, external file ingestion       │
└────────────────┬───────────────────────────────────────────┘
                 │
                 v
┌────────────────────────────────────────────────────────────┐
│  SCHEDULING (db_impl_compaction_flush.cc)                 │
│  ScheduleFlushes() → SwitchMemtable()                     │
│  EnqueuePendingFlush() → MaybeScheduleFlushOrCompaction() │
└────────────────┬───────────────────────────────────────────┘
                 │
                 v
┌────────────────────────────────────────────────────────────┐
│  BACKGROUND FLUSH THREAD                                   │
│  BGWorkFlush() → BackgroundCallFlush() → BackgroundFlush()│
└────────────────┬───────────────────────────────────────────┘
                 │
                 v
┌────────────────────────────────────────────────────────────┐
│  FLUSHJOB (db/flush_job.h, db/flush_job.cc)               │
│  ┌────────────────────────────────────────────┐           │
│  │ 1. PickMemTable()                          │           │
│  │    • PickMemtablesToFlush(max_memtable_id_)│           │
│  │    • Select immutable memtables to flush   │           │
│  │    • Mark flush_in_progress_ = true        │           │
│  └────────────────────────────────────────────┘           │
│  ┌────────────────────────────────────────────┐           │
│  │ 2. Run()                                   │           │
│  │    • Try MemPurge (experimental GC)        │           │
│  │    • Fall back to WriteLevel0Table()       │           │
│  │      - Release db_mutex                    │           │
│  │      - Create iterators over memtables     │           │
│  │      - BuildTable() → output SST file      │           │
│  │      - Re-acquire db_mutex                 │           │
│  │      - TryInstallMemtableFlushResults()    │           │
│  │        (called on MemTableList, not a     │           │
│  │         FlushJob method)                  │           │
│  └────────────────────────────────────────────┘           │
│  ┌────────────────────────────────────────────┐           │
│  │ 3. Caller: Install results                │           │
│  │    • Commit VersionEdit to MANIFEST        │           │
│  │    • Remove flushed memtables from imm     │           │
│  │    • Install new SuperVersion (in caller   │           │
│  │      FlushMemTableToOutputFile, not in     │           │
│  │      TryInstallMemtableFlushResults)       │           │
│  └────────────────────────────────────────────┘           │
└────────────────┬───────────────────────────────────────────┘
                 │
                 v
┌────────────────────────────────────────────────────────────┐
│  FLUSH OUTPUT                                              │
│  • L0 SST files (overlapping key ranges within and across) │
│  • Blob files (when BlobDB / integrated blob storage is    │
│    enabled)                                                │
│  • No output file if MemPurge succeeds or if flush output  │
│    is empty (meta_.fd.GetFileSize() == 0)                  │
│  • Triggers compaction when L0 file count is high          │
└────────────────────────────────────────────────────────────┘
```

---

## 1. Flush Triggers

**Files:** `include/rocksdb/options.h`, `include/rocksdb/listener.h:167`

Flushes are triggered by multiple conditions, categorized by `FlushReason`:

| FlushReason | Description | Trigger Condition |
|-------------|-------------|-------------------|
| `kWriteBufferFull` | MemTable size exceeds limit | `ShouldFlushNow()` returns true: arena-allocation heuristic based on `write_buffer_size`, explicit `MarkForFlush()` (e.g., from iterators), or `memtable_max_range_deletions` exceeded |
| `kWriteBufferManager` | Global memory limit | Total memory across all CFs ≥ `db_write_buffer_size` or `WriteBufferManager` limit |
| `kWalFull` | WAL size limit | Total WAL size ≥ `max_total_wal_size` |
| `kManualFlush` | User-initiated | `DB::Flush()` called |
| `kShutDown` | Database closing | `DB::Close()` or destructor |
| `kExternalFileIngestion` | Ingest files to L0 | Before ingesting external SST files |
| `kErrorRecovery` | Recovering from error | ErrorHandler recovery sequence |
| `kGetLiveFiles` | Get live files snapshot | `DB::GetLiveFiles()` requires flushing |
| `kManualCompaction` | Manual compaction needs flush | Before manual compaction if memtable has data |
| `kDeleteFiles` | Delete files operation | `DB::DeleteFile()` requires flushing |
| `kErrorRecoveryRetryFlush` | Retry flush during recovery | ErrorHandler retries failed flush |
| `kCatchUpAfterErrorRecovery` | Catch up after recovery | Flush CFs that advanced during recovery |

### Key Configuration Parameters

```cpp
// include/rocksdb/options.h

// Per-CF limit: flush when active memtable exceeds this size
size_t write_buffer_size = 64 << 20;  // 64 MB

// Max number of write buffers (active + immutable) in memory
int max_write_buffer_number = 2;

// Global limit: flush across all CFs when total exceeds this
size_t db_write_buffer_size = 0;  // 0 = disabled

// WAL size limit: flush oldest-WAL column families when exceeded.
// Only takes effect with more than one column family.
// 0 = dynamic limit (4 * total in-memory write buffer size)
uint64_t max_total_wal_size = 0;
```

⚠️ **NOTE:** `max_write_buffer_number` is the total number of write buffers (active + immutable) allowed in memory. The default of 2 allows one active memtable and one immutable memtable being flushed concurrently. A value of 1 is accepted but means writes block during every flush.

### Flush Decision Logic

**Files:** `db/db_impl/db_impl_write.cc:2420-2472` (`DBImpl::ScheduleFlushes`)

When a write triggers a flush:

1. **MemTable full check:** During memtable insertion, `MemTable::Add()` calls `UpdateFlushState()` which evaluates `ShouldFlushNow()`. Then `WriteBatchInternal::MemTableInserter::CheckMemtableFull()` checks `ShouldScheduleFlush()` and enqueues the CF to `flush_scheduler_`.
2. **Drain scheduler:** In `PreprocessWrite()`, `flush_scheduler_` is drained if non-empty, calling `ScheduleFlushes()`.
3. **Switch memtable:** `ScheduleFlushes()` calls `SwitchMemtable(cfd)` to freeze the active memtable and create a new one.
4. **Enqueue flush request:** `GenerateFlushRequest()` + `EnqueuePendingFlush()` adds the flush to `unscheduled_flushes_`.
5. **Schedule background work:** `MaybeScheduleFlushOrCompaction()` schedules `BGWorkFlush()` on the HIGH priority thread pool.

---

## 2. PickMemtablesToFlush: Memtable Selection

**Files:** `db/memtable_list.h:290`, `db/memtable_list.cc:410-463`

`MemTableList::PickMemtablesToFlush()` selects which immutable memtables to flush. Called by `FlushJob::PickMemTable()` with `max_memtable_id` as the upper bound.

### Selection Algorithm

```cpp
void MemTableList::PickMemtablesToFlush(uint64_t max_memtable_id,
                                        autovector<ReadOnlyMemTable*>* ret,
                                        uint64_t* max_next_log_number);
```

**Iteration order:** Iterates the immutable memtable list (`memlist_`) in **reverse** (oldest first). Since `MemTableList::Add()` inserts at the front (push_front), reverse iteration yields memtables sorted by increasing ID.

**Selection criteria (for each memtable `m`):**

1. **ID check:** `m->GetID() > max_memtable_id` → stop (reached newer memtables not yet eligible)
2. **Flush status:** `m->flush_in_progress_ == true` → skip (already flushing)
3. **Consecutive requirement:** If `flush_in_progress_ == true` and `ret` is non-empty → break (prevent non-consecutive selection after parallel flush rollback)

**State updates:**

- Mark `m->flush_in_progress_ = true`
- Decrement `num_flush_not_started_`
- If `num_flush_not_started_` reaches 0, set `imm_flush_needed.store(false)` (no more pending flushes)
- Append `m` to `ret` (output vector is oldest-first)

⚠️ **INVARIANT:** Selected memtables must be consecutive in the immutable list. Non-consecutive selection can break FIFO flush ordering.

⚠️ **INVARIANT:** Memtables in `ret` are ordered by increasing memtable ID (oldest first). This ensures L0 files are created in chronological order.

### Atomic Flush Handling

When `atomic_flush_seqno_ != kMaxSequenceNumber` is detected, the `atomic_flush` flag is set. At the end:

```cpp
if (!atomic_flush || num_flush_not_started_ == 0) {
  flush_requested_ = false;  // Complete the flush request
}
```

For atomic flush, `flush_requested_` is only cleared for this CF's immutable list when `num_flush_not_started_ == 0` for that CF (it is a per-`MemTableList` counter, not a global check across all CFs).

---

## 3. FlushJob Lifecycle

**Files:** `db/flush_job.h`, `db/flush_job.cc`

A `FlushJob` instance handles flushing one column family. It has a three-phase lifecycle:

| Phase | Mutex Held | Description |
|-------|------------|-------------|
| **PickMemTable()** | Yes | Select memtables, allocate file number, ref `Version` |
| **Run()** | Yes (released during I/O) | Execute MemPurge or WriteLevel0Table, then install results |
| **Cancel()** | Yes | Abort flush, unref `Version` (`base_->Unref()`). Does **not** rollback memtable flush flags; rollback is done by `RollbackMemtableFlush()` in `Run()` on error |

⚠️ **INVARIANT:** Once `PickMemTable()` is called, either `Run()` or `Cancel()` **must** be called. Failure to do so leaks the ref'd `Version` and leaves memtables in `flush_in_progress_` state.

### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `mems_` | `autovector<ReadOnlyMemTable*>` | Memtables selected for flush (oldest first) |
| `meta_` | `FileMetaData` | Metadata for the output SST file |
| `edit_` | `VersionEdit*` | From first memtable, records new L0 file |
| `base_` | `Version*` | Ref'd Version at flush time (for iterator creation) |
| `max_memtable_id_` | `uint64_t` | Upper bound for memtable selection |
| `flush_reason_` | `FlushReason` | Why this flush was triggered |

### PickMemTable Phase

```cpp
void FlushJob::PickMemTable() {
  db_mutex_->AssertHeld();
  assert(!pick_memtable_called);
  pick_memtable_called = true;

  uint64_t max_next_log_number = 0;
  cfd_->imm()->PickMemtablesToFlush(max_memtable_id_, &mems_,
                                    &max_next_log_number);
  if (mems_.empty()) {
    return;  // No memtables to flush
  }

  GetEffectiveCutoffUDTForPickedMemTables();  // User-defined timestamp handling
  GetPrecludeLastLevelMinSeqno();             // Tiering support
  ReportFlushInputSize(mems_);

  // Use first memtable's edit_ to record flush output
  edit_ = mems_[0]->GetEdits();
  edit_->SetPrevLogNumber(0);
  edit_->SetLogNumber(max_next_log_number);

  // Allocate file number for output SST
  meta_.fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);

  // Ref current Version for creating iterators
  base_ = cfd_->current();
  base_->Ref();
}
```

**Key actions:**

1. Call `PickMemtablesToFlush()` to populate `mems_`
2. Use first memtable's `VersionEdit` as `edit_` (all memtables in the batch contribute to this single edit)
3. Allocate a new file number via `versions_->NewFileNumber()`
4. Ref the current `Version` (needed for safe iterator creation outside the mutex)

### Run Phase: MemPurge vs. WriteLevel0Table

```cpp
Status FlushJob::Run(LogsWithPrepTracker* prep_tracker,
                     FileMetaData* file_meta,
                     bool* switched_to_mempurge,
                     bool* skipped_since_bg_error,
                     ErrorHandler* error_handler) {
  db_mutex_->AssertHeld();

  // Try MemPurge (experimental in-memory GC)
  // Inline condition (no ShouldAttemptMemPurge() helper):
  //   mempurge_threshold > 0.0 && flush_reason == kWriteBufferFull
  //   && !mems_.empty() && !atomic_flush && MemPurgeDecider(threshold)
  if (mempurge_eligible) {
    Status s = MemPurge();
    if (s.ok()) {
      *switched_to_mempurge = true;
      base_->Unref();
      // Still calls TryInstallMemtableFlushResults(..., write_edits=false)
      // to remove old memtables and install the mempurged memtable
      s = cfd_->imm()->TryInstallMemtableFlushResults(..., write_edits=false);
      return s;
    }
    // MemPurge failed, fall through to WriteLevel0Table
  }

  // Standard flush path: write SST file
  Status s = WriteLevel0Table();
  if (!s.ok()) {
    return s;
  }

  // Install flush results
  s = cfd_->imm()->TryInstallMemtableFlushResults(...);
  return s;
}
```

#### MemPurge (Experimental)

**Files:** `db/flush_job.cc:MemPurge()`, `db/flush_job.h:106-125`

MemPurge is an experimental in-memory garbage collection feature. It attempts to compact the memtable in place by filtering out obsolete entries (overwritten keys, deleted keys) and inserting survivors into a new memtable. If the output fits in one memtable, it replaces the original on the immutable list **without producing an SST file**.

**Conditions for attempting MemPurge:**

1. `experimental_mempurge_threshold > 0.0`
2. `flush_reason == kWriteBufferFull` (not manual flush, shutdown, etc.)
3. `!mems_.empty()`
4. `!atomic_flush` (MemPurge incompatible with atomic flush)
5. `MemPurgeDecider(threshold)` returns true (heuristic based on useful payload ratio)

**MemPurge algorithm:**

1. Create a `CompactionIterator` over the memtable with snapshot filtering
2. Filter entries: drop tombstones, overwritten versions, expired data
3. Insert survivors into a new memtable
4. If output memtable size < `write_buffer_size`, replace the original immutable memtable
5. Else, fall back to `WriteLevel0Table()`

⚠️ **INVARIANT:** MemPurge does not change the on-disk state. It only affects the in-memory immutable list. If MemPurge fails (output too large), the flush continues normally.

### WriteLevel0Table: Building the SST File

**Files:** `db/flush_job.cc:WriteLevel0Table()`, `db/builder.h`, `db/builder.cc`

The core flush work: convert memtables to an L0 SST file.

**Execution flow:**

```
WriteLevel0Table()
    |
    1. Release db_mutex (I/O cannot hold mutex)
    |
    2. Create iterators:
    |    • Point iterators (one per memtable via m->NewIterator())
    |    • Range tombstone iterators (via m->NewRangeTombstoneIterator())
    |    • (iterators created directly on each memtable, not via AddIterators helpers)
    |
    3. Merge via MergingIterator (memtables already sorted by key)
    |
    4. BuildTable(iterators, &meta_, ...)
    |    • Opens TableBuilder (via configured table_factory->NewTableBuilder())
    |    • Iterates merged stream, calls builder->Add(key, value)
    |    • Finalizes: builder->Finish(), file->Sync(), file->Close()
    |    • Populates meta_ with file size, key range, seqno range
    |    • May also create blob files when BlobDB is enabled
    |
    5. Re-acquire db_mutex
    |
    6. Update edit_:
    |    • edit_->AddFile(level=0, meta_)
    |    • (edit_->SetLogNumber() already set in PickMemTable(), not here)
```

**Key concurrency detail:** The mutex is released during `BuildTable()` to allow concurrent writes. Other threads can:

- Insert into the active memtable
- Switch memtables and create new immutable memtables
- Schedule other flushes

**WAL Sync Step:** For multi-CF DBs and/or 2PC, RocksDB syncs closed WALs before flush (`SyncClosedWals()`) and may apply WAL-tracking edits to the MANIFEST so persisted WAL state is at least as new as persisted SST state. This happens in `FlushMemTableToOutputFile()` / `AtomicFlushMemTablesToOutputFiles()` before `FlushJob::Run()` is called.

⚠️ **INVARIANT:** While the mutex is released during `WriteLevel0Table()`, the memtables in `mems_` are marked `flush_in_progress_ = true` and ref'd, preventing deletion or modification.

⚠️ **INVARIANT:** `BuildTable()` must populate `meta_` with accurate `smallest`, `largest`, `fd.smallest_seqno`, `fd.largest_seqno`. These are critical for iterator positioning and tombstone truncation.

### Rate Limiting

**Files:** `db/flush_job.cc:GetRateLimiterPriority()`

```cpp
Env::IOPriority FlushJob::GetRateLimiterPriority() {
  if (versions_ && versions_->GetColumnFamilySet() &&
      versions_->GetColumnFamilySet()->write_controller()) {
    WriteController* write_controller =
        versions_->GetColumnFamilySet()->write_controller();
    if (write_controller->IsStopped() || write_controller->NeedsDelay()) {
      return Env::IO_USER;  // Higher priority when writes are stalled/delayed
    }
  }
  return Env::IO_HIGH;  // Default: high priority
}
```

The rate limiter priority determines how flush I/O competes with compaction and user reads. Flush I/O is `IO_HIGH` by default, but escalates to `IO_USER` when the write controller indicates writes are stopped or delayed, prioritizing flush to relieve write pressure.

---

## 4. Atomic Flush

**Files:** `db/memtable_list.h:535`, `db/memtable_list.cc:858`, `db/db_impl/db_impl_compaction_flush.cc:759`

Atomic flush ensures that multiple column families are flushed **atomically**: either all CFs' flush results are visible, or none are. This is critical for cross-CF consistency (e.g., in transactions).

### Configuration

```cpp
// include/rocksdb/options.h
bool atomic_flush = false;
```

When `atomic_flush = true`:

- `SelectColumnFamiliesForAtomicFlush()` selects CFs to flush, which can include CFs with non-empty mutable memtables (not just those with pending immutable memtables)
- Each request captures a per-CF `max_memtable_id` so newer memtables created later are intentionally excluded
- A single `VersionEdit` batch is written to the MANIFEST
- Either all flushes succeed and are visible, or none are

### Atomic Flush Workflow

```cpp
// db/db_impl/db_impl_compaction_flush.cc:AtomicFlushMemTablesToOutputFiles()
// (NOT inlined in BackgroundFlush; called as a separate function)

Status DBImpl::AtomicFlushMemTablesToOutputFiles(...) {
  autovector<ColumnFamilyData*> cfds;
  autovector<FlushJob*> jobs;

  // Phase 1: PickMemTable for all CFs
  for (auto cfd : cfds) {
    jobs.push_back(new FlushJob(...));
    jobs.back()->PickMemTable();
  }

  // Phase 2: Run all flush jobs sequentially on one thread
  // (TODO: parallelize jobs with threads -- not yet implemented)
  for (auto job : jobs) {
    s = job->Run(...);
    if (!s.ok()) break;
  }

  // Phase 3: Wait for turn to commit (ordered by atomic_flush_install_cv_)
  // then install results atomically
  if (s.ok()) {
    s = InstallMemtableAtomicFlushResults(...);
    // SuperVersion installation happens here in the caller,
    // after InstallMemtableAtomicFlushResults() returns
    for (auto cfd : cfds) {
      InstallSuperVersionAndScheduleWork(cfd, ...);
    }
  }
}
```

### InstallMemtableAtomicFlushResults

**Files:** `db/memtable_list.cc:858`

```cpp
Status InstallMemtableAtomicFlushResults(
    const autovector<MemTableList*>* imm_lists,
    const autovector<ColumnFamilyData*>& cfds,
    const autovector<const autovector<ReadOnlyMemTable*>*>& mems_list,
    VersionSet* vset,
    LogsWithPrepTracker* prep_tracker,
    InstrumentedMutex* mu,
    const autovector<FileMetaData*>& file_metas,
    const autovector<std::list<std::unique_ptr<FlushJobInfo>>*>&
        committed_flush_jobs_info,
    autovector<ReadOnlyMemTable*>* to_delete,
    FSDirectory* db_directory,
    LogBuffer* log_buffer);
```

**Algorithm:**

1. **Mark all memtables as `flush_completed_ = true`**
2. **Collect all completed flush edits:** Iterate all CFs and gather `VersionEdit`s from memtables where `flush_completed_ == true`
3. **Single MANIFEST write:** Call `vset->LogAndApply(all_edits)` atomically
4. **Remove flushed memtables:** For each CF, remove memtables from the immutable list and add to `to_delete`
5. **Return to caller:** SuperVersion installation happens in the caller (`AtomicFlushMemTablesToOutputFiles`) via `InstallSuperVersionAndScheduleWork()` for each CF

**Note:** Ordering for atomic install is handled by the caller waiting on `atomic_flush_install_cv_` until its batch is the oldest pending. `InstallMemtableAtomicFlushResults()` does **not** check `commit_in_progress` or install SuperVersions itself.

⚠️ **INVARIANT:** All CFs in an atomic flush must commit together via a single `LogAndApply()` call. Partial commits violate atomicity.

⚠️ **INVARIANT:** If any flush job fails in step 2, **all** flush jobs must be rolled back via `RollbackMemtableFlush()`, and no `VersionEdit` is written.

---

## 5. Pipelined Write and Flush

RocksDB allows **concurrent writes to the active memtable** while a flush is executing. This pipelining is enabled by `max_write_buffer_number ≥ 2`.

### Pipeline Flow

```
Time  │ Active MemTable │ Immutable MemTables │ Flush Job
──────┼─────────────────┼────────────────────┼──────────────────
  t0  │ mem_A (writing) │ -                  │ -
      │                 │                    │
  t1  │ mem_B (writing) │ mem_A (immutable)  │ FlushJob(mem_A) starting
      │                 │                    │ • PickMemTable()
      │                 │                    │ • Run() → release mutex
  t2  │ mem_B (writing) │ mem_A              │ FlushJob(mem_A) in BuildTable()
      │                 │                    │ • mutex released
      │                 │                    │ • writing SST file
  t3  │ mem_C (writing) │ mem_B, mem_A       │ FlushJob(mem_A) in BuildTable()
      │                 │                    │
  t4  │ mem_C (writing) │ mem_B              │ FlushJob(mem_A) completed
      │                 │                    │ • mem_A deleted
      │                 │                    │
  t5  │ mem_C (writing) │ mem_B              │ FlushJob(mem_B) starting
```

**Key observations:**

- At `t1`, writes switch to `mem_B` while `mem_A` is flushing
- At `t2`, the flush thread releases the mutex and performs I/O. Writes continue to `mem_B`
- At `t3`, if `mem_B` fills, writes switch to `mem_C`. Now there are 2 immutable memtables: `mem_B` (pending flush) and `mem_A` (flushing)

⚠️ **INVARIANT:** If `max_write_buffer_number = N`, then at most `N - 1` memtables can be immutable (flushing or pending flush) at any time. Write stall checks count active (if non-empty) + immutable memtables via `GetUnflushedMemTableCountForWriteStallCheck()`. When this count reaches `N`, writes **stall** until flush completes.

### Write Stall Conditions

**Files:** `db/write_controller.h`, `db/write_controller.cc`, `db/column_family.cc:992-1027`

The `WriteController` enforces stalls and slowdowns when flush/compaction falls behind:

| Condition | Action |
|-----------|--------|
| `num_unflushed_memtables ≥ max_write_buffer_number` | **STOP**: Block writes until flush completes. `num_unflushed_memtables` = active (if non-empty) + immutable count via `GetUnflushedMemTableCountForWriteStallCheck()` |
| `max_write_buffer_number > 3` AND<br>`num_unflushed_memtables ≥ max_write_buffer_number - 1` AND<br>`num_unflushed_memtables - 1 ≥ min_write_buffer_number_to_merge` | **DELAY**: Slow down writes to `delayed_write_rate` |
| `num_l0_files ≥ level0_stop_writes_trigger` (default: 36) | **STOP**: Block writes (only when `disable_auto_compactions` is false) |
| `num_l0_files ≥ level0_slowdown_writes_trigger` (default: 20) | **DELAY**: Slow down writes (only when `disable_auto_compactions` is false) |
| `pending_compaction_bytes ≥ hard_pending_compaction_bytes_limit` | **STOP**: Block writes (only when `disable_auto_compactions` is false) |
| `pending_compaction_bytes ≥ soft_pending_compaction_bytes_limit` | **DELAY**: Slow down writes (only when `disable_auto_compactions` is false) |

⚠️ **NOTE:** Memtable-based write delay only activates when `max_write_buffer_number > 3`. With the default value of 2, only STOP occurs when both buffers are full.

---

## 6. Flush Scheduling

**Files:** `db/db_impl/db_impl_compaction_flush.cc:3021-3115` (`MaybeScheduleFlushOrCompaction`)

Flush scheduling coordinates background flush threads.

### Scheduling Flow

```
ScheduleFlushes()
    |
    v
SwitchMemtable(cfd)  // Freeze active memtable, create new one
    |
    v
EnqueuePendingFlush(flush_req)  // Increment unscheduled_flushes_
    |
    v
MaybeScheduleFlushOrCompaction()
    |
    +--[Checks]
    |  • opened_successfully_?
    |  • bg_work_paused_?
    |  • error_handler_.IsBGWorkStopped()?
    |  • shutting_down_?
    |
    +--[Schedule flush threads]
       While (unscheduled_flushes_ > 0 &&
              bg_flush_scheduled_ < max_flushes):
         bg_flush_scheduled_++
         env_->Schedule(&DBImpl::BGWorkFlush, ..., Env::Priority::HIGH)
         unscheduled_flushes_--
```

### Key Counters

| Counter | Description |
|---------|-------------|
| `unscheduled_flushes_` | Number of pending flush requests not yet assigned to a thread |
| `bg_flush_scheduled_` | Number of flush threads scheduled (may not be running yet) |
| `num_running_flushes_` | Number of flush threads actively running |

⚠️ **INVARIANT:** `bg_flush_scheduled_ ≤ max_background_flushes` (or `max_background_jobs / 4` if using unified job limit).

### Thread Pool Selection

Flush threads are scheduled on the **HIGH priority** thread pool by default. If the HIGH pool is empty (size = 0), flushes fall back to the **LOW priority** pool (shared with compaction).

### UDT Retention and Flush Rescheduling

When user-defined timestamps (UDT) are enabled with retention, non-atomic flush requests may be rescheduled instead of executed immediately. If the background flush determines that flushing would violate UDT retention guarantees, it returns `Status::TryAgain()`, re-enqueues the flush request, and sleeps briefly to avoid a hot retry loop.

```cpp
bool is_flush_pool_empty = env_->GetBackgroundThreads(Env::Priority::HIGH) == 0;

if (!is_flush_pool_empty) {
  // Schedule on HIGH priority pool
  env_->Schedule(&DBImpl::BGWorkFlush, fta, Env::Priority::HIGH, ...);
} else {
  // Fallback to LOW priority pool
  env_->Schedule(&DBImpl::BGWorkFlush, fta, Env::Priority::LOW, ...);
}
```

---

## 7. Flush Commit: TryInstallMemtableFlushResults

**Files:** `db/memtable_list.cc:522-627`

After `WriteLevel0Table()` completes, the flush must be committed: write the `VersionEdit` to the MANIFEST and remove flushed memtables from the immutable list.

### Commit Protocol

```cpp
Status MemTableList::TryInstallMemtableFlushResults(
    ColumnFamilyData* cfd,
    const autovector<ReadOnlyMemTable*>& mems,
    LogsWithPrepTracker* prep_tracker,
    VersionSet* vset, InstrumentedMutex* mu,
    uint64_t file_number,
    autovector<ReadOnlyMemTable*>* to_delete,
    FSDirectory* db_directory, LogBuffer* log_buffer,
    std::list<std::unique_ptr<FlushJobInfo>>* committed_flush_jobs_info,
    bool write_edits);
```

**Algorithm:**

1. **Mark completion:** For each memtable in `mems`, set `flush_completed_ = true` and `file_number_ = file_number`
2. **Check commit_in_progress:** If true, return immediately (another thread is committing; this flush's results will be picked up)
3. **Set commit_in_progress = true:** Only one thread can commit at a time
4. **Retry loop:** Commit all completed flushes, including ones that finished while this thread was writing the MANIFEST
   ```cpp
   while (s.ok()) {
     autovector<VersionEdit*> batch_edits;
     autovector<ReadOnlyMemTable*> memtables_to_flush;

     // Collect all completed flushes (may include other threads' results)
     for (auto m : memlist_) {
       if (!m->flush_completed_) break;  // FIFO: stop at first incomplete
       batch_edits.push_back(m->GetEdits());
       memtables_to_flush.push_back(m);
     }
     if (memtables_to_flush.empty()) break;  // All done

     // Write to MANIFEST
     if (write_edits) {
       s = vset->LogAndApply(cfd, mutable_cf_options, read_options,
                             write_options, batch_edits, mu, db_directory);
     }

     // Remove from immutable list
     for (auto m : memtables_to_flush) {
       memlist_.remove(m);
       to_delete->push_back(m);
     }
   }
   commit_in_progress_ = false;
   ```

⚠️ **INVARIANT (FIFO Ordering):** Memtables must be installed in the order they were created. Even if memtable `N+1` finishes flushing before memtable `N`, it **waits** until `N` completes. The loop breaks at the first `!flush_completed_` memtable, enforcing FIFO.

⚠️ **INVARIANT (Single Committer):** Only one thread executes the commit loop at a time (`commit_in_progress_` serializes access). This prevents interleaved MANIFEST writes and ensures atomic batching of multiple completed flushes.

### MANIFEST Write

The `VersionEdit` is persisted via `VersionSet::LogAndApply()`:

```cpp
s = vset->LogAndApply(cfd, mutable_cf_options, read_options, write_options,
                      batch_edits, mu, db_directory);
```

This writes a MANIFEST record containing:

- Column family ID
- New L0 file metadata: file number, size, key range, sequence number range
- Log number update (WAL segments older than this can be deleted)

⚠️ **INVARIANT:** `LogAndApply()` is atomic with respect to crash recovery. If RocksDB crashes before the MANIFEST write completes, the flush never happened (memtables are replayed from WAL on recovery).

---

## 8. SuperVersion Installation

**Files:** `db/db_impl/db_impl_compaction_flush.cc:InstallSuperVersionAndScheduleWork()`

After the flush commits, a new `SuperVersion` is installed to make the new L0 file visible to readers.

### SuperVersion Structure

A `SuperVersion` bundles a consistent snapshot of:

- `mem`: Current active memtable
- `imm`: Immutable memtable list version
- `current`: Current `Version` (set of SST files)

Readers acquire a ref to the `SuperVersion`, ensuring they see a consistent view even as flushes/compactions modify the underlying state.

### Installation Sequence

```
TryInstallMemtableFlushResults()   // MemTableList method
    |
    v
vset->LogAndApply()  // Update Version (add new L0 file)
    |
    v
VersionSet::AppendVersion(new_version)
    |
    v
Return to caller (FlushMemTableToOutputFile / AtomicFlushMemTablesToOutputFiles)
    |
    v
InstallSuperVersionAndScheduleWork(cfd, ...)   // Called by the CALLER, not by TryInstall
    |
    +-- Create new SuperVersion:
    |     sv = new SuperVersion(mem, imm->current_, cfd->current())
    |
    +-- Swap:
    |     old_sv = cfd->InstallSuperVersion(sv, ...)
    |
    +-- Cleanup:
          old_sv->Unref()  // Async delete when all readers release
```

⚠️ **INVARIANT:** `SuperVersion` installation must happen **after** `LogAndApply()` succeeds. If installation happens first, readers might observe the new L0 file before it's durable in the MANIFEST, risking data loss on crash.

---

## 9. Flush Rate Limiting and WriteController

**Files:** `db/write_controller.h`, `db/write_controller.cc`, `db/flush_job.cc:GetRateLimiterPriority()`

The `WriteController` dynamically adjusts write throughput based on flush and compaction backlog.

### Write Stall Triggers

| Condition | Stall Type | Description |
|-----------|------------|-------------|
| `num_unflushed ≥ max_write_buffer_number` | **STOP** | All write buffer slots full; writes blocked until flush completes. Count uses `GetUnflushedMemTableCountForWriteStallCheck()` (active if non-empty + immutable) |
| `num_unflushed ≥ max_write_buffer_number - 1` (when `max_write_buffer_number > 3`) | **DELAY** | One slot left; writes artificially delayed to slow ingestion |
| `l0_files ≥ level0_stop_writes_trigger` | **STOP** | Too many L0 files; compaction cannot keep up (disabled when `disable_auto_compactions` is true) |
| `l0_files ≥ level0_slowdown_writes_trigger` | **DELAY** | L0 nearing limit; slow down to give compaction time (disabled when `disable_auto_compactions` is true) |
| `pending_compaction_bytes ≥ hard limit` | **STOP** | Too much pending compaction work (disabled when `disable_auto_compactions` is true) |
| `pending_compaction_bytes ≥ soft limit` | **DELAY** | Approaching pending compaction limit (disabled when `disable_auto_compactions` is true) |

### Flush Urgency: Parallelizing Compaction

When the write controller detects compaction backlog, it increases background job parallelism:

```cpp
bool need_speedup = write_controller_.NeedSpeedupCompaction();
auto limits = GetBGJobLimits(max_background_flushes,
                             max_background_compactions,
                             max_background_jobs,
                             need_speedup);
```

If `need_speedup = true`, `max_compactions` is increased (instead of being throttled to 1).

---

## 10. Error Handling During Flush

**Files:** `db/error_handler.h`, `db/error_handler.cc`, `db/flush_job.cc:Run()`

Flush errors fall into two categories:

### I/O Errors (Recoverable)

If `WriteLevel0Table()` fails due to I/O error (disk full, corruption):

1. **Rollback memtable state:** `MemTableList::RollbackMemtableFlush(mems)` resets `flush_in_progress_ = false` and increments `num_flush_not_started_`
2. **Set background error:** `ErrorHandler::SetBGError(s)` records the error
3. **Pause background work:** `bg_work_paused_` is set to prevent new flushes/compactions
4. **Notify user:** EventLogger fires `OnBackgroundError()`
5. **Recovery attempt:** ErrorHandler may retry the flush or enter read-only mode depending on configuration

### Hard Errors (Unrecoverable)

Hard errors (MANIFEST corruption, critical metadata loss) trigger:

1. **Set `bg_error_`:** `DBImpl::bg_error_` is set to the error status
2. **Stop all writes:** `WriteController::SetStopped()` blocks all Put/Delete/Write calls
3. **Read-only mode:** Database becomes read-only; manual intervention required

⚠️ **INVARIANT:** On flush error, memtables are **never deleted** until a successful retry. They remain in the immutable list, and WAL segments are retained. This ensures data is not lost.

---

## Key Invariants Summary

| Invariant | Details |
|-----------|---------|
| **FIFO Flush Ordering** | Memtable `N` must be installed before `N+1`, even if `N+1` finishes first. Enforced by `TryInstallMemtableFlushResults()` breaking at first `!flush_completed_` entry. |
| **Consecutive Selection** | `PickMemtablesToFlush()` selects consecutive memtables; breaks on first `flush_in_progress_ == true` if `ret` is non-empty. |
| **Memtable Ordering** | Selected memtables are sorted by increasing ID (oldest first). L0 files inherit this chronological order. |
| **PickMemTable → Run/Cancel** | Once `PickMemTable()` is called, either `Run()` or `Cancel()` must follow. Failure to do so leaks `Version` ref and corrupts flush state. |
| **Atomic LogAndApply** | `LogAndApply()` is atomic w.r.t. crash recovery. Partial MANIFEST writes are unobservable. |
| **SuperVersion After MANIFEST** | `SuperVersion` installation happens **after** `LogAndApply()` succeeds, ensuring readers never see uncommitted files. |
| **Single Committer** | Only one thread executes the commit loop in `TryInstallMemtableFlushResults()` at a time (`commit_in_progress_` serialization). |
| **Flush Without Deletion** | On flush error, memtables are not deleted until successful retry. WAL segments are retained. |
| **Mutex Release During I/O** | `db_mutex_` is released during `BuildTable()` I/O to allow concurrent writes. Memtables in `mems_` are ref'd and marked `flush_in_progress_` to prevent premature deletion. |
| **max_write_buffer_number** | Total write buffers (active + immutable) allowed in memory. Default 2 allows pipelined write and flush. Value of 1 is accepted but writes block during every flush. |

---

## Interactions With Other Components

- **Write Path** (see [write_flow.md](write_flow.md)): Writes trigger flush via `PreprocessWrite()` → `ScheduleFlushes()`. `WriteController` stalls writes when flush falls behind.
- **Version Management** (see [version_management.md](version_management.md)): Flush creates `VersionEdit`, calls `LogAndApply()` to persist to MANIFEST. `SuperVersion` is installed to make new L0 file visible.
- **SST Format** (see [sst_table_format.md](sst_table_format.md)): `BuildTable()` uses the configured `table_factory` (via `NewTableBuilder()`) to write L0 SST files. Key range and seqno range are recorded in `FileMetaData`.
- **Compaction** (see [compaction.md](compaction.md)): Flush produces L0 files that trigger compaction. L0 → L1 compaction merges overlapping L0 files.
- **WAL** (see [write_flow.md](write_flow.md)): Flush allows WAL segments to be deleted. `edit_->SetLogNumber(max_next_log_number)` marks the new min log to keep.
- **MemTable** (see [write_flow.md](write_flow.md)): Flush iterates memtable via `MemTable::NewIterator()`. MemTable provides sorted key-value stream.
- **Cache** (see [cache.md](cache.md)): After flush, new L0 file's data blocks are not yet cached. First reads will populate block cache.
- **DBImpl** (see [db_impl.md](db_impl.md)): `DBImpl` owns flush scheduling, background thread pool, and `ErrorHandler` for flush failure recovery.

---

## Flush Metrics and Observability

**Files:** `db/internal_stats.h`, `include/rocksdb/listener.h`

Key metrics for flush monitoring:

| Metric | Description | Source |
|--------|-------------|--------|
| `rocksdb.num-immutable-mem-table` | Number of immutable memtables (pending flush) | `InternalStats` |
| `rocksdb.mem-table-flush-pending` | Boolean: is flush pending? | `InternalStats` |
| `rocksdb.cur-size-all-mem-tables` | Total memory used by active + immutable memtables | `InternalStats` |
| `FLUSH_WRITE_BYTES` | Total bytes written by flush jobs | `Statistics` |
| `FLUSH_TIME` | Total time spent in flush jobs (microseconds) | `Statistics` |
| `OnFlushBegin()` | EventListener callback before flush starts | `EventListener` |
| `OnFlushCompleted()` | EventListener callback after flush commits | `EventListener` |

---

## Flush Debugging and Testing

### Key Sync Points

**Files:** `db/flush_job.cc`, `db/memtable_list.cc`

| Sync Point | Location | Purpose |
|------------|----------|---------|
| `FlushJob::FlushJob()` | Constructor | Pause before flush job starts |
| `DBImpl::AtomicFlushMemTablesToOutputFiles:WaitToCommit` | Before waiting for atomic commit ordering | Test atomic flush commit ordering |
| `MemTableList::TryInstallMemtableFlushResults:InProgress` | When another thread is committing | Test concurrent flush commit |
| `MemTableList::TryInstallMemtableFlushResults:AfterComputeMinWalToKeep` | After determining min WAL to keep | Test WAL deletion logic |

### Testing Atomic Flush

```cpp
// Verify atomic flush: either all CFs flush or none
TEST_F(DBAtomicFlushTest, AtomicFlushBasic) {
  Options options = CurrentOptions();
  options.atomic_flush = true;
  CreateColumnFamilies({"cf1", "cf2"}, options);

  // Write to both CFs
  ASSERT_OK(Put(0, "key1", "value1"));  // default CF
  ASSERT_OK(Put(1, "key2", "value2"));  // cf1

  // Trigger flush
  ASSERT_OK(Flush({0, 1}));

  // Verify: both CFs have the new L0 file
  ASSERT_EQ(NumTableFilesAtLevel(0, 0), 1);
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 1);
}
```

---

## Future Work and Experimental Features

### MemPurge (Experimental)

MemPurge is under active development. Current limitations:

- Not compatible with atomic flush
- Only triggered by `kWriteBufferFull` (not manual flush, shutdown, etc.)
- Heuristic (`MemPurgeDecider`) may not always predict GC benefit accurately
- Compatible with Get/Put/Delete, Iterators, and CompactionFilters
- Restriction: `CompactionFilter::IgnoreSnapshots() == false` is unsupported

Future work:

- Better heuristics for deciding when to purge
- Support for atomic flush

### Tiering and Last-Level Optimization

**Files:** `db/flush_job.cc:GetPrecludeLastLevelMinSeqno()`

Tiering use cases (hot-cold separation) benefit from knowing which data is "old enough" to move to the last level (cold tier). Flush jobs compute `preclude_last_level_min_seqno_` to inform user property collectors which data should not go to the last level.

---

## Summary

The flush subsystem is the critical bridge between in-memory writes and durable on-disk storage. It:

1. **Monitors triggers** (memtable size, global memory, WAL size) to decide when to flush
2. **Selects memtables** via `PickMemtablesToFlush()` in FIFO order
3. **Executes FlushJob** to build L0 SST files (and optionally blob files) via `BuildTable()`
4. **Commits atomically** via `TryInstallMemtableFlushResults()` → `LogAndApply()` → MANIFEST write
5. **Supports atomic flush** across multiple column families for transactional consistency
6. **Allows pipelined writes** during flush to avoid blocking ingestion
7. **Schedules background threads** via `MaybeScheduleFlushOrCompaction()`
8. **Interacts with WriteController** to enforce stalls when flush falls behind
9. **Handles errors** by rolling back state and entering recovery mode

Understanding flush is essential for tuning write performance, diagnosing stalls, and ensuring data durability in RocksDB.
