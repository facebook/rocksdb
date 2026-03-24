# Codebase Context for Review: enforce_write_buffer_manager_during_recovery

## Feature Summary
This change adds a new `enforce_write_buffer_manager_during_recovery` option (default: `true`) that enables WBM (WriteBufferManager) memory limit enforcement during WAL recovery. Without this, a recovering RocksDB instance sharing a WBM with other instances could consume unbounded memory during WAL replay, potentially causing OOM.

## Commits Under Review
1. `183493d07` - Core implementation: option definition, recovery logic, initial test
2. `5357dfef0` - Remove MarkFlushScheduled() check from the WBM recovery code
3. `f51556c9e` - Add multiple DB scenario in test
4. `f239e5f71` - Fix test
5. `388e46269` - Address comments
6. `67ffb6d82` - Add stress test integration
7. `8bcfa9c5d` - Address feedback

## Files Changed
- `db/db_impl/db_impl_open.cc` - Core logic (22 lines added in InsertLogRecordToMemtable)
- `db/db_write_buffer_manager_test.cc` - Unit tests (191 lines)
- `db_stress_tool/db_stress_common.h` - Flag declaration
- `db_stress_tool/db_stress_gflags.cc` - Flag definition
- `db_stress_tool/db_stress_test_base.cc` - Stress test integration
- `include/rocksdb/options.h` - Public API (21 lines documentation)
- `options/db_options.cc` - Option plumbing (10 lines)
- `options/db_options.h` - Internal struct field
- `options/options_helper.cc` - BuildDBOptions
- `options/options_settable_test.cc` - Settable test
- `tools/db_crashtest.py` - Crash test randomization
- `unreleased_history/new_features/enforce_wbm_during_recovery.md` - Release note

## How the Recovery Subsystem Works

### WAL Recovery Flow (db_impl_open.cc)
```
DB::Open → Recover → RecoverLogFiles
  → for each WAL:
    → ProcessLogRecord (per WAL record)
      → InsertLogRecordToMemtable
        → WriteBatchInternal::InsertInto (inserts to memtable)
          → CheckMemtableFull() — may schedule flush via flush_scheduler_
            → calls ShouldScheduleFlush() && MarkFlushScheduled()
        → NEW: WBM check — may schedule ALL non-empty CFs via flush_scheduler_
      → MaybeWriteLevel0TableForRecovery
        → while (flush_scheduler_.TakeNextColumnFamily()):
          → WriteLevel0TableForRecovery (writes memtable to SST)
          → cfd->CreateNewMemtable()
  → MaybeFlushFinalMemtableOrRestoreActiveLogFiles
    → if (flushed || !avoid_flush_during_recovery):
      → flush ALL remaining non-empty memtables
    → else: RestoreAliveLogFiles (keep WALs for future replay)
```

### Critical Interaction: flush_scheduler_ Double-Scheduling
`WriteBatchInternal::InsertInto` (line 1583) can schedule a CF via `CheckMemtableFull()` when the per-CF memtable size limit is reached. This uses `MarkFlushScheduled()` as dedup — a CAS on the memtable's `flush_state_` from `FLUSH_REQUESTED` to `FLUSH_SCHEDULED`.

The NEW WBM code (lines 1599-1607) iterates ALL CFs and schedules those with `GetFirstSequenceNumber() != 0`. It does NOT check `MarkFlushScheduled()` or `flush_state_`.

**Potential double-scheduling scenario:**
1. `InsertInto` processes a batch, memtable for CF1 hits per-CF limit
2. `CheckMemtableFull()` calls `MarkFlushScheduled()` → true, schedules CF1
3. After `InsertInto` returns, WBM `ShouldFlush()` returns true
4. WBM code checks CF1: `GetFirstSequenceNumber() != 0` → true, schedules CF1 AGAIN
5. `FlushScheduler::ScheduleWork` has debug assertion: `assert(checking_set_.count(cfd) == 0)` → **CRASH in debug builds**

In release builds, CF1 would be consumed twice by `MaybeWriteLevel0TableForRecovery`:
- First: flush the memtable, create new empty memtable
- Second: flush the empty memtable → writes empty SST file

### WriteBufferManager::ShouldFlush() Semantics
```cpp
bool ShouldFlush() const {
  if (enabled()) {
    if (mutable_memtable_memory_usage() > mutable_limit_) return true;  // 7/8 of buffer_size
    if (memory_usage() >= buffer_size && mutable_memtable_memory_usage() >= buffer_size / 2) return true;
  }
  return false;
}
```
- `mutable_limit_` = 7/8 of `buffer_size_`
- Primary trigger: active memtable memory > 7/8 of buffer_size

### avoid_flush_during_recovery Interaction
At `MaybeFlushFinalMemtableOrRestoreActiveLogFiles` (line 1832):
```cpp
if (flushed || !immutable_db_options_.avoid_flush_during_recovery) {
    // flush final memtable
}
```
- If `flushed` is true (any mid-recovery flush happened), ALL remaining memtables get flushed regardless of `avoid_flush_during_recovery`
- The new WBM enforcement triggers mid-recovery flushes via `flush_scheduler_`, setting `flushed = true`
- This means: once WBM triggers a flush during recovery, `avoid_flush_during_recovery` is effectively overridden for the remainder

### ColumnFamilyData::CreateNewMemtable (column_family.cc:1233)
```cpp
void ColumnFamilyData::CreateNewMemtable(SequenceNumber earliest_seq) {
  if (mem_ != nullptr) {
    delete mem_->Unref();  // Frees old memtable, WBM memory is decremented
  }
  SetMemtable(ConstructNewMemtable(GetLatestMutableCFOptions(), earliest_seq));
  mem_->Ref();
}
```
The old memtable is immediately deleted (not moved to immutable list), which decrements WBM memory accounting.

### Normal Write Path WBM Handling (for comparison)
In the normal write path (`PreprocessWrite` → `HandleWriteBufferManagerFlush`):
- Uses `HandleWriteBufferManagerFlush()` which calls `SwitchMemtable()` for the CURRENT CF only
- Then schedules a flush job via the background thread pool
- Much more sophisticated: considers compaction pressure, write stalls, etc.

The recovery path is simpler: schedules ALL non-empty CFs for flush (like `atomic_flush=false` path) and writes L0 files synchronously.

## Key Invariants

1. **FlushScheduler dedup**: Each CFD must be scheduled at most once on `flush_scheduler_` between consumption cycles. Enforced by debug assertion in `ScheduleWork`.

2. **avoid_flush_during_recovery override**: Once `flushed = true`, all remaining memtables are flushed at end of recovery. The new option's docs correctly describe this behavior.

3. **Memory accounting**: `CreateNewMemtable` deletes the old memtable immediately, decrementing WBM's memory. This is important — if the old memtable weren't freed, WBM would still report high usage.

4. **Single-threaded recovery**: Recovery is single-threaded (no concurrent writes), so no mutex contention issues. The flush_scheduler_ is used without locks during recovery.

5. **2PC interaction**: When `allow_2pc` is true, `avoid_flush_during_recovery` is forced to false (line 152). The new option doesn't have a similar override, meaning WBM enforcement could still apply even with 2PC.

## Existing Conventions for New Options
- Options use `ImmutableDBOptions` (not `MutableDBOptions`) for recovery-time settings
- Must be plumbed through: `DBOptions` struct, `ImmutableDBOptions` struct/constructor, `BuildDBOptions`, option type map, settable test, `Dump()`
- Should have stress test coverage via `db_crashtest.py`
- Default value should be carefully considered for backwards compatibility

## Potential Pitfalls

1. **Double-scheduling bug**: As described above, if per-CF memtable limit AND WBM global limit trigger simultaneously, the same CF could be scheduled twice on `flush_scheduler_`, causing debug assertion failure.

2. **Default value = true**: This changes behavior for ALL existing users on upgrade. Any user with a WBM configured will now get flush enforcement during recovery, which could produce more L0 files. This is a behavioral change that could surprise users.

3. **Empty SST files**: If double-scheduling occurs in release builds, empty SST files could be created.

4. **WBM with buffer_size=0**: The `ShouldFlush()` comment says "ShouldFlush() will always return true" when buffer_size=0. If a user creates a WBM with size 0, the new code would try to flush on every single WAL record during recovery.

5. **No CF selection heuristic**: The TODO in the code acknowledges that flushing ALL CFs is suboptimal. CFs with very little data will get unnecessary L0 files.

6. **Interaction with atomic_flush**: The new code doesn't check `immutable_db_options_.atomic_flush`. In normal write path, atomic flush has different handling. During recovery this might not matter since recovery is single-threaded, but it's worth examining.
