# Design & Approach Review Findings

## Finding D1: Double-Scheduling Bug with FlushScheduler (HIGH)

**Description**: The new WBM enforcement code at `db/db_impl/db_impl_open.cc:1599-1607` schedules CFs onto `flush_scheduler_` by iterating all CFs with non-empty memtables. However, `WriteBatchInternal::InsertInto` (called just above at line 1583) can also schedule a CF onto the same `flush_scheduler_` via `CheckMemtableFull()` when the per-CF memtable size limit is hit (see `db/write_batch.cc:2930-2940`).

The `CheckMemtableFull` path uses `MarkFlushScheduled()` as a CAS guard, but the new WBM code does NOT check `MarkFlushScheduled()` or any other dedup mechanism. If both triggers fire for the same CF in the same `InsertLogRecordToMemtable` call:

1. `InsertInto` schedules CF1 via `CheckMemtableFull` (using `MarkFlushScheduled`)
2. After `InsertInto` returns, WBM `ShouldFlush()` returns true
3. WBM code checks CF1: `GetFirstSequenceNumber() != 0` is true, schedules CF1 **again**
4. `FlushScheduler::ScheduleWork` has `assert(checking_set_.count(cfd) == 0)` at `db/flush_scheduler.cc:18` -- **debug build crash**

In release builds, the CF would be consumed twice by `MaybeWriteLevel0TableForRecovery`, first flushing the actual memtable, then flushing the newly created empty memtable (producing an empty/trivial SST file).

**Recommendation**: Before scheduling in the WBM code path, check whether the CF was already scheduled. Either:
- Check `cfd->mem()->MarkFlushScheduled()` (same guard used by `CheckMemtableFull`)
- Or check `cfd->mem()->flush_state_` to see if already in FLUSH_SCHEDULED state
- Or add a dedup set within the WBM scheduling loop

## Finding D2: Default Value of `true` Is a Silent Behavioral Change (MEDIUM)

**Description**: The default value of `enforce_write_buffer_manager_during_recovery = true` changes behavior for ALL existing users who have a WriteBufferManager configured. On upgrade, their recovery path will now trigger mid-recovery flushes that didn't occur before. This leads to:
- More L0 files produced during recovery
- `avoid_flush_during_recovery = true` being effectively overridden once any WBM flush triggers (since `flushed = true` causes all remaining memtables to be flushed at end of recovery, per `MaybeFlushFinalMemtableOrRestoreActiveLogFiles`)
- Potentially different post-recovery database state (more SSTs, different compaction behavior)

Users relying on `avoid_flush_during_recovery = true` to minimize recovery I/O or preserve WAL files may be surprised.

**Recommendation**: Consider defaulting to `false` for backwards compatibility, with documentation encouraging users sharing WBM across instances to opt in. Alternatively, keep `true` but add a prominent note in the release notes about the behavioral change and its interaction with `avoid_flush_during_recovery`.

## Finding D3: Flushing ALL Column Families Is Overly Aggressive (MEDIUM)

**Description**: When WBM's `ShouldFlush()` triggers, the code schedules ALL CFs with non-empty memtables for flush (`db/db_impl/db_impl_open.cc:1603-1606`). This is acknowledged by the TODO comment but is still a design concern:

- A CF with only a few KB of data will produce a tiny L0 file unnecessarily
- This differs from the normal write path's `HandleWriteBufferManagerFlush` which only flushes the current CF (the one that pushed memory over the limit)
- With many CFs, this could produce a large number of small L0 files, increasing read amplification and future compaction work

The normal write path at `db/db_impl/db_impl_write.cc:2100` uses `SwitchMemtable` on only the current CF, which is more targeted.

**Recommendation**: Consider flushing only the CF with the largest memtable, or the CF that was just written to (the current CF in the batch). This would still release memory while producing fewer unnecessary L0 files. If flushing all CFs is intentional (simpler, ensures maximum memory release), document the rationale more explicitly beyond just a TODO.

## Finding D4: Approach to OOM Prevention Is Sound (LOW - Positive)

**Description**: The overall approach of checking `WriteBufferManager::ShouldFlush()` during recovery and triggering synchronous flushes is reasonable and appropriate for the problem being solved. The recovery path is single-threaded, so the synchronous flush via `MaybeWriteLevel0TableForRecovery` is safe. The placement after `InsertInto` mirrors where the normal write path checks WBM (`PreprocessWrite`). The option being an `ImmutableDBOption` is correct since it only applies at DB::Open time.

**Recommendation**: No change needed. Good design choice.

## Finding D5: Interaction with `avoid_flush_during_recovery` Is Documented but May Confuse Users (LOW)

**Description**: The documentation in `include/rocksdb/options.h` correctly describes that once a WBM-triggered flush occurs, `avoid_flush_during_recovery` is effectively overridden for the rest of recovery. This is because the existing code at `MaybeFlushFinalMemtableOrRestoreActiveLogFiles` checks `if (flushed || !immutable_db_options_.avoid_flush_during_recovery)` -- so any mid-recovery flush sets `flushed = true` which causes all remaining memtables to be flushed.

This creates a non-obvious coupling: enabling `enforce_write_buffer_manager_during_recovery` can silently negate `avoid_flush_during_recovery`. The documentation mentions it but the behavior is still surprising.

**Recommendation**: The documentation is adequate. Consider adding a log message when this override occurs so users can diagnose unexpected recovery behavior (e.g., "WBM-triggered flush during recovery overrides avoid_flush_during_recovery").

## Finding D6: No Handling for WBM with buffer_size=0 (LOW)

**Description**: `WriteBufferManager::ShouldFlush()` returns true on every call when `buffer_size_` is 0 (as noted in the WBM source comments). If a user creates a WBM with size 0, the new code would schedule flushes for every single WAL record during recovery, producing an excessive number of L0 files.

**Recommendation**: Add a guard: skip WBM enforcement if `write_buffer_manager_->buffer_size() == 0`, or document this edge case. In practice this is unlikely but worth guarding against.

## Finding D7: ImmutableDBOption Is the Correct Choice (LOW - Positive)

**Description**: Making this an `ImmutableDBOption` is correct. The option only matters during recovery (DB::Open), and recovery parameters should not be dynamically changeable. This is consistent with `avoid_flush_during_recovery` which is also immutable.

**Recommendation**: No change needed.

## Finding D8: Stress Test Integration Is Well Done (LOW - Positive)

**Description**: The stress test integration properly:
- Adds the flag to `db_stress_gflags.cc` with the default from `Options()`
- Randomizes it in `db_crashtest.py`
- Handles the `preserve_unverified_changes` interaction by flipping the option to false with a warning

**Recommendation**: No change needed.
