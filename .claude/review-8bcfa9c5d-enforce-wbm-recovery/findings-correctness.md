# Correctness Review Findings: enforce_write_buffer_manager_during_recovery

## Finding 1: Double-Scheduling Bug — Debug Assertion Crash

**Severity: HIGH**

The `flush_scheduler_` debug assertion at `db/flush_scheduler.cc:18` (`assert(checking_set_.count(cfd) == 0)`) will fire when a CF is scheduled twice in the same cycle. This can happen with the new code.

**Reproduction scenario:**

1. `InsertLogRecordToMemtable` calls `WriteBatchInternal::InsertInto`, which inserts into CF1's memtable.
2. CF1's memtable reaches its per-CF size limit (`write_buffer_size`).
3. `CheckMemtableFull()` (write_batch.cc:2930) calls `ShouldScheduleFlush()` -> true, `MarkFlushScheduled()` -> true (CAS succeeds), then `flush_scheduler_.ScheduleWork(cfd)` — CF1 is now on the scheduler.
4. `InsertInto` returns. The new WBM code checks `write_buffer_manager_->ShouldFlush()`.
5. If `ShouldFlush()` returns true (global memory is high), the code iterates all CFs and schedules any with `GetFirstSequenceNumber() != 0`.
6. CF1 has `GetFirstSequenceNumber() != 0` (we just inserted into it), so `flush_scheduler_.ScheduleWork(cfd)` is called again for CF1.
7. The debug assertion fires: `assert(checking_set_.count(cfd) == 0)` — **CRASH in debug builds**.

**In release builds:** CF1 would be dequeued twice by `MaybeWriteLevel0TableForRecovery`. The first dequeue flushes the memtable and calls `CreateNewMemtable`, which creates an empty memtable (with `GetFirstSequenceNumber() == 0`). The second dequeue calls `WriteLevel0TableForRecovery` on the *new empty* memtable, which would write an empty/trivial SST file. This wastes I/O and produces unnecessary L0 files.

**Fix:** Before scheduling a CF in the WBM loop, check that it hasn't already been scheduled. Options:
- Call `cfd->mem()->ShouldScheduleFlush() && cfd->mem()->MarkFlushScheduled()` in the WBM loop (mirroring `CheckMemtableFull`).
- Alternatively, drain the flush_scheduler first (call `MaybeWriteLevel0TableForRecovery` before the WBM check), then schedule remaining CFs. But this changes the control flow significantly.
- Or skip CFs whose `flush_state_` is already `FLUSH_SCHEDULED`.

## Finding 2: WBM ShouldFlush May Remain True After Flushing

**Severity: LOW** *(downgraded from MEDIUM after cross-review with test-reviewer)*

After `MaybeWriteLevel0TableForRecovery` flushes memtables and calls `CreateNewMemtable` (which deletes old memtables and decrements WBM memory), the next call to `InsertLogRecordToMemtable` will check `ShouldFlush()` again.

However, the "flush on every record" concern is less severe than initially assessed:

1. **Single-DB case:** After flushing all CFs, `CreateNewMemtable` frees old memtables and decrements WBM memory to near-zero. `ShouldFlush()` won't trigger again until enough new data accumulates to exceed 7/8 of buffer_size. The "every record" scenario requires each individual WAL record batch to exceed the WBM limit — an extremely degenerate configuration.

2. **Multi-DB shared WBM case:** Even if the other DB keeps WBM memory high, after the recovering DB flushes its CFs, the new memtables have `GetFirstSequenceNumber() == 0`. The WBM loop correctly skips them. Only CFs that receive data from subsequent WAL records get scheduled, and each flush is bounded by the data in that record.

3. **buffer_size=0:** Ruled out by Finding 8 — `ShouldFlush()` returns false when `buffer_size=0`.

**Verdict:** The repeated flush scenario is bounded and only degenerate in extreme configurations. No action required beyond the existing TODO for selective CF flushing.

## Finding 3: GetFirstSequenceNumber() != 0 Check

**Severity: LOW**

The condition `cfd->mem()->GetFirstSequenceNumber() != 0` is used to determine if a memtable has data. This is the same check used at `MaybeFlushFinalMemtableOrRestoreActiveLogFiles` (line 1828), so it is consistent with existing conventions.

However, sequence number 0 is technically a valid sequence number (the initial state before any writes). In practice, RocksDB starts sequence numbers at 1 (or from a recovered point), so `GetFirstSequenceNumber() == 0` reliably indicates an empty memtable. This is correct.

## Finding 4: Error Handling in MaybeWriteLevel0TableForRecovery

**Severity: LOW**

If `WriteLevel0TableForRecovery` fails mid-way (e.g., disk full), the function returns the error status immediately (line 1638). Any remaining CFs on the `flush_scheduler_` queue are NOT drained. These orphaned entries could cause issues if recovery is retried or if cleanup code doesn't clear the scheduler.

However, looking at the code flow: after `MaybeWriteLevel0TableForRecovery` returns an error, it propagates up through `ProcessLogRecord` -> `ProcessLogFile` -> `RecoverLogFiles`, and the DB open fails. The `FlushScheduler` is destroyed with the `DBImpl` object. The destructor should handle cleanup.

The `FlushScheduler::Clear()` method exists but I didn't verify it's called on error paths. The Ref'd CFDs in the scheduler could leak if not properly cleaned up. This is a pre-existing concern, not introduced by this change.

## Finding 5: Dropped CFs During Recovery

**Severity: LOW**

The WBM code iterates `versions_->GetColumnFamilySet()` which includes all CFs, potentially including ones being dropped. The `MaybeWriteLevel0TableForRecovery` function handles this via `TakeNextColumnFamily`, which skips dropped CFs (`if (!cfd->IsDropped())` at flush_scheduler.cc:57). So dropped CFs are correctly handled — they get scheduled but are skipped during consumption.

However, scheduling a dropped CF still triggers the `Ref()` in `ScheduleWork` and `UnrefAndTryDelete` in `TakeNextColumnFamily`. This is harmless but slightly wasteful. The WBM loop could add `!cfd->IsDropped()` to the condition, though this is a minor optimization.

## Finding 6: Interaction with allow_2pc

**Severity: LOW**

When `allow_2pc` is true, `avoid_flush_during_recovery` is forced to false (line 151-152). The new `enforce_write_buffer_manager_during_recovery` option is NOT similarly forced. This means:
- With 2PC + WBM, both the normal recovery flush path AND the WBM enforcement path are active.
- This is functionally fine since 2PC already forces flushing, but the WBM path may trigger flushes *earlier* than the per-CF memtable limit would.

No correctness issue here, just a behavioral note.

## Finding 7: Single-Threaded Recovery — Thread Safety

**Severity: LOW (Confirmed Safe)**

Recovery is single-threaded. The `flush_scheduler_` is used without locks during recovery, which is safe because:
- No concurrent writers exist during recovery.
- `InsertInto` and the WBM check run sequentially in the same thread.
- `MaybeWriteLevel0TableForRecovery` consumes the scheduler in the same thread.

The `flush_scheduler_` uses atomics internally (for the normal concurrent write path), but during recovery these are not needed. This is correct and consistent with existing recovery code.

## Finding 8: WBM with buffer_size = 0

**Severity: LOW (No Issue)**

When `buffer_size = 0`, `WriteBufferManager::enabled()` returns false, and `ShouldFlush()` returns false. The header comment says "ShouldFlush() will always return true" for buffer_size=0, but the actual code contradicts this — `ShouldFlush()` checks `enabled()` first and returns false when not enabled. So buffer_size=0 is safe with the new code; no infinite flushing will occur.

Note: the header comment at write_buffer_manager.h:41 is misleading/outdated but that's a pre-existing issue unrelated to this change.

## Summary

| # | Finding | Severity | Action Required |
|---|---------|----------|-----------------|
| 1 | Double-scheduling debug assertion crash | HIGH | Must fix before merge |
| 2 | Repeated WBM flush on every record with small buffer | LOW | No action (bounded behavior, per cross-review) |
| 3 | GetFirstSequenceNumber() != 0 correctness | LOW | Correct, no action |
| 4 | Error handling / scheduler drain on failure | LOW | Pre-existing, no action |
| 5 | Dropped CFs during recovery | LOW | Minor optimization opportunity |
| 6 | allow_2pc interaction | LOW | No issue, behavioral note |
| 7 | Thread safety during recovery | LOW | Confirmed safe |
| 8 | WBM buffer_size = 0 | LOW | No issue |
