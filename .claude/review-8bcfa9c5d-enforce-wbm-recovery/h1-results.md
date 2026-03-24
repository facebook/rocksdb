# H1 Bug Verification: Double-Scheduling in WBM Recovery Path

## Bug Confirmed: YES

## Summary

The double-scheduling bug in `InsertLogRecordToMemtable()` is **confirmed and reproducible**. It causes a debug assertion crash in `FlushScheduler::ScheduleWork()` at `flush_scheduler.cc:18`.

## Root Cause

In `db/db_impl/db_impl_open.cc`, `InsertLogRecordToMemtable()` has two independent paths that can schedule the same ColumnFamilyData on `flush_scheduler_`:

1. **Path 1 (per-CF limit):** `WriteBatchInternal::InsertInto()` calls `CheckMemtableFull()` after each record insertion. When the memtable exceeds `write_buffer_size`, `ShouldFlushNow()` sets `flush_state_ = FLUSH_REQUESTED`, then `CheckMemtableFull()` calls `MarkFlushScheduled()` (CAS `FLUSH_REQUESTED -> FLUSH_SCHEDULED`) and `ScheduleWork(cfd)`.

2. **Path 2 (WBM global limit):** After `InsertInto()` returns, the WBM code at lines 1599-1607 checks `write_buffer_manager_->ShouldFlush()` and iterates all CFs, calling `ScheduleWork(cfd)` for any with `GetFirstSequenceNumber() != 0`. This path does NOT check if the CF was already scheduled.

When a single large WriteBatch during WAL replay pushes both the per-CF memtable size and the global WBM limit past their thresholds, both paths fire for the same CF, triggering:
```
assert(checking_set_.count(cfd) == 0)  // flush_scheduler.cc:18
```

## Trigger Conditions

The bug requires a **single WAL record (WriteBatch)** that contains enough data to exceed both:
- The per-CF `write_buffer_size` (triggering `CheckMemtableFull`)
- The WBM global limit (triggering `ShouldFlush()`)

Individual small Puts (each in their own WAL record) do NOT trigger the bug because `MaybeWriteLevel0TableForRecovery()` drains the flush scheduler between WAL records.

## Reproducer Test

Added `WriteBufferManagerLimitDuringWALRecoveryNoDoubleSchedule` in `db/db_write_buffer_manager_test.cc`:
- `write_buffer_size = 64KB`, `wbm_limit = 64KB`
- Writes a single large WriteBatch (~200KB) with 20 entries of 10KB each
- Closes and reopens with the small WBM limit
- During recovery, the single large batch triggers both per-CF and WBM limits

### Without fix (assertion crash):
```
DEBUG WBM scheduling cfd=default flush_scheduled=1
Received signal 6 (Aborted)
assert(checking_set_.count(cfd) == 0)  at flush_scheduler.cc:18
```

### With fix (passes):
```
[  PASSED  ] 1 test.
```

## Fix Applied

### 1. Added `HasFlushScheduled()` to `db/memtable.h`

```cpp
// Returns true if the memtable has already been scheduled for flush.
bool HasFlushScheduled() const {
  return flush_state_.load(std::memory_order_relaxed) == FLUSH_SCHEDULED;
}
```

### 2. Added dedup check in `db/db_impl/db_impl_open.cc`

```cpp
for (auto cfd : *versions_->GetColumnFamilySet()) {
  if (cfd->mem() != nullptr && cfd->mem()->GetFirstSequenceNumber() != 0 &&
      !cfd->mem()->HasFlushScheduled()) {  // Skip if already scheduled
    flush_scheduler_.ScheduleWork(cfd);
  }
}
```

The `HasFlushScheduled()` check skips CFs whose memtable `flush_state_` is already `FLUSH_SCHEDULED` (set by `CheckMemtableFull` via `MarkFlushScheduled()`), preventing the duplicate `ScheduleWork()` call.

## Files Modified

- `db/memtable.h` - Added `HasFlushScheduled()` method
- `db/db_impl/db_impl_open.cc` - Added `!cfd->mem()->HasFlushScheduled()` guard
- `db/db_write_buffer_manager_test.cc` - Added reproducer test

## All WBM Recovery Tests Pass

```
[  PASSED  ] 3 tests.
  WriteBufferManagerLimitDuringWALRecoverySingleDB (20 ms)
  WriteBufferManagerLimitDuringWALRecoveryMultipleDBs (19 ms)
  WriteBufferManagerLimitDuringWALRecoveryNoDoubleSchedule (8 ms)
```
