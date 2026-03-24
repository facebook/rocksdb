# Test Coverage Review: enforce_write_buffer_manager_during_recovery

## Summary

The change adds two new unit tests (`WriteBufferManagerLimitDuringWALRecoverySingleDB` and `WriteBufferManagerLimitDuringWALRecoveryMultipleDBs`) plus crash/stress test integration. The tests verify the core happy path well, but several important edge cases and a critical bug scenario lack coverage.

---

## Findings

### 1. Double-Scheduling Bug Is Not Tested
**Severity: HIGH**

The context document identifies a potential double-scheduling scenario: if a per-CF memtable limit AND the WBM global limit trigger simultaneously, the same CF could be scheduled twice on `flush_scheduler_`, causing a debug assertion failure (`assert(checking_set_.count(cfd) == 0)` in `FlushScheduler::ScheduleWork`).

The new WBM code at `db/db_impl/db_impl_open.cc:1599-1607` iterates all CFs and calls `flush_scheduler_.ScheduleWork(cfd)` without checking if the CF was already scheduled by `CheckMemtableFull()` during `WriteBatchInternal::InsertInto`. There is no test that triggers both the per-CF memtable size limit and the WBM global limit simultaneously during recovery. This is a correctness bug that should be both fixed and tested.

A test could use a small `write_buffer_size` (e.g., 64KB) with a WBM limit slightly above it (e.g., 128KB) and multiple CFs so that one CF hits the per-CF limit while the global WBM limit is also exceeded.

### 2. No Test for WBM with buffer_size=0
**Severity: LOW** (downgraded after cross-review)

The `WriteBufferManager::ShouldFlush()` header comment states it "will always return true" when `buffer_size=0`. However, the correctness reviewer (Finding C8) clarified that the actual implementation checks `enabled()` first, which returns false when `buffer_size=0`, so `ShouldFlush()` returns false. The header comment is misleading/outdated but the code is safe. No flush storm will occur. A test would still document this behavior, but this is not a risk.

### 3. No Test for No WBM Configured
**Severity: LOW**

The code checks `write_buffer_manager_ != nullptr` before calling `ShouldFlush()`, but there is no explicit test that verifies recovery works correctly when `enforce_write_buffer_manager_during_recovery = true` but no WBM is set. This is a low-risk gap since the null check is simple, but a regression test would be prudent.

### 4. No Test for Multiple Column Families During Recovery
**Severity: MEDIUM**

Both unit tests use a single column family (the default CF). The implementation iterates `versions_->GetColumnFamilySet()` and schedules all CFs with non-empty memtables. There is no test that verifies:
- Correct behavior with multiple CFs where only some have data
- That all non-empty CFs are flushed (not just the one being written to)
- That empty CFs are correctly skipped (`GetFirstSequenceNumber() == 0`)

This is important because the TODO in the code acknowledges that flushing ALL CFs is suboptimal and may create unnecessary L0 files.

### 5. No Test for `avoid_flush_during_recovery` Interaction
**Severity: MEDIUM**

The context document explains a subtle interaction: once WBM triggers a mid-recovery flush (setting `flushed = true`), `avoid_flush_during_recovery` is effectively overridden for ALL remaining memtables at the end of recovery. The single-DB test sets `avoid_flush_during_recovery = true`, but it doesn't verify this override behavior explicitly. There should be a test that:
- Sets `avoid_flush_during_recovery = true` and `enforce_write_buffer_manager_during_recovery = true`
- Writes enough data to trigger a WBM flush mid-recovery
- Verifies that the final memtable is also flushed (because `flushed` became `true`)

This interaction is documented in the option's docstring but not tested.

### 6. No Test for `allow_2pc` Interaction
**Severity: LOW**

When `allow_2pc` is true, `avoid_flush_during_recovery` is forced to false. The new option doesn't have a similar override. There is no test that verifies WBM enforcement works correctly with 2PC enabled. Given that 2PC affects recovery flow, this gap should be noted.

### 7. No Test for Multiple WAL Files During Recovery
**Severity: MEDIUM**

The tests write data and close the DB, resulting in a single WAL file. Real-world recovery scenarios often involve multiple WAL files (e.g., after partial flushes). The WBM enforcement should be tested across WAL boundaries to verify that:
- Memory accounting remains correct across WAL files
- Flushes triggered by WBM properly interact with WAL-to-WAL transitions

### 8. Crash Test Parameter Is Not a Lambda
**Severity: LOW**

In `tools/db_crashtest.py`, the new parameter uses `random.randint(0, 1)` directly instead of a `lambda` like the nearby `avoid_flush_during_recovery`. This means the value is evaluated once at module load time, not per test run. Every test run in a single process invocation gets the same value, reducing randomization coverage. Compare:
```python
# Current (evaluated once):
"enforce_write_buffer_manager_during_recovery": random.randint(0, 1),
# Should be (evaluated per run):
"enforce_write_buffer_manager_during_recovery": lambda: random.randint(0, 1),
```

### 9. Assertion Strength on L0 File Count
**Severity: NON-ISSUE** (revised after cross-review)

The single-DB test computes `expected_num_l0_files` using the formula `memory_without_enforcement / (kWbmLimit * 7 / 8) + 1` and asserts exact equality. Initially flagged as potentially fragile, but the performance reviewer correctly pointed out that: (1) the 50KB value size gives ample margin over arena overhead (~few KB), so the count is deterministic in practice; (2) exact assertions catch regressions in both directions (too many or too few flushes); (3) preemptively weakening assertions loses regression detection value. The assertion is correctly precise.

### 10. Test Code Duplication
**Severity: LOW**

The two new tests (`SingleDB` and `MultipleDBs`) share substantial setup boilerplate: option configuration, data writing loop, WBM creation, and the pattern of testing with enforcement disabled then enabled. A helper function could extract:
- Option setup with WBM configuration
- The "write N keys of size K" loop
- The "close, swap WBM, reopen" pattern

This follows the CLAUDE.md guidance on extracting common reusable utility functions. However, with only two tests, this is not urgent.

### 11. `preserve_unverified_changes` Interaction in Stress Test
**Severity: LOW**

The stress test correctly handles the `preserve_unverified_changes` case by flipping `enforce_write_buffer_manager_during_recovery` to false (with a warning). This is well done. However, there is no corresponding unit test for this interaction.

---

## What Is Well Done

1. **Two-phase test structure**: Both tests compare enforcement-disabled vs enforcement-enabled behavior in the same test, making the contrast clear and providing a control group.

2. **Shared WBM multi-DB scenario**: The multi-DB test correctly simulates the primary use case (shared WBM with one DB recovering while another is active).

3. **Memory assertions**: Tests check both memory usage (bounded vs unbounded) and L0 file counts (files created vs not).

4. **Stress test integration**: The `db_crashtest.py` randomization and `db_stress_test_base.cc` integration provide long-running coverage via continuous stress testing.

5. **`preserve_unverified_changes` handling**: The stress test correctly disables the option when `preserve_unverified_changes` is active, with an appropriate warning message.
