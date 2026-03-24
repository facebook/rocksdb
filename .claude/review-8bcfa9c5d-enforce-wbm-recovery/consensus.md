# Consensus Document: enforce_write_buffer_manager_during_recovery Review

## Methodology
5 specialized review agents analyzed this change independently, followed by a cross-review debate phase. Findings are classified by agreement level.

---

## HIGH Severity Findings (Must Fix)

### H1: Double-Scheduling Bug — FlushScheduler Debug Assertion Crash
**Reported by:** Correctness (C1), Design (D1), Test Coverage (T1)
**Agreement:** 3/5 agents (HIGH confidence)

**Description:** `InsertLogRecordToMemtable` has two independent paths that can schedule the same CF on `flush_scheduler_`:
1. `WriteBatchInternal::InsertInto` → `CheckMemtableFull()` → `MarkFlushScheduled()` → `ScheduleWork(cfd)` (when per-CF memtable size limit hit)
2. New WBM code → iterates all CFs → `ScheduleWork(cfd)` for any with `GetFirstSequenceNumber() != 0`

The second path does NOT check `MarkFlushScheduled()`. If both trigger for the same CF:
- **Debug builds:** `assert(checking_set_.count(cfd) == 0)` at `flush_scheduler.cc:18` **crashes**
- **Release builds:** CF consumed twice, second time flushes an empty memtable, creating unnecessary SST

**Reproduction:** Set `write_buffer_size` small enough that a CF hits per-CF limit, with WBM limit also exceeded.

**Fix options:**
1. Add `!cfd->mem()->ShouldScheduleFlush()` or check `flush_state_` in the WBM loop
2. Use `MarkFlushScheduled()` as dedup guard (same as `CheckMemtableFull`)
3. Add `!cfd->IsDropped()` check as well (minor optimization from C5)

---

## MEDIUM Severity Findings (Should Fix or Document)

### M1: Default `true` Changes Behavior for Existing WBM Users
**Reported by:** Design (D2), API (A1), Performance (P4)
**Agreement:** 3/5 agents

All existing users with a WriteBufferManager will get new flush-during-recovery behavior on upgrade. This also silently overrides `avoid_flush_during_recovery=true` once any WBM flush triggers (because `flushed=true` cascades to flush all remaining memtables at end of recovery).

**Recommendation:** Either:
- Default to `false` for backwards compatibility, OR
- Keep `true` but enhance the release note to clearly call out the behavioral change

### M2: Flushing ALL CFs Is Overly Aggressive
**Reported by:** Design (D3), Performance (P2)
**Agreement:** 2/5 agents

The code schedules ALL CFs with non-empty memtables for flush when WBM triggers. The normal write path only flushes the current CF. This produces unnecessary small L0 files for CFs with minimal data. Acknowledged by TODO in code.

**Recommendation:** Acceptable for initial implementation (code has TODO), but consider flushing only the largest CF or adding a minimum size threshold in a follow-up.

### M3: Post-Recovery Compaction Storm
**Reported by:** Performance (P3)
**Agreement:** 1/5 agents (but logically follows from M2)

Extra L0 files from aggressive flushing will trigger compactions after DB opens. Could cause write stalls if L0 count approaches `level0_stop_writes_trigger`.

**Recommendation:** Document this behavior. The selective-CF-flush improvement (M2) would mitigate this.

### M4: Missing Java JNI Bindings
**Reported by:** API (A2)
**Agreement:** 1/5 agents

The sibling option `avoid_flush_during_recovery` has Java JNI bindings. The new option does not. Java users cannot configure it programmatically.

**Recommendation:** Add Java JNI bindings (can be a follow-up PR).

### M5: No Test for Multiple CFs / avoid_flush Interaction / Multiple WALs
**Reported by:** Test Coverage (T4, T5, T7)
**Agreement:** 1/5 agents

Tests only cover single-CF, single-WAL scenarios. Missing coverage for:
- Multiple CFs where only some have data
- `avoid_flush_during_recovery` override behavior
- Multiple WAL files during recovery

**Recommendation:** Add at least a multi-CF test.

---

## LOW Severity Findings (Nice to Have)

### L1: Crash Test Parameter Not a Lambda
**Reported by:** Test Coverage (T8)
`random.randint(0, 1)` evaluated once at module load, not per run. Should be `lambda: random.randint(0, 1)`.

### L2: No SanitizeOptions for 2PC Interaction
**Reported by:** API (A8)
With `allow_2pc`, `avoid_flush_during_recovery` is forced false. The new option has no similar handling. Not a bug, but should be documented.

### L3: Release Note Too Terse
**Reported by:** API (A9)
Should mention OOM prevention motivation and behavioral change for existing users.

### L4: WBM buffer_size=0 Safe (Confirmed)
**Reported by:** Correctness (C8), Performance (P6)
Both reviewers independently confirmed: `ShouldFlush()` returns false when `buffer_size=0` because `enabled()` returns false. The header comment is misleading but pre-existing. **No issue.**

### L5: Thread Safety Confirmed Safe
**Reported by:** Correctness (C7)
Recovery is single-threaded; no concurrency concerns.

### L6: Dropped CFs Handled Correctly
**Reported by:** Correctness (C5)
`TakeNextColumnFamily` skips dropped CFs. Minor optimization: add `!cfd->IsDropped()` to WBM loop.

---

## Verification Plan

| Finding | Action | Priority |
|---------|--------|----------|
| H1: Double-scheduling | Write reproducer test + fix | MUST FIX |
| M1: Default value | Decision needed (author preference) | SHOULD DISCUSS |
| M2: Flush-all-CFs | Accept with TODO (already documented) | ACCEPTABLE |
| M4: Java JNI | Follow-up PR | OPTIONAL |
| L1: Lambda fix | Trivial fix | NICE TO HAVE |
