# API & Compatibility Review Findings

## 1. Default Value `true` Changes Behavior for Existing Users [MEDIUM]

The default value of `true` means that all existing users who have a `WriteBufferManager` configured will get new flush-during-recovery behavior on upgrade, without any code change on their part. This can:
- Produce additional smaller L0 files during recovery
- Override `avoid_flush_during_recovery=true` once a WBM-triggered flush occurs

While the feature solves a real OOM problem (which justifies the default), this is a **behavioral change** that should be called out more prominently in the release note. Users who relied on `avoid_flush_during_recovery=true` to preserve WAL files after recovery may be surprised that WBM enforcement can override it.

**Recommendation:** The release note should explicitly call out that recovery may produce additional L0 files AND may override `avoid_flush_during_recovery` for existing WBM users. The current release note is too terse about this behavioral change.

## 2. Missing Java JNI Binding [MEDIUM — follow-up, not a blocker]

The analogous option `avoid_flush_during_recovery` has Java JNI bindings in `java/rocksjni/options.cc` (both for `Options` and `DBOptions` classes, with getter and setter). The new `enforce_write_buffer_manager_during_recovery` option has **no Java JNI bindings**.

Java users will not be able to configure this option programmatically. However, they can still set it via options file/string parsing (the `options_helper.cc` plumbing is complete), so they are not blocked — just inconvenienced. The safe default (`true`) also means most Java users won't need to change it. The Java API lagging behind C++ is a common pattern in this codebase, so this is acceptable as a follow-up diff rather than a blocker for this change.

**Files needing updates in follow-up:**
- `java/rocksjni/options.cc` (getter/setter for both Options and DBOptions)
- `java/src/main/java/org/rocksdb/Options.java`
- `java/src/main/java/org/rocksdb/DBOptions.java`
- `java/src/test/java/org/rocksdb/OptionsTest.java` or `DBOptionsTest.java`

## 3. Missing C API Binding [MEDIUM]

The `avoid_flush_during_recovery` option does NOT have a C API binding in `include/rocksdb/c.h`, so the new option also lacking one is **consistent**. However, if this option is considered important enough to default to `true`, providing a C API binding would be good practice for completeness.

**Verdict:** Not blocking since the sibling option also lacks it, but noting for completeness.

## 4. Option Name is Clear and Consistent [LOW - POSITIVE]

The name `enforce_write_buffer_manager_during_recovery` follows existing RocksDB naming conventions:
- Uses `snake_case`
- Descriptive and self-documenting
- Follows the `<verb>_<subject>_during_<phase>` pattern similar to `avoid_flush_during_recovery`
- Consistent with `enforce_single_del_contracts` pattern for the `enforce_` prefix

No issues with the naming.

## 5. Documentation Quality in `options.h` [LOW]

The documentation in `options.h` is well-written and covers:
- What the option does
- When it takes effect (WBM configured + WAL recovery)
- The OOM scenario it prevents
- Side effects (smaller L0 files, all CFs flushed)
- Interaction with `avoid_flush_during_recovery`

The `avoid_flush_during_recovery` cross-reference documentation update is also clear and accurate.

One minor improvement: the doc could mention that this option has no effect if no `WriteBufferManager` is configured (it's implied but could be explicit).

## 6. Correctly Classified as Immutable [LOW - POSITIVE]

The option is placed in `ImmutableDBOptions`, which is correct because:
- It only affects recovery behavior (DB::Open time)
- It cannot meaningfully be changed at runtime
- It follows the same pattern as `avoid_flush_during_recovery`

## 7. Option Plumbing Completeness [LOW - POSITIVE]

The core option plumbing is complete:
- `DBOptions` struct declaration with default value
- `ImmutableDBOptions` struct field
- `ImmutableDBOptions` constructor copies from `DBOptions`
- `BuildDBOptions` copies back from `ImmutableDBOptions`
- Option type map entry with correct offset, type, and flags
- `Dump()` logging
- `options_settable_test.cc` updated with the new option in the string

## 8. No SanitizeOptions Validation for 2PC Interaction [LOW]

*Downgraded from MEDIUM after debate with performance-reviewer.*

When `allow_2pc = true`, `SanitizeOptions` forces `avoid_flush_during_recovery = false` (db_impl_open.cc:152). However, there is no similar handling for `enforce_write_buffer_manager_during_recovery`.

This means with `allow_2pc = true`:
- `avoid_flush_during_recovery` is forced to `false` (flushes already happen)
- `enforce_write_buffer_manager_during_recovery` is left as-is

There is no semantic conflict here. Unlike `avoid_flush_during_recovery` (which could break 2PC recovery correctness if left true), the WBM option is purely about memory management and can't break recovery correctness. The WBM enforcement is additive — it just adds a global memory trigger on top of already-happening per-CF flushes.

**Recommendation:** A brief code comment in SanitizeOptions noting why no override is needed would be sufficient. No SanitizeOptions change required.

## 9. Release Note Quality [LOW]

The release note reads:
> Introduced enforce_write_buffer_manager_during_recovery option to allow WriteBufferManager to be enforced during WAL recovery. (Default: true)

This is adequate but could be improved:
- Should mention the backtick formatting for the option name
- Should briefly mention the OOM prevention motivation
- Should note it may change recovery behavior for existing WBM users

**Suggested improvement:**
> Added `enforce_write_buffer_manager_during_recovery` option (default: `true`) to enforce WriteBufferManager memory limits during WAL recovery, preventing potential OOM when multiple DB instances share a WriteBufferManager. Users with an existing WriteBufferManager may observe additional L0 files created during recovery, and `avoid_flush_during_recovery` may be overridden once a WBM-triggered flush occurs.

## 10. `avoid_flush_during_recovery` Doc Update [LOW - POSITIVE]

The added cross-reference note on `avoid_flush_during_recovery` is clear and accurately describes the interaction:
- WBM-triggered flushes can happen even when `avoid_flush_during_recovery=true`
- Once such a flush happens, all remaining memtables are flushed at end of recovery

This is well done and helps users understand the interaction between the two options.

## Cross-Review Critiques

### On Correctness C1 (Double-Scheduling Bug) — AGREE

Confirmed HIGH. The `FlushScheduler::ScheduleWork` debug assertion at flush_scheduler.cc:18
will fire when both per-CF memtable limit and WBM global limit trigger in the same
`InsertLogRecordToMemtable` call. The WBM loop at db_impl_open.cc:1603-1607 does not check
`flush_state_` before scheduling. I added a nuance on the fix: using
`ShouldScheduleFlush() && MarkFlushScheduled()` won't work because CFs that haven't hit
their per-CF limit will be in `FLUSH_NOT_REQUESTED` state — the CAS would fail and skip
them. A better guard is checking `flush_state_ != FLUSH_SCHEDULED` directly.

### On Correctness C2 (ShouldFlush stays true) — REFINE

Directionally correct but the "every record" scenario needs qualification. After flushing
ALL CFs, WBM memory drops significantly, so continuous triggering only happens when:
(a) WBM buffer_size is very small relative to individual WAL records, or
(b) shared WBM memory is dominated by OTHER instances. Case (b) is actually the intended
use case and the behavior (keeping recovering instance's footprint minimal) is correct.
Keep at MEDIUM but reframe as a shared-WBM interaction concern.

### On Performance P3 (Compaction Storm) — REFINE

Valid concern but severity framing is overstated. Write stall risk is low: with reasonable
WBM buffer sizes, L0 file count per CF from WBM flushes is bounded and unlikely to approach
`level0_slowdown_writes_trigger` (default 20). The total compaction I/O is bounded by total
WAL data regardless of L0 file count. Reframe as "suboptimal L0 file distribution" rather
than "compaction storm." The alternative (OOM crash) is far worse.

## Summary

| # | Finding | Severity |
|---|---------|----------|
| 1 | Default `true` changes behavior for existing WBM users | MEDIUM |
| 2 | Missing Java JNI bindings (follow-up, not blocker) | MEDIUM |
| 3 | Missing C API binding (consistent with sibling) | LOW |
| 4 | Option name is clear and consistent | POSITIVE |
| 5 | Documentation is good, minor improvement possible | LOW |
| 6 | Correctly immutable | POSITIVE |
| 7 | Core plumbing is complete | POSITIVE |
| 8 | No SanitizeOptions validation for 2PC interaction | LOW |
| 9 | Release note could be more informative | LOW |
| 10 | `avoid_flush_during_recovery` doc update is well done | POSITIVE |
