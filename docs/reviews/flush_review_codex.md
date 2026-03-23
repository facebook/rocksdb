# Review: flush — Codex

## Summary
Overall quality rating: needs work.

The doc set has a solid high-level shape. The index is concise and follows the expected structure, the memtable-selection chapter captures the FIFO/non-consecutive-selection rule well, and the MemPurge chapter at least notices the recent memtable-ID ordering issue. Those parts make the component easier to approach.

The bigger problem is that several of the most important cross-component and option-semantics details are wrong or oversimplified in ways that will mislead maintainers. The main trouble spots are trigger selection (`kWalFull`, `WriteBufferManager`), MemPurge post-processing, atomic-flush install behavior, and background-error handling. The docs also miss several tested behaviors: read-path-triggered flushes, manual-flush UDT semantics, shared-`WriteBufferManager` stalls, and stats-CF coupling.

## Correctness Issues

### [WRONG] `max_write_buffer_number` is not effectively allowed to be `1`
- **File:** `docs/components/flush/01_triggers_and_configuration.md` / Configuration Parameters
- **Claim:** "A value of 1 is accepted but means writes block during every flush."
- **Reality:** RocksDB sanitizes `max_write_buffer_number` upward to `2`. The minimum effective value is `2`, both on open and when options are changed dynamically.
- **Source:** `db/column_family.cc` `SanitizeCfOptions`; `include/rocksdb/advanced_options.h` `AdvancedColumnFamilyOptions::max_write_buffer_number`
- **Fix:** Say the minimum effective value is `2`, and explain that smaller user-provided values are clamped.

### [WRONG] The dynamic `max_total_wal_size` default is documented with the wrong formula
- **File:** `docs/components/flush/01_triggers_and_configuration.md` / Configuration Parameters and WAL Size Trigger
- **Claim:** "`max_total_wal_size` ... 0 = 4x total write buffer size."
- **Reality:** The derived limit is `4 * max_total_in_memory_state_`, and `max_total_in_memory_state_` is maintained as the sum across column families of `write_buffer_size * max_write_buffer_number`, not just total `write_buffer_size`.
- **Source:** `include/rocksdb/options.h` `DBOptions::max_total_wal_size`; `db/db_impl/db_impl_open.cc` initialization of `max_total_in_memory_state_`; `db/db_impl/db_impl_compaction_flush.cc` `InstallSuperVersionAndScheduleWork`; `db/db_impl/db_impl_write.cc` `GetMaxTotalWalSize`
- **Fix:** Replace the simplified formula with the real one and mention that `max_write_buffer_number` participates in the calculation.

### [WRONG] The WAL-full path does not flush just one CF, and there is no `HandleWALFull()`
- **File:** `docs/components/flush/01_triggers_and_configuration.md` / WAL Size Trigger
- **Claim:** "RocksDB identifies the column family with the oldest live WAL and triggers a flush for that CF. This is evaluated in `HandleWALFull()` during `PreprocessWrite()`."
- **Reality:** `PreprocessWrite()` directly calls `SwitchWAL()`. In the non-atomic path, `SwitchWAL()` can enqueue multiple column families whose `OldestLogToKeep()` is at or before the oldest alive WAL, plus the persistent-stats CF when needed. There is no `HandleWALFull()` function in the current code.
- **Source:** `db/db_impl/db_impl_write.cc` `PreprocessWrite`; `db/db_impl/db_impl_write.cc` `SwitchWAL`; `db/db_impl/db_impl_write.cc` `MaybeFlushStatsCF`; `db/column_family.cc` `OldestLogToKeep`
- **Fix:** Document the real `SwitchWAL()` selection logic and remove the nonexistent function reference.

### [WRONG] The WriteBufferManager-triggered flush is not “flushes across column families” in the common path
- **File:** `docs/components/flush/01_triggers_and_configuration.md` / WriteBufferManager Trigger
- **Claim:** "RocksDB triggers flushes across column families to reduce total memory usage."
- **Reality:** When `atomic_flush` is disabled, `HandleWriteBufferManagerFlush()` picks a single eligible column family: the one with the oldest mutable-memtable creation sequence and no pending/running immutable flush. Only the atomic-flush path groups CFs together. A shared `WriteBufferManager` can also affect multiple DB instances, which the docs do not mention.
- **Source:** `db/db_impl/db_impl_write.cc` `HandleWriteBufferManagerFlush`; `include/rocksdb/write_buffer_manager.h` `ShouldFlush` and `ShouldStall`; `db/db_write_buffer_manager_test.cc`
- **Fix:** Split the description into non-atomic single-CF selection, atomic multi-CF behavior, and shared-WBM cross-DB behavior.

### [WRONG] `memtable_max_range_deletions` is documented with the wrong type and header
- **File:** `docs/components/flush/01_triggers_and_configuration.md` / Configuration Parameters
- **Claim:** "`memtable_max_range_deletions` | `int` | 0 | ... (see `MutableCFOptions` in `include/rocksdb/advanced_options.h`)"
- **Reality:** The public option is `uint32_t memtable_max_range_deletions` in `include/rocksdb/options.h`.
- **Source:** `include/rocksdb/options.h` `ColumnFamilyOptions::memtable_max_range_deletions`
- **Fix:** Point to the public header and correct the type.

### [MISLEADING] `max_background_flushes = -1` does not always mean “use `max_background_jobs / 4`”
- **File:** `docs/components/flush/01_triggers_and_configuration.md` / Configuration Parameters
- **Claim:** "`max_background_flushes` ... `-1` uses `max_background_jobs / 4`"
- **Reality:** That quarter-allocation logic only applies when both legacy knobs, `max_background_flushes` and `max_background_compactions`, are `-1`. In the compatibility path, an unset side is treated as `1`.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `DBImpl::GetBGJobLimits`; `include/rocksdb/options.h` comments for `max_background_flushes` and `max_background_compactions`
- **Fix:** State the condition under which the quarter-allocation rule applies, or simplify to “deprecated legacy knob; see `max_background_jobs` and compatibility rules.”

### [WRONG] Successful MemPurge does not make `FlushJob::Run()` return immediately
- **File:** `docs/components/flush/03_flush_job_lifecycle.md` / Run Phase / Decision: MemPurge vs WriteLevel0Table
- **Claim:** "If MemPurge succeeds (returns `Status::OK()`), the `base_` Version is unref'd and the method returns -- no SST file is created."
- **Reality:** On MemPurge success, `Run()` skips `WriteLevel0Table()`, but it still executes the rest of the post-processing path. In the non-atomic path it calls `TryInstallMemtableFlushResults()` with `write_edits = false` so the old memtables are removed without writing a new MANIFEST edit.
- **Source:** `db/flush_job.cc` `FlushJob::Run`; `db/memtable_list.cc` `MemTableList::TryInstallMemtableFlushResults`
- **Fix:** Explain that MemPurge success still goes through flush-result installation/removal, just without creating an SST or a flush `VersionEdit`.

### [MISLEADING] The SST-build chapter skips the `CompactionIterator` layer and makes flush look like a raw merge-to-builder pass
- **File:** `docs/components/flush/04_building_sst_file.md` / Execution Flow / Step 5
- **Claim:** "`BuildTable()` ... iterates the merged stream, calling `builder->Add(key, value)` for each entry"
- **Reality:** `BuildTable()` wraps the merged memtable iterator in `CompactionIterator`, which is where snapshot filtering, merge handling, compaction-filter application, blob-file routing, and many entry drops happen before data reaches the table builder.
- **Source:** `db/builder.cc` `BuildTable`
- **Fix:** Reframe the description around `CompactionIterator` and treat the builder loop as the final stage, not the whole algorithm.

### [WRONG] Atomic-flush installation does not rescan each CF’s immutable list for extra completed batches
- **File:** `docs/components/flush/06_atomic_flush.md` / InstallMemtableAtomicFlushResults
- **Claim:** "For each CF, iterate the immutable list from oldest to newest, collecting `VersionEdit`s from all consecutively completed memtables."
- **Reality:** `InstallMemtableAtomicFlushResults()` consumes the specific `mems_list` passed in by the caller and uses only the first memtable’s `VersionEdit` per CF. The caller’s wait loop in `AtomicFlushMemTablesToOutputFiles()` is what guarantees ordering; the install helper does not do a non-atomic-style scan for extra completed batches.
- **Source:** `db/memtable_list.cc` `InstallMemtableAtomicFlushResults`; `db/db_impl/db_impl_compaction_flush.cc` `AtomicFlushMemTablesToOutputFiles`
- **Fix:** Separate the caller-side “wait until oldest” logic from the install helper, and document that the helper commits only the selected atomic-flush batch.

### [MISLEADING] The delayed-write-rate algorithm is not just 0.8x slowdowns and 1.25x recoveries
- **File:** `docs/components/flush/07_write_stalls.md` / Delayed Write Rate
- **Claim:** "The adjustment uses exponential backoff/recovery with a factor of 0.8x/1.25x"
- **Reality:** While delay is already active, compaction-debt growth/shrink uses 0.8x/1.25x, but near-stop conditions use a stronger 0.6x penalty, and recovery from delay uses a 1.4x reward when the stall condition clears.
- **Source:** `db/column_family.cc` `SetupDelay`; `db/column_family.cc` `RecalculateWriteStallConditions`
- **Fix:** Either document the full behavior or drop the numeric summary.

### [WRONG] The UDT-retention reschedule backoff is 100 ms, not 1 ms
- **File:** `docs/components/flush/09_scheduling.md` / UDT Retention and Flush Rescheduling
- **Claim:** "The flush request is re-enqueued and the background thread sleeps briefly (`1000 microseconds`) to avoid a hot retry loop"
- **Reality:** `BackgroundCallFlush()` sleeps `100000` microseconds after a retain-UDT reschedule.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `BackgroundCallFlush`
- **Fix:** Change the documented sleep to `100000 microseconds`.

### [MISLEADING] `SwitchMemtable()`’s empty-CF log-number update is not limited to a single-CF case
- **File:** `docs/components/flush/09_scheduling.md` / SwitchMemtable / Step 6
- **Claim:** "If WAL tracking is enabled and this is a single-CF scenario, update the log number for empty CFs to allow obsolete WAL deletion."
- **Reality:** `SwitchMemtable()` updates empty-CF log numbers more generally whenever a new WAL is created. In the manifest-tracking path it can also write a WAL-deletion edit before updating empty CFs. This is not restricted to single-CF DBs.
- **Source:** `db/db_impl/db_impl_write.cc` `SwitchMemtable`
- **Fix:** Describe the general empty-CF advancement behavior and separate the manifest-tracking case.

### [WRONG] Hard background errors do not work by incrementing `bg_work_paused_`
- **File:** `docs/components/flush/10_error_handling.md` / bg_work_paused_
- **Claim:** "When a hard error occurs, `bg_work_paused_` is incremented to prevent new background work from being scheduled."
- **Reality:** Hard-error gating is done through `error_handler_.IsBGWorkStopped()` checks in the scheduler and flush paths. `bg_work_paused_` is only manipulated by explicit `PauseBackgroundWork()` / `ContinueBackgroundWork()` calls.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `MaybeScheduleFlushOrCompaction`, `PauseBackgroundWork`, `ContinueBackgroundWork`; `db/error_handler.h`
- **Fix:** Replace the `bg_work_paused_` explanation with the actual `ErrorHandler`-based gate.

### [MISLEADING] The severity table is incomplete and wrong for some soft-error paths
- **File:** `docs/components/flush/10_error_handling.md` / ErrorHandler Integration / Error Severity
- **Claim:** "`kSoftError` | Background work continues; reads succeed; writes may be delayed"
- **Reality:** `Status::Severity` also includes `kUnrecoverableError`, and some soft-error paths explicitly stop all non-recovery background work by setting `soft_error_no_bg_work_` (notably retryable `kFlushNoWAL` / `kManifestWriteNoWAL` cases). Severity mapping also depends on error reason, code, subcode, and `paranoid_checks`.
- **Source:** `include/rocksdb/status.h` `Status::Severity`; `db/error_handler.cc` `SetBGError`; `db/error_handler.cc` `HandleKnownErrors`
- **Fix:** Document `kUnrecoverableError` and call out that some soft errors still gate background work except recovery.

### [UNVERIFIABLE] The docs refer to a separate “MANIFEST rename error” classification that the code does not expose
- **File:** `docs/components/flush/10_error_handling.md` / MANIFEST Write Errors
- **Claim:** "The error may be classified as a MANIFEST write error or MANIFEST rename error"
- **Reality:** Current flush error handling maps these failures to `BackgroundErrorReason::kManifestWrite` / `kManifestWriteNoWAL`, and the code comments explicitly say it is difficult to distinguish manifest write failure from CURRENT rename failure at this layer.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `FlushMemTableToOutputFile`; `db/db_impl/db_impl_compaction_flush.cc` `AtomicFlushMemTablesToOutputFiles`
- **Fix:** Remove the rename-specific classification unless the docs explain the shared error bucket and its limitation.

## Completeness Gaps

### Iterator-driven scan-triggered flushes are not actually documented
- **Why it matters:** The docs mention `MarkForFlush()` in passing, but they do not explain the real feature behind it. Iterators can mark the active memtable for flush when scans skip too many hidden entries, which changes the practical trigger set for `FlushReason::kWriteBufferFull`.
- **Where to look:** `include/rocksdb/advanced_options.h` `memtable_op_scan_flush_trigger` and `memtable_avg_op_scan_flush_trigger`; `db/memtable.h` `MarkForFlush`; `db/db_iterator_test.cc`
- **Suggested scope:** Add a subsection to chapter 1 covering scan-triggered flushes and how the read path hands them off to the write path.

### Option sanitization and incompatible combinations are under-documented
- **Why it matters:** Several of the options this doc set emphasizes do not have literal “what the user set is what the engine uses” semantics. This is exactly the kind of detail maintainers need when debugging production configs.
- **Where to look:** `db/column_family.cc` `SanitizeCfOptions`; `db/db_impl/db_impl_open.cc` option validation; `include/rocksdb/options.h`; `include/rocksdb/advanced_options.h`
- **Suggested scope:** Expand chapter 1 and chapter 6 with the main sanitization rules: `max_write_buffer_number >= 2`, `min_write_buffer_number_to_merge` clamping and `atomic_flush` sanitization to `1`, `atomic_flush` incompatibility with `enable_pipelined_write`, and UDT-retention feature incompatibility with `atomic_flush` / concurrent memtable writes.

### Manual-flush UDT semantics are missing
- **Why it matters:** The retain-UDT reschedule path is described as a general flush-scheduling behavior, but manual flush intentionally skips UDT retention. That difference matters when operators use `DB::Flush()` expecting the same behavior as auto flush.
- **Where to look:** `db/db_impl/db_impl_compaction_flush.cc` `ShouldRescheduleFlushRequestToRetainUDT`; `db/column_family_test.cc` `ColumnFamilyRetainUDTTest`
- **Suggested scope:** Add a chapter-9 note that UDT-retention rescheduling is an automatic non-atomic background-flush behavior, not a manual-flush behavior.

### WriteBufferManager-induced stalls across DBs are missing
- **Why it matters:** The docs discuss flushes and write stalls mostly as per-DB/per-CF behavior, but a shared `WriteBufferManager` can stall writers across multiple DB instances. That is a major operational behavior difference.
- **Where to look:** `include/rocksdb/write_buffer_manager.h`; `db/db_impl/db_impl_write.cc` `PreprocessWrite`; `db/db_write_buffer_manager_test.cc`; `db/db_properties_test.cc`
- **Suggested scope:** Add a chapter-7 subsection on global write-buffer-manager stalls and their interaction with flush scheduling.

### Persistent-stats CF coupling is missing
- **Why it matters:** Automated and manual flush paths can force an extra flush of the persistent-stats column family to release old WALs. That changes which CFs participate in a flush even when the original trigger came from another CF.
- **Where to look:** `db/db_impl/db_impl_write.cc` `MaybeFlushStatsCF`; `db/db_impl/db_impl_compaction_flush.cc` `FlushMemTable`
- **Suggested scope:** Mention this in chapter 1 trigger selection and chapter 9 scheduling.

### Recovery-only flush reasons and their special semantics are missing
- **Why it matters:** `kErrorRecoveryRetryFlush` and `kCatchUpAfterErrorRecovery` are not ordinary flushes. They reuse existing immutable memtables and intentionally avoid extra `SwitchMemtable()` calls.
- **Where to look:** `include/rocksdb/listener.h` `FlushReason`; `db/db_impl/db_impl_compaction_flush.cc` `RetryFlushesForErrorRecovery`; `db/db_impl/db_impl.cc` `ResumeImpl`
- **Suggested scope:** Add a chapter-10 subsection covering recovery flush submission and catch-up flushes after successful recovery.

## Depth Issues

### The SST-build chapter is too shallow about what actually decides the flush output
- **Current:** The chapter mostly describes iterator creation, merging, and a `BuildTable()` call.
- **Missing:** It does not explain that `CompactionIterator` is the central transformation stage for flush output, including snapshot filtering, merge handling, compaction-filter application, and blob-file routing.
- **Source:** `db/builder.cc` `BuildTable`

### The docs do not explain why non-atomic flush picks memtables before `NotifyOnFlushBegin()`
- **Current:** The lifecycle/scheduling chapters describe memtable picking and listener callbacks independently.
- **Missing:** The non-atomic path intentionally delays `NotifyOnFlushBegin()` until after `PickMemTable()` when closed-WAL sync is unnecessary, because listener callbacks release the DB mutex and could otherwise allow a new snapshot that the flush job’s snapshot set does not know about.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `FlushMemTableToOutputFile`

### Commit/install timing for listeners is not spelled out
- **Current:** The commit chapter says SuperVersion installation happens after `LogAndApply()`, but it does not discuss listener timing.
- **Missing:** `OnFlushCompleted()` is fired only after the flush result is committed, and one flush thread can commit another flush thread’s result before the listener fires. That behavior is tested explicitly.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc` `NotifyOnFlushCompleted`; `db/db_flush_test.cc` `FireOnFlushCompletedAfterCommittedResult`

## Structure and Style Violations

### Inline code quoting is pervasive across the whole doc set
- **File:** `docs/components/flush/index.md` and all chapter files
- **Details:** The review brief for this doc family explicitly says "NO inline code quotes." These docs use inline code formatting for function names, option names, paths, enum values, and ordinary prose almost everywhere. This is a systematic style violation, not an isolated one.

## Undocumented Complexity

### Forced stats-CF flushes to release WALs
- **What it is:** Flush selection can opportunistically add the persistent-stats column family so it does not keep older WALs alive after another CF flushes.
- **Why it matters:** It is an unexpected cross-CF side effect when debugging why “extra” flushes happen during WAL-full, WriteBufferManager, or manual flush flows.
- **Key source:** `db/db_impl/db_impl_write.cc` `MaybeFlushStatsCF`; `db/db_impl/db_impl_compaction_flush.cc` `FlushMemTable`
- **Suggested placement:** Chapter 1 or chapter 9

### Snapshot-correct listener ordering in the non-atomic flush path
- **What it is:** The non-atomic flush path intentionally avoids releasing the DB mutex between snapshot-context capture and memtable picking. When closed-WAL sync is not needed, it does this by calling `PickMemTable()` before `NotifyOnFlushBegin()`.
- **Why it matters:** Without this ordering, a new snapshot could appear after snapshot capture but before memtable selection, and the flush could incorrectly drop keys needed by that snapshot.
- **Key source:** `db/db_impl/db_impl_compaction_flush.cc` `FlushMemTableToOutputFile`
- **Suggested placement:** Chapter 3 or chapter 4

### MemPurge depends on memtable-rep support for random sampling
- **What it is:** `MemPurgeDecider()` calls `MemTable::UniqueRandomSample()`, and the default `MemTableRep` interface implementation asserts. Support is implemented only by specific reps.
- **Why it matters:** The docs describe MemPurge as a general flush alternative, but there is an implementation-level dependency on memtable-rep capabilities that can surprise anyone experimenting with non-default memtable factories.
- **Key source:** `db/flush_job.cc` `MemPurgeDecider`; `db/memtable.h` `MemTable::UniqueRandomSample`; `include/rocksdb/memtablerep.h` `MemTableRep::UniqueRandomSample`
- **Suggested placement:** Chapter 8

### Recovery can require a follow-up “catch-up” flush after success
- **What it is:** After successful automatic recovery, RocksDB schedules a `kCatchUpAfterErrorRecovery` flush pass because non-recovery flush requests are dropped while recovery is in progress and new memtables may have filled meanwhile.
- **Why it matters:** Without knowing about this extra phase, a maintainer can misread post-recovery flush activity as redundant or buggy.
- **Key source:** `db/db_impl/db_impl.cc` `ResumeImpl`; `db/db_impl/db_impl_compaction_flush.cc` `RetryFlushesForErrorRecovery`
- **Suggested placement:** Chapter 10

## Positive Notes

- `docs/components/flush/index.md` is exactly within the requested size budget and follows the expected overview/source-files/chapter-table/characteristics/invariants structure.
- `docs/components/flush/02_memtable_selection.md` correctly calls out the non-consecutive-selection hazard after rollback and ties it to FIFO commit safety.
- `docs/components/flush/08_mempurge.md` usefully documents the recent memtable-ID-ordering concern instead of pretending MemPurge is a trivial “skip the SST” fast path.
