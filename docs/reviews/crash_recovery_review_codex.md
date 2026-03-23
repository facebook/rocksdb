# Review: crash_recovery — Codex

## Summary
Overall quality rating: significant issues

The chapter split is reasonable and the docs cover the right high-level areas: open-time recovery, WAL replay, best-efforts recovery, background errors, WAL verification, and repair. The index is the right size, and every chapter has a `Files:` line, which makes the set navigable.

The main problem is factual accuracy. Several chapters describe the wrong control flow or overstate guarantees that the current code does not provide, especially around open-time side effects, best-efforts recovery, recovery-mode semantics, listener control of error recovery, and repair behavior. The docs also miss several 2024-2026 changes that materially affect crash-recovery debugging, including the sentinel empty-WAL write after corrupted-log recovery, async SST opening, and the recent WriteBufferManager/read-only recovery fixes.

## Correctness Issues

### [WRONG] Recovery is not rollback-atomic at the filesystem level
- **File:** `docs/components/crash_recovery/01_recovery_overview.md` — Recovery Atomicity; repeated in `docs/components/crash_recovery/index.md` — Key Invariants
- **Claim:** "Recovery is atomic from the user's perspective. Either `DB::Open()` succeeds with a consistent state, or it returns an error and the database remains unopened. If recovery fails partway through (e.g., WAL replay encounters corruption with `kAbsoluteConsistency`), the database directory is left unchanged and the lock is released."
- **Reality:** `DBImpl::Open()` can mutate on-disk state before it finishes successfully. The open path can create a new WAL, write and sync an empty sentinel `WriteBatch`, append recovery edits to the MANIFEST, truncate retained WALs, write `DeleteWalsBefore` / `SetMinLogNumberToKeep` edits, and delete obsolete files. If a later step fails, no DB handle is returned, but the directory is not guaranteed to be unchanged.
- **Source:** `db/db_impl/db_impl_open.cc` (`DBImpl::Open`, `DBImpl::Recover`, `DBImpl::LogAndApplyForRecovery`, `DBImpl::MaybeFlushFinalMemtableOrRestoreActiveLogFiles`)
- **Fix:** Rephrase this as "open either returns a usable DB handle or an error," and explicitly say recovery is not side-effect free or rollback-atomic on disk.

### [WRONG] BER does not simply use the first non-empty MANIFEST
- **File:** `docs/components/crash_recovery/05_best_efforts_recovery.md` — CURRENT File Bypass
- **Claim:** "With BER, if CURRENT is missing, it scans the database directory for any file matching `MANIFEST-*` with non-zero size. The first non-empty MANIFEST found is used for recovery."
- **Reality:** BER bypasses `CURRENT` by listing the DB directory and calling `VersionSet::TryRecover()`. `ManifestPicker` collects all MANIFEST files, sorts them by file number descending, and `TryRecover()` retries them one by one until one succeeds. The first file encountered is not the actual recovery policy.
- **Source:** `db/db_impl/db_impl_open.cc` (`DBImpl::Recover`), `db/version_set.cc` (`ManifestPicker`, `VersionSet::TryRecover`)
- **Fix:** Document BER as "retry newest MANIFESTs until one recovers," not "use the first non-empty MANIFEST found."

### [WRONG] BER chapter overstates what kind of state can be recovered
- **File:** `docs/components/crash_recovery/05_best_efforts_recovery.md` — Usage Considerations
- **Claim:** "BER always recovers to a valid point-in-time consistent state, even if data is lost. The recovered state corresponds to some MANIFEST snapshot where all referenced files exist."
- **Reality:** Current BER can recover an incomplete version with only a suffix of L0 files missing when there is no atomic-flush history blocking that optimization. That recovered state is not necessarily identical to any literal MANIFEST snapshot. BER can also fall back to a trivial or effectively empty state if that is the latest valid recoverable state. Atomic groups are treated all-or-nothing across pre-existing column families.
- **Source:** `include/rocksdb/options.h` (`DBOptions::best_efforts_recovery`), `db/version_set.cc` (`VersionSet::TryRecover`), `db/version_edit_handler.cc` (`VersionEditHandlerPointInTime`)
- **Fix:** Describe BER in terms of recovering an internally consistent usable state, then explicitly call out the L0-suffix exception, empty-state possibility, and atomic-flush limits.

### [MISLEADING] MANIFEST recovery chapter overstates SST validation during open
- **File:** `docs/components/crash_recovery/02_manifest_recovery.md` — SST File Verification
- **Claim:** "During MANIFEST recovery (without `best_efforts_recovery`), RocksDB verifies that all SST files referenced by VersionEdits exist on disk." / "When `verify_sst_unique_id_in_manifest` is enabled ... RocksDB also verifies that SST file unique IDs match those recorded in the MANIFEST"
- **Reality:** This depends on when SSTs are actually opened. `VersionSet::Recover()` passes `skip_load_table_files = db_options_->open_files_async`, so with `open_files_async=true` the table-loading path is deferred and missing/corrupt SSTs can surface after `DB::Open()`. Unique-ID verification also happens only when a file is opened, and the option comment explicitly says verifying every SST during `DB::Open()` is no longer guaranteed.
- **Source:** `db/version_set.cc` (`VersionSet::Recover`), `db/version_edit_handler.cc` (`VersionEditHandler::LoadTables`), `include/rocksdb/options.h` (`DBOptions::open_files_async`, `DBOptions::verify_sst_unique_id_in_manifest`)
- **Fix:** Reword this as file-open-time validation with an explicit `open_files_async` caveat, not universal MANIFEST-replay validation.

### [WRONG] Missing-column-family handling is attributed to the wrong component
- **File:** `docs/components/crash_recovery/02_manifest_recovery.md` — Column Family Discovery
- **Claim:** "The `VersionEditHandler` validates that all column families specified by the user in `DB::Open()` exist in the MANIFEST, and reports an error for unknown column families unless `create_missing_column_families=true`."
- **Reality:** `VersionEditHandler::CheckIterationResult()` errors when the MANIFEST contains column families the caller did not open, unless the DB is opened read-only. `create_missing_column_families` is handled later in `DBImpl::Open()`, after recovery, where requested column families missing from the recovered MANIFEST are created.
- **Source:** `db/version_edit_handler.cc` (`VersionEditHandler::CheckIterationResult`), `db/db_impl/db_impl_open.cc` (`DBImpl::Open`)
- **Fix:** Split this into two behaviors: MANIFEST-time validation of unopened existing CFs, and post-recovery creation of requested missing CFs.

### [WRONG] The cross-CF inconsistency check is not part of tolerate-tail recovery
- **File:** `docs/components/crash_recovery/04_wal_recovery_modes.md` — Cross-CF Consistency Check
- **Claim:** "After WAL replay stops due to corruption (in `kPointInTimeRecovery` or `kTolerateCorruptedTailRecords`), `MaybeHandleStopReplayForCorruptionForInconsistency()` checks that no column family has SST files containing data beyond the corruption point."
- **Reality:** `stop_replay_for_corruption` is set in `HandleNonOkStatusOrOldLogRecord()` only for `kPointInTimeRecovery`. In `kTolerateCorruptedTailRecords`, the error is returned instead of entering the PIT stop-and-check path. The helper still mentions tolerate-tail internally, but current control flow does not reach it with the flag set for that mode.
- **Source:** `db/db_impl/db_impl_open.cc` (`DBImpl::HandleNonOkStatusOrOldLogRecord`, `DBImpl::MaybeHandleStopReplayForCorruptionForInconsistency`)
- **Fix:** Scope the cross-CF inconsistency discussion to PIT recovery, or explain the current flag/control-flow mismatch explicitly.

### [MISLEADING] Recovery-mode summaries omit the `paranoid_checks` gate
- **File:** `docs/components/crash_recovery/04_wal_recovery_modes.md` — WALRecoveryMode Enum; kAbsoluteConsistency; kPointInTimeRecovery
- **Claim:** "`kAbsoluteConsistency` | ... Fail on any WAL corruption" / "`kPointInTimeRecovery` | ... Stop replay at first corruption; recover to valid point-in-time"
- **Reality:** Those summaries are only reliable when `paranoid_checks=true`. `DBImpl::InitializeLogReader()` sets `reporter->status = nullptr` when `paranoid_checks=false` or when the mode is `kSkipAnyCorruptedRecords`, and `DBImpl::MaybeIgnoreError()` clears many recovery errors in non-paranoid mode.
- **Source:** `db/db_impl/db_impl_open.cc` (`DBImpl::InitializeLogReader`, `DBOpenLogRecordReadReporter::Corruption`), `db/db_impl/db_impl.cc` (`DBImpl::MaybeIgnoreError`)
- **Fix:** Fold the `paranoid_checks` dependency into the per-mode descriptions instead of leaving it only in the reporter subsection.

### [WRONG] Listener section assigns auto-recovery control to the wrong callback
- **File:** `docs/components/crash_recovery/08_background_error_handling.md` — Listener Notifications
- **Claim:** "The `EventListener::OnBackgroundError()` callback ... Listeners can: ... Override the auto-recovery decision by modifying the `auto_recovery` flag"
- **Reality:** `OnBackgroundError()` only receives `(BackgroundErrorReason, Status*)`. The `auto_recovery` flag is exposed to `OnErrorRecoveryBegin()`, and only that callback can modify it.
- **Source:** `include/rocksdb/listener.h` (`EventListener::OnBackgroundError`, `EventListener::OnErrorRecoveryBegin`), `db/event_helpers.cc` (`EventHelpers::NotifyOnBackgroundError`)
- **Fix:** Move auto-recovery control to the `OnErrorRecoveryBegin()` description and keep `OnBackgroundError()` limited to inspecting or suppressing the status.

### [MISLEADING] Retryable-I/O auto-resume is not actually listener-controlled today
- **File:** `docs/components/crash_recovery/08_background_error_handling.md` — Retryable I/O Error Handling; Listener Notifications
- **Claim:** "Listeners can: ... Override the auto-recovery decision by modifying the `auto_recovery` flag"
- **Reality:** In the retryable-I/O branch of `ErrorHandler::SetBGError()`, RocksDB computes `auto_recovery`, notifies listeners, and then unconditionally calls `StartRecoverFromRetryableBGIOError(...)`. Listener changes to `auto_recovery` do not suppress that recovery thread today.
- **Source:** `db/error_handler.cc` (`ErrorHandler::SetBGError`), `db/event_helpers.cc` (`EventHelpers::NotifyOnBackgroundError`)
- **Fix:** Either document this branch-specific limitation explicitly or change the code and then update the docs to match. The current docs overpromise listener control.

### [WRONG] Manual soft-error recovery is not always "clear without flushing"
- **File:** `docs/components/crash_recovery/08_background_error_handling.md` — Manual Recovery
- **Claim:** "For soft errors, simply clears the error without flushing"
- **Reality:** `ErrorHandler::RecoverFromBGError()` only takes the no-flush fast path when the severity is `kSoftError` and `recover_context_.flush_reason == FlushReason::kErrorRecovery`. Manual resume after the `soft_error_no_bg_work_` path sets `FlushReason::kErrorRecoveryRetryFlush`, so `DB::Resume()` flushes instead of simply clearing the error.
- **Source:** `db/error_handler.cc` (`ErrorHandler::RecoverFromBGError`)
- **Fix:** Distinguish ordinary soft errors from the retryable-flush path, where manual resume does require a flush.

### [WRONG] Repair chapter misdescribes both archiving and unreadable-SST handling
- **File:** `docs/components/crash_recovery/10_database_repair.md` — Phase 2: Convert WALs to Tables; Phase 3: Extract Metadata; Limitations and Caveats
- **Claim:** "Archive the processed WAL file (renamed with `.archive` extension, not deleted)" / "Files that cannot be scanned (corrupted SSTs) are silently ignored." / "- **Data loss possible:** Corrupted WAL records and unreadable SST files are silently skipped"
- **Reality:** `Repairer::ArchiveFile()` moves files into a `lost/` subdirectory with the same filename; it does not add a `.archive` suffix. Corrupted or unreadable SSTs are logged with warnings or errors before being archived into `lost/`; they are not silent.
- **Source:** `db/repair.cc` (`Repairer::ConvertLogFilesToTables`, `Repairer::ExtractMetaData`, `Repairer::ArchiveFile`)
- **Fix:** Replace ".archive" with "`lost/` relocation" and describe unreadable files as "logged and archived," not "silently ignored."

## Completeness Gaps

### Async SST opening and late error surfacing
- **Why it matters:** This is the most important cross-component boundary missing from the docs. With `open_files_async=true`, some corruption and missing-file problems no longer surface in `DB::Open()` and instead appear later as background errors or read/compaction failures.
- **Where to look:** `include/rocksdb/options.h` (`DBOptions::open_files_async`), `db/version_set.cc` (`VersionSet::Recover`), `db/version_edit_handler.cc` (`VersionEditHandler::LoadTables`), `db/db_impl/db_impl_open.cc` (`DBImpl::Open`, `ScheduleAsyncFileOpening`)
- **Suggested scope:** Add a focused subsection to chapter 2 and cross-link it from chapter 1.

### BER semantics beyond "recover an older point in time"
- **Why it matters:** BER is one of the hardest recovery modes to reason about, and the current docs do not explain the real outcome space: multi-MANIFEST retry, incomplete L0-suffix recovery, empty/trivial recovery, and atomic-group all-or-nothing behavior.
- **Where to look:** `include/rocksdb/options.h` (`DBOptions::best_efforts_recovery`), `db/version_set.cc` (`ManifestPicker`, `VersionSet::TryRecover`), `db/version_edit_handler.cc` (`VersionEditHandlerPointInTime`)
- **Suggested scope:** Expand chapter 5 substantially rather than trying to patch it with a couple of sentences.

### Read-only and shared-WriteBufferManager recovery behavior
- **Why it matters:** Recent fixes changed the recovery path to avoid duplicate `ScheduleWork()` assertions in read-only mode and when multiple DBs share a `WriteBufferManager`. This is exactly the kind of operational nuance the crash-recovery docs should preserve.
- **Where to look:** `db/db_impl/db_impl_open.cc` (`DBImpl::InsertLogRecordToMemtable`, `DBImpl::MaybeWriteLevel0TableForRecovery`), `db/db_write_buffer_manager_test.cc`
- **Suggested scope:** Add a subsection to chapters 3 and 7.

### Test coverage reality for WAL-verification features
- **Why it matters:** Chapter 9 currently reads as if stress coverage is broad and symmetric, but the actual harness coverage is uneven. That matters when readers interpret how battle-tested each option is.
- **Where to look:** `tools/db_crashtest.py`, `db_stress_tool/db_stress_test_base.cc`
- **Suggested scope:** Add a short "test coverage notes" subsection to chapter 9.

### Sentinel WAL write after corrupted-log recovery
- **Why it matters:** This is a subtle but central correctness mechanism for surviving a second crash after PIT recovery. Anyone debugging the MANIFEST/WAL ordering here needs the docs to mention it.
- **Where to look:** `db/db_impl/db_impl_open.cc` (`DBImpl::Open`), `db/corruption_test.cc` (`CrashDuringRecovery`, `TxnDbCrashDuringRecovery`)
- **Suggested scope:** Add to chapter 1 or chapter 4, with a cross-link from chapter 7.

## Depth Issues

### Post-recovery ordering needs the real control flow
- **Current:** The overview says open does MANIFEST recovery, WAL replay, optional flush, and then returns.
- **Missing:** The important ordering details are absent: create new WAL, optionally write and sync the sentinel empty batch, persist recovery edits via `LogAndApplyForRecovery()`, then delete/truncate obsolete files and optionally schedule async SST opening.
- **Source:** `db/db_impl/db_impl_open.cc` (`DBImpl::Open`, `DBImpl::LogAndApplyForRecovery`, `DBImpl::MaybeFlushFinalMemtableOrRestoreActiveLogFiles`)

### BER chapter needs actual `TryRecover()` mechanics
- **Current:** Chapter 5 reduces BER to "scan for a MANIFEST, then recover an earlier point in time."
- **Missing:** There is not enough detail on newest-first MANIFEST retry, how `TryRecoverFromOneManifest()` interacts with missing files, why incomplete L0 suffixes are allowed only outside atomic-flush history, and how atomic groups are treated.
- **Source:** `db/version_set.cc` (`ManifestPicker`, `VersionSet::TryRecover`, `VersionSet::TryRecoverFromOneManifest`), `db/version_edit_handler.cc` (`VersionEditHandlerPointInTime`)

### Background-error chapter flattens several distinct recovery paths
- **Current:** Chapter 8 presents error handling as one mostly uniform severity-and-resume workflow.
- **Missing:** The code has meaningfully different branches for ordinary background errors, retryable I/O, no-space recovery through `SstFileManager`, and manual resume after `soft_error_no_bg_work_`. The docs need the branch structure, not just a summary table.
- **Source:** `db/error_handler.cc` (`ErrorHandler::SetBGError`, `ErrorHandler::OverrideNoSpaceError`, `ErrorHandler::RecoverFromBGError`, `ErrorHandler::StartRecoverFromRetryableBGIOError`)

## Structure and Style Violations

### Pervasive inline code quotes violate the requested doc style
- **File:** `docs/components/crash_recovery/*.md`
- **Details:** The prompt for this doc set explicitly forbids inline code quotes, but these chapters wrap almost every option, type, function, file path, and enum in backticks. This is systemic, not a spot-fix issue.

### "Key Invariant" is used for non-invariants and false statements
- **File:** `docs/components/crash_recovery/index.md`, `docs/components/crash_recovery/01_recovery_overview.md`, `docs/components/crash_recovery/02_manifest_recovery.md`, `docs/components/crash_recovery/05_best_efforts_recovery.md`
- **Details:** Several sections label ordinary behavior summaries as invariants, and some of those statements are not even true in the current code. In particular, "recovery is atomic," "CURRENT always points to a valid MANIFEST," and the BER invariant are not the sort of enforced corruption-prevention invariants the style guide calls for.

## Undocumented Complexity

### Sentinel empty batch after corrupted-WAL recovery
- **What it is:** After WAL recovery stops at corruption and produces `recovered_seq != kMaxSequenceNumber`, open creates a new WAL, writes an empty `WriteBatch` at `recovered_seq`, flushes it, and syncs the WAL before persisting recovery edits.
- **Why it matters:** This is the mechanism that makes a second crash after PIT recovery behave correctly. Without understanding it, the subsequent MANIFEST/WAL state looks inexplicable.
- **Key source:** `db/db_impl/db_impl_open.cc` (`DBImpl::Open`)
- **Suggested placement:** Add to chapter 4 with a short end-to-end note in chapter 1.

### BER is a search procedure, not a single alternate open path
- **What it is:** BER can walk multiple MANIFESTs newest-first, accept incomplete versions with a missing L0 suffix in limited cases, and reject partially recovered atomic groups.
- **Why it matters:** Readers debugging backup restore failures or missing-file recovery need to know what BER will actually search and what consistency properties it still preserves.
- **Key source:** `db/version_set.cc` (`ManifestPicker`, `VersionSet::TryRecover`), `db/version_edit_handler.cc` (`VersionEditHandlerPointInTime`), `include/rocksdb/options.h` (`DBOptions::best_efforts_recovery`)
- **Suggested placement:** Expand chapter 5.

### Async file opening changes when corruption is observed
- **What it is:** `open_files_async` pushes part of SST opening and validation into background work after `DB::Open()` returns.
- **Why it matters:** This changes the boundary between crash recovery, file I/O, and background error handling. Without it, the docs mislead users into expecting all recovery-time file issues to surface synchronously during open.
- **Key source:** `include/rocksdb/options.h` (`DBOptions::open_files_async`), `db/version_set.cc` (`VersionSet::Recover`), `db/db_impl/db_impl_open.cc` (`DBImpl::Open`, `ScheduleAsyncFileOpening`)
- **Suggested placement:** Mention in chapters 1, 2, and 8.

### WBM-enforced recovery has recent read-only and shared-DB edge cases
- **What it is:** Recovery can schedule flushes mid-replay to respect a shared `WriteBufferManager`, but read-only mode must skip scheduling because the scheduler would never be drained.
- **Why it matters:** This is a concrete concurrency and resource-management edge case that already produced debug assertions and dedicated tests. It belongs in crash-recovery docs because it changes what `avoid_flush_during_recovery` really means.
- **Key source:** `db/db_impl/db_impl_open.cc` (`DBImpl::InsertLogRecordToMemtable`, `DBImpl::MaybeWriteLevel0TableForRecovery`), `db/db_write_buffer_manager_test.cc`
- **Suggested placement:** Add to chapters 3 and 7.

## Positive Notes

- The overall chapter split is sensible, and `index.md` is within the requested 40-80 line range.
- Chapter 7 correctly captures that `enforce_write_buffer_manager_during_recovery` can override `avoid_flush_during_recovery`, and that once a WBM-triggered flush happens the remaining memtables are flushed too.
- Chapter 9 correctly calls out that live-WAL syncs are not tracked by `track_and_verify_wals_in_manifest`, and that old tracking state is cleared with `DeleteWalsBefore()` when tracking is disabled.
