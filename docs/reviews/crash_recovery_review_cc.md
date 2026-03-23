# Review: crash_recovery -- Claude Code

## Summary
(Overall quality rating: good)

The crash_recovery documentation is well-structured and largely accurate. Across 10 chapters, the vast majority of factual claims (90%+) are verified correct against the current codebase. The documentation covers the full recovery lifecycle from DB::Open() through MANIFEST recovery, WAL replay, error handling, and repair. The strongest chapters are WAL Recovery (Ch3), WAL Recovery Modes (Ch4), and WAL Verification (Ch9) -- all verified 100% correct. The main issues are: one factually WRONG claim (repair archive mechanism), several MISLEADING claims (especially in Ch8 and Ch10), and significant undocumented complexity around recovery variants, BlobDB interaction, and the dummy-write-after-corruption mechanism.

## Correctness Issues

### [WRONG] Repair archives WAL files with ".archive" extension
- **File:** 10_database_repair.md, Phase 2 section
- **Claim:** "Archive the processed WAL file (renamed with `.archive` extension, not deleted)"
- **Reality:** `ArchiveFile()` in `db/repair.cc` (lines 773-791) moves files to a `lost/` subdirectory (e.g., `dbname/lost/filename`), NOT renamed with a `.archive` extension. The function creates `dir + "/lost"` and renames the file there.
- **Source:** `db/repair.cc`, `Repairer::ArchiveFile()`
- **Fix:** Replace ".archive extension" with "moved to a `lost/` subdirectory within the database directory"

### [WRONG] OnBackgroundError can override auto_recovery flag
- **File:** 08_background_error_handling.md, "Listener Notifications" section
- **Claim:** "Listeners can: Override the auto-recovery decision by modifying the `auto_recovery` flag"
- **Reality:** `OnBackgroundError()` in `include/rocksdb/listener.h` (line 800) receives only `BackgroundErrorReason` and `Status*`. It can suppress errors by setting status to OK but has NO `auto_recovery` parameter. The `auto_recovery` flag is modifiable only via `OnErrorRecoveryBegin()` (line 848), which is a separate callback.
- **Source:** `include/rocksdb/listener.h`, lines 800-801 vs 848-850
- **Fix:** Split the bullet list: `OnBackgroundError()` can suppress errors via `Status::OK()`. `OnErrorRecoveryBegin()` can override auto-recovery via `bool* auto_recovery`. These are separate callbacks.

### [MISLEADING] BER "first non-empty MANIFEST found"
- **File:** 05_best_efforts_recovery.md, "CURRENT File Bypass" section
- **Claim:** "The first non-empty MANIFEST found is used for recovery"
- **Reality:** In `DBImpl::Recover()` (db_impl_open.cc lines 456-466), the code iterates `GetChildren()` results and `break`s on the first match. The iteration order depends on the filesystem's directory listing order, which is typically NOT sorted. This means an older, smaller MANIFEST could be selected over a newer one. In contrast, `VersionSet::TryRecover()` in version_set.cc sorts MANIFEST files descending and tries the highest-numbered one first.
- **Source:** `db/db_impl/db_impl_open.cc` lines 444-470; `db/version_set.cc` TryRecover
- **Fix:** Clarify that the MANIFEST selection in DBImpl::Recover is filesystem-order-dependent, while TryRecover internally sorts and tries the most recent MANIFEST first.

### [MISLEADING] RecoverLogFiles flow structure
- **File:** 03_wal_recovery.md, "RecoverLogFiles() Flow" section
- **Claim:** Steps 1-4 are presented as direct steps of RecoverLogFiles()
- **Reality:** `RecoverLogFiles()` calls three functions: `SetupLogFilesRecovery()`, `ProcessLogFiles()`, and `FinishLogFilesRecovery()`. Steps 2-4 described in the doc (ProcessLogFiles iteration, MaybeHandleStopReplayForCorruptionForInconsistency, MaybeFlushFinalMemtableOrRestoreActiveLogFiles) are actually internal steps of `ProcessLogFiles()`, not direct children of `RecoverLogFiles()`. Also, `FinishLogFilesRecovery()` is omitted entirely.
- **Source:** `db/db_impl/db_impl_open.cc` lines 1128-1145
- **Fix:** Restructure to show RecoverLogFiles -> {SetupLogFilesRecovery, ProcessLogFiles, FinishLogFilesRecovery}. Move Steps 2-4 under ProcessLogFiles as sub-steps.

### [MISLEADING] Repair four-phase process order
- **File:** 10_database_repair.md, "Four-Phase Repair Process" section
- **Claim:** Repair executes sequentially: Phase 1 (Find Files) -> Phase 2 (Convert WALs) -> Phase 3 (Extract Metadata) -> Phase 4 (Write New MANIFEST)
- **Reality:** The actual `Repairer::Run()` order is: (1) FindFiles, (2) archive old MANIFESTs, (3) create fresh MANIFEST via `NewDB()` + `vset_.Recover()`, (4) ExtractMetaData on existing SSTs, (5) ConvertLogFilesToTables, (6) ExtractMetaData again for new tables, (7) AddTables (incremental MANIFEST updates via LogAndApply). The MANIFEST is created BEFORE WAL conversion, not as a final step. ExtractMetaData runs twice.
- **Source:** `db/repair.cc`, `Repairer::Run()` lines 197-247
- **Fix:** Reorder the description to match the actual execution flow, noting that MANIFEST creation happens early and is incrementally updated.

### [MISLEADING] Corrupted SSTs "silently ignored" in repair
- **File:** 10_database_repair.md, Phase 3 section
- **Claim:** "Files that cannot be scanned (corrupted SSTs) are silently ignored"
- **Reality:** Failed SSTs produce `ROCKS_LOG_WARN` output and are archived to the `lost/` directory. Not silent.
- **Source:** `db/repair.cc` lines 518-527
- **Fix:** Change "silently ignored" to "logged as warnings and archived to the `lost/` directory"

### [MISLEADING] MANIFEST provides min_log_number_to_keep "determines which WALs to replay"
- **File:** 01_recovery_overview.md, "Why MANIFEST Before WAL" section
- **Claim:** "The MANIFEST provides `min_log_number_to_keep`, which determines which WALs to replay"
- **Reality:** `min_log_number_to_keep` is a lower bound. In non-2PC mode, the actual cutoff is `max(MinLogNumberToKeep(), MinLogNumberWithUnflushedData())`. Per-CF log numbers also participate. The claim oversimplifies the WAL selection logic.
- **Source:** `db/db_impl/db_impl_open.cc`, `SetupLogFilesRecovery()` lines 1177-1182
- **Fix:** Say "The MANIFEST provides `min_log_number_to_keep` and per-CF log numbers, which together determine the minimum WAL needed for recovery."

### [MISLEADING] 2PC avoid_flush_during_recovery reasoning
- **File:** 06_two_phase_commit_recovery.md, "2PC WAL Recovery Differences" section
- **Claim:** "2PC can create gaps in sequence numbers (prepared transactions reserve sequence numbers but don't make them visible until commit)"
- **Reality:** The code comment says "with 2PC we have no guarantee that consecutive log files have consecutive sequence id, which make recovery complicated." This is about non-consecutive sequence IDs across WAL file boundaries, not "gaps" in sequence numbers. The distinction matters: it's about WAL file sequencing, not sequence number allocation.
- **Source:** `db/db_impl/db_impl_open.cc` lines 148-150
- **Fix:** Rephrase to match the code comment: "2PC does not guarantee consecutive log files have consecutive sequence IDs, which complicates recovery."

### [MISLEADING] create_unknown_cfs as a user-facing parameter
- **File:** 10_database_repair.md, "API Variants" section
- **Claim:** "The `create_unknown_cfs` parameter controls whether column families found in existing SST files but not specified by the caller are created automatically."
- **Reality:** `create_unknown_cfs` is an internal boolean in the `Repairer` constructor, not a user-facing parameter. The behavior is implicitly controlled by which `RepairDB()` overload the user calls: the overload with `unknown_cf_opts` passes `true` internally.
- **Source:** `db/repair.cc` line 95 (constructor), line 267 (member field)
- **Fix:** Remove the parameter name reference. Instead describe the behavior per overload: "The overload accepting `unknown_cf_opts` automatically creates CFs found in SSTs but not specified by the caller."

## Completeness Gaps

### Recovery retry scope (FS kVerifyAndReconstructRead)
- **Why it matters:** Developers debugging recovery failures need to know what the retry mechanism covers.
- **Where to look:** `db/db_impl/db_impl_open.cc` lines 539-563, 1396-1399
- **Suggested scope:** Correct and expand the existing "Recovery Retry" section in Ch1. The doc says retry is triggered by MANIFEST corruption, which is correct. But it should also note that when triggered, the retry enables `verify_and_reconstruct_read` for WAL reads too (via `is_retry` flag passed to `InitializeLogReader()`). WAL-only corruption does NOT trigger a retry.

### Dummy write after corrupted WAL recovery
- **Why it matters:** Critical for correctness of repeated crash-recovery cycles with kPointInTimeRecovery. Without understanding this, someone modifying the recovery flow could break it.
- **Where to look:** `db/db_impl/db_impl_open.cc` lines 2513-2548
- **Suggested scope:** Add to Ch3 or Ch4. After DB::Open() recovers from a corrupted WAL, a dummy empty WriteBatch is written to the new WAL with the recovered sequence number, then flushed and synced. This provides the consecutive sequence number hint that MaybeReviseStopReplayForCorruption() uses on subsequent opens.

### BlobDB files during recovery
- **Why it matters:** Users with `enable_blob_files=true` need to know that recovery flushes produce both SST and blob files, affecting disk space.
- **Where to look:** `db/db_impl/db_impl_open.cc` lines 2012, 2105-2108, 2186-2188
- **Suggested scope:** Brief mention in Ch7 (Post-Recovery Flush)

### Auto-recovery disabled for multi-path DBs
- **Why it matters:** Users with multiple `db_paths` silently lose auto-recovery capability from no-space errors.
- **Where to look:** `db/db_impl/db_impl_open.cc` lines 2463-2467; `ErrorHandler::EnableAutoRecovery()` is only called when `paths.size() <= 1`
- **Suggested scope:** Add to Ch8 (Background Error Handling) as a documented limitation

### Read-only and CompactedDB recovery variants
- **Why it matters:** `DB::OpenForReadOnly()` first tries CompactedDBImpl (optimized for fully compacted DBs), then falls back to DBImplReadOnly. Both skip WAL replay, directory creation, and locking. Error behavior differs.
- **Where to look:** `db/db_impl/db_impl_readonly.cc`, `db/db_impl/compacted_db_impl.cc`
- **Suggested scope:** Brief section in Ch1

### OpenAndTrimHistory recovery variant
- **Why it matters:** Recovery-adjacent feature that modifies DB state during open for user-defined timestamp rollback.
- **Where to look:** `db/db_impl/db_impl_open.cc` lines 2277-2337
- **Suggested scope:** Mention in Ch1 as an alternative Open flow

### Follower mode recovery (2024+)
- **Why it matters:** Entirely new recovery mode (MANIFEST-only, hard links to leader files, periodic catch-up). Options: `follower_refresh_catchup_period_ms`, `follower_catchup_retry_count`, `follower_catchup_retry_wait_ms`.
- **Where to look:** `db/db_impl/db_impl_follower.cc`, `include/rocksdb/options.h` lines 1712-1732
- **Suggested scope:** Cross-reference from Ch1 to secondary_instance docs; or add to secondary_instance component docs

### Multi-threaded table loading and async file open during recovery
- **Why it matters:** While WAL replay is single-threaded, SST file opening during MANIFEST recovery IS multi-threaded via `LoadTableHandlersHelper()` (up to `max_file_opening_threads`, default 16). With `open_files_async=true`, table loading is deferred to a background thread after `DB::Open()` returns -- errors then surface as background errors, not Open failures.
- **Where to look:** `db/version_util.cc` lines 21-94; `db/db_impl/db_impl_open.cc` lines 2765-2861; `include/rocksdb/options.h` lines 783-803
- **Suggested scope:** Add to Ch1 or Ch2

## Depth Issues

### CheckSeqnoNotSetBackDuringRecovery called at two granularities
- **Current:** Ch3 says the check happens "after each WAL file"
- **Missing:** The check is also called per-record inside `ProcessLogFile()` (line 1333). The doc only describes the per-WAL-file check (line 1214).
- **Source:** `db/db_impl/db_impl_open.cc` lines 1214 and 1333

### ProcessLogFile flow omits several steps
- **Current:** Ch3 describes Steps 1-4 of ProcessLogFile
- **Missing:** Between validation and insertion, there are several steps not documented: (1) `HandleNonOkStatusOrOldLogRecord()` handles status errors and old log records, (2) `InitializeWriteBatchForLogRecord()` handles timestamp differences and protection info, (3) `MaybeReviseStopReplayForCorruption()` is called, (4) `InvokeWalFilterIfNeededOnWalRecord()` is called. The doc conflates some of these under Step 3 but the actual control flow is more complex with multiple early-return paths.
- **Source:** `db/db_impl/db_impl_open.cc` `ProcessLogFile()` function

### SanitizeOptions table in Ch1 is incomplete
- **Current:** Lists 3 adjustments
- **Missing:** SanitizeOptions also: (a) sets `delayed_write_rate` from rate_limiter if zero, (b) adjusts db_paths, (c) validates wal_dir, (d) handles max_open_files=-1 with table_cache_numshardbits, (e) validates block cache consistency, and many more. While not all are recovery-specific, some affect recovery behavior (e.g., wal_dir validation).
- **Source:** `db/db_impl/db_impl_open.cc` SanitizeOptions() full function

## Structure and Style Violations

### index.md is at the lower boundary
- **File:** index.md
- **Details:** 42 lines, which is at the lower end of the 40-80 line target. Acceptable but tight.

### No box-drawing characters found
- **File:** All files
- **Details:** Clean -- no violations.

### No line number references found
- **File:** All files
- **Details:** Clean -- no violations.

### Files: lines present and correct
- **File:** All chapter files
- **Details:** Each chapter has a **Files:** line. Paths verified correct for all chapters.

## Undocumented Complexity

### Dummy Write After Corrupted WAL Recovery
- **What it is:** When DB::Open() recovers from a corrupted WAL (`recovered_seq != kMaxSequenceNumber`), a dummy empty WriteBatch is written to the new WAL, flushed, and synced. This ensures the new WAL starts with the expected sequence number.
- **Why it matters:** Without this dummy write, if the process crashes again before any user writes, kPointInTimeRecovery could not distinguish previously-tolerated corruption from new corruption on the next recovery. This is the mechanism that makes `MaybeReviseStopReplayForCorruption()` work across recovery cycles.
- **Key source:** `db/db_impl/db_impl_open.cc` lines 2513-2548
- **Suggested placement:** Add to existing chapter 04 (WAL Recovery Modes) under kPointInTimeRecovery, or as a new section in chapter 03

### Auto-Recovery Disabled for Multi-Path DBs
- **What it is:** `ErrorHandler::EnableAutoRecovery()` is called only when `paths.size() <= 1`. Multi-path configurations silently lose auto-recovery from no-space errors.
- **Why it matters:** Users configuring tiered storage with multiple `db_paths` or `cf_paths` may not realize they lose automatic no-space recovery. No warning is emitted.
- **Key source:** `db/db_impl/db_impl_open.cc` lines 2463-2467
- **Suggested placement:** Add to chapter 08 (Background Error Handling) under "No-Space Error Recovery" as a limitation

### Multi-Threaded Table Loading During Recovery
- **What it is:** After MANIFEST recovery, `LoadTableHandlersHelper()` opens SST files using multiple threads controlled by `max_file_opening_threads`. This parallelizes the expensive I/O of opening table readers.
- **Why it matters:** Recovery time for large databases is significantly affected by this parallelism. Users tuning recovery performance should know about `max_file_opening_threads`.
- **Key source:** `db/version_set.cc`, `LoadTableHandlersHelper()`
- **Suggested placement:** Add to chapter 02 (MANIFEST Recovery) after Step 6

### FinishLogFilesRecovery Not Documented
- **What it is:** `FinishLogFilesRecovery()` is called after `ProcessLogFiles()` completes. It logs the recovery result (success/failure) via `event_logger_`.
- **Why it matters:** Minor gap, but the RecoverLogFiles flow description is incomplete without it.
- **Key source:** `db/db_impl/db_impl_open.cc` lines 1924-1928
- **Suggested placement:** Add as final step in chapter 03 RecoverLogFiles flow

### WAL Compression During Recovery
- **What it is:** `wal_compression` option enables ZSTD compression for WAL records. During recovery, compressed WAL records are automatically decompressed by `log::Reader` via `StreamingUncompress`. `SanitizeOptions()` silently disables unsupported compression types (lines 188-193), logging only a WARN.
- **Why it matters:** If WALs from a build with ZSTD are opened by a build without ZSTD, decompression fails. Also, `wal_compression` sanitization is missing from the Options Sanitization table in Ch1.
- **Key source:** `db/log_reader.h` lines 174-182, `db/db_impl/db_impl_open.cc` lines 188-193
- **Suggested placement:** Add to Ch3 (WAL Recovery) and add sanitization entry to Ch1

### Cross-CF Inconsistency Check Known False Negative
- **What it is:** `MaybeHandleStopReplayForCorruptionForInconsistency()` has a known false negative: when a CF is empty due to KV deletion (all data canceled out), `cfd->GetLiveSstFilesSize() > 0` skips it. The code comments explicitly acknowledge: "the check of (cfd->GetLiveSstFilesSize() > 0) may lead to the ignorance of a very rare inconsistency case caused in data cancellation."
- **Why it matters:** Recovery can succeed when cross-CF consistency is actually violated (rare but possible).
- **Key source:** `db/db_impl/db_impl_open.cc` lines 1775-1797
- **Suggested placement:** Add to Ch4 (Cross-CF Consistency Check section) as a known limitation

### User-Defined Timestamp Handling During WAL Recovery
- **What it is:** WAL recovery reconciles timestamp size differences between recorded WAL format and running CF configuration via `HandleWriteBatchTimestampSizeDifference()` with `kReconcileInconsistency` mode. `WriteLevel0TableForRecovery()` also handles UDT stripping when `persist_user_defined_timestamps=false`.
- **Why it matters:** Critical for users migrating CFs to/from UDT. Incorrect handling could corrupt data.
- **Key source:** `db/db_impl/db_impl_open.cc` lines 1262-1263, 1531-1538, 2027-2037
- **Suggested placement:** Add to Ch3 (WAL Recovery)

### flush_verify_memtable_count During Recovery Flush
- **What it is:** `WriteLevel0TableForRecovery()` verifies memtable entry count matches SST entry count (lines 2132-2163). If `flush_verify_memtable_count=true` (default), mismatch returns `Status::Corruption`, failing `DB::Open()`.
- **Why it matters:** Additional corruption detection during recovery that can cause Open failures.
- **Key source:** `db/db_impl/db_impl_open.cc` lines 2132-2163
- **Suggested placement:** Mention in Ch7 (Post-Recovery Flush)

### Recovery Failure Memtable Cleanup
- **What it is:** If `RecoverLogFiles()` fails, all CF memtables are reset via `cfd->CreateNewMemtable(kMaxSequenceNumber)` (lines 820-825). This prevents partially-replayed data from leaking into subsequent retry attempts.
- **Why it matters:** Ensures recovery atomicity -- partial WAL data never persists across retry.
- **Key source:** `db/db_impl/db_impl_open.cc` lines 820-825
- **Suggested placement:** Add to Ch1 (Recovery Atomicity section)

### skip_stats_update_on_db_open for Recovery Performance
- **What it is:** When `skip_stats_update_on_db_open=true`, MANIFEST recovery skips computing file statistics during `PrepareAppend()`, significantly speeding up recovery for databases with many SST files.
- **Why it matters:** Recovery performance tuning option not mentioned in the docs.
- **Key source:** `db/version_edit_handler.cc` lines 535-537; `include/rocksdb/options.h` lines 1381-1387
- **Suggested placement:** Brief mention in Ch1 or Ch2

## Positive Notes

- **Chapter 9 (WAL Verification)**: All 14 claims verified 100% correct. Well-structured with clear distinction between the two verification mechanisms. Accurate coverage of limitations and incompatibilities.
- **Chapter 4 (WAL Recovery Modes)**: All 7 claims verified correct. The WAL recycling incompatibility table accurately reflects the SanitizeOptions code. The cross-CF consistency check description including the new-CF exemption is precise.
- **Chapter 7 (Post-Recovery Flush)**: All 7 claims verified correct. Accurately describes the `flushed` flag behavior and WBM enforcement semantics.
- **Chapter 3 (WAL Recovery)**: All 14 claims verified correct at the factual level (structure presentation is slightly misleading but individual claims are accurate).
- **Options cross-references**: Consistently and correctly references options by field name and header path throughout.
- **Index structure**: Clean, within line limits, proper chapter table format, key characteristics and invariants are reasonable.
- **Code entity names**: Function names, class names, and method names are verified correct throughout all chapters -- no typos or outdated names.
