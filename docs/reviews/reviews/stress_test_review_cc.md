# Review: stress_test -- Claude Code

## Summary

**Overall quality: good**

The documentation provides a solid, well-structured overview of the stress test framework. The architecture chapter accurately describes the two-layer design (db_crashtest.py + db_stress), the expected state chapter correctly documents the bit-level encoding and two-phase commit mechanism, and the crash test chapter faithfully describes blackbox/whitebox modes. The main weaknesses are: (1) default values listed without clarifying whether they are gflag defaults or db_crashtest.py defaults (leading to several incorrect claims), (2) the probabilistic operations table in Chapter 5 is significantly incomplete (missing ~10 operations), (3) the documentation has not kept pace with recent codebase changes (UDI/trie index, interpolation search, SepKV, remote compaction), and (4) several important subsystems (DbStressListener, compaction filter, custom compression) are entirely undocumented.

## Correctness Issues

### [WRONG] Default operation percentages are db_crashtest.py values, not gflag defaults
- **File:** `05_operation_mix.md`, "Percentage-Based Operations" table
- **Claim:** "readpercent Default 45, prefixpercent Default 5, writepercent Default 35, delpercent Default 4, delrangepercent Default 1"
- **Reality:** These are the `default_params` values in `db_crashtest.py`. The gflag defaults (in `db_stress_gflags.cc`) are completely different: readpercent=10, prefixpercent=20, writepercent=45, delpercent=15, delrangepercent=0, iterpercent=10, customopspercent=0. When running `db_stress` directly (without `db_crashtest.py`), the actual defaults differ from what the doc states.
- **Source:** `db_stress_tool/db_stress_gflags.cc` lines 919-953 vs `tools/db_crashtest.py` lines 150-260
- **Fix:** Either label the column as "db_crashtest.py default" instead of just "Default", or show both gflag and crashtest.py defaults. Since the doc's context is about flags, the gflag defaults are what a reader would expect.

### [WRONG] Default log2_keys_per_lock value
- **File:** `04_thread_model.md`, "Locking Granularity" section
- **Claim:** "Default `log2_keys_per_lock = 10` means one lock per 1024 keys"
- **Reality:** The gflag default is 2 (one lock per 4 keys): `DEFINE_uint64(log2_keys_per_lock, 2, ...)`. The value 10 comes from `whitebox_default_params` in `db_crashtest.py`. The doc does not clarify this is the whitebox crashtest default.
- **Source:** `db_stress_tool/db_stress_gflags.cc` line 1013, `tools/db_crashtest.py` line 585
- **Fix:** Change to "Default gflag value is 2 (one lock per 4 keys). The whitebox crash test overrides this to 10 (one lock per 1024 keys)."

### [WRONG] blob_params described as "always merged"
- **File:** `08_parameter_randomization.md`, "Merge Priority" section
- **Claim:** "blob_params (always merged for blob DB testing)"
- **Reality:** `blob_params` is conditionally merged with approximately 10% probability. The code is: `if ... random.choice([0] * 9 + [1]) == 1: params.update(blob_params)`. Additionally, it requires `test_secondary == 0` and `best_efforts_recovery == 0`.
- **Source:** `tools/db_crashtest.py` lines 1326-1331
- **Fix:** Change to "blob_params (merged with ~10% probability when compatible with other test options)"

### [MISLEADING] DbStressFSWrapper purpose
- **File:** `01_architecture.md`, "Environment Setup" table
- **Claim:** "DbStressFSWrapper -- Tracks filesystem operations for stress test diagnostics"
- **Reality:** `DbStressFSWrapper` primarily verifies IOActivity assertions (checking that the correct `io_activity` is set for each IO operation) and validates file checksum propagation through `FileOptions` for SST file opens. It is an assertion/verification layer, not a diagnostics tracker.
- **Source:** `db_stress_tool/db_stress_env_wrapper.h`, `DbStressFSWrapper` class and `CheckIOActivity()` function
- **Fix:** Change to "DbStressFSWrapper -- Verifies IOActivity correctness and file checksum propagation for SST opens"

### [MISLEADING] Blackbox disable_wal distribution
- **File:** `06_crash_test.md`, "Key Parameters" table
- **Claim:** "`--disable_wal` Default: random (0 or 1)"
- **Reality:** The distribution is weighted 75/25, not 50/50: `lambda: random.choice([0, 0, 0, 1])`. WAL is disabled only 25% of the time.
- **Source:** `tools/db_crashtest.py` line 559
- **Fix:** Change to "random (0 with 75% probability, 1 with 25% probability)"

### [MISLEADING] snapshot_queue type
- **File:** `04_thread_model.md`, "ThreadState: Per-Thread State" table
- **Claim:** "`snapshot_queue` -- queue of `SnapshotState`"
- **Reality:** The actual type is `std::queue<std::pair<uint64_t, SnapshotState>>` where the uint64_t is the sequence number at snapshot creation time.
- **Source:** `db_stress_tool/db_stress_shared_state.h` line 552
- **Fix:** Change to "`snapshot_queue` -- queue of `(sequence_number, SnapshotState)` pairs"

### [MISLEADING] Delete(pending=true) "if key does not exist" conflates expected state with DB state
- **File:** `03_expected_state.md`, "Key Operations" section
- **Claim:** "Delete(pending): When pending=true, sets the pending delete flag (returns false if key does not exist)."
- **Reality:** The code checks `!Exists()` on the `ExpectedValue` object, where `Exists()` checks the internal deleted bit (`!IsDeleted()`). This is an expected-state-level concept, not a DB-level existence check. More precisely: `Delete(pending=true)` returns false if the expected value's deleted bit is already set (i.e., the key is already expected to be deleted in the tracking system).
- **Source:** `db_stress_tool/expected_value.cc` lines 23-35, `db_stress_tool/expected_value.h` lines 40-43
- **Fix:** Change to "returns false if the key is already marked as deleted in the expected state"

### [MISLEADING] Whitebox kill sub-mode odds formulas
- **File:** `06_crash_test.md`, "Kill Sub-Modes" table
- **Claim:** Sub-mode 1 uses "`kill_odds / 10`", sub-mode 2 uses "`kill_odds / 5000`"
- **Reality:** The code uses integer division with `+ 1` to prevent zero: `kill_random_test // 10 + 1` and `kill_random_test // 5000 + 1`. The `+ 1` matters when kill_odds is small.
- **Source:** `tools/db_crashtest.py` lines 1598-1613
- **Fix:** Add `+ 1` to the formulas: "`kill_odds // 10 + 1`" and "`kill_odds // 5000 + 1`"

### [MISLEADING] exclude_wal_from_write_fault_injection default
- **File:** `07_fault_injection.md`, "Write Errors" section
- **Claim:** "`--exclude_wal_from_write_fault_injection` flag (default: 0)"
- **Reality:** The gflag uses `DEFINE_bool(..., false, ...)` which is indeed 0/false, but the doc uses integer notation for a boolean flag. Minor but inconsistent with how the flag is actually defined.
- **Source:** `db_stress_tool/db_stress_gflags.cc` line 1188
- **Fix:** Change to "(default: false)" for consistency with boolean type

### [MISLEADING] metadata_read_fault_one_in scope understated
- **File:** `07_fault_injection.md`, "Read Errors" table
- **Claim:** "`--metadata_read_fault_one_in` targets `GetFileSize()`, `GetFileModificationTime()`"
- **Reality:** The flag covers far more operations: `GetFreeSpace`, `IsDirectory`, `FileExists`, `GetChildren`, `GetChildrenFileAttributes`, `GetAbsolutePath`, `NumFileLinks`, `AreFilesSame`, in addition to the two listed. The doc only mentions two of many targets.
- **Source:** `utilities/fault_injection_fs.h` and `utilities/fault_injection_fs.cc`
- **Fix:** Change to "Fails metadata read operations (`GetFileSize`, `GetFileModificationTime`, `FileExists`, `GetChildren`, `IsDirectory`, and others)"

### [MISLEADING] metadata_write_fault_one_in scope incorrect
- **File:** `07_fault_injection.md`, "Write Errors" table
- **Claim:** "`--metadata_write_fault_one_in` targets `Sync()`, `Fsync()`, directory operations"
- **Reality:** The flag does NOT inject faults on file-level `Sync()` or `RangeSync()`. It injects on directory `Fsync()`, `FsyncWithDirOptions()`, file `Close()`, `RenameFile()`, `LinkFile()`, and `DeleteFile()`. Claiming it affects file-level "Sync" is wrong.
- **Source:** `utilities/fault_injection_fs.cc`, file-level `Sync()` implementation has no `MaybeInjectThreadLocalError` call for metadata write
- **Fix:** Change to "Fails metadata write operations (`Close()`, `RenameFile()`, `LinkFile()`, `DeleteFile()`, directory `Fsync()`)"

### [MISLEADING] DeleteFilesCreatedAfterLastDirSync behavior oversimplified
- **File:** `07_fault_injection.md`, "sync_fault_injection" section
- **Claim:** "`DeleteFilesCreatedAfterLastDirSync()`: Removes files whose creation was not followed by a directory sync"
- **Reality:** The method does more than just delete -- for files that overwrote existing ones, it restores the original file content. This restore behavior is important for correctness.
- **Source:** `utilities/fault_injection_fs.cc`, `DeleteFilesCreatedAfterLastDirSync()` lines 1384-1393
- **Fix:** Add "For files that overwrote existing files, the original content is restored rather than deleted."

### [MISLEADING] inject_error_severity value 2 described as "fatal"
- **File:** `07_fault_injection.md`, "Error Severity" table
- **Claim:** "Value 2 = Fatal (permanent error) -- DB may enter read-only mode or stop processing"
- **Reality:** The code maps `inject_error_severity == 2` to `has_data_loss=true` (not "fatal" in RocksDB terminology). `SetThreadLocalErrorContext(... retryable=false, has_data_loss=true)`. In RocksDB, "fatal" is a different error severity from "data loss". The doc conflates these.
- **Source:** `db_stress_tool/db_stress_test_base.cc` lines 1064-1094
- **Fix:** Change to "Value 2 = Data loss (non-retryable error with has_data_loss=true)"

### [MISLEADING] Whitebox check mode 3 described as "level compaction"
- **File:** `06_crash_test.md`, "Check Modes" table
- **Claim:** "Mode 3 -- Normal run with default (level) compaction."
- **Reality:** Mode 3 is simply the `else` branch with no specific compaction style override. It uses whatever compaction style is set in the base parameters, which defaults to level but could be overridden by parameter randomization.
- **Source:** `tools/db_crashtest.py` lines 1647-1652, the `else` clause has no `compaction_style` key
- **Fix:** Change to "Mode 3 -- Normal run with base-parameter compaction style (no override)"

### [MISLEADING] GenerateOneKey location
- **File:** `05_operation_mix.md`, "Key Space" section
- **Claim:** "Keys are generated by `GenerateOneKey()` (see `db_stress_tool/db_stress_common.h`)"
- **Reality:** `GenerateOneKey()` is declared in `db_stress_common.h` but defined in `db_stress_common.cc`. The doc references only the header.
- **Source:** `db_stress_tool/db_stress_common.h` (declaration), `db_stress_tool/db_stress_common.cc` (definition)
- **Fix:** Change to "see `db_stress_tool/db_stress_common.{h,cc}`"

### [STALE] crash_test_with_txn make target
- **File:** `06_crash_test.md`, "Make Targets" table
- **Claim:** "`make crash_test_with_txn` -- Runs with `--use_txn=1`"
- **Reality:** `crash_test_with_txn` is deprecated and just maps to `crash_test_with_wc_txn` (write-committed). The current canonical targets are `crash_test_with_wc_txn`, `crash_test_with_wp_txn`, and `crash_test_with_wup_txn`.
- **Source:** `crash_test.mk` (search for `crash_test_with_txn`)
- **Fix:** Replace with `crash_test_with_wc_txn` (write-committed), `crash_test_with_wp_txn` (write-prepared), `crash_test_with_wup_txn` (write-unprepared). Note the deprecated alias.

### [WRONG] BuildOptionsTable contents
- **File:** `08_parameter_randomization.md`, "BuildOptionsTable" section
- **Claim:** "`max_write_buffer_number`: Original and 2x values" and includes "`compression_type`: Various algorithms"
- **Reality:** Two errors: (1) `max_write_buffer_number` includes original, 2x, AND 4x values (three entries, not two). (2) `compression_type` is NOT in `BuildOptionsTable` at all -- it is not a dynamically changeable option via `SetOptions()`.
- **Source:** `db_stress_tool/db_stress_test_base.cc`, `BuildOptionsTable()` method
- **Fix:** Change `max_write_buffer_number` to "Original, 2x, and 4x values". Remove `compression_type` from the list.

## Completeness Gaps

### Missing probabilistic operations in Chapter 5
- **Why it matters:** Developers looking at the doc to understand the full operation mix will miss ~10 operations that are exercised during stress testing
- **Where to look:** `db_stress_tool/db_stress_test_base.cc`, search for `OneInOpt(FLAGS_`
- **Missing operations:**
  - `--sync_wal_one_in` -- calls `SyncWAL()`
  - `--promote_l0_one_in` -- calls `PromoteL0()`
  - `--set_in_place_one_in` -- performs in-place updates
  - `--get_all_column_family_metadata_one_in` -- calls `GetAllColumnFamilyMetaData()`
  - `--get_sorted_wal_files_one_in` -- calls `GetSortedWalFiles()`
  - `--get_current_wal_file_one_in` -- calls `GetCurrentWalFile()`
  - `--abort_and_resume_compactions_one_in` -- aborts and resumes background compaction jobs
  - `--verify_file_checksums_one_in` -- calls `VerifyFileChecksums()`
  - `--get_properties_of_all_tables_one_in` -- calls `GetPropertiesOfAllTables()`
  - `--approximate_size_one_in` -- calls `GetApproximateSizes()`
- **Suggested scope:** Add all missing operations to the existing table in Chapter 5

### No coverage of UDI (User Defined Index / trie) integration
- **Why it matters:** UDI/trie index has seen 14+ commits in 2024-2025, with multiple compatibility restrictions added to `finalize_and_sanitize()`. It is a significant new feature exercised by the stress test.
- **Where to look:** `tools/db_crashtest.py` (search for `use_trie_index`), `db_stress_tool/db_stress_gflags.cc`
- **Suggested scope:** Add mention in Chapter 8 (Parameter Randomization) under a new "User-Defined Index (Trie)" subsection in finalize_and_sanitize, documenting the restrictions: disables mmap_read, disables parallel compression, blocks TransactionDB, disables prefix scanning, disables interpolation search

### No mention of DbStressListener
- **Why it matters:** `db_stress_tool/db_stress_listener.h` implements event listeners that coordinate fault injection on background threads (flush, compaction), track sequence numbers for expected state, and validate unique IDs. The docs describe fault injection as if it only affects foreground threads, but the listener enables it on background operations. This is the most critical documentation gap.
- **Where to look:** `db_stress_tool/db_stress_listener.h`
- **Suggested scope:** Section in Chapter 1 (Architecture) and cross-reference from Chapter 7 (Fault Injection)

### No coverage of remote compaction service simulation
- **Why it matters:** The stress test simulates a remote compaction service with its own job queue, worker threads, and fault injection setup. This is a substantial subsystem with fields in `SharedState` (`remote_compaction_queue_`, `remote_compaction_result_map_`) that is completely absent from the docs.
- **Where to look:** `db_stress_tool/db_stress_driver.cc` (RemoteCompactionWorkerThread), `db_stress_shared_state.h`
- **Suggested scope:** Add to Chapter 4 (Thread Model) background threads section and Chapter 8 (Parameter Randomization)

### No coverage of interpolation search
- **Why it matters:** Interpolation search was recently added as an alternative to binary search for data block seeks, and is exercised in the stress test
- **Where to look:** `db_stress_tool/db_stress_gflags.cc` (search for `interpolation_search`), `tools/db_crashtest.py`
- **Suggested scope:** Mention in Chapter 8 under a brief subsection

### No coverage of verify_manifest_content_on_close
- **Why it matters:** Recently added option (commit b6f498b2c) that validates manifest content on DB close, exercised in stress tests
- **Where to look:** `db_stress_tool/db_stress_gflags.cc`
- **Suggested scope:** Mention in Chapter 8 or Chapter 5

### Missing make targets in Chapter 6
- **Why it matters:** The doc lists 6 make targets but there are more (e.g., `crash_test_with_multiops_txn`, `crash_test_with_multiops_wc_txn`, `crash_test_with_multiops_wp_txn`)
- **Where to look:** `Makefile`, search for `crash_test`
- **Suggested scope:** Complete the make targets table

## Depth Issues

### Chapter 8 finalize_and_sanitize coverage is incomplete
- **Current:** Lists ~8 categories of sanitization rules
- **Missing:** Several important sanitization categories are not covered:
  - UDI/trie index restrictions (mmap_read, parallel compression, transactions, prefix scan, interpolation search)
  - Remote compaction restrictions (expanded since doc was written)
  - WritePrepared/WriteUnprepared transaction-specific restrictions
  - Separated key-value (SepKV) restrictions
- **Source:** `tools/db_crashtest.py`, `finalize_and_sanitize()` function (lines ~880-1270)

### Chapter 7 inject_error_severity description lacks detail
- **Current:** States value 1 = retryable, value 2 = fatal
- **Missing:** The gflag description says "The severity of the injected IO Error. 1 is soft error (e.g. retryable error), 2 is fatal error" but the doc also claims "The stress test currently only injects retryable IO errors" without verifying this. The actual behavior depends on how `inject_error_severity` is used in the fault injection layer.
- **Source:** `db_stress_tool/db_stress_gflags.cc` line 1226, `utilities/fault_injection_fs.h`

### Chapter 2 doesn't explain how test modes interact with crash recovery
- **Current:** Describes each mode's verification methods
- **Missing:** How does each mode handle crash recovery differently? NonBatchedOps uses expected state files, but BatchedOps, CfConsistency, and MultiOpsTxns use different recovery strategies that are not explained.
- **Source:** Each test mode's `VerifyDb()` implementation

## Structure and Style Violations

### Questionable use of "Key Invariant" label
- **File:** `04_thread_model.md` (2 instances), `02_test_modes.md` (1 instance)
- **Details:** The style guide says "INVARIANT used only for true correctness invariants (data corruption / crash if violated)." Three "Key Invariant" uses describe test-level invariants rather than data integrity invariants:
  1. `04_thread_model.md`: "All threads must call IncInitialized() before the stress test starts" -- this is a test deadlock issue, not data corruption
  2. `04_thread_model.md`: "key lock must be held while both updating expected state and performing the DB operation" -- this is test correctness, not DB data corruption
  3. `02_test_modes.md`: "During iterator verification, if the iterator's key is less than the expected key, the test aborts" -- this is a test assertion, not a DB invariant

  The other invariant uses (PendingExpectedValue destructor assertion, atomic_flush atomicity, DB data loss on retryable errors, index.md invariants) are appropriate.

### Chapter 5 default values context unclear
- **File:** `05_operation_mix.md`
- **Details:** The "Default" column in the percentage-based operations table shows `db_crashtest.py` default_params values without stating this. A reader running `db_stress` directly would get different defaults. Either label the column or add a note.

## Undocumented Complexity

### OperateDb probabilistic operation ordering and interaction
- **What it is:** In `OperateDb()`, the probabilistic operations (`_one_in` flags) are checked in a specific order each iteration. Some operations interact -- e.g., `pause_background_one_in` temporarily pauses all background operations, which affects concurrent `compact_range_one_in` calls. The ordering matters for correctness.
- **Why it matters:** Developers adding new probabilistic operations need to understand where to place them in the sequence and what interactions to consider.
- **Key source:** `db_stress_tool/db_stress_test_base.cc`, `OperateDb()` function, lines ~1100-1400
- **Suggested placement:** Add to Chapter 5 as a subsection after the probabilistic operations table

### Reopen voting mechanism
- **What it is:** The doc mentions "A voting mechanism ensures all threads agree to reopen simultaneously" but doesn't explain how it works. Threads check `FLAGS_reopen > 0` and coordinate via `SharedState` to ensure no operations are in-flight during reopen.
- **Why it matters:** Understanding the reopen mechanism is critical for debugging reopen-related failures and for adding new operations that must be quiesced during reopen.
- **Key source:** `db_stress_tool/db_stress_test_base.cc`, search for `reopen`
- **Suggested placement:** Add detail to Chapter 5, "DB Reopen" section

### Error injection ring buffer for diagnostics
- **What it is:** Recent commit 91f227d9b added an injected error log ring buffer to `FaultInjectionTestFS` for improved fault injection diagnostics. This is separate from the file-based log mentioned in Chapter 7.
- **Why it matters:** Helps diagnose whether crashes are caused by injected faults or genuine bugs
- **Key source:** `utilities/fault_injection_fs.h`, ring buffer implementation
- **Suggested placement:** Add to Chapter 7, "Fault Injection Log" section

### ProcessStatus error tracking
- **What it is:** `ProcessStatus()` is called after most operations in `OperateDb()` to track and handle errors from DB operations. It distinguishes between expected errors (e.g., from fault injection) and unexpected errors, and can trigger test termination.
- **Why it matters:** Understanding how errors are processed is essential for debugging stress test failures and for correctly handling errors in new operations.
- **Key source:** `db_stress_tool/db_stress_test_base.cc`, `ProcessStatus()` method
- **Suggested placement:** Add to Chapter 5 or Chapter 10

### Separate key-value (SepKV) data block format
- **What it is:** Recent commit 901c88e37 added support for separating keys and values in data blocks. This is exercised in the stress test and has specific sanitization rules.
- **Why it matters:** New data block format that changes SST structure
- **Key source:** `tools/db_crashtest.py`, search for `sep_kv`
- **Suggested placement:** Mention in Chapter 8

### DbStressCompactionFilter
- **What it is:** `db_stress_tool/db_stress_compaction_filter.h` implements a state-aware compaction filter that coordinates with expected state. It uses `TryLock()` (non-blocking) on key locks to avoid deadlocks with foreground threads, and distinguishes between Remove (SingleDelete-compatible) and Purge (for non-SD-compatible cases).
- **Why it matters:** The compaction filter's interaction with expected state is a subtle correctness concern -- it must coordinate properly or verification will report false positives.
- **Key source:** `db_stress_tool/db_stress_compaction_filter.h`
- **Suggested placement:** Chapter 5 or Chapter 8

### DbStressCustomCompressionManager
- **What it is:** The stress test includes a custom compression algorithm for testing non-standard compression paths, registered via `DbStressCustomCompressionManager`.
- **Why it matters:** Tests custom compression integration that is not covered by standard compression options
- **Key source:** `db_stress_tool/db_stress_gflags.cc` (search for `custom_compression`)
- **Suggested placement:** Chapter 8

### SstQueryFilterConfigs integration
- **What it is:** The stress test integrates `SstQueryFilterConfigsManager` for writing and reading SST query filters, controlled by `--sqfc_name` and `--sqfc_version`.
- **Why it matters:** Experimental feature completely undocumented in the stress test docs
- **Key source:** `db_stress_tool/db_stress_filters.h`, `db_stress_tool/db_stress_test_base.h`
- **Suggested placement:** Chapter 8

### Timestamped snapshot testing
- **What it is:** When `--create_timestamped_snapshot_one_in > 0`, `CommitTxn()` randomly calls `CommitAndTryCreateSnapshot()` with a nanosecond timestamp. Thread 0 creates standalone timestamped snapshots and verifies timestamp ordering.
- **Why it matters:** Tests a feature not mentioned anywhere in the stress test docs
- **Key source:** `db_stress_tool/db_stress_test_base.cc` (search for `timestamped_snapshot`)
- **Suggested placement:** Chapter 5, snapshot management section

## Positive Notes

- The ExpectedValue bit encoding documentation (Chapter 3) is exceptionally accurate -- every bit field, mask constant, and operation semantics match the code exactly.
- The two-phase commit explanation for PendingExpectedValue is clear and correct, including the fence semantics.
- The blackbox/whitebox mode distinction (Chapter 6) is well-explained with accurate descriptions of kill sub-modes and check modes.
- The environment layering diagram (Chapter 1) correctly captures the FS wrapper chain.
- The index.md is well-structured at 41 lines (within the 40-80 line target) with proper key source files, chapter table, characteristics, and invariants sections.
- No box-drawing characters, no line number references, and all chapters have proper **Files:** lines.
- The thread lifecycle description (Chapter 4) accurately describes the phased synchronization via SharedState.
