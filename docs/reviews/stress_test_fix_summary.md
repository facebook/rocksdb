# Fix Summary: stress_test

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness (WRONG) | 4 |
| Misleading | 12 |
| Completeness | 3 |
| Structure/Style | 3 |
| Undocumented complexity | 3 |
| **Total** | **25** |

## Disagreements Found

0 -- Only one reviewer (CC). No Codex review file found.

## Changes Made

### 01_architecture.md
- Fixed `DbStressFSWrapper` description: "Tracks filesystem operations for stress test diagnostics" -> "Verifies IOActivity correctness and file checksum propagation for SST opens"
- Added `DbStressListener` section documenting fault injection on background threads, sequence number tracking, and unique ID validation
- Updated **Files:** line to include `db_stress_listener.h` and `db_stress_env_wrapper.h`

### 02_test_modes.md
- Changed "Key Invariant" to "Important" for iterator verification abort (test-level assertion, not data corruption invariant)

### 03_expected_state.md
- Fixed `Delete(pending=true)` description: "returns false if key does not exist" -> "returns false if the key is already marked as deleted in the expected state"

### 04_thread_model.md
- Fixed `log2_keys_per_lock` default: 10 -> "gflag default 2 (one lock per 4 keys), whitebox overrides to 10"
- Fixed `snapshot_queue` type: "queue of SnapshotState" -> "queue of (sequence_number, SnapshotState) pairs"
- Changed 2 "Key Invariant" labels to "Important" (thread init deadlock and lock ordering are test-level, not DB data corruption)

### 05_operation_mix.md
- Split Default column into "gflag Default" and "crashtest.py Default" with note explaining the difference
- Fixed `GenerateOneKey` location: `.h` -> `.{h,cc}`
- Added 11 missing probabilistic operations: `promote_l0_one_in`, `set_in_place_one_in`, `abort_and_resume_compactions_one_in`, `verify_file_checksums_one_in`, `get_properties_of_all_tables_one_in`, `approximate_size_one_in`, `sync_wal_one_in`, `get_all_column_family_metadata_one_in`, `get_sorted_wal_files_one_in`, `get_current_wal_file_one_in`
- Removed `compression_type` from `BuildOptionsTable` description (not actually in that table)

### 06_crash_test.md
- Fixed blackbox `disable_wal` distribution: "random (0 or 1)" -> "random (0 with 75% probability, 1 with 25% probability)"
- Fixed kill sub-mode formulas: added `+ 1` to prevent zero (`kill_odds // 10 + 1`, `kill_odds // 5000 + 1`)
- Fixed check mode 3: "default (level) compaction" -> "base-parameter compaction style (no override)"
- Replaced deprecated `crash_test_with_txn` with canonical targets (`crash_test_with_wc_txn`, `crash_test_with_wp_txn`, `crash_test_with_wup_txn`) plus multiops targets
- Added deprecation note for `crash_test_with_txn`

### 07_fault_injection.md
- Fixed `exclude_wal_from_write_fault_injection` default: "0" -> "false" (boolean flag)
- Expanded `metadata_read_fault_one_in` targets: added `FileExists`, `GetChildren`, `IsDirectory`, and others
- Fixed `metadata_write_fault_one_in` targets: removed file-level `Sync()`/`Fsync()`, added `Close()`, `RenameFile()`, `LinkFile()`, `DeleteFile()`, directory `Fsync()`
- Fixed `DeleteFilesCreatedAfterLastDirSync` description: added restore behavior for overwritten files
- Fixed error severity value 2: "Fatal (permanent error)" -> "Data loss (non-retryable with has_data_loss=true)"
- Fixed note about error injection: removed incorrect claim that only retryable errors are injected

### 08_parameter_randomization.md
- Fixed `blob_params` merge: "always merged" -> "merged with ~10% probability when compatible"
- Fixed `max_write_buffer_number` in BuildOptionsTable: "Original and 2x" -> "Original, 2x, and 4x"
- Removed `compression_type` from BuildOptionsTable (not present in code)
- Expanded UDI trie restrictions: added TransactionDB blocking, prefix scanning disable, interpolation search disable
- Added `DbStressCompactionFilter` subsection documenting state-aware compaction filter with TryLock coordination
- Updated **Files:** line to include `db_stress_compaction_filter.h`
