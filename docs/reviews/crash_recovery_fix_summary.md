# Fix Summary: crash_recovery

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 12 |
| Completeness | 5 |
| Structure | 2 |
| Style | 2 |
| **Total** | **21** |

## Disagreements Found

4 disagreements documented in `crash_recovery_debates.md`:
1. **Recovery atomicity** (high risk) -- CC accepted, Codex flagged WRONG. Codex correct.
2. **Cross-CF consistency check scope** (medium risk) -- CC accepted, Codex flagged WRONG. Codex correct.
3. **Inline code quotes** (low risk) -- CC said clean, Codex flagged as violation. No change applied (consistent with project-wide convention).
4. **"Key Invariant" label usage** (medium risk) -- CC accepted, Codex flagged misuse. Codex partially correct; fixed for false claims, retained for genuine invariants.

## Changes Made

### index.md
- Fixed recovery atomicity invariant to describe actual behavior (on-disk side effects possible)
- Updated chapter 10 summary to reflect corrected repair process description

### 01_recovery_overview.md
- Replaced "Recovery Atomicity" section with "Recovery Side Effects" -- documents that Open can leave on-disk side effects (new WAL, MANIFEST edits, WAL truncation, SST files) even when it fails
- Fixed min_log_number_to_keep claim: now says "min_log_number_to_keep and per-CF log numbers, which together determine the minimum WAL"

### 02_manifest_recovery.md
- Added open_files_async caveat to SST file verification: with open_files_async=true, missing/corrupt SSTs surface as background errors, not during Open
- Split Column Family Discovery into two separate behaviors: MANIFEST-time validation (CFs in MANIFEST not opened by caller) and post-recovery creation (create_missing_column_families)
- Downgraded CURRENT file "Key Invariant" to behavioral description noting BER bypass

### 03_wal_recovery.md
- Restructured RecoverLogFiles flow to show three sub-functions: SetupLogFilesRecovery, ProcessLogFiles, FinishLogFilesRecovery
- Added FinishLogFilesRecovery mention (previously omitted)
- Added per-record granularity for CheckSeqnoNotSetBackDuringRecovery (previously only described per-WAL-file)

### 04_wal_recovery_modes.md
- Added sentinel WAL write mechanism for kPointInTimeRecovery (new subsection under PIT)
- Scoped cross-CF consistency check to kPointInTimeRecovery (was incorrectly described as covering kTolerateCorruptedTailRecords too)
- Added known limitation about false negative when CF is empty due to KV deletion
- Added "Interaction with paranoid_checks" section explaining dependency on paranoid_checks=true

### 05_best_efforts_recovery.md
- Fixed CURRENT File Bypass: changed "first non-empty MANIFEST" to "ManifestPicker sorts by file number descending, TryRecover tries newest first"
- Fixed BER state guarantee: now describes recovery as "maximal prefix of MANIFEST edits where all files exist," not "some MANIFEST snapshot"
- Added: can fall back to empty state, atomic groups are all-or-nothing

### 06_two_phase_commit_recovery.md
- Fixed 2PC flush reasoning: changed "gaps in sequence numbers" to "does not guarantee consecutive log files have consecutive sequence IDs" (matches code comment)

### 08_background_error_handling.md
- Split listener section into OnBackgroundError (Status suppression only) and OnErrorRecoveryBegin (auto_recovery control)
- Added limitation: retryable I/O errors unconditionally start recovery regardless of listener auto_recovery setting
- Fixed manual recovery: distinguished ordinary soft errors (clear without flush) from soft_error_no_bg_work path (requires flush)
- Added auto-recovery limitation: multi-path databases lose no-space auto-recovery

### 10_database_repair.md
- Rewritten repair process to match actual Repairer::Run() execution order (6 steps instead of 4 phases)
- Fixed ArchiveFile: changed ".archive extension" to "moved to lost/ subdirectory"
- Fixed corrupted SSTs: changed "silently ignored" to "logged as warnings and moved to lost/"
- Fixed create_unknown_cfs: removed internal parameter reference, described per-overload behavior
- Fixed limitations: changed "silently skipped" to "logged and archived to lost/"
