# Fix Summary: listener

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 16 |
| Completeness | 12 |
| Structure/Style | 3 |
| Total | 31 |

## Correctness Fixes

1. **OnManualFlushScheduled + FlushWAL** (ch02): Removed incorrect claim that FlushWAL() triggers this callback
2. **OnCompactionBegin "check inputs" step** (ch02): Replaced invented step with actual checks (empty listeners, shutting_down_, manual_compaction_paused_)
3. **num_l0_files description** (ch02): Changed from "before and after" to callback-dependent semantics
4. **Subcompaction events scope** (ch02): Corrected to only non-trivial compactions through CompactionJob::Run()
5. **table_properties "not set"** (ch02): Changed to "not present in SubcompactionJobInfo"
6. **OnTableFileCreationStarted call paths** (ch03): Split into flush/recovery (BuildTable) vs compaction (CompactionJob)
7. **Flush event ordering** (ch03): Fixed SST before blob (was incorrectly blob before SST)
8. **Compaction event ordering** (ch03): Split by compaction type (non-trivial, trivial move, FIFO)
9. **FileOperationInfo offset/length** (ch04): Corrected to "set for reads, writes, and range syncs" with warning about uninitialized values
10. **SequentialFileReader OnIOError** (ch04): Documented that it only dispatches OnFileReadFinish, not OnIOError
11. **OnIOError + ShouldBeNotifiedOnFileIO** (ch04): Added MANIFEST verification exception
12. **OnBackgroundError scope** (ch05): Broadened from "read-only mode" to all background errors including retryable
13. **Error dispatch flow** (ch05): Fixed to show per-listener interleaving (was incorrectly shown as bulk steps)
14. **OnErrorRecoveryEnd scope** (ch05): Broadened to include manual Resume(), shutdown interruption, retry exhaustion
15. **write_stall_proximity_pct** (ch06): Removed incorrect "memtable count" from computation description
16. **OnBackgroundError threading** (ch08): Added user write thread to possible invocation threads

## Completeness Fixes

1. **SubcompactionJobInfo fields** (ch02): Expanded from 2 to all 11 fields
2. **Missing FlushReason values** (ch02): Added kOthers, kErrorRecoveryRetryFlush, kCatchUpAfterErrorRecovery
3. **Missing CompactionReason::kUnknown** (ch02): Added as first row
4. **Missing FlushJobInfo::oldest_blob_file_number** (ch02): Added to field table
5. **Missing CompactionJobInfo::blob_compression_type** (ch02): Added to field table
6. **Atomic flush OnFlushBegin ordering** (ch02): Documented BEFORE PickMemTable vs after
7. **OnFlushCompleted cross-thread emission** (ch02): Documented delegatable flush result installation
8. **FIFO trivial-copy compaction exception** (ch02, ch03): Documented skipped table-file callbacks
9. **Failed/empty SST cleanup** (ch03): Documented no matching OnTableFileDeleted
10. **MANIFEST verification OnIOError** (ch04): Added new section
11. **OnMemTableSealed triggers** (ch07): Expanded from "memtable full" to all SwitchMemtable paths
12. **newest_udt precondition** (ch07): Added timestamp-size requirement

## Structure/Style Fixes

1. **index.md length** (index): Expanded from 37 to ~43 lines (within 40-80 target)
2. **Shutdown guard description** (index, ch01, ch08): Narrowed from "universal" to listing which callbacks check/skip
3. **OnManualFlushScheduled mutex pattern** (ch08): Documented as exception to standard pattern

## Disagreements Found

0 factual disagreements between reviewers. One minor accuracy note about Codex's OnExternalFileIngested per-file claim (details in listener_debates.md).

## Changes Made

| File | What Changed |
|------|-------------|
| index.md | Expanded key characteristics, narrowed shutdown guard, reformatted INVARIANT markers, reached 40+ lines |
| 01_registration.md | Narrowed shutdown description in lifecycle table |
| 02_flush_compaction_events.md | Major rewrite: fixed OnManualFlushScheduled, OnCompactionBegin flow, num_l0_files, subcompaction scope, expanded SubcompactionJobInfo, added missing enum values and struct fields, added atomic flush and cross-thread notes |
| 03_file_lifecycle_events.md | Split OnTableFileCreationStarted call paths, fixed event ordering (SST before blob), split compaction ordering by type, added trivial-copy and failed-SST notes |
| 04_file_io_events.md | Fixed offset/length descriptions, split dispatch by file wrapper, added SequentialFileReader limitation, added MANIFEST verification section, added uninitialized-field warning |
| 05_error_recovery_events.md | Fixed OnBackgroundError scope and per-listener interleaving, broadened OnErrorRecoveryEnd, added ordering dependency invariants |
| 06_write_stall_pressure.md | Fixed write_stall_proximity_pct (removed memtable count), added shutdown note, added bg_pressure_callback_in_progress_ shutdown safety |
| 07_other_events.md | Expanded OnMemTableSealed triggers, added newest_udt precondition, corrected OnExternalFileIngested per-CF behavior |
| 08_threading_safety.md | Fixed OnBackgroundError threading, enumerated shutdown-checked vs non-checked callbacks, added mutex pattern exceptions, added shutdown delay note |
