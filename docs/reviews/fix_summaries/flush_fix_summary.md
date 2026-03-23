# Fix Summary: flush

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 15 |
| Completeness | 6 |
| Structure/Style | 3 |
| Total | 24 |

## Disagreements Found

2 disagreements documented in `flush_debates.md`:
1. **Inline code quotes style** -- CC says clean, Codex says violation. Resolved: CC correct (backtick identifiers are standard, not "inline code quotes").
2. **MemPurge success return behavior** -- CC missed it, Codex flagged as WRONG. Resolved: Codex correct (Run() continues through install path).

## Changes Made

### 01_triggers_and_configuration.md
- Added `kOthers` to FlushReason table; noted `kTest` and `kAutoCompaction` existence
- Fixed `memtable_max_range_deletions` type from `int` to `uint32_t` and header from `advanced_options.h` to `options.h`
- Fixed `max_total_wal_size` default formula to `4 * sum(write_buffer_size * max_write_buffer_number)`
- Fixed `max_background_flushes` description to state both legacy knobs must be -1 for quarter-allocation
- Fixed `max_write_buffer_number` minimum: value sanitized to 2, not allowed to be 1
- Replaced nonexistent `HandleWALFull()` with correct `SwitchWAL()` path; documented multi-CF selection
- Fixed WriteBufferManager trigger: single CF in non-atomic path, multi-CF in atomic path
- Added shared WriteBufferManager cross-DB behavior
- Added persistent-stats CF coupling via `MaybeFlushStatsCF()`
- Added scan-triggered flushes section (`memtable_op_scan_flush_trigger`)

### 03_flush_job_lifecycle.md
- Fixed MemPurge eligibility reference from internal `options/db_options.h` to public `include/rocksdb/options.h`
- Fixed MemPurge success: Run() continues through install path with `write_edits = false`, not immediate return

### 04_building_sst_file.md
- Reframed Step 5 around `CompactionIterator` as the central transformation stage
- Added second verification check (output SST entry count vs builder count)

### 06_atomic_flush.md
- Fixed `InstallMemtableAtomicFlushResults()`: processes only the caller-provided `mems_list`, does not rescan for extra completed batches

### 07_write_stalls.md
- Added `db/column_family.h` to Files line
- Fixed memtable stall count: uses `imm()->NumNotFlushed()` (immutable only), not `GetUnflushedMemTableCountForWriteStallCheck()`
- Fixed `delayed_write_rate` default: 0 in DBOptions, inferred to 16 MB/s during DB open if no rate limiter
- Fixed rate adjustment factors: added 0.6x near-stop penalty and 1.4x recovery reward

### 08_mempurge.md
- Fixed eligibility reference to public header
- Added `SetMempurgeUsed()` flag documentation
- Added `ConstructFragmentedRangeTombstones()` ordering constraint before mutex reacquire
- Added MemPurge post-install note (TryInstallMemtableFlushResults with write_edits=false)
- Added memtable-rep random sampling dependency to limitations

### 09_scheduling.md
- Fixed UDT retention sleep from `1000 microseconds` to `100000 microseconds` (100 ms)
- Fixed SwitchMemtable empty-CF update: not limited to single-CF DBs
- Clarified GetBGJobLimits: quarter-allocation requires both legacy knobs at -1
- Clarified fallback to LOW pool cap semantics
- Fixed scheduling counter lifecycle description (decrement then MaybeSchedule, not simultaneous)
- Added UDT retention note: uses `GetUnflushedMemTableCountForWriteStallCheck()` (stricter check)
- Added note that manual flush skips UDT retention

### 10_error_handling.md
- Fixed `bg_work_paused_` description: only from PauseBackgroundWork(), not hard errors
- Added `ErrorHandler::IsBGWorkStopped()` as actual hard error gate
- Added `kUnrecoverableError` to severity table
- Added soft-error `soft_error_no_bg_work_` note
- Fixed MANIFEST error classification: write and rename share same error bucket
- Added recovery-only flush semantics and catch-up flush after recovery
