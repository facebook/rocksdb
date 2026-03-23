# Fix Summary: write_flow

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness (wrong facts) | 10 |
| Completeness (missing info) | 5 |
| Structure/Style | 3 |
| **Total** | **18** |

## Disagreements Found

0 direct disagreements between CC and Codex reviewers. Both reviewers flagged overlapping issues and agreed on all shared topics. Where one reviewer raised an issue not covered by the other, code verification confirmed the claim. No debates.md file created.

## Changes Made

### 01_write_apis.md
- **[correctness]** Fixed sequence number timing: changed "filled by write leader after WAL write" to "filled by write leader before WAL append" (Codex)
- **[correctness]** Fixed WriteBatch Important note: sequence is stamped into merged batch header BEFORE WAL append, not after (Codex)
- **[structure]** Split "WriteOptions Validation" into two sections: per-write "WriteOptions Validation" and "DBOptions Incompatibility Checks" for immutable option conflicts (CC)

### 02_write_thread.md
- **[correctness]** Fixed spin time from "approximately 1.4 microseconds" to "approximately 1 microsecond on modern hardware" (CC)
- **[correctness]** Fixed Adaptive Wait: described both mechanisms -- within-phase cutoff (3 slow yields break) AND cross-invocation AdaptationContext yield_credit with exponential decay (CC)
- **[correctness]** Rewrote Write Stall Barrier: replaced nonexistent `stall_flag_` with actual `write_stall_dummy_` sentinel mechanism -- LinkOne insertion, queued-writer scan, stall_cv_ condvar blocking, EndWriteStall unlinking (both CC + Codex)

### 03_wal.md
- **[completeness]** Added `kRecyclePredecessorWALInfoType = 131` to record types table and noted `kMaxRecordType` (CC)
- **[style]** Downgraded WAL fragment ordering from "Key Invariant" to "Format rule" (CC)
- **[correctness]** Rewrote WAL Tracking in MANIFEST: clarified that live-WAL syncs are not tracked, only closed/inactive WAL sizes (Codex)
- **[correctness]** Split WAL chain verification into separate section: attributed predecessor-WAL records to `track_and_verify_wals` option (not `track_and_verify_wals_in_manifest`), explained both mechanisms are independent (Codex)
- **[correctness]** WAL recycling: added exception for `two_write_queues && disable_memtable` internal path (Codex)

### 04_memtable_insert.md
- **[correctness]** Fixed ShouldFlushNow description: replaced "60% headroom" with precise formula "write_buffer_size + kArenaBlockSize * kAllowOverAllocationRatio" (CC)

### 05_sequence_numbers.md
- **[completeness]** Added `LastPublishedSequence` to sequence number space table with description (Codex)
- **[correctness]** Rewrote Normal Write Path steps: sequence is stamped into merged batch header via SetSequence() BEFORE WAL append, then individual writer sequences assigned AFTER WAL write (Codex)
- **[style]** Removed "Key Invariant" label for sequence timing claim; replaced with "Design note" (CC + Codex)
- **[correctness]** Fixed WBWI: changed "key count" to "operation count (GetWriteBatch()->Count())" (CC)
- **[correctness]** Fixed FetchAddLastAllocatedSequence: changed "Used only in two-queue mode" to include unordered write mode and recoverable-state path (Codex)

### 06_write_modes.md
- **[correctness]** Fixed pipelined write Step 5: only ONE writer is promoted to STATE_MEMTABLE_WRITER_LEADER, not all writers (Codex)
- **[correctness]** Rewrote unordered write tradeoff: explained publish-before-memtable mechanism, relaxed snapshot immutability, pending_memtable_writes_ tracking, and switch_cv_ blocking (both CC + Codex)

### 07_flow_control.md
- **[correctness]** Added missing third predicate to memtable delay condition: `num_unflushed - 1 >= min_write_buffer_number_to_merge` (both CC + Codex)
- **[correctness]** Fixed compaction pressure speedup threshold: changed from "midpoint" to actual formula `min(2 * trigger, trigger + (slowdown - trigger) / 4)` (both CC + Codex)

### 08_tombstone_lifecycle.md
- **[correctness]** Rewrote SingleDelete cleanup with all compaction iterator cases: consecutive SingleDeletes, SD+Delete with enforce_single_del_contracts, SD+non-Put types, zero Puts, snapshot retention (both CC + Codex)
- **[style]** Downgraded range tombstone truncation from "Key Invariant" to "Design property" (CC)

### 09_crash_recovery.md
- **[correctness]** Fixed WAL recovery mode default: changed from `kTolerateCorruptedTailRecords` to `kPointInTimeRecovery` (both CC + Codex)
- **[correctness]** Rewrote recovery Steps 4-5: added avoid_flush_during_recovery, enforce_write_buffer_manager_during_recovery, mid-recovery flush, read-only mode, RestoreAliveLogFiles behavior (both CC + Codex)
- **[correctness]** Fixed WAL Chain Verification: attributed to `track_and_verify_wals` (not `track_and_verify_wals_in_manifest`), cross-linked both mechanisms (Codex)
- **[correctness]** Fixed atomic flush recovery claim: replaced "all CFs flushed atomically" with accurate description of MANIFEST AtomicGroup semantics (Codex)

### 10_performance.md
- **[correctness]** Fixed L0-L1 write amplification: changed from "O(num_L0_files)" to "~1 + L1_size/L0_size" with steady-state explanation (CC)
- **[correctness]** Fixed WAL compression: changed "Snappy, ZSTD" to "ZSTD only" (Codex)
