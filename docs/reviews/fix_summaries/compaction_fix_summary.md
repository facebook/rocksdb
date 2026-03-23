# Fix Summary: compaction

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 12 |
| Completeness | 4 |
| Structure/style | 0 |

## Disagreements Found

8 disagreements documented in `compaction_debates.md`:
1. CompactionJob pipeline scope (Codex right, high risk)
2. Inline code quotes style interpretation (CC right, low risk)
3. CompactionService header location (CC wrong, low risk)
4. FIFO ttl default value (Codex right, medium risk)
5. Universal periodic_compaction_seconds default (Codex right, medium risk)
6. Dynamic level base shift threshold (Codex right, medium risk)
7. Unnecessary level draining (Codex right, medium risk)
8. L0 concurrency with intra-L0 (Codex right, low risk)

## Changes Made

### 01_overview.md
- Re-scoped pipeline description: added dispatch table showing FIFO deletion, FIFO trivial copy, trivial move, and full merge paths
- Qualified L0 concurrency: intra-L0 can run concurrently with L0-to-base

### 02_leveled_compaction.md
- Removed `level_compaction_dynamic_file_size` reference; stated optimization is always enabled since RocksDB 9.0
- Qualified L0 concurrency to note size-based intra-L0 concurrent fallback

### 03_file_selection_and_priority.md
- Fixed kMinOverlappingRatio tie-break: smallest-key ordering (not key-range width)

### 04_dynamic_levels.md
- Fixed base shift threshold: 256 MB (not 2.56 GB) with default settings
- Fixed unnecessary level description: targets are uint64_max but scores are boosted to ensure draining

### 05_trivial_move.md
- Updated Files line: added `db/db_impl/db_impl_compaction_flush.cc`, removed `compaction_job.cc`
- Fixed execution section: trivial move bypasses CompactionJob, calls PerformTrivialMove() directly

### 06_universal_compaction.md
- Clarified size amplification uses compensated sizes for candidates, raw for base
- Fixed periodic_compaction_seconds default to 30 days for block-based tables
- Added ttl folding explanation

### 07_fifo_compaction.md
- Updated Files line: added `db/column_family.cc` and `db/db_impl/db_impl_compaction_flush.cc`
- Removed phantom `change_temperature_compaction_only` option
- Added real `allow_trivial_copy_when_change_temperature` and `trivial_copy_buffer_size` options
- Fixed ttl default: 30 days for block-based tables, 0 otherwise (not simply 0)
- Explained temperature-change trivial copy is a real I/O copy, not metadata-only

### 08_compaction_job.md
- Scoped lifecycle description to merge-style compactions
- Removed `level_compaction_dynamic_file_size` reference from grandparent tracking

### 09_subcompaction.md
- Expanded ShouldFormSubcompactions table: added PlainTable exclusion, RoundRobin bypass, precise conditions for leveled/universal
- Fixed round-robin init: "start-level input files" (not "L0 input files")

### 10_remote_compaction.md
- Added CancelAwaitingJobs() to limitations section

### 12_compaction_filter.md
- Updated Files line: added `db/builder.cc` and `db/flush_job.cc`
- Scoped precedence rule to compaction-time filtering; noted flush requires factory path

### 13_sst_partitioner.md
- Attributed L0 guard to CompactionOutputs constructor (not CreateSstPartitioner)

### index.md
- Qualified L0 compaction invariant to note intra-L0 may run in parallel
