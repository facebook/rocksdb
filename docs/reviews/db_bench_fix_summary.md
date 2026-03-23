# Fix Summary: db_bench

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 20 |
| Completeness | 10 |
| Depth | 6 |
| Structure/Style | 2 |
| **Total** | **38** |

## Disagreements Found

7 cases where Codex flagged issues CC missed. No direct contradictions between reviewers. All verified against source code. Details in `db_bench_debates.md`.

## Changes Made

### index.md
- Removed hardcoded line count (~9400) that would become stale
- Renamed "Key Invariants" to "Key Measurement Rules" (items are usage guidance, not invariants)
- Fixed --num description to clarify dual purpose (key space + default op count)

### 01_architecture.md
- Added missing `kMultiScan` to OperationType enum table

### 02_write_benchmarks.md
- Fixed DoWrite Step 3: always uses WriteBatch (even batch_size=1), never direct Put(). Noted stacked BlobDB exception
- Fixed key prefix derivation: uses `v % (num_keys / keys_per_prefix)`, not contiguous groups. Clarified binary byte format
- Fixed user-defined timestamps: injected via WriteBatch::UpdateTimestamps(), not via separate API overloads
- Fixed overwrite description: doesn't guarantee hitting existing keys without prior load phase
- Added fillanddeleteuniquerandom benchmark
- Added disposable entries incompatibility note

### 03_read_benchmarks.md
- Fixed SeekRandom: always uses Seek() (not SeekForPrev for reverse); op count is 1 (not 1+seek_nexts); rate limiting every 256 ops
- Fixed ReadSequential: does NOT wrap with SeekToFirst when iterator becomes invalid
- Fixed multiscan: uses seek_nexts (default 50), not batch_size for per-scan key count
- Fixed readrandomfast: noted power-of-two rounding and intentional misses
- Fixed multireadrandom: noted it operates on default CF only
- Added deleteseq, deleterandom, getmergeoperands, newiterator, approximatesizerandom benchmarks

### 04_mixed_benchmarks.md
- Fixed concurrent read-write section: BGScan (readwhilescanning) does NOT exclude stats; BGWriter termination depends on reader threads and --finish_after_writes
- Added BGWriter private rate limiter note
- Fixed updaterandom: read-then-overwrite with fresh random value, NOT counter increment
- Fixed randomwithverify: three-key consistency pattern, NOT truth DB
- Fixed flush: only calls DB::Flush(), NOT FlushWAL
- Fixed randomtransaction: doesn't require transaction flags (falls back to DBInsert); transaction_sleep is unused
- Fixed timeseries: readers Seek and iterate forward (not "latest timestamp" lookups); writer excluded from stats with separate report; delete-mode thread split
- Replaced unverifiable "3x throughput" claim with qualitative explanation
- Fixed backup/restore: restore uses BOTH backup_dir and restore_dir
- Added block_cache_entry_stats, cache_report_problems, fillseekseq, randomreplacekeys utility benchmarks

### 05_configuration_flags.md
- Fixed format_version default: 7, not 6
- Fixed --duration description: applies to many benchmark types, not just random-mode
- Fixed compression_manager: added costpredictor and autoskip values
- Added note that level_compaction_dynamic_level_bytes defaults differ between db_bench (false) and library (true)

### 06_metrics_and_reporting.md
- Fixed CSV reporting: column value is raw interval ops, not QPS
- Added note about write histograms hiding rate limiter sleep via ResetLastOpTime
- Added note about done_=1 forcing for single-operation benchmarks
- Fixed time-based intervals: clarified 1000-op polling granularity

### 07_perf_integration.md
- Split interval stats description into single-CF vs multi-CF behavior

### 08_methodology.md
- Added new pitfall #8: DB state reuse across warmups and repeats (RNG seeds change, write benchmarks accumulate)
- Renumbered subsequent pitfalls (8->9, 9->10, 10->11)

### 09_mixgraph.md
- Fixed value size handling: modulo, not clamp
- Added key distribution precedence: key_dist_a/b must both be non-zero for prefix model
- Added sine_write_rate vs sine_mix_rate distinction
- Fixed rate limiting: conditional on sine_mix_rate, not unconditional

### 10_extending.md
- Fixed multi-DB selection: random per-call via thread->rand.Next(), not round-robin by thread ID. Noted sequential write exception
