# Fix Summary: debugging_tools

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 22 |
| Completeness | 8 |
| Structure/Style | 3 |
| Depth | 4 |

## Disagreements Found

3 disagreements documented in debugging_tools_debates.md:
1. ManifestDumpCommand internal function name (low risk)
2. RepairDB phase description level of detail (medium risk)
3. Level of detail for ldb Run() dispatch (low risk)

## Changes Made

### index.md
- Fixed FileSystemTracer -> FileSystemTracingWrapper in chapter 5 summary
- Fixed PerfLevel count (6 -> 7 levels including kUninitialized)
- Removed non-invariant about LOG file location from Key Invariants section
- Updated chapter 11 summary to use rocksdb_dump/rocksdb_undump names

### 01_ldb_tool.md
- Rewrote Run() dispatch flow: OpenDB calls PrepareOptions internally, not Run() directly; added NoDBOpen and try_load_options paths
- Fixed SingleDelete constraint: "only one Put since the previous SingleDelete for the same key"
- Fixed manifest_dump: calls DumpManifestFile() which calls VersionSet::DumpManifest(); added auto-discovery behavior
- Separated dump_live_files and list_live_files_metadata into distinct entries with accurate descriptions
- Fixed file_checksum_dump: recovers VersionSet from MANIFEST, not raw manifest dump
- Fixed checkconsistency: forces paranoid_checks during open, not a dedicated verification pass
- Fixed change_compaction_style: level-to-universal only, universal-to-level rejected
- Fixed reduce_levels: uses highest non-empty level, not configured num_levels; added single-nonempty-level precondition
- Fixed read_timestamp: required/rejected based on CF comparator, not always optional
- Fixed LDBTool::Run() deprecation: documented RunAndReturn() as preferred API
- Added 10 missing subcommands: approxsize, query, dump, load, idump, create_column_family, drop_column_family, list_file_range_deletes, update_manifest, compaction_progress_dump
- Added --txn_write_policy global option
- Expanded scan command description with TTL, timestamp, wide-column, write-time features

### 02_sst_dump_tool.md
- Fixed check comparison: num_entries - num_range_deletions, not raw num_entries
- Fixed raw output naming: strips .sst before appending _dump.txt
- Fixed scan output format: multiple format variants for wide columns, timestamps, blob indexes

### 03_operation_tracing.md
- Added utilities/trace/replayer_impl.cc to Files line
- Rewrote trace_analyzer as flag-driven tool with per-operation-type enable flags
- Documented MultiGet per-key accounting behavior
- Added GFLAGS build dependency note

### 04_block_cache_tracing.md
- Rewrote tracing lifecycle: documented both API overloads, clarified BlockCacheTraceOptions vs BlockCacheTraceWriterOptions
- Added spatial sampling section explaining block-key-based hash sampling
- Added full BlockCacheTraceRecord field table with all 17 fields

### 05_io_tracing.md
- Added env/file_system_tracer.h and env/file_system_tracer.cc to Files line
- Fixed timestamps: nanoseconds (NowNanos/ElapsedNanos), not microseconds
- Fixed class name: FileSystemTracingWrapper, not FileSystemTracer
- Fixed io_tracer_parser usage: --io_trace_file=<path>, not positional argument
- Added note about no footer on EndIOTrace
- Added GFLAGS build dependency note

### 06_perf_context.md
- Fixed PerfLevel default: kEnableCount for all build modes, not build-mode dependent
- Added kUninitialized (value 0) to PerfLevel table
- Fixed IOStatsContext temperature tiers: 7 categories with unknown_non_last_level and unknown_last_level
- Added Blob Metrics section (blob_cache_hit_count, blob_read_count, etc.)
- Added Iterator Call Counts section (iter_next_count, iter_prev_count, iter_seek_count)
- Added Transaction Metrics section (key_lock_wait_time, key_lock_wait_count)

### 07_statistics.md
- Fixed kDisableAll/kExceptTickers: clarified they are aliases (same value)
- Added persist_stats_to_disk option to Stats Dumping section

### 08_db_properties.md
- Fixed rocksdb.cfstats type: string/map (dual-form), not map-only

### 09_logger.md
- Fixed rotated log naming: documented both modes (db_log_dir empty vs set)
- Added keep_log_file_num > 0 validation requirement
- Documented old-log trimming on rollover and startup

### 10_thread_status.md
- Fixed enable_thread_tracking: controls operation detail reporting, not thread registration
- Fixed GetThreadList: returns registered threads including idle ones, not just active ones
- Added OP_GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST to operation types

### 11_repair_and_recovery.md
- Added rocksdb_dump.cc and rocksdb_undump.cc to Files line
- Fixed binary names: rocksdb_dump/rocksdb_undump, not db_dump/db_undump
- Fixed --db_path flag (not --db) for rocksdb_dump
- Fixed Phase 1: includes archiving old MANIFESTs and creating fresh descriptor
- Fixed Phase 2: per-WAL per-CF table creation via BuildTable(), not "flush when full"
- Fixed Phase 4: next_file_number may exceed initial scan value
- Added GFLAGS build dependency note

### 12_debugging_workflows.md
- Fixed slow-read workflow Step 3: use Statistics tickers for hit rate, not incompatible PerfContext counters
- Fixed sst_dump command form: --command=verify, not --verify
- Added EventListener cross-reference with link to listener.md
