# Debugging Workflows

**Files:** This chapter references tools and APIs documented in the preceding chapters.

## Overview

This chapter provides step-by-step debugging workflows for common RocksDB operational issues. Each workflow combines multiple tools and APIs for systematic diagnosis.

## Workflow 1: Investigating Data Corruption

**Symptoms:** Corruption: checksum mismatch errors during reads.

Step 1 -- Identify the corrupted file: Check the LOG file for the filename and offset in the error message. Search for "Corruption" in the LOG.

Step 2 -- Verify file integrity: Run sst_dump --file=<path> --command=verify to verify all block checksums. If verification fails, the file has storage-level corruption.

Step 3 -- Determine data loss scope: Run sst_dump --file=<path> --command=scan --output_hex to see which keys are readable. The last readable key before corruption establishes the scope of affected data.

Step 4 -- Recovery options:
- **Option A (data loss):** Remove the corrupted file and run ldb repair --db=<path> to rebuild the MANIFEST. Data in the corrupted file is lost.
- **Option B (restore):** Restore from a backup if available via BackupEngine::RestoreDBFromLatestBackup() or rocksdb_undump.

## Workflow 2: Diagnosing Slow Reads

**Symptoms:** High Get/MultiGet latency.

Step 1 -- Profile a single request: Set PerfLevel::kEnableTimeAndCPUTimeExceptForMutex, reset PerfContext, execute the Get, and examine the timing breakdown: get_from_memtable_time, get_from_output_files_time, block_read_time, block_decompress_time.

Step 2 -- Check bloom filter effectiveness: Examine bloom_filter_useful (negatives saved) vs bloom_sst_hit_count (filter checked) in PerfContext. Low bloom_filter_useful relative to bloom_sst_miss_count suggests the filter is not effective for the workload.

Step 3 -- Check block cache hit rate: Use BLOCK_CACHE_HIT and BLOCK_CACHE_MISS Statistics tickers to compute the cache hit rate (hit / (hit + miss)). For per-request analysis, block_cache_hit_count in PerfContext shows cache hits and block_read_count shows reads from storage (these are not directly numerator/denominator of a hit rate). Also check rocksdb.block-cache-usage and rocksdb.block-cache-capacity properties for capacity analysis. Block cache tracing (chapter 4) provides the most detailed per-block hit/miss analysis.

Step 4 -- Check level distribution: Use ldb manifest_dump --verbose or query rocksdb.levelstats property. Too many L0 files (more than level0_slowdown_writes_trigger) indicates compaction is falling behind.

Step 5 -- Check for key skipping: High internal_key_skipped_count or internal_delete_skipped_count in PerfContext indicates many obsolete versions or tombstones. Manual compaction (ldb compact) may help.

## Workflow 3: Diagnosing Slow Writes

**Symptoms:** High write latency or write stalls.

Step 1 -- Check write stalls: Query rocksdb.cf-write-stall-stats and rocksdb.db-write-stall-stats properties. Check STALL_MICROS ticker in Statistics.

Step 2 -- Profile write time: Use PerfContext fields write_wal_time, write_memtable_time, write_delay_time, write_scheduling_flushes_compactions_time. High write_delay_time confirms write stalls.

Step 3 -- Check compaction progress: Query rocksdb.num-running-compactions, rocksdb.compaction-pending, and rocksdb.estimate-pending-compaction-bytes properties. Use GetThreadList() to see what compaction threads are doing and how long they have been running.

Step 4 -- Check flush progress: High rocksdb.num-immutable-mem-table indicates flushes are falling behind. Check if flush threads are busy via GetThreadList().

Step 5 -- Check write rate: WAL_FILE_SYNCED and WAL_FILE_BYTES tickers show WAL sync frequency and throughput. High sync frequency with small writes suggests batching could help.

## Workflow 4: Investigating Space Amplification

**Symptoms:** Database size much larger than expected live data.

Step 1 -- Get per-level statistics: Query rocksdb.stats property or use ldb manifest_dump --verbose. Check the size distribution across levels.

Step 2 -- Check blob files: If BlobDB is enabled, query rocksdb.blob-stats and rocksdb.live-blob-file-garbage-size. High garbage ratio indicates blob GC is needed.

Step 3 -- Estimate live data: Query rocksdb.estimate-live-data-size and compare against rocksdb.total-sst-files-size. A large ratio indicates high space amplification.

Step 4 -- Check obsolete files: Query rocksdb.obsolete-sst-files-size. Non-zero values indicate files waiting for deletion (possibly held by iterators or DisableFileDeletions()).

Step 5 -- Solutions:
- Manual compaction: ldb compact to force compaction and reclaim space
- Enable level_compaction_dynamic_level_bytes (see ColumnFamilyOptions in include/rocksdb/advanced_options.h)
- Check rocksdb.num-snapshots -- old snapshots prevent garbage collection

## Workflow 5: Recovering from Crash

**Symptoms:** Database fails to open after crash.

Step 1 -- Check the LOG file: Read the tail of the LOG file for the last error message.

Step 2 -- Common errors and solutions:

| Error | Cause | Solution |
|-------|-------|---------|
| Corruption: missing WAL file | WAL file lost/deleted | Set wal_recovery_mode = kTolerateCorruptedTailRecords or run ldb repair |
| Corruption: bad magic number in MANIFEST | MANIFEST file corrupted | Run ldb repair |
| IO error: No such file: <N>.sst | SST file missing | Run ldb repair (data in that file is lost) |
| Corruption: checksum mismatch in WAL | WAL corruption | Set appropriate wal_recovery_mode or run ldb repair |

Step 3 -- If repair fails: Restore from backup or accept data loss and create a new database.

## Workflow 6: Analyzing Compaction Behavior

Step 1 -- Check compaction stats: Query rocksdb.stats for per-level compaction I/O, write amplification, and compaction time.

Step 2 -- Monitor active compactions: Use GetThreadList() with enable_thread_tracking = true to see compaction progress, input/output levels, and bytes processed.

Step 3 -- Check compaction IO via tracing: Use IO tracing (DB::StartIOTrace()) to capture all file system operations during compaction. Parse with io_tracer_parser to see per-file read/write patterns.

Step 4 -- Analyze key distribution: Use query tracing (DB::StartTrace()) to capture the workload, then run trace_analyzer to identify hot key prefixes and access patterns that might benefit from different compaction configurations.

## Workflow 7: Validating Compression Effectiveness

Step 1 -- Check current compression ratio: Query rocksdb.compression-ratio-at-level<N> for each level.

Step 2 -- Test alternative algorithms: Use sst_dump --command=recompress on representative SST files to compare algorithms and compression levels.

Step 3 -- Check compression stats: Monitor BYTES_COMPRESSED_FROM, BYTES_COMPRESSED_TO, BYTES_COMPRESSION_REJECTED tickers to see how much data is being compressed and how often compression is rejected.

## Tool Selection Guide

| Question | Primary Tool | Secondary Tool |
|----------|-------------|----------------|
| Why is this Get slow? | PerfContext | Statistics histograms |
| What is the cache hit rate? | Statistics tickers | PerfContext, block cache trace |
| What are threads doing? | GetThreadList() | LOG file |
| Why are writes stalling? | DB properties, Statistics | PerfContext |
| Is this SST file corrupted? | sst_dump --command=verify | ldb checkconsistency |
| What IO is happening? | IO tracing | IOStatsContext |
| What is the workload pattern? | Query tracing + trace_analyzer | Statistics |
| How much space is wasted? | DB properties | ldb manifest_dump |
| What happened before a crash? | LOG file | ldb dump_wal |
| Real-time event monitoring? | EventListener callbacks | LOG file |

**Note:** EventListener (see include/rocksdb/listener.h) provides callback hooks for significant database events (compaction, flush, file creation/deletion, background errors). For detailed EventListener documentation, see the listener.md component documentation.
