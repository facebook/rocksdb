# PerfContext and IOStatsContext

**Files:** include/rocksdb/perf_context.h, include/rocksdb/iostats_context.h, include/rocksdb/perf_level.h, monitoring/perf_context.cc, monitoring/iostats_context.cc

## Overview

PerfContext and IOStatsContext are thread-local structures that collect fine-grained performance counters for individual database operations. Unlike the global Statistics system, these provide per-thread, per-request metrics with zero inter-thread contention. They are the primary tool for understanding where time is spent within a single Get, Seek, or Write operation.

## PerfLevel Control

The PerfLevel enum (see include/rocksdb/perf_level.h) controls which metrics are collected. Levels are incremental -- each higher level enables all metrics from lower levels plus additional ones:

| Level | Value | Additional Metrics Enabled |
|-------|-------|---------------------------|
| kUninitialized | 0 | Sentinel value meaning "not yet set"; should not be used directly |
| kDisable | 1 | None (all counters disabled) |
| kEnableCount | 2 | Count metrics (e.g., block_cache_hit_count, block_read_count, bloom_filter_useful) |
| kEnableWait | 3 | Wait/delay time metrics (e.g., write_delay_time, write_memtable_time) |
| kEnableTimeExceptForMutex | 4 | End-to-end timing metrics (e.g., get_from_memtable_time, seek_internal_seek_time) |
| kEnableTimeAndCPUTimeExceptForMutex | 5 | CPU time metrics (e.g., get_cpu_nanos, iter_next_cpu_nanos) |
| kEnableTime | 6 | Mutex timing metrics (e.g., db_mutex_lock_nanos, db_condition_wait_nanos) |

Set the level per thread via SetPerfLevel() and query it via GetPerfLevel(). The default level is kEnableCount for all build modes.

## PerfContext Metrics

Access the thread-local PerfContext via get_perf_context() (see include/rocksdb/perf_context.h). The PerfContextBase struct contains all metrics. Key categories:

### Block Cache and Read Metrics

block_cache_hit_count, block_read_count, block_read_byte, block_read_time, block_read_cpu_time -- overall block access statistics.

block_cache_index_hit_count, index_block_read_count, block_cache_filter_hit_count, filter_block_read_count -- per-block-type cache hit/miss breakdown.

secondary_cache_hit_count, compressed_sec_cache_insert_real_count -- secondary cache interaction metrics.

### Bloom Filter Metrics

bloom_memtable_hit_count, bloom_memtable_miss_count -- memtable bloom filter effectiveness.

bloom_sst_hit_count, bloom_sst_miss_count -- SST bloom filter effectiveness.

### Iterator Metrics

internal_key_skipped_count, internal_delete_skipped_count, internal_recent_skipped_count -- keys skipped during iteration (high values indicate tombstone or version accumulation).

internal_merge_count, internal_merge_point_lookup_count -- merge operand processing.

internal_range_del_reseek_count -- reseeking to skip range deletions.

### Iterator Call Counts (Experimental)

iter_next_count, iter_prev_count, iter_seek_count -- total number of Next/Prev/Seek calls made during an operation.

### Blob Metrics

blob_cache_hit_count, blob_read_count, blob_read_byte, blob_read_time, blob_checksum_time, blob_decompress_time -- metrics for BlobDB operations including cache hits, reads, and processing time.

### Transaction Metrics

key_lock_wait_time, key_lock_wait_count -- time and count of key lock waits during transactions.

### Timing Breakdown for Get

get_snapshot_time, get_from_memtable_time, get_post_process_time, get_from_output_files_time -- time breakdown within a Get operation.

### Timing Breakdown for Write

write_wal_time, write_memtable_time, write_delay_time, write_scheduling_flushes_compactions_time, write_pre_and_post_process_time -- time breakdown within a Write operation.

### Per-Level Metrics

PerfContextByLevel (enabled via EnablePerLevelPerfContext()) provides per-LSM-level bloom filter stats, user key return count, and block cache hit/miss counts. Accessed via level_to_perf_context map.

## IOStatsContext Metrics

Access via get_iostats_context() (see include/rocksdb/iostats_context.h). The IOStatsContext struct tracks:

| Metric | Description |
|--------|-------------|
| bytes_written / bytes_read | Total bytes written/read by this thread |
| open_nanos | Time in open()/fopen() |
| allocate_nanos | Time in fallocate() |
| write_nanos / read_nanos | Time in write()/pwrite() and read()/pread() |
| range_sync_nanos | Time in sync_file_range() |
| fsync_nanos | Time in fsync() |
| prepare_write_nanos | Time preparing writes (fallocate etc.) |
| logger_nanos | Time in Logger::Logv() |
| cpu_write_nanos / cpu_read_nanos | CPU time (not wall time) in write/read |

### IO by Temperature

The FileIOByTemperature struct within IOStatsContext tracks read bytes and read counts per file temperature tier: hot, warm, cool, cold, ice, unknown_non_last_level, and unknown_last_level. The "unknown" tier is split into two categories to distinguish between files at the last level and those at other levels, which is important for tiered storage debugging.

### disable_iostats Flag

Setting IOStatsContext::disable_iostats = true disables counter collection for the current thread. This is useful when file operations (such as logging) should not pollute the IO statistics for user operations.

## Usage Pattern

Typical usage for profiling a single operation:

Step 1: Call SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex) at the start of the thread or before the operation.

Step 2: Call get_perf_context()->Reset() and get_iostats_context()->Reset() to zero all counters.

Step 3: Perform the database operation (Get, Seek, Write, etc.).

Step 4: Read counters from get_perf_context() and get_iostats_context().

Step 5: Call ToString() on either context to get a human-readable summary. Pass true to exclude zero-valued counters.

## Compile-Time Disable

Building with -DNPERF_CONTEXT replaces the thread-local PerfContext with a global no-op instance, eliminating all overhead. Similarly, -DNIOSTATS_CONTEXT disables IOStatsContext. Use these flags in production builds where per-request instrumentation is not needed.
