# Statistics

**Files:** include/rocksdb/statistics.h, monitoring/statistics.cc, monitoring/statistics_impl.h

## Overview

The Statistics class provides global, cumulative database statistics via two types of metrics: tickers (monotonically increasing counters) and histograms (distribution summaries). Unlike PerfContext (per-thread, per-request), Statistics aggregates across all threads and all operations over the database's lifetime.

## Setup

Create a Statistics instance via CreateDBStatistics() and assign it to options.statistics before opening the database:

See Statistics class in include/rocksdb/statistics.h for the full API.

## StatsLevel Control

The StatsLevel enum (see include/rocksdb/statistics.h) controls which statistics are collected:

| Level | Description |
|-------|-------------|
| kDisableAll (also named kExceptTickers) | Disable all metrics |
| kExceptHistogramOrTimers | Disable timers and histograms |
| kExceptTimers | Disable timer stats only |
| kExceptDetailedTimers | Collect all except mutex time and compression time |
| kExceptTimeForMutex | Collect all except mutex wait time |
| kAll | Collect everything (may reduce scalability on multi-core systems) |

Set via statistics->set_stats_level(). Higher levels enable more metrics but may introduce overhead from timing calls.

## Tickers

Tickers are uint64_t counters that increment monotonically. The Tickers enum (see include/rocksdb/statistics.h) defines approximately 200 counters. Key categories:

### Block Cache

BLOCK_CACHE_MISS, BLOCK_CACHE_HIT, BLOCK_CACHE_ADD -- overall cache performance. Broken down by block type: BLOCK_CACHE_INDEX_MISS/HIT, BLOCK_CACHE_FILTER_MISS/HIT, BLOCK_CACHE_DATA_MISS/HIT, BLOCK_CACHE_COMPRESSION_DICT_MISS/HIT.

SECONDARY_CACHE_HITS, SECONDARY_CACHE_FILTER_HITS, SECONDARY_CACHE_INDEX_HITS, SECONDARY_CACHE_DATA_HITS -- secondary cache stats.

### Bloom Filter

BLOOM_FILTER_USEFUL (negatives avoided reads), BLOOM_FILTER_FULL_POSITIVE, BLOOM_FILTER_FULL_TRUE_POSITIVE -- whole-key filter stats.

BLOOM_FILTER_PREFIX_CHECKED, BLOOM_FILTER_PREFIX_USEFUL, BLOOM_FILTER_PREFIX_TRUE_POSITIVE -- prefix filter stats for point lookups.

### Read/Write Counts

NUMBER_KEYS_WRITTEN, NUMBER_KEYS_READ, BYTES_WRITTEN, BYTES_READ -- throughput counters.

NUMBER_DB_SEEK, NUMBER_DB_NEXT, NUMBER_DB_PREV -- iterator operation counts with corresponding _FOUND variants.

### Compaction

COMPACTION_KEY_DROP_NEWER_ENTRY, COMPACTION_KEY_DROP_OBSOLETE, COMPACTION_KEY_DROP_RANGE_DEL, COMPACTION_KEY_DROP_USER -- key drop reasons during compaction.

COMPACT_READ_BYTES, COMPACT_WRITE_BYTES, FLUSH_WRITE_BYTES -- I/O throughput for background operations.

### Write Path

STALL_MICROS -- cumulative time writers were stalled waiting for compaction/flush.

WRITE_DONE_BY_SELF, WRITE_DONE_BY_OTHER -- write batch group efficiency.

WAL_FILE_SYNCED, WAL_FILE_BYTES -- WAL sync frequency and size.

### Error Handling

ERROR_HANDLER_BG_ERROR_COUNT, ERROR_HANDLER_BG_IO_ERROR_COUNT, ERROR_HANDLER_AUTORESUME_COUNT, ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT -- background error tracking and auto-recovery stats.

### Compression

NUMBER_BLOCK_COMPRESSED, NUMBER_BLOCK_DECOMPRESSED -- compression/decompression counts.

BYTES_COMPRESSED_FROM, BYTES_COMPRESSED_TO, BYTES_COMPRESSION_BYPASSED, BYTES_COMPRESSION_REJECTED -- compression effectiveness metrics.

## Histograms

Histograms track distributions. The Histograms enum (see include/rocksdb/statistics.h) defines approximately 50 histograms. Each histogram provides median, p95, p99, average, standard deviation, min, max, count, and sum (see HistogramData struct).

Key histograms:

| Histogram | Description |
|-----------|-------------|
| DB_GET | Get() latency distribution |
| DB_WRITE | Write() latency distribution |
| DB_MULTIGET | MultiGet() latency distribution |
| DB_SEEK | Seek() latency distribution |
| COMPACTION_TIME | Wall clock compaction time |
| COMPACTION_CPU_TIME | CPU time for compaction |
| FLUSH_TIME | Memtable flush time |
| WRITE_STALL | Write stall duration distribution |
| SST_READ_MICROS | SST file read latency |
| WAL_FILE_SYNC_MICROS | WAL sync latency |
| MANIFEST_FILE_SYNC_MICROS | MANIFEST sync latency |
| BYTES_PER_READ / BYTES_PER_WRITE | Value size distribution |

## Stats Dumping and Persistence

RocksDB can automatically dump statistics to the LOG file and persist them for historical analysis:

| Option | Default | Description |
|--------|---------|-------------|
| stats_dump_period_sec | 600 | Seconds between stats dumps to LOG (0 disables) |
| stats_persist_period_sec | 600 | Seconds between stats persistence to internal storage |
| stats_history_buffer_size | 1 MB | Memory limit for in-memory stats history |
| persist_stats_to_disk | false | When true, stats snapshots are persisted to a hidden column family (___rocksdb_stats_history___) instead of in-memory history. This provides durable stats history across restarts but adds a hidden column family to the database |

See DBOptions in include/rocksdb/options.h for these options.

Historical stats can be retrieved via DB::GetStatsHistory() which returns a StatsHistoryIterator over timestamped stats snapshots.

## Querying Statistics

- getTickerCount(ticker) -- returns cumulative count
- histogramData(histogram, &data) -- fills HistogramData struct with distribution summary
- getAndResetTickerCount(ticker) -- atomically reads and resets (useful for interval stats)
- ToString() -- returns a multi-line string with all non-zero stats
