# DB Properties

**Files:** include/rocksdb/db.h (Properties struct), db/internal_stats.h, db/internal_stats.cc

## Overview

DB properties provide runtime visibility into database state, including memory usage, compaction progress, file counts, write stall status, and aggregated table properties. They are queried via DB::GetProperty(), DB::GetIntProperty(), and DB::GetMapProperty().

## Query Methods

| Method | Return Type | Use When |
|--------|------------|----------|
| GetProperty(cf, name, &string) | string | Multi-line human-readable output (e.g., rocksdb.stats) |
| GetIntProperty(cf, name, &uint64) | uint64_t | Single numeric value (e.g., rocksdb.estimate-num-keys) |
| GetMapProperty(cf, name, &map) | map<string,string> | Structured key-value data (e.g., rocksdb.cfstats) |
| GetAggregatedIntProperty(name, &uint64) | uint64_t | Aggregated across all column families |

All property names start with "rocksdb.". See DB::Properties struct in include/rocksdb/db.h for the complete list.

**Note:** Property names cannot end in numbers, as trailing numbers are parsed as arguments (e.g., rocksdb.num-files-at-level<N> uses <N> as a level number argument).

**Note:** Several properties support both string and map access paths. Properties like rocksdb.cfstats, rocksdb.dbstats, rocksdb.block-cache-entry-stats, rocksdb.cf-write-stall-stats, rocksdb.db-write-stall-stats, and rocksdb.aggregated-table-properties can be queried via either GetProperty() or GetMapProperty().

## Key Properties

### Compaction and Level Stats

| Property | Type | Description |
|----------|------|-------------|
| rocksdb.stats | string | Combined CF stats + DB stats (the main debugging property) |
| rocksdb.cfstats | string/map | Column family stats per level |
| rocksdb.dbstats | string/map | Database-wide stats (cumulative and interval) |
| rocksdb.levelstats | string | Files per level and total size |
| rocksdb.num-files-at-level<N> | string | Number of files at level N |
| rocksdb.compression-ratio-at-level<N> | string | Compression ratio at level N |
| rocksdb.aggregated-table-properties | string/map | Aggregated table properties for a column family |

### Memory Usage

| Property | Type | Description |
|----------|------|-------------|
| rocksdb.cur-size-active-mem-table | int | Active memtable size (bytes) |
| rocksdb.cur-size-all-mem-tables | int | Active + unflushed immutable memtables |
| rocksdb.size-all-mem-tables | int | Active + unflushed + pinned immutable memtables |
| rocksdb.estimate-table-readers-mem | int | Memory for SST table readers (excluding block cache) |
| rocksdb.block-cache-capacity | int | Block cache capacity |
| rocksdb.block-cache-usage | int | Current block cache memory usage |
| rocksdb.block-cache-pinned-usage | int | Pinned entries in block cache |
| rocksdb.block-cache-entry-stats | string/map | Block cache usage breakdown by entry type |

### File and Data Sizes

| Property | Type | Description |
|----------|------|-------------|
| rocksdb.total-sst-files-size | int | Total size of all SST files across all versions |
| rocksdb.live-sst-files-size | int | Total size of SST files in the current version |
| rocksdb.obsolete-sst-files-size | int | Size of obsolete SST files not yet deleted |
| rocksdb.estimate-live-data-size | int | Estimated live data size |
| rocksdb.estimate-pending-compaction-bytes | int | Estimated bytes needing compaction |
| rocksdb.num-blob-files | int | Number of blob files in current version |
| rocksdb.total-blob-file-size | int | Total blob file size across all versions |

### Operational Status

| Property | Type | Description |
|----------|------|-------------|
| rocksdb.num-running-compactions | int | Currently running compactions |
| rocksdb.num-running-flushes | int | Currently running flushes |
| rocksdb.compaction-pending | int | 1 if compaction is pending, 0 otherwise |
| rocksdb.mem-table-flush-pending | int | 1 if flush is pending, 0 otherwise |
| rocksdb.is-write-stopped | int | 1 if writes are stopped |
| rocksdb.actual-delayed-write-rate | int | Current write throttle rate (0 = no throttling) |
| rocksdb.background-errors | int | Accumulated background error count |

### Write Stall Stats

rocksdb.cf-write-stall-stats and rocksdb.db-write-stall-stats provide statistics on write stalls at the column family and database scope. Available as both string and map properties.

### Snapshots

| Property | Type | Description |
|----------|------|-------------|
| rocksdb.num-snapshots | int | Number of unreleased snapshots |
| rocksdb.oldest-snapshot-time | int | Unix timestamp of oldest unreleased snapshot |
| rocksdb.oldest-snapshot-sequence | int | Sequence number of oldest unreleased snapshot |

### Versioning

| Property | Type | Description |
|----------|------|-------------|
| rocksdb.num-live-versions | int | Live Version count (high values indicate pinned iterators) |
| rocksdb.current-super-version-number | int | Current LSM version number (resets on restart) |
| rocksdb.base-level | int | Target level for L0 compaction output |

## The "rocksdb.stats" Property

The rocksdb.stats property is the most commonly used debugging property. It combines column family stats and database stats into a multi-line report including:

- Per-level compaction stats: files, size, score, read/write bytes, write amplification, compaction time
- Cumulative and interval write statistics
- Cumulative and interval WAL statistics
- Stall statistics
- Block cache stats (if options.statistics is set)

This output is also automatically dumped to the LOG file every stats_dump_period_sec seconds (default: 600).

## Programmatic Access via ldb

The ldb get_property command provides CLI access to DB properties:

ldb --db=/path/to/db get_property rocksdb.stats
ldb --db=/path/to/db get_property rocksdb.estimate-num-keys
