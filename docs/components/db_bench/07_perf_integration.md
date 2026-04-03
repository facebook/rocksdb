# PerfContext and Statistics Integration

**Files:** `tools/db_bench_tool.cc`, `include/rocksdb/perf_context.h`, `include/rocksdb/iostats_context.h`, `monitoring/statistics_impl.h`

## PerfContext

PerfContext provides per-thread performance counters for individual DB operations. Enabled via `--perf_level`.

### PerfLevel Options

| Level | Value | Overhead | Metrics Collected |
|-------|-------|----------|-------------------|
| `kUninitialized` | 0 | N/A | Unknown setting |
| `kDisable` | 1 | None | No metrics (default) |
| `kEnableCount` | 2 | Low | Operation counts only (no timers) |
| `kEnableWait` | 3 | Low-Medium | Wait/delay time metrics |
| `kEnableTimeExceptForMutex` | 4 | Medium | Time metrics except mutex waits |
| `kEnableTimeAndCPUTimeExceptForMutex` | 5 | Medium-High | Time and CPU time except mutex |
| `kEnableTime` | 6 | High | All time metrics including mutex waits |

### Integration with db_bench

`db_bench` sets the perf level via `SetPerfLevel()` at thread startup. After each benchmark completes, if `--perf_level > kDisable`, it appends the PerfContext string to the stats message via `get_perf_context()->ToString()`.

Key PerfContext counters for benchmarking:

| Counter | Interpretation |
|---------|----------------|
| `user_key_comparison_count` | Number of user key comparisons (indicates search efficiency) |
| `block_cache_hit_count` | Block cache hits (compute hit rate with `block_read_count`) |
| `block_read_count` | Blocks read from storage |
| `block_read_time` | Total time reading blocks from storage (nanoseconds) |
| `get_from_memtable_time` | Time spent in memtable lookups |
| `bloom_sst_hit_count` | SST bloom filter hits (positive results) |
| `get_snapshot_time` | Time acquiring snapshots |
| `write_wal_time` | Time writing to WAL |
| `write_memtable_time` | Time writing to memtable |

### Useful Derived Metrics

- **Cache hit rate**: `block_cache_hit_count / (block_cache_hit_count + block_read_count)`
- **I/O time fraction**: `block_read_time / total_operation_time`
- **Memtable vs storage ratio**: `get_from_memtable_time / (get_from_memtable_time + block_read_time)`

## IOStatsContext

IOStatsContext provides thread-local I/O statistics (see `IOStatsContext` in `include/rocksdb/iostats_context.h`):

| Counter | Description |
|---------|-------------|
| `bytes_written` | Total bytes written |
| `bytes_read` | Total bytes read |
| `write_nanos` | Time in `write()`/`pwrite()` syscalls |
| `read_nanos` | Time in `read()`/`pread()` syscalls |
| `fsync_nanos` | Time in `fsync()` syscalls |

Note: `db_bench` does not print `IOStatsContext` directly in its output. To examine I/O stats, use `get_iostats_context()` programmatically or add custom reporting in the benchmark method.

### Useful Derived Metrics

- **Write bandwidth**: `bytes_written / (write_nanos / 1e9)` bytes/sec
- **Read bandwidth**: `bytes_read / (read_nanos / 1e9)` bytes/sec
- **Fsync overhead**: `fsync_nanos / (write_nanos + fsync_nanos)` as fraction of write time

## Statistics (DB-Wide Counters)

Enabled by `--statistics=1`. Provides aggregated counters across all threads and operations.

### Enabling in db_bench

```bash
./db_bench --benchmarks=fillrandom,stats --statistics=1
```

The `stats` benchmark prints the full statistics output. Alternatively, `--stats_per_interval > 0` prints DB stats at each reporting interval.

### Key Statistics for Benchmarking

| Statistic | Description |
|-----------|-------------|
| `rocksdb.block.cache.miss` | Block cache misses |
| `rocksdb.block.cache.hit` | Block cache hits |
| `rocksdb.bloom.filter.useful` | Read requests avoided by bloom filters (negative results) |
| `rocksdb.bloom.filter.full.positive` | All bloom filter positive results (true + false) |
| `rocksdb.bloom.filter.full.true.positive` | True positive bloom results |
| `rocksdb.number.keys.written` | Keys written to DB |
| `rocksdb.bytes.written` | Bytes written to DB |
| `rocksdb.compaction.times.micros` | Time spent in compaction |
| `rocksdb.write.wal.time` | Histogram of WAL write times |

### Bloom Filter Analysis

From statistics:
- **False positive count**: `full.positive - full.true.positive`
- **False positive rate**: `(full.positive - full.true.positive) / full.positive`
- **Filter efficiency**: `bloom.filter.useful / (bloom.filter.useful + full.positive)` (fraction of lookups avoided)

### Stats Level

Controlled by `--stats_level` (see `StatsLevel` in `include/rocksdb/statistics.h`):

| Level | Description |
|-------|-------------|
| `kExceptHistogramOrTimers` | Only ticker stats, no histograms or timers |
| `kExceptTimers` | Ticker stats and histograms, no timers |
| `kExceptDetailedTimers` | Everything except detailed per-operation timers (default) |
| `kExceptTimeForMutex` | Everything except mutex wait times |
| `kAll` | All statistics |

## Interval Stats with DB Properties

When `--stats_per_interval > 0` and reporting triggers (via `--stats_interval` or `--stats_interval_seconds`), thread 0 prints additional DB properties. The output differs between single-CF and multi-CF modes:

**Single-CF mode**: Prints `rocksdb.stats`, `rocksdb.num-running-compactions`, `rocksdb.num-running-flushes`, and optional per-level table properties when `--show_table_properties=true`.

**Multi-CF mode**: Prints `rocksdb.cfstats` for each created column family, plus optional per-level table properties. Does not print the running compaction/flush counts.

This provides real-time visibility into LSM state during long benchmarks.
