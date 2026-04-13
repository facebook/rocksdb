# Configuration Flags

**Files:** `tools/db_bench_tool.cc`

## Database Sizing

| Flag | Default | Description |
|------|---------|-------------|
| `--num` | 1000000 | Number of keys in the key space |
| `--key_size` | 16 | Key size in bytes |
| `--value_size` | 100 | Value size in bytes (for fixed distribution) |
| `--value_size_distribution_type` | `fixed` | Value size distribution: `fixed`, `uniform`, `normal` |
| `--value_size_min` | 100 | Min value size (for uniform/normal distributions) |
| `--value_size_max` | 102400 | Max value size (for uniform/normal distributions) |
| `--compression_ratio` | 0.5 | Target compression ratio (0.5 = 50% size after compression) |
| `--num_column_families` | 1 | Number of column families |
| `--num_hot_column_families` | 0 | Number of actively-used CFs (0 = use all) |

## Concurrency

| Flag | Default | Description |
|------|---------|-------------|
| `--threads` | 1 | Number of concurrent benchmark threads |
| `--duration` | 0 | Run for N seconds (0 = run for `--num` ops). Used by many benchmarks including random writes, reads, mixed workloads, MixGraph, timeseries, and randomtransaction |
| `--benchmark_write_rate_limit` | 0 | Write rate limit in bytes/sec (0 = unlimited) |
| `--benchmark_read_rate_limit` | 0 | Read rate limit in ops/sec (0 = unlimited) |
| `--seed` | 0 | RNG seed base (0 = derive from current time) |
| `--enable_numa` | false | Bind threads and memory to NUMA nodes |

## Cache

| Flag | Default | Description |
|------|---------|-------------|
| `--cache_size` | 33554432 (32MB) | Block cache size in bytes |
| `--cache_numshardbits` | -1 | Cache shard count = 2^N (negative = library default) |
| `--cache_type` | `hyper_clock_cache` | Cache implementation type |
| `--cache_index_and_filter_blocks` | false | Cache index/filter blocks in block cache |
| `--cache_high_pri_pool_ratio` | 0.0 | Fraction of cache reserved for high-priority blocks |
| `--cache_low_pri_pool_ratio` | 0.0 | Fraction of cache reserved for low-priority blocks |
| `--row_cache_size` | 0 | Row cache size (0 = disabled) |
| `--compressed_cache_size` | -1 | Compressed block cache size (-1 = disabled) |

### Tiered Cache

| Flag | Default | Description |
|------|---------|-------------|
| `--use_compressed_secondary_cache` | false | Enable compressed secondary cache |
| `--compressed_secondary_cache_size` | 33554432 (32MB) | Secondary cache size |
| `--use_tiered_cache` | false | Distribute reservations proportionally across primary and secondary |
| `--tiered_adm_policy` | `auto` | Admission policy: `auto`, `placeholder`, `allow_cache_hits`, `three_queue` |

## Compression

| Flag | Default | Description |
|------|---------|-------------|
| `--compression_type` | `snappy` | Algorithm: `none`, `snappy`, `zlib`, `bzip2`, `lz4`, `lz4hc`, `xpress`, `zstd` |
| `--compression_level` | 32767 | Compression level (32767 = library default) |
| `--min_level_to_compress` | -1 | Apply compression from this level onwards (-1 = all levels) |
| `--compression_max_dict_bytes` | 0 | Max dictionary size for compression |
| `--compression_zstd_max_train_bytes` | 0 | Max training data for ZSTD dictionary |
| `--compression_parallel_threads` | 1 | Parallel compression threads per table builder |
| `--compression_manager` | `none` | Compression manager type: `none` (built-in), `mixed` (round-robin), `costpredictor` (cost-aware), `autoskip` (auto-skip) |

## Bloom Filter

| Flag | Default | Description |
|------|---------|-------------|
| `--bloom_bits` | -1 | Bits per key (-1 = preserve table factory default, 0 = disabled) |
| `--use_ribbon_filter` | false | Use Ribbon filter instead of Bloom |
| `--memtable_bloom_size_ratio` | 0 | Memtable bloom filter size as fraction of memtable (0 = disabled) |
| `--whole_key_filtering` | true | Use whole keys in SST bloom filter |
| `--optimize_filters_for_hits` | false | Skip bloom filter on bottommost level |

## Block Format

| Flag | Default | Description |
|------|---------|-------------|
| `--block_size` | 4096 | Data block size in bytes |
| `--block_restart_interval` | 16 | Keys between restart points for delta encoding |
| `--index_block_restart_interval` | 1 | Restart interval for index blocks |
| `--format_version` | 7 | SST format version |
| `--enable_index_compression` | true | Compress index blocks |
| `--block_align` | false | Align data blocks on page boundaries |

## Write Options

| Flag | Default | Description |
|------|---------|-------------|
| `--sync` | false | fsync after each write |
| `--disable_wal` | false | Skip WAL (faster but not durable) |
| `--batch_size` | 1 | Operations per WriteBatch |
| `--manual_wal_flush` | false | Buffer WAL until full or manual flush |
| `--wal_compression` | `none` | WAL compression algorithm |

## Compaction

| Flag | Default | Description |
|------|---------|-------------|
| `--disable_auto_compactions` | false | Disable background compaction |
| `--max_background_jobs` | (library default) | Max concurrent background jobs |
| `--max_background_compactions` | (library default) | Max concurrent compactions |
| `--compaction_style` | 0 (level) | 0=level, 1=universal, 2=FIFO |
| `--level0_file_num_compaction_trigger` | 4 | L0 file count to trigger compaction |
| `--level_compaction_dynamic_level_bytes` | false | Dynamic level size calculation. Note: db_bench defaults this to `false`, unlike the library default of `true`. Set `--level_compaction_dynamic_level_bytes=true` to match production defaults |
| `--max_bytes_for_level_base` | (library default) | Max bytes for level 1 |
| `--subcompactions` | 1 | Max sub-compactions per compaction job |

## Reporting and Monitoring

| Flag | Default | Description |
|------|---------|-------------|
| `--histogram` | false | Print per-operation-type latency histograms |
| `--statistics` | false | Enable DB-wide statistics counters |
| `--stats_level` | kExceptDetailedTimers | Statistics detail level |
| `--stats_interval` | 0 | Report interval in operations (0 = growing intervals) |
| `--stats_interval_seconds` | 0 | Report interval in seconds (overrides `stats_interval`) |
| `--stats_per_interval` | 0 | Print DB stats at each interval when > 0 |
| `--perf_level` | kDisable (1) | PerfContext collection level (1=disabled, 6=all timers) |
| `--report_interval_seconds` | 0 | CSV report interval (0 = disabled) |
| `--report_file` | `report.csv` | CSV output file path |
| `--confidence_interval_only` | false | Print only CI bounds (not full stats) for multi-run |

## I/O Options

| Flag | Default | Description |
|------|---------|-------------|
| `--use_direct_reads` | false | O_DIRECT for reads (bypasses OS page cache) |
| `--use_direct_io_for_flush_and_compaction` | false | O_DIRECT for background writes |
| `--mmap_read` | false | Allow mmap reads |
| `--mmap_write` | false | Allow mmap writes |
| `--rate_limiter_bytes_per_sec` | 0 | I/O rate limiter (0 = disabled) |
| `--bytes_per_sync` | 0 | Incremental sync interval for SST files |

## Advanced Options

| Flag | Default | Description |
|------|---------|-------------|
| `--use_existing_db` | false | Reuse existing DB instead of creating fresh |
| `--use_existing_keys` | false | Load existing keys into memory for read benchmarks |
| `--num_multi_db` | 0 | Number of parallel DB instances (0 = single DB) |
| `--options_file` | `""` | Load options from RocksDB options file |
| `--readonly` | false | Open DB in read-only mode |
| `--user_timestamp_size` | 0 | User-defined timestamp size in bytes (only 8 supported) |
| `--ops_between_duration_checks` | 1000 | Frequency of time-based duration checks |
