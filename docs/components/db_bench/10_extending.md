# Building, Extending, and Best Practices

**Files:** `tools/db_bench_tool.cc`, `tools/db_bench.cc`

## Build Modes

| Command | Use Case |
|---------|----------|
| `make clean && DEBUG_LEVEL=0 make -j128 db_bench` | Release build for performance benchmarking. Required for accurate measurements |
| `make -j128 db_bench` | Debug build. Includes assertions, no optimizations. 5-10x slower than release |

Important: Always run `make clean` when switching between debug and release builds. Mixing object files from different build modes produces incorrect binaries.

### Detecting Debug Builds

`db_bench` prints warnings at startup if running in debug mode:

```
WARNING: Assertions are enabled; benchmarks unnecessarily slow
WARNING: Optimization is disabled: benchmarks unnecessarily slow
```

If either warning appears, rebuild with `DEBUG_LEVEL=0`.

## Adding a New Benchmark

### Step 1: Add the Method

Add a new method to the `Benchmark` class in `tools/db_bench_tool.cc`:

- Accept `ThreadState* thread` as the only parameter
- Use `Duration` for termination control
- Call `FinishedOps()` after each operation for latency recording
- Call `AddBytes()` for throughput tracking
- Use `AllocateKey()` and `GenerateKeyFromInt()` for key management

### Step 2: Register in Run()

Add an `else if` clause in `Benchmark::Run()` mapping the benchmark name to the method pointer:

```
} else if (name == "mybenchmark") {
  method = &Benchmark::MyBenchmark;
```

Set `fresh_db = true` if the benchmark needs a clean DB. Adjust `num_threads` if the benchmark uses additional background threads.

### Step 3: Add Help Text

Add a description line to the `--benchmarks` flag help string.

### Step 4: Rebuild

```bash
make clean && DEBUG_LEVEL=0 make -j128 db_bench
```

## Multi-DB Mode

`--num_multi_db=N` creates N independent DB instances. Benchmarks select a DB per operation via `SelectDBWithCfh()`, which uses `thread->rand.Next() % N` (random selection, not round-robin). Exception: `DoWrite()` has its own sequential per-DB progression logic for sequential write benchmarks. Useful for:

- Measuring performance with multiple RocksDB instances sharing the same process
- Testing cache contention across instances
- Simulating multi-shard deployments

Each DB is opened in a subdirectory of `--db` path.

## NUMA Support

`--enable_numa=true` binds each thread to a NUMA node in round-robin order. Requires the `libnuma` library (compile with `-DNUMA`).

Thread `i` is bound to NUMA node `i % num_nodes`. Memory allocation is local to the bound node. This is particularly beneficial for:
- Large block cache sizes spread across NUMA nodes
- Reducing cross-node memory access latency
- Multi-socket benchmarking

## Tracing

### Operation Tracing

`--trace_file=path` records all DB operations to a trace file during the benchmark. The trace can be replayed later with `--benchmarks=replay --trace_file=path`.

Controlled by:
- `--trace_replay_fast_forward`: Speed multiplier for replay (must be > 0.0)
- `--trace_replay_threads`: Number of replay threads

### Block Cache Tracing

`--block_cache_trace_file=path` records block cache accesses during the benchmark. Useful for analyzing cache access patterns and hit rates.

Controlled by:
- `--block_cache_trace_sampling_frequency`: Spatial downsampling (1 = no sampling)
- `--block_cache_trace_max_trace_file_size_in_bytes`: Max trace file size (default 64GB)

## Options File Loading

`--options_file=path` loads RocksDB options from a serialized options file instead of command-line flags. When set, most RocksDB option flags are ignored. Only these flags are still honored:

- `--use_existing_db`, `--use_existing_keys`
- `--statistics`
- `--row_cache_size`
- `--enable_io_prio`
- `--dump_malloc_stats`
- `--num_multi_db`

## Simulated Storage

### Hybrid File System

`--simulate_hybrid_fs_file=path` enables simulated tiered storage where cold data (at `kWarm` temperature) experiences higher latency. The multiplier is controlled by `--simulate_hybrid_hdd_multipliers`.

### HDD Simulation

`--simulate_hdd=true` adds artificial read/write latency to simulate HDD-class storage.

## Secondary and Follower Modes

| Flag | Mode |
|------|------|
| `--use_secondary_db` | Opens as secondary instance with periodic catchup (`--secondary_update_interval` seconds) |
| `--open_as_follower` | Opens as follower of a leader DB at `--leader_path` |

## Best Practices Summary

1. **Always use release builds** for performance measurements
2. **Run multiple iterations** with `[X5]` or more and report confidence intervals
3. **Warm the cache** with `readtocache` or `[WN]` warmup syntax before read benchmarks
4. **Control variables**: disable turbo boost, clear page cache, disable auto-compaction for write benchmarks
5. **Use direct I/O** (`--use_direct_reads=1`) to bypass OS page cache
6. **Match --num** between write and read phases when using `--use_existing_db`
7. **Monitor intervals** with `--stats_interval_seconds=1` for long benchmarks to detect anomalies
8. **Enable statistics** (`--statistics=1`) and PerfContext (`--perf_level=2`) for diagnostic benchmarks
9. **Isolate features**: use `--compression_type=none` when benchmarking non-compression features
10. **Scale --num** with thread count for multi-threaded benchmarks to avoid premature termination

## benchmark.sh Driver Script

The script `tools/benchmark.sh` provides standardized benchmark workflows. It supports subcommands like `bulkload`, `fillseq_disable_wal`, `overwrite`, `readrandom`, `readwhilewriting`, `fwdrange`, `revrange`, and `multireadrandom`.

Key environment variables:

| Variable | Description |
|----------|-------------|
| `NUM_KEYS` | Number of keys |
| `NUM_THREADS` | Concurrent threads |
| `CACHE_SIZE` | Block cache size in bytes |
| `DURATION` | Test duration in seconds |
| `MB_WRITE_PER_SEC` | Write rate limit for read-while-writing tests |
| `DB_DIR` | Database directory |
| `WAL_DIR` | WAL directory |
| `OUTPUT_DIR` | Output directory for results |

A companion script `tools/benchmark_compare.sh` helps compare results across runs.

## Bulk Load Strategy

For maximum bulk load throughput, use a two-pass approach:

Step 1: Fill L0 with compaction disabled and an unsorted vector memtable:
```bash
./db_bench --benchmarks=fillrandom --disable_auto_compactions=1 \
  --memtablerep=vector --allow_concurrent_memtable_write=false \
  --disable_wal=1 --num=1000000000
```

Step 2: Compact all data into sorted files:
```bash
./db_bench --benchmarks=compact --use_existing_db=1
```

This separates the ingestion phase (I/O-bound) from the sorting phase (CPU-bound), achieving significantly higher throughput than a single-pass approach with compaction enabled.

## Direct I/O Considerations

Direct I/O (`--use_direct_reads=1`) bypasses the OS page cache. Performance impact varies by workload:

- **Point reads**: Direct I/O is typically faster because it avoids page cache contention and double-buffering
- **Range scans**: Direct I/O can be slower because the OS page cache provides effective readahead that direct I/O bypasses
- **Write benchmarks**: `--use_direct_io_for_flush_and_compaction=1` can reduce page cache pollution from background writes

## Statistics and Measurement Overhead

Enabling statistics and PerfContext adds measurement overhead. For maximum-throughput benchmarks (especially in-memory workloads):

- Set `--statistics=0` (or omit `--statistics`)
- Set `--perf_level=1` (kDisable)
- These are appropriate for throughput measurement but should be enabled for diagnostic benchmarks
