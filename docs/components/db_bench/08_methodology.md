# Benchmarking Methodology

**Files:** `tools/db_bench_tool.cc`

## Baseline vs Experimental Comparison

### Protocol

Step 1: Build baseline binary from the reference branch:
```bash
make clean && DEBUG_LEVEL=0 make -j128 db_bench
```

Step 2: Run baseline benchmark with the built-in repeat syntax:
```bash
./db_bench --benchmarks=fillrandom[X5] --num=10000000
```

Step 3: Rebuild with experimental changes:
```bash
make clean && DEBUG_LEVEL=0 make -j128 db_bench
```

Step 4: Run the same benchmark with identical flags.

Step 5: Compare the `[AVG N runs]` output. If the 95% confidence intervals do not overlap, the difference is statistically significant.

### Statistical Significance

- **Minimum 5 runs** for each configuration to produce meaningful confidence intervals
- `CombinedStats` uses Bessel's correction (`n-1` denominator) for sample variance
- 95% CI half-width = `1.96 * stddev / sqrt(n)`
- Non-overlapping CIs strongly suggest a real performance difference

### Coefficient of Variation

`CV = stddev / mean` indicates measurement stability:

| CV | Interpretation |
|----|---------------|
| < 5% | Low variance; reliable measurements |
| 5-10% | Moderate variance; acceptable for most comparisons |
| > 10% | High variance; investigate noise sources before drawing conclusions |

## Controlling Variables

| Variable | Control Method |
|----------|----------------|
| CPU frequency scaling | Disable turbo boost: `echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo` |
| OS page cache | Clear before runs: `sync; echo 3 > /proc/sys/vm/drop_caches` |
| Compaction interference | `--disable_auto_compactions=1` for write-only benchmarks |
| Block cache state | Run `readtocache` or warmup iterations (`[WN]` syntax) |
| I/O scheduling | Use `--use_direct_reads=1` to bypass OS page cache entirely |
| Background I/O | Pin I/O-intensive processes to separate cores |

## Common Pitfalls

### 1. Warm Cache Assumption

Read benchmarks assume a warm block cache. Without warming, the first run measures cold-cache I/O, which can be 10x+ slower than steady-state.

**Solution**: Use `--benchmarks=readtocache,readrandom` or `readrandom[W1-X5]`.

### 2. Compaction Interference

Background compaction during write benchmarks causes throughput drops and high variance. Visible in interval reporting as periodic throughput halving.

**Solution**:
- `--disable_auto_compactions=1` for write-only measurements
- Or run `compact` first to stabilize LSM shape before mixed workloads
- Or use `--stats_interval_seconds=1` to monitor compaction triggers

### 3. OS Page Cache Pollution

Even with `--mmap_read=0`, the OS page cache can serve SST reads, making disk I/O appear faster than production.

**Solution**: `--use_direct_reads=1` is the only reliable method to bypass the OS page cache. `drop_caches` is a weaker alternative that must be repeated between runs.

### 4. Insufficient Key Space for Multi-Threaded Benchmarks

With `--threads=16` and `--num=1000000`, each thread operates over the full key space but finishes quickly (62.5K ops per thread). Startup overhead dominates.

**Solution**: Scale `--num` proportionally: `--num=$((1000000 * threads))`. Alternatively, use `--duration=30` to run for a fixed time period regardless of operation count.

### 5. Debug Build Performance

Debug builds include assertions (`NDEBUG` not defined) and skip compiler optimizations. Performance measurements are 5-10x slower with high variance.

**Detection**: `db_bench` prints warnings at startup:
```
WARNING: Assertions are enabled; benchmarks unnecessarily slow
WARNING: Optimization is disabled: benchmarks unnecessarily slow
```

**Solution**: Always `make clean && DEBUG_LEVEL=0 make -j128 db_bench`.

### 6. Mismatched --num Between Write and Read Phases

Running a read benchmark with `--use_existing_db=1` but a different `--num` than the write phase causes most lookups to miss.

**Detection**: Check the `(found of total found)` output. If found << total, `--num` is mismatched.

**Solution**: Use the same `--num` for both phases, or use `--use_existing_keys=1` to discover existing keys at startup.

### 7. Single-Run Comparisons

A single benchmark run is not sufficient for performance claims. Variance from OS scheduler, CPU frequency, page cache, and compaction timing can produce misleading results.

**Solution**: Always use `[X5]` or more repetitions and report confidence intervals.

### 8. DB State Reuse Across Warmups and Repeats

When using `[W2-X5]` or `[X5]` syntax, `fresh_db` destruction/reopen happens once before the warmup/repeat loop, not before each iteration. Repeated write benchmarks accumulate data across iterations (they do not get a clean DB). Additionally, per-thread RNG seeds change between runs because `total_thread_count_` keeps incrementing, so even read benchmarks do not replay identical random sequences.

**Implication**: Repeat statistics for write benchmarks are not sampling identical starting conditions. This is expected but should be understood when interpreting variance.

### 9. Ignoring Compression Interaction

Default `--compression_type=snappy` adds CPU overhead. When benchmarking features unrelated to compression, consider `--compression_type=none` to isolate the effect.

Note: `db_bench` checks at startup whether the configured compression algorithm is available and effective. If not, it prints a warning.

### 10. Database State (Fragmentation and Memtable)

The state of the database significantly affects read benchmark results. A fragmented database (many overlapping files, tombstones) with a non-empty memtable can show 4X lower read QPS than a freshly-compacted database with an empty memtable.

**Solution**: Run `compact` and `waitforcompaction` before read benchmarks, or ensure consistent DB state between baseline and experimental runs.

### 11. Write Scaling Without Fsync

RocksDB does not scale for concurrent `Put()` without `sync=true`. With `sync=false`, peak write throughput is typically at 1 thread because the write-ahead log serializes writes. With `sync=true`, multiple threads achieve speedup by overlapping fsync waits (group commit).

**Implication**: Do not expect write throughput to increase linearly with `--threads` for `fillrandom` or `overwrite` benchmarks with `--sync=0`.

## Benchmark Selection Guide

| Goal | Benchmark | Key Flags |
|------|-----------|-----------|
| Write throughput (sequential) | `fillseq` | `--num`, `--value_size`, `--disable_wal` |
| Write throughput (random) | `fillrandom` | `--num`, `--threads`, `--compression_type` |
| Read latency (point lookup) | `readrandom` | `--reads`, `--cache_size`, `--bloom_bits` |
| Read throughput (scan) | `readseq` | `--num`, `--threads` |
| Mixed workload | `readrandomwriterandom` | `--readwritepercent`, `--threads` |
| Production-like workload | `mixgraph` | Key distribution params, `--mix_get_ratio`, `--mix_put_ratio` |
| Compaction throughput | `compact` | `--num`, `--max_background_compactions` |
| Bloom filter effectiveness | `readrandom` + `--statistics=1` | `--bloom_bits` |
| Iterator performance | `seekrandom` | `--seek_nexts`, `--num` |
| Batched reads | `multireadrandom` | `--batch_size`, `--multiread_batched` |
| Durable writes | `fillsync` | `--sync=1`, `--wal_dir` |
| Write amplification | `fillrandom,stats` | Examine compaction stats in `stats` output |
