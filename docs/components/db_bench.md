# db_bench: RocksDB Benchmarking Tool

## Overview

`db_bench` is RocksDB's primary benchmarking tool for measuring database performance across various workloads. It provides standardized benchmarks for write throughput, read latency, compaction behavior, and overall system performance.

**Purpose:**
- Measure ops/sec, latency percentiles, and throughput (MB/s)
- Compare performance across different configurations
- Validate optimizations and detect regressions
- Stress-test specific features (bloom filters, compression, compaction)

**Key Source Files:**
- `tools/db_bench_tool.cc` (main benchmark logic, ~9360 lines)
- `include/rocksdb/perf_context.h` (per-operation performance counters)
- `include/rocksdb/iostats_context.h` (I/O statistics)
- `monitoring/statistics_impl.h` (aggregated DB statistics)

**Building:**
```bash
make -j128 db_bench              # Debug build
DEBUG_LEVEL=0 make db_bench      # Release build (required for accurate benchmarks)
```

⚠️ **INVARIANT:** Always use release builds (`DEBUG_LEVEL=0`) for performance measurements. Debug builds have assertions enabled and lack compiler optimizations, skewing results by 5-10x.

---

## Architecture

### Benchmark Execution Flow

```
┌──────────────────────────────────────────────────────────┐
│ Benchmark::Run()                                         │
│  ├─ Parse benchmarks from FLAGS_benchmarks              │
│  ├─ Open DB with configured options                     │
│  ├─ For each benchmark:                                 │
│  │   ├─ Select method pointer (e.g., &ReadRandom)      │
│  │   ├─ Determine num_threads                          │
│  │   ├─ Launch ThreadState workers                     │
│  │   └─ Collect Stats from all threads                 │
│  └─ Report aggregated statistics                        │
└──────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────┐
│ ThreadState (per-thread execution context)              │
│  ├─ Random number generator (thread-safe seed)          │
│  ├─ Stats (latency histograms, throughput counters)     │
│  └─ Shared state (rate limiters, barriers)              │
└──────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────┐
│ Benchmark Method (e.g., ReadRandom, DoWrite)            │
│  ├─ Loop until duration expires or ops exhausted        │
│  ├─ Generate key (sequential, random, unique_random)    │
│  ├─ Execute DB operation (Get, Put, Seek, etc.)         │
│  ├─ Record latency in histogram (FinishedOps)           │
│  └─ Update byte counters (AddBytes)                     │
└──────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────┐
│ Stats::Report()                                          │
│  ├─ Calculate throughput: ops / elapsed_time            │
│  ├─ Print: micros/op, ops/sec, MB/s                     │
│  ├─ Print latency histogram (if --histogram=1)          │
│  └─ Print perf_context (if --perf_level > kDisable)     │
└──────────────────────────────────────────────────────────┘
```

### Key Classes

| Class | Responsibility |
|-------|---------------|
| `Benchmark` | Main orchestrator. Parses flags, opens DB, dispatches benchmark methods |
| `ThreadState` | Per-thread execution context. Holds RNG, stats, shared state |
| `Stats` | Per-thread statistics. Tracks latency histograms, throughput, bytes processed |
| `CombinedStats` | Aggregates stats from multiple runs. Computes mean, median, confidence intervals |
| `Duration` | Time-based or operation-count-based termination condition |
| `RandomGenerator` | Generates compressible values with controlled compression ratio |
| `KeyGenerator` | Generates keys in SEQUENTIAL, RANDOM, or UNIQUE_RANDOM order |

---

## Benchmark Types

### Write Benchmarks

| Benchmark | Write Pattern | Use Case |
|-----------|---------------|----------|
| `fillseq` | Sequential keys | Measure write throughput with minimal overhead. Best case for LSM (no sorting in memtable) |
| `fillrandom` | Random keys | Realistic write workload. Measures memtable sorting and compaction overhead |
| `filluniquerandom` | Random unique keys (single-threaded) | Write without overwrites. Used with `--writes_per_range_tombstone` for DeleteRange testing |
| `fillseqdeterministic` | Deterministic LSM shape | Fill all levels with known structure. Requires `--disable_auto_compactions=1` |
| `overwrite` | Random keys (overwrites existing) | Measure update performance on hot keys |
| `fillsync` | Random keys with `sync=true` | Measure durable write latency (WAL fsync per operation) |
| `fill100K` | Random keys, 100KB values | Measure large-value write performance |

**Write Mode Enum (internal):**
```cpp
enum WriteMode {
  RANDOM,           // Keys: rand(), rand(), ...
  SEQUENTIAL,       // Keys: 0, 1, 2, 3, ...
  UNIQUE_RANDOM     // Keys: shuffle([0..num-1]), no duplicates
};
```

### Read Benchmarks

| Benchmark | Access Pattern | Use Case |
|-----------|---------------|----------|
| `readseq` | Sequential iterator scan | Measure scan throughput. Tests iterator, prefetching, block cache |
| `readreverse` | Reverse iterator scan | Measure backward scan performance (Prev() calls) |
| `readrandom` | Random point lookups | Measure Get() latency. Tests bloom filters, block cache hit rate |
| `readrandomfast` | Optimized random reads | Skips some overhead for microbenchmarking |
| `multireadrandom` | Batched random reads | Measure MultiGet() performance. Tests batch optimizations |
| `readwhilewriting` | N readers + 1 writer | Concurrent workload. Tests read performance under write load |
| `readwhilemerging` | N readers + 1 merger | Tests read performance during merge operations |
| `seekrandom` | Random seeks + N nexts | Measure iterator seek cost. Controlled via `--seek_nexts` |

**Read Execution (from `ReadRandom`):**
```cpp
void ReadRandom(ThreadState* thread) {
  while (!duration.Done(1)) {
    key_rand = GetRandomKey(&thread->rand);
    GenerateKeyFromInt(key_rand, FLAGS_num, &key);

    Status s = db->Get(options, cfh, key, &pinnable_val);
    if (s.ok()) {
      found++;
      bytes += key.size() + pinnable_val.size();
    }
    thread->stats.FinishedOps(..., kRead);  // Record latency
  }
}
```

### Mixed Benchmarks

| Benchmark | Workload | Use Case |
|-----------|----------|----------|
| `readrandomwriterandom` | Configurable read/write mix (default 90% reads / 10% writes via `--readwritepercent`) | Mixed read/write workload. Total iterations = `max(reads, writes)` |
| `updaterandom` | Read-modify-write | Measure RMW performance |
| `xorupdaterandom` | Read-XOR-write via Get+Put | Tests read-modify-write with XOR (uses `BytesXOROperator` in-process, not DB merge) |
| `readrandommergerandom` | Random Get or Merge (default 70% merges via `--mergereadpercent`) | Tests mixed read/merge workload. Requires `--merge_operator` |

### Compaction Benchmarks

| Benchmark | Operation | Use Case |
|-----------|-----------|----------|
| `compact` | `CompactRange` on selected DB (default column family) | Measure full compaction throughput |
| `compactall` | `CompactRange` on all DB instances (default column family each) | Multi-DB compaction test |
| `compact0` | Compact from L0 to next populated level | Measure L0 compaction cost (adapts to dynamic leveling) |
| `compact1` | Compact from first populated level above L0 to next | Measure upper-level compaction cost (adapts to dynamic leveling) |
| `waitforcompaction` | Block until compaction finishes | Used in multi-phase benchmarks |

### Utility Benchmarks

| Benchmark | Purpose |
|-----------|---------|
| `stats` | Print DB statistics (`rocksdb.stats`) |
| `sstables` | Print SST file metadata |
| `levelstats` | Print per-level file counts and sizes |
| `approximatememtablestats` | Test `GetApproximateMemTableStats()` accuracy |

---

## Key Configuration Flags

### Database Size

| Flag | Default | Description |
|------|---------|-------------|
| `--num` | 1000000 | Number of keys to operate on |
| `--key_size` | 16 | Key size in bytes |
| `--value_size` | 100 | Value size in bytes (fixed distribution) |
| `--value_size_distribution_type` | `fixed` | Value size distribution: `fixed`, `uniform`, `normal` |
| `--compression_ratio` | 0.5 | Target compression ratio (0.5 = 50% size after compression) |

**Key Generation (`GenerateKeyFromInt`):**
```
If --keys_per_prefix > 0:
  ┌──────────────────────┬──────────────────────┐
  │ prefix (prefix_size) │ key number + padding  │
  └──────────────────────┴──────────────────────┘

If --keys_per_prefix == 0 (default):
  ┌──────────────────────────────────────────────┐
  │          key number + zero padding            │
  └──────────────────────────────────────────────┘

Note: User timestamps are NOT appended to keys. They are passed
separately via timestamp-aware DB APIs. Only 64-bit (8-byte)
user timestamps are supported.
```

### Concurrency

| Flag | Default | Description |
|------|---------|-------------|
| `--threads` | 1 | Number of concurrent threads. Note: some composite benchmarks (e.g., `readwhilewriting`, `readwhilemerging`) internally add +1 background thread |
| `--duration` | 0 | Run for N seconds (0 = run for --num ops) |
| `--benchmark_write_rate_limit` | 0 | Write rate limit in bytes/sec (0 = unlimited) |
| `--benchmark_read_rate_limit` | 0 | Read rate limit in ops/sec |

### Cache

| Flag | Default | Description |
|------|---------|-------------|
| `--cache_size` | 32MB | Block cache size (shared across threads) |
| `--cache_numshardbits` | -1 | Number of cache shards as 2^N (negative = use default) |
| `--cache_index_and_filter_blocks` | false | Cache index/filter blocks in block cache |

⚠️ **INVARIANT:** Cache must be warm for read benchmarks. Run `readtocache` first or results will reflect cold-cache performance.

### Compression

| Flag | Default | Description |
|------|---------|-------------|
| `--compression_type` | `snappy` | Options: `none`, `snappy`, `zlib`, `bzip2`, `lz4`, `lz4hc`, `xpress`, `zstd` |
| `--compression_level` | `kDefaultCompressionLevel` (32767) | Compression level (codec-specific, 32767 = use library default) |
| `--min_level_to_compress` | -1 | Apply compression from this level onwards |

### Block Format

| Flag | Default | Description |
|------|---------|-------------|
| `--block_size` | 4096 | Data block size in bytes |
| `--block_restart_interval` | 16 | Number of keys between restart points |
| `--index_block_restart_interval` | 1 | Restart interval for index blocks |

### Bloom Filter

| Flag | Default | Description |
|------|---------|-------------|
| `--bloom_bits` | -1 | Bits per key for bloom filter (negative = preserve table-factory default, 0 = disabled, positive = set explicitly) |
| `--use_ribbon_filter` | false | Use Ribbon filter (lower memory) instead of Bloom |

**False Positive Rate vs Bits Per Key:**
| Bits/Key | FPR | Use Case |
|----------|-----|----------|
| 5 | ~6% | Low memory, high FPR acceptable |
| 10 | ~1% | Standard choice for point lookups |
| 15 | ~0.1% | Lookup-heavy workloads |
| 20 | ~0.01% | Very low FPR required |

### Write Options

| Flag | Default | Description |
|------|---------|-------------|
| `--sync` | false | Call fsync after each write (durable writes) |
| `--disable_wal` | false | Skip WAL (faster but not durable) |
| `--batch_size` | 1 | Group N operations in a WriteBatch |

### Compaction

| Flag | Default | Description |
|------|---------|-------------|
| `--disable_auto_compactions` | false | Disable background compaction |
| `--max_background_compactions` | -1 | Max concurrent compaction threads (negative = use default) |
| `--level0_file_num_compaction_trigger` | 4 | Trigger L0→L1 compaction at N files |

---

## Performance Metrics

### Primary Metrics (from `Stats::Report`)

**Output Format:**
```
fillrandom   :      12.345 micros/op 81015 ops/sec 12.346 seconds 1000000 operations; 8.1 MB/s
  ^^              ^^                ^^            ^^             ^^                   ^^
  name            latency           throughput    duration       operation count      bandwidth
```

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| **micros/op** | `(finish - start) * 1e6 / done` | Average latency per operation |
| **ops/sec** | `done / elapsed_seconds` | Throughput in operations per second |
| **MB/s** | `(bytes / 1048576.0) / elapsed` | Data bandwidth (only if bytes tracked) |

⚠️ **INVARIANT:** `micros/op` is computed from per-thread `seconds_` (sum of thread times), while `ops/sec` uses wall-clock elapsed time. For multi-threaded benchmarks, `micros/op * ops/sec ≠ 1,000,000`.

### Latency Histograms (--histogram=1)

**Example Output:**
```
Microseconds per read:
Count: 1000000  Average: 1.2345  StdDev: 0.89
Min: 0  Median: 1.0234  Max: 156
Percentiles: P50: 1.02 P75: 1.45 P99: 4.23 P99.9: 12.45 P99.99: 45.67
```

**Key Percentiles:**
| Percentile | Meaning | Use Case |
|------------|---------|----------|
| P50 (median) | 50% of ops complete in ≤ this time | Typical latency |
| P99 | 99% of ops complete in ≤ this time | User-facing SLA target |
| P99.9 | Tail latency | Detect outliers, compaction pauses |
| P99.99 | Extreme tail | Debugging rare stalls |

**Implementation (from `Stats` class in `tools/db_bench_tool.cc`):**
```cpp
void FinishedOps(..., OperationType op_type) {
  uint64_t now = clock_->NowMicros();
  uint64_t micros = now - last_op_finish_;
  hist_[op_type]->Add(micros);  // Record latency
  last_op_finish_ = now;
}
```

### Multi-Run Statistics (--benchmarks=fillrandom[X3])

**Repeat Syntax:** `benchmark[X<N>]` runs benchmark N times
**Warmup Syntax:** `benchmark[W<N>]` runs N warmup iterations (not reported)

**CombinedStats Output:**
```
fillrandom [AVG    3 runs] : 81015 (± 1234) ops/sec; 8.1 (± 0.2) MB/sec
fillrandom [MEDIAN 3 runs] : 81200 ops/sec; 8.2 MB/sec
```

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| AVG | `mean(throughput_ops_)` | Average throughput across runs |
| ± value | `1.96 * stddev / sqrt(n)` | 95% confidence interval |
| MEDIAN | `median(throughput_ops_)` | Robust to outliers |

---

## Benchmarking Specific Features

### Bloom Filters

**Goal:** Measure bloom filter effectiveness (negatives avoided, false positives)

**Setup:**
```bash
# Write 10M keys
./db_bench --benchmarks=fillrandom --num=10000000 --bloom_bits=10

# Read with bloom filter (measure negative lookups avoided)
./db_bench --benchmarks=readrandom --use_existing_db=1 --num=10000000 \
           --reads=10000000 --bloom_bits=10 --statistics=1

# Compare with no bloom filter
./db_bench --benchmarks=readrandom --use_existing_db=1 --num=10000000 \
           --reads=10000000 --bloom_bits=0 --statistics=1
```

**Key Statistics:**
```
rocksdb.bloom.filter.useful COUNT : 9900000   # Negatives avoided (filter returned false)
rocksdb.bloom.filter.full.positive COUNT : 100000   # All positive results (includes true and false positives)
rocksdb.bloom.filter.full.true.positive COUNT : 99000  # True positives (data actually exists)
```

**False Positive Count:** `full_positive - full_true_positive`
**False Positive Rate:** `(full_positive - full_true_positive) / full_positive`

### Compression

**Goal:** Measure compression ratio and impact on read/write performance

**Test Matrix:**
```bash
for comp in none snappy lz4 zstd; do
  ./db_bench --benchmarks=fillrandom,stats --compression_type=$comp \
             --num=1000000 --value_size=1000
done
```

**Metrics to Compare:**
| Metric | Source | Interpretation |
|--------|--------|----------------|
| Write throughput | `fillrandom` output | Compression CPU cost |
| Read throughput | `readrandom` output | Decompression CPU cost |
| Amplification | `stats` output → `Compression ratio` | Space savings |
| LSM size | `stats` output → `Sum files` | On-disk size |

**Expected Results:**
- `none`: Fastest writes, slowest reads (more I/O), largest size
- `lz4`: Balanced (fast compression/decompression)
- `zstd`: Slowest writes, best compression ratio

Note: `db_bench` defaults to `--compression_type=snappy` applied uniformly to all levels. Per-level compression (e.g., lz4 for L0-L1, zstd for L2+) must be configured separately via `--min_level_to_compress` or application-level `compression_per_level` settings.

### Compaction

**Goal:** Measure compaction throughput and write amplification

**Setup:**
```bash
# Disable auto-compaction during load
./db_bench --benchmarks=fillrandom --num=10000000 \
           --disable_auto_compactions=1 --level0_file_num_compaction_trigger=1000

# Manually trigger compaction and measure
./db_bench --benchmarks=compact --use_existing_db=1
# Note: --stats_interval_seconds does not provide per-second progress for
# compact, because Compact() does not call FinishedOps() during compaction.
```

**Key Metrics (from `stats` output):**
```
Compaction Stats:
  Level  Files  Size(MB) Time(sec)  Read(MB)  Write(MB)  Rn(MB)  Rnp1(MB)  Wnew(MB)  RW-Amp
  L0       100      500       10.5       0         500       0       0         500      1.0
  L1        50     1000       25.3     1500       1500     500    1000        500      3.0
                                      ^^^^^^      ^^^^^^                              ^^^^
                                      input       output                              amplification
```

**Write Amplification:** `(bytes_written_to_storage) / (bytes_written_by_user)`
- Ideal: 1.0 (no compaction)
- Typical: 10-30 (depends on LSM shape, size ratio)
- High (>50): Check level sizes, compaction style, or file size

---

## Comparison Methodology

### Baseline vs Experimental

**Goal:** Detect performance regressions or validate optimizations

**Protocol:**
1. **Establish baseline:** Run benchmark 5+ times on main branch, record mean ± CI
2. **Apply change:** Rebuild with experimental code
3. **Run experiment:** Same benchmark, same flags, 5+ runs
4. **Statistical test:** Check if confidence intervals overlap

**Example:**
```bash
# Baseline (main branch) — use built-in repeat syntax for aggregation
./db_bench --benchmarks=fillrandom[X5] --num=10000000
# Output includes:
# fillrandom [AVG    5 runs] : 81015 (± 523) ops/sec; ...
# fillrandom [MEDIAN 5 runs] : 81200 ops/sec; ...

# Experimental (feature branch)
# ... rebuild with new code ...
./db_bench --benchmarks=fillrandom[X5] --num=10000000
# Output: fillrandom [AVG 5 runs] : 85234 (± 612) ops/sec

# Conclusion: 85234 - 81015 = 4219 ops/sec improvement (5.2%)
# Non-overlapping CIs: [80492, 81538] vs [84622, 85846] → statistically significant

# NOTE: The [AVG N runs] output requires the repeat syntax (e.g., fillrandom[X5])
# within a single db_bench invocation. Running db_bench in a shell loop produces
# N independent reports without aggregation.
```

### Statistical Significance

⚠️ **INVARIANT:** Single-run comparisons are unreliable. Variance in benchmarks comes from:
- OS scheduler noise
- CPU frequency scaling
- Page cache state
- Background compaction timing

**Minimum Requirements:**
- **5+ runs** for each configuration
- **Report confidence intervals** (mean ± 1.96*stddev/sqrt(n) for 95% CI)
- **Check overlap:** No overlap = likely real difference

**Coefficient of Variation:** `stddev / mean`
- <5%: Low variance, reliable benchmark
- 5-10%: Moderate variance, acceptable
- >10%: High variance, investigate noise sources

### Controlling Variables

| Variable | Control Method |
|----------|----------------|
| CPU frequency | Disable turbo boost: `echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo` |
| Page cache | Clear before each run: `echo 3 > /proc/sys/vm/drop_caches` (requires root) |
| Compaction timing | Use `--disable_auto_compactions=1` for write benchmarks |
| File system state | Run on fresh SSD partition or re-format between runs |

---

## Common Benchmarking Pitfalls

### 1. Warm Cache Assumption

**Problem:** Read benchmarks assume block cache is warm, but first run has cold cache.

**Symptom:**
```
# First run (cold cache)
readrandom : 15.234 micros/op 65634 ops/sec

# Second run (warm cache)
readrandom : 1.234 micros/op 810345 ops/sec  ← 12x faster!
```

**Solution:** Run `readtocache` or `readrandom` once to warm cache, then re-run benchmark.

### 2. Compaction Interference

**Problem:** Background compaction triggers during write benchmark, skewing results.

**Symptom:**
```
# Throughput drops mid-run
fillrandom : thread 0: (100000,200000) ops and (45123,50234) ops/second
                                               ^^^^^^ ^^^^^^
                                               halved during compaction
```

**Solution:**
- Use `--disable_auto_compactions=1` for write-only benchmarks
- Or run `compact` first to stabilize LSM shape
- Or use `--stats_interval_seconds=1` to monitor compaction triggers

### 3. OS Page Cache Pollution

**Problem:** OS caches SST file data in page cache, bypassing RocksDB block cache.

**Impact:** Reads appear faster than real-world (page cache hit instead of disk I/O).

**Detection:** Both `--mmap_read=0` and `--mmap_read=1` can still use the OS page cache.

**Solution:**
- Use `--use_direct_reads=1` to bypass OS page cache (the only reliable method)
- Or clear page cache: `sync; echo 3 > /proc/sys/vm/drop_caches`

### 4. Small --num for Multi-Threaded Benchmarks

**Problem:** With `--threads=16` and `--num=1000000`, each thread processes only 62.5K ops.

**Symptom:** Low ops/sec because threads finish quickly and overhead dominates.

**Solution:** Scale `--num` with `--threads`: `--num=$((1000000 * threads))`

### 5. Debug Build Performance

**Problem:** Benchmarking with `make db_bench` (debug build) instead of `DEBUG_LEVEL=0 make db_bench`.

**Symptom:** 5-10x slower than expected, high variance.

**Detection:** `db_bench` prints warning:
```
WARNING: Assertions are enabled; benchmarks unnecessarily slow
WARNING: Optimization is disabled: benchmarks unnecessarily slow
```

**Solution:** Always use `make clean && DEBUG_LEVEL=0 make db_bench` for performance testing.

### 6. Using --use_existing_db Without Prep

**Problem:** Running read benchmark with `--use_existing_db=1` but DB has wrong number of keys.

**Symptom:**
```
# DB has 100K keys, but benchmark reads 1M keys
readrandom : ... (900000 NotFound, 100000 Found)
```

**Solution:** Match `--num` between write and read phases, or use `--use_existing_keys=1`.

---

## Custom Benchmark Implementation

### Adding a New Benchmark

**Steps:**

1. **Add method to `Benchmark` class** (`tools/db_bench_tool.cc`):
```cpp
void MyCustomBenchmark(ThreadState* thread) {
  Duration duration(FLAGS_duration, reads_);
  std::unique_ptr<const char[]> key_guard;
  Slice key = AllocateKey(&key_guard);
  std::string value;

  while (!duration.Done(1)) {
    // 1. Select DB and generate key
    DB* db = SelectDB(thread);
    int64_t key_rand = GetRandomKey(&thread->rand);
    GenerateKeyFromInt(key_rand, FLAGS_num, &key);

    // 2. Execute operation
    Status s = db->Get(read_options_, key, &value);

    // 3. Record latency (first arg is DBWithColumnFamilies* or nullptr)
    thread->stats.FinishedOps(nullptr, db, 1, kRead);

    // 4. Update byte counter
    if (s.ok()) {
      thread->stats.AddBytes(key.size() + value.size());
    }
  }
}
```

2. **Register in `Run()` method** (around line 3740):
```cpp
} else if (name == "mycustom") {
  method = &Benchmark::MyCustomBenchmark;
```

3. **Add to `--benchmarks` help text** (around line 167):
```cpp
"\tmycustom      -- description of custom benchmark\n"
```

4. **Rebuild:**
```bash
make clean
DEBUG_LEVEL=0 make db_bench
```

### Example: Benchmarking Delete()

```cpp
void DeleteRandom(ThreadState* thread) {
  Duration duration(FLAGS_duration, deletes_);
  std::unique_ptr<const char[]> key_guard;
  Slice key = AllocateKey(&key_guard);

  while (!duration.Done(1)) {
    DB* db = SelectDB(thread);
    int64_t key_rand = GetRandomKey(&thread->rand);
    GenerateKeyFromInt(key_rand, FLAGS_num, &key);

    Status s = db->Delete(write_options_, key);
    if (!s.ok()) {
      fprintf(stderr, "Delete failed: %s\n", s.ToString().c_str());
    }

    thread->stats.FinishedOps(nullptr, db, 1, kDelete);
    thread->stats.AddBytes(key.size());
  }
}
```

---

## Report Generation

### Standard Output Format

**Per-Benchmark Report:**
```
fillrandom   :      12.345 micros/op 81015 ops/sec 12.346 seconds 1000000 operations; 8.1 MB/s
```

### Histogram Output (--histogram=1)

**Per-Operation-Type Breakdown:**
```cpp
if (FLAGS_histogram) {
  for (auto it = hist_.begin(); it != hist_.end(); ++it) {
    fprintf(stdout, "Microseconds per %s:\n%s\n",
            OperationTypeString[it->first].c_str(),
            it->second->ToString().c_str());
  }
}
```

**OperationType Enum:**
```cpp
enum OperationType : unsigned char {
  kRead = 0, kWrite, kDelete, kSeek, kMerge, kUpdate,
  kCompress, kUncompress, kCrc, kHash, kOthers, kMultiScan
};
```

### Statistics Output (--statistics=1)

**Aggregate DB Stats:**
```bash
./db_bench --benchmarks=fillrandom,stats --statistics=1
```

**Output (from `Statistics::ToString()`):**
```
rocksdb.block.cache.miss COUNT : 1234567
rocksdb.block.cache.hit COUNT : 9876543
rocksdb.bloom.filter.useful COUNT : 5432109
rocksdb.number.keys.written COUNT : 1000000
rocksdb.bytes.written COUNT : 123456789
rocksdb.write.wal.time P50 : 1.234 P99 : 5.678
```

---

## Integration with Perf Tools

### PerfContext (Per-Thread Performance Counters)

**Enable:**
```bash
./db_bench --benchmarks=readrandom --perf_level=6  # kEnableTime
```

**PerfLevel Options:**
| Level | Value | Overhead | Metrics Collected |
|-------|-------|----------|-------------------|
| `kUninitialized` | 0 | N/A | Unknown setting |
| `kDisable` | 1 | None | No metrics |
| `kEnableCount` | 2 | Low | Operation counts (no timers) |
| `kEnableWait` | 3 | Low-Medium | Wait/delay time metrics |
| `kEnableTimeExceptForMutex` | 4 | Medium | Time metrics except mutex waits |
| `kEnableTimeAndCPUTimeExceptForMutex` | 5 | Medium-High | Time and CPU time metrics except mutex |
| `kEnableTime` | 6 | High | All time metrics including mutex |
| `kOutOfBounds` | 7 | N/A | Invalid |

**Usage in Benchmark:**
```cpp
SetPerfLevel(static_cast<PerfLevel>(FLAGS_perf_level));
get_perf_context()->Reset();

// ... run benchmark operations ...

if (FLAGS_perf_level > PerfLevel::kDisable) {
  thread->stats.AddMessage(std::string("PERF_CONTEXT:\n") +
                           get_perf_context()->ToString());
}
```

**Example Output:**
```
user_key_comparison_count = 1234567
block_cache_hit_count = 987654
block_read_count = 123456
block_read_time = 1234567890  # nanoseconds
get_from_memtable_time = 987654321
bloom_sst_hit_count = 543210
```

**Key Metrics:**
- `block_cache_hit_count / (block_cache_hit_count + block_read_count)` → Cache hit rate
- `bloom_sst_hit_count` → Total SST bloom filter hits (positive results). The counter for reads *avoided* by bloom is `bloom_filter_useful` (a ticker stat, not perf context)
- `get_from_memtable_time` → Time spent in memtable lookups

### IOStatsContext (I/O Statistics)

**Counters (thread-local):**
```cpp
struct IOStatsContext {
  uint64_t bytes_written;
  uint64_t bytes_read;
  uint64_t write_nanos;     // time in write()/pwrite()
  uint64_t read_nanos;      // time in read()/pread()
  uint64_t fsync_nanos;
  // ... temperature-based file I/O stats ...
};
```

Note: `db_bench` does not print `IOStatsContext` directly. It only appends `PERF_CONTEXT` output (when `--perf_level > kDisable`) and optionally `Statistics::ToString()`. To examine I/O stats, use `get_iostats_context()` programmatically or add custom reporting.

**Metric Interpretation:**
- `write_nanos / bytes_written` → Write bandwidth
- `fsync_nanos` → Time blocked on durable writes
- `read_nanos / bytes_read` → Read I/O bandwidth (raw I/O time, does not include decompression)

### Statistics (DB-Wide Aggregated Counters)

**Enable:**
```bash
./db_bench --statistics=1 --stats_interval_seconds=10
```

**Real-Time Stats (during benchmark, requires `--stats_per_interval > 0`):**
```
thread 0: (100000,200000) ops and (50123,60234) ops/second in (2.0,3.3) seconds

Compaction Stats:
Level  Files  Size(MB) Score ...
  L0      12      120   1.5
  L1      50      500   0.5
```

**Final Stats (after benchmark):**
```bash
./db_bench --benchmarks=stats --use_existing_db=1
```

---

## Summary Table: Benchmark Selection Guide

| Goal | Benchmark | Key Flags |
|------|-----------|-----------|
| Write throughput (sequential) | `fillseq` | `--num`, `--value_size`, `--disable_wal` |
| Write throughput (random) | `fillrandom` | `--num`, `--threads`, `--compression_type` |
| Read latency (point lookup) | `readrandom` | `--reads`, `--cache_size`, `--bloom_bits` |
| Read throughput (scan) | `readseq` | `--num`, `--threads` |
| Mixed workload | `readrandomwriterandom` | `--num`, `--readwritepercent`, `--threads` |
| Compaction performance | `compact` | `--num`, `--max_background_compactions` |
| Bloom filter effectiveness | `readrandom` + stats | `--bloom_bits`, `--statistics=1` |
| Compression ratio | `fillrandom,stats` | `--compression_type`, `--compression_level` |
| Iterator performance | `seekrandom` | `--seek_nexts`, `--num` |
| Batched reads | `multireadrandom` | `--batch_size`, `--multiread_batched` |
| Durable writes | `fillsync` | `--sync=1`, `--wal_dir` |

---

## References

- **Source Code:** `tools/db_bench_tool.cc` (main implementation)
- **Performance Context:** `include/rocksdb/perf_context.h`
- **I/O Stats:** `include/rocksdb/iostats_context.h`
- **Histograms:** `monitoring/histogram.h`
- **Statistics:** `monitoring/statistics_impl.h`

⚠️ **INVARIANT:** This documentation reflects db_bench as of RocksDB 11.x. New benchmarks and flags are added regularly—consult `db_bench --help` for the authoritative flag list.
