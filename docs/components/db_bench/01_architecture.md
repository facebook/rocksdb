# Architecture and Execution Flow

**Files:** `tools/db_bench_tool.cc`, `tools/db_bench.cc`

## Key Classes

| Class | Responsibility |
|-------|---------------|
| `Benchmark` | Main orchestrator. Parses flags, opens DB, dispatches benchmark methods via `Run()` |
| `ThreadState` | Per-thread execution context. Holds `Random64` RNG (seeded per-thread), `Stats`, and pointer to `SharedState` |
| `Stats` | Per-thread statistics collector. Tracks latency histograms (per `OperationType`), throughput counters, byte counters, and interval reporting |
| `CombinedStats` | Aggregates stats from multiple runs of the same benchmark. Computes mean, median, standard deviation, and 95% confidence intervals |
| `SharedState` | Shared synchronization state across benchmark threads. Contains mutex/condvar for barrier, rate limiters, and perf level |
| `Duration` | Termination condition. Supports time-based (`--duration`) or operation-count-based completion with configurable check frequency |
| `RandomGenerator` | Generates compressible values with controlled compression ratio via `CompressibleString()`. Supports fixed, uniform, and normal size distributions |
| `DBWithColumnFamilies` | Wrapper holding DB pointer, column family handles, and hot-CF rotation logic for multi-CF benchmarks |
| `ReporterAgent` | Background thread that writes periodic throughput samples to CSV file (enabled by `--report_interval_seconds`) |

## Execution Flow

### Benchmark Startup

Step 1: `db_bench_tool()` entry point parses command-line flags via gflags, then creates a `Benchmark` instance.

Step 2: `Benchmark::Run()` opens the DB via `Open()`, which calls either `InitializeOptionsFromFile()` (if `--options_file` is set) or `InitializeOptionsFromFlags()` to translate gflags into RocksDB `Options`.

Step 3: `Run()` prints a header with environment info (CPU model, cache size, RocksDB version), key/value sizes, compression type, and warnings for debug builds or unavailable compression.

### Benchmark Dispatch

Step 4: `Run()` iterates over comma-separated benchmark names from `--benchmarks`. For each name:

- Parses optional arguments: `benchmark[X5]` sets `num_repeat=5`, `benchmark[W2]` sets `num_warmup=2`, combinable as `benchmark[X5-W2]`
- Maps the name to a method pointer (e.g., `"fillrandom"` maps to `&Benchmark::WriteRandom`)
- Sets `fresh_db=true` for write benchmarks that need a clean DB (`fillseq`, `fillrandom`, etc.)
- Adjusts `num_threads` for composite benchmarks (e.g., `readwhilewriting` adds +1 for the writer thread)

Step 5: If `fresh_db` is true and `--use_existing_db` is false, destroys the existing DB and re-opens.

### Thread Execution

Step 6: `RunBenchmark()` launches `n` threads:

- Creates `ThreadArg` array with `Benchmark*`, method pointer, and per-thread `ThreadState`
- Each `ThreadState` gets a unique `Random64` seed derived from `seed_base + total_thread_count`
- Optionally binds threads to NUMA nodes when `--enable_numa` is true
- Threads are launched via `Env::StartThread()` calling `ThreadBody()`

Step 7: `ThreadBody()` implements a barrier pattern:

- Each thread signals initialization complete
- All threads wait until `shared->start == true`
- Each thread runs the benchmark method
- Each thread records `stats.Stop()` and signals completion

Step 8: After all threads complete, `RunBenchmark()` merges stats from all threads (excluding threads with `exclude_from_merge_` set, used by background writer threads in composite benchmarks) and calls `Stats::Report()`.

### Multi-Run Aggregation

Step 9: When `num_repeat > 1`, each run's stats are added to `CombinedStats`. After each run, intermediate averages and confidence intervals are printed. After all runs, `CombinedStats::ReportFinal()` prints:

- AVG: mean throughput with 95% confidence interval (using 1.96 * standard error)
- MEDIAN: median throughput (robust to outliers)
- ms/op: milliseconds per operation derived from average throughput

## Stats Collection

### FinishedOps

The `Stats::FinishedOps()` method is called after each operation (or batch of operations). It:

1. Reports to `ReporterAgent` if configured
2. If `--histogram` is enabled: records per-operation latency (`now - last_op_finish_`) in a per-`OperationType` histogram; logs slow operations exceeding `--slow_usecs`
3. Increments `done_` counter
4. At reporting intervals: prints per-thread interval throughput and cumulative throughput; optionally prints DB stats (`rocksdb.cfstats`) and thread status

### Stats::Report

The final report computes:

- `micros/op = seconds_ * 1e6 / done_` where `seconds_` is per-thread elapsed time (sum of all thread times for merged stats)
- `ops/sec = done_ / elapsed` where `elapsed` is wall-clock time from earliest start to latest finish
- `MB/s = (bytes_ / 1048576.0) / elapsed` if bytes were tracked

Important: For multi-threaded benchmarks, `seconds_` is the sum of per-thread times, so `micros/op` reflects average per-thread latency, while `ops/sec` reflects aggregate throughput.

## DB Opening Modes

The `OpenDb()` method supports multiple DB types selected by flags:

| Flag | DB Type |
|------|---------|
| `--readonly` | `DB::OpenForReadOnly()` |
| `--optimistic_transaction_db` | `OptimisticTransactionDB::Open()` |
| `--transaction_db` | `TransactionDB::Open()` |
| `--use_blob_db` | `blob_db::BlobDB::Open()` (stacked BlobDB) |
| `--use_secondary_db` | `DB::OpenAsSecondary()` with periodic catchup thread |
| `--open_as_follower` | `DB::OpenAsFollower()` |
| (default) | `DB::Open()` |

When `--num_column_families > 1`, the DB is opened with multiple column families. The `--num_hot_column_families` flag controls how many CFs are actively used at a time, with rotation via `CreateNewCf()`.

## OperationType Enum

Operations are categorized for histogram reporting:

| Type | Used By |
|------|---------|
| `kRead` | `ReadRandom`, `ReadSequential`, `MultiReadRandom`, `MixGraph` (Get) |
| `kWrite` | `DoWrite`, `MixGraph` (Put) |
| `kDelete` | `DeleteSeq`, `DeleteRandom` |
| `kSeek` | `SeekRandom`, `MixGraph` (Seek) |
| `kMerge` | `MergeRandom`, `ReadRandomMergeRandom` |
| `kUpdate` | `UpdateRandom`, `XORUpdateRandom` |
| `kCompress` | `Compress` (raw compression benchmark) |
| `kUncompress` | `Uncompress` (raw decompression benchmark) |
| `kCrc` | `Crc32c` |
| `kHash` | `xxHash`, `xxHash64`, `xxh3` |
| `kMultiScan` | `MultiScan` |
| `kOthers` | `AcquireLoad`, misc operations |
