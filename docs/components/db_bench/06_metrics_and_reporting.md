# Performance Metrics and Reporting

**Files:** `tools/db_bench_tool.cc`

## Standard Output Format

Each benchmark produces a single-line report:

```
fillrandom   :      12.345 micros/op 81015 ops/sec 12.346 seconds 1000000 operations; 8.1 MB/s
```

| Field | Formula | Notes |
|-------|---------|-------|
| `micros/op` | `seconds_ * 1e6 / done_` | Average per-operation latency. `seconds_` is sum of per-thread elapsed times |
| `ops/sec` | `done_ / elapsed` | Aggregate throughput. `elapsed` is wall-clock time (earliest start to latest finish) |
| `seconds` | `(finish_ - start_) * 1e-6` | Wall-clock duration |
| `operations` | `done_` | Total operations across all threads |
| `MB/s` | `(bytes_ / 1048576.0) / elapsed` | Data bandwidth (only printed if bytes were tracked) |

Important: For multi-threaded benchmarks, `micros/op` and `ops/sec` measure different things. `micros/op` uses the sum of per-thread times (reflecting average per-thread latency), while `ops/sec` uses wall-clock time (reflecting aggregate throughput). Therefore `micros/op * ops/sec != 1,000,000` for multi-threaded runs.

## Latency Histograms

Enabled by `--histogram=1`. Histograms are collected per `OperationType` and printed after the benchmark completes.

Example output:

```
Microseconds per read:
Count: 1000000  Average: 1.2345  StdDev: 0.89
Min: 0  Median: 1.0234  Max: 156
Percentiles: P50: 1.02 P75: 1.45 P99: 4.23 P99.9: 12.45 P99.99: 45.67
```

### Latency Measurement

Each call to `Stats::FinishedOps()` records the time delta since the previous operation completed (`now - last_op_finish_`). This means:

- Latency includes inter-operation overhead (key generation, rate limiting waits, etc.)
- Exception: write benchmarks call `ResetLastOpTime()` after rate-limiter sleeps in `DoWrite()`, so write histograms exclude rate limiter wait time while read histograms generally include it
- The first operation in each thread includes setup time from `last_op_finish_ = start_`
- Slow operations (exceeding `--slow_usecs`, default 1 second) are logged to stderr
- For single-operation benchmarks (`compact`, `flush`, `backup`, `restore`, `openandcompact`), `done_` is forced to 1 if `FinishedOps()` was never called, so `micros/op` and `ops/sec` reflect the entire benchmark invocation rather than per-DB-operation metrics

### Key Percentiles

| Percentile | Use Case |
|------------|----------|
| P50 (median) | Typical latency experienced by most operations |
| P99 | User-facing SLA target |
| P99.9 | Tail latency; detects compaction pauses and I/O stalls |
| P99.99 | Extreme tail; useful for debugging rare stalls |

## Multi-Run Statistics

### Repeat Syntax

Append `[XN]` to a benchmark name to run it N times with aggregated statistics:

```bash
./db_bench --benchmarks=fillrandom[X5]
```

Produces per-run reports plus final aggregation:

```
fillrandom [AVG    5 runs] : 81015 (+/- 523) ops/sec; 0.012 ms/op; 8.1 (+/- 0.1) MB/sec
fillrandom [MEDIAN 5 runs] : 81200 ops/sec; 8.2 MB/sec
```

### Warmup Syntax

Append `[WN]` to run N warmup iterations that are not included in statistics:

```bash
./db_bench --benchmarks=readrandom[W2-X5]
```

Runs 2 warmup iterations (not reported), then 5 measured iterations with aggregation.

### CombinedStats Calculations

| Metric | Formula |
|--------|---------|
| AVG | `mean(throughput_ops_)` |
| +/- | `1.96 * stddev / sqrt(n)` (95% confidence interval half-width using Bessel's correction for sample variance) |
| MEDIAN | Middle value of sorted throughput samples |
| ms/op | `1000.0 / avg_ops_per_sec` |

Note: `CombinedStats` requires at least 2 runs to produce aggregated output. A single run produces only the standard per-benchmark report.

### Confidence Interval Only Mode

When `--confidence_interval_only=true`, each intermediate report shows only the CI bounds:

```
fillrandom [CI95 5 runs] : (80492, 81538) ops/sec; (7.9, 8.3) MB/sec
```

## Interval Reporting

### Operation-Based Intervals

When `--stats_interval > 0`, progress reports are printed at regular operation counts:

```
2026/03/23-10:00:00 ... thread 0: (100000,200000) ops and (50123.0,60234.0) ops/second in (2.000000,3.300000) seconds
```

The tuple format is `(interval_ops, cumulative_ops)` and `(interval_rate, cumulative_rate)`.

### Time-Based Intervals

`--stats_interval_seconds` provides time-based reporting. When set, it forces `FLAGS_stats_interval = 1000` internally, so the wall-clock timer is only checked at 1000-operation boundaries. Reports include optional DB stats (`--stats_per_interval > 0`), thread status (`--thread_status_per_interval > 0`), and table properties (`--show_table_properties`).

### CSV Reporting

`--report_interval_seconds > 0` spawns a `ReporterAgent` background thread that writes periodic samples to `--report_file` (default `report.csv`):

```csv
secs_elapsed,interval_qps
10,81234
20,79876
```

Note: Despite the column header `interval_qps`, the second column is the raw count of operations completed during the interval, not ops/sec. The value is not divided by the interval duration.

## Progress Reporting

Without `--stats_interval`, progress updates use an adaptive schedule:

- First 1000 ops: every 100
- 1000-5000: every 500
- 5000-10000: every 1000
- 10000-50000: every 5000
- 50000-100000: every 10000
- 100000-500000: every 50000
- 500000+: every 100000

Disabled by `--progress_reports=false`.
