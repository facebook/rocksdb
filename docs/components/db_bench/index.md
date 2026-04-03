# db_bench: RocksDB Benchmarking Tool

## Overview

`db_bench` is RocksDB's primary benchmarking tool for measuring database performance across write, read, scan, compaction, and mixed workloads. It supports multi-threaded execution, statistical aggregation across repeated runs, latency histograms, and integration with RocksDB's performance monitoring infrastructure (PerfContext, IOStatsContext, Statistics).

**Key source files:** `tools/db_bench_tool.cc`, `tools/db_bench.cc`, `include/rocksdb/perf_context.h`, `include/rocksdb/iostats_context.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Architecture and Execution Flow | [01_architecture.md](01_architecture.md) | `Benchmark`, `Stats`, `ThreadState`, `CombinedStats` classes; benchmark dispatch via `Run()`; thread launch and stats merging. |
| 2. Write Benchmarks | [02_write_benchmarks.md](02_write_benchmarks.md) | `fillseq`, `fillrandom`, `filluniquerandom`, `overwrite`, `fillsync`, `fill100K`; `DoWrite()` flow; `WriteMode` enum; key generation and value generation. |
| 3. Read Benchmarks | [03_read_benchmarks.md](03_read_benchmarks.md) | `readrandom`, `readseq`, `readreverse`, `multireadrandom`, `seekrandom`; cache warming; key distribution; `MultiGet` batching modes. |
| 4. Mixed and Specialized Benchmarks | [04_mixed_benchmarks.md](04_mixed_benchmarks.md) | `readwhilewriting`, `readrandomwriterandom`, `updaterandom`, `mixgraph`, `timeseries`, `randomtransaction`; concurrent workload patterns. |
| 5. Configuration Flags | [05_configuration_flags.md](05_configuration_flags.md) | Database sizing, concurrency, cache, compression, bloom filter, compaction, write options, and advanced flag groups. |
| 6. Performance Metrics and Reporting | [06_metrics_and_reporting.md](06_metrics_and_reporting.md) | Output format; latency histograms; multi-run statistics with `[XN]` repeat and `[WN]` warmup syntax; `CombinedStats` confidence intervals. |
| 7. PerfContext and Statistics Integration | [07_perf_integration.md](07_perf_integration.md) | `PerfLevel` options; `PerfContext` per-thread counters; `IOStatsContext`; `Statistics` DB-wide aggregates; periodic reporting with `--stats_interval_seconds`. |
| 8. Benchmarking Methodology | [08_methodology.md](08_methodology.md) | Baseline vs experimental comparison; statistical significance; controlling variables; common pitfalls and their solutions. |
| 9. MixGraph Workload Modeling | [09_mixgraph.md](09_mixgraph.md) | Two-term exponential key distribution; Generalized Pareto value/scan length distributions; sine-wave QPS control; configurable Get/Put/Seek ratios. |
| 10. Building, Extending, and Best Practices | [10_extending.md](10_extending.md) | Build modes; adding new benchmarks; multi-DB mode; NUMA support; tracing and block cache tracing; options file loading. |

## Key Characteristics

- **Single binary**: All benchmarks compiled into one tool (`tools/db_bench_tool.cc`)
- **Multi-threaded**: Configurable via `--threads`; some benchmarks (e.g., `readwhilewriting`) automatically add background threads
- **Statistical aggregation**: Built-in repeat (`[XN]`) and warmup (`[WN]`) syntax with 95% confidence intervals
- **Flexible key/value generation**: Sequential, random, unique-random write modes; fixed, uniform, or normal value size distributions
- **Compressible data**: `RandomGenerator` produces data targeting `--compression_ratio` using `CompressibleString()`
- **Rate limiting**: Independent read and write rate limiters via `--benchmark_read_rate_limit` and `--benchmark_write_rate_limit`
- **Multiple DB support**: `--num_multi_db` for running across multiple DB instances simultaneously
- **Transaction support**: `--optimistic_transaction_db` and `--transaction_db` for transaction benchmarking

## Key Measurement Rules

- Always use release builds (`DEBUG_LEVEL=0`) for performance measurements; debug builds include assertions and skip compiler optimizations
- `micros/op` uses per-thread cumulative time (`seconds_`), while `ops/sec` uses wall-clock elapsed time; for multi-threaded benchmarks, `micros/op * ops/sec != 1,000,000`
- Cache must be warm for read benchmarks; use `readtocache` first or results reflect cold-cache performance
- The `--num` flag defines both the key space and the default per-thread operation count (when `--reads`/`--writes` are not specified). Use `--reads` or `--duration` to decouple operation count from key space size
