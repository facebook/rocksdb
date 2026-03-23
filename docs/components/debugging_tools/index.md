# RocksDB Debugging Tools

## Overview

RocksDB provides a comprehensive suite of debugging and diagnostic tools spanning CLI utilities, programmatic APIs, and runtime instrumentation. The ldb and sst_dump CLI tools enable offline inspection and recovery. The tracing subsystem (query, block cache, and IO tracing) captures runtime operations for replay and analysis. Built-in instrumentation via PerfContext, IOStatsContext, Statistics, and DB properties provides fine-grained runtime metrics without external tools. The Logger system and GetThreadList() API offer continuous visibility into database operations.

**Key source files:** tools/ldb_cmd.cc, tools/sst_dump_tool.cc, include/rocksdb/perf_context.h, include/rocksdb/iostats_context.h, include/rocksdb/statistics.h, include/rocksdb/trace_record.h, trace_replay/io_tracer.h, include/rocksdb/thread_status.h

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. ldb Tool | [01_ldb_tool.md](01_ldb_tool.md) | CLI architecture, subcommand dispatch, data query commands, metadata inspection, manipulation commands, and global options. |
| 2. sst_dump Tool | [02_sst_dump_tool.md](02_sst_dump_tool.md) | SST file inspection, block-level analysis, checksum verification, compression benchmarking, and table properties output. |
| 3. Operation Tracing | [03_operation_tracing.md](03_operation_tracing.md) | Query-level tracing (StartTrace/EndTrace), trace file format, Replayer API, and the trace_analyzer analysis tool. |
| 4. Block Cache Tracing | [04_block_cache_tracing.md](04_block_cache_tracing.md) | Block cache access tracing, BlockCacheTraceRecord format, spatial sampling, lookup context propagation, and cache access analysis. |
| 5. IO Tracing | [05_io_tracing.md](05_io_tracing.md) | File system operation tracing via IOTracer, IOTraceRecord format, FileSystemTracingWrapper integration, and the io_tracer_parser tool. |
| 6. PerfContext and IOStatsContext | [06_perf_context.md](06_perf_context.md) | Thread-local performance counters, PerfLevel control, per-level metrics, IO statistics by temperature, and usage patterns. |
| 7. Statistics | [07_statistics.md](07_statistics.md) | Global database statistics via Tickers and Histograms, StatsLevel control, stats dumping, persistent stats history, and stats history iteration. |
| 8. DB Properties | [08_db_properties.md](08_db_properties.md) | Runtime property queries via GetProperty/GetIntProperty/GetMapProperty, available properties, and stats dump configuration. |
| 9. Logger and Info Log | [09_logger.md](09_logger.md) | Logger interface, InfoLogLevel enum, AutoRollLogger for size/time-based rotation, log configuration options, and the LOG file. |
| 10. Thread Status | [10_thread_status.md](10_thread_status.md) | GetThreadList() API, ThreadStatus structure, operation/stage/state tracking, and compaction/flush property interpretation. |
| 11. RepairDB and Recovery | [11_repair_and_recovery.md](11_repair_and_recovery.md) | RepairDB process (find files, replay WALs, extract metadata, write MANIFEST), reduce_levels, and rocksdb_dump/rocksdb_undump. |
| 12. Debugging Workflows | [12_debugging_workflows.md](12_debugging_workflows.md) | Step-by-step workflows for corruption investigation, performance diagnosis, space amplification analysis, crash recovery, and troubleshooting. |

## Key Characteristics

- **Two CLI tools**: ldb (~39 subcommands for database-level operations) and sst_dump (file-level SST analysis)
- **Three tracing subsystems**: query-level, block cache, and IO tracing -- each with independent start/stop lifecycle
- **Thread-local instrumentation**: PerfContext and IOStatsContext provide per-request metrics with zero contention
- **Configurable overhead**: PerfLevel (7 levels including kUninitialized) and StatsLevel (6 levels) control instrumentation granularity
- **Auto-rolling LOG file**: size-based and time-based rotation with configurable retention
- **Stats dump**: periodic stats output to LOG file (default: every 600 seconds via stats_dump_period_sec)
- **Trace replay**: captured traces can be replayed against a database with configurable speed and parallelism
- **RepairDB**: best-effort recovery that reconstructs MANIFEST from surviving SST files

## Key Invariants

- PerfContext and IOStatsContext are thread-local; each thread must call get_perf_context() and get_iostats_context() independently
- Trace files use a common binary format with header/footer framing; all three tracing subsystems share the TraceWriter/TraceReader interface
- RepairDB places all recovered SST files at L0; compaction reorganizes levels on next DB open
