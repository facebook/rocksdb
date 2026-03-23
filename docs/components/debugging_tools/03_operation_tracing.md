# Operation Tracing

**Files:** include/rocksdb/trace_record.h, include/rocksdb/trace_reader_writer.h, include/rocksdb/utilities/replayer.h, include/rocksdb/options.h (TraceOptions), trace_replay/trace_replay.h, trace_replay/trace_record_handler.h, utilities/trace/replayer_impl.cc, tools/trace_analyzer_tool.h, tools/trace_analyzer_tool.cc

## Overview

RocksDB's query-level tracing system captures database operations (Get, MultiGet, Write, Iterator Seek/SeekForPrev) as timestamped records to a trace file. These traces can be replayed against a database or analyzed offline using the trace_analyzer tool. This enables workload characterization, regression testing, and performance analysis.

## Tracing Lifecycle

Step 1: Create a TraceWriter via NewFileTraceWriter() or a custom implementation.

Step 2: Call DB::StartTrace() with TraceOptions and the writer. Tracing begins immediately.

Step 3: All matching operations are recorded with timestamps. Operations can be filtered and sampled via TraceOptions.

Step 4: Call DB::EndTrace() to stop recording and flush the trace file.

## TraceOptions Configuration

TraceOptions (see include/rocksdb/options.h) controls tracing behavior:

| Field | Default | Description |
|-------|---------|-------------|
| max_trace_file_size | 64 GB | Maximum trace file size; tracing stops when reached |
| sampling_frequency | 1 | Capture one per N requests (1 = capture all) |
| filter | kTraceFilterNone | Bitmask of operation types to exclude |
| preserve_write_order | false | When true, write records match WAL order (may impact performance) |

Filter flags include kTraceFilterGet, kTraceFilterWrite, kTraceFilterIteratorSeek, kTraceFilterIteratorSeekForPrev, and kTraceFilterMultiGet. Filtering is applied before sampling.

**Note on write ordering:** When preserve_write_order is false, writes are traced early in the write path for lower latency. When true, writes are traced later under trace_mutex_ in batch or WAL order. This affects what "ground truth ordering" means in a trace file.

## TraceRecord Types

The TraceType enum (see include/rocksdb/trace_record.h) defines the trace record types:

| TraceType | Class | Captured Data |
|-----------|-------|---------------|
| kTraceWrite | WriteQueryTraceRecord | Serialized WriteBatch representation |
| kTraceGet | GetQueryTraceRecord | Column family ID + key |
| kTraceIteratorSeek | IteratorSeekQueryTraceRecord | Column family ID + seek key + bounds |
| kTraceIteratorSeekForPrev | IteratorSeekQueryTraceRecord | Column family ID + seek key + bounds |
| kTraceMultiGet | MultiGetQueryTraceRecord | Column family IDs + keys |

Each record includes a microsecond-precision timestamp from the system clock.

## TraceWriter and TraceReader

The TraceWriter and TraceReader interfaces (see include/rocksdb/trace_reader_writer.h) abstract trace I/O. The default file-based implementations are created via NewFileTraceWriter() and NewFileTraceReader(). Custom implementations can export traces to any system (e.g., network, in-memory buffer).

**Note:** The default implementations are not thread-safe. The tracing system handles thread safety internally before invoking the writer.

## Replayer API

The Replayer class (see include/rocksdb/utilities/replayer.h) replays captured traces against a database. It can be created via DB::NewDefaultReplayer().

Replay workflow:

Step 1: Create a TraceReader pointing to the trace file.

Step 2: Call DB::NewDefaultReplayer() with column family handles and the reader.

Step 3: Call Replayer::Prepare() to initialize (reads the trace header).

Step 4: Either replay all traces via Replayer::Replay() with ReplayOptions, or read and execute individual records via Next() and Execute().

ReplayOptions (see include/rocksdb/utilities/replayer.h) provides:

| Field | Default | Description |
|-------|---------|-------------|
| num_threads | 1 | Number of threads for parallel replay |
| fast_forward | 1.0 | Speed multiplier (>1.0 speeds up, <1.0 slows down) |

**Note:** The replayer tolerates missing footer or EOF, treating them as success. This allows replay of partially closed or interrupted traces.

## Trace Analyzer Tool

The trace_analyzer tool (see tools/trace_analyzer_tool.h) provides offline analysis of trace files. It is a flag-driven tool that produces optional reports based on gflags configuration. Available analyses include:

- Access frequency and key distribution per column family (via --analyze_get, --analyze_put, etc.)
- QPS (queries per second) over time (via --output_qps_stats)
- Key size and value size distributions (via --output_value_distribution)
- Operation type correlations (via --print_correlation)
- Per-key access counts and temporal patterns (via --output_key_stats, --output_access_count_stats)
- Whole-keyspace statistics (via --key_space_dir)

Analysis is enabled per operation type via separate flags: --analyze_get, --analyze_put, --analyze_delete, --analyze_single_delete, --analyze_range_delete, --analyze_merge, --analyze_iterator, --analyze_multiget.

**MultiGet accounting:** MultiGet operations are counted on a per-requested-key basis, not per MultiGet call. Three MultiGet calls requesting nine total keys contribute a MultiGet QPS of nine.

**Build dependency:** The trace_analyzer binary requires GFLAGS to be available at build time.

Statistics are tracked per column family in the TraceStats struct, which includes access counts, key/value size distributions, peak/average QPS, per-key statistics, and time-series data.

## Workflow: Capturing and Analyzing a Workload

Step 1: Start tracing on a running database with appropriate sampling and filtering.

Step 2: Let the workload run for the desired duration.

Step 3: Stop tracing via EndTrace().

Step 4: Analyze with trace_analyzer to understand workload characteristics (key hotspots, operation mix, temporal patterns).

Step 5: Optionally replay the trace against a test database with different configurations to compare performance.
