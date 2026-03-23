# Operation Tracing

**Files:** `include/rocksdb/trace_record.h`, `include/rocksdb/trace_reader_writer.h`, `include/rocksdb/utilities/replayer.h`, `include/rocksdb/options.h` (TraceOptions), `trace_replay/trace_replay.h`, `trace_replay/trace_record_handler.h`, `tools/trace_analyzer_tool.h`, `tools/trace_analyzer_tool.cc`

## Overview

RocksDB's query-level tracing system captures database operations (Get, MultiGet, Write, Iterator Seek/SeekForPrev) as timestamped records to a trace file. These traces can be replayed against a database or analyzed offline using the `trace_analyzer` tool. This enables workload characterization, regression testing, and performance analysis.

## Tracing Lifecycle

Step 1: Create a `TraceWriter` via `NewFileTraceWriter()` or a custom implementation.

Step 2: Call `DB::StartTrace()` with `TraceOptions` and the writer. Tracing begins immediately.

Step 3: All matching operations are recorded with timestamps. Operations can be filtered and sampled via `TraceOptions`.

Step 4: Call `DB::EndTrace()` to stop recording and flush the trace file.

## TraceOptions Configuration

`TraceOptions` (see `include/rocksdb/options.h`) controls tracing behavior:

| Field | Default | Description |
|-------|---------|-------------|
| `max_trace_file_size` | 64 GB | Maximum trace file size; tracing stops when reached |
| `sampling_frequency` | 1 | Capture one per N requests (1 = capture all) |
| `filter` | `kTraceFilterNone` | Bitmask of operation types to exclude |
| `preserve_write_order` | false | When true, write records match WAL order (may impact performance) |

Filter flags include `kTraceFilterGet`, `kTraceFilterWrite`, `kTraceFilterIteratorSeek`, `kTraceFilterIteratorSeekForPrev`, and `kTraceFilterMultiGet`. Filtering is applied before sampling.

## TraceRecord Types

The `TraceType` enum (see `include/rocksdb/trace_record.h`) defines the trace record types:

| TraceType | Class | Captured Data |
|-----------|-------|---------------|
| `kTraceWrite` | `WriteQueryTraceRecord` | Serialized `WriteBatch` representation |
| `kTraceGet` | `GetQueryTraceRecord` | Column family ID + key |
| `kTraceIteratorSeek` | `IteratorSeekQueryTraceRecord` | Column family ID + seek key + bounds |
| `kTraceIteratorSeekForPrev` | `IteratorSeekQueryTraceRecord` | Column family ID + seek key + bounds |
| `kTraceMultiGet` | `MultiGetQueryTraceRecord` | Column family IDs + keys |

Each record includes a microsecond-precision timestamp from the system clock.

## TraceWriter and TraceReader

The `TraceWriter` and `TraceReader` interfaces (see `include/rocksdb/trace_reader_writer.h`) abstract trace I/O. The default file-based implementations are created via `NewFileTraceWriter()` and `NewFileTraceReader()`. Custom implementations can export traces to any system (e.g., network, in-memory buffer).

**Note:** The default implementations are not thread-safe. The tracing system handles thread safety internally before invoking the writer.

## Replayer API

The `Replayer` class (see `include/rocksdb/utilities/replayer.h`) replays captured traces against a database. It can be created via `DB::NewDefaultReplayer()`.

Replay workflow:

Step 1: Create a `TraceReader` pointing to the trace file.

Step 2: Call `DB::NewDefaultReplayer()` with column family handles and the reader.

Step 3: Call `Replayer::Prepare()` to initialize (reads the trace header).

Step 4: Either replay all traces via `Replayer::Replay()` with `ReplayOptions`, or read and execute individual records via `Next()` and `Execute()`.

`ReplayOptions` (see `include/rocksdb/utilities/replayer.h`) provides:

| Field | Default | Description |
|-------|---------|-------------|
| `num_threads` | 1 | Number of threads for parallel replay |
| `fast_forward` | 1.0 | Speed multiplier (>1.0 speeds up, <1.0 slows down) |

## Trace Analyzer Tool

The `trace_analyzer` tool (see `tools/trace_analyzer_tool.h`) provides offline analysis of trace files. It computes per-operation statistics including:

- Access frequency and key distribution per column family
- QPS (queries per second) over time
- Key size and value size distributions
- Operation type correlations (e.g., Get after Put patterns)
- Per-key access counts and temporal patterns

The analyzer defines operation types via `TraceOperationType` enum: Get, Put, Delete, SingleDelete, RangeDelete, Merge, IteratorSeek, IteratorSeekForPrev, MultiGet, and PutEntity.

Statistics are tracked per column family in the `TraceStats` struct, which includes access counts, key/value size distributions, peak/average QPS, per-key statistics, and time-series data.

## Workflow: Capturing and Analyzing a Workload

Step 1: Start tracing on a running database with appropriate sampling and filtering.

Step 2: Let the workload run for the desired duration.

Step 3: Stop tracing via `EndTrace()`.

Step 4: Analyze with `trace_analyzer` to understand workload characteristics (key hotspots, operation mix, temporal patterns).

Step 5: Optionally replay the trace against a test database with different configurations to compare performance.
