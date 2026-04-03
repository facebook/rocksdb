# IO Tracing

**Files:** trace_replay/io_tracer.h, trace_replay/io_tracer.cc, env/file_system_tracer.h, env/file_system_tracer.cc, tools/io_tracer_parser_tool.h, tools/io_tracer_parser_tool.cc, tools/io_tracer_parser.cc

## Overview

IO tracing captures file system operations (reads, writes, syncs, opens, etc.) with timestamps, latencies, file names, and operation parameters. It wraps the FileSystem interface to intercept all IO without modifying application code. The captured traces can be parsed into human-readable format using the io_tracer_parser tool.

## Tracing Lifecycle

Step 1: Create a TraceWriter via NewFileTraceWriter().

Step 2: Call DB::StartIOTrace() with TraceOptions and the writer.

Step 3: All file system operations are recorded with latency and context.

Step 4: Call DB::EndIOTrace() to stop recording.

## IOTraceRecord Format

Each IO operation is recorded as an IOTraceRecord (see trace_replay/io_tracer.h) containing:

| Field | Description |
|-------|-------------|
| access_timestamp | Nanosecond timestamp of the operation (recorded via NowNanos()) |
| trace_type | Always kIOTracer |
| file_operation | Name of the file system operation (e.g., "Read", "Write", "Sync") |
| latency | Operation latency in nanoseconds (recorded via ElapsedNanos()) |
| io_status | Status string from the IO operation |
| file_name | File basename (not full path) of the target file |
| io_op_data | Bitmask indicating which optional fields are present |

Optional fields (controlled by io_op_data bitmask):

| IOTraceOp Bit | Field | Description |
|---------------|-------|-------------|
| kIOFileSize (bit 0) | file_size | Current file size |
| kIOLen (bit 1) | len | Bytes read or written |
| kIOOffset (bit 2) | offset | File offset of the operation |

Additional fields include trace_data and request_id from IODebugContext, which carry implementation-specific debugging information.

## IOTracer Architecture

The IOTracer class (see trace_replay/io_tracer.h) manages the tracing lifecycle:

- StartIOTrace() initializes the IOTraceWriter and sets tracing_enabled to true
- EndIOTrace() stops tracing and releases the writer (no trace footer is written)
- WriteIOOp() writes a trace record if tracing is enabled
- is_tracing_enabled() provides a fast check using a non-atomic boolean (performance optimization, see TSAN suppression comment in source)

The IOTraceWriter handles serialization of IOTraceRecord to the trace file via the shared TraceWriter interface. It writes a header record at trace start containing the RocksDB version and start timestamp.

**Note:** The tracing_enabled flag is intentionally not atomic for performance. The writer_ field is protected by a mutex, so even if tracing_enabled shows a stale value, WriteIOOp() checks the writer under mutex and safely ignores the operation if the writer is null.

## FileSystem Integration

IO tracing is implemented by wrapping the FileSystem with a tracing layer (FileSystemTracingWrapper, see env/file_system_tracer.h). This wrapper intercepts all file system calls (read, write, sync, open, close, etc.) and records them via IOTracer::WriteIOOp() before or after delegating to the underlying file system.

The wrapper is transparent to the rest of RocksDB -- no code changes are needed in table readers, WAL writers, or other components.

## io_tracer_parser Tool

The io_tracer_parser tool (see tools/io_tracer_parser_tool.h) converts binary IO trace files into human-readable output. The IOTraceRecordParser class reads records sequentially via IOTraceReader and prints them in a tabular format showing timestamp, operation, file name, latency, offset, length, and status.

Usage: io_tracer_parser --io_trace_file=<path>

**Build dependency:** The io_tracer_parser binary requires GFLAGS to be available at build time.

## Analysis Use Cases

IO traces help diagnose:

- Which files generate the most IO (hot files identification)
- Read/write amplification at the file system level
- IO latency distribution per operation type
- Temporal IO patterns (burst vs. steady-state)
- Whether read-ahead or prefetching is effective
- Correlation between high-level operations (compaction, flush) and low-level IO patterns
