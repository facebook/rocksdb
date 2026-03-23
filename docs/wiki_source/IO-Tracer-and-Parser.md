On Posix systems we can use existing tracing mechanism(strace, blktrace etc.) to understand the system calls, IO requests. But on storage systems where we cannot use existing tracing tools, we added a mechanism to trace IO operations to understand IO behavior of RocksDB while accessing data on the storage.

### Table of Contents
* **[IO Trace Format](#io-trace-format)**<br>
* **[Usage](#usage)**<br>
* **[Implementation](#implementation)**<br>
* **[IO Tracer Parser](#io-tracer-parser)**<br>
* **[Planned Work](#planned-work)**<br>


# IO Trace Format
IO trace record contains following information:

Required for all records:
| Column Name   |  Values     | Comment |
| :------------- |:-------------|:-------------|
| Access timestamp in microseconds     | unsigned long | |
| File Operation     | string     | type of operation (Append, Read,...). |
| Latency | unsigned long  | |
| IO Status |      | IO Status of the file operation returned. |
| File Name   | string          | File name is printed instead of full file path |

Based on File Operation:
| Column Name   |  Values     | Comment |
| :------------- |:-------------|:-------------|
| Length      | unsigned long   |  |
| Offset      | unsigned long   |  |
| File Size   | unsigned long   |  | 

# Usage
An example to start IO tracing: 
```
Env* env = rocksdb::Env::Default();
EnvOptions env_options;
std::string trace_path = "/tmp/binary_trace_test_example”;
std::unique_ptr<TraceWriter> trace_writer;
DB* db = nullptr;
std::string db_name = "/tmp/rocksdb”;

/*Create the trace file writer*/
NewFileTraceWriter(env, env_options, trace_path, &trace_writer);

DB::Open(options, dbname);

/*Start IO tracing*/
db->StartIOTrace(env, trace_opt, std::move(trace_writer));

/*Your call of RocksDB APIs */
DB::Put();

/*End IO tracing*/
db->EndIOTrace();
```
If you call DB::Put then io_tracer will record all the FileSystem APIs called during DB::Put.

# Implementation
* Added tracing wrappers like ```FileSystemTracingWrapper``` extends ```FileSystemWrapper```, ```FSRandomRWFileTracingWrapper``` extends ```FSRandomRWFileWrapper```
, etc that calls the underlying FileSystem APIs and log the tracing.
* In FileSystemTracingWrapper APIs (for eg ```FileSystemTracingWrapper::Close()```):
  * Call underlying ```FileSystem::Close()```,
  * Create IOTraceRecord,
  * Call ```IOTracer::WriteIOOp``` to dump the trace in trace file.
* Added new classes ```FileSystemPtr```, etc. that overloads -> operator. It returns the appropriate f/s pointer based on
tracing is enabled/disabled to avoid tracing overhead.
*  Details can be found in:
   * https://github.com/facebook/rocksdb/blob/main/env/file_system_tracer.h
   * https://github.com/facebook/rocksdb/blob/main/trace_replay/io_tracer.h

# IO Tracer Parser
The trace file generated from IO tracing is in binary format. So parser can be used to read that binary trace file
```
./io_tracer_parser -io_trace_file trace_file
```
Implementation details can be found in https://github.com/facebook/rocksdb/tree/main/tools/io_tracer_parser_tool.h

# Planned Work
* Trace DB::Open
* Include more information in trace format