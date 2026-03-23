# Thread Status

**Files:** include/rocksdb/thread_status.h, include/rocksdb/env.h (GetThreadList), include/rocksdb/options.h (enable_thread_tracking), monitoring/thread_status_updater.h, monitoring/thread_status_util.h

## Overview

The GetThreadList() API provides real-time visibility into what RocksDB background threads are doing. It reports the operation type, stage, elapsed time, and operation-specific properties for each registered thread. This is valuable for diagnosing stuck compactions, long-running flushes, and thread pool utilization issues.

## Enabling Thread Tracking

Thread status tracking must be explicitly enabled via DBOptions::enable_thread_tracking = true (see include/rocksdb/options.h). This option controls whether per-DB operation details (operation type, stage, elapsed time) are reported for threads working on that database. When disabled, background threads are still registered and appear in GetThreadList() but with OP_UNKNOWN operation type and empty db_name/cf_name fields.

**Note:** Thread tracking adds minor overhead to each thread's operation lifecycle (updating shared status structures). The overhead is negligible for most workloads.

## Using GetThreadList

Call Env::GetThreadList() to retrieve the current status of all RocksDB threads:

The function returns a vector of ThreadStatus structs, one per registered RocksDB thread. This includes idle background threads (which appear with empty db_name/cf_name and OP_UNKNOWN operation type) as well as threads actively performing operations.

## ThreadStatus Structure

The ThreadStatus struct (see include/rocksdb/thread_status.h) contains:

| Field | Type | Description |
|-------|------|-------------|
| thread_id | uint64_t | Unique thread ID |
| thread_type | ThreadType | Thread pool type |
| db_name | string | Database path (empty if thread is idle) |
| cf_name | string | Column family name (empty if not applicable) |
| operation_type | OperationType | Current high-level operation |
| op_elapsed_micros | uint64_t | Elapsed time of current operation |
| operation_stage | OperationStage | Current stage within the operation |
| op_properties | uint64_t[] | Operation-specific properties |
| state_type | StateType | Current low-level state |

## Thread Types

| ThreadType | Description |
|------------|-------------|
| HIGH_PRIORITY | Background thread in the high-priority pool (flushes) |
| LOW_PRIORITY | Background thread in the low-priority pool (compactions) |
| USER | User thread (non-background) |
| BOTTOM_PRIORITY | Background thread in the bottom-priority pool |

## Operation Types

| OperationType | Description |
|---------------|-------------|
| OP_COMPACTION | Running a compaction job |
| OP_FLUSH | Flushing a memtable to SST |
| OP_DBOPEN | Opening a database |
| OP_GET | Executing a Get operation |
| OP_MULTIGET | Executing a MultiGet operation |
| OP_DBITERATOR | Iterator operation |
| OP_VERIFY_DB_CHECKSUM | Verifying database checksums |
| OP_VERIFY_FILE_CHECKSUMS | Verifying file checksums |
| OP_GETENTITY | Executing a GetEntity operation |
| OP_MULTIGETENTITY | Executing a MultiGetEntity operation |
| OP_GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST | Retrieving file checksums from the current MANIFEST |

## Operation Stages

Stages provide finer-grained progress tracking within an operation:

| OperationStage | Description |
|----------------|-------------|
| STAGE_FLUSH_RUN | Flush is running |
| STAGE_FLUSH_WRITE_L0 | Writing L0 output file |
| STAGE_COMPACTION_PREPARE | Preparing compaction inputs |
| STAGE_COMPACTION_RUN | Compaction is running |
| STAGE_COMPACTION_PROCESS_KV | Processing key-value pairs |
| STAGE_COMPACTION_INSTALL | Installing compaction results |
| STAGE_COMPACTION_SYNC_FILE | Syncing output files |
| STAGE_PICK_MEMTABLES_TO_FLUSH | Selecting memtables for flush |
| STAGE_MEMTABLE_ROLLBACK | Rolling back memtable |
| STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS | Installing flush results |

## Operation Properties

Operations carry type-specific properties in the op_properties array. Use ThreadStatus::InterpretOperationProperties() to decode them into named key-value pairs.

### Compaction Properties

| Property | Description |
|----------|-------------|
| COMPACTION_JOB_ID | Compaction job identifier |
| COMPACTION_INPUT_OUTPUT_LEVEL | Encoded input and output levels |
| COMPACTION_PROP_FLAGS | Compaction flags |
| COMPACTION_TOTAL_INPUT_BYTES | Total input bytes |
| COMPACTION_BYTES_READ | Bytes read so far |
| COMPACTION_BYTES_WRITTEN | Bytes written so far |

### Flush Properties

| Property | Description |
|----------|-------------|
| FLUSH_JOB_ID | Flush job identifier |
| FLUSH_BYTES_MEMTABLES | Memtable data size |
| FLUSH_BYTES_WRITTEN | Bytes written to SST so far |

## Thread States

| StateType | Description |
|-----------|-------------|
| STATE_UNKNOWN | Unknown or not tracked |
| STATE_MUTEX_WAIT | Waiting on a mutex |

## Utility Methods

The ThreadStatus struct provides static utility methods:

- GetThreadTypeName() -- human-readable thread type name
- GetOperationName() -- human-readable operation name
- GetOperationStageName() -- human-readable stage name
- GetStateName() -- human-readable state name
- MicrosToString() -- format elapsed time as human-readable string
- GetOperationPropertyName() -- name of the i-th property for a given operation type
