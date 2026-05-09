# Write Modes

**Files:** `db/db_impl/db_impl_write.cc`, `db/write_thread.h`, `db/write_thread.cc`, `include/rocksdb/options.h`

## Overview

RocksDB supports four write modes that trade ordering guarantees, concurrency, and latency. The mode is selected at DB open time via `DBOptions` and cannot be changed at runtime.

## Mode Comparison

| Mode | Config | WAL Queue | MemTable Queue | Concurrency | Use Case |
|------|--------|-----------|----------------|-------------|----------|
| Normal (batched) | Default | `write_thread_` | Same leader | Leader handles WAL + memtable for entire group | General purpose |
| Pipelined | `enable_pipelined_write` | `write_thread_` | `newest_memtable_writer_` | WAL and memtable groups overlap | Higher throughput with ordering |
| Two-queue | `two_write_queues` | `nonmem_write_thread_` | `write_thread_` | WAL-only and memtable writes decoupled | WritePrepared/WriteUnprepared transactions |
| Unordered | `unordered_write` | `write_thread_` | Per-writer (independent) | No ordering between writers | Maximum throughput, relaxed consistency |

## Normal Batched Write

The default mode. `DBImpl::WriteImpl()` drives the entire write group through a single pipeline:

Step 1 - Writer joins `write_thread_` via `JoinBatchGroup()`. Either becomes leader or waits.

Step 2 - Leader calls `PreprocessWrite()` (flow control, flush scheduling, stall checks).

Step 3 - Leader forms the write group via `EnterAsBatchGroupLeader()`.

Step 4 - Leader writes all batches to WAL via `WriteGroupToWAL()`.

Step 5 - Leader assigns sequence numbers to each writer in the group.

Step 6 - If parallel memtable writes are possible (no merge operations, `allow_concurrent_memtable_write` enabled, group size > 1): launch parallel memtable writers. Otherwise, the leader serially inserts all batches.

Step 7 - After all memtable insertions complete, publish the last sequence and exit the group.

**Tradeoff:** Simple and correct, but the next WAL write group cannot start until the current group finishes memtable insertion.

## Pipelined Write

`DBImpl::PipelinedWriteImpl()` overlaps WAL writing and memtable insertion by using two separate leader elections:

**WAL Phase:**

Step 1 - Writer joins `write_thread_` and becomes WAL leader.

Step 2 - If the previous memtable write group is still running, wait via `WaitForMemTableWriters()` (only if the writer's callback disallows batching).

Step 3 - Leader calls `PreprocessWrite()`, forms the WAL write group, assigns sequences, and writes to WAL.

Step 4 - Leader exits the WAL group, linking eligible writers (those needing memtable writes) onto the `newest_memtable_writer_` queue.

**MemTable Phase:**

Step 5 - One writer from the memtable queue is promoted to `STATE_MEMTABLE_WRITER_LEADER` via `EnterAsMemTableWriter()`. Other writers remain queued as followers or parallel memtable writers.

Step 6 - The memtable leader calls `EnterAsMemTableWriter()` to form the memtable write group.

Step 7 - If concurrent memtable writes are possible, launch parallel memtable writers. Otherwise, insert serially.

Step 8 - After completion, publish the last sequence and exit.

**Key benefit:** While the memtable phase of group N is executing, the WAL phase of group N+1 can proceed concurrently. This significantly improves throughput when both WAL and memtable operations are CPU-bound.

**Implementation detail:** When the WAL leader exits via `ExitAsBatchGroupLeader()`, it inserts a dummy Writer before the memtable write group to prevent the next WAL leader from overtaking and adding to the memtable writer list out of order. The dummy is unlinked once the memtable group is properly formed.

**Restrictions:** Pipelined write is incompatible with `two_write_queues`, `seq_per_batch`, `unordered_write`, and `post_memtable_callback`.

## Two-Queue Write

Two-queue mode (`two_write_queues`) decouples WAL-only writes from memtable writes using two independent `WriteThread` instances:

- `write_thread_`: Handles memtable writes (and WAL writes in the normal path)
- `nonmem_write_thread_`: Handles WAL-only writes (e.g., 2PC prepare)

**WAL-only path** (`WriteImplWALOnly()`):

When `disable_memtable` is true, the write is routed to `nonmem_write_thread_`. The writer joins this queue, and the leader writes all batches to WAL under `wal_write_mutex_`. Sequence numbers are allocated atomically via `FetchAddLastAllocatedSequence()`.

**Normal path with two queues:**

When a write needs both WAL and memtable, it uses `write_thread_` as usual, but WAL writes go through `ConcurrentWriteGroupToWAL()` which acquires `wal_write_mutex_` to coordinate with the non-memtable queue.

**Primary use case:** WritePreparedTxnDB and WriteUnpreparedTxnDB, where prepare writes go to WAL only (via the non-memtable queue) and commit writes go to both WAL and memtable.

## Unordered Write

`unordered_write` provides maximum write throughput by removing ordering guarantees between writers:

Step 1 - WAL write happens through `WriteImplWALOnly()` on `write_thread_`, which still groups and orders WAL writes.

Step 2 - After WAL write completes, each writer independently inserts into the memtable via `UnorderedWriteMemtable()`. There is no grouping or coordination between memtable writers.

Step 3 - `pending_memtable_writes_` tracks the count of outstanding memtable writes. When it reaches zero, `switch_cv_` is signaled to allow memtable switches to proceed.

**Tradeoff:** Highest throughput because memtable insertions proceed without any ordering between writers. The key relaxation is that `LastSequence` is published before all memtable inserts for those sequences have completed. This means a snapshot or iterator may observe additional lower-sequence writes appearing after the snapshot was taken, violating the normal snapshot immutability guarantee. The WAL still records the correct sequence order for recovery. `pending_memtable_writes_` tracks outstanding inserts, and `switch_cv_` blocks memtable switches until those inserts finish, ensuring memtable switch safety.

**Restrictions:** Incompatible with `enable_pipelined_write` and WBWI ingestion.

## Choosing a Write Mode

| Consideration | Recommended Mode |
|--------------|-----------------|
| General use, correctness first | Normal (default) |
| High write throughput with ordering | Pipelined |
| WritePrepared/WriteUnprepared transactions | Two-queue |
| Maximum throughput, can tolerate relaxed ordering | Unordered |
| Need `post_memtable_callback` | Normal only |
