# WriteThread and Group Commit

**Files:** `db/write_thread.h`, `db/write_thread.cc`

## Overview

`WriteThread` coordinates concurrent writers using lock-free leader election and write batching. One thread (the leader) performs WAL and memtable writes on behalf of a group of writers, amortizing synchronization and fsync overhead. The design minimizes latency through an adaptive three-phase wait strategy.

## Writer State Machine

Each writer passes through states defined in `WriteThread` (see `db/write_thread.h`):

| State | Value | Description |
|-------|-------|-------------|
| `STATE_INIT` | 1 | Initial state before joining the queue |
| `STATE_GROUP_LEADER` | 2 | Elected as the leader of a write group |
| `STATE_MEMTABLE_WRITER_LEADER` | 4 | Leader of the memtable write phase (pipelined mode) |
| `STATE_PARALLEL_MEMTABLE_WRITER` | 8 | Participant in parallel memtable insertion |
| `STATE_COMPLETED` | 16 | Write finished; result available |
| `STATE_LOCKED_WAITING` | 32 | Blocked on mutex+condvar (escalated from spinning) |
| `STATE_PARALLEL_MEMTABLE_CALLER` | 64 | Stride leader in large parallel write groups |

## Writer Struct

The `Writer` struct (see `db/write_thread.h`) encapsulates a pending write request:

| Field | Type | Purpose |
|-------|------|---------|
| `batch` | `WriteBatch*` | The batch to write |
| `sync` | `bool` | Requires fsync after WAL |
| `no_slowdown` | `bool` | Fail immediately if stalled |
| `disable_wal` | `bool` | Skip WAL (non-durable) |
| `disable_memtable` | `bool` | WAL-only write (2PC prepare) |
| `rate_limiter_priority` | `Env::IOPriority` | Rate limiter priority |
| `protection_bytes_per_key` | `size_t` | Per-key integrity bytes |
| `state` | `atomic<uint8_t>` | Current state in the state machine |
| `sequence` | `SequenceNumber` | Assigned sequence number |
| `status` | `Status` | Result status |
| `link_older` / `link_newer` | `Writer*` | Intrusive linked list pointers |

## Lock-Free Enqueue

`WriteThread::LinkOne()` uses a CAS loop on the `newest_writer_` atomic pointer to enqueue a writer. If the queue was empty (previous value was `nullptr`), the caller becomes the leader immediately. Otherwise, the writer links itself behind the current newest writer and waits for state transition.

This design ensures that exactly one writer wins the leader role per group without any mutex acquisition.

## Group Formation

Once a leader is elected, `WriteThread::EnterAsBatchGroupLeader()` walks the linked list from the leader (oldest enqueued) to the current `newest_writer_`, collecting compatible writers into a `WriteGroup`.

**Compatibility criteria** (a follower is excluded if any condition fails):

- Sync compatibility: a sync follower cannot join a non-sync leader
- Must have the same `disable_wal`, `protection_bytes_per_key`, and `rate_limiter_priority`
- Must have the same `no_slowdown` setting
- Batch must be non-null
- No callback that disallows batching (`AllowWriteBatching() == false`)
- Not an unbatched WriteBatchWithIndex ingestion
- Total size does not exceed `max_write_batch_group_size_bytes` (see `DBOptions` in `include/rocksdb/options.h`)

**Size limiting heuristic:** If the leader's batch is small (at most `max_size / 8`), the group size is capped at `leader_size + max_size / 8`. This prevents small writes from being penalized by waiting for a large batch to accumulate. Incompatible writers are spliced out of the group and re-linked for the next leader.

## Adaptive Wait

`WriteThread::AwaitState()` implements a three-phase wait to minimize latency:

**Phase 1 - Spin (200 iterations):** Executes `AsmVolatilePause()` (CPU pause instruction). Total spin time is approximately 1 microsecond on modern hardware (200 iterations of ~7ns each on Xeon CPUs). This catches very short waits without context-switch overhead.

**Phase 2 - Yield (adaptive):** Calls `std::this_thread::yield()` for up to `max_yield_usec_` (default 100 microseconds). Two adaptation mechanisms control this phase:

1. **Within-phase cutoff:** If 3 consecutive yields take longer than `slow_yield_usec_` (`kMaxSlowYieldsWhileSpinning = 3`), the yield loop breaks early and falls through to blocking.
2. **Cross-invocation adaptation:** `AdaptationContext` maintains a `yield_credit` counter with exponential decay (decay constant 1/1024). Each yield outcome adjusts the credit: effective yields increase it, slow yields decrease it. When `yield_credit < 0`, future invocations skip the yield phase entirely and proceed directly to condvar blocking.

**Phase 3 - Block:** Creates a per-writer mutex+condvar, atomically transitions state to `STATE_LOCKED_WAITING`, and waits on the condvar. The corresponding `SetState()` checks for `STATE_LOCKED_WAITING` and uses the mutex+notify path to prevent lost wakeups.

Note: `SetState()` must check whether the writer is in `STATE_LOCKED_WAITING` to decide whether to use condvar notification. This prevents a race where the writer transitions to `LOCKED_WAITING` after the leader has already set the completion state.

## Parallel Memtable Writes

When `allow_concurrent_memtable_write` is enabled and the write group has more than one writer (and no merge operations), memtable insertion can proceed in parallel.

`WriteThread::LaunchParallelMemTableWriters()` transitions each writer in the group to `STATE_PARALLEL_MEMTABLE_WRITER`. Each writer then inserts its own batch into the memtable concurrently.

**Large group optimization (O(sqrt(n)) two-level scheme):** For groups with 20 or more writers, the system selects `sqrt(group_size)` stride leaders, each set to `STATE_PARALLEL_MEMTABLE_CALLER`. Each stride leader wakes its stride of workers via `SetMemWritersEachStride()`, reducing the serial wake-up overhead from O(n) to O(sqrt(n)).

`CompleteParallelMemTableWriter()` atomically decrements the `running` counter. Exactly one thread observes `running == 0` and returns `true`, making it responsible for publishing the last sequence number and exiting the group.

## Write Stall Barrier

When write stalls are active (from `WriteController` or `WriteBufferManager`), the write thread sets up a barrier via `BeginWriteStall()`:

1. Links `write_stall_dummy_` (a sentinel Writer node with `batch == nullptr`) into the `newest_writer_` queue via `LinkOne()`
2. Walks the existing writer queue backward from the dummy. For each pending writer not yet in a write group:
   - Writers with `no_slowdown` are unlinked and completed with `Status::Incomplete("Write stall")`
   - Other writers remain queued behind the dummy
3. Subsequent writers enqueuing via `JoinBatchGroup()` link behind the dummy and block on `stall_cv_` (condvar protected by `stall_mu_`)
4. `EndWriteStall()` atomically unlinks `write_stall_dummy_` from the queue and signals all blocked writers via `stall_cv_.notify_all()`

The dummy node blocks the queue because no leader will process it as a follower (its `batch` is `nullptr`). This prevents new writers from accumulating during a stall, which would cause a thundering-herd problem when the stall clears.
