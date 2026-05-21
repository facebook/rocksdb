# Sequence Number Assignment

**Files:** `db/version_set.h`, `db/db_impl/db_impl_write.cc`, `db/dbformat.h`

## Overview

Every key written to RocksDB is tagged with a monotonically increasing sequence number that serves as a logical timestamp. Sequence numbers enable snapshot isolation, determine key visibility, and order merge operands.

## Sequence Number Space

| Concept | Source | Range | Description |
|---------|--------|-------|-------------|
| `LastSequence` | `VersionSet::LastSequence()` | `[0, kMaxSequenceNumber]` | Highest sequence visible to readers |
| `LastAllocatedSequence` | `VersionSet::LastAllocatedSequence()` | `>= LastSequence` | Highest allocated sequence (may be ahead of `LastSequence` in two-queue or unordered write mode) |
| `LastPublishedSequence` | `VersionSet::LastPublishedSequence()` | `>= LastSequence` | Highest sequence published to readers; equals `LastSequence` in simple mode, may differ in two-queue mode |

`kMaxSequenceNumber` is `(1 << 56) - 1`, defined in `db/dbformat.h`. The sequence number occupies the upper 56 bits of the 64-bit packed tag, with the lower 8 bits storing the `ValueType`.

## Normal Write Path

In the default write mode (no pipelined or two-queue writes):

Step 1 - The leader fetches `last_sequence = versions_->LastSequence()`.

Step 2 - Compute `seq_inc`: the total number of sequence numbers consumed by the group. In `seq_per_batch_` mode, each batch consumes one sequence; otherwise, each key in each batch consumes one sequence.

Step 3 - Set `current_sequence = last_sequence + 1` and advance `last_sequence += seq_inc`.

Step 4 - The leader calls `WriteGroupToWAL()`, which stamps the sequence into the merged batch header via `WriteBatchInternal::SetSequence()` before appending the record to the WAL. The WAL record thus contains the assigned sequence number.

Step 5 - After WAL write, assign per-writer sequences by walking the write group. For each writer that did not fail its callback:
- Set `writer->sequence = next_sequence`
- Advance `next_sequence` by the writer's batch count (seq_per_batch) or key count (seq_per_key)

Step 6 - After memtable insertion completes, publish the new sequence: `versions_->SetLastSequence(last_sequence)`. This makes the new writes visible to readers.

**Design note:** Sequences are embedded in the WAL record before append so that recovery can extract correct sequence numbers during replay. Individual writer sequence assignment (for memtable insertion callbacks) happens after the WAL write returns.

## Two-Queue Path

With `two_write_queues_`, sequence allocation uses `versions_->FetchAddLastAllocatedSequence(total_count)` atomically under `wal_write_mutex_`. The `LastAllocatedSequence` may advance ahead of `LastSequence` because WAL-only writes (e.g., 2PC prepare) allocate sequences before the corresponding memtable writes publish them.

`ConcurrentWriteGroupToWAL()` performs the WAL write and sequence allocation under `wal_write_mutex_`, ensuring WAL order matches sequence order even with concurrent write groups.

## Sequence Per Batch vs Sequence Per Key

| Mode | `seq_per_batch_` | Sequence Consumption | Used By |
|------|------------------|----------------------|---------|
| Sequence per key | `false` | Each key in the batch consumes one sequence | Default mode |
| Sequence per batch | `true` | Entire batch shares one sequence number | WritePrepared and WriteUnprepared transactions |

Sequence-per-batch mode is used by pessimistic transaction policies (WritePreparedTxnDB, WriteUnpreparedTxnDB) to detect write conflicts at batch granularity rather than individual key granularity.

## WBWI Sequence Reservation

When a `WriteBatchWithIndex` (WBWI) is ingested as a memtable (used by transaction commit), additional sequence numbers are reserved. The `seq_inc` is increased by the WBWI's operation count (`GetWriteBatch()->Count()`, which counts Put/Delete/Merge/SingleDelete operations, not distinct keys) to ensure each ingested entry gets a unique sequence number. During recovery, transactions do not commit by WBWI ingestion, so these reserved sequences prevent conflicts between different transactions' entries.

## Visibility Publishing

The separation between sequence allocation and visibility publishing is crucial:

- `SetLastSequence()`: Called after memtable insertion completes successfully. Makes writes visible to snapshot readers.
- `FetchAddLastAllocatedSequence()`: Atomically allocates a range of sequence numbers. Used in two-queue mode, unordered write mode, and the recoverable-state path -- any write path where sequence allocation must be atomic relative to concurrent writers.

If memtable insertion fails (partial batch write), the sequence is not published to prevent readers from seeing incomplete data. The `HandleMemTableInsertFailure()` function records the failure, and the WAL may be truncated to remove the failed write (see [Crash Recovery](09_crash_recovery.md)).

## Cross-Component Sequence Usage

| Consumer | How Sequence Is Used |
|----------|---------------------|
| Snapshot isolation | Readers only see keys with `seq <= snapshot_seq` |
| Merge ordering | Merge operands are applied newest-to-oldest by sequence number |
| Compaction | Determines which key versions can be dropped |
| WAL replay | Assigns correct sequences during crash recovery |
| Tombstone visibility | A tombstone at `seq_t` covers keys with `seq < seq_t` |
