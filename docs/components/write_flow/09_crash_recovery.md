# Crash Recovery

**Files:** `db/db_impl/db_impl_open.cc`, `db/wal_manager.h`, `db/log_reader.h`, `db/log_reader.cc`

## Overview

When RocksDB opens a database, it recovers un-flushed writes by replaying WAL records into fresh memtables. The recovery process relies on the WAL-before-memtable invariant: every write that reached the memtable was first persisted in the WAL.

## Recovery Flow

Step 1 - **Read MANIFEST**: `VersionSet::Recover()` reads the MANIFEST file to reconstruct the LSM state (which SST files exist, their levels, and the last flushed sequence number).

Step 2 - **Scan WAL directory**: Find all `.log` files, sorted by log number. Filter out stale WALs based on the MANIFEST state. If `track_and_verify_wals_in_manifest` is enabled, `WalSet` provides additional integrity verification.

Step 3 - **Replay each WAL** (in log_number order): For each WAL file:
- `log::Reader::ReadRecord()` reassembles logical records from 32 KB block fragments
- CRC verification detects corruption
- For recyclable headers, log number verification detects stale data from previous file incarnations
- Decompression is applied if WAL compression was active
- `WriteBatchInternal::InsertInto(batch, memtable)` replays each record into a fresh memtable
- If `enforce_write_buffer_manager_during_recovery` is true (default: true) and `WriteBufferManager::ShouldFlush()` triggers, a mid-recovery flush may occur, writing recovered memtables to L0 SSTs during replay

Step 4 - **Flush or restore**: The end-of-recovery behavior depends on several conditions:
- If a mid-recovery flush already occurred, remaining recovered memtables are flushed to SST files regardless of other options
- If `avoid_flush_during_recovery` is false (default): recovered memtables are flushed to SST files, establishing a clean state
- If `avoid_flush_during_recovery` is true and no mid-recovery flush occurred: memtables are kept unflushed and `RestoreAliveLogFiles()` preserves the WAL files for future recovery
- In read-only mode: no flush or WAL truncation occurs

Step 5 - **Delete obsolete WALs**: WAL files whose data has been fully flushed to SST are removed (or recycled). If memtables were not flushed (due to `avoid_flush_during_recovery`), WALs are preserved.

**Key Invariant:** WAL replay order matches write order because WAL files are processed in `log_number` order, which corresponds to the sequence number order of writes.

## WAL Truncation on Memtable Failure

If memtable insertion fails during normal write (after the WAL write succeeds), `logs_.back().SetAttemptTruncateSize()` records the WAL file size before the failed write. On the next recovery attempt, the WAL is truncated to this size, discarding the record that could not be applied to the memtable.

This mechanism handles the edge case where a write is durably persisted in the WAL but cannot be applied to the memtable (e.g., due to memory allocation failure). Without truncation, recovery would repeatedly fail on the same record.

## Corruption Handling

The WAL reader handles several types of corruption:

| Corruption Type | Detection | Behavior |
|----------------|-----------|----------|
| CRC mismatch | `crc32c` verification on each physical record | Record dropped, error reported |
| Stale data in recycled file | Log number mismatch in recyclable header | Record ignored (expected for recycled files) |
| Fragment ordering error | Out-of-sequence `kFirst`/`kMiddle`/`kLast` | Error reported, partial record dropped |
| Truncated record | Incomplete header or payload at end of file | May indicate clean shutdown (partial write); behavior controlled by `WALRecoveryMode` |

## WAL Recovery Modes

`WALRecoveryMode` (see `DBOptions` in `include/rocksdb/options.h`) controls how aggressively the reader tolerates corruption:

| Mode | Behavior |
|------|----------|
| `kPointInTimeRecovery` (default) | Stop at the first corruption; all records before it are valid |
| `kTolerateCorruptedTailRecords` | Tolerate incomplete records at the tail of the WAL (common after unclean shutdown) |
| `kAbsoluteConsistency` | Report error on any corruption, even at the tail |
| `kSkipAnyCorruptedRecords` | Skip corrupted records and continue; use with caution |

## WAL Chain Verification

When `track_and_verify_wals` is enabled, each new WAL file records information about its predecessor via `PredecessorWALInfoType` records (or `kRecyclePredecessorWALInfoType` for recycled files). This creates a verifiable chain of WAL files:

- Each WAL stores the predecessor's log number, file size, and last sequence number
- During recovery, these can be verified to detect missing or truncated WAL files
- This provides defense against filesystem-level corruption that deletes or renames WAL files

This is separate from `track_and_verify_wals_in_manifest`, which tracks WAL metadata (synced sizes of closed WALs) in the MANIFEST file. See [Write-Ahead Log](03_wal.md) for details on both mechanisms.

## Interaction with Other Features

| Feature | Recovery Behavior |
|---------|-------------------|
| Multiple column families | WAL records may interleave writes to different CFs; recovery routes each to the correct memtable |
| User-defined timestamps | `kUserDefinedTimestampSizeType` meta-records in the WAL store per-CF timestamp sizes for correct decoding |
| WAL compression | `kSetCompressionType` meta-records activate streaming decompression during recovery |
| Atomic flush | Affects MANIFEST AtomicGroup semantics during recovery; does not provide a special atomic recovery flush path |
