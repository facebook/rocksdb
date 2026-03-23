# Fix Summary: wal

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 13 |
| Completeness | 10 |
| Structure/Files | 4 |
| Style | 0 (inline backticks kept as existing convention) |

## Disagreements Found

3 disagreements documented in `wal_debates.md`:
1. **Recycling invariant direction** (medium risk) -- CC wrong, Codex right. Code enforces recyclable-after-non-recyclable rejection, not the reverse.
2. **2PC WritePrepared flow** (high risk) -- CC wrong, Codex right. WritePrepared prepare writes to WAL+MemTable, not WAL-only.
3. **TransactionLogIteratorImpl reader type** (medium risk) -- CC wrong, Codex right. Uses `log::Reader`, not `FragmentBufferedReader`.

## Changes Made

### 01_record_format.md
- Added "Recyclability Convention for Types >= 10" section explaining the bit 0 convention

### 02_writer.md
- Fixed `last_seqno_recorded_` description: clarified it records the starting sequence number of the last batch, not the highest sequence number
- Added "Metadata Record Block Alignment" section documenting `MaybeSwitchToNewBlock()`
- Added "Writer Destructor" section documenting buffer flush on destruction
- Added "Close and PublishIfClosed" section documenting lifecycle methods
- Expanded error handling to mention all subsequent writes failing after I/O error

### 03_reader.md
- Fixed `kBadRecord` error code: removed "Invalid CRC", now reads "Zero-length record, decompression failure, or other malformed record (not CRC)"
- Fixed `stop_replay_for_corruption` description: changed from "Fail immediately on corruption" to accurate description as internal recovery state
- Fixed `FragmentBufferedReader` section: corrected claim about `TransactionLogIteratorImpl` using `FragmentBufferedReader` (it uses `log::Reader`)

### 04_recovery_modes.md
- Added `paranoid_checks` caveat at the top of the section
- Expanded point-in-time recovery description with `stop_replay_for_corruption` revision and cross-CF inconsistency check details
- Added `enforce_write_buffer_manager_during_recovery` note to `avoid_flush_during_recovery` section

### 05_lifecycle.md
- Fixed SwitchWAL 2PC behavior: replaced incorrect "returns without action" with accurate two-step description (first call schedules flushes but doesn't mark WAL; subsequent calls are no-ops)

### 06_sync_and_durability.md
- Added `db/db_impl/db_impl.cc` to Files line
- Fixed group size cap description: was backwards ("exceeds 1/8" -> "at most 1/8")
- Expanded `FlushWALOptions` description with `allow_write_stall` and `rate_limiter_priority` fields
- Renamed "Two Write Queues" section to "Advanced Write Queue Modes" and split into separate subsections for pipelined write and two write queues

### 07_recycling.md
- Added note about `kPointInTimeRecovery` re-enablement (PR #12403)
- Rewrote "Reader Behavior with Recycled Logs" section to accurately describe one-directional enforcement (recyclable-after-non-recyclable rejected, not vice versa)

### 08_compression.md
- Added note that compression format version 2 is hardcoded in both writer and reader

### 09_tracking_and_verification.md
- Fixed WalAddition description: changed "recorded when a WAL is created or synced" to "recorded when an inactive WAL has been synced"
- Added caveats: live-WAL sync not tracked, not compatible with secondary instances
- Fixed WalSet::CheckWals: changed "sizes match" to "must be at least the recorded synced size"
- Fixed `last_seqno_recorded_` description: clarified it is the starting sequence number of the last logical WAL record
- Clarified predecessor WAL info record uses `MaybeSwitchToNewBlock()` for single-block alignment

### 10_filter_and_replication.md
- Added `db/transaction_log_impl.cc` to Files line
- Fixed `TransactionLogIteratorImpl` implementation description: changed from `FragmentBufferedReader` to `log::Reader` with `UnmarkEOF()` tailing
- Added `seq_per_batch` incompatibility explanation
- Added "Iterator Tailing Behavior" subsection documenting `Status::OK()` invalid state, `UnmarkEOF()` re-arming, and `Status::TryAgain()`

### 11_2pc_transactions.md
- Added `utilities/transactions/write_prepared_txn.cc`, `utilities/transactions/pessimistic_transaction.cc`, `db/db_impl/db_impl_files.cc` to Files line
- Rewrote overview to distinguish WriteCommitted from WritePrepared flows
- Added "Write Policies" table showing WAL/MemTable behavior per policy
- Fixed prepare phase: clarified WriteCommitted is WAL-only but WritePrepared is WAL+MemTable
- Fixed commit phase: clarified WriteCommitted triggers MemTable insertion but WritePrepared is WAL-only
- Added `FindMinPrepLogReferencedByMemTable()` to WAL retention description
- Fixed SwitchWAL interaction: accurate two-step behavior description

### 12_configuration.md
- Updated Files line: replaced `write_batch.h` with `db.h` and `db_impl_open.cc`
- Fixed `wal_filter` type: changed from `shared_ptr<WalFilter>` to `WalFilter*` with lifetime note
- Added missing options: `paranoid_checks`, `wal_bytes_per_sync`, `strict_bytes_per_sync`, `enforce_write_buffer_manager_during_recovery`, `background_close_inactive_wals`, `max_write_batch_group_size_bytes`
