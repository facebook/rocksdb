# Review: wal — Codex

## Summary
Overall quality rating: significant issues

The WAL docs cover a broad surface area and the chapter split is sensible. The index is the right size, the lifecycle/purge material points readers toward the correct subsystems, and the docs at least attempt to cover newer features like WAL compression, recyclable records, predecessor-WAL verification, and user-defined timestamp metadata.

The main problem is accuracy. Several chapters describe the wrong code path entirely, especially replication/tailing, 2PC, and MANIFEST/WAL verification. The configuration chapter also claims to be a complete reference but omits multiple WAL-affecting options added or exercised in 2024-2026 code and tests. The docs need a pass from someone reading the current implementation, not just the APIs.

## Correctness Issues

### [WRONG] 2PC chapter reverses the WritePrepared WAL/memtable flow
- **File:** `docs/components/wal/11_2pc_transactions.md` — Overview, Prepare Phase, Commit Phase
- **Claim:** "the prepare phase writes to the WAL only (deferring MemTable insertion to the commit phase)." / "The prepare phase ... is written via `WriteImpl()` with `disable_memtable=true`" / "The commit triggers MemTable insertion of the prepared data."
- **Reality:** WritePrepared prepare writes to both WAL and memtable. `WritePreparedTxn::PrepareInternal()` passes `!DISABLE_MEMTABLE`, i.e. memtable writes are enabled. The common commit path is often WAL-only: `WritePreparedTxn::CommitInternal()` computes `disable_memtable = !includes_data`, so a pure commit marker does not insert the prepared data into the memtable.
- **Source:** `utilities/transactions/write_prepared_txn.cc` (`WritePreparedTxn::PrepareInternal`, `WritePreparedTxn::CommitInternal`), `utilities/transactions/write_prepared_txn.h` (phase walkthrough comments)
- **Fix:** Rewrite the chapter around the actual WritePrepared flow and explicitly separate WritePrepared from WriteUnprepared. Do not describe a universal "prepare=WAL only, commit=memtable insert" model.

### [WRONG] Replication docs claim `TransactionLogIteratorImpl` uses `FragmentBufferedReader`
- **File:** `docs/components/wal/03_reader.md` — FragmentBufferedReader
- **Claim:** "This reader is used by `TransactionLogIteratorImpl` for tailing WAL files during replication."
- **Reality:** `TransactionLogIteratorImpl` stores `std::unique_ptr<log::Reader>` and opens a plain `log::Reader`, not `FragmentBufferedReader`. Tailing is implemented by checking `IsEOF()` and then calling `UnmarkEOF()` on the regular reader.
- **Source:** `db/transaction_log_impl.h` (`current_log_reader_`), `db/transaction_log_impl.cc` (`TransactionLogIteratorImpl::OpenLogReader`, `TransactionLogIteratorImpl::NextImpl`)
- **Fix:** Update the section to describe `FragmentBufferedReader` as a specialized reader, but not the one currently wired into `TransactionLogIteratorImpl`.

### [WRONG] Replication chapter repeats the same incorrect reader implementation
- **File:** `docs/components/wal/10_filter_and_replication.md` — Implementation
- **Claim:** "Create a `TransactionLogIteratorImpl` that uses `FragmentBufferedReader` to tail WAL files sequentially."
- **Reality:** Same issue as above: `TransactionLogIteratorImpl` uses `log::Reader`.
- **Source:** `db/transaction_log_impl.cc` (`TransactionLogIteratorImpl::OpenLogReader`, `TransactionLogIteratorImpl::NextImpl`)
- **Fix:** Replace this with the real control flow: `GetUpdatesSince()` builds `TransactionLogIteratorImpl`, which opens `log::Reader`, uses `RestrictedRead()`, and re-checks EOF via `UnmarkEOF()`.

### [WRONG] MANIFEST tracking chapter says `WalAddition` is recorded on WAL creation
- **File:** `docs/components/wal/09_tracking_and_verification.md` — MANIFEST-Based WAL Tracking / How It Works
- **Claim:** "`WalAddition`: Recorded when a WAL is created or synced."
- **Reality:** Current code records `WalAddition` only when an inactive WAL has been synced and has a non-zero pre-sync size. WAL creation itself is not recorded. The API comment also explicitly says only syncing closed WALs are tracked; syncing the live WAL through `DB::SyncWAL()` or `WriteOptions::sync=true` is intentionally not tracked.
- **Source:** `include/rocksdb/options.h` (`track_and_verify_wals_in_manifest` comment), `db/db_impl/db_impl.cc` (`DBImpl::MarkLogsSynced`)
- **Fix:** State that MANIFEST tracking records synced size for inactive/closed WALs only, and that live-WAL sync is intentionally excluded.

### [WRONG] `WalSet::CheckWals()` is documented as requiring exact size equality
- **File:** `docs/components/wal/09_tracking_and_verification.md` — Verification at Open Time
- **Claim:** "`WalSet::CheckWals()` verifies: ... Synced WAL sizes match the recorded sizes"
- **Reality:** The code only rejects WALs whose on-disk size is smaller than the recorded synced size. Larger files are allowed, which matters for unsynced tails and preallocation.
- **Source:** `db/wal_edit.cc` (`WalSet::CheckWals`)
- **Fix:** Change "match" to "must be at least the recorded synced size."

### [WRONG] Reader error-code table misdescribes `kBadRecord`
- **File:** `docs/components/wal/03_reader.md` — Corruption Reporting
- **Claim:** "`kBadRecord` | Invalid CRC or zero-length record"
- **Reality:** Invalid CRC returns `kBadRecordChecksum`, not `kBadRecord`. `kBadRecord` is a broader catch-all used for zero-length records, recycled/non-recycled layout violations, decompression failures, etc.
- **Source:** `db/log_reader.cc` (`Reader::ReadPhysicalRecord`, `FragmentBufferedReader::TryReadFragment`)
- **Fix:** Update the table so `kBadRecordChecksum` owns CRC mismatches and `kBadRecord` is described as a generic malformed-record code.

### [WRONG] Reader constructor table assigns the wrong meaning to `stop_replay_for_corruption`
- **File:** `docs/components/wal/03_reader.md` — Reader Construction
- **Claim:** "`stop_replay_for_corruption` | Fail immediately on corruption"
- **Reality:** This flag is not a generic fail-fast switch. In `log::Reader` it is only consulted by predecessor-WAL verification to suppress cascading WAL-hole checks once higher layers have already decided replay should stop.
- **Source:** `db/log_reader.h` (`stop_replay_for_corruption_` member), `db/log_reader.cc` (`Reader::MaybeVerifyPredecessorWALInfo`)
- **Fix:** Describe it as higher-layer state used by WAL-chain verification, not as a standalone recovery policy.

### [WRONG] Recycling chapter invents a reader invariant the code does not enforce
- **File:** `docs/components/wal/07_recycling.md` — Reader Behavior with Recycled Logs
- **Claim:** "All subsequent records must also be recyclable (a non-recyclable record after a recyclable one is treated as `kBadRecord`)."
- **Reality:** The implementation enforces the opposite transition: if a recyclable record appears after the reader has already established the file as non-recycled, it returns `kBadRecord`. It does not reject a later legacy record solely because `recycled_` is already true.
- **Source:** `db/log_reader.cc` (`Reader::ReadPhysicalRecord`, `FragmentBufferedReader::TryReadFragment`)
- **Fix:** Document the actual enforcement, or avoid claiming an invariant that is not checked.

### [WRONG] WAL lifecycle chapter describes the wrong `SwitchWAL()` behavior with outstanding prepared transactions
- **File:** `docs/components/wal/05_lifecycle.md` — SwitchWAL
- **Claim:** "If `allow_2pc` is true, check whether the oldest WAL contains uncommitted prepared transactions. If so, the WAL cannot be released and the function returns without action."
- **Reality:** On the first encounter, `SwitchWAL()` sets `unable_to_release_oldest_log_ = true` but still switches memtables and schedules flushes for the affected column families. Only subsequent calls return immediately with no action if the oldest WAL is still blocked by the same prepared transaction.
- **Source:** `db/db_impl/db_impl_write.cc` (`DBImpl::SwitchWAL`)
- **Fix:** Describe the two-step behavior: first call attempts flushes; later calls become no-ops until the prepared transaction is resolved.

### [WRONG] Configuration chapter gives the wrong type for `DBOptions::wal_filter`
- **File:** `docs/components/wal/12_configuration.md` — DBOptions (WAL Behavior)
- **Claim:** "| `wal_filter` | `shared_ptr<WalFilter>` | nullptr | No | ..."
- **Reality:** The field is a raw pointer, `WalFilter* wal_filter = nullptr`.
- **Source:** `include/rocksdb/options.h` (`DBOptions::wal_filter`)
- **Fix:** Update the type and mention the lifetime implication: the filter object must outlive recovery / DB open.

### [MISLEADING] `last_seqno_recorded_` is documented as the last sequence written, but the code records the start sequence of the last WAL record
- **File:** `docs/components/wal/09_tracking_and_verification.md` — PredecessorWALInfo
- **Claim:** "`last_seqno_recorded_`: The last sequence number written to the predecessor WAL"
- **Reality:** The writer updates this field from the `seqno` argument passed to `Writer::AddRecord()`. The write path passes the `WriteBatch` sequence stored in the WAL record header, i.e. the batch's starting sequence number. Recovery compares against `WriteBatchInternal::Sequence()` from the last record it read, which is the same start-sequence notion.
- **Source:** `db/log_writer.cc` (`Writer::AddRecord`), `db/db_impl/db_impl_write.cc` (`WriteToWAL`), `db/db_impl/db_impl_open.cc` (`ProcessLogRecord`), `db/dbformat.h` (`PredecessorWALInfo`)
- **Fix:** Document the current semantics precisely, or rename the concept in the docs to "starting sequence number of the last logical WAL record." Do not call it the last per-key sequence number.

### [MISLEADING] Recovery-mode chapter ignores the `paranoid_checks` gate
- **File:** `docs/components/wal/04_recovery_modes.md` — WALRecoveryMode Enum / Recovery Behavior by Mode
- **Claim:** "`kAbsoluteConsistency` ... Any corruption ... causes `DB::Open()` to fail with an error." / "`kPointInTimeRecovery` ... Stops WAL replay at the first detected corruption"
- **Reality:** Those statements are only reliable when `paranoid_checks=true`. `InitializeLogReader()` sets `reporter->status = nullptr` when `paranoid_checks=false`, so many reader-detected corruptions are logged and skipped instead of surfacing as an open failure. The recovery code still verifies checksums, but the status propagation path changes.
- **Source:** `db/db_impl/db_impl_open.cc` (`InitializeLogReader`), `db/log_reader.cc` (`Reader::ReadRecord`), `db/corruption_test.cc` (`CorruptionTest.Recovery`)
- **Fix:** Add an explicit caveat that the chapter's recovery-mode behavior assumes `DBOptions::paranoid_checks=true`, and move `paranoid_checks` into the configuration chapter.

## Completeness Gaps

### Recovery-affecting options missing from the "complete" configuration reference
- **Why it matters:** `12_configuration.md` claims to be a complete reference, but it omits options and API structs that materially change WAL behavior and recovery results.
- **Where to look:** `include/rocksdb/options.h` (`paranoid_checks`, `wal_bytes_per_sync`, `strict_bytes_per_sync`, `enforce_write_buffer_manager_during_recovery`, `background_close_inactive_wals`, `max_write_batch_group_size_bytes`), `include/rocksdb/options.h` (`FlushWALOptions`)
- **Suggested scope:** Expand chapter 12. At minimum add `paranoid_checks`, `enforce_write_buffer_manager_during_recovery`, `wal_bytes_per_sync`, `strict_bytes_per_sync`, `background_close_inactive_wals`, `max_write_batch_group_size_bytes`, and `FlushWALOptions::rate_limiter_priority`.

### User-defined timestamp reconciliation during WAL recovery is undocumented
- **Why it matters:** The docs mention timestamp-size metadata records, but not the recovery-time rewrite path that reconciles on-disk WAL timestamp sizes with the current running column-family configuration. This is a real behavioral contract, not an implementation detail.
- **Where to look:** `db/db_impl/db_impl_open.cc` (`InitializeWriteBatchForLogRecord`), `util/udt_util.h` (`HandleWriteBatchTimestampSizeDifference` comment), `db/db_wal_test.cc` (`DBWALTestWithTimestamp.EnableDisableUDT`)
- **Suggested scope:** Add a subsection to chapter 3 or chapter 11 explaining how recovery pads or strips timestamps when the feature is toggled across restarts.

### Recovery docs omit the WriteBufferManager-enforced flush path
- **Why it matters:** `avoid_flush_during_recovery=true` is not absolute anymore. With `enforce_write_buffer_manager_during_recovery=true`, recovery can still schedule flushes to avoid OOM, and once any such flush happens, remaining memtables are flushed at the end as well.
- **Where to look:** `include/rocksdb/options.h` (`enforce_write_buffer_manager_during_recovery`), `db/db_impl/db_impl_open.cc` (`InsertLogRecordToMemtable`, `MaybeFlushFinalMemtableOrRestoreActiveLogFiles`), `db/db_write_buffer_manager_test.cc`
- **Suggested scope:** Add this to chapters 4 and 12, and cross-link it from the `avoid_flush_during_recovery` discussion.

### Read-only WAL recovery and read-only API limits are not covered
- **Why it matters:** The docs describe recovery and WAL APIs as if they are always available on a read-write DB. In read-only open, recovery never flushes memtables, and APIs like `FlushWAL()` are `NotSupported`.
- **Where to look:** `db/db_impl/db_impl_open.cc` (`MaybeWriteLevel0TableForRecovery`, `MaybeFlushFinalMemtableOrRestoreActiveLogFiles`), `db/db_basic_test.cc` (`ReadOnlyDBFlushWAL`)
- **Suggested scope:** Brief mention in chapters 4, 6, and 12.

### MANIFEST tracking caveats are missing
- **Why it matters:** The option comment includes caveats that affect how operators interpret failures: it does not work with secondary instances, and live-WAL sync is intentionally not tracked.
- **Where to look:** `include/rocksdb/options.h` (`track_and_verify_wals_in_manifest` comment)
- **Suggested scope:** Add the caveats to chapters 9 and 12.

## Depth Issues

### Recovery-mode chapter flattens the point-in-time logic too much
- **Current:** Chapter 4 says higher layers decide whether corruption is fatal based on whether it is in the last WAL and whether there is no hole in recovered data.
- **Missing:** The actual code also revises `stop_replay_for_corruption` when sequence numbers remain consecutive, and separately checks cross-column-family inconsistency using CF log numbers and live SST size.
- **Source:** `db/db_impl/db_impl_open.cc` (`MaybeReviseStopReplayForCorruption`, `MaybeHandleStopReplayForCorruptionForInconsistency`)

### Replication chapter does not explain iterator tailing and invalid states
- **Current:** Chapter 10 presents `GetUpdatesSince()` as a straightforward iterator over WAL records.
- **Missing:** `TransactionLogIteratorImpl` has nuanced behavior at the live tail: it can become invalid with `Status::OK()`, recover via `UnmarkEOF()` when new data arrives, or return `Status::TryAgain()` when continuity is lost and a new iterator is required.
- **Source:** `db/transaction_log_impl.cc` (`RestrictedRead`, `NextImpl`, `SeekToStartSequence`), `db/db_log_iter_test.cc` (`TransactionLogIteratorStallAtLastRecord`)

### 2PC WAL retention section omits the memtable-side prepare references
- **Current:** Chapter 11 reduces retention to live memtables plus `logs_with_prep_tracker_`.
- **Missing:** The 2PC retention code also considers prepared sections already referenced from mutable and immutable memtables via `FindMinPrepLogReferencedByMemTable()`.
- **Source:** `db/db_impl/db_impl_files.cc` (`PrecomputeMinLogNumberToKeep2PC`, `FindMinPrepLogReferencedByMemTable`)

## Structure and Style Violations

### Inaccurate `Files:` lines
- **File:** `docs/components/wal/06_sync_and_durability.md`
- **Details:** The chapter discusses `FlushWAL()`, `SyncWAL()`, `LockWAL()`, and `UnlockWAL()`, but the implementation lives in `db/db_impl/db_impl.cc`, which is not listed.

### Inaccurate `Files:` lines
- **File:** `docs/components/wal/10_filter_and_replication.md`
- **Details:** The chapter discusses `TransactionLogIteratorImpl`, but omits `db/transaction_log_impl.cc`, where the real reader/tailing logic lives.

### Inaccurate `Files:` lines
- **File:** `docs/components/wal/11_2pc_transactions.md`
- **Details:** The chapter's behavior is primarily implemented in `utilities/transactions/write_prepared_txn.h`, `utilities/transactions/write_prepared_txn.cc`, and related transaction DB files, none of which are listed.

### Inaccurate `Files:` lines
- **File:** `docs/components/wal/12_configuration.md`
- **Details:** The chapter lists `include/rocksdb/write_batch.h`, which is not the primary source for the documented options, while omitting `include/rocksdb/db.h` and `db/db_impl/db_impl_open.cc` for API/sanitization behavior.

### Inline-code rule is violated throughout the doc set
- **File:** `docs/components/wal/index.md` and all chapter files
- **Details:** The docs use inline backticks heavily for options, types, file names, and functions, even though the stated house rule for these component docs is "NO inline code quotes."

## Undocumented Complexity

### Recovery-time timestamp-size rewriting
- **What it is:** WAL recovery can rewrite `WriteBatch` contents to reconcile recorded timestamp sizes with the current running CF configuration, including padding min timestamps or stripping timestamps when the feature is toggled across restarts.
- **Why it matters:** This is exactly the kind of non-obvious behavior someone debugging timestamp reads or recovery regressions needs to know about.
- **Key source:** `db/db_impl/db_impl_open.cc` (`InitializeWriteBatchForLogRecord`), `util/udt_util.h`
- **Suggested placement:** Add to chapter 3, with a short cross-link from chapter 11 if the transaction path also mentions UDTs.

### WriteBufferManager can override "avoid flush during recovery"
- **What it is:** Recovery can schedule flushes even with `avoid_flush_during_recovery=true` when global WBM limits are exceeded, and a single such flush flips the end-of-recovery behavior for the remaining memtables.
- **Why it matters:** Without this, operators will misread recovery performance/memory behavior and assume the option is broken.
- **Key source:** `include/rocksdb/options.h` (`enforce_write_buffer_manager_during_recovery`), `db/db_impl/db_impl_open.cc` (`InsertLogRecordToMemtable`)
- **Suggested placement:** Chapters 4 and 12.

### Replication tailing is built on `Reader::UnmarkEOF()`, not a special tail reader
- **What it is:** `TransactionLogIteratorImpl` keeps using the same plain `log::Reader` and re-arms it at EOF via `UnmarkEOF()` when new WAL data arrives.
- **Why it matters:** This explains why the iterator can become invalid with `Status::OK()` and later become valid again, which is otherwise surprising from the public API.
- **Key source:** `db/transaction_log_impl.cc` (`NextImpl`), `db/log_reader.cc` (`Reader::UnmarkEOF`)
- **Suggested placement:** Chapter 10.

### MANIFEST WAL tracking intentionally ignores live-WAL sync
- **What it is:** `track_and_verify_wals_in_manifest` records synced size only for inactive WALs, which means a synced live WAL can still be absent from WAL tracking until it rolls inactive.
- **Why it matters:** This is a debugging trap when someone expects `SyncWAL()` to immediately make MANIFEST WAL tracking stricter.
- **Key source:** `include/rocksdb/options.h` (`track_and_verify_wals_in_manifest` comment), `db/db_impl/db_impl.cc` (`MarkLogsSynced`)
- **Suggested placement:** Chapter 9.

### Predecessor-WAL verification is per-record-sequence, not per-key-sequence
- **What it is:** The recorded sequence in `PredecessorWALInfo` is the start sequence of the last logical WAL record, not the last individual key sequence in that record.
- **Why it matters:** Anyone extending WAL-hole detection or interpreting mismatch reports needs the real granularity.
- **Key source:** `db/log_writer.cc` (`Writer::AddRecord`), `db/db_impl/db_impl_write.cc` (`WriteToWAL`), `db/db_impl/db_impl_open.cc` (`ProcessLogRecord`)
- **Suggested placement:** Chapter 9.

## Positive Notes

- The chapter layout is much better than a single monolithic WAL page. It makes it easy to isolate format, write path, read path, recovery, and retention concerns.
- The docs cover newer WAL features that are easy to miss, including predecessor-WAL verification and user-defined timestamp size records.
- The lifecycle chapter correctly centers `MinLogNumberToKeep()` as the key retention boundary, which is the right mental model even though some of the surrounding prose needs correction.
