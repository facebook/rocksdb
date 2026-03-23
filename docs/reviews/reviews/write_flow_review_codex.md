# Review: write_flow -- Codex

## Summary
Overall quality rating: significant issues

The write_flow doc set is organized well. The chapter split follows the real write path, the index is the requested size, and several tables are useful, especially the WriteBatch layout and the memtable encoding material.

The problem is reliability at subsystem boundaries. Recovery behavior, WAL verification options, sequence-assignment timing, and unordered-write semantics are materially wrong or stale in multiple chapters. Several 2024-2026 behaviors that are exercised by current tests are absent, especially recovery-time WriteBufferManager enforcement and the split between MANIFEST-based WAL tracking and predecessor-WAL verification.

## Correctness Issues

### [WRONG] WAL recovery mode default is wrong
- **File:** `docs/components/write_flow/09_crash_recovery.md` -- WAL Recovery Modes
- **Claim:** "`kTolerateCorruptedTailRecords` (default)"
- **Reality:** The default is `WALRecoveryMode::kPointInTimeRecovery`.
- **Source:** `include/rocksdb/options.h` (`DBOptions::wal_recovery_mode`)
- **Fix:** Mark `kPointInTimeRecovery` as the default and describe the other modes relative to it.

### [WRONG] Recovery is documented as always flushing recovered memtables and deleting the recovered WALs
- **File:** `docs/components/write_flow/09_crash_recovery.md` -- Recovery Flow
- **Claim:** "Step 4 - **Flush recovered memtables**: The recovered memtables are flushed to SST files, establishing a clean state." / "Step 5 - **Delete obsolete WALs**: WAL files whose data has been fully flushed to SST are removed (or recycled)."
- **Reality:** Recovery can skip the final flush when `avoid_flush_during_recovery=true` and no mid-recovery flush occurred. In that case `MaybeFlushFinalMemtableOrRestoreActiveLogFiles()` keeps the recovered memtables and `RestoreAliveLogFiles()` keeps the WALs alive for future recovery. Read-only recovery also skips flush/truncate behavior. In the opposite direction, `enforce_write_buffer_manager_during_recovery=true` can force flushes even when `avoid_flush_during_recovery=true`.
- **Source:** `db/db_impl/db_impl_open.cc` (`InsertLogRecordToMemtable`, `MaybeFlushFinalMemtableOrRestoreActiveLogFiles`, `RestoreAliveLogFiles`), `include/rocksdb/options.h` (`avoid_flush_during_recovery`, `enforce_write_buffer_manager_during_recovery`), `db/db_wal_test.cc` (`AvoidFlushDuringRecovery`, `RecoverWithoutFlush`, `ReadOnlyRecoveryNoTruncate`), `db/db_write_buffer_manager_test.cc`
- **Fix:** Rewrite the end-of-recovery flow as a decision tree: mid-recovery flushes, final flush vs restore-active-WALs, read-only behavior, and the WBM override.

### [WRONG] Predecessor-WAL verification is attributed to the wrong option
- **File:** `docs/components/write_flow/09_crash_recovery.md` -- WAL Chain Verification
- **Claim:** "When `track_and_verify_wals_in_manifest` is enabled, each new WAL file records information about its predecessor via `PredecessorWALInfoType` records."
- **Reality:** `PredecessorWALInfoType` and `kRecyclePredecessorWALInfoType` records are written and verified only when `track_and_verify_wals=true`. `track_and_verify_wals_in_manifest` is a different mechanism based on `WalSet` entries in MANIFEST.
- **Source:** `include/rocksdb/options.h` (`track_and_verify_wals_in_manifest`, `track_and_verify_wals`), `db/db_impl/db_impl_open.cc` (`CreateWAL`, WAL-dir verification), `db/log_writer.cc` (`Writer::MaybeAddPredecessorWALInfo`), `db/log_reader.cc` (`Reader::MaybeVerifyPredecessorWALInfo`)
- **Fix:** Split the two mechanisms into separate subsections and attribute predecessor-WAL records to `track_and_verify_wals`.

### [WRONG] Sequence assignment timing around the WAL write is reversed
- **File:** `docs/components/write_flow/01_write_apis.md` -- WriteBatch Binary Format; `docs/components/write_flow/05_sequence_numbers.md` -- Normal Write Path
- **Claim:** "Placeholder; filled by write leader after WAL write" / "The sequence number field in the header is initialized to 0 and only populated by the write group leader during `DBImpl::WriteImpl()` after WAL write but before memtable insertion." / "Sequence numbers are assigned after WAL write but before memtable insertion."
- **Reality:** The caller's batch header starts at zero, but the merged batch written to the WAL gets its sequence before append. `WriteGroupToWAL()` and `ConcurrentWriteGroupToWAL()` call `WriteBatchInternal::SetSequence()` and then append the record to the WAL. What happens after WAL success is per-writer sequence bookkeeping and later publication through `SetLastSequence()`.
- **Source:** `db/db_impl/db_impl_write.cc` (`WriteImpl`, `WriteGroupToWAL`, `ConcurrentWriteGroupToWAL`), `db/write_batch.cc` (`WriteBatchInternal::SetSequence`, `WriteBatchInternal::InsertInto`)
- **Fix:** Distinguish three phases: zeroed header in the caller-visible batch, sequence assignment to the merged WAL batch before append, and reader-visible publication after memtable success.

### [WRONG] The write-stall barrier mechanism is described as a flag that does not exist
- **File:** `docs/components/write_flow/02_write_thread.md` -- Write Stall Barrier
- **Claim:** "Sets `stall_flag_` to true" and "Any new writer attempting to enqueue via `JoinBatchGroup()` checks the stall flag"
- **Reality:** There is no `stall_flag_`. `BeginWriteStall()` links `write_stall_dummy_` into `newest_writer_`, and `LinkOne()` treats that dummy as the stall sentinel. `BeginWriteStall()` also walks already-queued writers and completes any `no_slowdown` writers with `Status::Incomplete("Write stall")`.
- **Source:** `db/write_thread.h` (`write_stall_dummy_`), `db/write_thread.cc` (`LinkOne`, `BeginWriteStall`, `EndWriteStall`)
- **Fix:** Rewrite the section around the dummy-writer queue barrier, the queued-writer scan, and `stall_cv_`.

### [WRONG] Pipelined memtable-phase leadership is described as if every eligible writer becomes a leader
- **File:** `docs/components/write_flow/06_write_modes.md` -- Pipelined Write
- **Claim:** "Step 5 - Writers whose batches should be written to memtable transition to `STATE_MEMTABLE_WRITER_LEADER`."
- **Reality:** The WAL leader links the remaining memtable writers onto `newest_memtable_writer_`, but only one writer is promoted to `STATE_MEMTABLE_WRITER_LEADER`. The others stay queued and later become followers or parallel memtable writers after `EnterAsMemTableWriter()` builds the memtable group.
- **Source:** `db/write_thread.cc` (`ExitAsBatchGroupLeader`, `EnterAsMemTableWriter`, `ExitAsMemTableWriter`), `db/db_impl/db_impl_write.cc` (`PipelinedWriteImpl`)
- **Fix:** Describe the separate memtable queue and single-leader election, rather than saying all memtable writers become leaders.

### [WRONG] The recovery chapter claims atomic flush makes recovered memtables flush atomically
- **File:** `docs/components/write_flow/09_crash_recovery.md` -- Interaction with Other Features
- **Claim:** "| Atomic flush | If enabled, all CFs' recovered memtables are flushed atomically |"
- **Reality:** Recovery writes recovered memtables per column family through `MaybeWriteLevel0TableForRecovery()` and the final cleanup in `MaybeFlushFinalMemtableOrRestoreActiveLogFiles()`. The visible effect of `atomic_flush` here is in MANIFEST AtomicGroup semantics and best-efforts recovery constraints, not a special atomic recovery flush algorithm.
- **Source:** `db/db_impl/db_impl_open.cc` (`MaybeWriteLevel0TableForRecovery`, `MaybeFlushFinalMemtableOrRestoreActiveLogFiles`), `include/rocksdb/options.h` (best-efforts recovery comment about AtomicGroup)
- **Fix:** Replace this with the real impact of `atomic_flush` on recovery, or omit it from the table.

### [MISLEADING] Unordered write is explained as if live reads lose key ordering rather than snapshot immutability
- **File:** `docs/components/write_flow/06_write_modes.md` -- Unordered Write
- **Claim:** "two concurrent writes to the same key may be visible in either order (the WAL records the 'true' order for recovery, but live reads may see them in arbitrary order until compaction resolves the ordering)."
- **Reality:** Sequence numbers still define the order. The relaxed guarantee is that `LastSequence` can be published before lower-sequence memtable inserts finish, so a snapshot, iterator, or MultiGet can observe additional lower-sequence writes later. That is a snapshot/point-in-time consistency caveat, not a loss of sequence ordering.
- **Source:** `include/rocksdb/options.h` (`DBOptions::unordered_write` comment), `db/db_impl/db_impl_write.cc` (`WriteImplWALOnly`, `UnorderedWriteMemtable`)
- **Fix:** Explain unordered write in terms of publish-before-memtable and relaxed snapshot immutability.

### [MISLEADING] The memtable delay condition omits a required predicate
- **File:** `docs/components/write_flow/07_flow_control.md` -- Delay Conditions
- **Claim:** "Near-full immutable memtables | `max_write_buffer_number > 3 && num_unflushed >= max_write_buffer_number - 1`"
- **Reality:** The actual delay condition also requires `num_unflushed_memtables - 1 >= min_write_buffer_number_to_merge`. Without that third predicate the doc overstates when memtable-based delay begins.
- **Source:** `db/column_family.cc` (`ColumnFamilyData::GetWriteStallConditionAndCause`)
- **Fix:** Add the third predicate or explain that the delay is gated by the merge threshold.

### [WRONG] The compaction-pressure thresholds are described with the wrong formulas
- **File:** `docs/components/write_flow/07_flow_control.md` -- Compaction Pressure
- **Claim:** "This is triggered when L0 files reach a speedup threshold (midpoint between compaction trigger and slowdown trigger) or when pending compaction bytes reach a configurable fraction of the soft limit."
- **Reality:** The L0 speedup threshold is `min(2 * level0_file_num_compaction_trigger, trigger + (slowdown - trigger) / 4)`, not the midpoint. The pending-bytes threshold is derived from bottommost size, `max_bytes_for_level_base`, and `soft_pending_compaction_bytes_limit / 4`, not a generic configurable fraction.
- **Source:** `db/column_family.cc` (`GetL0FileCountForCompactionSpeedup`, `GetPendingCompactionBytesForCompactionSpeedup`)
- **Fix:** Replace the prose with the real formulas or a precise explanation of those helper functions.

### [MISLEADING] SingleDelete cleanup is flattened into "exactly one Put or corruption"
- **File:** `docs/components/write_flow/08_tombstone_lifecycle.md` -- SingleDelete Cleanup
- **Claim:** "If multiple `Put`s exist or zero `Put`s are found, the situation is treated as a corruption or user error"
- **Reality:** The compaction path has multiple tolerated branches. Consecutive `SingleDelete`s increment mismatch stats and continue. `SingleDelete` plus `Delete` can warn or fail depending on `enforce_single_del_contracts`. Timestamp GC can force keeping the `SingleDelete`. `allow_ingest_behind` blocks some drops. There is also the optimization that keeps the `SingleDelete` but clears the matched Put's value when an earlier snapshot still needs a conflict marker.
- **Source:** `db/compaction/compaction_iterator.cc` (`CompactionIterator::NextFromInput`), `include/rocksdb/options.h` (`enforce_single_del_contracts`, `cf_allow_ingest_behind`)
- **Fix:** Rewrite the section around the real decision tree: drop-both, keep-for-snapshot, mismatch accounting/warnings, and the timestamp / ingest-behind restrictions.

### [MISLEADING] MANIFEST WAL tracking overstates what syncs are actually recorded
- **File:** `docs/components/write_flow/03_wal.md` -- WAL Tracking in MANIFEST
- **Claim:** "`WalAddition` records are written when a WAL is created or its synced size is updated" and "This tracking is enabled by `track_and_verify_wals_in_manifest` ... and enables WAL integrity verification during recovery."
- **Reality:** Current tracking records synced size for inactive or closed WALs only. Live-WAL sync through `DB::SyncWAL()` or `WriteOptions::sync=true` is intentionally not tracked. WAL creation itself is not sufficient.
- **Source:** `include/rocksdb/options.h` (`track_and_verify_wals_in_manifest` comment), `db/db_impl/db_impl.cc` (`MarkLogsSynced`, `ApplyWALToManifest`)
- **Fix:** Say explicitly that MANIFEST tracking covers synced closed WALs and does not fully cover live-WAL sync.

### [STALE] WAL compression support is documented as broader than the current sanitizer allows
- **File:** `docs/components/write_flow/10_performance.md` -- WAL
- **Claim:** "WAL supports streaming compression (Snappy, ZSTD) to reduce I/O bandwidth"
- **Reality:** Option sanitization currently allows only ZSTD for WAL compression. Unsupported values are reset to `kNoCompression`.
- **Source:** `include/rocksdb/options.h` (`DBOptions::wal_compression`), `db/db_impl/db_impl_open.cc` (`SanitizeOptions`)
- **Fix:** Update the docs to say WAL compression currently supports ZSTD only.

### [MISLEADING] WAL recycling is described as flatly incompatible with `disableWAL`
- **File:** `docs/components/write_flow/03_wal.md` -- WAL Recycling
- **Claim:** "Note: WAL recycling is incompatible with `disableWAL` because corruption detection in recycled files relies on sequential sequence numbers."
- **Reality:** `DBImpl::WriteImpl()` has an explicit exception for the internal WAL-only path used with `two_write_queues_ && disable_memtable`. The general restriction is correct for ordinary writes, but the chapter states it as absolute.
- **Source:** `db/db_impl/db_impl_write.cc` (`DBImpl::WriteImpl`)
- **Fix:** Add the internal exception or cross-link the validation section that explains it.

### [WRONG] `FetchAddLastAllocatedSequence()` is not limited to two-queue mode
- **File:** `docs/components/write_flow/05_sequence_numbers.md` -- Visibility Publishing
- **Claim:** "`FetchAddLastAllocatedSequence()`: Atomically allocates a range of sequence numbers. Used only in two-queue mode."
- **Reality:** Unordered write also uses `FetchAddLastAllocatedSequence()` through `WriteImplWALOnly()` / `ConcurrentWriteGroupToWAL()`, and the recoverable-state path uses it as well when two write queues are active. The gap between allocated and visible sequence numbers is therefore not a two-queue-only concept.
- **Source:** `db/db_impl/db_impl_write.cc` (`WriteImplWALOnly`, `ConcurrentWriteGroupToWAL`, `WriteRecoverableState`), `db/version_set.h`
- **Fix:** Document it as the atomic reservation primitive for the non-simple write paths, not as a two-queue-only helper.

## Completeness Gaps

### Missing: `enforce_write_buffer_manager_during_recovery`
- **Why it matters:** This option now defaults to true and materially changes DB-open behavior under shared `WriteBufferManager` pressure. Anyone debugging memory spikes or surprise flushes during recovery needs this.
- **Where to look:** `include/rocksdb/options.h` (`enforce_write_buffer_manager_during_recovery`), `db/db_impl/db_impl_open.cc` (`InsertLogRecordToMemtable`), `db/db_write_buffer_manager_test.cc`
- **Suggested scope:** Add to Chapter 7 and Chapter 9, with an explicit note that it can override `avoid_flush_during_recovery`.

### Missing: `track_and_verify_wals` vs `track_and_verify_wals_in_manifest`
- **Why it matters:** These are two different integrity mechanisms with different metadata, different performance tradeoffs, and different failure modes. The docs currently blur them together.
- **Where to look:** `include/rocksdb/options.h`, `db/log_writer.cc`, `db/log_reader.cc`, `db/db_impl/db_impl_open.cc`
- **Suggested scope:** Split Chapter 3 and Chapter 9 into one subsection per mechanism and cross-link them.

### Missing: the full `DBImpl::WriteImpl()` validation matrix
- **Why it matters:** The docs present validation as if the listed combinations are the whole story, but the current implementation also rejects missing timestamp updates, `rate_limiter_priority` with `disableWAL` or `manual_wal_flush`, `post_memtable_callback` with pipelined or `seq_per_batch`, and several `WriteBatchWithIndex` ingestion combinations.
- **Where to look:** `db/db_impl/db_impl_write.cc` (`DBImpl::WriteImpl`)
- **Suggested scope:** Expand Chapter 1 with a more complete validation table.

### Missing: `LastPublishedSequence` and the three-counter model
- **Why it matters:** Chapter 5 talks about allocation vs visibility but omits `LastPublishedSequence`, which is part of the real sequencing model for two-write-queue and recovery behavior.
- **Where to look:** `db/version_set.h` (`LastSequence`, `LastAllocatedSequence`, `LastPublishedSequence`), `db/db_impl/db_impl_write.cc`, `db/db_impl/db_impl_open.cc`
- **Suggested scope:** Expand Chapter 5 and cross-link it from Chapter 6.

### Missing: read-only recovery behavior
- **Why it matters:** Read-only open does not flush recovered memtables or truncate the recovered WAL, and the recovery-time WBM scheduler intentionally skips scheduling in read-only mode. That is operationally important and currently absent.
- **Where to look:** `db/db_impl/db_impl_open.cc` (`MaybeWriteLevel0TableForRecovery`, `InsertLogRecordToMemtable`, `MaybeFlushFinalMemtableOrRestoreActiveLogFiles`), `db/db_wal_test.cc` (`ReadOnlyRecoveryNoTruncate`), `db/db_write_buffer_manager_test.cc` (`ReadOnlyRecoveryWithEnforceWBMDoesNotAssert`)
- **Suggested scope:** Add a dedicated note in Chapter 9.

## Depth Issues

### Recovery final-state handling needs the real branching logic
- **Current:** Chapter 9 presents recovery as a linear five-step pipeline ending in flush and WAL deletion.
- **Missing:** The actual code branches on whether a recovery-time flush already happened, whether `avoid_flush_during_recovery` is enabled, whether the DB is read-only, and whether WBM enforcement forced a flush. That control flow is the main thing a developer needs when debugging recovery output.
- **Source:** `db/db_impl/db_impl_open.cc` (`ProcessLogFiles`, `MaybeWriteLevel0TableForRecovery`, `MaybeFlushFinalMemtableOrRestoreActiveLogFiles`)

### Unordered write needs the mechanism, not just the tradeoff label
- **Current:** Chapter 6 says unordered write removes ordering between memtable writers.
- **Missing:** The important detail is that sequence publication happens before memtable landing, `pending_memtable_writes_` tracks outstanding inserts, and `switch_cv_` blocks memtable switches until those inserts finish. Without that, the relaxed snapshot semantics are hard to understand.
- **Source:** `db/db_impl/db_impl_write.cc` (`WriteImplWALOnly`, `UnorderedWriteMemtable`), `db/db_impl/db_impl.h` (`pending_memtable_writes_`, `switch_cv_`)

### PreprocessWrite ordering needs rationale, not just a numbered list
- **Current:** Chapter 7 lists the `PreprocessWrite()` steps in order.
- **Missing:** The docs do not explain why WBM-triggered flush comes before `WriteController` delay/stop, why `max_total_wal_size` only matters in multi-CF mode, or how this ordering interacts with later write stalls.
- **Source:** `db/db_impl/db_impl_write.cc` (`PreprocessWrite`), `db/column_family.cc`, `include/rocksdb/options.h`

## Structure and Style Violations

### Inline code formatting rule is violated throughout the doc set
- **File:** `docs/components/write_flow/index.md` and all chapter files
- **Details:** Every file in `docs/components/write_flow/` uses inline backticks heavily for identifiers, options, file paths, and prose references, even though the stated house rule for these component docs says to avoid inline code quotes.

### `Key Invariant` is used for format/design properties, not only crash/corruption invariants
- **File:** `docs/components/write_flow/03_wal.md`, `docs/components/write_flow/05_sequence_numbers.md`, `docs/components/write_flow/08_tombstone_lifecycle.md`
- **Details:** Examples include the WAL fragment sequence rule, the sequence-assignment timing sentence, and range-tombstone truncation. These are either ordinary format rules, implementation properties, or in one case factually wrong, not the narrow class of invariants the style guide reserves for corruption/crash-critical guarantees.

## Undocumented Complexity

### Recovery-time WBM enforcement is a separate recovery policy
- **What it is:** WAL recovery now checks `WriteBufferManager::ShouldFlush()` and can schedule flushes while replaying WAL records. Once one of those flushes happens, the end-of-recovery behavior changes for the remaining memtables as well.
- **Why it matters:** This is the difference between bounded memory and "why did open create L0 files even though avoid-flush was enabled?"
- **Key source:** `include/rocksdb/options.h` (`enforce_write_buffer_manager_during_recovery`), `db/db_impl/db_impl_open.cc` (`InsertLogRecordToMemtable`, `MaybeFlushFinalMemtableOrRestoreActiveLogFiles`)
- **Suggested placement:** Chapter 7 and Chapter 9

### RocksDB now has two distinct WAL-integrity features
- **What it is:** `track_and_verify_wals_in_manifest` tracks synced closed WAL sizes in MANIFEST, while `track_and_verify_wals` emits and verifies predecessor-WAL metadata inside WAL files themselves.
- **Why it matters:** They catch different failure classes and should not be tuned or debugged as if they were one feature.
- **Key source:** `include/rocksdb/options.h`, `db/log_writer.cc` (`Writer::MaybeAddPredecessorWALInfo`), `db/log_reader.cc` (`Reader::MaybeVerifyPredecessorWALInfo`), `db/db_impl/db_impl_open.cc`
- **Suggested placement:** Chapter 3 and Chapter 9

### Unordered write's real mechanism is publish-before-memtable plus switch blocking
- **What it is:** The WAL-only phase publishes the new visible sequence range before the memtable phase finishes, then `pending_memtable_writes_` and `switch_cv_` prevent memtable switches while those inserts are still landing.
- **Why it matters:** This is the actual reason snapshots, iterators, and MultiGet lose their normal immutability guarantees.
- **Key source:** `db/db_impl/db_impl_write.cc` (`WriteImplWALOnly`, `UnorderedWriteMemtable`), `db/db_impl/db_impl.h`
- **Suggested placement:** Chapter 6

### `LastPublishedSequence` is part of the write-path state model
- **What it is:** The code tracks three related sequence counters: allocated, published, and visible. Recovery and multi-queue paths update more than just `LastSequence`.
- **Why it matters:** Developers touching write ordering or recovery can make incorrect assumptions if they only know about allocation and visibility.
- **Key source:** `db/version_set.h`, `db/db_impl/db_impl_write.cc` (`WriteRecoverableState`), `db/db_impl/db_impl_open.cc` (`FinishLogFileProcessing`)
- **Suggested placement:** Chapter 5

## Positive Notes

- `docs/components/write_flow/index.md` follows the expected compressed index shape and is exactly within the requested 40-80 line range.
- `docs/components/write_flow/01_write_apis.md` has a useful WriteBatch binary-layout table that is close to the real encoding and easy to act on.
- `docs/components/write_flow/04_memtable_insert.md` gives a good developer-facing overview of memtable entry encoding, the split between point keys and range tombstones, and the major data structures involved.
- `docs/components/write_flow/10_performance.md` has a helpful cross-component interaction table that points readers toward the main subsystems they need to inspect next.
- Every chapter has a `Files:` line, and most of those file selections point readers at the right implementation area even where the prose needs correction.
