# Review: user_defined_timestamp — Codex

## Summary

Overall quality rating: **significant issues**

The documentation is well segmented, easy to navigate, and covers the right top-level areas: encoding, write path, read path, compaction, flush, recovery, migration, transactions, and tooling. The index is the requested size, and the chapter layout is close to what a maintainer would want when orienting themselves in the subsystem.

The problem is factual reliability. Several of the highest-value claims are wrong in the current codebase: `WriteBatch` timestamp handling, `DeleteRange` encoding, `SeekForPrev()` target construction, `IncreaseFullHistoryTsLow()` error behavior, migration toggle rules, WAL-recovery fast paths, transaction read/commit semantics, BlobDB compatibility, and benchmark/stress support. The docs also repeatedly present usage assumptions or current implementation details as hard invariants, which is dangerous in a subsystem where cross-component behavior matters.

## Correctness Issues

### [MISLEADING] The seq/timestamp relation is documented as an engine-maintained invariant
- **File:** `01_key_encoding_and_comparator.md`, `Timestamp-Sequence Ordering Constraint`; `index.md`, `Key Invariants`
- **Claim:** "For correctness, RocksDB maintains a bidirectional ordering invariant..." and "For the same user key: if seq1 < seq2 then ts1 <= ts2, and if ts1 < ts2 then seq1 < seq2"
- **Reality:** The docs present this as a universal engine invariant, but current write paths mostly enforce timestamp size and comparator compatibility, not per-key monotonic ordering between sequence numbers and user timestamps. The transaction tests explicitly note that the caller must keep commit timestamps ordered.
- **Source:** `db/write_batch.cc`, `WriteBatch::Put(ColumnFamilyHandle*, const Slice&, const Slice&, const Slice&)`; `utilities/transactions/pessimistic_transaction.cc`, `WriteCommittedTxn::SetCommitTimestamp`; `utilities/transactions/write_committed_transaction_ts_test.cc`, `BlindWrite`
- **Fix:** Rephrase this as an application-level requirement / expected usage pattern, not something RocksDB always verifies or maintains for every write path.

### [WRONG] The CF-aware `WriteBatch::Put(cf, key, value)` semantics are reversed
- **File:** `02_write_path.md`, `WriteBatch with Timestamps`
- **Claim:** "`WriteBatch::Put(cf, key, value)` - key must include the timestamp suffix (user is responsible for appending it)"
- **Reality:** The CF-aware overload without an explicit timestamp does not require a caller-appended suffix. For timestamp-enabled column families it appends a zero-filled placeholder timestamp internally, sets `needs_in_place_update_ts_ = true`, and relies on later `UpdateTimestamps()`.
- **Source:** `db/write_batch.cc`, `WriteBatch::Put(ColumnFamilyHandle*, const Slice&, const Slice&)`
- **Fix:** Distinguish this overload from `WriteBatch::Put(cf, key, ts, value)`. The former writes a placeholder timestamp for later update; the latter takes the explicit timestamp from the caller.

### [WRONG] Timestamped `DeleteRange` is not encoded with `kTypeDeletionWithTimestamp`
- **File:** `02_write_path.md`, `DeleteRange with Timestamps`
- **Claim:** "The value type for timestamped deletions is `kTypeDeletionWithTimestamp` (value 0x14 in the `ValueType` enum in `db/dbformat.h`)."
- **Reality:** Range deletes remain `kTypeRangeDeletion`. The timestamp is encoded into the begin and end user keys, not represented by a separate timestamped deletion value type.
- **Source:** `db/write_batch.cc`, `WriteBatchInternal::DeleteRange`; `db/dbformat.h`, `ValueType`
- **Fix:** Say that timestamped range deletes still use `kTypeRangeDeletion`, with timestamp bytes embedded in both endpoints.

### [WRONG] `SeekForPrev()` target construction is oversimplified
- **File:** `03_read_path.md`, `Seek Behavior`
- **Claim:** "For `SeekForPrev()`, the minimum timestamp is appended to find the last key <= target"
- **Reality:** `DBIter::SetSavedKeyToSeekForPrevTarget()` is more complex. It seeds the target with `timestamp_ub_`, then, when timestamps are enabled, uses `timestamp_lb_` if present and otherwise the minimum timestamp. If `iterate_upper_bound` clamps the seek target, it uses the maximum timestamp instead.
- **Source:** `db/db_iter.cc`, `DBIter::SetSavedKeyToSeekForPrevTarget`
- **Fix:** Document the lower-bound and upper-bound cases explicitly instead of saying `SeekForPrev()` always appends the minimum timestamp.

### [WRONG] `IncreaseFullHistoryTsLow()` does not return a retry-style error here
- **File:** `04_compaction_and_gc.md`, `Setting full_history_ts_low`
- **Claim:** "`IncreaseFullHistoryTsLow()` is monotonic: the new value must be >= the current value. If another thread concurrently increases the value beyond the requested value, a try-again error is returned."
- **Reality:** The implementation checks the current value under the DB mutex and returns `Status::InvalidArgument` when the requested timestamp is lower than the current one. The documented "try-again" behavior does not match the code.
- **Source:** `db/db_impl/db_impl_compaction_flush.cc`, `DBImpl::IncreaseFullHistoryTsLowImpl`
- **Fix:** Document `InvalidArgument` for a lower-than-current value. Do not describe this path as a retryable race.

### [MISLEADING] `GetNewestUserDefinedTimestamp()` is described as SST-metadata driven
- **File:** `04_compaction_and_gc.md`, `GetNewestUserDefinedTimestamp`; `07_migration_and_compatibility.md`, `Timestamp Metadata in SST Files`
- **Claim:** "For SST files, the `full_history_ts_low` is used as a proxy..." and "`rocksdb.timestamp_min` and `rocksdb.timestamp_max` ... are used for ... Tracking the newest timestamp across SST files for `GetNewestUserDefinedTimestamp()`"
- **Reality:** The API checks the mutable memtable, then immutable memtables, then derives a U64 cutoff from `SuperVersion::full_history_ts_low` if SST data exists. It does not scan per-file timestamp metadata or table properties to find the newest SST timestamp.
- **Source:** `db/db_impl/db_impl.cc`, `DBImpl::GetNewestUserDefinedTimestamp`
- **Fix:** Keep file timestamp metadata separate from this API. Document the API as memtable state plus a `full_history_ts_low`-based proxy in memtable-only mode.

### [MISLEADING] The `newest_udt_` concurrency story is attributed to the wrong layer
- **File:** `05_flush_and_persistence.md`, `Memtable UDT Tracking`
- **Claim:** "This is updated by `MaybeUpdateNewestUDT()` in `db/memtable.cc` on every insertion (when `allow_concurrent_memtable_write` is false)." and "Concurrent memtable writes (`allow_concurrent_memtable_write=true`) do not currently support updating `newest_udt_`."
- **Reality:** `MaybeUpdateNewestUDT()` itself has no concurrent-write guard. The relevant compatibility rule is enforced earlier by option validation: when timestamps are enabled and `persist_user_defined_timestamps=false`, RocksDB rejects `allow_concurrent_memtable_write=true` at open time.
- **Source:** `db/memtable.cc`, `MemTable::MaybeUpdateNewestUDT`; `db/column_family.cc`, `ColumnFamilyData::ValidateOptions`
- **Fix:** Describe the open-time option restriction as the reason the feature combination is unsupported, and avoid implying that the method itself branches on `allow_concurrent_memtable_write`.

### [WRONG] The migration table omits allowed persist-flag toggles when timestamps are disabled
- **File:** `07_migration_and_compatibility.md`, `Allowed Transitions`
- **Claim:** "`No change | Toggle persist flag | UDT is enabled (ts_sz > 0) | Rejected: persist_user_defined_timestamps cannot be toggled while UDT is enabled`"
- **Reality:** When the comparator is unchanged and `timestamp_size() == 0`, `ValidateUserDefinedTimestampsOptions()` allows toggling `persist_user_defined_timestamps` in either direction. Only the `ts_sz > 0` case is rejected.
- **Source:** `util/udt_util.cc`, `ValidateUserDefinedTimestampsOptions`; `util/udt_util_test.cc`, `ValidateUserDefinedTimestampsOptionsTest.UserComparatorUnchanged`
- **Fix:** Split the matrix by `ts_sz == 0` versus `ts_sz > 0` so the allowed no-UDT case is explicit.

### [WRONG] Existing SST files are not marked with a separate persisted `has_no_udt` bit
- **File:** `07_migration_and_compatibility.md`, `Migration Procedure -> Enabling UDT`
- **Claim:** "Existing SST files are marked as `user_defined_timestamps_persisted=true` but `has_no_udt=true` ..."
- **Reality:** There is no separate persisted `has_no_udt` field. The migration path uses `mark_sst_files_has_no_udt` / `cfds_to_mark_no_udt_` to rewrite file boundaries and force `FileMetaData.user_defined_timestamps_persisted = false` for the affected files.
- **Source:** `util/udt_util.cc`, `ValidateUserDefinedTimestampsOptions`; `db/version_edit_handler.cc`, `VersionEditHandler::ExtractInfoFromVersionEdit`, `VersionEditHandler::MaybeHandleFileBoundariesForNewFiles`
- **Fix:** Describe the actual mechanism: open-time detection, boundary rewriting, and `user_defined_timestamps_persisted=false` for existing files after enabling memtable-only UDT.

### [MISLEADING] Dropped-CF entries are not always copied into a rebuilt batch
- **File:** `06_recovery_and_wal_replay.md`, `Dropped Column Families`
- **Claim:** "Their entries are copied to the new `WriteBatch` unchanged."
- **Reality:** Dropped column families are ignored during consistency checking. If all running column families are already consistent, `HandleWriteBatchTimestampSizeDifference()` returns OK without rebuilding any batch at all. Unchanged copying happens only when some other entry forces reconciliation.
- **Source:** `util/udt_util.cc`, `CheckWriteBatchTimestampSizeConsistency`, `HandleWriteBatchTimestampSizeDifference`
- **Fix:** Say dropped-CF entries are ignored for consistency checks and are only copied if a rebuilt batch is otherwise required.

### [WRONG] `GetForUpdate()` does not ignore `ReadOptions::timestamp`
- **File:** `08_transaction_integration.md`, `Transaction::GetForUpdate() (Locking Read)`
- **Claim:** "`Transaction::GetForUpdate()` performs a locking read and **ignores** `ReadOptions::timestamp`. Instead, it uses the transaction's read timestamp..."
- **Reality:** If `ReadOptions::timestamp` is absent, `GetForUpdateImpl()` synthesizes it from `read_timestamp_`. If it is provided, it must match `read_timestamp_` or the call fails with `InvalidArgument`. The field is not ignored.
- **Source:** `utilities/transactions/pessimistic_transaction.cc`, `WriteCommittedTxn::GetForUpdateImpl`
- **Fix:** Document the real rule: `ReadOptions::timestamp` is either injected from `read_timestamp_` or must exactly match it.

### [MISLEADING] `enable_udt_validation` is described too broadly
- **File:** `08_transaction_integration.md`, `UDT Validation`
- **Claim:** "When enabled, the transaction layer validates that timestamps are correctly propagated through the transaction lifecycle (prepare, commit, etc.)."
- **Reality:** The option mainly gates timestamp-based validation where it is applicable: read-timestamp sanity checks and timestamp-aware conflict checking. It is not a broad "timestamp propagation through the lifecycle" mechanism.
- **Source:** `include/rocksdb/utilities/transaction_db.h`, `TransactionDBOptions::enable_udt_validation`; `utilities/transactions/pessimistic_transaction.cc`, `WriteCommittedTxn::SanityCheckReadTimestamp`; `utilities/transactions/transaction_util.cc`, `TransactionUtil::CheckKey`
- **Fix:** Scope the description to validation / conflict-check behavior rather than generic lifecycle enforcement.

### [WRONG] `write_batch_track_timestamp_size` is attached to the wrong options struct
- **File:** `08_transaction_integration.md`, `MyRocks Compatibility`
- **Claim:** "`TransactionDBOptions::write_batch_track_timestamp_size` is a temporary option dedicated to MyRocks compatibility."
- **Reality:** The field lives in `TransactionOptions`, not `TransactionDBOptions`. It is configured per transaction.
- **Source:** `include/rocksdb/utilities/transaction_db.h`, `TransactionOptions::write_batch_track_timestamp_size`; `utilities/transactions/write_committed_transaction_ts_test.cc`, `WritesBypassTransactionAPIs`
- **Fix:** Move this option to the `TransactionOptions` discussion and describe it as per-transaction MyRocks compatibility state.

### [WRONG] `SetCommitTimestamp()` is not required to happen after `Prepare()` in 2PC flows
- **File:** `08_transaction_integration.md`, `SetCommitTimestamp and Commit`
- **Claim:** "For two-phase commit (2PC), `SetCommitTimestamp()` must be called **after** `Transaction::Prepare()` succeeds."
- **Reality:** `SetCommitTimestamp()` just stores `commit_timestamp_` after validation. Tests exercise both orders: some transactions call `SetCommitTimestamp()` before `Prepare()`, others after `Prepare()`. The enforced requirement is that a commit timestamp be set before commit when UDT writes are present.
- **Source:** `utilities/transactions/pessimistic_transaction.cc`, `WriteCommittedTxn::SetCommitTimestamp`; `utilities/transactions/write_committed_transaction_ts_test.cc`, `WritesBypassTransactionAPIs`, WAL replay tests
- **Fix:** Say the timestamp must be set before `Commit()`, and note that both pre-prepare and post-prepare ordering are used today.

### [WRONG] The compatibility matrix says `BlobDB` is fully compatible
- **File:** `10_best_practices.md`, `Feature Compatibility Matrix`
- **Claim:** "`BlobDB | Yes | Timestamps apply to the key, not blob content`"
- **Reality:** Stacked BlobDB does not implement timestamp-returning `Get()` / `MultiGet()` paths and returns `NotSupported` for them. A blanket "Yes" is not defensible for the current codebase.
- **Source:** `utilities/blob_db/blob_db_impl.cc`, `BlobDBImpl::Get`, `BlobDBImpl::MultiGet`; `utilities/blob_db/blob_db.h`, timestamp-returning `Get` signature
- **Fix:** Either remove BlobDB from the matrix or qualify it narrowly as partial / unsupported for timestamp-returning read paths.

### [WRONG] `db_bench` UDT flag semantics are misstated
- **File:** `09_tools_and_testing.md`, `db_bench`
- **Claim:** "`--user_timestamp_size=N` | Number of bytes in a user-defined timestamp (0 = disabled)" and "Read operations can use either the latest timestamp or a random past timestamp, controlled by the `--read_with_latest_user_timestamp` flag."
- **Reality:** `db_bench` only supports 8-byte timestamps when UDT is enabled. Also, when `read_with_latest_user_timestamp` is true, `TimestampEmulator::GetTimestampForRead()` calls `Allocate()`, which advances the emulated timestamp counter. It does not read against a stable "latest" value.
- **Source:** `tools/db_bench_tool.cc`, flag handling for `FLAGS_user_timestamp_size`; `tools/db_bench_tool.cc`, `TimestampEmulator::GetTimestampForRead`
- **Fix:** Document the 8-byte-only support and explain that "latest" reads advance the emulator rather than pinning a stable newest timestamp.

### [MISLEADING] The stress/crash-test section overstates actual UDT coverage
- **File:** `09_tools_and_testing.md`, `Stress Test`
- **Claim:** "The stress test validates UDT correctness under concurrent reads, writes, compactions, flushes, and recovery. It exercises: ..."
- **Reality:** The stress and crash test harnesses disable many UDT combinations. Examples: crash test forces `use_multiscan=0` with timestamps; memtable-only UDT disables blob files, atomic flush, concurrent memtable write, multiget, multi-get entity, and iterator-heavy verification; db_stress exits if timestamps are combined with transactions or external file ingestion.
- **Source:** `tools/db_crashtest.py`, UDT option rewrites around `user_timestamp_size`; `db_stress_tool/db_stress_test_base.cc`, `CheckAndSetOptionsForUserTimestamp`; `db_stress_tool/db_stress_tool.cc`, validation logic
- **Fix:** Replace the generic bullets with a support/unsupported matrix and list the key disabled combinations explicitly.

## Completeness Gaps

### Manual flush versus automatic flush retention behavior is missing
- **Why it matters:** The memtable-only mode is not just "postpone flush to retain timestamps." Manual flush requests have a separate skip-reschedule path, which changes what users can rely on during maintenance or downgrade workflows.
- **Where to look:** `db/column_family.cc`, `ColumnFamilyData::SetFlushSkipReschedule`, `GetAndClearFlushSkipReschedule`, `ShouldPostponeFlushToRetainUDT`; `db/db_impl/db_impl_compaction_flush.cc`, `DBImpl::ShouldRescheduleFlushRequestToRetainUDT`
- **Suggested scope:** Expand chapter 5 with a short subsection contrasting automatic flush postponement with manual flush behavior.

### The file-level timestamp metadata lifecycle is not explained end-to-end
- **Why it matters:** The docs mention `min_timestamp`, `max_timestamp`, and `user_defined_timestamps_persisted`, but they do not explain how those values are collected, propagated into `FileMetaData`, and consumed by readers / file-selection code.
- **Where to look:** `table/block_based/block_based_table_builder.cc`, `TimestampTablePropertiesCollector`; `db/compaction/compaction_outputs.cc`; `db/version_set.cc`; `table/block_based/block_based_table_reader.cc`
- **Suggested scope:** Add a focused subsection to chapter 5 or chapter 4 covering collector -> metadata -> read/compaction use.

### The tooling chapter omits `ldb` even though it has UDT-specific behavior
- **Why it matters:** Developers debugging production data often use `ldb` before they read C++. The current tools chapter mentions `db_bench` and stress tests but skips the CLI tool that already exposes UDT read timestamps and WAL-recovery handling.
- **Where to look:** `tools/ldb_cmd.cc`, timestamp flag parsing and `HandleWriteBatchTimestampSizeDifference`; `util/udt_util.cc`, `MaybeAddTimestampsToRange`
- **Suggested scope:** Add an `ldb` subsection to chapter 9.

### External SST ingestion needs more than the current four bullets
- **Why it matters:** UDT ingestion depends on file metadata validation, memtable-only mode handling, and boundary rewriting. The current text lists surface restrictions but not the actual compatibility checks.
- **Where to look:** `db/external_sst_file_ingestion_job.cc`, `ValidateUserDefinedTimestampsOptions` call sites and file-boundary handling; `db/external_sst_file_test.cc`
- **Suggested scope:** Expand chapter 9 or chapter 7 with a compact "ingest path" subsection.

### `OpenAndTrimHistory()` is documented as an API, not as an implementation path
- **Why it matters:** The API works today by reopening and then forcing compaction-based trimming through `HistoryTrimmingIterator`. That matters for performance, for correctness expectations, and for understanding current TODOs.
- **Where to look:** `db/db_impl/db_impl_open.cc`, `DB::OpenAndTrimHistory`; `db/history_trimming_iterator.h`; `db/compaction/compaction_job.cc`
- **Suggested scope:** Deepen chapter 7, or add a dedicated trim-history subsection.

## Depth Issues

### Range-bound conversion needs the actual inclusive/exclusive mapping
- **Current:** Chapter 3 says the end timestamp in `MaybeAddTimestampsToRange()` "depends on whether the end is exclusive or inclusive."
- **Missing:** The exact mapping is important: start gets max timestamp; exclusive end gets max timestamp; inclusive end gets min timestamp.
- **Source:** `util/udt_util.cc`, `MaybeAddTimestampsToRange`

### `GetNewestUserDefinedTimestamp()` omits the concurrency protocol
- **Current:** Chapter 4 presents the API as a simple mutable-memtable -> immutable-memtable -> SST lookup.
- **Missing:** The mutable-memtable read can enter the write thread to avoid racing with concurrent writes when the referenced memtable is still current. That is the critical concurrency detail for this API.
- **Source:** `db/db_impl/db_impl.cc`, `DBImpl::GetNewestUserDefinedTimestamp`

### `OpenAndTrimHistory()` needs the actual control-flow description
- **Current:** Chapter 7 explains the purpose and one option restriction.
- **Missing:** The implementation opens the DB normally, then runs a forced bottommost compaction with `trim_ts`, using `HistoryTrimmingIterator` inside compaction. The TODOs in the open path also matter because they explain current limitations.
- **Source:** `db/db_impl/db_impl_open.cc`, `DB::OpenAndTrimHistory`; `db/compaction/compaction_job.cc`, `HistoryTrimmingIterator` hookup

### The tools/testing chapter needs a real support matrix, not just examples
- **Current:** Chapter 9 gives one `db_bench` flag table and a short stress-test bullet list.
- **Missing:** The support story depends on whether the tool is `db_bench`, `ldb`, `db_stress`, or `db_crashtest`, and on whether memtable-only UDT is enabled. The current summary hides the actual limits maintainers need.
- **Source:** `tools/db_bench_tool.cc`; `tools/ldb_cmd.cc`; `db_stress_tool/db_stress_test_base.cc`; `tools/db_crashtest.py`

## Structure and Style Violations

### Inline code quotes are used throughout despite the stated rule
- **File:** repo-wide across `docs/components/user_defined_timestamp/*.md`
- **Details:** The generation prompt explicitly said "NO inline code quotes". Every chapter and the index use inline backticks heavily for APIs, options, types, and claims. This is systematic, not incidental.

### `INVARIANT` is used for assumptions and implementation contracts, not only true correctness invariants
- **File:** `index.md`; `01_key_encoding_and_comparator.md`; `06_recovery_and_wal_replay.md`
- **Details:** The "Key Invariants" sections include items like the seq/timestamp relation and WAL record ordering that are not universally enforced corruption-preventing invariants in the current code. They should move to constraints / assumptions / implementation notes.

### Some `Files:` lines omit the primary implementation files actually discussed
- **File:** `08_transaction_integration.md`; `09_tools_and_testing.md`
- **Details:** The transaction chapter omits `utilities/transactions/pessimistic_transaction.cc` and `utilities/transactions/transaction_util.cc`, which are the core sources for the documented behavior, while listing unsupported write-prepared / write-unprepared implementation files. The tools chapter omits `tools/db_crashtest.py` and `tools/ldb_cmd.cc`, both of which are directly relevant to its claims.

## Undocumented Complexity

### Manual flush can deliberately bypass the UDT-retention reschedule logic
- **What it is:** Memtable-only UDT retention is not a single policy. `flush_skip_reschedule_` lets some manual flush paths proceed without being postponed to retain user-defined timestamps.
- **Why it matters:** Users and maintainers will otherwise assume that "retain timestamps in memory" always wins, which is false for some manual-flush paths.
- **Key source:** `db/column_family.cc`, `ColumnFamilyData::SetFlushSkipReschedule`, `GetAndClearFlushSkipReschedule`; `db/db_impl/db_impl_compaction_flush.cc`, `DBImpl::ShouldRescheduleFlushRequestToRetainUDT`
- **Suggested placement:** Chapter 5

### File timestamp metadata is collected in one subsystem and consumed in others
- **What it is:** `TimestampTablePropertiesCollector` gathers per-file timestamp metadata during table building, then that state propagates through `FileMetaData` and reader / compaction structures.
- **Why it matters:** Without this path, the docs read like `min_timestamp` / `max_timestamp` are static facts rather than data produced and consumed across block builder, manifest metadata, and table reader boundaries.
- **Key source:** `table/block_based/block_based_table_builder.cc`, `TimestampTablePropertiesCollector`; `db/compaction/compaction_outputs.cc`; `table/block_based/block_based_table_reader.cc`
- **Suggested placement:** Chapter 5 or chapter 4

### `GetNewestUserDefinedTimestamp()` enters the write thread to read the mutable memtable safely
- **What it is:** When the referenced mutable memtable is still current, the API enters the write thread and waits for pending writes before reading `GetNewestUDT()`.
- **Why it matters:** This is the real concurrency story behind the API. The current docs make it sound like a plain read of mutable state.
- **Key source:** `db/db_impl/db_impl.cc`, `DBImpl::GetNewestUserDefinedTimestamp`
- **Suggested placement:** Chapter 4

### `OpenAndTrimHistory()` is a compaction-based feature with explicit current limitations
- **What it is:** The open path contains TODOs for flush-path trimming, recovery-time trimming, and more selective file picking. Today it trims by reopening and forcing compaction with `HistoryTrimmingIterator`.
- **Why it matters:** Maintainers need to know this is not a fully general "open and rewrite history everywhere" facility.
- **Key source:** `db/db_impl/db_impl_open.cc`, `DB::OpenAndTrimHistory`; `db/history_trimming_iterator.h`; `db/compaction/compaction_job.cc`
- **Suggested placement:** Chapter 7

### External SST ingestion has a memtable-only UDT compatibility path that rewrites assumptions
- **What it is:** Ingestion validates `user_defined_timestamps_persisted`, may coerce it to `false`, and can rebuild file-boundary handling depending on the DB and file combination.
- **Why it matters:** This is the real cross-component interaction between SST metadata, migration rules, and ingest-time validation.
- **Key source:** `db/external_sst_file_ingestion_job.cc`, UDT validation and file-preparation path; `db/external_sst_file_test.cc`
- **Suggested placement:** Chapter 9 or chapter 7

### The MyRocks compatibility flag preserves a known anti-pattern rather than a normal transaction flow
- **What it is:** `write_batch_track_timestamp_size` exists to keep direct writes into the inner `WriteBatch` limping along. The transaction tests show that bypassing transaction write APIs without it can make committed keys unreadable until WAL recovery.
- **Why it matters:** Readers need to understand this is not a recommended integration pattern and why the option exists at all.
- **Key source:** `include/rocksdb/utilities/transaction_db.h`, `TransactionOptions::write_batch_track_timestamp_size`; `utilities/transactions/write_committed_transaction_ts_test.cc`, `WritesBypassTransactionAPIs`
- **Suggested placement:** Chapter 8

## Positive Notes

- `index.md` follows the requested compressed pattern well: overview, key source files, chapter table, characteristics, and invariants, and it is within the requested 40-80 line range.
- Every chapter has a `Files:` line, which makes code-audit follow-up easier even though some of those lists need correction.
- The read chapter gets one subtle point right: `Iterator::key()` changes shape depending on whether `iter_start_ts` is set, which matches `DBIter::key()` and the timestamp iterator tests.
- The docs consistently point readers at the major subsystem boundaries that matter for UDT: recovery, compaction, migration, transactions, and tooling are all present rather than hidden in one monolithic chapter.
