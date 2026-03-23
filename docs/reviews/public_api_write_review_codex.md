# Review: public_api_write — Codex

## Summary
Overall quality rating: significant issues

The chapter set has decent coverage of the major feature areas and some of the higher-value internals, especially range tombstone fragmentation, merge failure scopes, and low-priority write throttling. The index is also structured in the expected shape.

The main problem is correctness drift at subsystem boundaries. Several chapters blur `DB` APIs with raw `WriteBatch` behavior, overstate what always goes through `DB::Write()`, and simplify option semantics in ways that are false in current code. The `WriteBatchWithIndex`, `SingleDelete`, `SstFileWriter`, and external-ingestion chapters need the most attention.

## Correctness Issues

### [WRONG] The index says all write APIs flow through `WriteBatch` and `DB::Write()`
- **File:** `index.md`, Overview
- **Claim:** "All write operations ultimately flow through `WriteBatch` encoding and the `DB::Write()` path"
- **Reality:** `SstFileWriter` writes SSTs directly through `TableBuilder`, and `DB::IngestExternalFile()` / `DB::IngestExternalFiles()` use `ExternalSstFileIngestionJob` rather than `WriteBatch` + `DB::Write()`.
- **Source:** `table/sst_file_writer.cc`, `SstFileWriter::Open`, `SstFileWriter::Put`, `SstFileWriter::Finish`; `db/external_sst_file_ingestion_job.cc`, `ExternalSstFileIngestionJob::Prepare`, `Run`, `AssignGlobalSeqnoForIngestedFile`
- **Fix:** Narrow the statement to mutating APIs built on `DB::Write()` and call out SST creation / ingestion as a separate write path that bypasses WAL + memtable insertion.

### [WRONG] The timestamp section conflates `DB` convenience APIs with raw `WriteBatch` behavior
- **File:** `01_overview.md`, User-Defined Timestamp Support
- **Claim:** "Without explicit timestamp ... a placeholder timestamp of all zeros is appended ... and the actual timestamp is assigned later via `WriteBatch::UpdateTimestamps()` before the batch is committed."
- **Reality:** That placeholder flow is implemented by `WriteBatch`, not by the `DB` convenience APIs. `DBImpl::Put`, `Delete`, `SingleDelete`, `DeleteRange`, and `Merge` reject the timestamp-less overload when the target column family has timestamps enabled. Also, `DBImpl::WriteImpl` rejects a batch that still has `needs_in_place_update_ts_` set when it is going to memtable.
- **Source:** `db/db_impl/db_impl_write.cc`, `DBImpl::Put`, `DBImpl::Delete`, `DBImpl::SingleDelete`, `DBImpl::DeleteRange`, `DBImpl::Merge`, `DBImpl::WriteImpl`; `db/write_batch.cc`, `WriteBatch::Put`, `WriteBatch::Delete`, `WriteBatch::SingleDelete`, `WriteBatch::DeleteRange`, `WriteBatch::Merge`
- **Fix:** Split the section into `DB` API semantics versus `WriteBatch` semantics. Document that deferred timestamp filling is a `WriteBatch` feature and that callers must run `UpdateTimestamps()` before `DB::Write()` unless they are in the special WAL-only transaction path.

### [MISLEADING] `WriteOptions::rate_limiter_priority` is documented too broadly
- **File:** `01_overview.md`, WriteOptions
- **Claim:** "Rate limiter priority for WAL writes. Only `IO_USER` and `IO_TOTAL` are valid."
- **Reality:** Current code only supports non-`IO_TOTAL` for automatic WAL flushes, and rejects it when `disableWAL=true` or `manual_wal_flush=true`.
- **Source:** `include/rocksdb/options.h`, `WriteOptions::rate_limiter_priority` comment; `db/db_impl/db_impl_write.cc`, `DBImpl::WriteImpl`
- **Fix:** Document the extra preconditions and note that unsupported combinations fail with `InvalidArgument`.

### [WRONG] The `Delete` versus `SingleDelete` comparison overstates how long normal delete tombstones must live
- **File:** `03_single_delete.md`, How SingleDelete Differs from Delete
- **Claim:** "Delete tombstone persists until reaching bottommost level" and "Space reclamation: Deferred until bottommost compaction"
- **Reality:** Regular delete tombstones can also be dropped before bottommost level when `KeyNotExistsBeyondOutputLevel()` and snapshot conditions allow it.
- **Source:** `db/compaction/compaction_iterator.cc`, delete-handling paths in `CompactionIterator::NextFromInput`
- **Fix:** Rephrase the comparison to say `SingleDelete` enables pairwise cancellation earlier and more often, but regular deletes also have optimized early-drop cases.

### [WRONG] The doc says two consecutive `SingleDelete`s log a warning
- **File:** `03_single_delete.md`, Anomalous Cases
- **Claim:** "A warning is logged because this pattern indicates the application may be mixing SingleDelete and Delete."
- **Reality:** The code increments mismatch counters and skips the first `SingleDelete`, but it does not log a warning in this branch.
- **Source:** `db/compaction/compaction_iterator.cc`, `CompactionIterator::NextFromInput`
- **Fix:** Say the iterator records a mismatch and skips the first `SingleDelete`; do not claim a warning is logged.

### [MISLEADING] The `SingleDelete` + `Delete` case is described as dropping both records
- **File:** `03_single_delete.md`, Anomalous Cases
- **Claim:** "When `enforce_single_del_contracts_` is enabled ... compaction fails ... When disabled ... a warning is logged and the records are dropped."
- **Reality:** With enforcement disabled, the current `SingleDelete` is dropped and the following `Delete` is left for later processing; the code does not drop both records in this branch.
- **Source:** `db/compaction/compaction_iterator.cc`, `CompactionIterator::NextFromInput`
- **Fix:** Describe the actual control flow precisely: error when enforcement is on; otherwise warn, drop the `SingleDelete`, and continue with the following entry.

### [WRONG] `GetFromBatch()` is documented as unable to resolve merge chains with an in-batch base value
- **File:** `04_write_batch_with_index.md`, GetFromBatch
- **Claim:** "If the latest entry is a Merge, returns `Status::MergeInProgress()` (cannot resolve without a base value)."
- **Reality:** `GetFromBatch()` resolves merges when the batch itself contains the base `Put`, `PutEntity`, `Delete`, or `SingleDelete`. It only returns `MergeInProgress` when the batch lacks enough history.
- **Source:** `utilities/write_batch_with_index/write_batch_with_index_internal.cc`, `WriteBatchWithIndexInternal::GetFromBatchImpl`; `utilities/write_batch_with_index/write_batch_with_index_test.cc`, `TestGetFromBatchMerge`
- **Fix:** Say that `MergeInProgress` is only returned when the batch cannot determine the base value on its own.

### [WRONG] The no-savepoint rollback behavior for WBWI is described backwards
- **File:** `04_write_batch_with_index.md`, Save Points
- **Claim:** "`RollbackToSavePoint()` in WBWI behaves differently from the base `WriteBatch` version. Instead of returning `NotFound` when there is no save point, it behaves the same as `Clear()`."
- **Reality:** WBWI delegates to `rep->write_batch.RollbackToSavePoint()`, so it returns `NotFound` when no save point exists. The test suite asserts this exact behavior.
- **Source:** `utilities/write_batch_with_index/write_batch_with_index.cc`, `WriteBatchWithIndex::RollbackToSavePoint`; `utilities/write_batch_with_index/write_batch_with_index_test.cc`, `SavePointTest`
- **Fix:** Replace the note with the actual behavior and keep the rebuild/invalidation note.

### [MISLEADING] The WBWI timestamp support table overstates what is unsupported
- **File:** `04_write_batch_with_index.md`, Unsupported Operations
- **Claim:** "`Put`/`Delete`/`SingleDelete` with user-defined timestamp | `Status::NotSupported()`"
- **Reality:** Only the overloads that take an explicit timestamp are `NotSupported`. WBWI still supports timestamp-enabled column families through the timestamp-less overloads plus `GetWriteBatch()->UpdateTimestamps(...)`, and the tests cover lookup and iteration after that flow.
- **Source:** `utilities/write_batch_with_index/write_batch_with_index.cc`, timestamp-taking overloads and regular overloads; `utilities/write_batch_with_index/write_batch_with_index_test.cc`, `ColumnFamilyWithTimestamp`, `IndexNoTs`
- **Fix:** Distinguish "explicit timestamp overloads are not supported" from "timestamp-enabled CFs are still usable through deferred timestamp filling."

### [MISLEADING] `AssociativeMergeOperator` is described as needing commutativity
- **File:** `05_merge_operator.md`, AssociativeMergeOperator
- **Claim:** "This interface is appropriate for operations where the merge function is associative and commutative in practice, such as integer addition or string concatenation."
- **Reality:** RocksDB preserves operand order. Associativity matters for this interface; commutativity is not required, and string concatenation is a counterexample to the wording because it is order-dependent.
- **Source:** `include/rocksdb/merge_operator.h`, `AssociativeMergeOperator`; `db/merge_operator.cc`, `AssociativeMergeOperator::FullMergeV2`
- **Fix:** Remove "commutative" and explain that the implementation applies operands in order.

### [MISLEADING] The merge-operator name is said not to be persisted anywhere
- **File:** `05_merge_operator.md`, Merge Operator Naming
- **Claim:** "the merge operator name is currently not stored persistently in the database metadata"
- **Reality:** The name is persisted in SST table properties as `merge_operator_name`, but RocksDB still does not enforce DB-open consistency based on that field.
- **Source:** `table/block_based/block_based_table_builder.cc`, table-properties population; `table/meta_blocks.cc`, properties serialization; `include/rocksdb/table_properties.h`, `TableProperties::merge_operator_name`
- **Fix:** Clarify that the name is persisted per SST file but not used as an enforced DB-open compatibility check.

### [MISLEADING] The `max_successive_merges` section says RocksDB reads only from the memtable
- **File:** `06_merge_implementation.md`, max_successive_merges Optimization
- **Claim:** "RocksDB attempts to read the current value from the memtable and perform a full merge immediately during the write."
- **Reality:** The write path calls `DB::GetEntity()` with a snapshot at the current sequence number. With the default `strict_max_successive_merges=false`, it avoids filesystem reads by using `kBlockCacheTier`, but it is still using the DB read path, not a memtable-only lookup. The chapter also omits the `strict_max_successive_merges` option that allows blocking I/O to stay under the threshold.
- **Source:** `db/write_batch.cc`, `MemTableInserter::MergeCF`; `include/rocksdb/advanced_options.h`, `max_successive_merges` and `strict_max_successive_merges`
- **Fix:** Document the actual read path and add `strict_max_successive_merges` as a paired option with materially different behavior.

### [UNVERIFIABLE] The docs invent a `SingleDelete`/`DeleteRange` restriction without code support
- **File:** `07_delete_range.md`, Compatibility and Restrictions
- **Claim:** "Range tombstones and `SingleDelete` should not be used on overlapping key ranges."
- **Reality:** I could not find a code check, header contract, or test documenting this as a supported restriction.
- **Source:** Searched `include/rocksdb/db.h`, `db/compaction/compaction_iterator.cc`, `db/db_range_del_test.cc`, and related range-delete / `SingleDelete` code
- **Fix:** Either remove the claim or replace it with a concrete, code-backed explanation of what combination is unsafe.

### [WRONG] The `SstFileWriter` lifecycle section says destruction without `Finish()` deletes the file
- **File:** `09_sst_file_writer.md`, Lifecycle
- **Claim:** "If the writer is destroyed without calling `Finish()`, or if `Finish()` fails, the builder is abandoned and the incomplete file is deleted."
- **Reality:** `Finish()` failure does delete the file, but the destructor only calls `builder->Abandon()`; it does not call `DeleteFile()`.
- **Source:** `table/sst_file_writer.cc`, `SstFileWriter::~SstFileWriter`, `SstFileWriter::Finish`
- **Fix:** Split the two cases. Only claim deletion for the `Finish()` failure path.

### [WRONG] The `SstFileWriter` sequence-number section says ingestion always assigns and writes a global seqno
- **File:** `09_sst_file_writer.md`, Sequence Numbers
- **Claim:** "The actual sequence number is assigned later during ingestion via the global sequence number mechanism. This is stored as a table property ... in the SST file's metablock."
- **Reality:** Non-overlapping files with no snapshot pressure can remain at seqno 0, `ingest_behind` intentionally keeps seqno 0, and with `write_global_seqno=false` any assigned seqno is tracked in the MANIFEST rather than patched into the file.
- **Source:** `table/sst_file_writer.cc`, `SstFileWriterPropertiesCollectorFactory` initialization; `db/external_sst_file_ingestion_job.cc`, `AssignLevelAndSeqnoForIngestedFile`, `AssignGlobalSeqnoForIngestedFile`; `include/rocksdb/options.h`, `IngestExternalFileOptions::write_global_seqno`
- **Fix:** Say the file is created with seqno 0 and may later be assigned a global seqno during ingestion when ordering requires it; when `write_global_seqno=false`, that assigned seqno is not written back into the SST.

### [WRONG] The ingestion fallback-to-copy rule is broader in the docs than in the code
- **File:** `10_external_file_ingestion.md`, Ingestion Workflow, Phase 1 Step 5
- **Claim:** "If linking fails and `failed_move_fall_back_to_copy=true`: fall back to copying."
- **Reality:** The fallback only happens when `LinkFile()` returns `Status::NotSupported()`. Other link failures do not trigger the copy fallback.
- **Source:** `db/external_sst_file_ingestion_job.cc`, `ExternalSstFileIngestionJob::Prepare`
- **Fix:** Document the `NotSupported` condition explicitly.

### [WRONG] The overlapping-input-files rule forgets the DB-generated-file exception
- **File:** `10_external_file_ingestion.md`, Ingestion Workflow, Phase 1 Step 3
- **Claim:** "Overlapping files require `allow_global_seqno=true`"
- **Reality:** That is true for normal external files, but not when `allow_db_generated_files=true`. In that mode, overlapping input files are allowed without global-seqno reassignment as long as ordering constraints are satisfied.
- **Source:** `db/external_sst_file_ingestion_job.cc`, `ExternalSstFileIngestionJob::Prepare`; `include/rocksdb/options.h`, `IngestExternalFileOptions::allow_global_seqno`, `allow_db_generated_files`
- **Fix:** Add the `allow_db_generated_files` exception where the rule is introduced, not only later in the options section.

## Completeness Gaps

### Missing public write APIs
- **Why it matters:** The doc set is named `public_api_write`, but several public write-facing APIs are absent or only mentioned in passing.
- **Where to look:** `include/rocksdb/db.h`, `WriteWithCallback`, `IngestWriteBatchWithIndex`, `FlushWAL`, `SyncWAL`, `LockWAL`, `UnlockWAL`; `include/rocksdb/write_batch_base.h`, `PutLogData`, `TimedPut`
- **Suggested scope:** At minimum, add an overview section that classifies these APIs and points readers to the relevant implementation or sibling docs.

### Missing `atomic_replace_range` restrictions
- **Why it matters:** The current chapter explains the feature but misses the two most operationally important caveats: `snapshot_consistency=true` is not supported, and the range upper bound currently has an inclusive/exclusive bug called out in the option comment.
- **Where to look:** `include/rocksdb/options.h`, `IngestExternalFileArg::atomic_replace_range`; `db/external_sst_file_basic_test.cc`
- **Suggested scope:** Add a prominent warning block in the external-ingestion chapter.

### Missing `verify_file_checksum` semantics
- **Why it matters:** The options table currently reduces this option to "verify file-level checksum," but the real behavior depends on whether the DB has a checksum generator and whether ingestion metadata already includes a checksum.
- **Where to look:** `include/rocksdb/options.h`, `IngestExternalFileOptions::verify_file_checksum`; `db/external_sst_file_ingestion_job.cc`, checksum generation / verification helpers
- **Suggested scope:** Expand the option table entry with the current branch behavior.

### Missing timestamp-enabled WBWI usage pattern
- **Why it matters:** The current unsupported-operations table will mislead anyone trying to use WBWI with timestamped column families, even though the supported pattern exists and is tested.
- **Where to look:** `utilities/write_batch_with_index/write_batch_with_index_test.cc`, `ColumnFamilyWithTimestamp`, `IndexNoTs`
- **Suggested scope:** Add a short subsection in the WBWI chapter describing deferred timestamp filling through the wrapped `WriteBatch`.

## Depth Issues

### External-ingestion level-placement needs the per-batch constraints, not just the per-file overlap rule
- **Current:** The chapter explains overlap checks and "deepest level that fits."
- **Missing:** Later batches in a multi-batch ingest are constrained by the earlier batch's uppermost level, FIFO forces placement to L0, and some cases force global-seqno assignment even before any DB overlap is checked.
- **Source:** `db/external_sst_file_ingestion_job.cc`, `AssignLevelAndSeqnoForIngestedFile`

### The merge chapter needs the iterator path, not only `Get()` and compaction
- **Current:** Chapter 6 says merge resolution happens during reads and compaction, but only the `Get()` read path is explained.
- **Missing:** Iterator merge resolution goes through `DBIter` and uses `MergeHelper::TimedFullMerge` as well. This matters for readers reasoning about pinned operands, iteration order, and `kMustMerge` failures.
- **Source:** `db/db_iter.cc`, merge-resolution helpers around `MergeHelper::TimedFullMerge`

### DeleteRange validation timing is underspecified
- **Current:** The docs describe `DB::DeleteRange()` as returning `InvalidArgument` when the range is backwards.
- **Missing:** For the `DB`/`WriteBatch` path, the begin/end ordering check happens during write-batch application, not in `WriteBatch::DeleteRange()` itself. The docs should be explicit about which layer validates what.
- **Source:** `db/write_batch.cc`, `MemTableInserter::DeleteRangeCF`; `db/db_impl/db_impl_write.cc`, row-cache / write-path checks

## Structure and Style Violations

### Inline code formatting rule is violated throughout the doc set
- **File:** `index.md` and all chapter files
- **Details:** The docs use pervasive inline code spans for API names, type tags, options, and even prose references, despite the requested style saying to avoid inline code quotes and instead refer to headers / structs / functions directly.

### `INVARIANT` is used for ordinary API validation rules
- **File:** `index.md`, `01_overview.md`, `09_sst_file_writer.md`
- **Details:** Examples include `sync=true` with `disableWAL=true` and strict ascending key order in `SstFileWriter`. Those are important preconditions, but not crash/corruption invariants in the style-guide sense.

## Undocumented Complexity

### Deferred timestamps in `WriteBatch` are only half the story
- **What it is:** `WriteBatch` can append placeholder timestamps and track `needs_in_place_update_ts_`, but `DB::Write()` rejects those batches unless the timestamps were filled in first, except for special WAL-only transaction paths.
- **Why it matters:** This is the exact boundary where users get confused if they move from `DB::Put()` to direct `WriteBatch` construction for timestamped column families.
- **Key source:** `db/write_batch.cc`, `WriteBatch::Put` / `Delete` / `Merge` / `DeleteRange`; `db/db_impl/db_impl_write.cc`, `DBImpl::WriteImpl`
- **Suggested placement:** Expand Chapter 1 and Chapter 2.

### External ingestion has a real compatibility matrix, not a single "bulk load" path
- **What it is:** Behavior changes materially based on overlap, snapshots, DB-generated files, `write_global_seqno`, `fill_cache`, timestamps, `ingest_behind`, and `atomic_replace_range`.
- **Why it matters:** The code has many early rejections and option interactions that users need to understand before depending on ingestion in production.
- **Key source:** `include/rocksdb/options.h`, `IngestExternalFileOptions`; `db/external_sst_file_ingestion_job.cc`
- **Suggested placement:** Split Chapter 10 into workflow plus option-interaction subsections.

### `WriteBatchWithIndex` on timestamped CFs depends on the wrapped `WriteBatch`
- **What it is:** WBWI does not support the explicit timestamp overloads for `Put` / `Delete` / `SingleDelete`, but it still works with timestamp-enabled column families by relying on the underlying `WriteBatch` placeholder-timestamp flow and later `UpdateTimestamps()`.
- **Why it matters:** Without this detail, the current unsupported-operations table sends readers in the wrong direction.
- **Key source:** `utilities/write_batch_with_index/write_batch_with_index.cc`; `utilities/write_batch_with_index/write_batch_with_index_test.cc`, `ColumnFamilyWithTimestamp`, `IndexNoTs`
- **Suggested placement:** Add to Chapter 4.

### `max_successive_merges` has a hidden second knob
- **What it is:** `strict_max_successive_merges` changes whether the eager-merge path is allowed to block on read I/O versus staying in memory / block-cache tiers only.
- **Why it matters:** This is a materially different write-latency tradeoff and should not be hidden behind a single-option explanation.
- **Key source:** `include/rocksdb/advanced_options.h`; `db/write_batch.cc`, `MemTableInserter::MergeCF`
- **Suggested placement:** Expand Chapter 6.

## Positive Notes

- The range-tombstone chapters correctly identify fragmentation and snapshot striping as the core ideas, which is the right mental model for the current implementation.
- The low-priority write chapter accurately captures the early best-effort check outside the DB mutex and the special-case exemption for 2PC commit / rollback.
- The merge chapter does a good job covering `OpFailureScope`, including the distinction between `kTryMerge` and `kMustMerge`, which is easy to miss if you only read old merge-operator docs.
