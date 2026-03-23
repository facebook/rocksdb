# Review: public_api_write -- Claude Code

## Summary
(Overall quality rating: good)

The public_api_write documentation is comprehensive and well-structured, covering 11 chapters across the full breadth of RocksDB's write API surface. The binary format descriptions, compaction interactions, and range tombstone fragmentation chapters are particularly strong. However, there are several correctness issues -- mostly wrong function/method names and one incorrect behavioral claim about WBWI rollback. The doc also has a few misleading claims about merge operator persistence and a wrong header path reference. Completeness is good but some recent value types (kTypeBlobIndex) and the TimedPut operation lack dedicated coverage.

## Correctness Issues

### [WRONG] DefinitelyNotInNextLevel() does not exist
- **File:** 03_single_delete.md, "Anomalous Cases" section
- **Claim:** "If the key doesn't appear below the current level (`DefinitelyNotInNextLevel()` returns true and we are not in `ingest_behind` mode), the SingleDelete can be dropped on its own."
- **Reality:** No method named `DefinitelyNotInNextLevel()` exists in the codebase. The actual method is `CompactionProxy::KeyNotExistsBeyondOutputLevel()` (see `compaction_iterator.h` lines 93-94). In `compaction_iterator.cc` line 837-841, the drop-SingleDelete-with-no-following-key logic uses `compaction_->KeyNotExistsBeyondOutputLevel(ikey_.user_key, &level_ptrs_)`.
- **Source:** `db/compaction/compaction_iterator.h` lines 93-94, `db/compaction/compaction_iterator.cc` lines 837-841
- **Fix:** Replace `DefinitelyNotInNextLevel()` with `KeyNotExistsBeyondOutputLevel()`

### [WRONG] WBWI RollbackToSavePoint behavior when no save point exists
- **File:** 04_write_batch_with_index.md, "Save Points" section
- **Claim:** "Note: `RollbackToSavePoint()` in WBWI behaves differently from the base `WriteBatch` version. Instead of returning `NotFound` when there is no save point, it behaves the same as `Clear()`."
- **Reality:** WBWI's `RollbackToSavePoint()` directly delegates to `rep->write_batch.RollbackToSavePoint()`, which returns `Status::NotFound()` when no save point exists. There is no special handling in WBWI to convert this to a `Clear()` operation. The behavior is identical to the base `WriteBatch`.
- **Source:** `utilities/write_batch_with_index/write_batch_with_index.cc` line 1145-1155, `db/write_batch.cc` line 1829-1831
- **Fix:** Remove the "Note:" paragraph entirely, or state that WBWI's `RollbackToSavePoint()` delegates to the base `WriteBatch` version with identical behavior (returns `Status::NotFound()` when no save point exists). Note: the WBWI header itself is contradictory -- line 358 says "behaves the same as Clear()" but line 363 says "Status::NotFound() if no previous call to SetSavePoint()". The implementation matches line 363.

### [WRONG] CompactOnDeletionCollectorFactory header path
- **File:** 03_single_delete.md, "Known Performance Pitfall" section
- **Claim:** "`CompactOnDeletionCollectorFactory` (see `NewCompactOnDeletionCollectorFactory()` in `include/rocksdb/table_properties.h`)"
- **Reality:** `NewCompactOnDeletionCollectorFactory()` is declared in `include/rocksdb/utilities/table_properties_collectors.h`, not `include/rocksdb/table_properties.h`.
- **Source:** `include/rocksdb/utilities/table_properties_collectors.h`
- **Fix:** Change path to `include/rocksdb/utilities/table_properties_collectors.h`

### [WRONG] DB::IngestWriteBatchWithIndex() API does not exist
- **File:** 04_write_batch_with_index.md, "Use in Transactions" section
- **Claim:** "WBWIMemTable can ingest a WBWI directly into the DB, bypassing the normal write path (experimental, via `DB::IngestWriteBatchWithIndex()`)"
- **Reality:** There is no public method `DB::IngestWriteBatchWithIndex()` in `include/rocksdb/db.h`. The `WBWIMemTable` class does exist (`memtable/wbwi_memtable.h`) and implements `ReadOnlyMemTable`, but the ingestion mechanism is through the transaction commit path via `TransactionOptions::commit_bypass_memtable` (see `include/rocksdb/utilities/transaction_db.h` lines 407-428), not through a standalone DB method.
- **Source:** `include/rocksdb/db.h` (no such method), `include/rocksdb/utilities/transaction_db.h` lines 407-428
- **Fix:** Replace with: "`WBWIMemTable` can ingest a WBWI directly as an immutable memtable, bypassing the normal write path (experimental, via `TransactionOptions::commit_bypass_memtable` during `Transaction::Commit()`)"

### [MISLEADING] Merge operator name not stored persistently
- **File:** 05_merge_operator.md, "Merge Operator Naming" section
- **Claim:** "the merge operator name is currently not stored persistently in the database metadata, so enforcement relies on the client providing a consistent operator."
- **Reality:** The merge operator name IS stored persistently in the OPTIONS file (via `ImmutableCFOptions` serialization in `options/cf_options.cc`). RocksDB serializes all CF options including the merge operator to the OPTIONS file on DB open/flush. The OPTIONS file is used for sanity checking on subsequent opens. What is NOT stored is the name in the MANIFEST (version edits). This claim gives the wrong impression that there is no persistence or enforcement at all.
- **Source:** `options/cf_options.cc` line 887-889 (merge_operator serialization)
- **Fix:** Replace with: "The merge operator name is stored in the OPTIONS file and verified during `DB::Open()`. However, it is not stored in the MANIFEST, so if the OPTIONS file is lost or ignored, enforcement depends on the client providing a consistent operator."

### [MISLEADING] PersistStats file location
- **File:** 11_low_priority_write.md, "Use Cases" section
- **Claim:** "RocksDB itself uses `low_pri=true` when writing persistent statistics to the stats column family (see `DBImpl::PersistStats()` in `db/db_impl/db_impl_open.cc`)."
- **Reality:** `DBImpl::PersistStats()` is defined in `db/db_impl/db_impl.cc` (line 1004), not in `db/db_impl/db_impl_open.cc`. The function does use `low_pri=true` and `no_slowdown=true` (line 1057-1058), which is correct.
- **Source:** `db/db_impl/db_impl.cc` line 1004-1060
- **Fix:** Change the source reference to `db/db_impl/db_impl.cc`

### [MISLEADING] CompactionRangeDelAggregator::NewIterator() return type
- **File:** 08_delete_range_implementation.md, "NewIterator for Compaction Output" section
- **Claim:** "CompactionRangeDelAggregator::NewIterator() produces a merged iterator over all range tombstones... It creates a TruncatedRangeDelMergingIter that merges all parent iterators"
- **Reality:** `NewIterator()` does create a `TruncatedRangeDelMergingIter` internally, but that is an anonymous-namespace class used only as an intermediate input to a new `FragmentedRangeTombstoneList`. The actual return type is `std::unique_ptr<FragmentedRangeTombstoneIterator>`. The doc gives the impression that `TruncatedRangeDelMergingIter` is what callers receive.
- **Source:** `db/range_del_aggregator.cc` lines 537-551
- **Fix:** Clarify that `NewIterator()` returns a `FragmentedRangeTombstoneIterator`, and that `TruncatedRangeDelMergingIter` is an internal step that feeds into re-fragmentation.

### [MISLEADING] max_successive_merges reads "from the memtable"
- **File:** 06_merge_implementation.md, "max_successive_merges Optimization" section
- **Claim:** "RocksDB attempts to read the current value from the memtable and perform a full merge immediately during the write"
- **Reality:** The code calls `db_->GetEntity()`, which is a full DB read path (not memtable-only). By default it uses `read_options.read_tier = kBlockCacheTier` (see `db/write_batch.cc` line 2788), which limits reads to in-memory data (memtable + block cache), avoiding disk I/O. If the value is not found in memory, the merge operand is stored normally without eager merging.
- **Source:** `db/write_batch.cc` lines 2757-2801
- **Fix:** Replace "read the current value from the memtable" with "read the current value from the DB (limited to in-memory data -- memtable and block cache -- by default to avoid disk I/O on the write path)"

### [MISLEADING] Questionable use of "Key Invariant" label
- **File:** 01_overview.md line 56, 09_sst_file_writer.md line 38
- **Claim:** Uses "Key Invariant" for enforced API constraints (`sync+disableWAL` combination, ascending key order in SstFileWriter)
- **Reality:** These are runtime-enforced API constraints that return error statuses, not true correctness invariants. A correctness invariant is one where violation causes data corruption or crashes (like the SingleDelete contract or range tombstone non-overlap guarantee). These are input validation checks.
- **Fix:** Relabel as "Constraint" or "Precondition" rather than "Key Invariant". Reserve "Invariant" for conditions where violation leads to data corruption (e.g., the SingleDelete one-Put contract and the fragmentation non-overlap property).

## Completeness Gaps

### Missing: kTypeDeletionWithTimestamp value type
- **Why it matters:** `kTypeDeletionWithTimestamp` (0x14) is a distinct value type used when `Delete()` is called with an explicit user-defined timestamp. The Chapter 1 operation types table only shows `kTypeDeletion` for Delete, and Chapter 2's encoding tables omit this tag entirely. A developer reading the code will encounter this type and find no documentation.
- **Where to look:** `db/dbformat.h` line 69 (kTypeDeletionWithTimestamp = 0x14)
- **Suggested scope:** Add a row to the Chapter 1 table and a corresponding entry in Chapter 2's record encoding tables.

### Missing: kTypeBlobIndex handling in write path
- **Why it matters:** BlobDB stores large values in blob files and uses `kTypeBlobIndex` entries in the LSM tree. This value type is not mentioned in the Chapter 1 operation types table or anywhere else in the write API docs. The `Handler::PutBlobIndexCF` callback in `WriteBatch::Handler` is also missing from the Chapter 2 handler method table.
- **Where to look:** `db/dbformat.h` (kTypeBlobIndex = 0x11, kTypeColumnFamilyBlobIndex = 0x10), `include/rocksdb/write_batch.h` (PutBlobIndexCF handler callback), `db/blob/blob_index.h`
- **Suggested scope:** Add a row to the Chapter 1 operation types table noting BlobDB's internal use of kTypeBlobIndex, add PutBlobIndexCF to the Chapter 2 handler table, and mention in Chapter 6 that MergeUntil handles kTypeBlobIndex by fetching blob values.

### Missing: TimedPut detailed coverage
- **Why it matters:** TimedPut (`kTypeValuePreferredSeqno`) is mentioned in the overview table but has no dedicated section explaining its semantics, value packing format (value + write_time), limitations (not compatible with UDT or wide columns, experimental, can break snapshot immutability), or interaction with compaction (where it gets converted to kTypeValue). The `WriteBatchBase::TimedPut` declaration in `write_batch_base.h` has a substantial doc comment not reflected in any chapter.
- **Where to look:** `include/rocksdb/write_batch_base.h` lines 45-59 (API + semantics), `db/write_batch.cc` (TimedPutCF encoding), `include/rocksdb/db.h` (DB::TimedPut), `db/compaction/compaction_iterator.cc` (TimedPut handling)
- **Suggested scope:** Either a dedicated section in Chapter 1 or a brief paragraph explaining the packing format, limitations, and the compaction-time conversion to kTypeValue.

### Missing: WBWI utility methods not documented in Chapter 4
- **Why it matters:** Several public WBWI methods are not covered: `GetEntityFromBatch()` (batch-only entity read), `GetDataSize()`, `GetCFStats()`, `GetWBWIOpCount()`, `GetOverwriteKey()`. The `GetEntityFromBatch()` method is particularly important since the related `GetFromBatch()` IS documented.
- **Where to look:** `include/rocksdb/utilities/write_batch_with_index.h` lines 267-268, 374-384
- **Suggested scope:** Add `GetEntityFromBatch()` next to the existing `GetFromBatch` section. Briefly mention the utility accessors.

### Missing: WriteBatch::Append() and related batch manipulation methods
- **Why it matters:** `WriteBatch::Append()` allows merging one batch into another, which is used in transaction commit paths. Not documented in Chapter 2.
- **Where to look:** `include/rocksdb/write_batch.h` (Append methods), `db/write_batch.cc`
- **Suggested scope:** Brief mention in Chapter 2 alongside the existing encoding/decoding coverage.

### Missing: DB::Write() group commit details
- **Why it matters:** Chapter 1 mentions "Group commit merges concurrent DB::Write() calls into a single WAL write" but doesn't explain the WriteThread mechanics (leader election, follower joining, pipelining). This is one of the most important performance aspects of the write path.
- **Where to look:** `db/write_thread.h`, `db/write_thread.cc`, `db/db_impl/db_impl_write.cc`
- **Suggested scope:** Either expand Chapter 1's "Write Path Overview" or create a dedicated chapter. The write_flow.md doc covers this, so a cross-reference may suffice.

### Missing: DB::WriteWithCallback and WriteCallback
- **Why it matters:** `DB::WriteWithCallback()` (via `DB::Write()` overload taking `WriteCallback*`) is the mechanism used by transactions for write-conflict detection. Not mentioned in any chapter.
- **Where to look:** `include/rocksdb/db.h`, `include/rocksdb/write_callback.h`
- **Suggested scope:** Brief mention in Chapter 1 with a note that this is primarily used by the transaction layer.

## Depth Issues

### SingleDelete compaction behavior needs more precision on "Optimization 3"
- **Current:** Chapter 3 describes "Optimization 3" as clearing the Put's value when Rule 2 is not satisfied.
- **Missing:** The doc doesn't mention that Optimization 3 only applies when `is_timestamp_eligible_for_gc` is true (when timestamps are disabled or the timestamp is below `full_history_ts_low_`). The conditions for applying this optimization are more nuanced than described.
- **Source:** `db/compaction/compaction_iterator.cc` lines 666-691, 695-697

### MergeUntil stop_before semantics
- **Current:** Chapter 6 says "MergeUntil() stops at the `stop_before` sequence number, which represents the oldest snapshot that must be preserved."
- **Missing:** The actual parameter is `stop_before` which is passed from the compaction iterator. The doc doesn't clarify that this is the sequence number below which entries belong to a different snapshot stripe, and that different invocations of MergeUntil may get different `stop_before` values for the same key depending on snapshot boundaries.
- **Source:** `db/merge_helper.h` MergeUntil signature, `db/merge_helper.cc` MergeUntil implementation

### External file ingestion: DivideInputFilesIntoBatches algorithm
- **Current:** Chapter 10 mentions "divided into non-overlapping batches" but doesn't explain the algorithm.
- **Missing:** The batching logic assigns each file to the first batch where it doesn't overlap with any existing file in that batch. This means the order of input files matters for batch formation.
- **Source:** `db/external_sst_file_ingestion_job.cc` DivideInputFilesIntoBatches()

## Structure and Style Violations

### index.md line count is acceptable
- **File:** index.md
- **Details:** 43 lines, within the 40-80 line target range. Good.

### No box-drawing characters found
- **Details:** Clean across all files.

### No line number references found
- **Details:** Clean across all files.

### Invariant usage needs tightening
- **File:** 01_overview.md, 09_sst_file_writer.md, 10_external_file_ingestion.md, index.md
- **Details:** "Key Invariant" is used for runtime-enforced API constraints (sync+disableWAL, ascending key order, db-generated file overlap check) which are not true correctness invariants. True correctness invariants in this doc: SingleDelete one-Put contract (03), range tombstone non-overlap after fragmentation (08). The others should be labeled "Constraint" or "Precondition."

## Undocumented Complexity

### WriteBatch HasXxx methods can trigger full iteration
- **What it is:** When a WriteBatch is constructed from a serialized string (WAL recovery), `content_flags_` is set to `DEFERRED`. The first call to any `HasPut()`, `HasDelete()`, etc. method triggers `ComputeContentFlags()`, which iterates the entire batch. This can be expensive for large batches.
- **Why it matters:** Users relying on `HasPut()` checks during recovery might unknowingly trigger O(n) scans.
- **Key source:** `db/write_batch.cc` ComputeContentFlags(), `include/rocksdb/write_batch.h` HasPut() etc.
- **Suggested placement:** Add to Chapter 2, "Content Flags" section.

### WriteBatch count overflow to kMaxCount
- **What it is:** When the write batch count reaches `kMaxCount` (0xFFFFFFFF), additional entries are silently accepted but the count stays at kMaxCount. This affects iteration which checks count against entries found.
- **Why it matters:** Extremely large batches (>4B entries) would have incorrect count semantics.
- **Key source:** `db/write_batch_internal.h` kMaxCount, `db/write_batch.cc` SetCount logic
- **Suggested placement:** Mention in Chapter 2, "Size Limits" section.

### SstFileWriter: file_checksum generation
- **What it is:** SstFileWriter can generate file checksums during Finish() if a `file_checksum_gen_factory` is configured in the Options. The checksum and function name are stored in ExternalSstFileInfo and verified during ingestion.
- **Why it matters:** Users doing ingestion with checksum verification need to ensure the writer was configured with the same checksum factory.
- **Key source:** `table/sst_file_writer.cc` Finish(), `include/rocksdb/sst_file_writer.h` ExternalSstFileInfo
- **Suggested placement:** Add to Chapter 9, mention alongside the existing ExternalSstFileInfo table.

### External ingestion: concurrent ingestion mutual exclusion
- **What it is:** Multiple concurrent IngestExternalFile calls are serialized through the write thread mechanism and DB mutex. The implementation enters both the write thread and the non-memory write thread to become the sole writer during Run().
- **Why it matters:** Users doing parallel ingestion from multiple threads should understand the serialization bottleneck.
- **Key source:** `db/db_impl/db_impl.cc` IngestExternalFiles() locking sequence
- **Suggested placement:** Add a note to Chapter 10 under "Phase 2: Flush Check and Run".

## Positive Notes

- The binary format table in Chapter 2 (record encoding for all 7 operation types with both CF variants) is exceptionally well done -- precise and complete.
- The range tombstone fragmentation algorithm walkthrough in Chapter 8 with the concrete example (`[a,e)@5` and `[c,g)@3`) makes a complex algorithm accessible.
- The SingleDelete compaction behavior description in Chapter 3 correctly identifies Rule 1 and Rule 2 and the peek-ahead strategy, matching the actual code structure.
- The separation of Merge Operator (interface, Chapter 5) from Merge Implementation (runtime, Chapter 6) is a good structural decision that mirrors the code organization.
- The SstFileWriter compression selection priority order (Chapter 9) is correct and well-explained.
- The low-priority write throttling chapter (11) accurately captures the interaction with 2PC commit/rollback bypass, which is a subtle implementation detail.
- The ExternalFileIngestion chapter (10) covers the three-phase workflow accurately and the level placement algorithm description matches the code.
- Content flags, save points, and per-key protection in Chapter 2 are all correctly described with accurate field names and types.
