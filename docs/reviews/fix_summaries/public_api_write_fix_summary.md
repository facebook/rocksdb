# Fix Summary: public_api_write

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 14 |
| Completeness | 3 |
| Structure/Style | 3 |
| Total | 20 |

## Disagreements Found

3 disagreements documented in `public_api_write_debates.md`:
1. **Merge operator name persistence** -- Both reviewers were partially right; name is stored in OPTIONS file AND SST properties
2. **SingleDelete/DeleteRange restriction** -- Codex correctly identified as unverifiable; reworded
3. **WBWI RollbackToSavePoint** -- Both agreed the doc was wrong; removed incorrect claim

## Changes Made

### index.md
- Narrowed "all writes flow through WriteBatch" to exclude SstFileWriter/ingestion path
- Relabeled "Key Invariants" section to "Key Invariants and Constraints" with proper categorization

### 01_overview.md
- Narrowed write path claim to exclude SstFileWriter/ingestion
- Fixed timestamp section to distinguish DB API behavior from WriteBatch behavior
- Added preconditions for `rate_limiter_priority` option
- Relabeled sync/disableWAL from "Key Invariant" to "Constraint"

### 03_single_delete.md
- Fixed `DefinitelyNotInNextLevel()` to `KeyNotExistsBeyondOutputLevel()`
- Fixed `CompactOnDeletionCollectorFactory` header path to `include/rocksdb/utilities/table_properties_collectors.h`
- Corrected Delete tombstone lifetime comparison (can be dropped before bottommost)
- Fixed two-consecutive-SingleDeletes case: no warning logged, counters incremented
- Fixed SingleDelete+Delete case: only SingleDelete dropped, Delete left for later
- Added note about regular Delete early-drop optimization

### 04_write_batch_with_index.md
- Fixed `GetFromBatch()` merge resolution: CAN resolve merges with in-batch base value
- Removed incorrect "Note" about WBWI RollbackToSavePoint behaving as Clear()
- Fixed `DB::IngestWriteBatchWithIndex()` to correct mechanism (`TransactionOptions::commit_bypass_memtable`)
- Clarified WBWI timestamp support: explicit overloads unsupported, but deferred filling works
- Added note about timestamp-enabled CF usage pattern

### 05_merge_operator.md
- Removed "commutative" from AssociativeMergeOperator description
- Fixed merge operator name persistence: stored in OPTIONS file (verified at open) AND SST properties (informational)

### 06_merge_implementation.md
- Fixed `max_successive_merges` read path: uses `DB::GetEntity()` with `kBlockCacheTier`, not memtable-only
- Added documentation for `strict_max_successive_merges` paired option

### 07_delete_range.md
- Reworded SingleDelete/DeleteRange restriction as not explicitly enforced but potentially unsafe

### 08_delete_range_implementation.md
- Clarified `CompactionRangeDelAggregator::NewIterator()` returns `FragmentedRangeTombstoneIterator`, not `TruncatedRangeDelMergingIter`

### 09_sst_file_writer.md
- Relabeled "Key Invariant" to "Constraint" for ascending key order
- Split destructor vs Finish() failure: destructor does NOT delete file, only Finish() failure does
- Clarified sequence number assignment: files may remain at seqno 0; `write_global_seqno=false` tracks in MANIFEST

### 10_external_file_ingestion.md
- Added `allow_db_generated_files` exception to overlapping input files rule
- Fixed link fallback: only triggers on `Status::NotSupported()`, not any link failure

### 11_low_priority_write.md
- Fixed PersistStats file location: `db/db_impl/db_impl.cc`, not `db/db_impl/db_impl_open.cc`
