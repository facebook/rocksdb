# Fix Summary: iterator

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 12 |
| Completeness | 8 |
| Structure/Style | 1 (inline code quotes stripped from all 15 files) |
| Depth | 3 |

## Disagreements Found

2 disagreements documented in iterator_debates.md:
1. kTypeMaxValid sorting order (CC wrong, Codex correct) -- high risk
2. auto_prefix_mode same-prefix requirements (CC wrong, Codex correct) -- medium risk

## Changes Made

| File | Changes |
|------|---------|
| index.md | Fixed layered tree description to scope to non-tailing iterators and note arena allocation limits. Fixed kTypeMaxValid sorting claim (sorts before, not after). Stripped backticks. |
| 01_hierarchy_and_assembly.md | Scoped iterator tree to non-tailing path with cross-ref to ch12. Clarified arena allocation only covers root skeleton, not lazily opened SST iterators. Fixed SuperVersion ownership: cleanup on internal iterator, not ArenaWrappedDBIter pointer. Stripped backticks. |
| 02_dbiter.md | Added TempPinData() context for value pinning nuance. Expanded memtable flush trigger: invisible entries (not just hidden), two-option dependency, tailing limitation. Stripped backticks. |
| 03_merging_iterator.md | Fixed kTypeMaxValid ordering explanation: sorts before point keys (start < end < point). Stripped backticks. |
| 04_block_based_table_iterator.md | Fixed index iterator class name: IndexBlockIter created by BinarySearchIndexReader/PartitionedIndexReader, not BinarySearchIndexIter. Narrowed readahead state transfer to sequential forward path with adaptive_readahead. Stripped backticks. |
| 05_level_iterator.md | Replaced FileIndexer with LevelFilesBrief + FindFile(). Narrowed readahead state transfer to sequential forward path. Noted file iterators are heap-allocated (arena=nullptr). Stripped backticks. |
| 06_seek_and_iteration.md | Fixed Next()/Prev() error semantics: UB precondition, not error propagation. Fixed SeekForPrev target: seq=0, not snapshot sequence. Added upper-bound clamping. Fixed SeekToLast: delegates to SeekForPrev(*upper_bound). Stripped backticks. |
| 07_prefix_seek.md | Rewrote auto_prefix_mode: same-prefix vs adjacent-prefix as two separate cases. Removed wrong claims about requiring IsSameLengthImmediateSuccessor and fixed/capped-only. Fixed prefix extractor mismatch: recreate-from-properties as main behavior. Added prefix_same_as_start silent disable note. Stripped backticks. |
| 08_bounds_and_readahead.md | Fixed upper bound semantics: SeekToLast delegates to SeekForPrev, SeekForPrev clamps. Added manual prefix caveat. Added num_file_reads_for_auto_readahead option. Added readahead zero-value/sanitization notes. Narrowed adaptive readahead to sequential forward path. Stripped backticks. |
| 09_pinning_and_resources.md | Expanded auto-refresh: MaybeAutoRefresh copies key, rebuilds stack, reseeks to reconcile. Stripped backticks. |
| 10_range_tombstones.md | Fixed kTypeMaxValid ordering: sorts before point keys (start < end < point). Stripped backticks. |
| 11_multi_cf_iterators.md | Added construction validation section (empty CF vectors, mixed comparators). Stripped backticks. |
| 12_tailing_iterator.md | Stripped backticks (no correctness issues). |
| 13_multiscan.md | Rewrote workflow as layered pipeline: DBIter -> MergingIterator -> LevelIterator -> BlockBasedTableIterator. Fixed fallback: forward reseeks tolerated, only backward resets. Added exhaustion error. Stripped backticks. |
| 14_performance_tuning.md | Added missing ReadOptions table: read_tier, verify_checksums, rate_limiter_priority, deadline, io_timeout. Added max_sequential_skip_in_iterations config details. Stripped backticks. |
