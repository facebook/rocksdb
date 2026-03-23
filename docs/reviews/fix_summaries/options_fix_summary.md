# Fix Summary: options

## Issues Fixed

- **Correctness: 5 fixes**
  - `level_compaction_dynamic_level_bytes` default changed from `false` to `true` (03_cf_options.md)
  - `PrepareForBulkLoad` table: `max_write_buffer_number` corrected from 2 to 6, added 8 missing options (05_tuning_guide.md)
  - `OptimizeForPointLookup`: removed false claims about `HashSkipListRepFactory` and `cache_index_and_filter_blocks`; documented actual behavior (kDataBlockBinaryAndHash, memtable_prefix_bloom, memtable_whole_key_filtering) (05_tuning_guide.md)
  - `strict_bytes_per_sync` mutable column changed from No to Yes (02_db_options.md)
  - `level_compaction_dynamic_level_bytes` in Step 3 tuning guide: noted it is enabled by default (05_tuning_guide.md)

- **Completeness: 4 fixes**
  - Added `use_kv_ratio_compaction` and `max_data_files_size` precedence note to FIFO compaction section (03_cf_options.md)
  - Added `max_read_amp` to universal compaction description (03_cf_options.md)
  - Added `memtable_veirfy_per_key_checksum_on_seek` and `memtable_batch_lookup_optimization` fields (03_cf_options.md)
  - Clarified `block_cache` serialization in OPTIONS files: config is serializable, state is not; documented `LoadLatestOptions()` cache parameter (08_options_file.md)

- **Structure: 1 fix**
  - Moved 3 non-invariant items from "Key Invariants" to new "Key Characteristics" section in index.md

- **Depth: 1 fix**
  - `OptimizeForSmallDb` description expanded: added 16MB cache size, `cache_index_and_filter_blocks`, reduced compaction byte limits, thread/file settings (05_tuning_guide.md)

## Reviewer Errors (CC was wrong, no fix applied)

1. CC claimed `OptimizeForSmallDb` does NOT set two-level index search. **Code confirms it does** set `kTwoLevelIndexSearch` with comment "Two level iterator to avoid LRU cache imbalance". Doc was correct; no change made.

2. CC claimed `flush_verify_memtable_count` is NOT deprecated. **Source code comment reads** "DEPRECATED: This option might be removed in a future release." Doc label is correct; no change made.

## Issues Not Addressed (out of scope or low priority)

- 22 undocumented DBOptions fields: large scope, deferred to separate pass
- ReadOptions/WriteOptions coverage: out of scope for this component doc
- CompactionOptionsUniversal full field table: deferred
- max_total_wal_size single-CF behavior clarification: minor, current text acceptable
- Compression validation error code nuance: minor, current text acceptable
- Mutable BlockBasedTableOptions enumeration: requires extensive verification, deferred

## Changes Made

| File | What Changed |
|------|-------------|
| `03_cf_options.md` | Fixed `level_compaction_dynamic_level_bytes` default; added `use_kv_ratio_compaction` + precedence note; added `max_read_amp`; added 2 missing memtable fields |
| `05_tuning_guide.md` | Fixed `PrepareForBulkLoad` table (13 options); fixed `OptimizeForPointLookup`; expanded `OptimizeForSmallDb`; noted `dynamic_level_bytes` is default |
| `02_db_options.md` | Fixed `strict_bytes_per_sync` mutability |
| `08_options_file.md` | Clarified `block_cache` serialization behavior |
| `index.md` | Split Key Invariants into Invariants + Characteristics |
