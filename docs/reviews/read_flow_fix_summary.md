# Fix Summary: read_flow

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 14 |
| Completeness | 8 |
| Structure/Style | 2 |
| Depth | 3 |
| **Total** | **27** |

## Disagreements Found

0 direct contradictions between CC and Codex. 2 near-disagreements where reviewers approached the same issue from different angles (SuperVersion cleanup, kPersistedTier scope). Details in `read_flow_debates.md`.

## Changes Made

### 01_point_lookup.md
- Added 3 missing GetImplOptions fields: `value_found`, `get_value`, `number_of_operands`
- Fixed LookupKey format: timestamp is embedded within user_key bytes, not a separate field
- Fixed DB_GET: marked as histogram (latency distribution), not ticker
- Added Step 2a: FailIfReadCollapsedHistory check after SuperVersion acquisition
- Added Step 2b: GetWithTimestampReadCallback installation for UDT
- Added row cache description to Step 7 (SST file search)

### 02_multiget.md
- Added coroutine gating caveats (disabled for L0, requires async_io, row cache interaction)
- Added MultiGetEntity section
- Added cross-CF snapshot coordination (MultiCFSnapshot) section

### 03_superversion_and_snapshots.md
- Fixed cleanup description: Cleanup() is immediate under mutex, deletion may be deferred to background purge
- Fixed Step 4 in SuperVersion Installation to reflect immediate vs deferred distinction

### 04_memtable_lookup.md
- Fixed IsEmpty() description: returns true (not false) when memtable has no entries
- Added bloom filter priority note: whole_key_filtering takes precedence over prefix for Get()

### 05_sst_file_lookup.md
- Fixed epoch_number: tracks ordering for flushed, ingested, and imported files (not just flush-time)
- Clarified IsFilterSkipped "bottommost non-empty level" meaning
- Added kUnexpectedBlobIndex and kMergeOperatorFailed to Version::Get result processing table

### 06_block_cache.md
- Fixed CacheItemHelper callback names to actual field names: del_cb, size_cb, saveto_cb, create_cb
- Fixed kPersistedTier: Get/MultiGet only, conditional memtable skip, NotSupported for iterators
- Fixed cache_index_and_filter_blocks: partition blocks always use block cache regardless

### 07_iterator_scan.md
- Fixed max_sequential_skip_in_iterations location: AdvancedColumnFamilyOptions, not DBOptions
- Fixed prefix_seek_opt_in_only function reference: Init(), not SetIterUnderDBIter()
- Rewrote auto-refresh iterator section: not periodic, triggered by SV change detection, requires explicit snapshot
- Added auto_prefix_mode memtable limitation note
- Fixed filter_block_reader_common.cc to full path
- Added NewMultiScan section

### 08_range_deletions.md
- Fixed early termination description: check happens BEFORE each file search (top of loop), not after

### 10_prefetching_and_async_io.md
- Removed unsourced hard latency/throughput numbers, replaced with qualitative descriptions

### 11_read_options_and_tuning.md
- Fixed max_sequential_skip_in_iterations location to AdvancedColumnFamilyOptions
- Fixed auto_refresh_iterator description: not periodic, requires explicit snapshot
- Fixed kPersistedTier description: Get/MultiGet only
- Fixed cache_index_and_filter_blocks: noted partition block exception
- Fixed auto_prefix_mode: added memtable limitation and prefix_seek_opt_in_only description
- Added merge_operand_count_threshold to options table
- Removed unsourced latency numbers from Common Read Patterns
- Removed specific "2-3" key comparison count from Iterator Next() description

### index.md
- Updated prefix seek description to note memtable limitation and prefix_seek_opt_in_only gating
