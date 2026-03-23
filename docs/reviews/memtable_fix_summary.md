# Fix Summary: memtable

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness (wrong option scope/header) | 10 |
| Correctness (wrong behavioral claim) | 7 |
| Completeness (missing features/coverage) | 7 |
| Depth (insufficient detail) | 4 |
| Style | 0 |
| **Total** | **28** |

## Disagreements Found

None. CC and Codex raised different issues without contradicting each other. Where they overlapped (option scopes, ReadOnlyMemTable, MemPurge, VectorRep), they agreed on the problems. No debates.md file created.

## Changes Made

### index.md
- Fixed FIFO flush ordering invariant: clarified that manifest commit order is enforced, but SST writing may overlap

### 01_representations.md
- Fixed SkipListFactory max height: "12 (internal default, not user-configurable)" instead of "32 (configurable default 12)"
- Fixed VectorRep: sorting is lazy on first seek, not on MarkReadOnly(). Get() copies+sorts while mutable
- Fixed hash rep full-order iteration: "rebuilds sorted view" instead of "merge across buckets"
- Fixed comparison table to match
- Added ReadOnlyMemTable / WBWIMemTable section

### 02_inlineskiplist.md
- Fixed memtable_insert_with_hint_prefix_extractor scope: AdvancedColumnFamilyOptions, not ImmutableDBOptions
- Added note: hint optimization only applies on non-concurrent path, point entries only
- Fixed memtable_batch_lookup_optimization scope: AdvancedColumnFamilyOptions, not ImmutableDBOptions
- Added note: finger search is skiplist-specific

### 03_arena_allocation.md
- Fixed arena_block_size default: added min(1 MB, ...) cap and 4 KB alignment
- Fixed shard block size: min(128 KB, block_size / 8), not hw concurrency dependent

### 04_insert_path.md
- Added concurrent delete counting asymmetry note (only kTypeDeletion counted in concurrent path)

### 05_lookup_path.md
- Fixed memtable_batch_lookup_optimization scope reference
- Added note that finger search is skiplist-specific

### 06_bloom_filter.md
- Added timestamp stripping note for bloom filter operations with UDT

### 07_flush_triggers.md
- Fixed write_buffer_size header: options.h, not advanced_options.h
- Fixed memtable_max_range_deletions header: options.h, not advanced_options.h
- Broadened MarkForFlush trigger to include iterator-driven flushes
- Added ShouldFlushNow range_del_table assertion note
- Added full iterator-driven scan flush trigger section
- Added MemPurge section

### 08_immutable_list.md
- Fixed flush ordering: clarified selection vs manifest commit order, in-progress gap behavior
- Fixed rollback: explained why younger completed memtables get rolled back
- Fixed max_write_buffer_size_to_maintain scope: AdvancedColumnFamilyOptions
- Added ReadOnlyMemTable / WBWIMemTable note

### 09_concurrent_writes.md
- Fixed: non-supporting reps are rejected at validation, not silently downgraded

### 10_range_tombstones.md
- Fixed flush interaction: uses NewRangeTombstoneIterator / NewTimestampStrippingRangeTombstoneIterator, not raw MemTableIterator

### 11_inplace_updates.md
- Fixed inplace_update_support scope: AdvancedColumnFamilyOptions, not DBOptions
- Fixed inplace_callback scope: AdvancedColumnFamilyOptions, not Options
- Fixed: concurrent writes + in-place updates is a hard incompatibility, not "careful lock ordering"

### 12_data_integrity.md
- Added: memtable_veirfy_per_key_checksum_on_seek depends on memtable_protection_bytes_per_key > 0

### 13_configuration.md
- Fixed arena_block_size default: added min/alignment formula
- Fixed memtable_insert_with_hint_prefix_extractor scope: AdvancedColumnFamilyOptions
- Fixed inplace_update_support scope: AdvancedColumnFamilyOptions
- Fixed memtable_batch_lookup_optimization scope: AdvancedColumnFamilyOptions
- Fixed max_write_buffer_size_to_maintain scope: AdvancedColumnFamilyOptions
- Added scan flush trigger options to Flush Control table
- Added experimental_mempurge_threshold to Flush Control table
- Added Option Sanitization section with rules from SanitizeCfOptions
