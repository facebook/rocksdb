# Fix Summary: filter

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 13 |
| Completeness | 4 |
| Structure/Style | 3 |
| **Total** | **20** |

## Disagreements Found

0 direct disagreements. 2 near-debates documented in filter_debates.md (LegacyBloom threshold semantics, corruption handling). Both were cases where one reviewer caught something the other didn't, with no contradiction.

## Changes Made

### 02_fast_local_bloom.md
- Fixed num_probes table: added missing entries for 9-12 probes and formula range 13-23
- Fixed LegacyBloom FP threshold: changed "above ~3 million keys" to "~40 million keys at 10 bits/key; warning at >= 3 million"
- Fixed LegacyBloom delta explanation: changed "delta = 0" to "increment being zero within cache-line address space"

### 04_full_filter_blocks.md
- Fixed AddWithPrevKey: documented that full filter ignores prev_key and delegates to Add()
- Fixed Finish(): removed claim about MaybePostVerify() being called in Finish(); documented that post-verification happens in WriteMaybeCompressedBlock()
- Fixed UpdateFilterSizeEstimate: removed thread-safety claim (applies to partitioned, not full filter)
- Fixed Filter Block Creation: corrected !use_cache behavior (reads into reader-owned memory, not block cache)

### 05_partitioned_filter_blocks.md
- Fixed SanitizeOptions reference: changed to InitializeOptions()
- Fixed error handling: partitioned filter construction latches first error and stops the build, does NOT emit always-true filters for subsequent partitions

### 06_construction_pipeline.md
- Fixed AddKeyAndAlt dedup list: corrected count from "four-way" to "five-way", reordered bullets to match code comment's k/a notation
- Fixed corruption detection: documented that corruption stops the build rather than degrading to always-true; distinguished full filter vs partitioned filter post-verification timing
- Added note about AddKey() after AddKeyAndAlt() only deduping against prev key hash

### 07_storage_format.md
- Fixed metaindex probe order: fullfilter. is checked first, then partitionedfilter., then filter.

### 08_memtable_bloom.md
- Fixed bloom allocation: no DynamicBloom is allocated when neither prefix_extractor nor memtable_whole_key_filtering is set
- Added memtable_prefix_bloom_size_ratio sanitization to [0, 0.25]
- Added db/memtable.cc and include/rocksdb/advanced_options.h to Files: line

### 09_prefix_filtering.md
- Fixed prefix extractor compatibility: split behavior by operation type (point queries vs iterator/seek)
- Documented SST-local extractor reconstruction and RangeMayExist() compatibility check for iterators
- Added filter_block_reader_common.cc to Files: line

### 10_caching_and_pinning.md
- Fixed cache_index_and_filter_blocks default: changed from true to false
- Fixed false semantics: distinguished unpartitioned (reader-owned) from partitioned (partitions still use block cache)
- Fixed kFlushedAndSimilar description: added note about L0-plus-size heuristic including ingested/intra-L0 outputs

### 11_monitoring_and_best_practices.md
- Fixed FP rate formula: labeled as "whole-key point lookups only", added separate guidance for prefix and seek counters
- Fixed format_version < 5 pitfall: corrected threshold from ~3M to ~40M with proper warning vs degradation distinction
