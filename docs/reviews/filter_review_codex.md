# Review: filter — Codex

## Summary
Overall quality rating: significant issues.

The doc set has good breadth and a usable shape. The index is within the requested size budget, the chapter split is sensible, and the docs do cover the major sub-areas: policy selection, full vs. partitioned SST filters, memtable bloom, storage format, and monitoring. A few explanations are genuinely useful, especially the `AddKeyAndAlt` de-dup framing and the correctness motivation for cross-partition prefix duplication.

The main problem is that several of the highest-value semantics are wrong in ways that will mislead maintainers working on real bugs: prefix-extractor changes, cache/pinning behavior, corruption-handling behavior, and the differences between full and partitioned filter workflows. There are also important tested behaviors the docs do not explain at all, especially iterator filtering after prefix-extractor changes, memtable MultiGet behavior with range tombstones, and seek-specific filter statistics.

## Correctness Issues

### [WRONG] Changed `prefix_extractor` does not simply disable prefix filtering for old SSTs
- **File:** `docs/components/filter/09_prefix_filtering.md` / Prefix Extractor Compatibility
- **Claim:** "If incompatible, the table marks prefix filtering as unavailable. Queries fall through without prefix filter checks."
- **Reality:** The reader does not just disable prefix filtering at open. If the current extractor differs from the SST's stored extractor, `BlockBasedTable::Open()` tries to reconstruct the SST-local extractor from `TableProperties::prefix_extractor_name` and stores it in `table_prefix_extractor`. Iterator/prefix-seek filtering can still use old SST filters when `RangeMayExist()` determines the start key and upper bound are compatible with that SST-local extractor. Point `Get`/`MultiGet` with `whole_key_filtering = false` are a different path and do skip prefix filtering when the current extractor changed.
- **Source:** `table/block_based/block_based_table_reader.cc` `BlockBasedTable::Open`; `table/block_based/block_based_table_reader.cc` `BlockBasedTable::PrefixMayMatch`; `table/block_based/block_based_table_reader.cc` `BlockBasedTable::PrefixExtractorChanged`; `table/block_based/filter_block_reader_common.cc` `FilterBlockReaderCommon<TBlocklike>::RangeMayExist`; `table/block_based/filter_block_reader_common.cc` `FilterBlockReaderCommon<TBlocklike>::IsFilterCompatible`; `db/db_bloom_filter_test.cc` `DynamicBloomFilterUpperBound`; `db/db_bloom_filter_test.cc` `DynamicBloomFilterMultipleSST`
- **Fix:** Split the docs by operation type. Document that `Get`/`MultiGet` and iterator prefix filtering have different behavior after a prefix-extractor change, and explain the SST-local extractor reconstruction plus upper-bound compatibility check.

### [WRONG] `cache_index_and_filter_blocks` is documented with the wrong default and the wrong `false` semantics
- **File:** `docs/components/filter/10_caching_and_pinning.md` / `cache_index_and_filter_blocks`
- **Claim:** "`BlockBasedTableOptions::cache_index_and_filter_blocks` (default `true`) controls whether filters are stored in the block cache" and "`false` -- Filters are loaded into the table reader's memory, pinned for the table's lifetime, and not managed by the block cache."
- **Reality:** The public default is `false`, not `true`. Also, `false` does not mean "no filter block cache usage" across the board. Unpartitioned full filters are loaded into table-reader memory outside the cache, but partitioned filter partitions still use the block-cache path when a block cache exists; only the top-level partition index/filter block follows the unpartitioned reader-memory path.
- **Source:** `include/rocksdb/table.h` `BlockBasedTableOptions::cache_index_and_filter_blocks`; `include/rocksdb/table.h` `BlockBasedTableOptions::partition_filters`; `table/block_based/full_filter_block.cc` `FullFilterBlockReader::Create`; `table/block_based/partitioned_filter_block.cc` `PartitionedFilterBlockReader::GetFilterPartitionBlock`; `table/block_based/partitioned_filter_block.cc` `PartitionedFilterBlockReader::CacheDependencies`
- **Fix:** Correct the default to `false` and explicitly separate unpartitioned filter behavior from partitioned-filter-partition behavior.

### [WRONG] The full-filter open path is described as loading into block cache even when cache use is disabled
- **File:** `docs/components/filter/04_full_filter_blocks.md` / Filter Block Creation
- **Claim:** "If `prefetch` or `!use_cache`, read the filter block into the block cache."
- **Reality:** `!use_cache` does not populate the block cache. In that case, `FullFilterBlockReader::Create()` eagerly reads the filter into a reader-owned `CachableEntry` outside the cache. Only the `use_cache == true` path inserts/looks up the filter in block cache.
- **Source:** `table/block_based/full_filter_block.cc` `FullFilterBlockReader::Create`; `table/block_based/filter_block_reader_common.cc` `FilterBlockReaderCommon<TBlocklike>::ReadFilterBlock`
- **Fix:** Say that `!use_cache` forces eager loading into table-reader memory, while `prefetch` only affects the cached path.

### [WRONG] Corruption detection is documented as a graceful "always-true" degradation, but RocksDB treats it as build failure
- **File:** `docs/components/filter/06_construction_pipeline.md` / Corruption Detection
- **Claim:** "On mismatch, returns `Status::Corruption` and the filter degrades to always-true."
- **Reality:** The builder may internally return always-true bytes together with a corruption status, but RocksDB does not silently keep using that filter. `WriteFilterBlock()` stops on `Status::Corruption`, and post-verification failures in `WriteMaybeCompressedBlock()` also fail the table build / flush. The tested behavior is that `Flush()` returns corruption when `detect_filter_construct_corruption` is enabled and the builder is tampered with.
- **Source:** `table/block_based/filter_policy.cc` `XXPH3FilterBitsBuilder::MaybeVerifyHashEntriesChecksum`; `table/block_based/filter_policy.cc` `FastLocalBloomBitsBuilder::Finish`; `table/block_based/filter_policy.cc` `Standard128RibbonBitsBuilder::Finish`; `table/block_based/block_based_table_builder.cc` `WriteFilterBlock`; `table/block_based/block_based_table_builder.cc` `WriteMaybeCompressedBlock`; `db/db_bloom_filter_test.cc` dynamic `detect_filter_construct_corruption` test around `SetOptions` and tampered hash entries
- **Fix:** Document the distinction between builder-internal fallback bytes and RocksDB's actual behavior: when corruption detection trips, flush/compaction fails instead of silently installing an always-true filter.

### [WRONG] Partitioned-filter corruption handling is described as emitting later always-true partitions, which the code does not do
- **File:** `docs/components/filter/05_partitioned_filter_blocks.md` / Error Handling
- **Claim:** "When this happens, all subsequent partitions generate AlwaysTrue filters to avoid using potentially corrupted data."
- **Reality:** The first non-OK construction status is latched in `partitioned_filters_construction_status_`. Later `Finish()` returns that status, and `BlockBasedTableBuilder::WriteFilterBlock()` stops. There is no path that keeps emitting later always-true partitions.
- **Source:** `table/block_based/partitioned_filter_block.cc` `PartitionedFilterBlockBuilder::CutAFilterBlock`; `table/block_based/partitioned_filter_block.cc` `PartitionedFilterBlockBuilder::Finish`; `table/block_based/block_based_table_builder.cc` `WriteFilterBlock`
- **Fix:** Say that partitioned-filter construction aborts on corruption rather than degrading subsequent partitions.

### [WRONG] The filter metaindex probe order is documented backwards
- **File:** `docs/components/filter/07_storage_format.md` / SST File Metaindex Entries
- **Claim:** "During table open, the reader tries these prefixes in order: `\"partitionedfilter.\"` first, then `\"fullfilter.\"`, then the obsolete `\"filter.\"` for backward compatibility."
- **Reality:** The code checks `fullfilter.` first, then `partitionedfilter.`, then the obsolete `filter.` prefix.
- **Source:** `table/block_based/block_based_table_reader.cc` `BlockBasedTable::PrefetchIndexAndFilterBlocks`
- **Fix:** Reverse the documented lookup order.

### [WRONG] The memtable-bloom chapter says the bloom is allocated even when the feature is disabled
- **File:** `docs/components/filter/08_memtable_bloom.md` / Configuration
- **Claim:** "If neither is set, the DynamicBloom is allocated but no entries are ever added."
- **Reality:** If neither `prefix_extractor` nor `memtable_whole_key_filtering` is enabled, memtable bloom is disabled and no `DynamicBloom` is allocated. The constructor only allocates it when at least one of those features is enabled and `memtable_prefix_bloom_size_ratio > 0`.
- **Source:** `include/rocksdb/advanced_options.h` `AdvancedColumnFamilyOptions::memtable_prefix_bloom_size_ratio`; `db/memtable.cc` `MemTable::MemTable`
- **Fix:** Replace the claim with "no memtable bloom is created." Also mention that the ratio is sanitized to the range `[0, 0.25]`.

### [WRONG] The full-filter chapter describes a workflow that the implementation does not use
- **File:** `docs/components/filter/04_full_filter_blocks.md` / Key Addition Workflow and AddWithPrevKey Optimization
- **Claim:** "`AddWithPrevKey(key, prev_key)` provides the previous key to enable more efficient deduplication" and "Finish() calls `filter_bits_builder_->Finish(buf, &status)` to generate the filter bits, then optionally runs `MaybePostVerify()` if corruption detection is enabled."
- **Reality:** In the full-filter builder, `AddWithPrevKey()` ignores the previous key and simply delegates to `Add()`. Also, full-filter post-verification is not performed inside `FullFilterBlockBuilder::Finish()`; it happens later in `BlockBasedTableBuilder::WriteMaybeCompressedBlock()` via `MaybePostVerifyFilter()`.
- **Source:** `table/block_based/full_filter_block.cc` `FullFilterBlockBuilder::AddWithPrevKey`; `table/block_based/full_filter_block.cc` `FullFilterBlockBuilder::Finish`; `table/block_based/block_based_table_builder.cc` `WriteMaybeCompressedBlock`
- **Fix:** Remove the nonexistent full-filter `AddWithPrevKey` optimization and document the real post-verification timing.

### [MISLEADING] The thread-safety note for full-filter size estimation is backwards
- **File:** `docs/components/filter/04_full_filter_blocks.md` / Size Estimation
- **Claim:** "`UpdateFilterSizeEstimate()` caches this value and is called when data blocks are finalized. It must be thread-safe because it can be called from background worker threads during parallel compression."
- **Reality:** The full-filter builder stores `estimated_filter_size_` as a plain `size_t` and is updated on the emit thread before blocks are handed off to parallel workers. The partitioned builder is the one with atomic sizing state and explicit thread-safety handling.
- **Source:** `table/block_based/full_filter_block.h` `FullFilterBlockBuilder`; `table/block_based/full_filter_block.cc` `FullFilterBlockBuilder::UpdateFilterSizeEstimate`; `table/block_based/block_based_table_builder.cc` call site for `OnDataBlockFinalized`; `table/block_based/partitioned_filter_block.h` `PartitionedFilterBlockBuilder`; `table/block_based/partitioned_filter_block.cc` `PartitionedFilterBlockBuilder::UpdateFilterSizeEstimate`
- **Fix:** Remove the thread-safety claim from the full-filter chapter, or explicitly say it applies to the partitioned builder rather than the full builder.

### [WRONG] The legacy Bloom flaw is explained as `delta == 0`, which is not what the code says
- **File:** `docs/components/filter/02_fast_local_bloom.md` / LegacyBloom (Deprecated)
- **Claim:** "`h += delta` where `delta = (h >> 17) | (h << 15)`, with a 1/512 chance of `delta = 0` causing all probes to hit the same bit"
- **Reality:** The implementation comment explains a 1/512 chance that the increment is zero within the 512-bit cache-line address space, not that the full 32-bit `delta` value is literally zero. That masked-within-line collapse is the accuracy flaw.
- **Source:** `util/bloom_impl.h` `LegacyLocalityBloomImpl` comment and implementation
- **Fix:** Rephrase the bug as "zero increment within the cache-line bit address space" rather than "`delta = 0`."

### [MISLEADING] The documented "effective FP rate" formula only works for whole-key point lookups
- **File:** `docs/components/filter/11_monitoring_and_best_practices.md` / Computing Effective FP Rate
- **Claim:** "`False Positives = BLOOM_FILTER_FULL_POSITIVE - BLOOM_FILTER_FULL_TRUE_POSITIVE`" and "`Actual FP Rate = False Positives / Total Filter Checks`"
- **Reality:** That calculation only applies to whole-key point-lookup counters. Prefix point lookups use `BLOOM_FILTER_PREFIX_*`, and iterator seeks use `NON_LAST_LEVEL_SEEK_FILTER_*`. The current text presents the formula as generic filter monitoring guidance.
- **Source:** `include/rocksdb/statistics.h` ticker comments for `BLOOM_FILTER_PREFIX_*` and `NON_LAST_LEVEL_SEEK_*`; `table/block_based/block_based_table_reader.cc` `BlockBasedTable::FullFilterKeyMayMatch`; `table/block_based/block_based_table_reader.cc` `BlockBasedTable::FullFilterKeysMayMatch`
- **Fix:** Label the formula as "whole-key point lookup only" and add separate guidance for prefix point lookups and iterator seeks.

## Completeness Gaps

### Iterator filtering after `prefix_extractor` changes is missing as a separate concept
- **Why it matters:** This is the most subtle filter/read-path interaction in the current code. Maintainers need to know that old SST-local prefix extractors can still be used for iterators, and that the decision depends on the seek key, the upper bound, and compatibility checks in `RangeMayExist()`.
- **Where to look:** `table/block_based/block_based_table_reader.cc` `BlockBasedTable::PrefixMayMatch`; `table/block_based/filter_block_reader_common.cc` `FilterBlockReaderCommon<TBlocklike>::RangeMayExist`; `db/db_bloom_filter_test.cc` `DynamicBloomFilterUpperBound`; `db/db_bloom_filter_test.cc` `DynamicBloomFilterMultipleSST`
- **Suggested scope:** Add a dedicated subsection in chapter 9 contrasting point lookups with iterator/prefix-seek behavior after a prefix-extractor change.

### Memtable MultiGet behavior with range tombstones is not documented
- **Why it matters:** Memtable bloom is effectively disabled for MultiGet when range tombstones are present, unless range deletions are being ignored. That is a real, user-visible behavior change that matters when debugging bloom hit/miss rates.
- **Where to look:** `db/memtable.cc` `MemTable::MultiGet`
- **Suggested scope:** Add a short subsection in chapter 8 covering the range-tombstone bypass.

### Seek-specific filter statistics are omitted
- **Why it matters:** The monitoring chapter currently reads as if `BLOOM_FILTER_*` counters cover all filter behavior, but iterator/prefix-seek filtering uses separate `NON_LAST_LEVEL_SEEK_FILTER_*` tickers. Without them, the docs are not aligned with the tested read path.
- **Where to look:** `include/rocksdb/statistics.h`; `table/block_based/block_based_table_iterator.cc`; `db/db_bloom_filter_test.cc` `DynamicBloomFilterUpperBound`
- **Suggested scope:** Expand chapter 11 with a separate "seek / iterator filtering" statistics subsection.

### Partition sizing rules are under-documented
- **Why it matters:** `metadata_block_size` is only a target. Actual filter partition sizing depends on `block_size_deviation`, `ApproximateNumEntries()`, prefix-extractor adjustments, and a small-size fallback path. Those details affect tuning and explain why recent bugs/fixes happened in this area.
- **Where to look:** `table/block_based/block_based_table_builder.cc` `CreateFilterBlockBuilder`; `table/block_based/partitioned_filter_block.cc` `PartitionedFilterBlockBuilder::PartitionedFilterBlockBuilder`; recent history `9d5c8c89a` and `9245550e8`
- **Suggested scope:** Add a tuning-oriented note to chapter 5 rather than treating `metadata_block_size` as a direct byte-size guarantee.

### Option sanitization for memtable bloom is missing
- **Why it matters:** The review brief explicitly calls for option-semantics precision. `memtable_prefix_bloom_size_ratio` is sanitized into `[0, 0.25]`, and the docs currently present it as a raw unconstrained ratio.
- **Where to look:** `include/rocksdb/advanced_options.h` `AdvancedColumnFamilyOptions::memtable_prefix_bloom_size_ratio`; `db/column_family.cc` `SanitizeCfOptions`
- **Suggested scope:** Add the sanitization rule to chapter 8 and chapter 11's best-practices section.

### Cache/pinning docs do not explain the real meaning of `PinningTier::kFlushedAndSimilar`
- **Why it matters:** The current text collapses the tier into "L0", but the code uses an L0-plus-size heuristic that also covers some ingested and intra-L0-compaction outputs. That matters when users reason about pinning policy.
- **Where to look:** `include/rocksdb/table.h` `PinningTier`; `table/block_based/block_based_table_reader.cc` `BlockBasedTable::PrefetchIndexAndFilterBlocks`
- **Suggested scope:** Add a short semantics note in chapter 10.

## Depth Issues

### Chapter 9 is too shallow about the actual query-path split
- **Current:** The chapter reduces filter usage to "`Get` does X" and "`Seek` does `PrefixMayMatch`."
- **Missing:** It does not explain the real split between `FullFilterKeyMayMatch()` for point lookups and `RangeMayExist()` for iterator/prefix-seek filtering, including upper-bound compatibility and SST-local prefix extractors.
- **Source:** `table/block_based/block_based_table_reader.cc` `BlockBasedTable::FullFilterKeyMayMatch`; `table/block_based/block_based_table_reader.cc` `BlockBasedTable::PrefixMayMatch`; `table/block_based/filter_block_reader_common.cc` `FilterBlockReaderCommon<TBlocklike>::RangeMayExist`

### Chapters 4 and 6 blur together full-filter and partitioned-filter verification timing
- **Current:** The docs talk about `Finish()` and post-verification as if there is one unified workflow.
- **Missing:** Full filters and partitioned filters do not verify at the same stage. Full filters are post-verified from the table-writer path, while partitioned filters do verification inside `CutAFilterBlock()`.
- **Source:** `table/block_based/full_filter_block.cc` `FullFilterBlockBuilder::Finish`; `table/block_based/partitioned_filter_block.cc` `PartitionedFilterBlockBuilder::CutAFilterBlock`; `table/block_based/block_based_table_builder.cc` `WriteMaybeCompressedBlock`

### Chapter 10 needs a clearer model of three different storage modes
- **Current:** The chapter largely describes one generic "filter cache" behavior.
- **Missing:** The docs should distinguish reader-owned full filters, reader-owned top-level partition metadata, and block-cached filter partitions. Without that split, the pinning and memory-accounting advice is not precise enough to be trusted.
- **Source:** `table/block_based/full_filter_block.cc` `FullFilterBlockReader::Create`; `table/block_based/partitioned_filter_block.cc` `PartitionedFilterBlockReader::Create`; `table/block_based/partitioned_filter_block.cc` `PartitionedFilterBlockReader::GetFilterPartitionBlock`

## Structure and Style Violations

### Inline code quoting is pervasive across the whole doc set
- **File:** `docs/components/filter/index.md` and all chapter files
- **Details:** The doc-family brief explicitly says "NO inline code quotes." These docs use inline code formatting for paths, types, function names, options, enum values, and ordinary prose throughout the entire chapter set.

### One chapter cites a nonexistent sanitization function
- **File:** `docs/components/filter/05_partitioned_filter_blocks.md`
- **Details:** The text says partition-filter sanitization happens in `BlockBasedTableFactory::SanitizeOptions()`, but there is no such function in the current code. The relevant logic is in `BlockBasedTableFactory::InitializeOptions()`.

### Several `Files:` lines omit the files that actually implement the documented behavior
- **File:** `docs/components/filter/08_memtable_bloom.md`; `docs/components/filter/09_prefix_filtering.md`; `docs/components/filter/10_caching_and_pinning.md`
- **Details:** The memtable-bloom chapter omits `db/memtable.cc` and `include/rocksdb/advanced_options.h`, which contain the allocation and option-semantics logic. The prefix-filtering chapter omits `table/block_based/filter_block_reader_common.cc`, where iterator compatibility is implemented. The caching chapter omits the common reader helper that actually decides whether the filter comes from reader memory or cache.

## Undocumented Complexity

### Iterator filtering can still use old SST-local prefix extractors after config changes
- **What it is:** When the current `prefix_extractor` changes, the reader may reconstruct the SST-local extractor from table properties and still use the filter if `RangeMayExist()` considers the query bounds compatible.
- **Why it matters:** Without this mental model, a maintainer will misdiagnose old-SST iterator behavior as inconsistent or buggy.
- **Key source:** `table/block_based/block_based_table_reader.cc` `BlockBasedTable::Open`; `table/block_based/filter_block_reader_common.cc` `FilterBlockReaderCommon<TBlocklike>::RangeMayExist`
- **Suggested placement:** Chapter 9

### Memtable MultiGet intentionally bypasses bloom filtering when range tombstones exist
- **What it is:** `MemTable::MultiGet()` only uses memtable bloom when either range deletions are ignored or the range-tombstone memtable is empty.
- **Why it matters:** This is a non-obvious reason for "bloom didn't help" behavior during MultiGet-heavy workloads with range deletes.
- **Key source:** `db/memtable.cc` `MemTable::MultiGet`
- **Suggested placement:** Chapter 8

### Post-verification has a documented blind spot for corruption-to-AlwaysTrue
- **What it is:** `MaybePostVerify()` explicitly notes that converting an XXPH3-based filter into `AlwaysTrueFilter` will not be detected by the current hash-requery check.
- **Why it matters:** Anyone relying on `detect_filter_construct_corruption` as complete coverage should understand the limitation.
- **Key source:** `table/block_based/filter_policy.cc` `XXPH3FilterBitsBuilder::MaybePostVerify`
- **Suggested placement:** Chapter 6

### Partitioned-filter lookups have extra correctness hacks beyond cross-boundary prefix insertion
- **What it is:** The reader falls back to the last filter partition when the top-level index seek runs past the end, because prefix queries can still be satisfied there. The builder also chooses separator encoding differently depending on whether the partitioned index separator includes sequence numbers.
- **Why it matters:** These are the kinds of boundary behaviors that matter when debugging prefix-seek bugs or changing index/filter coupling.
- **Key source:** `table/block_based/partitioned_filter_block.cc` `PartitionedFilterBlockReader::GetFilterPartitionHandle`; `table/block_based/partitioned_filter_block.cc` `PartitionedFilterBlockBuilder::CutAFilterBlock`
- **Suggested placement:** Chapter 5

## Positive Notes

- `docs/components/filter/index.md` is within the requested 40-80 line budget and follows the expected overview/source-files/chapter-table/characteristics/invariants shape.
- The docs correctly identify cross-partition prefix duplication as a correctness issue, and that matches both the implementation and the dedicated tests in `table/block_based/partitioned_filter_block_test.cc`.
- `docs/components/filter/06_construction_pipeline.md` correctly recognizes `XXPH3FilterBitsBuilder` as the shared base for modern SST filters and correctly calls out the `std::deque<uint64_t>` hash-entry storage.
- The chapter coverage is materially better than a lot of generated docs: it at least attempts to cover storage format, cache behavior, memtable bloom, and monitoring rather than stopping at high-level API descriptions.
