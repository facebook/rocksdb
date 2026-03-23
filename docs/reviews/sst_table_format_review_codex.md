# Review: sst_table_format — Codex

## Summary
Overall quality rating: significant issues.

The doc set has good surface coverage. The index is within the requested size budget, every chapter has a `Files:` line, and the chapter split is sensible: block layout, block fetching, table building, alternate table formats, table properties, and external SST writing are all separated in a maintainable way.

The main problem is accuracy. Several of the most important cross-component facts are stale or wrong: default `format_version`, checksum-context derivation, `BlockBasedTable::Open()` ordering, filter defaults, data-block-hash behavior, and external-SST global-seqno handling. Many of these mismatches line up with 2024-2025 code changes such as `format_version=7` by default, separated key/value storage, automated interpolation search, and the deprecation of ingestion-time `write_global_seqno`. The docs also leave out tested edge cases and important sanitization rules, so a maintainer would be misled exactly where precision matters most.

## Correctness Issues

### [WRONG] The default block-based `format_version` is not `6`
- **File:** `docs/components/sst_table_format/01_overview.md` / Key Configuration Options; `docs/components/sst_table_format/06_index_formats.md` / Configuration Options
- **Claim:** "`format_version` | 6 | On-disk format version" and "`format_version` | 6 | Controls index encoding. Version >= 3 strips sequence numbers. Version >= 4 delta-encodes block handles."
- **Reality:** `BlockBasedTableOptions::format_version` defaults to `7`, not `6`.
- **Source:** `include/rocksdb/table.h` `BlockBasedTableOptions`
- **Fix:** Update both chapters to say the current default is `7`, and keep older-version notes separate from the default-value row.

### [WRONG] The XXH3 default is not conditional on `format_version >= 5`
- **File:** `docs/components/sst_table_format/01_overview.md` / Checksum Types
- **Claim:** "`kXXH3` | XXH3 (default since format_version >= 5). Supported since RocksDB 6.27."
- **Reality:** The option default is simply `ChecksumType checksum = kXXH3`. It is not conditionally set by the `format_version` field at option-definition time.
- **Source:** `include/rocksdb/table.h` `BlockBasedTableOptions::checksum`
- **Fix:** Say XXH3 is the current default checksum type, and document any version-compatibility constraints separately.

### [WRONG] Context checksums are not derived from the file unique ID
- **File:** `docs/components/sst_table_format/01_overview.md` / Context-Aware Checksums; `docs/components/sst_table_format/index.md` / Key Invariants
- **Claim:** "Each SST file has a `base_context_checksum` (derived from the file's unique ID)." and "For format_version >= 6, block checksums include a context modifier derived from the file's unique ID and block offset"
- **Reality:** The writer generates a non-zero semi-random `base_context_checksum`, stores it in the footer, and combines it with the block offset. The code comments explicitly say the ideal unique-ID-based scheme is still a TODO.
- **Source:** `table/block_based/block_based_table_builder.cc` writer initialization of `base_context_checksum`; `table/format.h` `ChecksumModifierForContext`
- **Fix:** Describe `base_context_checksum` as a per-file random/semi-random footer field, not as something derived from the SST unique ID.

### [MISLEADING] The option table hides important sanitization and override rules
- **File:** `docs/components/sst_table_format/01_overview.md` / Key Configuration Options
- **Claim:** "`block_size_deviation` | 10 | Percentage threshold for closing a block early", "`index_block_restart_interval` | 1 | Restart interval for index blocks", and "`format_version` | 6 | On-disk format version"
- **Reality:** Several of these knobs are not "set exactly what the user asked for" options. `block_size_deviation` is reset to `0` outside `[0, 100]`; both restart intervals are clamped to at least `1`; `kHashSearch` forces `index_block_restart_interval = 1`; `partition_filters` is silently disabled unless `index_type == kTwoLevelIndexSearch`; and too-small write-side `format_version` values are sanitized upward.
- **Source:** `table/block_based/block_based_table_factory.cc` `BlockBasedTableFactory::InitializeOptions`
- **Fix:** Add a sanitization/interactions subsection so the docs reflect the effective runtime semantics, not just the nominal fields.

### [WRONG] `TailPrefetchStats` does not choose the smallest qualifying size
- **File:** `docs/components/sst_table_format/01_overview.md` / TailPrefetchStats
- **Claim:** "`GetSuggestedPrefetchSize()` computes the smallest prefetch size that wastes no more than 12.5% (1/8) of total read bytes, capped at 512KB."
- **Reality:** The algorithm sorts historical sizes and returns the maximum qualifying size under the `wasted <= read / 8` rule, capped at 512KB.
- **Source:** `table/block_based/block_based_table_factory.cc` `TailPrefetchStats::GetSuggestedPrefetchSize`
- **Fix:** Change "smallest" to "maximum historical size that still satisfies the waste threshold."

### [WRONG] The block-type compression table is stricter than the read path
- **File:** `docs/components/sst_table_format/02_block_based_format.md` / Block Types
- **Claim:** The table marks `kProperties`, `kRangeDeletion`, `kMetaIndex`, and `kHashIndexPrefixes` / `kHashIndexMetadata` as "Never" compressed, and the note says "Filter blocks, compression dictionaries, and user-defined index blocks are always stored uncompressed."
- **Reality:** On the read path, `BlockTypeMaybeCompressed()` only excludes `kFilter`, `kCompressionDictionary`, and `kUserDefinedIndex`. Other meta blocks may be compressed if written that way.
- **Source:** `table/block_based/block_based_table_reader.h` `BlockBasedTable::BlockTypeMaybeCompressed`
- **Fix:** Document the actual read-side rule and, if useful, separately call out which blocks the current writer typically emits uncompressed.

### [WRONG] The documented `BlockBasedTable::Open()` sequence no longer matches the code
- **File:** `docs/components/sst_table_format/02_block_based_format.md` / `BlockBasedTable::Open()`
- **Claim:** The numbered sequence says the table open path reads footer, metaindex, properties, configures decompressor, sets up cache keys, reads range tombstones, and then "Prefetch index and filter blocks."
- **Reality:** After tail prefetch and footer parsing, the current order is: metaindex, properties, decompressor setup, unique-ID verification, prefix-extractor compatibility/recreation, base-cache-key setup, range-deletion block, then `PrefetchIndexAndFilterBlocks()`. Compression-dictionary discovery and user-defined-index setup happen inside that later phase, not in the earlier numbered steps.
- **Source:** `table/block_based/block_based_table_reader.cc` `BlockBasedTable::Open`; `table/block_based/block_based_table_reader.cc` `PrefetchIndexAndFilterBlocks`
- **Fix:** Rewrite the sequence to follow the actual call chain, including unique-ID verification, prefix-extractor recreation, compression-dictionary handling, and UDI setup.

### [MISLEADING] The data-block chapter uses the wrong public option name and blurs data-block vs index-block behavior
- **File:** `docs/components/sst_table_format/03_data_blocks.md` / Separated KV Storage and Uniform Key Detection
- **Claim:** "When `use_separated_kv_storage` is enabled in `BlockBuilder`..." and "Blocks marked as uniform enable interpolation search when `index_block_search_type = kAuto`."
- **Reality:** The public option is `separate_key_value_in_data_block`, not `use_separated_kv_storage`. Also the `kAuto` interpolation-search decision is consumed by index-block iteration, and `ScanForUniformity()` is used by index builders as well as plain data blocks.
- **Source:** `include/rocksdb/table.h` `BlockBasedTableOptions::separate_key_value_in_data_block`; `table/block_based/index_builder.h`; `table/block_based/block.cc` `Block::NewIndexIterator` / `IndexBlockIter`
- **Fix:** Rename the option throughout the chapter and explain that the footer hint is relevant to index blocks and index search, not only data-block layout.

### [WRONG] Persistent-cache keys are not raw `BlockHandle`s
- **File:** `docs/components/sst_table_format/04_block_fetcher.md` / Persistent Cache
- **Claim:** "Blocks are inserted after file read and before decompression" and the chapter describes compressed persistent-cache entries as keyed by `BlockHandle`.
- **Reality:** Persistent-cache lookups and inserts use `BlockBasedTable::GetCacheKey(base_cache_key, handle)`, not the bare handle.
- **Source:** `table/persistent_cache_helper.cc` helper functions; `table/block_based/block_based_table_reader.h` `BlockBasedTable::GetCacheKey`
- **Fix:** Describe the cache key as the table's base cache key combined with the block handle.

### [WRONG] The BlockFetcher pipeline and retry behavior are documented in the wrong order
- **File:** `docs/components/sst_table_format/04_block_fetcher.md` / Block Read Flow
- **Claim:** "Step 1 -- Try uncompressed persistent cache", "Step 2 -- Try prefetch buffer", "Step 3 -- Try serialized persistent cache", and "Step 6 -- Retry on corruption..."
- **Reality:** The actual order is uncompressed persistent cache, then prefetch buffer, then serialized persistent cache, then file read. Retry-on-corruption can happen after prefetch-buffer trailer verification and after the direct file read when the FS supports `kVerifyAndReconstructRead`.
- **Source:** `table/block_fetcher.cc` `BlockFetcher::ReadBlockContents`; `table/block_fetcher.cc` `BlockFetcher::ReadBlock`
- **Fix:** Reorder the steps and mention both retry points explicitly.

### [MISLEADING] The builder-state discussion omits `kClosed`, and the alignment rule is too broad
- **File:** `docs/components/sst_table_format/05_table_builder.md` / State Machine and Block Alignment
- **Claim:** The state table lists only `kBuffered` and `kUnbuffered`, and the alignment section says "Block alignment disables compression."
- **Reality:** The implementation also has `kClosed`, entered by `Finish()` and `Abandon()`. Also only `block_align` is rejected with compression; `super_block_alignment_size` is still supported and can force `skip_delta_encoding` for the corresponding index entry handles.
- **Source:** `table/block_based/block_based_table_builder.cc` `Rep::State`; `table/block_based/block_based_table_builder.cc` constructor validation; `table/block_based/block_based_table_builder.cc` `WriteMaybeCompressedBlockImpl`
- **Fix:** Add `kClosed` to the state machine and separate `block_align` from `super_block_alignment_size` behavior.

### [STALE] The index-format evolution section ties `kBinarySearchWithFirstKey` to the wrong concept
- **File:** `docs/components/sst_table_format/06_index_formats.md` / Format Version Evolution
- **Claim:** "Version 5 (RocksDB 6.6) adds `kBinarySearchWithFirstKey` encoding..."
- **Reality:** `kBinarySearchWithFirstKey` is an `index_type` selected at runtime by `IndexBuilder::CreateIndexBuilder()`, not a format-version-only mode.
- **Source:** `include/rocksdb/table.h` `BlockBasedTableOptions::IndexType`; `table/block_based/index_builder.cc` `IndexBuilder::CreateIndexBuilder`
- **Fix:** Move `kBinarySearchWithFirstKey` into the index-type discussion and keep the format-version table focused on what format version changes inside a chosen index format.

### [WRONG] `HashIndexBuilder` does not match the chapter's metadata and size-estimate claims
- **File:** `docs/components/sst_table_format/06_index_formats.md` / Hash Search Index; introduction to `CurrentIndexSizeEstimate()`
- **Claim:** "At `Finish()`, these are emitted as two meta blocks: `kHashIndexPrefixesBlock` ... `kHashIndexPrefixesMetadataBlock` ..." and "`CurrentIndexSizeEstimate()` provides a running estimate... Implementations cache this estimate..."
- **Reality:** The two hash-search metadata entries are written through `IndexBlocks::meta_blocks` but tagged as `BlockType::kIndex`. Also `HashIndexBuilder::CurrentIndexSizeEstimate()` currently returns `0`, so the generic "running estimate" statement is not true for all implementations.
- **Source:** `table/block_based/index_builder.h` `HashIndexBuilder::Finish`; `table/block_based/index_builder.h` `HashIndexBuilder::CurrentIndexSizeEstimate`
- **Fix:** Document the actual on-disk block type and qualify the size-estimate section as implementation-dependent.

### [WRONG] The filter chapter is stale on both legacy support and the default memory option
- **File:** `docs/components/sst_table_format/07_filter_blocks.md` / Overview and Filter Configuration
- **Claim:** "The deprecated block-based filter (one filter per data block range) has been removed from current code." and "`optimize_filters_for_memory` | false | ..."
- **Reality:** The reader still recognizes obsolete `filter.` meta-block prefixes for backward compatibility, and `BlockBasedTableOptions::optimize_filters_for_memory` defaults to `true`.
- **Source:** `table/block_based/block_based_table_reader.cc` `BlockBasedTable::PrefetchIndexAndFilterBlocks`; `table/block_based/block_based_table_builder.cc` `BlockBasedTable::kObsoleteFilterBlockPrefix`; `include/rocksdb/table.h` `BlockBasedTableOptions::optimize_filters_for_memory`
- **Fix:** Say the old format is still readable, and update the default to `true`.

### [MISLEADING] `RibbonFilterPolicy::bloom_before_level` semantics are simplified enough to be wrong in the default case
- **File:** `docs/components/sst_table_format/07_filter_blocks.md` / Bloom vs Ribbon
- **Claim:** "`RibbonFilterPolicy` ... can be configured to use Bloom for lower LSM levels ... and Ribbon for higher levels ... via `bloom_before_level`."
- **Reality:** The public API defines sharper behavior. In particular, `bloom_before_level=0` means Bloom only for flushes under Level/Universal compaction and Ribbon elsewhere. `INT_MAX` means always Bloom, and `-1` means always Ribbon except rare fallback cases.
- **Source:** `include/rocksdb/filter_policy.h` comments for `NewRibbonFilterPolicy`; `table/block_based/filter_policy.cc` `RibbonFilterPolicy::GetBuilderWithContext`
- **Fix:** Replace the broad prose with the exact threshold semantics, especially for the default `0`.

### [MISLEADING] Partitioned-filter pinning is not an all-or-nothing mirror of partitioned-index pinning
- **File:** `docs/components/sst_table_format/08_partitioned_index_filter.md` / Partitioned Filter Reader
- **Claim:** "`PartitionedFilterBlockReader::CacheDependencies()` mirrors `PartitionIndexReader::CacheDependencies()`..."
- **Reality:** `PartitionIndexReader` keeps `partition_map_` as "all or none", but `PartitionedFilterBlockReader::filter_map_` is explicitly allowed to hold only a subset of partitions when some cache pins fail.
- **Source:** `table/block_based/partitioned_index_reader.h` `partition_map_` comment; `table/block_based/partitioned_filter_block.h` `filter_map_` comment
- **Fix:** Distinguish the two caching semantics instead of implying they are identical.

### [MISLEADING] `PartitionedIndexBuilder::AddIndexEntry()` can flush before adding the new entry
- **File:** `docs/components/sst_table_format/08_partitioned_index_filter.md` / `PartitionedIndexBuilder`
- **Claim:** "Step 1: `AddIndexEntry()` or `FinishIndexEntry()` adds the entry to the active sub-index builder. Then `MaybeFlush()` checks..."
- **Reality:** When `first_key_in_next_block` is available, `AddIndexEntry()` can call `MaybeFlush()` before adding the incoming entry, so the flush decision is anchored to the previous block's separator.
- **Source:** `table/block_based/index_builder.cc` `PartitionedIndexBuilder::AddIndexEntry`; `table/block_based/index_builder.cc` `PartitionedIndexBuilder::MaybeFlush`
- **Fix:** Describe the pre-add flush case so partition-boundary behavior matches the code.

### [WRONG] The data-block-hash chapter overstates both the fast path and the meaning of `kNoEntry`
- **File:** `docs/components/sst_table_format/09_data_block_hash_index.md` / Overview and Read Path
- **Claim:** "Without it, `DataBlockIter::Seek()` performs a binary search..." and "`kNoEntry` (255): key is definitely not in this block" and "If the result is `kNoEntry`, the key is not in this block -- return immediately."
- **Reality:** The optimization is for `SeekForGet()`, not generic `Seek()`. On `kNoEntry`, the iterator can still search the last restart interval and may continue to the next block because the hash index intentionally leaves some boundary cases to fallback logic.
- **Source:** `table/block_based/block.cc` `DataBlockIter::SeekForGetImpl`
- **Fix:** Reframe the chapter around `SeekForGet()` and explain the last-restart-interval / next-block fallback behavior.

### [STALE] The backward-compatibility story for the packed footer is outdated
- **File:** `docs/components/sst_table_format/09_data_block_hash_index.md` / On-Disk Layout
- **Claim:** "The packed footer's MSB (bit 31) signals the presence of the hash index..." and "This format is backward-compatible ... because the MSB of `num_restarts` was previously unused..."
- **Reality:** Current `DataBlockFooter` reserves the top 4 bits and caps `num_restarts` at `2^28 - 1`. The modern compatibility story is about reserved feature bits, not the old "effectively unused MSB" explanation.
- **Source:** `table/block_based/data_block_footer.h` `DataBlockFooter::kMaxNumRestarts`; `table/block_based/data_block_footer.cc`
- **Fix:** Update the chapter to the current 28-bit restart-count plus 4-bit feature layout.

### [WRONG] The supported record-type list for hash-index-assisted lookup is too narrow
- **File:** `docs/components/sst_table_format/09_data_block_hash_index.md` / Limitations
- **Claim:** "| Record type restriction | Only `kTypePut`, `kTypeDelete`, `kTypeSingleDelete`, and `kTypeBlobIndex` are hash-indexed. `kTypeMerge` records fall back to binary search. |"
- **Reality:** `SeekForGetImpl()` explicitly supports `kTypeMerge`, `kTypeWideColumnEntity`, `kTypeValuePreferredSeqno`, and other point-lookup record types beyond the small legacy list.
- **Source:** `table/block_based/block.cc` `DataBlockIter::SeekForGetImpl`
- **Fix:** Either list the full supported set or say the optimization supports a broader set of point-lookup value types than older docs suggested.

### [WRONG] PlainTable footer version does not vary by encoding type
- **File:** `docs/components/sst_table_format/10_plain_table.md` / File Layout
- **Claim:** "The footer uses `kPlainTableMagicNumber` ... and format version 0 (for `kPlain` encoding) or 1 (for `kPrefix` encoding)."
- **Reality:** `PlainTableBuilder` always writes footer format version `0`. The actual encoding type is stored separately in the user property `PlainTablePropertyNames::kEncodingType`.
- **Source:** `table/plain/plain_table_builder.cc` constructor setup of `kEncodingType`; `table/plain/plain_table_builder.cc` `PlainTableBuilder::Finish`
- **Fix:** Say the footer format version is always `0` and move encoding-type specifics to the properties discussion.

### [WRONG] `full_scan_mode` still supports sequential scan entry via `SeekToFirst()`
- **File:** `docs/components/sst_table_format/10_plain_table.md` / Configuration Options
- **Claim:** "When true, no index is built. File can only be read sequentially. Point lookups and seeks are not supported."
- **Reality:** Point lookups and keyed `Seek()` are unsupported in full-scan mode, but `SeekToFirst()` still works and is the entry point for sequential iteration.
- **Source:** `table/plain/plain_table_reader.cc` `PlainTableIterator::SeekToFirst`; `table/plain/plain_table_reader.cc` `PlainTableIterator::Seek`
- **Fix:** Narrow the claim to point lookups and keyed `Seek()`, while preserving sequential full-scan support.

### [MISLEADING] CuckooTable does not require equal value size for deletion entries
- **File:** `docs/components/sst_table_format/11_cuckoo_table.md` / Build Workflow and Limitations
- **Claim:** "The builder validates that all keys have the same size and all values have the same size." and "| Fixed key and value lengths | All keys must have the same size. All values must have the same size. |"
- **Reality:** Equal value size is enforced only for `kTypeValue` entries. Deletions are stored separately in `deleted_keys_`, and the reader synthesizes a dummy value for those buckets.
- **Source:** `table/cuckoo/cuckoo_table_builder.cc` `CuckooTableBuilder::Add`; `table/cuckoo/cuckoo_table_builder.cc` `CuckooTableBuilder::GetValue`
- **Fix:** Qualify the rule as "all non-deletion values must have the same size."

### [WRONG] The table-properties chapter overstates both aggregation and memory-accounting APIs
- **File:** `docs/components/sst_table_format/12_table_properties.md` / Aggregation and Comparison; Approximate Memory Usage
- **Claim:** "`Add()`: Sums all numeric fields from another `TableProperties` instance." and "`ApproximateMemoryUsage()` estimates the total memory footprint ... including ... all string properties..."
- **Reality:** `TableProperties::Add()` aggregates only a subset of numeric fields. `ApproximateMemoryUsage()` omits some members such as `seqno_to_time_mapping`, does not include all container overhead, and is intentionally approximate.
- **Source:** `table/table_properties.cc` `TableProperties::Add`; `table/table_properties.cc` `TableProperties::ApproximateMemoryUsage`
- **Fix:** Describe both APIs as partial/approximate helpers instead of exhaustive aggregation/accounting.

### [MISLEADING] The internal-collector section describes a much smaller and more uniform system than the code has
- **File:** `docs/components/sst_table_format/12_table_properties.md` / Internal Property Collectors
- **Claim:** "RocksDB includes internal property collectors ... that are always active:" followed by only `InternalKeyPropertiesCollector`.
- **Reality:** Block-based table building also installs a `BlockBasedTablePropertiesCollector`; timestamp-aware configurations can add `TimestampTablePropertiesCollector`; and `SstFileWriter` injects its own internal collector. The collector set depends on table format and configuration.
- **Source:** `table/block_based/block_based_table_builder.cc` collector setup; `db/table_properties_collector.h` `TimestampTablePropertiesCollector`; `table/sst_file_writer.cc` `SstFileWriter::Open`; `table/sst_file_writer_collectors.h`
- **Fix:** Expand this section to separate always-present, format-specific, and configuration-specific internal collectors.

### [STALE] The `SstFileWriter` chapter documents the deprecated global-seqno rewrite path as the standard ingestion behavior
- **File:** `docs/components/sst_table_format/13_sst_file_writer.md` / Overview and External SST File Properties
- **Claim:** "All keys written by `SstFileWriter` have sequence number 0. The files are assigned a global sequence number during ingestion." and "During ingestion (`DB::IngestExternalFile()`), RocksDB assigns a real sequence number and updates the global seqno property in-place within the file..."
- **Reality:** Rewriting the file property is controlled by deprecated `IngestExternalFileOptions::write_global_seqno`, which defaults to `false`. In the common path, RocksDB records the assigned sequence number in DB metadata instead of rewriting the SST, and `allow_db_generated_files` preserves original sequence numbers entirely.
- **Source:** `include/rocksdb/options.h` `IngestExternalFileOptions::write_global_seqno` and `allow_db_generated_files`; `db/external_sst_file_ingestion_job.cc` `AssignGlobalSeqnoForIngestedFile`
- **Fix:** Document in-place global-seqno rewriting as a deprecated compatibility mode, not the normal ingestion flow.

## Completeness Gaps

### User Defined Index integration is barely represented
- **Why it matters:** Recent changes added trie-based UDI support, soft-failure behavior on SST open, more supported operation types, and read-path integration. The SST-format docs currently mention `kUserDefinedIndex` mostly as a block type, which is far too shallow for current code.
- **Where to look:** `table/block_based/block_based_table_reader.cc`; `table/block_based/index_builder.cc`; `include/rocksdb/user_defined_index.h`; recent `git log --since="2024-01-01" -- table/block_based include/rocksdb/table*.h`
- **Suggested scope:** Add a brief subsection in chapter 2 or chapter 6 describing how UDI metadata is written and loaded, with a cross-link to the dedicated user-defined-index component doc.

### The caller-facing `TableBuilder` concurrency contract is missing
- **Why it matters:** Chapter 5 talks about internal worker threads for parallel compression, but it never states the public API rule: concurrent callers need external synchronization if any non-const method is involved.
- **Where to look:** `table/table_builder.h` `TableBuilder`
- **Suggested scope:** Add a short thread-safety note near the chapter-5 state machine or API table.

### Property-collector failure semantics are under-documented
- **Why it matters:** `AddUserKey()` and `Finish()` errors are logged and otherwise ignored rather than failing table creation. That is a surprising contract boundary for anyone debugging missing properties.
- **Where to look:** `include/rocksdb/table_properties.h` `TablePropertiesCollector`; `table/meta_blocks.cc` `NotifyCollectTableCollectorsOnAdd`; `table/meta_blocks.cc` `NotifyCollectTableCollectorsOnFinish`
- **Suggested scope:** Add a short failure-semantics subsection in chapter 12.

### External-SST ingestion boundary conditions are missing from the writer chapter
- **Why it matters:** Overlapping ingested files, snapshots, `allow_global_seqno`, `allow_db_generated_files`, and atomic-replace ranges all change how the external SST is assigned sequence numbers and where it can land. The writer chapter currently describes only the simplest path.
- **Where to look:** `db/external_sst_file_ingestion_job.cc`; `include/rocksdb/options.h` `IngestExternalFileOptions`
- **Suggested scope:** Expand chapter 13's interactions section with a concise ingestion-time behavior matrix.

### Tested edge cases are not reflected in the prose
- **Why it matters:** The codebase already has unit tests for many of the behaviors the docs currently get wrong: format-version sanitization, `TailPrefetchStats`, hash-index fallback behavior, `optimize_filters_for_memory`, `bloom_before_level`, partitioned-filter pinning, and timestamp-stripping/range-delete edge cases in `SstFileWriter`.
- **Where to look:** `table/table_test.cc`; `util/bloom_test.cc`; `table/block_based/partitioned_filter_block_test.cc`; `table/sst_file_reader_test.cc`
- **Suggested scope:** Add small "tested edge cases" callouts in the relevant chapters rather than a separate testing chapter.

## Depth Issues

### The `BlockBasedTable::Open()` description needs real call-chain depth
- **Current:** The chapter presents a clean numbered sequence, but it stops before the actual branching complexity becomes visible.
- **Missing:** It does not explain unique-ID verification, prefix-extractor recreation, base-cache-key setup, range-tombstone soft-failure behavior, or that `PrefetchIndexAndFilterBlocks()` owns compression-dictionary/filter/index/UDI setup.
- **Source:** `table/block_based/block_based_table_reader.cc` `BlockBasedTable::Open`; `table/block_based/block_based_table_reader.cc` `PrefetchIndexAndFilterBlocks`

### The BlockFetcher chapter needs a clearer ownership and fallback model
- **Current:** It gives a high-level flow for caches, read, verify, decompress, and return.
- **Missing:** It does not explain the lifetime/ownership branches in `GetBlockContents()`, when a returned `BlockContents` can alias file-owned memory, or how corruption retries differ between prefetch-buffer hits and direct file reads.
- **Source:** `table/block_fetcher.cc` `BlockFetcher::GetBlockContents`; `table/block_fetcher.cc` `BlockFetcher::ReadBlockContents`

### The writer chapter conflates writer-time behavior with ingestion-time behavior
- **Current:** Chapter 13 mixes "what `SstFileWriter` writes" with "how ingestion later interprets or mutates it."
- **Missing:** The docs need a sharper split between writer-side guarantees, ingestion-side sequence-number assignment policy, and timestamp/range-delete edge cases already codified in tests.
- **Source:** `table/sst_file_writer.cc`; `db/external_sst_file_ingestion_job.cc`; `table/sst_file_reader_test.cc`

## Structure and Style Violations

### Inline code quoting is pervasive across the whole doc set
- **File:** `docs/components/sst_table_format/index.md` and all chapter files
- **Details:** The review brief for this doc family explicitly says "NO inline code quotes." These docs use inline-code formatting almost everywhere for option names, function names, enum values, file paths, and ordinary prose. This is a systematic style violation, not an isolated one.

## Undocumented Complexity

### External-SST property-block checksum handling has a special rewritten-global-seqno path
- **What it is:** `ReadTablePropertiesHelper()` first reads/parses the properties block without checksum verification, then verifies the checksum afterward, and if the file has an external-SST global-seqno offset it can patch the stored bytes back to zero before retrying checksum verification. It also has a corruption-retry path when the filesystem supports reconstruction.
- **Why it matters:** This is exactly the kind of subtle behavior that matters when debugging ingested-file checksum mismatches.
- **Key source:** `table/meta_blocks.cc` `ReadTablePropertiesHelper`
- **Suggested placement:** Chapter 12 with a cross-reference from chapter 13

### Separated key/value storage is an end-to-end feature, not just a block-layout trick
- **What it is:** The feature touches the block footer, builder, table properties, and reader. The writer persists a table property and footer bits, and the reader reconstructs a table-wide flag from the properties block before using the layout.
- **Why it matters:** A maintainer changing only the data-block chapter will miss the reader/property implications and can easily create stale docs again.
- **Key source:** `table/block_based/data_block_footer.h`; `table/block_based/data_block_footer.cc`; `table/block_based/block_based_table_builder.cc`; `table/block_based/block_based_table_reader.cc`; `include/rocksdb/table.h`
- **Suggested placement:** Add brief cross-links among chapters 3, 5, and 12

### UDI now participates in SST open/read behavior
- **What it is:** The block-based table reader and index-building pipeline now carry UDI-specific setup and metadata handling, including newer behavior added in 2024-2025.
- **Why it matters:** Anyone debugging SST open or index behavior in current RocksDB will run into UDI-related branches that these docs currently ignore.
- **Key source:** `table/block_based/block_based_table_reader.cc`; `table/block_based/index_builder.cc`; `include/rocksdb/user_defined_index.h`
- **Suggested placement:** Chapter 2 or chapter 6 with a cross-link to the dedicated UDI doc

### The public `TableBuilder` contract differs from the implementation's internal parallelism
- **What it is:** `BlockBasedTableBuilder` can use background worker threads internally for compression, but the public `TableBuilder` API still requires external synchronization whenever non-const methods may be called concurrently.
- **Why it matters:** Without that distinction, readers can incorrectly infer that the builder object is generally safe for concurrent mutation.
- **Key source:** `table/table_builder.h`; `table/block_based/block_based_table_builder.cc`
- **Suggested placement:** Chapter 5

## Positive Notes

- `docs/components/sst_table_format/index.md` is within the requested size budget and follows the expected overview/source-files/chapter-table/characteristics/invariants structure.
- The chapter split is strong: block layout, fetch/caching, table building, alternative formats, and external SST writing are separated in a way that will scale well once the factual issues are fixed.
- `docs/components/sst_table_format/13_sst_file_writer.md` correctly captures several writer-side basics, especially sorted point-key requirements, timestamp-aware overloads, and page-cache invalidation behavior.
