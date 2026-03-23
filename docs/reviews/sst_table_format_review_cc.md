# Review: sst_table_format -- Claude Code

## Summary
(Overall quality rating: good)

The documentation is comprehensive in scope -- 13 chapters covering all major aspects of the SST table format from block layout through PlainTable and CuckooTable alternatives. The writing is clear, well-structured, and includes useful detail about encoding formats, configuration options, and build/read workflows. However, there are several correctness issues, the most significant being a wrong default for `format_version` (says 6, actually 7). The format version evolution table in chapter 06 conflates index type features with format version gating. Several record type lists are outdated (missing `kTypeWideColumnEntity` and `kTypeValuePreferredSeqno`). Completeness is good for the core block-based format but some recent features (user-defined index, super block alignment, pre-defined compression dictionaries) are not covered.

## Correctness Issues

### [WRONG] Default format_version is 7, not 6
- **File:** 01_overview.md, "Key Configuration Options" table; also index.md line 31
- **Claim:** "`format_version` | 6 | On-disk format version"
- **Reality:** The default is 7. See `uint32_t format_version = 7;` in `BlockBasedTableOptions` in `include/rocksdb/table.h`.
- **Source:** `include/rocksdb/table.h` `BlockBasedTableOptions::format_version`
- **Fix:** Change default from 6 to 7 in both the overview config table and the index.md characteristics. Also update index.md line 31 which says "Version 2 (minimum) through 7 (latest)" to note that 7 is also the current default.

### [WRONG] SeekForGet record type restriction claim
- **File:** 09_data_block_hash_index.md, "Limitations" table
- **Claim:** "Only `kTypePut`, `kTypeDelete`, `kTypeSingleDelete`, and `kTypeBlobIndex` are hash-indexed. `kTypeMerge` records fall back to binary search."
- **Reality:** `SeekForGetImpl()` in `table/block_based/block.cc` explicitly lists `kTypeValue`, `kTypeDeletion`, `kTypeSingleDeletion`, `kTypeBlobIndex`, `kTypeWideColumnEntity`, `kTypeValuePreferredSeqno`, AND `kTypeMerge` as supported types. kTypeMerge does NOT fall back to binary search. Additionally `kTypeWideColumnEntity` and `kTypeValuePreferredSeqno` are supported but not mentioned.
- **Source:** `table/block_based/block.cc` `DataBlockIter::SeekForGetImpl()` comment and implementation
- **Fix:** Remove the record type restriction row from the limitations table, or rewrite it to say: "For types other than kTypeValue, kTypeDeletion, kTypeSingleDeletion, kTypeBlobIndex, kTypeWideColumnEntity, kTypeValuePreferredSeqno, and kTypeMerge, SeekForGet falls back to standard Seek()."

### [WRONG] PlainTable kValueTypeSeqId0 byte value
- **File:** 10_plain_table.md, "Internal Key Optimization"
- **Claim:** "the internal key bytes are followed by a single byte `0x80` instead of the full 8-byte sequence number"
- **Reality:** `kValueTypeSeqId0 = char(~0)` which is `0xFF`, not `0x80`.
- **Source:** `table/plain/plain_table_factory.h` line 174
- **Fix:** Change `0x80` to `0xFF`.

### [MISLEADING] Format version evolution table conflates index type with format version
- **File:** 06_index_formats.md, "Format Version Evolution" table
- **Claim:** "5 (RocksDB 6.6) | Adds `kBinarySearchWithFirstKey` encoding"
- **Reality:** `kBinarySearchWithFirstKey` is an `IndexType` enum value (`kBinarySearchWithFirstKey = 0x03`), independent of `format_version`. It was introduced around the same time as format_version 5, but it works regardless of format version. Format version 5 actually changed the filter block format (FastLocalBloom).
- **Source:** `include/rocksdb/table.h` `BlockBasedTableOptions::IndexType`, `table/block_based/index_builder.cc` `CreateIndexBuilder()`
- **Fix:** Remove the row about kBinarySearchWithFirstKey from the format version table, or add a note that this is an index type, not gated by format_version. The format_version 5 row should instead say "New full/partitioned filter on-disk format (FastLocalBloom); XXH3 becomes default checksum."

### [MISLEADING] Format version 5 claim about XXH3
- **File:** 01_overview.md, Format Versions table
- **Claim:** "5 | Full and partitioned filters use a different on-disk format with improved memory utilization (uses XXH3 as default checksum)."
- **Reality:** XXH3 is the default checksum for ALL tables via `BlockBasedTableOptions::checksum = kXXH3`, regardless of format_version. Format version 5 introduced FastLocalBloom filter format, but the XXH3 default is a separate config default.
- **Source:** `include/rocksdb/table.h` `ChecksumType checksum = kXXH3;`
- **Fix:** Rephrase: "5 | Full and partitioned filters use a different on-disk format with improved memory utilization (FastLocalBloom). Legacy bloom and block-based filter deprecated."

### [WRONG] DecodeKeyV4 file reference
- **File:** 01_overview.md, Format Versions table
- **Claim:** "see `DecodeKeyV4()` in `table/block_based/block_builder.cc`"
- **Reality:** `DecodeKeyV4` is defined in `table/block_based/block_util.h`, not `block_builder.cc`. It is merely called from `block_builder.cc`.
- **Source:** `table/block_based/block_util.h` line 78
- **Fix:** Change reference to `table/block_based/block_util.h`.

### [MISLEADING] PlainTable Prev() claim
- **File:** 10_plain_table.md, "Limitations" table
- **Claim:** "`Prev()` returns `Status::NotSupported`"
- **Reality:** `Prev()` only calls `assert(false)` in debug builds. In release builds, behavior is undefined. It does NOT set `Status::NotSupported`.
- **Source:** `table/plain/plain_table_reader.cc` `PlainTableIterator::Prev()`
- **Fix:** Say "Prev() is not supported (asserts in debug, undefined in release)" rather than "returns Status::NotSupported".

### [MISLEADING] CuckooTable SeekForPrev claim
- **File:** 11_cuckoo_table.md, "Limitations" table
- **Claim:** "No `SeekForPrev()` | Not implemented."
- **Reality:** Like PlainTable's `Prev()`, `SeekForPrev()` only calls `assert(false)` rather than returning a proper error status.
- **Source:** `table/cuckoo/cuckoo_table_reader.cc` `CuckooTableIterator::SeekForPrev()`
- **Fix:** Note that it asserts rather than returning an error.

### [STALE] InternalKeyPropertiesCollector class no longer exists
- **File:** 12_table_properties.md, "Internal Property Collectors" section
- **Claim:** "InternalKeyPropertiesCollector: Tracks deletion counts and merge operand counts."
- **Reality:** The class `InternalKeyPropertiesCollector` has been removed. The functionality was folded into the core `TableProperties` struct fields (`num_deletions`, `num_merge_operands`). The deprecated accessor functions `GetDeletedKeys()` and `GetMergeOperands()` still exist in `table_properties.h` but the collector class is gone.
- **Source:** `db/table_properties_collector.h` -- contains `InternalTblPropColl`, `UserKeyTablePropertiesCollector`, `TimestampTablePropertiesCollector`, but no `InternalKeyPropertiesCollector`.
- **Fix:** Remove the reference to `InternalKeyPropertiesCollector` or rewrite to say the deletion/merge counts are now tracked directly in `TableProperties` fields during table building, not via a separate collector.

### [MISLEADING] Coupled mode disables parallel compression -- incomplete
- **File:** 08_partitioned_index_filter.md, "Filter Partitioning Modes" table
- **Claim:** "Coupled mode (`decouple_partitioned_filters=false`) disables parallel compression because the tight coupling between filter and index partitioning creates ordering dependencies incompatible with concurrent block processing."
- **Reality:** Parallel compression is also disabled when `user_defined_index_factory` is set, not only for coupled partitioned filters. The code in `block_based_table_builder.cc` sets `compression_parallel_threads = 1` when either condition is true.
- **Source:** `table/block_based/block_based_table_builder.cc` `Rep` constructor
- **Fix:** Add a note that UDI also disables parallel compression.

### [MISLEADING] format_version 3 "introduced" index_key_is_user_key
- **File:** 06_index_formats.md, "Format Version Evolution" table
- **Claim:** "3 (RocksDB 5.15) | Strips sequence numbers from index keys when not needed. Sets `index_key_is_user_key` table property."
- **Reality:** The `index_key_is_user_key` table property exists for all format versions. What format_version 3 changed is that `must_use_separator_with_seq_` is initialized to `true` for format_version <= 2, so only format_version >= 3 can actually produce user-key-only index entries. The property just evaluates to false for older versions.
- **Source:** `table/block_based/index_builder.h` `ShortenedIndexBuilder` constructor
- **Fix:** Reword to: "Enables stripping sequence numbers from index keys when not needed (the `index_key_is_user_key` property reflects whether stripping occurred)."

### [WRONG] Builder has three states, not two
- **File:** 05_table_builder.md, "Builder States" section
- **Claim:** "The builder operates in two states, tracked by `Rep::State`: kBuffered, kUnbuffered"
- **Reality:** There are three states: `kBuffered`, `kUnbuffered`, and `kClosed`. The `kClosed` state is set in both `Finish()` and `Abandon()`. The enum and all three states are documented in the code comments at `block_based_table_builder.cc` lines 908-930.
- **Source:** `table/block_based/block_based_table_builder.cc` `Rep::State` enum
- **Fix:** Add `kClosed` to the states table with description: "Terminal state. Set after `Finish()` or `Abandon()` completes. No further operations allowed."

### [MISLEADING] CompressAndVerifyBlock ratio check description
- **File:** 05_table_builder.md, "CompressAndVerifyBlock" section
- **Claim:** "If `compressed_size > uncompressed_size * max_compressed_bytes_per_kb / 1024`, the compression ratio is insufficient and the block is stored uncompressed"
- **Reality:** The actual logic computes `max_compressed_size = (max_compressed_bytes_per_kb * uncompressed_size) >> 10` and passes it as a maximum output size limit to `Compressor::CompressBlock()`. This is not a post-compression ratio check -- if compression output exceeds this limit, the compressor itself stops and the block is stored uncompressed. Also, `max_compressed_bytes_per_kb` is sanitized to `min(1023, ...)`.
- **Source:** `table/block_based/block_based_table_builder.cc` `CompressAndVerifyBlock()` lines 1950-1957
- **Fix:** Rephrase to: "The maximum compressed output size is computed as `(max_compressed_bytes_per_kb * uncompressed_size) >> 10`. This limit is passed to the compressor; if compression cannot fit within it, the block is stored uncompressed."

### [MISLEADING] kBlockTrailerSize location
- **File:** 02_block_based_format.md, "SST File Structure" section
- **Claim:** "see `kBlockTrailerSize` in `table/block_based/block_based_table_reader.h`"
- **Reality:** `kBlockTrailerSize` is defined as `static constexpr size_t kBlockTrailerSize = 5;` inside `class BlockBasedTable` in `block_based_table_reader.h`, so the file is technically correct, but it is referenced as `BlockBasedTable::kBlockTrailerSize` throughout the codebase. The reference is acceptable but could note it is a member of `BlockBasedTable`.
- **Source:** `table/block_based/block_based_table_reader.h` line 78
- **Fix:** Minor: clarify as `BlockBasedTable::kBlockTrailerSize`.

## Completeness Gaps

### User-Defined Index (UDI) -- entire subsystem missing
- **Why it matters:** UDI was added as a major new feature (commits `975848236`, `768ef1f`, `3d53af9`, `169f90c`). It allows users to provide their own index format via `UserDefinedIndexFactory`. The block type `kUserDefinedIndex` is mentioned in the block types table but nothing else about UDI appears in the docs.
- **Where to look:** `include/rocksdb/user_defined_index.h`, `table/block_based/user_defined_index_builder.h`, `table/block_based/user_defined_index_reader.h`
- **Suggested scope:** A new chapter (14_user_defined_index.md) covering the factory, builder, reader, and configuration. At minimum, mention it in chapters 05 (table builder) and 06 (index formats).

### Super Block Alignment
- **Why it matters:** Commit `742741b` added `super_block_alignment_size` and `super_block_alignment_space_overhead_ratio` to `BlockBasedTableOptions`. This is a distinct feature from `block_align` and targets filesystem super-block alignment for direct I/O efficiency.
- **Where to look:** `include/rocksdb/table.h` (`super_block_alignment_size`, `super_block_alignment_space_overhead_ratio`), `table/block_based/block_based_table_builder.cc`
- **Suggested scope:** Add to chapter 05 (table builder) or chapter 02 (file layout) as a subsection on alignment.

### Pre-defined Compression Dictionaries
- **Why it matters:** Commit `6a79e02` added support for pre-defined compression dictionaries (as opposed to sampled dictionaries). This is a compression feature that affects the table builder pipeline.
- **Where to look:** Search for "pre-defined" in compression-related code and `BlockBasedTableBuilder`
- **Suggested scope:** Mention in chapter 05 (table building pipeline) in the compression section.

### Custom Compression Algorithms (CompressionManager)
- **Why it matters:** Commit `9d49059` and format_version 7 introduced `CompressionManager` support. The doc mentions format_version 7 changes `compression_name` for CompressionManager compatibility but doesn't explain what CompressionManager is or how custom compression works.
- **Where to look:** `include/rocksdb/compression.h`, `table/block_based/block_based_table_builder.cc` (Compressor/Decompressor usage)
- **Suggested scope:** Brief mention in chapter 01 (overview) and chapter 05 (table builder) about the Compressor/Decompressor abstraction.

### Table property: key_smallest_seqno and newest_key_time
- **Why it matters:** Recent commits added `key_smallest_seqno` and `newest_key_time` as table properties. These are documented in chapter 12 but should be verified against the current code to ensure the descriptions are accurate.
- **Where to look:** `include/rocksdb/table_properties.h`
- **Suggested scope:** Already in chapter 12 -- verify accuracy only.

### Removed API: SstFileWriter::Add() and skip_filters
- **Why it matters:** Commit `4c89ff1` removed the deprecated `SstFileWriter::Add()` method and the `skip_filters` parameter. If the doc mentioned these, they should be removed.
- **Where to look:** `include/rocksdb/sst_file_writer.h`
- **Suggested scope:** Verify chapter 13 doesn't reference removed APIs.

### Block cache prepopulation during compaction
- **Why it matters:** Commit `1ed5052` added compaction-time block cache prepopulation. The doc mentions `prepopulate_block_cache` briefly in chapter 05 but doesn't detail the compaction path.
- **Where to look:** `BlockBasedTableOptions::prepopulate_block_cache`, `table/block_based/block_based_table_builder.cc`
- **Suggested scope:** Brief mention in chapter 05.

## Depth Issues

### Index size estimation lacks detail
- **Current:** Chapter 06 says "`CurrentIndexSizeEstimate()` provides a running estimate" and mentions `UpdateIndexSizeEstimate()`.
- **Missing:** The doc doesn't explain that `PartitionedIndexBuilder` uses `estimated_completed_partitions_size_` and adds a 2x buffer, or that the estimate includes ~70 bytes per partition for the top-level index. Chapter 05's tail size estimation section is better but still brief.
- **Source:** `table/block_based/index_builder.h` `PartitionedIndexBuilder::UpdateIndexSizeEstimate()`

### Parallel compression architecture is too high-level
- **Current:** Chapter 05 says "ring buffer" and "power-of-two" and "NextToWrite counter" but gives minimal detail.
- **Missing:** The actual thread coordination (mutexes, condition variables), the quasi-work-stealing mechanism, and the file size estimation formula are not explained in enough detail to understand the implementation.
- **Source:** `table/block_based/block_based_table_builder.cc` (search for `ParallelCompressionRep`)

### MaybeEnterUnbuffered transition is underexplained
- **Current:** Chapter 05 mentions the buffered-to-unbuffered transition but not the conditions (size threshold, compression dictionary training).
- **Missing:** The size threshold and its interaction with `CompressionOptions::max_dict_bytes` and `CompressionOptions::zstd_max_train_bytes`.
- **Source:** `table/block_based/block_based_table_builder.cc` `MaybeEnterUnbuffered()`

## Structure and Style Violations

### index.md is slightly short but acceptable
- **File:** index.md
- **Details:** 45 lines, within the 40-80 line range. No violations.

### No box-drawing characters found
- **Details:** All files pass this check.

### No line number references found
- **Details:** All files pass this check.

### INVARIANT usage is appropriate
- **File:** index.md "Key Invariants" section, 02_block_based_format.md "Key Invariant" inline
- **Details:** All invariants listed are true correctness invariants (checksum computation, key ordering, footer position, separator key bounds). The checksum-before-decompression invariant in 02_block_based_format.md is also appropriate. No misuse found.

## Undocumented Complexity

### BlockSearchType (kBinary, kInterpolation, kAuto) deserves its own section
- **What it is:** The `BlockSearchType` enum controls how index blocks (and potentially data blocks) are searched internally. `kAuto` uses the `is_uniform` flag from `DataBlockFooter` to choose between binary and interpolation search at runtime. This interacts with `uniform_cv_threshold` at write time.
- **Why it matters:** The interpolation search path has different CPU characteristics and requires uniformly distributed keys. A developer modifying search logic or tuning performance needs to understand the full chain from write-time uniformity detection to read-time search selection.
- **Key source:** `include/rocksdb/table.h` (`BlockSearchType` enum), `table/block_based/block_builder.cc` (`ScanForUniformity()`), `table/block_based/block.cc` (search dispatch)
- **Suggested placement:** Add to chapter 03 (data blocks) or chapter 06 (index formats). Currently split across chapters 03 and 06 without a cohesive explanation.

### OffsetableCacheKey and portable cache keys
- **What it is:** `OffsetableCacheKey` (see `cache/cache_key.h`) constructs per-block cache keys from the table's `base_cache_key` plus the block offset. The base cache key is derived from `db_session_id` and `file_number`, making cache keys unique across DB instances and reopens.
- **Why it matters:** Cache key collisions can cause data corruption. Understanding the cache key scheme is important for anyone working on the block cache or table reader.
- **Key source:** `cache/cache_key.h`, `table/block_based/block_based_table_reader.cc` `SetupBaseCacheKey()`
- **Suggested placement:** Add to chapter 04 (block reading and caching) as a subsection.

### Table reader "immortal" mode
- **What it is:** `Rep::immortal_table` flag indicates the table reader should never be evicted. When set, block contents can reference the table reader's memory directly without copying, since the table reader lifetime is guaranteed to exceed the block's use.
- **Why it matters:** Affects buffer management decisions in BlockFetcher -- an optimization for tables pinned in memory.
- **Key source:** `table/block_based/block_based_table_reader.h` `Rep::immortal_table`
- **Suggested placement:** Mention in chapter 04 (block reading).

### FSBuffer / FSAllocationPtr support in read path
- **What it is:** Commit `b9cb7b9` added support for filesystem-provided buffers (`FSBuffer` / `FSAllocationPtr`) in the point lookup read path. This allows the filesystem to provide its own memory management for read buffers.
- **Why it matters:** This is an important optimization for custom filesystems. `BlockFetcher` already mentions `fs_buf_` in the buffer table but doesn't explain when/how it's used.
- **Key source:** `table/block_fetcher.h`, `table/block_fetcher.cc`
- **Suggested placement:** Expand the buffer management section in chapter 04.

## Positive Notes

- The block-based file layout diagram in chapter 02 is clear and accurate, showing the correct ordering of blocks including the metaindex and footer.
- The data block footer bit layout explanation in chapter 03 is excellent -- the forward compatibility analysis of bit 30 is a particularly valuable detail that matches the code comments exactly.
- The separated KV storage description in chapter 03 is detailed and well-structured, covering both restart and non-restart entry formats.
- The partitioned filter boundary handling description in chapter 08 (adding next prefix to current partition and prev prefix to new partition) is a subtle but important detail that is correctly documented.
- The CuckooTable chapter 11 provides a thorough end-to-end description of the build and read workflows, including the BFS collision resolution and cache-friendly design rationale.
- The table properties chapter 12 is comprehensive, covering both built-in and user-collected properties with correct field names and descriptions.
- The SstFileWriter chapter 13 correctly documents the compression selection priority and global sequence number workflow.
- The TailPrefetchStats adaptive prefetching description in chapter 01 is detailed and accurate (512KB cap, 12.5% waste threshold, 32-entry circular buffer).
- Overall structure follows conventions well: each chapter has a Files line, uses consistent formatting, and organizes content logically from overview to details.
