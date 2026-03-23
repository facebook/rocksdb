# Fix Summary: sst_table_format

## Issues Fixed

**Correctness: 18**
- format_version default 6 -> 7 (index.md, 01_overview.md, 06_index_formats.md)
- TailPrefetchStats "smallest" -> "maximum" qualifying size (01_overview.md)
- Context checksums source: "file unique ID" -> "per-file semi-random value" (01_overview.md, index.md)
- XXH3 default not conditional on format_version (01_overview.md)
- Format version 5 description: removed XXH3 claim, added FastLocalBloom (01_overview.md)
- DecodeKeyV4 file reference: block_builder.cc -> block_util.h (01_overview.md)
- CompressAndVerifyBlock: post-compression ratio check -> limit passed to compressor (05_table_builder.md)
- Builder has three states, not two: added kClosed (05_table_builder.md)
- Format version 3 description: reworded index_key_is_user_key semantics (06_index_formats.md)
- Format version 5 evolution: removed kBinarySearchWithFirstKey conflation (06_index_formats.md)
- HashIndexBuilder::CurrentIndexSizeEstimate() returns 0 (06_index_formats.md)
- SeekForGetImpl record types: expanded list to include kTypeMerge, kTypeWideColumnEntity, kTypeValuePreferredSeqno (09_data_block_hash_index.md)
- kValueTypeSeqId0 byte: 0x80 -> 0xFF (10_plain_table.md)
- PlainTable footer version: always 0, not encoding-dependent (10_plain_table.md)
- CuckooTable value size rule: qualified for non-deletion values only (11_cuckoo_table.md)
- InternalKeyPropertiesCollector: removed (class no longer exists), expanded internal collectors list (12_table_properties.md)
- TableProperties::Add() and ApproximateMemoryUsage(): documented as partial/approximate (12_table_properties.md)
- Global seqno: documented write_global_seqno=false default and deprecated status (13_sst_file_writer.md)

**Completeness: 5**
- Block type compression: added read-side vs write-side distinction (02_block_based_format.md)
- Separated KV option name: use_separated_kv_storage -> separate_key_value_in_data_block (03_data_blocks.md)
- Uniform key detection scope: noted relevance to index blocks (03_data_blocks.md)
- Persistent cache keys: documented base cache key + handle, not raw handle (04_block_fetcher.md)
- Coupled mode parallel compression: added UDI also disables it (08_partitioned_index_filter.md)

**Structure: 3**
- PartitionedIndexBuilder::AddIndexEntry: fixed flush-before-add ordering (08_partitioned_index_filter.md)
- PartitionedFilterBlockReader: distinguished filter_map_ (subset OK) from partition_map_ (all-or-none) (08_partitioned_index_filter.md)
- Block alignment: separated block_align from super_block_alignment_size behavior (05_table_builder.md)

**Style: 4**
- PlainTable Prev() behavior: assert(false), not Status::NotSupported (10_plain_table.md)
- CuckooTable SeekForPrev() behavior: assert(false), not "not implemented" (11_cuckoo_table.md)
- PlainTable full_scan_mode: SeekToFirst() still works (10_plain_table.md)
- Data block hash index footer: updated to modern 28-bit + 4-bit layout (09_data_block_hash_index.md)
- kNoEntry semantics: qualified with last-restart-interval fallback (09_data_block_hash_index.md)
- Legacy block-based filter: noted reader still recognizes old format (07_filter_blocks.md)
- optimize_filters_for_memory default: false -> true (07_filter_blocks.md)
- bloom_before_level: added exact threshold semantics (07_filter_blocks.md)
- BlockBasedTable::kBlockTrailerSize qualification (02_block_based_format.md)

## Disagreements Found

5 debates documented in `docs/reviews/sst_table_format_debates.md`:
1. TailPrefetchStats smallest vs maximum (Codex correct)
2. Context checksums source (Codex correct)
3. PlainTable footer format_version (Codex correct)
4. Block type compression table vs read-side rule (both partially correct)
5. Partitioned filter vs index caching semantics (Codex correct)

## Changes Made

| File | Changes |
|------|---------|
| `index.md` | format_version default 7, context checksum source |
| `01_overview.md` | format_version 7, TailPrefetchStats maximum, context checksums, XXH3 default, format version 5 FastLocalBloom, DecodeKeyV4 location |
| `02_block_based_format.md` | Block type compression read-side note, kBlockTrailerSize qualified |
| `03_data_blocks.md` | Option name fix, uniform key detection scope, cross-reference |
| `04_block_fetcher.md` | Persistent cache key description |
| `05_table_builder.md` | Added kClosed state, CompressAndVerifyBlock limit, block_align vs super_block_alignment_size |
| `06_index_formats.md` | format_version 7, version 3 reworded, version 5 rewritten, HashIndexBuilder estimate qualified |
| `07_filter_blocks.md` | Legacy filter reader compat, optimize_filters_for_memory=true, bloom_before_level semantics |
| `08_partitioned_index_filter.md` | UDI parallel compression, flush-before-add ordering, filter_map_ vs partition_map_ semantics |
| `09_data_block_hash_index.md` | SeekForGet vs Seek, record types expanded, footer 28-bit layout, kNoEntry semantics |
| `10_plain_table.md` | kValueTypeSeqId0=0xFF, footer version always 0, Prev() assert, full_scan_mode SeekToFirst |
| `11_cuckoo_table.md` | Non-deletion value size rule, SeekForPrev assert, deletion storage |
| `12_table_properties.md` | Removed InternalKeyPropertiesCollector, expanded internal collectors, Add/ApproximateMemoryUsage qualified |
| `13_sst_file_writer.md` | Global seqno write_global_seqno=false default, deprecated, allow_db_generated_files |
| `docs/reviews/sst_table_format_debates.md` | NEW -- 5 disagreements documented |
