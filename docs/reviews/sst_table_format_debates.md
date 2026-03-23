# Debates: sst_table_format Documentation Reviews

## Debate: TailPrefetchStats smallest vs maximum qualifying size
- **CC position**: "GetSuggestedPrefetchSize() computes the smallest prefetch size that wastes no more than 12.5%"
- **Codex position**: "The algorithm sorts historical sizes and returns the maximum qualifying size under the wasted <= read / 8 rule"
- **Code evidence**: `table/block_based/block_based_table_factory.cc` line 62 comment: "Of the historic size, we find the maximum one that satisfies the condition that if prefetching all, less than 1/8 will be wasted." The algorithm sorts sizes ascending and iterates from largest to smallest, returning the first that satisfies the waste constraint.
- **Resolution**: Codex is correct. The function returns the maximum qualifying size, not the smallest.
- **Risk level**: medium -- the doc gives the exact opposite impression of the algorithm's intent.

## Debate: Context checksums source (file unique ID vs semi-random)
- **CC position**: Did not challenge the doc's claim that base_context_checksum is "derived from the file's unique ID"
- **Codex position**: "The writer generates a non-zero semi-random base_context_checksum, stores it in the footer... The code comments explicitly say the ideal unique-ID-based scheme is still a TODO."
- **Code evidence**: `table/block_based/block_based_table_builder.cc` initializes `base_context_checksum` from a random value. The unique-ID-based derivation is a TODO in code comments.
- **Resolution**: Codex is correct. The current implementation uses a random value, not a file unique ID derivation.
- **Risk level**: medium -- a reader relying on the unique-ID claim would have wrong expectations about cross-file checksum uniqueness guarantees.

## Debate: PlainTable footer format_version (always 0 vs encoding-dependent)
- **CC position**: Did not flag this issue (focused on kValueTypeSeqId0 byte value instead)
- **Codex position**: "PlainTableBuilder always writes footer format version 0. The actual encoding type is stored separately in the user property."
- **Code evidence**: `table/plain/plain_table_builder.cc` line 313: `footer.Build(kPlainTableMagicNumber, /* format_version */ 0, ...)` -- always 0. Separately, line 97: `properties_.format_version = (encoding_type == kPlain) ? 0 : 1;` -- the TABLE PROPERTY format_version varies, but the FOOTER's format_version is always 0.
- **Resolution**: Codex is correct about the footer. The doc conflates the footer format_version (always 0) with the table property format_version (0 or 1 by encoding type).
- **Risk level**: low -- subtle distinction between footer version and property version.

## Debate: Block type compression -- doc vs read-side rule
- **CC position**: Did not flag the compression table issue
- **Codex position**: "BlockTypeMaybeCompressed() only excludes kFilter, kCompressionDictionary, and kUserDefinedIndex. Other meta blocks may be compressed if written that way."
- **Code evidence**: Read side (`block_based_table_reader.h` line 569-572): `BlockTypeMaybeCompressed` excludes only kFilter, kCompressionDictionary, kUserDefinedIndex. Write side (`block_based_table_builder.cc`): only kData and kIndex go through `CompressAndVerifyBlock`. The doc's table marks kProperties, kRangeDeletion, kMetaIndex as "Never" which is correct for the standard writer but not enforced on the read side.
- **Resolution**: Both are partially correct. The doc accurately describes write-side behavior. Adding a note about the read-side rule (which is more permissive) would improve clarity.
- **Risk level**: low -- in practice, only the standard writer creates these files.

## Debate: Partitioned filter vs index caching semantics
- **CC position**: Did not flag this distinction
- **Codex position**: "PartitionIndexReader keeps partition_map_ as 'all or none', but PartitionedFilterBlockReader::filter_map_ is explicitly allowed to hold only a subset of partitions"
- **Code evidence**: `partitioned_index_reader.h` line 53: "This is expected to be 'all or none'". `partitioned_filter_block.h` line 224: "Can be a subset of blocks in case some fail insertion on attempt to pin."
- **Resolution**: Codex is correct. The two caching maps have different contracts.
- **Risk level**: medium -- a developer assuming they work the same way could introduce bugs.
