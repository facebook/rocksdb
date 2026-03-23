# Review: filter -- Claude Code

## Summary
Overall quality rating: **good**

The filter documentation is comprehensive and well-structured, covering all major filter subsystems (FastLocalBloom, Ribbon, DynamicBloom, partitioned filters, prefix filtering, caching) with accurate algorithmic descriptions and code-level detail. The chapter organization is logical and the index follows the established pattern.

The biggest strength is the depth of algorithmic coverage -- the FastLocalBloom SIMD path, Ribbon construction/fallback logic, and DynamicBloom double-probe design are explained clearly and match the code. The biggest concern is a **wrong default value** for `cache_index_and_filter_blocks` (claimed `true`, actually `false`), plus an incomplete `num_probes` table and a misleading claim about LegacyBloom FP rate degradation threshold.

## Correctness Issues

### [WRONG] cache_index_and_filter_blocks default is false, not true
- **File:** `10_caching_and_pinning.md`, section "cache_index_and_filter_blocks"
- **Claim:** "`BlockBasedTableOptions::cache_index_and_filter_blocks` (default `true`)"
- **Reality:** The default is `false`. The field is declared as `bool cache_index_and_filter_blocks = false;` in `include/rocksdb/table.h` (line 176).
- **Source:** `include/rocksdb/table.h:176`
- **Fix:** Change "(default `true`)" to "(default `false`)" and adjust the explanation. With `false` as default, filters are pinned in the table reader by default, which changes the practical implications described.

### [MISLEADING] Num probes table truncated -- missing entries for 9-24 probes
- **File:** `02_fast_local_bloom.md`, section "Num Probes Selection"
- **Claim:** Table shows entries up to `> 50000 -> 24` but jumps from `11721-14001 -> 8` directly to `> 50000 -> 24`.
- **Reality:** `FastLocalBloomImpl::ChooseNumProbes()` in `util/bloom_impl.h` has explicit entries for 9 probes (<=16050), 10 probes (<=18300), 11 probes (<=22001), 12 probes (<=25501), and a formula `(millibits_per_key - 1) / 2000 - 1` for the range 25501-50000.
- **Source:** `util/bloom_impl.h:188-197`
- **Fix:** Either include the full table (all thresholds through 12 probes plus the formula range) or add a note like "Intermediate values omitted; see `ChooseNumProbes()` for the complete mapping up to 24 probes."

### [MISLEADING] LegacyBloom FP degradation threshold conflated with warning threshold
- **File:** `02_fast_local_bloom.md`, section "LegacyBloom (Deprecated)"
- **Claim:** "32-bit hash only -- significant FP rate degradation above ~3 million keys"
- **Reality:** The code comment in `util/bloom_impl.h` (lines 113-115) says the inflection point for 32-bit hash fingerprint FP rate is about **40 million keys** at 10 bits/key. The 3 million threshold is the **warning** trigger in `LegacyBloomBitsBuilder::Finish()` (`filter_policy.cc:1193`), which fires when estimated FP exceeds 1.5x the expected rate -- that's a more conservative threshold for issuing the warning, not the point where degradation actually becomes significant.
- **Source:** `util/bloom_impl.h:113-115`, `table/block_based/filter_policy.cc:1193`
- **Fix:** Change to something like: "32-bit hash only -- at 10 bits/key, fingerprint FP rate becomes significant around ~40 million keys. A warning is logged at >= 3 million keys when estimated FP exceeds 1.5x expected."

### [MISLEADING] AddKeyAndAlt deduplication description lists 5 checks but one is wrong
- **File:** `06_construction_pipeline.md`, section "Deduplication / AddKeyAndAlt"
- **Claim:** Lists `key_hash vs prev_alt_hash` as a check "(e.g., key == prefix of previous key group, as in reverse byte-wise comparator)"
- **Reality:** Looking at the `AddKeyAndAlt` comment in `filter_policy_internal.h` (lines 38-43), the documented dedup pairs are: `k2<>k1, k2<>a2, a2<>k1` for the first call and `k3<>k2, a3<>a2, k3<>a2, a3<>k2` for subsequent calls. The doc reorganizes these into a bulleted list of 5 items. The last bullet `key_hash vs prev_alt_hash` is correct per the code comment (`a3<>k2` maps to current alt vs previous key, not current key vs previous alt). The doc's label is **reversed**: it says `key_hash vs prev_alt_hash` but should say `alt_hash vs prev_key_hash` for that entry, which is already listed as item 3. The actual 5th dedup is `key_hash vs prev_alt_hash`, matching `k3<>a2` in the comment -- but that's listed as item 3 in the doc. The bullets have two items swapped.
- **Source:** `table/block_based/filter_policy_internal.h:38-43`
- **Fix:** Verify the exact dedup pairs against `XXPH3FilterBitsBuilder::AddKeyAndAlt()` implementation in `filter_policy.cc` and reorder the bullet list to match the code comment's enumeration. Alternatively, just reference the comment and list fewer items.

## Completeness Gaps

### Missing: ReadOnlyBuiltinFilterPolicy context
- **Why it matters:** The doc mentions `ReadOnlyBuiltinFilterPolicy` in the class hierarchy and Available Filter Implementations table (chapter 01) but doesn't explain when it's encountered. Developers debugging old OPTIONS files or migration scenarios need to know this is the policy used when an OPTIONS file specifies `"rocksdb.BuiltinBloomFilter"` without a bits-per-key configuration.
- **Where to look:** `table/block_based/filter_policy_internal.h:172-184`, `FilterPolicy::CreateFromString` in `filter_policy.cc`
- **Suggested scope:** Brief mention in chapter 01 under "ReadOnlyBuiltin" row

### Missing: FullFilterBlockBuilder timestamp stripping
- **Why it matters:** When user-defined timestamps are enabled, filter keys are stripped of timestamps before being added to the filter. This is handled by the table builder calling `AddWithPrevKey(key_without_ts, ...)`. Without this context, someone debugging timestamp-related filter behavior would be confused.
- **Where to look:** `table/block_based/block_based_table_builder.cc` (the table builder strips timestamps before calling filter builder methods), `FullFilterKeyMayMatch` in `block_based_table_reader.cc:2327` (`StripTimestampFromUserKey`)
- **Suggested scope:** Add a note in chapter 04 (Full Filter Blocks) or chapter 09 (Prefix Filtering)

### Missing: UncacheAggressivenessAdvisor in EraseFromCacheBeforeDestruction
- **Why it matters:** The `EraseFromCacheBeforeDestruction` description in chapter 05 doesn't mention the `UncacheAggressivenessAdvisor` which controls how many partitions to erase. With many partitions, only a subset may be erased per call.
- **Where to look:** `table/block_based/partitioned_filter_block.cc:686-730`
- **Suggested scope:** Brief mention in chapter 05 or chapter 10

## Depth Issues

### FastLocalBloom construction prefetching needs more detail
- **Current:** "Construction uses an 8-entry ring buffer: prefetch the cache line for entry i+8 while processing entry i"
- **Missing:** This describes the behavior in `AddAllEntries()` in `filter_policy.cc`, but the doc doesn't mention that `AddAllEntries()` is the function doing this, or that it processes all accumulated hashes in a single pass with sorted order and deduplication already handled by the `AddKey`/`AddKeyAndAlt` stage.
- **Source:** `table/block_based/filter_policy.cc`, `FastLocalBloomBitsBuilder::AddAllEntries()`

### Ribbon space efficiency table uses approximate values without sourcing
- **Current:** Table claims "1% FP -> Bloom 10 bits/key, Ribbon ~7 bits/key"
- **Missing:** The ~7 bits/key for Ribbon is approximate. The actual value depends on the number of entries and the internal `GetBytesForOneInFpRate()` calculation. The doc should note these are approximate and reference where to compute exact values.
- **Source:** `table/block_based/filter_policy.cc`, `Standard128RibbonBitsBuilder::CalculateSpace()`

## Structure and Style Violations

### index.md line count
- **File:** `index.md`
- **Details:** 44 lines. Within the 40-80 line guideline.

### No violations detected for:
- No line number references found
- No box-drawing characters found
- No inappropriate INVARIANT usage (the invariants listed are genuine correctness invariants)
- Files: lines present on all chapters
- Options referenced with field names and header paths

## Undocumented Complexity

### FilterBitsBuilder::AddKeyAndAlt full dedup implementation
- **What it is:** The `XXPH3FilterBitsBuilder::AddKeyAndAlt()` implementation in `filter_policy.cc` maintains `prev_key_hash_` and `prev_alt_hash_` state variables. The alt (prefix) is always added first, then the key, so that `entries.back()` always holds the most recent key hash. This ordering matters for the primary `AddKey()` dedup path which checks `entries.back()`.
- **Why it matters:** The interaction between `AddKey()` and `AddKeyAndAlt()` is subtle -- `AddKey()` after `AddKeyAndAlt()` only deduplicates against the previous key hash (not the previous alt hash), as noted in the `FilterBitsBuilder` interface comment (`AddKey(k4) // de-dup k4<>k3 BUT NOT k4<>a3`). This can lead to duplicate prefix entries if `AddKey` is called after `AddKeyAndAlt` and the key happens to equal the previous alt.
- **Key source:** `table/block_based/filter_policy_internal.h:34-44`, `filter_policy.cc` XXPH3FilterBitsBuilder::AddKeyAndAlt implementation
- **Suggested placement:** Add detail to chapter 06 (Construction Pipeline)

### Partitioned filter parallel compression interaction
- **What it is:** `UpdateFilterSizeEstimate()` in `partitioned_filter_block.cc` uses `RelaxedAtomic<size_t>` for `completed_partitions_size_` because it can be called from background worker threads during parallel compression. The decoupled partitioning mode is compatible with parallel compression; coupled mode forces `parallel_threads = 1`.
- **Why it matters:** This is a non-obvious threading constraint that affects performance configuration.
- **Key source:** `table/block_based/partitioned_filter_block.cc`, `PartitionedFilterBlockBuilder::UpdateFilterSizeEstimate()`
- **Suggested placement:** Add to chapter 05 (Partitioned Filter Blocks) in the Decoupled vs Coupled table (already partially covered)

### FullFilterKeysMayMatch (MultiGet filter path in table reader)
- **What it is:** `BlockBasedTable::FullFilterKeysMayMatch()` in `block_based_table_reader.cc` handles the MultiGet filter check path. It checks `whole_key_filtering` to choose between `KeysMayMatch` and `PrefixesMayMatch`. The prefix path skips filtering if the prefix extractor has changed (checked via `PrefixExtractorChanged()`).
- **Why it matters:** The MultiGet filter path is different from the single-key path and has its own prefix extractor compatibility check.
- **Key source:** `table/block_based/block_based_table_reader.cc:2358-2407`
- **Suggested placement:** Mention in chapter 09 (Prefix Filtering) alongside the single-key query path

## Positive Notes

- The FastLocalBloom SIMD description (chapter 02) is exceptionally detailed and accurate, covering the AVX2 permute+blend hack, the golden ratio power advancement (`0xab25f4c1`), and probe-count selector mask construction. All verified against `bloom_impl.h`.
- The Ribbon fallback conditions (chapter 03) are complete and accurate -- all four conditions (too many keys, small filter, construction failure, cache reservation failure) match the code.
- The DynamicBloom chapter (08) correctly describes the relaxed atomic memory ordering rationale (happens-before via sequence number visibility) and the check-before-CAS optimization.
- The cross-version compatibility explanation (chapter 07) is clear and practically useful, explaining the graceful degradation to `AlwaysTrueFilter` for unknown discriminator bytes.
- The `bloom_before_level` table (chapter 01) is accurate and the mutable-at-runtime note with `SetOptions()` example is a useful practical detail.
- The `AddKeyAndAlt` deduplication concept (chapters 04 and 06) correctly identifies this as a key innovation for combined whole-key + prefix filtering efficiency.
