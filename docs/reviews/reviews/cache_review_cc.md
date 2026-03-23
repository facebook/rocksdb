# Review: cache -- Claude Code

## Summary
Overall quality rating: **good**

The cache documentation is comprehensive, well-organized across 10 chapters, and covers the major components with reasonable accuracy. Most technical claims about data structures, algorithms, and configuration options are verified correct against the current codebase. The documentation provides genuine value for developers working with the cache subsystem.

The main concerns are: (1) a missing CacheEntryRole enum value (`kDeprecatedFilterBlock`), (2) wrong CacheEntryStatsCollector timing constants (300s/100x vs actual 180s/500x), (3) several misleading simplifications in LRU and HCC descriptions, and (4) a code quote in chapter 8 that violates style guidelines.

## Correctness Issues

### [WRONG] Missing CacheEntryRole enum value
- **File:** 01_cache_interface.md, "CacheEntryRole" section
- **Claim:** The table lists 13 roles: kDataBlock through kMisc
- **Reality:** The actual enum in `include/rocksdb/cache.h` has 14 values. Between `kFilterMetaBlock` and `kIndexBlock`, there is `kDeprecatedFilterBlock` (described as "OBSOLETE / DEPRECATED: old/removed block-based filter"). The doc omits this entirely.
- **Source:** `include/rocksdb/cache.h`, `CacheEntryRole` enum, line 63
- **Fix:** Add `kDeprecatedFilterBlock` to the table with description "Deprecated block-based filter (obsolete)" or explicitly note it is omitted as deprecated. This matters because `kNumCacheEntryRoles` counts it, so monitoring output includes a slot for it.

### [WRONG] CacheEntryStatsCollector timing constants
- **File:** 09_monitoring.md, "CacheEntryStatsCollector" section
- **Claim:** "Time-based caching: recent stats are reused within a configurable window (default: minimum 300 seconds or 100x the last scan duration, whichever is longer)"
- **Reality:** The actual values used by the caller in `db/internal_stats.cc` are: background mode: 180 seconds / 500x; foreground mode: 10 seconds / 10x. Neither "300 seconds" nor "100x" is used anywhere. The doc copied example parameters from the `cache_entry_stats.h` interface comment (which uses 300/100 as an illustrative example), not the actual defaults.
- **Source:** `db/internal_stats.cc` lines 698-700: `min_interval_seconds = foreground ? 10 : 180; min_interval_factor = foreground ? 10 : 500;`
- **Fix:** Replace with: "Background stats are reused if less than 180 seconds old or less than 500x the last scan duration. Foreground stats use tighter thresholds: 10 seconds or 10x."

### [MISLEADING] Compression framework terminology
- **File:** 05_compressed_secondary_cache.md, "Compression Behavior" section
- **Claim:** "Compression and decompression are performed using `Compressor`/`Decompressor` from the compression framework"
- **Reality:** The code uses `CompressionManager::GetCompressor()` and `GetDecompressorOptimizeFor()` from the V2 compression framework (`include/rocksdb/advanced_compression.h`). The doc names the types but does not mention the CompressionManager or the V2 framework, which may confuse readers familiar with the older `CompressionContext`/`UncompressionContext` API.
- **Source:** `cache/compressed_secondary_cache.cc`, constructor lines 54-57
- **Fix:** Change to: "Compression and decompression use the V2 compression framework via `CompressionManager` (see `include/rocksdb/advanced_compression.h`), which provides `Compressor` and `Decompressor` objects."

### [MISLEADING] LRU insertion placement oversimplification
- **File:** 02_lru_cache.md, "Insertion Placement" section
- **Claim:** "Entries with cache hits (HasHit()) are inserted into the highest available priority pool, regardless of their declared priority"
- **Reality:** This is what the `LRUCacheOptions` comment says (line 236-244 in cache.h), and the code follows it. However, the doc fails to clarify what "highest available" means: it depends on which pools are configured via non-zero ratios. If `high_pri_pool_ratio == 0`, then the "highest available" is LOW (or BOTTOM if `low_pri_pool_ratio == 0`). The doc could be read as "always goes to HIGH."
- **Source:** `include/rocksdb/cache.h`, LRUCacheOptions comments lines 236-244
- **Fix:** Add a clarifying note: "The 'highest available' pool depends on which pool ratios are non-zero. A pool is only available if its ratio > 0."

### [MISLEADING] Eviction "batch of 4 slots" oversimplification
- **File:** 03_hyper_clock_cache.md, "Eviction Algorithm" section
- **Claim:** "Each thread atomically advances a shared `clock_pointer_` by a batch of 4 slots"
- **Reality:** For FixedHyperClockTable, `step_size=4` does mean 4 individual slots. For AutoHyperClockTable, `step_size=4` refers to 4 chain homes (each of which may contain multiple entries in a chain), not 4 individual slots. The doc does not distinguish between the two variants.
- **Source:** `cache/clock_cache.cc`, FixedHyperClockTable::Evict line 1110; AutoHyperClockTable::Evict line 3440
- **Fix:** Add: "In FixedHyperClockTable, this is 4 individual slots. In AutoHyperClockTable, this is 4 chain homes, each potentially containing multiple entries."

### [MISLEADING] "strict_capacity_limit=false (default), inserts always succeed"
- **File:** index.md, "Key Invariants" bullet; 01_cache_interface.md, "Abstract Base Class" section
- **Claim:** "with strict_capacity_limit=false (default), inserts always succeed even when over capacity"
- **Reality:** For HyperClockCache, this is mostly true but with a caveat: with `eviction_effort_cap`, insert succeeds but the item may go over capacity. For LRUCache, inserts can technically fail with an allocation failure, though this is not a capacity-related failure. The statement is operationally correct but omits the HCC `eviction_effort_cap` interaction already documented in chapter 3. More importantly, `CreateStandalone` can also fail if strict_capacity_limit is true.
- **Source:** `cache/clock_cache.cc`, `cache/lru_cache.cc`
- **Fix:** Minor -- add "(see eviction_effort_cap for HyperClockCache behavior under pressure)" as a cross-reference.

### [MISLEADING] use_adaptive_mutex default described as "platform-dependent"
- **File:** 02_lru_cache.md, "Configuration" table
- **Claim:** `use_adaptive_mutex` default is "platform-dependent"
- **Reality:** The default is controlled by the compile flag `-DROCKSDB_DEFAULT_TO_ADAPTIVE_MUTEX`, not by the platform. The constant `kDefaultToAdaptiveMutex` is `true` if this flag is set, `false` otherwise. The same platform with different compile flags produces different defaults.
- **Source:** `include/rocksdb/cache.h` lines 248-252; `port/port_posix.cc` lines 43-47
- **Fix:** Change to "build-configuration-dependent (true if compiled with -DROCKSDB_DEFAULT_TO_ADAPTIVE_MUTEX, false otherwise)"

## Completeness Gaps

### Missing: V2 Compression Framework in CompressedSecondaryCache
- **Why it matters:** The compressed secondary cache now uses the newer `CompressionManager`/`Compressor`/`Decompressor` API rather than the older `CompressionContext`. Developers debugging compression issues need to know which API is in use.
- **Where to look:** `cache/compressed_secondary_cache.cc` constructor, `include/rocksdb/advanced_compression.h`
- **Suggested scope:** A brief mention in chapter 5 that the V2 compression framework is used.

### Missing: `kDeprecatedFilterBlock` role in monitoring data
- **Why it matters:** When reading `kBlockCacheEntryStats` output, users may see an extra role slot they can't explain. The deprecated role still occupies a position in `kNumCacheEntryRoles` and appears in monitoring output.
- **Where to look:** `include/rocksdb/cache.h` enum, `cache/cache_entry_roles.cc`
- **Suggested scope:** Mention in existing chapter 1 CacheEntryRole table.

### Missing: `SupportForceErase()` method on SecondaryCache
- **Why it matters:** The secondary cache lookup flow in chapter 4 mentions `SupportForceErase()` but it's not in the method table at the top of the chapter. This is a required virtual method.
- **Where to look:** `include/rocksdb/secondary_cache.h`, line 120
- **Suggested scope:** Add to the SecondaryCache method table in chapter 4.

### Missing: `SecondaryCacheWrapper` delegation class
- **Why it matters:** Similar to `CacheWrapper`, `SecondaryCacheWrapper` provides a delegation pattern for extending `SecondaryCache`. `TieredSecondaryCache` extends it. Not mentioned anywhere in the docs.
- **Where to look:** `include/rocksdb/secondary_cache.h`, line 158
- **Suggested scope:** Brief mention in chapter 4.

### Missing: TypedCache / BlockCacheInterface template layer
- **Why it matters:** The docs mention `FullTypedCacheInterface` and `BlockCacheInterface` briefly in chapter 8, but the typed cache layer (`cache/typed_cache.h`) with its template hierarchy (`BaseCacheInterface`, `PlaceholderCacheInterface`, `BasicTypedCacheInterface`, `FullTypedCacheInterface`) is the primary way RocksDB code interacts with the cache. Developers modifying cache interactions will encounter this layer.
- **Where to look:** `cache/typed_cache.h`
- **Suggested scope:** Brief mention in chapter 1 under a "Typed Cache Wrappers" section.

### Missing: `MakeSharedRowCache()` on LRUCacheOptions
- **Why it matters:** LRUCacheOptions has both `MakeSharedCache()` and `MakeSharedRowCache()`. The row cache variant may have different behavior or constraints. Not mentioned in the docs.
- **Where to look:** `include/rocksdb/cache.h`, line 274
- **Suggested scope:** Mention in chapter 2 or chapter 10.

### Missing: `uncache_aggressiveness` option
- **Why it matters:** New CF option (`ColumnFamilyOptions::uncache_aggressiveness`) proactively erases block cache entries when their files become obsolete. Default is 0 (off). Particularly beneficial for HCC which has longer memory than LRU. This directly affects cache behavior.
- **Where to look:** `include/rocksdb/options.h`, `uncache_aggressiveness` field
- **Suggested scope:** Mention in chapter 10 configuration/best practices.

### Missing: `PrepopulateBlockCache::kFlushAndCompaction` enum value
- **Why it matters:** New option value enables block cache warming during compaction (not just flush). Compaction-warmed blocks use BOTTOM priority vs LOW for flush-warmed. Not mentioned in the configuration chapter's block cache options table.
- **Where to look:** `include/rocksdb/table.h`, `PrepopulateBlockCache` enum
- **Suggested scope:** Add to chapter 10 block cache options table.

### Missing: `ApplyToHandle()` cache method
- **Why it matters:** New virtual method on `Cache` (and forwarded by `CacheWrapper`) that allows callbacks on cache handles. Useful for multi-instance caches to determine which instance a handle belongs to.
- **Where to look:** `include/rocksdb/advanced_cache.h`
- **Suggested scope:** Add to chapter 1 core operations table.

### Missing: `compress_format_version` removal
- **Why it matters:** `CompressedSecondaryCacheOptions::compress_format_version` was removed as a useless option. The docs correctly do NOT mention it, but there is no note about the removal for users migrating from older versions.
- **Where to look:** Commit `48ec45d7b`
- **Suggested scope:** Brief mention in chapter 5 or chapter 10 as a removed option.

### Missing: `block_cache_compressed` deprecation context
- **Why it matters:** Chapter 10 mentions `block_cache_compressed = nullptr (deprecated; use secondary cache instead)` but doesn't explain what it was or why it was deprecated, which would help users migrating old configurations.
- **Where to look:** `include/rocksdb/table.h`
- **Suggested scope:** One-sentence explanation in chapter 10.

## Depth Issues

### Tiered cache initial capacity math needs clarification
- **Current:** Chapter 6 says "primary cache capacity equals total_capacity" and "a reservation of secondary_capacity is placed against the primary cache"
- **Missing:** This is confusing because it sounds like the primary gets the full capacity AND then a reservation is taken from it. The actual mechanism: the primary cache is created with `total_capacity`, then a reservation of `total_capacity * compressed_secondary_ratio` is made via dummy entries, leaving `total_capacity * (1 - compressed_secondary_ratio)` for actual cache entries. The secondary cache also gets its own capacity of `total_capacity * compressed_secondary_ratio`.
- **Source:** `cache/secondary_cache_adapter.cc`, constructor with `distribute_cache_res`

### EvictionHandler admission policy table could show kAdmPolicyPlaceholder/kAdmPolicyAuto behavior more clearly
- **Current:** Chapter 4 says kAdmPolicyPlaceholder/kAdmPolicyAuto do "regular insert (secondary cache applies its own two-hit admission)"
- **Missing:** The distinction between `force_insert=true` and `force_insert=false` in the secondary cache `Insert()` call is the key behavior difference. With force=false, the secondary cache's own admission policy (e.g., two-hit) can reject the entry. With force=true, it's a guaranteed insert.
- **Source:** `cache/secondary_cache_adapter.cc`, `EvictionHandler()`

### CacheKey uniqueness guarantee could be more precise
- **Current:** Chapter 8 says "Globally unique with very high probability"
- **Missing:** The uniqueness guarantee is actually stronger for files generated within a single process lifetime (guaranteed unique by construction). The "very high probability" qualifier applies to cross-process uniqueness, which depends on the randomness of `db_session_id`.
- **Source:** `cache/cache_key.cc`, `OffsetableCacheKey` construction

## Structure and Style Violations

### Inline code quote in chapter 8
- **File:** 08_block_cache_keys.md
- **Details:** Contains a fenced code block showing `block_key = CacheKey(file_num_etc64_, offset_etc64_ ^ block_offset)`. Per style guidelines, inline code quotes are not allowed. This should be described in prose, e.g., "Individual block keys are derived by XOR-ing the file's `offset_etc64_` with the block offset."

### Index.md line count
- **File:** index.md
- **Details:** 41 lines -- within the 40-80 line range. No issue.

### Files: lines present in all chapters
- **Details:** All chapters have correct `**Files:**` lines with plausible paths. Verified: all referenced files exist in the codebase.

### No box-drawing characters
- **Details:** Confirmed no box-drawing characters found.

### No line number references
- **Details:** Confirmed no line number references found.

### INVARIANT keyword usage
- **Details:** The word "Invariant" appears only in the index.md section header "Key Invariants" -- not used to label claims within chapters. The invariants listed (key uniqueness, 16-byte requirement, 256KB multiples, strict_capacity_limit behavior) are reasonable correctness properties.

## Undocumented Complexity

### V2 Compression Manager integration
- **What it is:** The compressed secondary cache now uses `CompressionManager::GetCompressor()` and `GetDecompressorOptimizeFor()` from the new V2 compression framework. This is a departure from the older compression infrastructure.
- **Why it matters:** Developers adding new compression support or debugging compression issues in the secondary cache need to know they're working with the V2 API. The `CompressedSecondaryCacheOptions::compression_opts` is passed through to the V2 compressor.
- **Key source:** `cache/compressed_secondary_cache.cc` constructor, `include/rocksdb/advanced_compression.h`
- **Suggested placement:** Add to existing chapter 05

### `cache_index_and_filter_blocks_with_high_priority` default changed to true
- **What it is:** The doc in chapter 10 correctly states the default is `true`, but this was a change from earlier versions where it defaulted to `false`. This is a potentially impactful behavioral change for users reading older documentation.
- **Why it matters:** Users upgrading from older RocksDB versions may not realize this default changed, which affects cache eviction behavior.
- **Key source:** `include/rocksdb/table.h`, `BlockBasedTableOptions`
- **Suggested placement:** Note in chapter 10

### `ReportProblems()` in HyperClockCache
- **What it is:** Documented briefly in chapter 9 but could benefit from more detail. HyperClockCache's `ReportProblems()` checks for: load factor issues (table too full or too empty), excessive eviction effort, potential performance problems from configuration mismatch.
- **Why it matters:** This is a key diagnostic tool for production HCC users. Understanding what it reports helps with troubleshooting.
- **Key source:** `cache/clock_cache.cc`, `ReportProblems()` implementations in both Fixed and Auto variants
- **Suggested placement:** Expand in chapter 9 or add to chapter 3

### `kSliceCacheItemHelper` for secondary cache warming
- **What it is:** A pre-defined `CacheItemHelper` for entries that just need to be copied into secondary cache (like compressed blocks). Defined in `include/rocksdb/secondary_cache.h`.
- **Why it matters:** This is a convenience utility that users of the secondary cache API should know about.
- **Key source:** `include/rocksdb/secondary_cache.h`, line 218
- **Suggested placement:** Mention in chapter 4

## Positive Notes

- **Excellent coverage of HyperClockCache internals** -- chapter 3 accurately describes the SlotMeta bit layout, countdown values, load factors, and eviction algorithm. All numerical values verified correct against the code.
- **Accurate description of the three-tier LRU list** -- chapter 2 correctly describes the marker pointers, eviction order, insertion placement, and pool size maintenance.
- **Good practical guidance in chapter 10** -- the "Common Pitfalls" section covers genuinely important issues (cache thrashing, scan pollution, index/filter blocks outside cache, iterator pinned memory, NUMA effects) with actionable advice.
- **Cache reservation chapter is thorough** -- accurately describes the 256KB granularity, delayed decrease at 3/4 threshold, WriteBufferManager integration, and all memory consumers.
- **Tiered cache chapter captures the complexity well** -- the proportional reservation distribution mechanism with Deflate/Inflate is correctly described.
- **Secondary cache adapter flow is well-documented** -- the lookup and eviction flows in chapter 4 match the actual code paths in `secondary_cache_adapter.cc`.
- **Block cache keys chapter correctly explains the XOR derivation** and its performance properties.
