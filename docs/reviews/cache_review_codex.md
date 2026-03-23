# Review: cache — Codex

## Summary
Overall quality rating: significant issues

The chapter split and file coverage are good, and the docs generally point readers at the right implementation areas. The LRU/HCC high-level comparisons are mostly directionally correct, and the tiered-cache chapter does a decent job explaining why resize ordering matters.

The main problem is that several user-facing semantics are stale or overgeneralized in ways that will mislead someone configuring or debugging RocksDB today. The biggest errors are in option semantics, secondary-cache admission behavior, and monitoring. A lot of the prose appears to have copied older comments or implementation-specific behavior into generic documentation without re-checking the current code and tests.

## Correctness Issues

### [WRONG] `fill_cache` is documented as a `BlockBasedTableOptions` field
- **File:** `docs/components/cache/10_configuration.md`, section `Block Cache Options in BlockBasedTableOptions`
- **Claim:** "`fill_cache` | true | Add blocks to cache on read"
- **Reality:** `fill_cache` is a `ReadOptions` field, not a `BlockBasedTableOptions` field. The block-based table options struct does not contain this member.
- **Source:** `include/rocksdb/options.h`, struct `ReadOptions`; `include/rocksdb/table.h`, struct `BlockBasedTableOptions`
- **Fix:** Remove `fill_cache` from the `BlockBasedTableOptions` table and document it with read-path tuning instead.

### [STALE] `block_cache_compressed` is documented as a live deprecated option
- **File:** `docs/components/cache/10_configuration.md`, section `Block Cache Options in BlockBasedTableOptions`
- **Claim:** "`block_cache_compressed` | nullptr | Deprecated; use secondary cache instead"
- **Reality:** the public option has been removed. The option parser still recognizes the name only as deprecated input, but there is no `BlockBasedTableOptions::block_cache_compressed` field anymore.
- **Source:** `table/block_based/block_based_table_factory.cc`, `block_based_table_type_info`; `include/rocksdb/advanced_options.h`
- **Fix:** Remove it from the live options table. If you want a migration note, state separately that the old option name is deprecated/removed and secondary cache is the replacement.

### [STALE] Pinning guidance is built around deprecated fallback flags and misstates the scope of `pin_l0_filter_and_index_blocks_in_cache`
- **File:** `docs/components/cache/10_configuration.md`, sections `Index and Filter Blocks Outside Cache` and `Block Cache Options in BlockBasedTableOptions`
- **Claim:** "The top-level index/filter can be configured separately via `pin_top_level_index_and_filter`." Also: "`pin_l0_filter_and_index_blocks_in_cache` | false | Keep L0 index/filter blocks pinned"
- **Reality:** both `pin_top_level_index_and_filter` and `pin_l0_filter_and_index_blocks_in_cache` are deprecated compatibility knobs. The current API is `MetadataCacheOptions`. More importantly, the fallback semantics for `pin_l0_filter_and_index_blocks_in_cache` are not literally "L0 only": they map to `PinningTier::kFlushedAndSimilar`, which can include small ingested files and intra-L0 compaction outputs.
- **Source:** `include/rocksdb/table.h`, `PinningTier`, `MetadataCacheOptions`, and the deprecated flag comments; `table/block_based/block_based_table_reader.cc`, pinning logic in the `is_pinned` lambda
- **Fix:** Rewrite pinning around `metadata_cache_options` and clearly mark the two legacy bools as deprecated fallback behavior.

### [WRONG] The tiered-cache doc says disabling compressed secondary cache cannot be undone
- **File:** `docs/components/cache/06_tiered_cache.md`, section `Dynamic Updates`
- **Claim:** "once `compressed_secondary_ratio` is set to 0.0 (disabling secondary cache), it cannot be dynamically re-enabled."
- **Reality:** current code and tests re-enable it successfully, both with and without distributed cache reservations.
- **Source:** `cache/secondary_cache_adapter.cc`, method `CacheWithSecondaryAdapter::UpdateCacheReservationRatio`; `cache/compressed_secondary_cache_test.cc`, tests `CompressedSecCacheTestWithTiered.DynamicUpdate` and `CompressedSecCacheTestWithTiered.DynamicUpdateWithReservation`
- **Fix:** Remove this limitation from the docs, or narrow it to a specific still-broken case if one exists.

### [WRONG] Admission-policy docs attribute built-in compressed-secondary-cache behavior to `TieredAdmissionPolicy` itself
- **File:** `docs/components/cache/04_secondary_cache.md`, section `Admission Policies`
- **Claim:** "`kAdmPolicyPlaceholder` | Insert placeholder on first eviction; full entry on second hit" and "`kAdmPolicyAllowCacheHits` | Same as Placeholder, plus force-insert primary hits into secondary"
- **Reality:** `CacheWithSecondaryAdapter` only decides whether an eviction calls secondary `Insert()` with `force=false` or `force=true`. The first-eviction dummy / second-hit admission behavior is implemented by the built-in `CompressedSecondaryCache`, not by `TieredAdmissionPolicy` generically. A custom `SecondaryCache` can implement completely different admission semantics.
- **Source:** `cache/secondary_cache_adapter.cc`, method `CacheWithSecondaryAdapter::EvictionHandler`; `cache/compressed_secondary_cache.cc`, methods `MaybeInsertDummy`, `Insert`, and `Lookup`
- **Fix:** Split the documentation into adapter policy semantics vs. `CompressedSecondaryCache` admission semantics. Do not describe placeholder/second-hit behavior as a generic property of `TieredAdmissionPolicy`.

### [MISLEADING] `kAdmPolicyAllowCacheHits` does not insert on every primary-cache hit
- **File:** `docs/components/cache/04_secondary_cache.md`, section `Admission Policies`
- **Claim:** "`kAdmPolicyAllowCacheHits` | Same as Placeholder, plus force-insert primary hits into secondary"
- **Reality:** the force path is only used during eviction, when the evicted primary entry's hit bit is set. Primary lookup hits do not immediately insert into secondary cache.
- **Source:** `cache/secondary_cache_adapter.cc`, method `CacheWithSecondaryAdapter::EvictionHandler`
- **Fix:** Reword this as "on eviction, force secondary insertion if the primary entry had been hit."

### [WRONG] The stats-collector refresh window does not match the current implementation
- **File:** `docs/components/cache/09_monitoring.md`, section `CacheEntryStatsCollector`
- **Claim:** "default: minimum 300 seconds or 100x the last scan duration, whichever is longer"
- **Reality:** `CacheEntryStatsCollector` itself has no built-in default window. `InternalStats::CollectCacheEntryStats()` currently uses 10s/10x for foreground collection and 180s/500x for background collection.
- **Source:** `cache/cache_entry_stats.h`, method `CacheEntryStatsCollector::CollectStats`; `db/internal_stats.cc`, method `InternalStats::CollectCacheEntryStats`
- **Fix:** Document the foreground/background thresholds actually used by `InternalStats`, or say the collector window is caller-controlled instead of claiming a fixed default.

### [MISLEADING] `ApplyToAllEntries()` batching is described more loosely than the code actually behaves
- **File:** `docs/components/cache/09_monitoring.md`, section `CacheEntryStatsCollector`
- **Claim:** "`ApplyToAllEntries()` ... rotates between shards in small batches to minimize impact on concurrent cache operations."
- **Reality:** the current `ShardedCache::ApplyToAllEntries()` implementation clamps `average_entries_per_lock` to 1, so iteration is effectively one entry per lock acquisition, not an adjustable multi-entry batch.
- **Source:** `cache/sharded_cache.h`, method `ShardedCache::ApplyToAllEntries`
- **Fix:** Describe the current one-entry-per-lock behavior, or avoid implying that the batch size is materially larger/configurable today.

### [MISLEADING] LRU eviction is not strict global age order across priority tiers
- **File:** `docs/components/cache/02_lru_cache.md`, section `Eviction`
- **Claim:** "Entries are evicted in strict age order across all priority tiers"
- **Reality:** the three-pool midpoint insertion and spill logic intentionally overrides pure global age ordering. Lower-priority entries can be evicted before older higher-priority entries.
- **Source:** `cache/lru_cache.cc`, methods `LRUCacheShard::LRU_Insert`, `LRUCacheShard::MaintainPoolSize`, and `LRUCacheShard::EvictFromLRU`
- **Fix:** Say eviction starts from the oldest bottom-priority entries and that pool membership changes retention relative to strict global LRU order.

### [MISLEADING] Key stability is stated as a cache-wide invariant instead of an SST-block-key property
- **File:** `docs/components/cache/index.md`, section `Key Invariants`
- **Claim:** "Cache keys are globally unique with very high probability, stable across open/close and backup/restore"
- **Reality:** that stability claim only applies to SST-derived `OffsetableCacheKey` values. `CacheKey::CreateUniqueForCacheLifetime()` and `CreateUniqueForProcessLifetime()` are deliberately ephemeral and not restart-stable.
- **Source:** `cache/cache_key.h`, `CacheKey` and `OffsetableCacheKey`; `cache/cache_key.cc`
- **Fix:** Narrow this invariant to SST block-cache keys derived from persisted file identity.

### [MISLEADING] The block-cache type table omits actual cached types and implies meta-index blocks are cached
- **File:** `docs/components/cache/08_block_cache_keys.md`, section `Block Type Wrappers`
- **Claim:** the table lists `Block_kMetaIndex` as a normal block-cache type and omits full filters / compression dictionaries.
- **Reality:** the helper tables currently cache `ParsedFullFilterBlock` and `DecompressorDict`; `kMetaIndex` is explicitly `nullptr` in the helper arrays because meta-index blocks are not yet stored in block cache.
- **Source:** `table/block_based/block_cache.cc`, helper arrays `kCacheItemFullHelperForBlockType` and `kCacheItemBasicHelperForBlockType`; `table/block_based/parsed_full_filter_block.h`; `util/compression.h`, struct `DecompressorDict`
- **Fix:** Replace the table with the actual cached blocklike types and explicitly note that meta-index blocks are not currently cached.

### [UNVERIFIABLE] Operational tuning numbers are presented as if they were code-backed facts
- **File:** `docs/components/cache/10_configuration.md`, sections `Block Cache`, `Tiered Cache Sizing`, `NUMA Effects on LRU Contention`, and `JemallocNodumpAllocator`
- **Claim:** examples include "A good starting point is 1/3 of available memory", "Typical values: 0.1 to 0.4", the NUMA CPU/IPC breakdown, and "reduce compressed core dump sizes by 5-8x"
- **Reality:** the code supports the mechanisms, but not those specific quantitative recommendations. For example, `JemallocNodumpAllocator` does use `MADV_DONTDUMP`, but nothing in the current code or tests substantiates the quoted reduction factor.
- **Source:** `include/rocksdb/memory_allocator.h`; `memory/jemalloc_nodump_allocator.cc`
- **Fix:** either remove the quantitative claims, label them explicitly as anecdotal/operator guidance, or cite the benchmark/source they come from.

### [MISLEADING] The tracing section overstates coverage and points at the wrong SimCache header
- **File:** `docs/components/cache/09_monitoring.md`, sections `Block Cache Tracing` and `Simulation Cache`
- **Claim:** "records every cache access" and "`SimCache` (see `utilities/simulator_cache/sim_cache.h`)..."
- **Reality:** block-cache tracing can be sampled through `BlockCacheTraceOptions::sampling_frequency`, so not every access is necessarily recorded. Also, the public SimCache header is `include/rocksdb/utilities/sim_cache.h`.
- **Source:** `include/rocksdb/block_cache_trace_writer.h`, struct `BlockCacheTraceOptions`; `trace_replay/block_cache_tracer.cc`, function `ShouldTrace`; `include/rocksdb/utilities/sim_cache.h`
- **Fix:** mention sampling explicitly and correct the SimCache header path.

## Completeness Gaps

### `lowest_used_cache_tier` is missing from the cache docs
- **Why it matters:** this is the switch that decides whether secondary cache is even consulted. A DB can share a cache object with a secondary tier configured and still never spill/look up secondary entries.
- **Where to look:** `include/rocksdb/options.h`, field `lowest_used_cache_tier`; `cache/typed_cache.h`, `FullTypedCacheInterface::{InsertFull,LookupFull,StartAsyncLookupFull}`; `cache/lru_cache_test.cc`, tests `TestSecondaryCacheOptionBasic` and `TestSecondaryCacheOptionChange`; `cache/tiered_secondary_cache_test.cc`, test `VolatileTierTest`
- **Suggested scope:** add a subsection to chapter 4 or 10, plus at least one sentence in the index

### `prepopulate_block_cache` is not documented
- **Why it matters:** it materially changes cache warming behavior at flush/compaction time and uses different priorities for those two paths.
- **Where to look:** `include/rocksdb/table.h`, enum `BlockBasedTableOptions::PrepopulateBlockCache`; `table/block_based/block_based_table_builder.cc`, `WarmCacheConfig::Compute`; `db/db_block_cache_test.cc`, tests `WarmCacheWithDataBlocksDuringFlush`, `WarmCacheWithDataBlocksDuringCompaction`, and `WarmCacheWithBlocksDuringFlush`
- **Suggested scope:** add to chapter 10, with a brief cross-reference from the LRU priority chapter

### `MetadataCacheOptions` / `PinningTier` need first-class documentation
- **Why it matters:** current docs tell readers to reason with deprecated fallback flags, which is the wrong control surface for current RocksDB.
- **Where to look:** `include/rocksdb/table.h`, `PinningTier` and `MetadataCacheOptions`; `table/block_based/block_based_table_reader.cc`; `db/db_block_cache_test.cc`, test `DBBlockCachePinningTest.TwoLevelDB`
- **Suggested scope:** full subsection in chapter 10

### The `NewClockCache()` compatibility trap is undocumented
- **Why it matters:** the public API name strongly suggests HyperClockCache, but today it returns a new `LRUCache` for compatibility.
- **Where to look:** `include/rocksdb/cache.h`, function `NewClockCache`; `cache/clock_cache.cc`, function `NewClockCache`
- **Suggested scope:** brief migration note in chapter 3 and chapter 10

### The monitoring chapter omits important cache-related counters
- **Why it matters:** compressed-secondary-cache behavior is impossible to reason about from the current doc's ticker list alone.
- **Where to look:** `include/rocksdb/statistics.h`, tickers `COMPRESSED_SECONDARY_CACHE_*`, `BLOCK_CACHE_*_BYTES_INSERT`, and `BLOCK_CACHE_COMPRESSION_DICT_*`
- **Suggested scope:** expand chapter 9's ticker table

### The reservation chapter understates what `kBlockBasedTableReader` charging covers
- **Why it matters:** readers could incorrectly assume only the reader object is charged, when the charged scope also includes table properties, metadata blocks held outside cache, and internal creation-time structures.
- **Where to look:** `include/rocksdb/table.h`, comments under `CacheUsageOptions` for `CacheEntryRole::kBlockBasedTableReader`; `table/block_based/block_based_table_reader_test.cc`, test `ChargeTableReaderTest.Basic`
- **Suggested scope:** extend chapter 7's consumer table and chapter 10's memory-accounting guidance

## Depth Issues

### The `kBlockCacheEntryStats` property is described with helper names instead of actual map keys
- **Current:** chapter 9 lists `CacheId`, `CacheCapacityBytes`, `EntryCount(role)`, etc.
- **Missing:** the actual strings a caller sees in the property output, such as `id`, `capacity`, `count.data-block`, and `bytes.filter-block`
- **Source:** `cache/cache_entry_roles.cc`, struct `BlockCacheEntryStatsMapKeys`

### `BlockCreateContext` is too shallow for someone debugging secondary-cache reconstruction
- **Current:** the doc lists only six fields and says `Create()` handles decompression and construction
- **Missing:** `ioptions`, `statistics`, `index_value_is_full`, and `index_has_first_key`, plus the per-block-type reconstruction differences in `BlockCreateContext::Create(...)`
- **Source:** `table/block_based/block_cache.h`, struct `BlockCreateContext`; `table/block_based/block_cache.cc`, all `BlockCreateContext::Create` overloads

### The tiered-cache docs do not explain what `GetSecondaryCachePinnedUsage()` actually returns
- **Current:** the docs mention dynamic reservation distribution but not how the exposed "pinned usage" number is derived
- **Missing:** in tiered mode the value is reservation-based (`secondary capacity - primary reservation`), not an actual scan of secondary-cache pinned entries
- **Source:** `cache/secondary_cache_adapter.cc`, method `CacheWithSecondaryAdapter::GetSecondaryCachePinnedUsage`

## Structure and Style Violations

### Inline code quotes are used throughout all chapters
- **File:** all files under `docs/components/cache/`
- **Details:** the docs rely heavily on inline backticked identifiers and option names even though the style requirements for this doc set explicitly say not to use inline code quotes.

### `Key Invariants` contains non-invariants
- **File:** `docs/components/cache/index.md`
- **Details:** "Cache keys are globally unique with very high probability" and "With strict_capacity_limit=false inserts always succeed" are useful facts, but they are not correctness invariants in the "corruption/crash if violated" sense requested by the style guide.

## Undocumented Complexity

### Secondary-cache enablement is controlled per DB, not just per cache object
- **What it is:** `lowest_used_cache_tier` switches block-cache helpers between basic and full helper sets. That is what actually enables or disables secondary spill/lookup on block-based-table code paths.
- **Why it matters:** it is the first thing to check when a secondary cache is configured but appears completely idle.
- **Key source:** `include/rocksdb/options.h`, field `lowest_used_cache_tier`; `cache/typed_cache.h`, `FullTypedCacheInterface` helper selection
- **Suggested placement:** chapter 4 and chapter 10

### Fixed HyperClockCache capacity changes do not resize the table
- **What it is:** `SetCapacity()` changes the charge target, but the fixed-table variant keeps the original occupancy structure. If `estimated_entry_charge` was wrong, only recreating the cache fixes the table shape.
- **Why it matters:** otherwise users can keep tuning capacity and never fix the actual lookup/occupancy problem.
- **Key source:** `cache/clock_cache.h`, `FixedHyperClockTable`; `cache/clock_cache.cc`, `BaseClockTable::SetCapacity` and `FixedHyperClockCache::ReportProblems`
- **Suggested placement:** chapter 3 and chapter 10

### HyperClockCache pinned-usage reporting is intentionally slow
- **What it is:** `GetPinnedUsage()` scans the whole table instead of maintaining an exact counter on the hot path.
- **Why it matters:** this is important for anyone wiring block-cache properties or monitoring into a tight polling loop.
- **Key source:** `cache/clock_cache.cc`, method `ClockCacheShard<Table>::GetPinnedUsage`
- **Suggested placement:** chapter 3 or chapter 9

### Flush warming and compaction warming use different priorities
- **What it is:** `prepopulate_block_cache = kFlushAndCompaction` warms flush outputs at `LOW` priority and compaction outputs at `BOTTOM` priority.
- **Why it matters:** it directly affects cache pollution and explains why compaction-warmed blocks are more disposable than flush-warmed blocks.
- **Key source:** `include/rocksdb/table.h`, enum `PrepopulateBlockCache`; `table/block_based/block_based_table_builder.cc`, `WarmCacheConfig::Compute`
- **Suggested placement:** chapter 2 and chapter 10

### Tiered placeholder reservation distribution is chunked and therefore laggy
- **What it is:** placeholder reservations are redistributed between primary and secondary caches in 1MB chunks, not continuously.
- **Why it matters:** users comparing usage numbers across tiers will otherwise think the accounting is "wrong" when it is just intentionally batched.
- **Key source:** `cache/secondary_cache_adapter.h`, `kReservationChunkSize`; `cache/secondary_cache_adapter.cc`, `CacheWithSecondaryAdapter::Insert` and `Release`
- **Suggested placement:** chapter 6 or chapter 7

## Positive Notes

- The index file is within the requested size range and the chapter decomposition is sensible.
- The docs consistently point readers at the right core files for LRU, HyperClockCache, and the adapter layers.
- The tiered-cache chapter correctly explains the resize ordering that avoids temporary over-budget spikes.
