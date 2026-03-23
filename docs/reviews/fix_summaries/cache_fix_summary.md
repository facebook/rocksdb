# Fix Summary: cache

## Issues Fixed

| Category | Count |
|----------|-------|
| Correctness | 10 |
| Completeness | 12 |
| Misleading | 8 |
| Structure/Style | 2 |
| **Total** | **32** |

## Disagreements Found

4 disagreements documented in `cache_debates.md`:
1. **Tiered cache re-enablement**: Codex claimed re-enable works; code proves doc is correct (Codex wrong)
2. **Inline code quotes scope**: CC flagged only fenced code block; Codex flagged all backticks (CC interpretation adopted)
3. **LRU eviction order**: CC didn't flag; Codex correctly identified misleading "strict age order" claim (Codex correct)
4. **CacheEntryStatsCollector defaults**: Both agree on fix (interface example vs actual implementation values)

## Changes Made

### index.md
- Narrowed key stability invariant to SST block cache keys; noted ephemeral keys are not restart-stable
- Added cross-reference to `eviction_effort_cap` for strict_capacity_limit behavior

### 01_cache_interface.md
- Added missing `kDeprecatedFilterBlock` to CacheEntryRole table
- Added cross-reference to `eviction_effort_cap` for capacity behavior

### 02_lru_cache.md
- Rewrote eviction description: replaced "strict age order across all priority tiers" with accurate pool-based retention explanation
- Clarified "highest available" insertion placement depends on non-zero pool ratios
- Changed `use_adaptive_mutex` default from "platform-dependent" to "build-config-dependent" with compile flag detail

### 03_hyper_clock_cache.md
- Distinguished eviction batch units between Fixed (individual slots) and Auto (chain homes) variants
- Added note that FixedHyperClockTable `SetCapacity` does not resize the hash table

### 04_secondary_cache.md
- Added `SupportForceErase()` to SecondaryCache method table
- Rewrote admission policies to separate adapter semantics (`force_insert` true/false) from CompressedSecondaryCache-specific behavior (two-hit protocol)
- Added `SecondaryCacheWrapper` delegation class section

### 05_compressed_secondary_cache.md
- Updated compression description to reference V2 framework via `CompressionManager` / `GetBuiltinV2CompressionManager()`

### 06_tiered_cache.md
- Clarified capacity math: primary gets `total_capacity`, reservation of `total_capacity * ratio` taken via dummy entries, leaving effective primary capacity at `total_capacity * (1 - ratio)`

### 08_block_cache_keys.md
- Removed fenced code block (style violation); replaced with prose description
- Narrowed key stability claim to SST-derived keys; noted ephemeral keys are not restart-stable
- Strengthened uniqueness description: guaranteed in single process, probabilistic cross-process
- Fixed block type table: removed `Block_kMetaIndex` (not cached), added `ParsedFullFilterBlock` and `DecompressorDict`

### 09_monitoring.md
- Fixed CacheEntryStatsCollector timing: replaced 300s/100x with actual bg 180s/500x, fg 10s/10x
- Fixed `ApplyToAllEntries` description: one entry per lock acquisition (clamped to 1)
- Fixed block cache tracing: changed "records every cache access" to "records cache accesses"; added `sampling_frequency` note
- Fixed SimCache header path from `utilities/simulator_cache/sim_cache.h` to `include/rocksdb/utilities/sim_cache.h`
- Added compressed secondary cache tickers (DUMMY_HITS, HITS, PROMOTIONS, PROMOTION_SKIPS)

### 10_configuration.md
- Removed `fill_cache` from BlockBasedTableOptions table (it's a ReadOptions field); added note clarifying this
- Updated `block_cache_compressed` to note the field was removed from the struct (not just deprecated)
- Rewrote pinning options: marked `pin_l0_filter_and_index_blocks_in_cache` and `pin_top_level_index_and_filter` as DEPRECATED with mapping details; added `metadata_cache_options` as current API
- Added `prepopulate_block_cache` option with priority details for flush vs compaction warming
- Added DB-level cache options section with `lowest_used_cache_tier` and `uncache_aggressiveness`
- Added migration note for deprecated `NewClockCache()` (returns LRUCache)
- Updated pinning guidance section to reference `MetadataCacheOptions` / `PinningTier`
- Labeled unverifiable operational numbers (1/3 memory, 0.1-0.4 ratio, 5-8x dump reduction) as guidance
