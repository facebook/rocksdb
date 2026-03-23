# Fix Summary: monitoring

## Issues Fixed

**Correctness (6):**
1. PerfLevel count: "Seven levels" -> "Six levels" (index.md)
2. Persistent stats CF name: `__stats_history__` -> `___rocksdb_stats_history___` (09_stats_history.md)
3. EventLogger log level: removed false claim that events are independent of info_log_level; clarified they use INFO_LEVEL (07_event_logger.md)
4. ThreadType enum ordering: reordered to match actual enum (HIGH_PRIORITY, LOW_PRIORITY, USER, BOTTOM_PRIORITY) (08_thread_status.md)
5. Reset() description: simplified misleading parenthetical about core 0 (02_core_local_implementation.md)
6. Internal tickers claim: fixed incorrect `INTERNAL_TICKER_ENUM_MAX == TICKER_ENUM_MAX` to describe empty range (02_core_local_implementation.md)

**Style/Structure (2):**
1. INVARIANT misuse: split into "Key Invariants" (true correctness invariants) and "Key Guarantees" (conventions/guarantees) (index.md)
2. Bloom filter prefix: clarified Get/MultiGet scope with iterator cross-reference (01_statistics_framework.md)

**Completeness (10):**
1. Added WRITE_PRE_COMP_GB to LevelStatType table (06_compaction_stats.md)
2. Added kIntStatsWriteBufferManagerLimitStopsCounts to DB-Level Stats (06_compaction_stats.md)
3. Added OP_GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST to operation types (08_thread_status.md)
4. Added note that ticker/histogram lists are curated subsets (01_statistics_framework.md)
5. Added ~12 missing PerfContext fields grouped by category (03_perf_context.md)
6. Added PeriodicTaskScheduler 5-task-type note (09_stats_history.md)
7. Added CacheEntryRole values for block-cache-entry-stats property (05_db_properties.md)
8. Added file read sampling extrapolation detail (06_compaction_stats.md)
9. Added collapsible entry file read sampling (06_compaction_stats.md)
10. Added Customizable Statistics note (01_statistics_framework.md)

**Depth (4):**
1. Clarified IOSTATS_TIMER_GUARD does NOT check disable_iostats (04_iostats_context.md)
2. Added PERF_TIMER_FOR_WAIT_GUARD and PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD macros (03_perf_context.md)
3. Added StopWatch and StopWatchNano descriptions (10_instrumented_mutex.md)
4. Added PerfStepTimer SystemClock::Default() fallback note (10_instrumented_mutex.md)

## Disagreements Found

0 -- Only one review file existed (Codex review was missing), so no inter-reviewer disagreements to resolve.

## Changes Made

| File | Changes |
|------|---------|
| `index.md` | Fixed PerfLevel count, split invariants vs guarantees, added BLOCK_CACHE_HIT note about uncategorized types |
| `01_statistics_framework.md` | Clarified prefix filter scope, added non-exhaustive list note, added Customizable Statistics |
| `02_core_local_implementation.md` | Fixed Reset() description, fixed internal tickers claim |
| `03_perf_context.md` | Added 2 missing macros, added ~12 missing PerfContext fields |
| `04_iostats_context.md` | Clarified IOSTATS_TIMER_GUARD vs IOSTATS_ADD disable_iostats behavior |
| `05_db_properties.md` | Added CacheEntryRole values for block-cache-entry-stats |
| `06_compaction_stats.md` | Added WRITE_PRE_COMP_GB, WBM stop counts, file read sampling extrapolation, collapsible entry sampling |
| `07_event_logger.md` | Fixed EventLogger log level claim |
| `08_thread_status.md` | Fixed ThreadType enum ordering, added OP_GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST |
| `09_stats_history.md` | Fixed CF name, added PeriodicTaskScheduler task types |
| `10_instrumented_mutex.md` | Added StopWatch/StopWatchNano, PerfStepTimer clock fallback note |
