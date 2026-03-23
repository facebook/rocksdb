# Review: monitoring -- Claude Code

## Summary
Overall quality rating: **good**

The monitoring documentation covers a broad surface area well, with 11 chapters spanning statistics, PerfContext, IOStatsContext, DB properties, compaction stats, event logging, thread status, stats history, instrumented mutex, and best practices. The architecture is clearly explained and the structure is logical. However, there are several factual errors -- most notably an incorrect persistent stats column family name, a wrong claim about EventLogger log level independence, a wrong PerfLevel count, and a misordered ThreadType enum. The documentation also has completeness gaps around recently added metrics and some enum values.

## Correctness Issues

### [STALE] BLOCK_CACHE_HIT invariant comment in source is stale
- **File:** `index.md` line 40, `01_statistics_framework.md` "Important" note
- **Claim:** "BLOCK_CACHE_HIT equals the sum of BLOCK_CACHE_INDEX_HIT + BLOCK_CACHE_FILTER_HIT + BLOCK_CACHE_DATA_HIT + BLOCK_CACHE_COMPRESSION_DICT_HIT"
- **Reality:** The documentation is actually CORRECT. The source code comment at `include/rocksdb/statistics.h` is stale -- it says `BLOCK_CACHE_HIT == INDEX_HIT + FILTER_HIT + DATA_HIT` without COMPRESSION_DICT_HIT, but `UpdateCacheHitMetrics()` in `block_based_table_reader.cc` increments BLOCK_CACHE_HIT for ALL block types including compression dict. The doc correctly includes COMPRESSION_DICT_HIT; the source comment needs updating.
- **Source:** `table/block_based/block_based_table_reader.cc`, `UpdateCacheHitMetrics()` -- BLOCK_CACHE_HIT is incremented at line 280 before the block-type switch, so it includes all types.
- **Fix:** No doc fix needed. The source comment in `statistics.h` should be updated to include COMPRESSION_DICT_HIT. One subtlety the doc could note: `BlockType::kRangeDeletion` and other uncategorized block types fall into the `default` case in `UpdateCacheHitMetrics()` and are counted as DATA_HIT.

### [WRONG] PerfLevel count in index.md
- **File:** `index.md` line 29
- **Claim:** "Seven levels from kDisable to kEnableTime"
- **Reality:** There are 6 levels from kDisable(1) to kEnableTime(6). kUninitialized(0) and kOutOfBounds(7) are sentinel values, not usable levels.
- **Source:** `include/rocksdb/perf_level.h`, PerfLevel enum
- **Fix:** Change "Seven levels" to "Six levels"

### [WRONG] Persistent stats column family name
- **File:** `09_stats_history.md`, Persistent Storage section
- **Claim:** "RocksDB creates an internal column family (`__stats_history__`)"
- **Reality:** The actual name is `___rocksdb_stats_history___` (triple underscores, with "rocksdb_" prefix)
- **Source:** `db/db_impl/db_impl.cc`, `kPersistentStatsColumnFamilyName` definition
- **Fix:** Replace `__stats_history__` with `___rocksdb_stats_history___`

### [WRONG] EventLogger log level independence
- **File:** `07_event_logger.md`, final Note
- **Claim:** "EventLogger output is independent of DBOptions::info_log_level. Events are always logged regardless of the configured log level."
- **Reality:** `EventLogger::Log()` calls `ROCKSDB_NAMESPACE::Log(logger, ...)` which internally calls `Logger::Logv(InfoLogLevel::INFO_LEVEL, ...)`. The `Logger::Logv(log_level, ...)` implementation checks `if (log_level < log_level_) return;`. If `info_log_level` is set to WARN_LEVEL or higher, EventLogger events are filtered out.
- **Source:** `logging/event_logger.cc` line 53, `env/env.cc` lines 919-931
- **Fix:** Remove or correct this claim. EventLogger uses INFO_LEVEL and is subject to `info_log_level` filtering.

### [WRONG] ThreadType enum ordering
- **File:** `08_thread_status.md`, Thread types section
- **Claim:** Lists thread types as: HIGH_PRIORITY, LOW_PRIORITY, BOTTOM_PRIORITY, USER
- **Reality:** The actual enum order is: HIGH_PRIORITY(0), LOW_PRIORITY(1), USER(2), BOTTOM_PRIORITY(3)
- **Source:** `include/rocksdb/thread_status.h`, ThreadType enum
- **Fix:** Reorder to match the actual enum: HIGH_PRIORITY, LOW_PRIORITY, USER, BOTTOM_PRIORITY

### [MISLEADING] Reset() description
- **File:** `02_core_local_implementation.md`, Reset Semantics section
- **Claim:** "Sets all tickers to zero by clearing each core's ticker array (core 0 gets the value, others get 0)"
- **Reality:** For `Reset()`, the value passed to `setTickerCountLocked` is 0, so all cores get 0. The parenthetical "(core 0 gets the value, others get 0)" describes the general `setTickerCountLocked` mechanism but is misleading in the context of Reset() since the value is always 0.
- **Source:** `monitoring/statistics.cc`, `Reset()` and `setTickerCountLocked()` implementations
- **Fix:** Simplify to: "Sets all tickers to zero across all cores" or describe the setTickerCountLocked mechanism separately from Reset.

### [MISLEADING] Bloom filter prefix stats description
- **File:** `01_statistics_framework.md`, Bloom Filter section
- **Claim:** "BLOOM_FILTER_PREFIX_CHECKED / BLOOM_FILTER_PREFIX_USEFUL / BLOOM_FILTER_PREFIX_TRUE_POSITIVE -- prefix filter stats for point lookups"
- **Reality:** Per the source comment, these are "Prefix filter stats when used for point lookups (Get / MultiGet)" and explicitly notes "(For prefix filter stats on iterators, see *_LEVEL_SEEK_*)". The doc's shorthand is correct but could mislead someone into thinking these cover iterator prefix filter usage too.
- **Source:** `include/rocksdb/statistics.h`, BLOOM_FILTER_PREFIX_CHECKED comment
- **Fix:** Add "(Get/MultiGet only; for iterator prefix filter stats, see separate _LEVEL_SEEK_ tickers)"

### [WRONG] Internal tickers claim INTERNAL_TICKER_ENUM_MAX == TICKER_ENUM_MAX
- **File:** `02_core_local_implementation.md`, Internal Tickers and Histograms section
- **Claim:** "Currently both ranges are empty (INTERNAL_TICKER_ENUM_MAX == TICKER_ENUM_MAX)"
- **Reality:** Due to how C++ enums work, `INTERNAL_TICKER_ENUM_START = TICKER_ENUM_MAX` and `INTERNAL_TICKER_ENUM_MAX` is the next enumerator, so `INTERNAL_TICKER_ENUM_MAX == TICKER_ENUM_MAX + 1`. The ranges are indeed empty (no actual internal-only tickers), but the numeric values differ by 1. Same for histograms.
- **Source:** `monitoring/statistics_impl.h`, TickersInternal and HistogramsInternal enums
- **Fix:** Change to "Currently both ranges are empty (no internal-only tickers or histograms defined between INTERNAL_*_START and INTERNAL_*_MAX)"

## Completeness Gaps

### Missing LevelStatType: WRITE_PRE_COMP_GB
- **Why it matters:** Anyone parsing compaction stats output will see this column but the docs don't explain it
- **Where to look:** `db/internal_stats.h`, `LevelStatType` enum -- `WRITE_PRE_COMP_GB` is between `WRITE_GB` and `W_NEW_GB`
- **Suggested scope:** Add one row to the table in `06_compaction_stats.md`

### Missing InternalDBStatsType: kIntStatsWriteBufferManagerLimitStopsCounts
- **Why it matters:** WriteBufferManager-triggered write stops are tracked but undocumented
- **Where to look:** `db/internal_stats.h`, `InternalDBStatsType` enum
- **Suggested scope:** Add one bullet to the DB-Level Stats section in `06_compaction_stats.md`

### Missing ThreadStatus operation type: OP_GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST
- **Why it matters:** Thread status monitoring users won't know about this operation type
- **Where to look:** `include/rocksdb/thread_status.h`, OperationType enum
- **Suggested scope:** Add to the operation types list in `08_thread_status.md`

### Missing recently added histograms
- **Why it matters:** New monitoring capabilities are undocumented
- **Where to look:** `include/rocksdb/statistics.h`, Histograms enum -- notably MULTISCAN_PREPARE_ITERATORS, MULTISCAN_PREPARE_MICROS, MULTISCAN_BLOCKS_PER_PREPARE, BLOCK_KEY_DISTRIBUTION_CV, NUM_OP_PER_TRANSACTION
- **Suggested scope:** Mention in `01_statistics_framework.md` or note that the histogram list is non-exhaustive

### Many recently added tickers not documented
- **Why it matters:** ~25+ tickers exist in the current enum that are not mentioned anywhere in the docs, spanning multiple feature areas
- **Where to look:** `include/rocksdb/statistics.h` -- notably: MULTISCAN_PREPARE_CALLS through MULTISCAN_SEEK_ERRORS (9 tickers, lines 559-576), PREFETCH_MEMORY_BYTES_GRANTED/RELEASED/REQUESTS_BLOCKED (lines 580-584), SST_FOOTER_CORRUPTION_COUNT (line 541), FILE_READ_CORRUPTION_RETRY_COUNT/SUCCESS_COUNT (lines 545-546), FIFO_MAX_SIZE_COMPACTIONS/FIFO_TTL_COMPACTIONS/FIFO_CHANGE_TEMPERATURE_COMPACTIONS (lines 526-528), PREFETCH_BYTES/PREFETCH_BYTES_USEFUL/PREFETCH_HITS (lines 531-538), READAHEAD_TRIMMED (line 523), COMPACTION_ABORTED (line 166), NUMBER_WBWI_INGEST (line 552), SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT (line 555)
- **Suggested scope:** The doc should either enumerate key new tickers by category or explicitly note the list is a curated subset

### Many PerfContext fields not documented
- **Why it matters:** ~12 PerfContext fields in the current header are not mentioned in chapter 03
- **Where to look:** `include/rocksdb/perf_context.h` -- notably: file_ingestion_nanos, file_ingestion_blocking_live_writes_nanos (lines 291-293), per-block-category read bytes (data_block_read_byte, index_block_read_byte, filter_block_read_byte, compression_dict_block_read_byte, metadata_block_read_byte at lines 296-300), block_cache_standalone_handle_count/block_cache_real_handle_count (lines 83-86), internal_merge_point_lookup_count (line 159), internal_range_del_reseek_count (line 163), iter_next_count/iter_prev_count/iter_seek_count (lines 277-279, marked EXPERIMENTAL)
- **Suggested scope:** Add to chapter 03 Key Metrics section, grouped by category

### Three of five PeriodicTaskTypes undocumented
- **Why it matters:** The docs mention PeriodicTaskScheduler only for stats dump and stats persist, but three other task types exist: kFlushInfoLog (always registered, cannot be disabled), kRecordSeqnoTime, and kTriggerCompaction
- **Where to look:** `db/periodic_task_scheduler.h`, PeriodicTaskType enum
- **Suggested scope:** Brief mention in chapter 06 or chapter 09 that PeriodicTaskScheduler manages 5 task types total

### CacheEntryRole values not listed for block-cache-entry-stats property
- **Why it matters:** Chapter 05 mentions the `rocksdb.block-cache-entry-stats` property but does not list the 14 cache entry roles (kDataBlock, kFilterBlock, kIndexBlock, kWriteBuffer, kBlobValue, etc.) or the BlockCacheEntryStatsMapKeys used to access the map property
- **Where to look:** `include/rocksdb/cache.h` lines 55-110, CacheEntryRole enum and BlockCacheEntryStatsMapKeys
- **Suggested scope:** Add a table of cache entry roles and map keys to chapter 05

### Missing file read sampling extrapolation detail
- **Why it matters:** The sampled count is not raw -- `sample_file_read_inc()` adds `kFileReadSampleRate` (1024) to extrapolate back to estimated total reads. Someone reading the docs might think the stored count is 1 per sample.
- **Where to look:** `monitoring/file_read_sample.h`, `sample_file_read_inc()` function
- **Suggested scope:** Add one sentence to `06_compaction_stats.md` File Read Sampling section

### Missing InternalCFStatsType beyond write stalls
- **Why it matters:** `InternalCFStatsType` includes BYTES_FLUSHED, BYTES_INGESTED_ADD_FILE, INGESTED_NUM_FILES_TOTAL, etc. beyond just write stall counters
- **Where to look:** `db/internal_stats.h`, InternalCFStatsType enum
- **Suggested scope:** Brief mention in `06_compaction_stats.md` that CF stats track more than just write stalls

## Depth Issues

### IOSTATS_TIMER_GUARD does not check disable_iostats
- **Current:** Chapter 04 says IOSTATS_ADD "checks disable_iostats" and lists IOSTATS_TIMER_GUARD as another recording macro, potentially implying both check it
- **Missing:** IOSTATS_TIMER_GUARD does NOT check `disable_iostats`. It creates a PerfStepTimer that checks PerfLevel only. Only IOSTATS_ADD checks `disable_iostats`. This distinction matters for understanding when I/O timing is collected vs. counters.
- **Source:** `monitoring/iostats_context_imp.h`, IOSTATS_ADD vs IOSTATS_TIMER_GUARD macro definitions

### HistogramImpl internal mutex not explained
- **Current:** Chapter 02 describes core-local histograms using HistogramImpl but doesn't explain that HistogramImpl wraps HistogramStat with an additional mutex
- **Missing:** HistogramImpl has a private `std::mutex mutex_` member. The relationship between HistogramStat (lock-free atomics for individual fields) and HistogramImpl (adds mutex for Merge and toString operations) is unclear.
- **Source:** `monitoring/histogram.h`, HistogramImpl class

### PerfContext macro-driven implementation not explained
- **Current:** Chapter 03 describes PerfContext at API level
- **Missing:** PerfContext uses a sophisticated offset-based approach with DEF_PERF_CONTEXT_METRICS macros and compile-time static_assert validation to ensure the macro-generated internal struct matches the public header struct. This design is non-obvious and important for anyone adding new PerfContext metrics.
- **Source:** `monitoring/perf_context.cc`, DEF_PERF_CONTEXT_METRICS macro and offset assertions

## Structure and Style Violations

### index.md: Line count is 42 (acceptable, within 40-80 range)

### Missing chapter numbering gap
- **File:** `docs/components/monitoring/`
- **Details:** Chapters jump from 02 to 03 etc. sequentially through 11 -- no gaps. Structure is clean.

### INVARIANT misuse in index.md
- **File:** `index.md`, Key Invariants section
- **Details:** "Ticker name map ordering must match the enum definition ordering" -- this is a code maintenance convention (build will fail if violated due to TickersNameMap assertions), not a true correctness invariant that causes data corruption or crashes. Similarly, "PerfContext and IOStatsContext get_* functions never return nullptr" is a guarantee, not an invariant whose violation causes corruption. Consider relabeling these as "Key Guarantees" or only keeping the BLOCK_CACHE_HIT one as an invariant (after fixing it).

## Undocumented Complexity

### COERCE_CONTEXT_SWITCH compile flag in InstrumentedMutex
- **What it is:** When `COERCE_CONTEXT_SWITCH` is defined, InstrumentedMutex gains an additional constructor that accepts an `InstrumentedCondVar*` (bg_cv_). This is used for testing or debugging lock behavior by forcing context switches.
- **Why it matters:** Developers debugging lock contention or writing tests may need to know about this flag.
- **Key source:** `monitoring/instrumented_mutex.h`, `#ifdef COERCE_CONTEXT_SWITCH` sections
- **Suggested placement:** Brief mention in chapter 10

### sample_collapsible_entry_file_read_inc
- **What it is:** A separate file read sampling function for "collapsible entry" reads, tracking `num_collapsible_entry_reads_sampled` in `FileMetaData::stats`. This is distinct from the general file read sampling.
- **Why it matters:** This is a separate dimension of file read tracking that affects compaction decisions differently.
- **Key source:** `monitoring/file_read_sample.h`, `sample_collapsible_entry_file_read_inc()` function
- **Suggested placement:** Add to chapter 06 File Read Sampling section

### StopWatch utility class
- **What it is:** `util/stop_watch.h` is listed in chapter 10's Files line but never described. StopWatch provides simpler timing utilities (start/stop/elapsed) compared to PerfStepTimer, and is used in various places in the codebase for timing that feeds into Statistics tickers.
- **Why it matters:** Understanding when StopWatch vs PerfStepTimer is used helps trace where timing overhead comes from.
- **Key source:** `util/stop_watch.h`
- **Suggested placement:** Brief description in chapter 10 or chapter 11

### PERF_TIMER_FOR_WAIT_GUARD and PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD macros
- **What it is:** Chapter 03 documents PERF_TIMER_GUARD, PERF_CPU_TIMER_GUARD, PERF_COUNTER_ADD, and PERF_COUNTER_BY_LEVEL_ADD, but omits two important macros: PERF_TIMER_FOR_WAIT_GUARD (uses kEnableWait level) and PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD (uses kEnableTime level, conditional start). These macros map directly to the PerfLevel hierarchy and are used for write_memtable_time and db_mutex_lock_nanos respectively.
- **Why it matters:** Without documenting these macros, the connection between PerfLevel tiers and specific metrics is incomplete.
- **Key source:** `monitoring/perf_context_imp.h`, PERF_TIMER_FOR_WAIT_GUARD and PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD macro definitions
- **Suggested placement:** Add to chapter 03 Implementation/Macros section

### StopWatch and StopWatchNano utility classes
- **What it is:** `util/stop_watch.h` is listed in chapter 10's Files line but never described. It contains two classes:
  - `StopWatch`: Auto-scoped timer that records elapsed time to up to TWO histogram types simultaneously. Constructor takes SystemClock*, Statistics*, hist_type_1, optional hist_type_2. Supports `DelayStart()`/`DelayStop()`/`GetDelay()` for subtracting delay periods from elapsed time. Checks StatsLevel and HistEnabledForType at construction.
  - `StopWatchNano`: Template class parameterized on `use_cpu_time`. Provides nanosecond-precision timing using either `NowNanos()` or `CPUNanos()`. Not auto-scoped (manual start/read). Used when only the elapsed time value is needed without histogram recording.
- **Why it matters:** Both classes are central to how timing measurements feed into Statistics histograms throughout the codebase. Understanding StopWatch vs PerfStepTimer is essential for tracing timing overhead sources.
- **Key source:** `util/stop_watch.h`
- **Suggested placement:** Brief description in chapter 10 or chapter 11

### PerfStepTimer implicit fallback to SystemClock::Default()
- **What it is:** When PerfStepTimer is constructed with `clock = nullptr` but either PerfLevel or Statistics are enabled, it silently falls back to `SystemClock::Default().get()`. This means PerfContext time metrics may use the system's real clock even if a custom SystemClock is configured for the DB.
- **Why it matters:** Users with custom SystemClock (e.g., simulated time for testing) may get inconsistent timing between DB operations (using custom clock) and PerfContext metrics (using default clock) if the calling code doesn't propagate the custom clock.
- **Key source:** `monitoring/perf_step_timer.h`, constructor lines 16-27
- **Suggested placement:** Note in chapter 03 or chapter 11

### Statistics class is Customizable
- **What it is:** Statistics extends Customizable, allowing custom Statistics implementations to be loaded via string-based factory (`CreateFromString`). This is not mentioned in the docs.
- **Why it matters:** Users can provide custom Statistics implementations that are loaded by name, useful for plugin architectures.
- **Key source:** `include/rocksdb/statistics.h`, Statistics class declaration
- **Suggested placement:** Brief mention in chapter 01

## Positive Notes

- The three-scope organization (global Statistics, per-thread PerfContext/IOStatsContext, point-in-time DB Properties) is clearly explained and provides an excellent mental model.
- Chapter 02 (Core-Local Implementation) does a good job explaining the hot-path/cold-path tradeoff of lock-free recording vs lock-requiring aggregation.
- The StatsLevel and PerfLevel tables in chapters 01 and 03 are well-structured and easy to reference.
- Chapter 11 (Performance and Best Practices) provides practical guidance with a useful overhead comparison table and monitoring decision tree.
- File references are correct and consistently provided per chapter.
- The compile-time disabling documentation (NPERF_CONTEXT, NIOSTATS_CONTEXT, NROCKSDB_THREAD_STATUS) is thorough.
- The IOStatsContext temperature-based tracking documentation accurately reflects the FileIOByTemperature struct.
- Write stall tracking documentation in chapter 06 is detailed and matches the InternalCFStatsType enum well.
