# Review: read_flow -- Codex

## Summary
Overall quality rating: **needs work**

The chapter structure is good, the index is within the requested length, and several of the algorithm-heavy chapters are useful. In particular, the overall breakdown of point lookup, MultiGet, iterator, range deletion, merge resolution, and prefetching is the right shape for the codebase.

The biggest problems are option-semantics errors and stale read-path descriptions. The most serious ones are `kPersistedTier`, `auto_refresh_iterator_with_snapshot`, and metadata-cache behavior for partitioned index/filter blocks. There are also important completeness gaps: the docs do not cover row cache or MultiScan at all, and they omit timestamp history-collapse failures that can reject reads before lookup even starts.

## Correctness Issues

### [WRONG] `kPersistedTier` is documented as a general "persisted-only" read mode
- **File:** `06_block_cache.md`, "ReadTier Option"; `11_read_options_and_tuning.md`, "Options for Point Lookups and Scans"
- **Claim:** "`kPersistedTier` | Skip memtables, read only from persisted data" and "`read_tier` ... controls which tiers are accessible: cache only (`kBlockCacheTier`), all tiers, or persisted only."
- **Reality:** `ReadTier::kPersistedTier` currently supports only `Get` and `MultiGet`; iterator creation returns `Status::NotSupported()`. For point lookups and MultiGet, memtables are skipped only when `has_unpersisted_data_` is true. With WAL-enabled but unflushed data, persisted-tier reads can still return memtable values because that data is already durable in WAL.
- **Source:** `include/rocksdb/options.h` enum `ReadTier`; `db/db_impl/db_impl.cc` `DBImpl::GetImpl`, `DBImpl::NewIterator`, `DBImpl::NewIterators`, `DBImpl::MultiCFSnapshot`; `db/db_test.cc` `DBTest::ReadFromPersistedTier`; `db/db_basic_test.cc` `DBBasicTest::MultiGetWithSnapshotsAndPersistedTier`; `db/db_iterator_test.cc` `DBIteratorTest::PersistedTierOnIterator`
- **Fix:** Document `kPersistedTier` as a Get/MultiGet-only mode that excludes only non-durable memtable state, not all memtable state.

### [WRONG] Iterator auto-refresh is not periodic and does nothing without an explicit snapshot
- **File:** `07_iterator_scan.md`, "Auto-Refresh Iterator"; `11_read_options_and_tuning.md`, iterator options table and tuning recommendations
- **Claim:** "When `ReadOptions::auto_refresh_iterator_with_snapshot` is true, the iterator periodically refreshes its SuperVersion..." and "`auto_refresh_iterator_with_snapshot` | false | Periodically refresh SuperVersion to release old resources."
- **Reality:** The auto-refresh path is entered only when `read_options.snapshot != nullptr`, refresh is enabled, and the iterator observes that the SuperVersion number changed during `Seek`, `SeekForPrev`, `Next`, or `Prev`. There is no timer or periodic scheduler. If no explicit snapshot is supplied, enabling the option has no effect. The public API comments also call out explicit incompatibilities/cautions.
- **Source:** `include/rocksdb/options.h` field `ReadOptions::auto_refresh_iterator_with_snapshot`; `db/arena_wrapped_db_iter.cc` `ArenaWrappedDBIter::MaybeAutoRefresh`, `ArenaWrappedDBIter::DoRefresh`; `db/db_iterator_test.cc` `DBIteratorTest::RefreshIteratorWithSnapshot`
- **Fix:** Say it refreshes on observed SuperVersion changes during iterator progress, requires an explicit snapshot, and should be documented with its compatibility notes.

### [WRONG] Metadata-cache behavior ignores the partitioned index/filter exception
- **File:** `06_block_cache.md`, "Block Types in Cache"; `11_read_options_and_tuning.md`, "BlockBasedTableOptions Affecting Reads"
- **Claim:** "`cache_index_and_filter_blocks` | false | If true, index/filter blocks go through block cache. If false, pinned in table reader." and "When `cache_index_and_filter_blocks` is false (default), index and filter blocks are loaded when the table reader opens and pinned in memory for the table reader's lifetime, bypassing the cache entirely."
- **Reality:** `BlockBasedTableOptions::cache_index_and_filter_blocks` applies only to unpartitioned index/filter blocks. Index and filter partition blocks always use block cache regardless of this option.
- **Source:** `include/rocksdb/table.h` struct `BlockBasedTableOptions`; `table/block_based/block_based_table_reader.cc` `BlockBasedTable::Open`
- **Fix:** Split the explanation into unpartitioned metadata blocks vs partitioned metadata blocks, and note that partition blocks always go through block cache.

### [MISLEADING] Prefix-seek coverage overstates `auto_prefix_mode` and understates opt-in gating
- **File:** `07_iterator_scan.md`, "Prefix Seek Modes"; `11_read_options_and_tuning.md`, "Prefix Seek Modes"; `index.md`, "Key Characteristics"
- **Claim:** "`auto_prefix_mode` ... automatically enables prefix seek optimization when it would produce the same result as total-order seek" and "Prefix seek: Configurable prefix-bounded iteration with auto_prefix_mode for automatic optimization."
- **Reality:** Memtable iteration explicitly says auto-prefix mode is not implemented there yet. Also, DB option `prefix_seek_opt_in_only` can force `total_order_seek = true` unless the caller explicitly opts into prefix behavior. Chapter 07 has a short `prefix_seek_opt_in_only` section, but chapter 11 and the index describe default semantics as if this gate did not exist.
- **Source:** `include/rocksdb/options.h` fields `ReadOptions::auto_prefix_mode` and `ImmutableDBOptions::prefix_seek_opt_in_only`; `db/arena_wrapped_db_iter.cc` `ArenaWrappedDBIter::Init`; `db/memtable.cc` `MemTableIterator`; `db/db_test2.cc` `PrefixBloomFilteredOut`, `AutoPrefixMode1`
- **Fix:** State that `auto_prefix_mode` is not fully implemented in memtable iteration today, and add `prefix_seek_opt_in_only` to chapter 11 and the index-level description of prefix behavior.

### [MISLEADING] `epoch_number` is described as a flush-time creation-order stamp
- **File:** `05_sst_file_lookup.md`, "L0 Search Strategy"
- **Claim:** "`epoch_number` is assigned at flush time and tracks creation order"
- **Reality:** Metadata describes `epoch_number` as ordering for flushed or ingested/imported files, and compaction outputs inherit the minimum epoch number from their inputs. For L0 read order, larger epoch means newer L0 file, but the field is not a simple file-creation counter.
- **Source:** `include/rocksdb/metadata.h` field `epoch_number`; `db/version_set.h` comment on L0 ordering; `db/version_set.cc` `VersionStorageInfo::RecoverEpochNumbers`
- **Fix:** Explain it as recency/order metadata used for flushed, ingested, and imported files, with compaction-output inheritance semantics.

### [MISLEADING] SuperVersion cleanup is not universally deferred
- **File:** `03_superversion_and_snapshots.md`, "Reference Counting" and "SuperVersion Installation"
- **Claim:** "When the last reference is released, cleanup is deferred: obsolete memtables and versions are scheduled for deletion"
- **Reality:** Once `Unref()` hits zero, `SuperVersion::Cleanup()` runs immediately and drops refs on `mem`, `imm`, and `current`. `DBImpl::CleanupSuperVersion()` deletes the SuperVersion immediately unless `avoid_unnecessary_blocking_io` or iterator background-purge handling defers the final free/purge path. Some obsolete-file purging can also run on the caller thread.
- **Source:** `db/column_family.cc` `SuperVersion::Cleanup`; `db/db_impl/db_impl.cc` `DBImpl::CleanupSuperVersion`, `CleanupSuperVersionHandle`
- **Fix:** Separate immediate cleanup from optional deferred purge/free, instead of presenting the whole cleanup path as deferred.

### [UNVERIFIABLE] Chapters 10 and 11 include hard latency/throughput/comparison counts without a source
- **File:** `10_prefetching_and_async_io.md`, "Performance Impact"; `11_read_options_and_tuning.md`, "Common Read Patterns" and "Operational Insights"
- **Claim:** "`Latency: ~1-2 us.`", "`Latency: ~100 us - 1 ms`", "`Sequential scan (HDD) | ~100 IOPS | ~200 MB/s throughput`", and "`Each Iterator::Next()` performs `2-3` key comparisons on the hot path"
- **Reality:** I could not find code, tests, or benchmark artifacts in this tree that establish these exact numbers as current-code facts. The implementation is highly configuration-dependent: cache state, comparator cost, block format, row cache, prefix mode, and storage medium all materially change the result.
- **Source:** Checked `db/db_impl/db_impl.cc`, `db/db_iter.cc`, `table/block_based/block_based_table_reader.cc`, `file/file_prefetch_buffer.h`, and `include/rocksdb/options.h`; there is no benchmark citation or invariant-backed source for these numbers
- **Fix:** Remove exact numbers unless they are cited to a benchmark; otherwise keep the text qualitative and point readers to `db_bench`.

## Completeness Gaps

### MultiScan is missing entirely
- **Why it matters:** `DB::NewMultiScan()` is a real public read-path API with its own range model, async I/O controls, and correctness rules. The current read-flow set describes point lookups, MultiGet, and iterators, but omits a whole scan mode that has seen substantial recent work and test coverage.
- **Where to look:** `include/rocksdb/db.h` `DB::NewMultiScan`; `include/rocksdb/options.h` `MultiScanArgs`; `db/db_impl/db_impl.cc` `DBImpl::NewMultiScan`; `db/db_iter.cc` `DBIter::ValidateScanOptions`, `DBIter::Prepare`; `db/db_iterator_test.cc` `DBMultiScanIteratorTest`
- **Suggested scope:** Add a new chapter, then mention it in `index.md`, chapter 07, chapter 10, and chapter 11.

### Row cache is missing from both point lookup and MultiGet
- **Why it matters:** `TableCache::Get()` and `TableCache::MultiGet()` can satisfy reads from row cache before the normal table-reader/block-cache path. The row-cache key is snapshot-aware and replays a serialized `GetContext` log. This also changes MultiGet filtering behavior because `TableCache::MultiGetFilter()` returns `Status::NotSupported()` when row cache must be consulted.
- **Where to look:** `db/table_cache.cc` `CreateRowCacheKeyPrefix`, `GetFromRowCache`, `TableCache::Get`, `TableCache::MultiGetFilter`; `db/table_cache_sync_and_async.h` `TableCache::MultiGet`; `db/db_test2.cc` `DBTest2::RowCacheSnapshot`
- **Suggested scope:** Add a short subsection to chapter 01 and chapter 02, plus a cache-layer note in chapter 06.

### Timestamp history-collapse failure paths are missing
- **Why it matters:** Timestamped reads can fail before memtable/SST lookup if the requested timestamp is below `full_history_ts_low`. The point-lookup chapter currently reads as if lookup begins immediately after SuperVersion acquisition.
- **Where to look:** `db/db_impl/db_impl.cc` `FailIfReadCollapsedHistory`, `DBImpl::GetImpl`, `DBImpl::MultiCFSnapshot`, `DBImpl::NewIterator`; `db/db_impl/db_impl.h`; `03_superversion_and_snapshots.md` field `full_history_ts_low`
- **Suggested scope:** Add this to chapter 01 and chapter 03, with brief mentions in the iterator and MultiGet material.

### Cross-column-family snapshot coordination for MultiGet is not documented
- **Why it matters:** MultiGet across column families has a special `MultiCFSnapshot` path. For `kPersistedTier`, it can take the DB mutex to freeze SuperVersion changes across CFs and keep the view consistent. That is a real cross-component behavior difference, not a small implementation detail.
- **Where to look:** `db/db_impl/db_impl.cc` `MultiCFSnapshot`, `MultiGetCommon`; `db/db_basic_test.cc` `DBBasicTest::MultiGetWithSnapshotsAndPersistedTier`
- **Suggested scope:** Add an advanced subsection to chapter 02.

## Depth Issues

### MultiGet async section needs the real coroutine gating and caveats
- **Current:** "Within a level: Multiple SST files are processed concurrently via `folly::coro::collectAllRange`. `MultiGetFilter` is called first..."
- **Missing:** The coroutine path is disabled for L0, disabled when `async_io` or `optimize_multiget_for_io` is off, and disabled when coroutine or async-I/O support is unavailable. Also, `TableCache::MultiGetFilter()` can return `Status::NotSupported()` when row cache must be checked, so the "filter first, then launch coroutines" sequence is not universal.
- **Source:** `db/version_set.cc` `Version::MultiGet`, `Version::ProcessBatch`; `db/table_cache.cc` `TableCache::MultiGetFilter`

### Auto-refresh iterator section omits the actual refresh algorithm
- **Current:** The section compresses behavior into one paragraph about periodically refreshing SuperVersion.
- **Missing:** `Next()` and `Prev()` first advance on the old iterator, copy the target key, rebuild the iterator on the new SuperVersion, and reseek to that key. `Seek()` and `SeekForPrev()` refresh before performing the seek. That is the key correctness-preserving mechanism.
- **Source:** `db/arena_wrapped_db_iter.cc` `ArenaWrappedDBIter::MaybeAutoRefresh`, `ArenaWrappedDBIter::DoRefresh`

## Structure and Style Violations

### Inline code spans are used in every file
- **File:** All files in `docs/components/read_flow/`
- **Details:** The instructions for this doc set explicitly say "NO inline code quotes." Every file in the directory, including `index.md`, uses inline code spans for identifiers, options, file paths, and table entries.

### One section cites a basename instead of the actual repo path
- **File:** `07_iterator_scan.md`
- **Details:** The text points readers to `filter_block_reader_common.cc`, but the actual file is `table/block_based/filter_block_reader_common.cc`. That makes the source trail weaker than it needs to be for a debugging-oriented doc set.

## Undocumented Complexity

### Row cache stores replay logs, not raw values
- **What it is:** `TableCache` row-cache keys are `row_cache_id + fd_number + cache_entry_seq_no + user_key`. On hit, RocksDB replays a serialized `GetContext` log into the caller rather than returning a simple raw value blob. Snapshot-aware sequence selection is part of the key prefix.
- **Why it matters:** This explains why row cache interacts with snapshots, why `NeedToReadSequence()` disables it, and why MultiGet filter pre-work has to step aside when row cache is enabled.
- **Key source:** `db/table_cache.cc` `CreateRowCacheKeyPrefix`, `GetFromRowCache`, `TableCache::Get`; `db/table_cache_sync_and_async.h` `TableCache::MultiGet`
- **Suggested placement:** Add a subsection to chapter 01 and a short note in chapter 02 or chapter 06.

### `kPersistedTier` uses a cross-CF snapshot protocol
- **What it is:** Multi-CF reads use `MultiCFSnapshot`, and `kPersistedTier` can force mutex acquisition so all CFs observe a consistent persisted view even while memtables are being sealed or flushed.
- **Why it matters:** Without this note, readers will assume `kPersistedTier` is just a local filtering mode on a single Get/MultiGet call. It actually changes synchronization and cross-column-family behavior.
- **Key source:** `db/db_impl/db_impl.cc` `MultiCFSnapshot`
- **Suggested placement:** Chapter 02 and chapter 11 under `read_tier`

### Timestamped reads inject a special `ReadCallback`
- **What it is:** When user-defined timestamps are enabled, `GetImpl()` installs a `GetWithTimestampReadCallback` after snapshot selection so returned entries satisfy both sequence visibility and timestamp visibility.
- **Why it matters:** This is a subtle cross-component interaction between the public read API, snapshot handling, and per-entry filtering. It is also why some callback combinations are unsupported.
- **Key source:** `db/db_impl/db_impl.cc` `DBImpl::GetImpl`
- **Suggested placement:** Chapter 01 after snapshot assignment

## Positive Notes

- The overall chapter split is good. `index.md` is within the requested line budget and the chapter ordering mirrors the real read-path decomposition.
- Chapter 05 gets the big-picture `FilePicker` behavior right: L0 newest-first probing, L1+ binary search, and per-file table-cache handoff.
- Chapters 08 and 09 are the strongest parts of the set. The range-deletion and merge-resolution material is detailed and aligns well with the code.
- Chapter 10 has a solid high-level layering of config, iterator integration, prefetch orchestration, and buffer management, even though some of the performance claims need to be toned down or sourced.
