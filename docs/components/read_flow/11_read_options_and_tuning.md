# ReadOptions and Tuning

**Files:** `include/rocksdb/options.h`, `include/rocksdb/advanced_options.h`, `include/rocksdb/table.h`

## ReadOptions Overview

`ReadOptions` (see `ReadOptions` in `include/rocksdb/options.h`) controls the behavior of individual read operations. Options are divided into those relevant to both point lookups and scans, and those relevant only to iterators.

## Options for Point Lookups and Scans

| Option | Default | Purpose |
|--------|---------|---------|
| `snapshot` | nullptr | Read as of this snapshot. If null, use implicit snapshot at read time. |
| `timestamp` | nullptr | Read as of this user-defined timestamp. Used with UDT-enabled column families. |
| `verify_checksums` | true | Verify data block checksums on read. Disable for performance if data integrity is trusted. |
| `fill_cache` | true | Insert read blocks into block cache. Set false for bulk scans to avoid cache pollution. |
| `read_tier` | `kReadAllTier` | Controls which tiers are accessible: cache only (`kBlockCacheTier`), all tiers, or persisted only (`kPersistedTier`, Get/MultiGet only). |
| `deadline` | 0 | Maximum time (microseconds since epoch) for the operation. Best-effort. |
| `io_timeout` | 0 | Per-file-read timeout in microseconds. |
| `async_io` | false | Enable asynchronous prefetching. Requires file system support. |
| `value_size_soft_limit` | max uint64 | Abort MultiGet when cumulative value size exceeds this limit. |
| `ignore_range_deletions` | false | Skip range tombstone processing. Only safe if no DeleteRange calls were made. DEPRECATED. |
| `merge_operand_count_threshold` | 0 | If non-zero and merge operand count exceeds this, Get returns early with `kMergeOperandThresholdExceeded` subcode. |

## Iterator-Only Options

| Option | Default | Purpose |
|--------|---------|---------|
| `readahead_size` | 0 | Fixed readahead size. If 0, auto-readahead is used. |
| `iterate_upper_bound` | nullptr | Exclusive upper bound for forward iteration. Enables file/block skipping. |
| `iterate_lower_bound` | nullptr | Inclusive lower bound for backward iteration. |
| `total_order_seek` | false | Force total order seek even with hash index. Also skips prefix bloom for Get. |
| `auto_prefix_mode` | false | Automatically choose prefix or total order seek based on key and upper bound. |
| `prefix_same_as_start` | false | Restrict iteration to keys with same prefix as the seek key. |
| `pin_data` | false | Pin data blocks in memory for iterator lifetime. Enables zero-copy value access. |
| `adaptive_readahead` | false | Carry readahead size across files within a level. |
| `auto_readahead_size` | true | Auto-tune readahead based on cache, upper bound, and prefix. |
| `max_skippable_internal_keys` | 0 | Fail seek as Incomplete after skipping this many keys. 0 = unlimited. |
| `tailing` | false | Create tailing iterator that can read newly added data. |
| `background_purge_on_iterator_cleanup` | false | Delete obsolete files in background when iterator is destroyed. |
| `table_filter` | empty | Callback to skip entire SST files based on table properties. |
| `allow_unprepared_value` | false | Defer value loading for BlobDB values and multi-CF iterators. |
| `auto_refresh_iterator_with_snapshot` | false | Refresh SuperVersion on observed changes during iteration. Requires explicit snapshot. |

## Prefix Seek Modes

RocksDB supports prefix-bounded iteration that leverages prefix bloom filters for efficiency:

**`prefix_same_as_start = true`:**
- Iteration is restricted to keys sharing the prefix of the seek key (extracted via `prefix_extractor`)
- Enables prefix bloom filter usage for both Seek and iteration
- `Valid()` returns false when a key with a different prefix is encountered

**`total_order_seek = true`:**
- Forces total-order iteration regardless of index format
- Skips prefix bloom filters (both in memtable and SST files)
- Required when iterating across prefix boundaries with a prefix extractor configured

**`auto_prefix_mode = true`:**
- Defaults to total order seek behavior
- Automatically enables prefix seek when it would produce the same result as total order seek
- Decision based on comparing the seek key's prefix with `iterate_upper_bound`'s prefix
- Note: Has a known bug with "short keys" (shorter than full prefix length) that may be omitted
- Note: Not yet implemented for memtable iteration (memtable iterators use total-order path)

When `prefix_extractor` is set but none of the prefix-related `ReadOptions` (`prefix_same_as_start`, `total_order_seek`, `auto_prefix_mode`) are enabled, and `ImmutableDBOptions::prefix_seek_opt_in_only` is true, the iterator forces `total_order_seek = true` to default to total-order behavior unless the user explicitly opts in.

## Column Family Options Affecting Reads

| Option | Location | Effect on Reads |
|--------|----------|-----------------|
| `prefix_extractor` | `ColumnFamilyOptions` | Enables prefix bloom filters in memtable and SST files |
| `memtable_prefix_bloom_size_ratio` | `ColumnFamilyOptions` | Controls memtable bloom filter size relative to memtable size |
| `memtable_whole_key_filtering` | `ColumnFamilyOptions` | Enable whole-key bloom filter in memtable |
| `optimize_filters_for_hits` | `ColumnFamilyOptions` | Skip bloom filters at bottommost level |
| `max_sequential_skip_in_iterations` | `AdvancedColumnFamilyOptions` | Threshold for skip-to-seek optimization in iterators (default: 8) |

## BlockBasedTableOptions Affecting Reads

| Option | Default | Effect |
|--------|---------|--------|
| `cache_index_and_filter_blocks` | false | If true, top-level index/filter blocks go through block cache. If false, pinned in table reader. Partition blocks always use block cache regardless. |
| `cache_index_and_filter_blocks_with_high_priority` | true | Give index/filter blocks HIGH eviction priority in cache |
| `pin_l0_filter_and_index_blocks_in_cache` | false | Pin L0 index/filter blocks in cache (never evict) |
| `partition_filters` | false | Use partitioned bloom filters for reduced memory and I/O |
| `block_size` | 4KB | Data block size. Larger blocks improve compression but increase read amplification. |
| `index_type` | `kBinarySearch` | Index block format. `kTwoLevelIndexSearch` for partitioned indexes. |
| `filter_policy` | nullptr | Bloom/Ribbon filter. nullptr means no filter. |

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Get() memtable | O(log N) | Skiplist seek + optional bloom filter check |
| Get() SST (L1+) | O(log L * log F) | L levels, F files per level |
| Get() SST (L0) | O(F) | All F L0 files checked (with bloom filter gating) |
| Block cache lookup | O(1) expected | Hash table in LRUCache/HyperClockCache |
| MultiGet (batch of B) | O(B log N) amortized | Block reuse and coalesced I/O |
| Iterator Seek | O(log N) per child | Binary search in blocks and index |
| Iterator Next | O(1) amortized | Sequential within block, seek optimization for many versions |
| Bloom filter check | O(k) | k hash functions (typically ~10) |
| RangeDelAggregator | O(log K) amortized | K range tombstones |

## Common Read Patterns

**Hot key (cached):** Get -> memtable miss -> immutable miss -> TableCache hit (pinned reader) -> BlockCache hit (data block) -> return value. Latency dominated by in-memory lookups (use `db_bench` to measure for your workload).

**Cold key (uncached):** Get -> memtable miss -> L0 bloom negative -> L1 bloom negative -> Ln bloom positive -> disk read index -> disk read data -> decompress -> cache insert -> return. Latency dominated by disk I/O (highly dependent on storage medium and block size).

**Prefix scan:** Iterator created with `prefix_same_as_start=true`. Seek to prefix start, iterate until prefix changes. Bloom filters applied at each SST file. Auto-readahead trimmed to prefix boundary.

**Full table scan:** Iterator with `fill_cache=false` to avoid cache pollution. Auto-readahead grows to 256KB for sequential throughput. Consider `pin_data=true` for zero-copy access if values are large.

## Tuning Recommendations

| Goal | Recommendation |
|------|----------------|
| Reduce read latency | Increase block cache size, enable bloom filters, use `optimize_filters_for_hits` |
| Improve scan throughput | Increase `readahead_size` or rely on auto-readahead, set `fill_cache=false` for large scans |
| Reduce memory for filters | Use partitioned filters (`partition_filters=true`), or Ribbon filters. Bottom-level bloom typically occupies ~89% of total filter size -- `optimize_filters_for_hits` eliminates this. |
| Minimize I/O for MultiGet | Enable `async_io=true`, keep `optimize_multiget_for_io=true` |
| Long-running iterators | Use `auto_refresh_iterator_with_snapshot=true` with an explicit snapshot (required for refresh to take effect) |
| Bounded scans | Set `iterate_upper_bound` to enable file/block skipping and readahead trimming |
| Reduce cache contention | Use `HyperClockCache` instead of `LRUCache` for high-concurrency workloads |
| Many L0 files / FIFO style | Be aware that bloom filter false positives accumulate across files: with 1% FPR and 30K files, each Get may trigger ~300 false positive I/Os |

## Operational Insights

**Iterator Next() cost:** Each `Iterator::Next()` performs key comparisons on the hot path (heap merge in `MergingIterator`, multiversion skip in `DBIter`). For workloads with expensive comparators or very long keys, comparison cost can dominate CPU.

**Block cache contention:** Block cache mutex contention is historically the dominant CPU cost on the read path for concurrent workloads. `HyperClockCache` uses lock-free operations to reduce this bottleneck.

**Bloom filter I/O tradeoffs:** When bloom filters are not cached in memory, reading them from disk may cost MORE I/O than they save (one extra read for the filter block). This matters when DRAM is insufficient to cache all filters. Partitioned filters reduce per-file filter memory by loading only the relevant partition.

**Readahead waste:** Without `iterate_upper_bound`, auto-readahead may prefetch significant amounts of data beyond the scan range. Setting `iterate_upper_bound` or enabling `auto_readahead_size` (default: true) trims readahead to avoid wasted I/O.
