# Iterator and Scan Path

**Files:** `db/db_iter.cc`, `db/db_iter.h`, `table/merging_iterator.cc`, `table/merging_iterator.h`, `db/arena_wrapped_db_iter.cc`, `db/arena_wrapped_db_iter.h`

## Iterator Stack Architecture

RocksDB iterators form a layered stack:

**DBIter** (user-facing) -- Resolves internal keys into user keys. Handles merge resolution, deletion skipping, snapshot visibility, and direction changes. This is the iterator returned by `DB::NewIterator()`.

**MergingIterator** -- Merges multiple sorted input streams (memtable iterators, SST file iterators) into a single sorted stream using a min-heap (forward) or max-heap (reverse). Also integrates range tombstones.

**Child iterators** -- Individual iterators for each data source:
- One iterator per memtable (mutable + each immutable)
- L0: one `BlockBasedTableIterator` per file
- L1+: one `LevelIterator` per level (lazy-opens files as needed)
- Each child may have an associated `TruncatedRangeDelIterator` for range tombstones

## NewIterator() Construction

`DB::NewIterator()` builds the iterator stack via `ArenaWrappedDBIter` (see `NewArenaWrappedDbIterator()` in `db/arena_wrapped_db_iter.cc`):

Step 1: Acquire SuperVersion for the column family

Step 2: Create child iterators:
- Mutable memtable iterator + its range tombstone iterator
- Immutable memtable iterators via `MemTableListVersion::AddIterators()`
- SST file iterators via `Version::AddIterators()`

Step 3: Build `MergingIterator` using `MergeIteratorBuilder` which registers all point and tombstone iterators

Step 4: Wrap in `DBIter` which handles user-facing semantics

Step 5: Register SuperVersion cleanup -- the SuperVersion reference is held until the iterator is destroyed

## DBIter Direction Model

`DBIter` operates in one of two directions:

| Direction | Internal Iterator Position |
|-----------|---------------------------|
| `kForward` | At the entry yielding `key()/value()` (non-merge case), or immediately after the last merge operand |
| `kReverse` | Just BEFORE all entries with `user_key == this->key()` |

**Direction changes** are expensive because they require re-positioning:
- `ReverseToForward()`: Re-seeks the internal iterator to the correct forward position
- `ForwardToReverse()`: Repositions before all entries of the current user key

## FindNextUserEntry() -- Core Resolution Loop

`FindNextUserEntryInternal()` in `db/db_iter.cc` is the core loop that converts internal keys into user-visible entries:

Step 1: **Visibility check** -- Skip entries with `sequence > snapshot_seq` or invisible timestamps

Step 2: **Duplicate key skipping** -- If the current user key has already been processed (a Put or Delete was returned), skip all remaining versions of this key by setting `skipping_saved_key_`

Step 3: **Type dispatch:**
- `kTypeDeletion` / `kTypeSingleDeletion`: If `timestamp_lb_` is set (timestamp range query), return the tombstone. Otherwise, mark the user key as "skip" and advance.
- `kTypeValue` / `kTypeBlobIndex` / `kTypeWideColumnEntity`: Save key/value and return to the user.
- `kTypeMerge`: Call `MergeValuesNewToOld()` to collect and resolve merge operands.

Step 4: **Seek optimization** -- If `num_skipped > max_skip_` (configurable via `max_sequential_skip_in_iterations` in `AdvancedColumnFamilyOptions`, see `include/rocksdb/advanced_options.h`), instead of scanning through all versions of a key one-by-one, seek directly past the key. This avoids O(N) scanning when a key has many versions.

## MergingIterator

`MergingIterator` (see `MergingIterator` in `table/merging_iterator.cc`) maintains a min-heap of child iterators for forward iteration and a max-heap for reverse.

**Key operations:**
- `Seek(target)`: Seek all children, then rebuild the heap
- `Next()`: Advance the current top-of-heap child, then re-heapify
- `Prev()`: Advance the current top-of-heap child backward, then re-heapify

### Range Tombstone Integration in MergingIterator

`SkipNextDeleted()` is called after each Next/Seek to filter out point keys covered by range tombstones:

1. Check if the current top-of-heap point key is covered by any active range tombstone
2. If covered, seek the point iterator past the tombstone's end key
3. This can cascade: the new position might be covered by a tombstone from a different level

This cascading seek mechanism efficiently skips over large deleted ranges without iterating through each covered key.

## Iterator Stability Guarantees

**Pinned blocks (zero-copy iteration):**
- `PinnedIteratorsManager` in `DBIter` manages block lifetime
- When `ReadOptions::pin_data` is true, data blocks are pinned in memory for the iterator's lifetime
- Values remain valid until the iterator moves or is destroyed, avoiding memcpy overhead

**Consistent snapshot:**
- The SuperVersion reference is held until iterator destruction (registered via `CleanupSuperVersionHandle`)
- This prevents file deletion during iteration
- On cleanup, `CleanupSuperVersionHandle()` unrefs the SuperVersion, potentially triggering obsolete file deletion

## Seek Optimization Details

Two complementary optimizations reduce the cost of skipping keys:

**Within-key skip-to-seek:** In `FindNextUserEntry()`, when too many versions of the same key are scanned (`num_skipped > max_skip_`), DBIter seeks to `(user_key, 0, kTypeDeletion)` to jump past all versions. The threshold is controlled by `max_sequential_skip_in_iterations` (default: 8).

**Range tombstone cascading seek:** In `MergingIterator::SeekImpl()`, when a point key is covered by a range tombstone, the iterator seeks past the tombstone's end key. If the new position is covered by another tombstone (possibly from a different level), the seek cascades. This avoids iterating over keys in large deleted ranges.

## Auto-Refresh Iterator

When `ReadOptions::auto_refresh_iterator_with_snapshot` is true and an explicit snapshot is provided (`ReadOptions::snapshot != nullptr`), the iterator refreshes its SuperVersion when it detects a change. On each `Seek()`, `Next()`, or `Prev()`, the iterator checks if the SuperVersion number has changed via a relaxed atomic read. If it has:

- **For `Seek()`/`SeekForPrev()`:** The iterator refreshes before performing the seek on the new SuperVersion.
- **For `Next()`/`Prev()`:** The iterator first advances to capture the target key, then rebuilds on the new SuperVersion and re-seeks to that key.

This releases resources held by the old SuperVersion (obsolete memtables, old SST files) during long-running iterations. The explicit snapshot ensures consistent visibility across refreshes. Without an explicit snapshot, enabling the option has no effect.

## Iterator Bounds

`ReadOptions::iterate_lower_bound` and `ReadOptions::iterate_upper_bound` constrain the iteration range:

- `iterate_upper_bound` (exclusive): DBIter returns `Valid() == false` when the key reaches or exceeds this bound. Also enables optimizations like trimming prefetch ranges and `UpperBoundCheckResult` in `BlockBasedTableIterator`.
- `iterate_lower_bound` (inclusive): Constrains backward iteration. When reached during `Prev()`, `Valid()` becomes false.

These bounds enable the internal iterators to skip irrelevant SST files and blocks, improving scan performance for bounded range queries.

## Prefix Seek Modes

RocksDB supports three prefix-related modes that control how bloom filters and iteration boundaries interact:

### prefix_same_as_start

When `ReadOptions::prefix_same_as_start` is true:
- On `Seek(key)`, the prefix of the seek key is saved in `prefix_` (via `prefix_extractor`)
- On each `Next()`, if the current key's prefix differs from `prefix_`, `Valid()` returns false
- Enables prefix bloom filter checks in SST file iterators via `CheckPrefixMayMatch()` in `BlockBasedTableIterator`
- Also enables readahead trimming to avoid prefetching blocks outside the prefix boundary

### total_order_seek

When `ReadOptions::total_order_seek` is true:
- Forces total-order iteration regardless of index format (e.g., hash index)
- Skips prefix bloom filters in both memtable and SST files
- Required when iterating across prefix boundaries with a prefix extractor configured
- Also skips prefix bloom when calling `Get()` (not just iteration)

### auto_prefix_mode

When `ReadOptions::auto_prefix_mode` is true:
- Defaults to total-order seek behavior
- Automatically enables prefix seek optimization when it would produce the same result as total-order seek
- The decision is based on comparing the seek key's prefix with `iterate_upper_bound`'s prefix
- `IsFilterCompatible()` in `table/block_based/filter_block_reader_common.cc` checks if the prefix extractor and upper bound allow safe prefix filtering
- Note: Has a known bug with "short keys" (shorter than full prefix length) that may be omitted from iteration
- Note: `auto_prefix_mode` is not yet implemented for memtable iteration. Memtable iterators fall through to the total-order path when this mode is enabled

### prefix_seek_opt_in_only

When `prefix_extractor` is set but none of the prefix-related `ReadOptions` are enabled, `ArenaWrappedDBIter` forces `total_order_seek = true` (see `Init()` in `db/arena_wrapped_db_iter.cc`). This ensures that iterators default to total-order behavior unless the user explicitly opts into prefix mode.

## Prefix Bloom in BlockBasedTableIterator

`CheckPrefixMayMatch()` in `BlockBasedTableIterator` (see `block_based_table_iterator.h`) calls `PrefixRangeMayMatch()` in `BlockBasedTableReader`, which:

1. Checks if the prefix extractor is compatible with the current SST file
2. Calls `filter->RangeMayExist()` which probes the bloom filter with the prefix
3. If the filter says the prefix definitely doesn't exist in this file, the iterator is marked as invalid without reading any data blocks

## table_filter Callback

`ReadOptions::table_filter` (see `ReadOptions` in `include/rocksdb/options.h`) is a callback invoked during `TableCache` iteration (see `TableCache::NewIterator()` in `db/table_cache.cc`). If the callback returns false for a given SST file's properties, the entire file is skipped. This only affects iterators, not point lookups.

## max_skippable_internal_keys

When `ReadOptions::max_skippable_internal_keys` is non-zero, `DBIter` counts internal keys skipped during seek operations. If the count exceeds the threshold, the operation returns `Status::Incomplete()`. This prevents unbounded latency from keys with many versions or large deleted ranges. Default is 0 (unlimited).

## NewMultiScan

`DB::NewMultiScan()` (see `DB` in `include/rocksdb/db.h`) is a batch scan API that scans multiple key ranges with a single `MultiScan` object. It supports async I/O controls and its own range model via `MultiScanArgs` (see `MultiScanArgs` in `include/rocksdb/options.h`). `DBImpl::NewMultiScan()` provides the full implementation. This API does not yet support user-defined timestamps.
