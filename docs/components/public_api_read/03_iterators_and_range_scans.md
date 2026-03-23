# Iterators and Range Scans

**Files:** `include/rocksdb/iterator.h`, `include/rocksdb/iterator_base.h`, `include/rocksdb/db.h`, `db/db_iter.h`, `db/arena_wrapped_db_iter.h`

## Creating Iterators

`DB::NewIterator()` returns a heap-allocated iterator (see `include/rocksdb/db.h`). The iterator is initially invalid -- caller must call a `Seek*()` method before use.

**Lifetime**: The caller owns the returned `Iterator*` and must delete it before closing the DB.

The default-column-family overload `NewIterator(options)` delegates to `NewIterator(options, DefaultColumnFamily())`.

## Core Iterator Operations

All operations are defined in `IteratorBase` (see `include/rocksdb/iterator_base.h`) and `Iterator` (see `include/rocksdb/iterator.h`).

### Positioning

| Operation | Behavior | Invalid When |
|-----------|----------|-------------|
| `Seek(target)` | Position at first key >= target | No key >= target exists |
| `SeekForPrev(target)` | Position at last key <= target | No key <= target exists |
| `SeekToFirst()` | Position at smallest key | DB/range is empty |
| `SeekToLast()` | Position at largest key | DB/range is empty |

### Navigation

| Operation | Behavior | Precondition |
|-----------|----------|-------------|
| `Next()` | Advance to next key | `Valid()` must be true |
| `Prev()` | Move to previous key | `Valid()` must be true |

### State

| Method | Returns |
|--------|---------|
| `Valid()` | `true` if positioned at a valid entry; always `false` if `!status().ok()` |
| `key()` | Current key as `Slice` (valid only until next iterator modification) |
| `value()` | Current value as `Slice` (valid only until next iterator modification) |
| `columns()` | Wide columns for current entry |
| `status()` | Error status; `OK` if no error |
| `timestamp()` | User-defined timestamp of current entry |

Note: `Seek*()` operations clear any previous error status. After a seek, `status()` reflects only seek-time errors.

## Iterator Bounds

`ReadOptions::iterate_lower_bound` and `ReadOptions::iterate_upper_bound` constrain the iteration range (see `ReadOptions` in `include/rocksdb/options.h`).

- **Lower bound** is inclusive: the backward iterator stops at this key
- **Upper bound** is exclusive: the iterator never returns a key >= upper bound
- `SeekToLast()` with an upper bound positions at the first key smaller than the bound
- Both bounds must point to keys without timestamp suffixes when user-defined timestamps are enabled

Setting `iterate_upper_bound` is strongly recommended for range scans because it enables:
- SST file filtering via index/bloom checks against the bound
- Early termination in the merging iterator

## Prefix Iteration

When a `prefix_extractor` is configured on the column family, prefix-based optimizations are available:

### prefix_same_as_start

When `ReadOptions::prefix_same_as_start=true`, the iterator only returns keys sharing the same prefix as the seek key. Uses the column family's current `prefix_extractor`. Prefix filtering applies to both `Seek()` and `SeekForPrev()`.

### total_order_seek

When `ReadOptions::total_order_seek=true`, prefix bloom filters are bypassed and the iterator returns keys in full sort order. This also affects `Get()` -- when true, prefix bloom is skipped during point lookups on block-based tables.

### auto_prefix_mode

When `ReadOptions::auto_prefix_mode=true`, RocksDB automatically decides whether to use prefix iteration based on the seek key and `iterate_upper_bound`. Defaults to total-order seek but enables prefix mode when the bound and seek key analysis indicates it would produce equivalent results.

Important: `auto_prefix_mode` has a known limitation -- "short keys" (shorter than the full prefix length) can be omitted from iteration results when prefix mode is enabled, even if they would appear in total-order iteration. See the documented BUG in `ReadOptions::auto_prefix_mode` in `include/rocksdb/options.h`.

## Lazy Value Loading (PrepareValue)

When `ReadOptions::allow_unprepared_value=true`, the iterator may defer loading values during positioning operations (`Seek*`, `Next`, `Prev`). The application must call `PrepareValue()` before accessing `value()` or `columns()` (see `IteratorBase::PrepareValue()` in `include/rocksdb/iterator_base.h`).

`PrepareValue()` returns `true` on success. On failure, it sets `Valid()=false` and `status()` to an error.

Note: This currently only applies to: (1) large values stored in blob files using BlobDB, and (2) multi-column-family iterators (`CoalescingIterator` and `AttributeGroupIterator`). For other cases, `PrepareValue()` is a no-op returning `true`.

## Refresh()

`Refresh()` updates the iterator to see the latest DB state without creating a new iterator (see `IteratorBase::Refresh()` in `include/rocksdb/iterator_base.h`). After refresh, the iterator is invalidated and must be repositioned with a `Seek*()` call.

Two overloads:
- `Refresh()` -- refreshes to the latest state
- `Refresh(const Snapshot*)` -- refreshes to state under the given snapshot

This is more efficient than deleting and recreating the iterator because it reuses internal data structures.

### Auto-Refresh

`ReadOptions::auto_refresh_iterator_with_snapshot` (experimental) enables automatic refresh of long-running iterators to release resources (SuperVersion references, pinned memtables) from older LSM versions. This prevents long-running iterators from blocking compaction garbage collection.

Note: Not compatible with `WRITE_PREPARED` or `WRITE_UNPREPARED` transaction policies.

## Tailing Iterators

When `ReadOptions::tailing=true`, the iterator sees writes that occur after its creation. It provides a view of the complete database and can read newly inserted data, suitable for sequential reads of a growing dataset.

Note: Tailing iterators have performance overhead and limited snapshot consistency guarantees compared to regular iterators.

## Iterator Properties

`Iterator::GetProperty()` returns runtime properties (see `Iterator::GetProperty()` in `include/rocksdb/iterator.h`):

| Property | Description |
|----------|-------------|
| `rocksdb.iterator.is-key-pinned` | "1" if `key()` remains valid until iterator deletion |
| `rocksdb.iterator.is-value-pinned` | "1" if `value()` remains valid until iterator deletion |
| `rocksdb.iterator.super-version-number` | LSM version number used by this iterator |
| `rocksdb.iterator.internal-key` | User-key portion of the internal key at current position |
| `rocksdb.iterator.write-time` | Best-estimate write time as 64-bit raw value (decode with `DecodeU64Ts`) |

Key pinning is guaranteed when `ReadOptions::pin_data=true` and `BlockBasedTableOptions::use_delta_encoding=false`. Value pinning is guaranteed when `pin_data=true` and the value is in a `kTypeValue` record.

## Prepare() for Multi-Range Prefetching

`Iterator::Prepare(const MultiScanArgs&)` hints the iterator about upcoming scan ranges, enabling prefetching of relevant blocks from disk (see `Iterator::Prepare()` in `include/rocksdb/iterator.h`). The upper bound and table-specific limits should be specified for each scan for best results. If `Prepare()` is called, it overrides `iterate_upper_bound` in `ReadOptions`.

## Table Filter

`ReadOptions::table_filter` is a callback that receives `TableProperties` for each SST file. If it returns `false`, the file is skipped entirely. Only affects iterators, not point lookups.

## Background Purge

`ReadOptions::background_purge_on_iterator_cleanup=true` schedules deletion of obsolete files in the flush job queue background thread when the iterator is destroyed, rather than deleting inline.

## Iterator::Next() CPU Cost

Each `Next()` call performs approximately 2-3 key comparisons (assuming upper bound is set and no merge operator):

1. **Heap merge** via `replace_top()`: In most workloads, data has good locality -- ~90% of keys are in the bottommost sorted run. When the top iterator stays on top after `Next()` (~81% of the time), only 1 comparison is needed.
2. **Duplicate key check**: `DBIter` checks whether the current entry has the same user key as the previous entry (to skip older versions in the multi-version system). This rarely finds a duplicate since compaction collects garbage, but the comparison is always required.

For most operations, CPU cache misses dominate the cost of comparisons, not the comparisons themselves. However, `Iterator::Next()` is simple enough that comparison cost becomes a non-trivial fraction, especially with very long keys or expensive custom comparators.

## Error Handling During Iteration

When a data block is corrupted, the iterator sets `status()` to `Corruption` but may still return `Valid()=true` for entries in non-corrupted blocks. The correct iteration pattern must check `status()` within the loop body, not just after loop completion:

Step 1: Call `Seek*()` to position
Step 2: Check `Valid()` and `status()` in the loop condition
Step 3: Process `key()`/`value()`
Step 4: Call `Next()`/`Prev()`
Step 5: After loop, check `status()` for any final errors

Important: The common pattern `for (Seek; Valid && key < bound; Next)` with only a post-loop `status()` check is insufficient -- it will miss mid-iteration corruption.
