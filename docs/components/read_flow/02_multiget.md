# MultiGet Optimizations

**Files:** `db/db_impl/db_impl.cc`, `db/db_impl/db_impl.h`, `table/multiget_context.h`, `db/version_set.cc`, `db/version_set_sync_and_async.h`, `table/block_based/block_based_table_reader.cc`, `table/block_based/block_based_table_reader_sync_and_async.h`, `db/table_cache_sync_and_async.h`

## Overview

`DB::MultiGet()` retrieves multiple keys in a single call, enabling optimizations impossible with individual `Get()` calls. Keys are pre-sorted, grouped by column family, and processed in batches to maximize block reuse, coalesce I/O operations, and leverage async prefetching. Internally, `DBImpl::MultiGet()` handles the public API, while `DBImpl::MultiGetWithCallback()` provides a variant for transactions that need custom visibility via `ReadCallback`.

## MultiGetContext and Bitmask Tracking

`MultiGetContext` (see `MultiGetContext` in `table/multiget_context.h`) manages batched key state using 64-bit bitmasks for O(1) tracking:

| Constant/Field | Purpose |
|----------------|---------|
| `MAX_BATCH_SIZE` (32) | Maximum keys per batch, fits in a 64-bit `Mask` type |
| `value_mask_` | Shared bitmask marking keys whose final value is found |
| `skip_mask_` | Per-range: keys to skip (e.g., filtered by bloom) |
| `invalid_mask_` | Per-range: keys that don't belong to this range |

Each key's state is tracked in a `KeyContext` struct (see `KeyContext` in `table/multiget_context.h`) containing the key, column family, output value/status, merge context, and per-key metadata.

### The Range Class: Zero-Allocation Key Narrowing

`Range` (see `MultiGetContext::Range` in `table/multiget_context.h`) is the mechanism that allows each lookup layer (memtable, immutable memtables, SST files) to operate on only the unresolved keys — without copying or allocating. A `Range` is a view over a contiguous slice of `sorted_keys_` defined by `[start_, end_)` indices, combined with three bitmasks:

- `value_mask_` (shared across all Ranges via `MultiGetContext`): bit set when a key's final value is found at any layer
- `skip_mask_` (per-Range): bit set when a key should be skipped within this particular Range (e.g., filtered out by bloom)
- `invalid_mask_` (per-Range): bit set when a key doesn't belong to this Range's scope (e.g., it maps to a different SST file)

The `Range::Iterator` (a forward iterator) advances through keys by skipping any index whose bit is set in `value_mask_ | skip_mask_ | invalid_mask_`. This means when you iterate a Range, you automatically see only the keys that still need resolution in that context.

**How Range flows through lookup layers:**

Consider a batch of 8 keys (K0-K7). Initially all bits are clear — every key is unresolved:

Step 1: **MemTable lookup** — `MultiGetImpl()` creates a `Range` covering all 8 keys and calls `super_version->mem->MultiGet(read_options, &range, ...)`. The memtable's `MultiGet` looks up each key; when it finds K2 and K5, it calls `range.MarkKeyDone(iter)` which sets bits 2 and 5 in the shared `value_mask_`. The `Range` is checked with `range.empty()` — if all keys are resolved, SST lookup is skipped entirely.

Step 2: **Immutable memtable lookup** — If `!range.empty()`, the same `Range` is passed to `super_version->imm->MultiGet(read_options, &range, ...)`. The immutable memtable list iterates the same Range, but its Iterator now skips K2 and K5 (bits set in `value_mask_`). If it finds K0, bit 0 is set. Now 3 of 8 keys are resolved.

Step 3: **SST file lookup** — If `!range.empty()`, `super_version->current->MultiGet(read_options, &range, ...)` is called. `Version::MultiGet()` uses `FilePickerMultiGet` to route keys to SST files level by level. For each SST file, `FilePickerMultiGet` produces a narrowed `current_file_range_` — a sub-Range where `invalid_mask_` marks keys that fall outside this file's key range. Within a file, the bloom filter check (`FullFilterKeysMayMatch`) may set `skip_mask_` bits for keys definitely absent from this file. Only keys surviving both masks reach the data block lookup.

Step 4: **Level-by-level narrowing** — At L0, files overlap and must be checked individually. At L1+, files are sorted and non-overlapping, so `FilePickerMultiGet` efficiently maps sorted keys to files using binary search. As keys are resolved at each level (via `MarkKeyDone`), subsequent levels see fewer keys. If all keys are found before reaching the bottommost level, remaining levels are skipped.

The key insight is that `Range` never copies or reallocates the key array — it only manipulates bitmasks. The shared `value_mask_` propagates resolved keys across all layers, while per-Range `skip_mask_` and `invalid_mask_` handle local filtering without affecting the global state. This is what makes MultiGet's batch processing efficient: a single 64-bit integer tracks the resolution state of up to 32 keys.

## Batching Strategy

`DBImpl::MultiGet()` processes keys in batches of `MultiGetContext::MAX_BATCH_SIZE`. For each batch:

Step 1: **Sort keys** -- `PrepareMultiGetKeys()` sorts keys by `(column_family_id, user_key)` via `CompareKeyContext`. Sorted order is critical: it enables sequential index seeks and adjacent-block detection.

Step 2: **Group by column family** -- Keys are partitioned so each batch targets a single column family, sharing a single SuperVersion acquisition.

Step 3: **MemTable lookup** -- `MultiGetImpl()` calls `mem->MultiGet()` then `imm->MultiGet()`. Keys resolved in memtables are marked done via `MarkKeyDone`, and remaining keys proceed to SST lookup.

Step 4: **SST file lookup** -- `Version::MultiGet()` searches SST files level by level using `FilePickerMultiGet` to map sorted keys to overlapping files.

## Key Optimizations Summary

| Optimization | Mechanism | Benefit |
|--------------|-----------|---------|
| Batch size limit | `MAX_BATCH_SIZE = 32` with 64-bit bitmask | Fits in registers, zero-allocation tracking |
| Pre-sorted keys | `PrepareMultiGetKeys()` sorts by CF + user key | Sequential index seeks, adjacent block detection |
| Shared SuperVersion | Acquired once per batch | Single snapshot, reduced atomic operations |
| Batched bloom filters | `FullFilterKeysMayMatch()` checks all keys in one pass | Single filter block load serves multiple keys |
| Block reuse | `reused_mask` + `NullBlockHandle` detection | Skip redundant cache lookup and I/O for co-located keys |
| Adjacent block coalescing | `RetrieveMultipleBlocks()` merges nearby reads | Fewer I/O syscalls |
| Async cache probes | `StartAsyncLookupFull()` + `WaitAll()` | Parallel cache lookups |
| Shared cleanable | `SharedCleanablePtr` for reused blocks | Reduces cache refcount contention |
| Coroutine parallelism | `folly::coro::collectAllRange` for per-level files | Parallel SST lookups within a level |
| Stack scratch buffer | `kMultiGetReadStackBufSize` (8192 bytes) | Avoids heap allocation for small reads |

## Bloom Filter Batch Check

`FullFilterKeysMayMatch()` in `block_based_table_reader.cc` checks all remaining keys against the bloom filter in a single pass by calling `filter->KeysMayMatch(range, ...)`. Keys that are definitely absent are removed from the range via `SkipKey`. This avoids per-key bloom filter overhead.

The standalone `MultiGetFilter()` is used in the coroutine path to perform filter checking before launching async data block reads.

## Block Reuse

Inside `BlockBasedTable::MultiGet()`, when the index is seeked for each key, block handle offsets are compared. If the current key's block handle offset equals the previous key's, the key maps to the same data block. A `NullBlockHandle` is stored and a bit in `reused_mask` is set, avoiding redundant cache lookups and I/O.

During data iteration, keys that reused a previous block share the existing block iterator. The `SharedCleanablePtr` mechanism handles reference counting -- when multiple keys pin the same block cache entry, a shared cleanable avoids redundant `Ref`/`Unref` operations on the cache.

## I/O Coalescing

`RetrieveMultipleBlocks()` in `block_based_table_reader_sync_and_async.h` is called for cache-miss blocks:

Step 1: When compression is enabled and blocks are physically adjacent (`prev_end == handle.offset()`), they are merged into a single `FSReadRequest`

Step 2: A scratch buffer strategy is used: if total read size fits in `kMultiGetReadStackBufSize` (8192 bytes), a stack buffer avoids heap allocation; otherwise a heap buffer is used

Step 3: I/O dispatch -- synchronous path uses `file->MultiRead()` to issue all requests at once; coroutine path uses `co_await batch->context()->reader().MultiReadAsync()`

Step 4: After reads complete, each block is verified (checksum), decompressed if needed, and inserted into block cache

This reduces system calls from O(keys) to O(distinct_read_regions), which is often much smaller.

## Async I/O and Coroutine Integration

When `ReadOptions::async_io` and `ReadOptions::optimize_multiget_for_io` are both true, and the filesystem supports `kAsyncIO`:

**Within a level:** Multiple SST files are processed concurrently via `folly::coro::collectAllRange`. `MultiGetFilter` is called first to filter keys, then coroutines are launched for each file. Note: the coroutine path is disabled for L0 (where files overlap and must be processed in order), and requires both coroutine and async-I/O support to be available at compile time. Also, `TableCache::MultiGetFilter()` returns `Status::NotSupported()` when row cache is enabled, so the filter-then-launch sequence is skipped in that case.

**Async cache probes:** For each unique block handle, `block_cache.StartAsyncLookupFull()` initiates an async cache lookup. All lookups are started, then `WaitAll()` collects results. Cache hits populate results directly; misses accumulate into the I/O phase.

**Async disk I/O:** `AsyncFileReader` (see `AsyncFileReader` in `util/async_file_reader.h`) implements the C++20 Awaitable concept. `MultiReadAsyncImpl` calls `RandomAccessFileReader::ReadAsync`, which on Linux uses `io_uring` via `PosixFileSystem::Poll()`. The `ReadAwaiter` stores pending I/O handles and the suspended coroutine handle.

## Version::MultiGet SST File Routing

`Version::MultiGet()` uses `FilePickerMultiGet` to determine which sorted keys overlap with which SST files, level by level:

- **L0 (overlapping files):** Each file is processed individually against the relevant key subset
- **L1+ (sorted runs):** Keys are naturally batched into fewer files due to non-overlapping ranges
- The `current_file_range_` produced by `FilePickerMultiGet` is a `MultiGetRange` subset of keys overlapping the current SST file

## Performance Characteristics

MultiGet achieves sub-linear scaling: N keys via MultiGet is significantly faster than N individual Get calls:

- Amortized SuperVersion acquisition (one atomic operation instead of N)
- Shared bloom filter and index block loads
- Reduced system call overhead through I/O coalescing (N reads merged into O(1) syscalls for adjacent keys)
- Better CPU cache utilization from sorted access patterns
- Async I/O hides latency when keys span multiple levels

## MultiGetEntity

`DB::MultiGetEntity()` retrieves multiple keys as `PinnableWideColumns`. Three overloads exist: single-CF, multi-CF, and attribute-group. All are fully implemented in `DBImpl` and route through the same batched infrastructure as `MultiGet()`, with `GetImplOptions.columns` set instead of `GetImplOptions.value`.

## Cross-Column-Family Snapshot Coordination

When `MultiGet()` spans multiple column families, `MultiCFSnapshot` coordinates SuperVersion acquisition. For `kPersistedTier`, it acquires the DB mutex to freeze SuperVersion changes across all column families, ensuring a consistent persisted view even while memtables are being sealed or flushed. This is a real cross-component behavior difference from single-CF MultiGet.
