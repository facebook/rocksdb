# Prefetching and Async I/O

**Files:** `file/file_prefetch_buffer.h`, `file/file_prefetch_buffer.cc`, `table/block_based/block_prefetcher.h`, `table/block_based/block_prefetcher.cc`, `table/block_based/block_based_table_iterator.h`, `table/block_based/block_based_table_iterator.cc`, `env/io_posix.cc`, `env/io_posix.h`, `include/rocksdb/file_system.h`, `include/rocksdb/options.h`, `include/rocksdb/table.h`

## Overview

RocksDB's read-path I/O optimization has four layers:

| Layer | Component | Role |
|-------|-----------|------|
| Configuration | `ReadOptions`, `BlockBasedTableOptions` | Set readahead size, async mode, adaptive behavior |
| Iterator integration | `BlockBasedTableIterator` | Detects sequential access, triggers prefetch, two-pass async seek |
| Prefetch orchestration | `BlockPrefetcher` | Decides when/how to create `FilePrefetchBuffer` |
| Buffer management + I/O | `FilePrefetchBuffer` | Multi-buffer prefetch, sync/async reads, readahead tuning |

## BlockBasedTableOptions for Readahead

These options control auto-readahead behavior at the SST level (see `BlockBasedTableOptions` in `include/rocksdb/table.h`):

| Option | Default | Purpose |
|--------|---------|---------|
| `initial_auto_readahead_size` | 8 KB | Starting readahead size for auto prefetching |
| `max_auto_readahead_size` | 256 KB | Maximum readahead size (doubles from initial up to this cap) |
| `num_file_reads_for_auto_readahead` | 2 | Number of sequential reads before auto prefetch enables |

## ReadOptions for Prefetching

| Option | Default | Effect |
|--------|---------|--------|
| `readahead_size` | 0 | If non-zero, overrides auto-readahead with a fixed readahead size |
| `async_io` | false | Enable async prefetching via `ReadAsync()` and `Poll()` |
| `adaptive_readahead` | false | Carry readahead state across files within the same level |
| `auto_readahead_size` | true | Auto-tune readahead based on block cache, upper bound, and prefix |
| `optimize_multiget_for_io` | true | Async reads across SST levels in MultiGet |

## BlockPrefetcher -- Prefetch Decision Layer

`BlockPrefetcher` (see `BlockPrefetcher` in `table/block_based/block_prefetcher.h`) is owned by `BlockBasedTableIterator` and decides when to create a `FilePrefetchBuffer`. The `PrefetchIfNeeded()` method handles three cases:

**Compaction reads:** Uses `compaction_readahead_size` (from `DBOptions`). First tries the FS-level `Prefetch()` syscall (Linux `readahead()`). If unsupported, falls back to internal `FilePrefetchBuffer` with a fixed readahead size.

**Explicit user readahead** (`ReadOptions::readahead_size > 0`): Immediately creates `FilePrefetchBuffer` with the specified fixed size.

**Implicit auto readahead** (the most common path):

Step 1: Track sequential access via `IsBlockSequential()` and `num_file_reads_` counter

Step 2: Wait for `num_file_reads_for_auto_readahead` sequential reads (default: 2) before enabling

Step 3: First try FS-level `Prefetch()` (POSIX `readahead()` syscall to populate page cache)

Step 4: If FS `Prefetch()` is unsupported, create a `FilePrefetchBuffer` internally

Step 5: Readahead doubles exponentially: `readahead_size = min(max_auto_readahead_size, readahead_size * 2)`

When `async_io` is enabled, `num_buffers` is set to 2 for double-buffering.

## FilePrefetchBuffer -- Core Prefetch Engine

`FilePrefetchBuffer` (see `FilePrefetchBuffer` in `file/file_prefetch_buffer.h`) manages a deque of `BufferInfo` objects (`bufs_` for active buffers, `free_bufs_` for reuse, `overlap_buf_` for data spanning two buffers).

**Synchronous mode (num_buffers == 1):**
- `TryReadFromCache()` checks if requested data is in the buffer
- On miss, `PrefetchInternal()` reads current data + readahead bytes in one synchronous `pread()`

**Asynchronous mode (num_buffers > 1, typically 2 -- double-buffering):**
- While the iterator reads from one buffer, the next buffer is filled asynchronously via `ReadAsync()`
- `PrefetchAsync()` submits an async read and returns `Status::TryAgain`
- On the next `TryReadFromCache()` call, `PollIfNeeded()` completes the async read, then serves data
- Completion callbacks update buffer state via `PrefetchAsyncCallback()`

### ReadaheadParams

`ReadaheadParams` (see `ReadaheadParams` in `file/file_prefetch_buffer.h`) controls the prefetch buffer:

| Field | Purpose |
|-------|---------|
| `initial_readahead_size` | Starting readahead size |
| `max_readahead_size` | Cap for exponential growth |
| `implicit_auto_readahead` | Whether RocksDB-initiated (not user-configured) |
| `num_file_reads` | Current sequential read count |
| `num_file_reads_for_auto_readahead` | Threshold to activate prefetch |
| `num_buffers` | 1 = synchronous, 2 = async double-buffer |

## Auto-Readahead for Scans

When iterating over SST files, auto-readahead activates after detecting sequential access:

Step 1: The first `num_file_reads_for_auto_readahead` reads (default: 2) are served without prefetching

Step 2: On the next sequential read, readahead enables with `initial_auto_readahead_size` (default: 8KB)

Step 3: Readahead doubles on each subsequent sequential read, up to `max_auto_readahead_size` (default: 256KB)

Step 4: When the iterator moves to a new file, readahead size resets (unless `adaptive_readahead` is true)

## Adaptive Readahead

When `ReadOptions::adaptive_readahead` is true:

- `GetReadaheadState()` saves `readahead_size` and `num_file_reads` from the `FilePrefetchBuffer` when leaving a file
- `SetReadaheadState()` restores them when entering the next SST file at the same level
- This avoids resetting readahead back to `initial_auto_readahead_size` at file boundaries, benefiting long scans spanning multiple SST files

## Auto Readahead Size (Cache-Aware Tuning)

When `ReadOptions::auto_readahead_size` is true (default) and block cache is enabled:

A `readaheadsize_cb` callback bound to `BlockBasedTableIterator::BlockCacheLookupForReadAheadSize()` is passed to `BlockPrefetcher`. This callback:

1. **Cache hit trimming**: Iterates ahead in the index, looks up blocks in the block cache, and trims readahead to avoid re-fetching blocks already cached
2. **Upper bound trimming**: Trims readahead so data beyond `iterate_upper_bound` is not prefetched
3. **Prefix trimming**: When `prefix_same_as_start` is true, trims readahead at the prefix boundary

`DecreaseReadAheadIfEligible()` reduces readahead size when prefetched blocks were already in cache (wasted prefetch).

## Async I/O Architecture

### FSRandomAccessFile Interface

Async I/O requires the file system to implement three methods (see `FSRandomAccessFile` in `include/rocksdb/file_system.h`):

| Method | Purpose |
|--------|---------|
| `ReadAsync()` | Submit non-blocking read, returns an `io_handle` for later polling |
| `Poll()` | Check/wait for async reads to complete, invoke callbacks |
| `AbortIO()` | Cancel outstanding async reads |

The file system must also set `FSSupportedOps::kAsyncIO` in `SupportedOps()`. The default implementation falls back to synchronous `Read()` with immediate callback.

### io_uring Implementation (Linux)

On Linux, `PosixRandomAccessFile` uses per-thread `io_uring` instances:

**ReadAsync** (see `PosixRandomAccessFile` in `env/io_posix.cc`):
1. Allocate a `Posix_IOHandle` storing callback, offset, length, and io_uring pointer
2. Get a submission queue entry via `io_uring_get_sqe()`
3. Prepare a `readv` operation: `io_uring_prep_readv(sqe, fd, &iov, 1, offset)`
4. Submit: `io_uring_submit()` -- returns immediately
5. Completion handled by `Poll()` later

**MultiRead** (synchronous scatter/gather, see `PosixRandomAccessFile` in `env/io_posix.cc`):
- Batches all read requests into io_uring SQEs
- Uses `io_uring_submit_and_wait()` for combined submit+wait
- Handles partial reads via resubmission
- Queue depth capped at `kIoUringDepth = 256`
- Falls back to serial `pread()` if io_uring init fails

**io_uring setup flags:**
- `IORING_SETUP_SINGLE_ISSUER`: Only the submitting thread accesses the ring
- `IORING_SETUP_DEFER_TASKRUN`: Task work runs in submitter's context

### Two-Pass Async Seek Pattern

When `ReadOptions::async_io` is true, `BlockBasedTableIterator::SeekImpl()` uses a two-pass pattern:

**Pass 1:** Calls `AsyncInitDataBlock(is_first_pass=true)`:
- Creates `FilePrefetchBuffer` with `num_buffers=2` (double-buffering)
- Calls `NewDataBlockIterator()` with `async_read=true`
- If data is not in cache/buffer, `FilePrefetchBuffer` submits async read via io_uring and returns `TryAgain`
- Sets `async_read_in_progress_ = true` and returns to caller

**Pass 2:** The caller calls `Seek()` again, which detects `async_read_in_progress_`:
- Calls `SeekSecondPass()` -> `AsyncInitDataBlock(is_first_pass=false)`
- `TryReadFromCache()` calls `PollIfNeeded()` to complete the async read
- Creates the data block iterator from the now-available buffer
- Completes the seek

During sequential `Next()` calls, the double-buffer scheme means the next data block is prefetched asynchronously while the current block is being iterated.

## MultiGet I/O Coalescing

`RetrieveMultipleBlocks()` in `block_based_table_reader_sync_and_async.h` optimizes disk reads for MultiGet:

Step 1: Identify data blocks needed for all keys in the batch

Step 2: Sort blocks by file offset

Step 3: Merge adjacent or near-adjacent block reads into single I/O requests

Step 4: Issue merged reads via `MultiRead()` (scatter/gather I/O using io_uring on Linux)

This reduces the number of system calls from O(keys) to O(distinct_read_regions).

## Performance Impact

| Scenario | Without Prefetch | With Prefetch |
|----------|-----------------|---------------|
| Sequential scan (HDD) | Random IOPS limited | Near-sequential throughput (use `db_bench` to measure) |
| Sequential scan (SSD) | Random read latency per block | Near-sequential throughput |
| MultiGet (10 keys, 3 levels) | 10+ read syscalls | 3-5 coalesced reads |
| Prefix scan (short) | May over-prefetch | Trimmed to prefix boundary |

Note: Over-aggressive prefetching can waste I/O bandwidth and pollute OS page cache. The auto-tuning features (`auto_readahead_size`, `adaptive_readahead`) help balance prefetch benefit against waste.
