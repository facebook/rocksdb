# File I/O and Blob Storage

## Overview

This document covers RocksDB's file I/O abstractions (Env, FileSystem, readers/writers), rate limiting, SST file management, and the integrated BlobDB system for separating large values from the LSM tree.

---

## 1. Env and FileSystem

**Files:** `include/rocksdb/env.h`, `include/rocksdb/file_system.h`

### Env

`Env` is the top-level OS abstraction bundling three categories:
1. **File system operations** -- creating/opening files, directories
2. **Thread management** -- thread pools with priority levels (BOTTOM, LOW, HIGH, USER)
3. **Miscellaneous** -- time, dynamic libraries

Modern pattern: `Env` composes a `FileSystem` (for storage) and `SystemClock` (for time). `Env::Default()` returns the singleton POSIX environment.

### FileSystem

The finer-grained replacement for `Env`'s file operations. Every API takes `IOOptions` and `IODebugContext*` for per-request control, and returns `IOStatus` (richer than `Status`, with retryable/scope metadata).

### File Abstractions

| Class | Purpose | Thread Safety |
|-------|---------|---------------|
| `FSSequentialFile` | Sequential reads (WAL replay, compaction input) | Single-thread |
| `FSRandomAccessFile` | Random reads (SST block reads) | Multi-thread safe |
| `FSWritableFile` | Sequential writes (SST building, WAL) | Single-thread |
| `FSRandomRWFile` | Random read/write | Implementation-dependent |
| `FSDirectory` | Directory handle for fsync | Implementation-dependent |

### IOOptions (Per-Request Hints)

| Field | Purpose |
|-------|---------|
| `timeout` | Operation timeout |
| `rate_limiter_priority` | IO_LOW / IO_MID / IO_HIGH / IO_USER |
| `type` | IOType: Data, Filter, Index, WAL, Manifest, etc. |
| `force_dir_fsync` | Force directory fsync (btrfs) |
| `verify_and_reconstruct_read` | Stronger integrity checking |

### FileOptions (Open-Time Settings)

| Field | Default | Purpose |
|-------|---------|---------|
| `use_direct_reads` | false | O_DIRECT for reads |
| `use_direct_writes` | false | O_DIRECT for writes |
| `use_mmap_reads` | false | Memory-mapped reads |
| `bytes_per_sync` | 0 | Incremental background sync interval |
| `writable_file_max_buffer_size` | 1MB | Max write buffer size |
| `temperature` | kUnknown | Hot/warm/cold hint |

### Optimization Hooks

FileSystem provides workload-specific `FileOptions` tuning:
- `OptimizeForLogRead/Write()` -- WAL access patterns
- `OptimizeForManifestRead/Write()` -- MANIFEST access
- `OptimizeForCompactionTableWrite/Read()` -- compaction I/O
- `OptimizeForBlobFileRead()` -- blob file access

---

## 2. WritableFileWriter

**Files:** `file/writable_file_writer.h`, `file/writable_file_writer.cc`

### What It Does

Internal wrapper over `FSWritableFile` that adds buffering, rate limiting, checksum computation, and event listener notifications. Used by WAL writer, SST builder, and blob file builder.

### Key Methods

| Method | Description |
|--------|-------------|
| `Append(opts, data, crc32c)` | Append data with optional pre-computed CRC for handoff checksums |
| `Flush(opts)` | Flush buffer to underlying file |
| `Sync(opts, use_fsync)` | Flush + sync to disk |
| `Close(opts)` | Flush + close |
| `GetFileSize()` | Logical size including unflushed buffer |

### Buffering Strategy

- Initial buffer: `min(65536, max_buffer_size)` with alignment from underlying file
- Data accumulates in `AlignedBuffer buf_` until `Flush()` is called
- For direct I/O, aligned sector writes are required
- `bytes_per_sync_` triggers incremental `RangeSync()` calls

### Rate Limiting

When `rate_limiter_` is set, write operations call `RateLimiter::Request()` before issuing I/O. The priority is resolved between file-level and per-operation settings.

---

## 3. RandomAccessFileReader

**Files:** `file/random_access_file_reader.h`, `file/random_access_file_reader.cc`

### What It Does

Internal wrapper over `FSRandomAccessFile` that adds rate limiting, statistics, event listener notifications, and direct I/O alignment handling. Used by SST reader and blob file reader.

### Key Methods

| Method | Description |
|--------|-------------|
| `Read(opts, offset, n, result, scratch, aligned_buf)` | Single positioned read |
| `MultiRead(opts, reqs, num_reqs, aligned_buf)` | Batch read (requires ascending, non-overlapping offsets) |
| `Prefetch(opts, offset, n)` | Readahead hint |
| `ReadAsync(req, opts, cb, cb_arg, ...)` | Async read with callback |

### Direct I/O Handling

For direct I/O, the reader internally allocates aligned buffers and copies to user buffers. The `aligned_buf` output parameter allows callers to take ownership of the aligned buffer for zero-copy usage.

---

## 4. RateLimiter

**Files:** `include/rocksdb/rate_limiter.h`

### What It Does

Token bucket rate limiter for I/O operations. Controls the rate of reads, writes, or both.

### Interface

| Method | Description |
|--------|-------------|
| `Request(bytes, pri, stats, op_type)` | Blocking: acquires tokens for the given byte count |
| `RequestToken(bytes, alignment, pri, stats, op_type)` | Higher-level: respects burst size and alignment |
| `SetBytesPerSecond(rate)` | Dynamically change rate limit |
| `GetSingleBurstBytes()` | Max bytes per single request |

### Modes

| Mode | Rate-Limited Operations |
|------|------------------------|
| `kWritesOnly` (default) | Only write I/O |
| `kReadsOnly` | Only read I/O |
| `kAllIo` | Both reads and writes |

### GenericRateLimiter

```cpp
NewGenericRateLimiter(rate_bytes_per_sec,
                      refill_period_us = 100000,
                      fairness = 10,
                      mode = kWritesOnly,
                      auto_tuned = false);
```

- `refill_period_us` (100ms default): token refill interval
- `fairness`: 1/fairness chance low-priority gets through when high-priority is queued
- `auto_tuned`: dynamic adjustment within `[rate/20, rate]` based on demand

---

## 5. SstFileManager and DeleteScheduler

**Files:** `file/sst_file_manager_impl.h`, `file/delete_scheduler.h`

### SstFileManagerImpl

Tracks all SST and blob files in the DB. Controls deletion rate and manages space accounting. Thread-safe.

| Method | Description |
|--------|-------------|
| `OnAddFile(path, size)` | Register new file |
| `OnDeleteFile(path)` | Unregister deleted file |
| `SetMaxAllowedSpaceUsage(max)` | When total exceeds max, writes fail |
| `EnoughRoomForCompaction(cfd, inputs)` | Check if compaction can proceed given reserved space |
| `ScheduleFileDeletion(path)` | Mark as trash, schedule background deletion |

### DeleteScheduler

Rate-limits file deletion to avoid I/O spikes:
- Files are renamed with `.trash` extension
- Background thread deletes them, sleeping proportionally to maintain `rate_bytes_per_sec`
- Rate = 0: immediate deletion (rate limiting disabled)
- `max_trash_db_ratio`: if trash/DB ratio exceeds threshold, new deletes are immediate
- **Trash buckets**: logical grouping for coordinated `WaitForEmptyTrashBucket()`

---

## 6. BlobDB (Integrated Blob Storage)

### Architecture

Large values are separated from the LSM tree into blob files. The LSM stores a `BlobIndex` (file number + offset) instead of the value. This reduces write amplification for large values since blob files are not rewritten during compaction.

```
LSM Tree                    Blob Files
+--------+                  +------------------+
| Key: A |                  | blob_file_001    |
| Val: BlobIndex(1, 100) ---|---> [blob A data] |
+--------+                  +------------------+
| Key: B |                  | blob_file_002    |
| Val: BlobIndex(2, 50) ----|---> [blob B data] |
+--------+                  +------------------+
```

### Blob File Format

**Files:** `db/blob/blob_log_format.h`

```
[BlobLogHeader: 30 bytes]
[BlobLogRecord 0: 32-byte header + key + value]
[BlobLogRecord 1: 32-byte header + key + value]
...
[BlobLogFooter: 32 bytes]   (present only when properly closed)
```

**BlobLogHeader (30 bytes)**:

| Field | Size | Description |
|-------|------|-------------|
| Magic number | 4B | `0x00248f37` |
| Version | 4B | kVersion1 = 1 |
| Column family ID | 4B | |
| Flags | 1B | has_ttl |
| Compression type | 1B | |
| Expiration range | 16B | min/max expiration (TTL) |

**BlobLogRecord (32-byte header + data)**:

| Field | Size | Description |
|-------|------|-------------|
| Key length | 8B | |
| Value length | 8B | Compressed length if compression used |
| Expiration | 8B | 0 if no TTL |
| Header CRC | 4B | CRC of header fields |
| Blob CRC | 4B | CRC of key + value |
| Key | variable | |
| Value | variable | |

**BlobLogFooter (32 bytes)**: Magic number, blob count, expiration range, footer CRC.

### BlobFileBuilder

**Files:** `db/blob/blob_file_builder.h`

Used during flush and compaction to write blob files.

| Method | Description |
|--------|-------------|
| `Add(key, value, blob_index*)` | Write blob value; outputs BlobIndex (file number + offset) for LSM |
| `Finish()` | Close current blob file, write footer |
| `Abandon(status)` | Abort on error |

Internal logic:
- Opens new blob file when none is open or current exceeds `blob_file_size_`
- Values below `min_blob_size_` stay inline in LSM (not separated)
- Optional compression via `blob_compression_type_`
- Can pre-populate blob cache (`PrepopulateBlobCache` option)

### BlobFileReader

**Files:** `db/blob/blob_file_reader.h`

Reads individual blob values from blob files.

| Method | Description |
|--------|-------------|
| `Create(opts, file_number, reader*)` | Static factory: opens file, validates header/footer |
| `GetBlob(opts, user_key, offset, value_size, compression, result*, bytes_read*)` | Read single blob at offset, verify CRC, decompress |
| `MultiGetBlob(opts, blob_reqs, bytes_read*)` | Batch read (offsets must be sorted ascending) |

### BlobSource

**Files:** `db/blob/blob_source.h`

Unified access to blobs from cache or storage. The caching gateway.

```
BlobSource
  +-- blob_cache_ (typed cache for BlobContents)
  +-- blob_file_cache_ (caches BlobFileReader instances)
```

| Method | Description |
|--------|-------------|
| `GetBlob(opts, user_key, file_number, offset, ...)` | Cache-first blob read; falls back to file |
| `MultiGetBlob(opts, blob_reqs, bytes_read*)` | Batch read across multiple blob files |

Cache keys: `OffsetableCacheKey(db_id, db_session_id, file_number).WithOffset(offset)` -- each blob uniquely keyed by file + offset.

### Blob Garbage Collection

During compaction, `CompactionIterator` can relocate blobs from old blob files:
- `GarbageCollectBlobIfNeeded()` reads the blob from the old file and writes to a new blob file via `BlobFileBuilder`
- Controlled by `enable_blob_garbage_collection` and `blob_garbage_collection_age_cutoff`
- `VersionStorageInfo::ComputeFilesMarkedForForcedBlobGC()` identifies SST files referencing old blob files

---

## Key Invariants

| Invariant | Details |
|-----------|---------|
| WritableFileWriter buffers are aligned | For direct I/O, writes must be sector-aligned |
| RateLimiter request <= burst size | Caller must ensure bytes <= GetSingleBurstBytes() |
| DeleteScheduler preserves ordering | Trash files deleted oldest-first within a bucket |
| BlobIndex offsets point to value start | Offset = header start + header_size + key_size |
| Blob file CRC covers key + value | Both key and value are integrity-protected |
| Blob files are append-only | Once written, blob records are never modified |

## Interactions With Other Components

- **SST Format** (see [sst_table_format.md](sst_table_format.md)): `BlockBasedTableBuilder` uses `WritableFileWriter`; `BlockBasedTable` uses `RandomAccessFileReader`.
- **Write Path** (see [write_path.md](write_path.md)): WAL writer uses `WritableFileWriter` with rate limiting.
- **Compaction** (see [compaction.md](compaction.md)): `CompactionJob` uses `BlobFileBuilder` for blob separation and `BlobFileReader` for blob GC.
- **Read Path** (see [flush_and_read_path.md](flush_and_read_path.md)): `BlobSource` resolves blob references during point lookups and iteration.
- **Version Management** (see [version_management.md](version_management.md)): `VersionEdit` tracks blob file additions and garbage.
- **Cache** (see [cache.md](cache.md)): `BlobSource` uses the block cache for blob caching.
