# File I/O Infrastructure

**Overview**: RocksDB's File I/O layer abstracts all operating system file operations, providing buffered and direct I/O modes, rate limiting, prefetching, and cross-platform support. This document covers the complete I/O stack from high-level abstractions to low-level file operations.

**File**: `docs/components/file_io.md`

---

## Table of Contents

1. [Env and FileSystem Abstraction](#1-env-and-filesystem-abstraction)
2. [WritableFileWriter](#2-writablefilewriter)
3. [RandomAccessFileReader](#3-randomaccessfilereader)
4. [SequentialFileReader](#4-sequentialfilereader)
5. [FilePrefetchBuffer](#5-fileprefetchbuffer)
6. [RateLimiter](#6-ratelimiter)
7. [File Metadata](#7-file-metadata)
8. [Directory Operations](#8-directory-operations)
9. [Composite Env Pattern](#9-composite-env-pattern)
10. [Error Handling](#10-error-handling)

---

## 1. Env and FileSystem Abstraction

RocksDB uses two abstraction layers for OS interaction:

- **`Env`** (`include/rocksdb/env.h`): Legacy monolithic interface for filesystem, threading, and time operations
- **`FileSystem`** (`include/rocksdb/file_system.h`): Modern interface separating filesystem operations from other OS concerns

### Architecture

```
RocksDB Core
  |
File Readers/Writers
  - WritableFileWriter, RandomAccessFileReader, SequentialFileReader
  |
FileSystem Layer
  - FSWritableFile, FSRandomAccessFile, FSSequentialFile
  |
Platform-Specific Implementation
  - PosixFileSystem, WinFileSystem, MockFileSystem
```

### FileSystem Interface

**File Creation APIs**:
```cpp
// file_system.h:422-458
IOStatus NewSequentialFile(const std::string& fname,
                          const FileOptions& file_opts,
                          std::unique_ptr<FSSequentialFile>* result,
                          IODebugContext* dbg);

IOStatus NewRandomAccessFile(const std::string& fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSRandomAccessFile>* result,
                             IODebugContext* dbg);

IOStatus NewWritableFile(const std::string& fname,
                        const FileOptions& file_opts,
                        std::unique_ptr<FSWritableFile>* result,
                        IODebugContext* dbg);
```

### EnvOptions and FileOptions

**`EnvOptions`** (`env.h:76-141`) configures I/O behavior:
- `use_mmap_reads/writes`: Memory-mapped I/O (not recommended for 32-bit)
- `use_direct_reads/writes`: O_DIRECT mode (bypass page cache)
- `bytes_per_sync`: Background incremental sync (default: 0 = disabled)
- `strict_bytes_per_sync`: Wait for sync_file_range completion (default: false)
- `compaction_readahead_size`: Readahead for compaction reads
- `writable_file_max_buffer_size`: Write buffer size (default: 1MB)
- `rate_limiter`: I/O rate limiter instance

**⚠️ INVARIANT**: Direct writes require `writable_file_max_buffer_size > 0` (checked in `writable_file_writer.cc:51-54`).

**⚠️ INVARIANT**: All FileSystem implementations must be safe for concurrent access from multiple threads without external synchronization (`file_system.h:11-12`).

### IOOptions

**`IOOptions`** (`file_system.h:100-157`) are per-request hints:
- `timeout`: Operation timeout in microseconds
- `prio`: I/O priority (deprecated, use `rate_limiter_priority`)
- `type`: IOType (kData, kFilter, kIndex, kMetadata, kWAL, kManifest, kLog, kUnknown, kInvalid)
- `property_bag`: Opaque option map for custom FileSystem contracts
- `force_dir_fsync`: Force directory fsync (field exists but not consulted by current implementations; `PosixDirectory::FsyncWithDirOptions()` uses `DirFsyncOptions::reason` instead)
- `do_not_recurse`: Skip recursing through subdirectories in `GetChildren`
- `verify_and_reconstruct_read`: Request data reconstruction from redundancy on error
- `io_activity`: Activity classification for statistics

### Supported Operations

FileSystem capabilities are queried via `FSSupportedOps` enum (`file_system.h:86-94`):
- `kAsyncIO`: Asynchronous read support
- `kFSBuffer`: File system can hand off allocated buffers to avoid copies
- `kVerifyAndReconstructRead`: Data integrity with reconstruction
- `kFSPrefetch`: Prefetch operation support

---

## 2. WritableFileWriter

**`WritableFileWriter`** (`file/writable_file_writer.h`) is a wrapper over `FSWritableFile` that handles buffered/direct writes, rate limiting, sync, and statistics.

### Key Responsibilities

1. **Buffered and Direct Writes**: Manages internal buffer (`AlignedBuffer buf_`) for batching writes
2. **Rate Limiting**: Integrates with `RateLimiter` for compaction/flush I/O throttling
3. **Alignment**: Ensures writes satisfy alignment requirements for direct I/O
4. **Sync Operations**: Flush, RangeSync, Sync for durability
5. **Checksum**: Optional file checksum calculation
6. **Statistics and Listeners**: Updates I/O stats and notifies event listeners

### Internal State

```cpp
// writable_file_writer.h:140-170
std::string file_name_;
FSWritableFilePtr writable_file_;       // Underlying file handle
AlignedBuffer buf_;                      // Write buffer
size_t max_buffer_size_;                 // Max buffer capacity
std::atomic<uint64_t> filesize_;         // Logical file size
std::atomic<uint64_t> flushed_size_;     // Flushed to FSWritableFile
uint64_t next_write_offset_;             // For direct I/O alignment
bool pending_sync_;                      // Sync needed flag
std::atomic<bool> seen_error_;           // Error state
uint64_t last_sync_size_;                // Tracking for bytes_per_sync
uint64_t bytes_per_sync_;                // Incremental sync threshold
RateLimiter* rate_limiter_;              // Rate limiting
std::unique_ptr<FileChecksumGenerator> checksum_generator_; // Optional file checksum (line 164)
Temperature temperature_;                // File temperature hint (line 169)
```

**⚠️ INVARIANT**: `filesize_` is the total bytes written (including unflushed buffer). `flushed_size_` is bytes actually sent to `FSWritableFile` (`writable_file_writer.h:274-285`).

**⚠️ INVARIANT**: For direct I/O, `next_write_offset_` tracks the file offset for the next aligned write. Unaligned writes trigger buffer refill (`writable_file_writer.h:152`).

### Write Flow

```
User calls Append(data)
    ↓
Calculate checksum (if enabled)
    ↓
Try to fit data in buffer
    ↓
Buffer full? → Flush() → Write buffer contents
    ↓
Append to buffer
    ↓
Update filesize_
```

**Append** (`writable_file_writer.cc:64-200`):
1. Check `seen_error_` flag; return error if previous operation failed
2. Calculate file checksum (via `UpdateFileChecksum`)
3. Call `writable_file_->PrepareWrite()` for preparation
4. Enlarge buffer if needed (up to `max_buffer_size_`)
5. For buffered I/O: flush if buffer can't fit data, then either append to buffer or write directly (bypassing buffer only when buffer is empty and payload exceeds buffer capacity)
6. For direct I/O: always accumulate data in buffer, flushing when full
7. Update `filesize_` atomically

**Flush** (`writable_file_writer.cc` - writes buffer contents):
- For buffered I/O: calls `WriteBuffered()` or `WriteBufferedWithChecksum()`
- For direct I/O: calls `WriteDirect()` or `WriteDirectWithChecksum()`
- Updates `flushed_size_` after successful flush
- For buffered I/O: clears buffer after flush
- For direct I/O: retains partial tail via `buf_.RefitTail()` (partial blocks are re-written with the next batch)

**Sync** (`writable_file_writer.h:266`):
- Signature: `IOStatus Sync(const IOOptions& opts, bool use_fsync)`
- Flushes buffer first
- Calls `SyncInternal()` which invokes `writable_file_->Sync()` or `Fsync()` based on the `use_fsync` parameter
- Clears `pending_sync_` after successful sync

**SyncWithoutFlush** (`writable_file_writer.h:268-271`):
- Syncs only data already `Flush()`ed, without flushing the buffer
- Safe to call concurrently with `Append()` and `Flush()` only if `writable_file_->IsSyncThreadSafe()` returns true
- Returns `NotSupported` if `IsSyncThreadSafe()` is false

**RangeSync** (`writable_file_writer.h:366`):
- Used for `bytes_per_sync` incremental sync
- Delegates to `writable_file_->RangeSync()`
- Default `FSWritableFile::RangeSync()` is a no-op unless `strict_bytes_per_sync_` is true, in which case it calls `Sync()`
- On POSIX with `sync_file_range()` available: uses `SYNC_FILE_RANGE_WRITE` in normal mode; in strict mode uses `SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE`
- Falls back to `FSWritableFile::RangeSync()` default when `sync_file_range()` is unavailable

**⚠️ INVARIANT**: Once `seen_error_` is set, all subsequent operations return error until `reset_seen_error()` is called (`writable_file_writer.h:308-327`).

### Data Verification (Checksum Handoff)

WritableFileWriter supports checksum handoff to the underlying `FSWritableFile` via `DataVerificationInfo` (`file_system.h:1088-1093`):
- When `perform_data_verification_` is true, CRC32c checksums are computed for each write and passed to `FSWritableFile::Append()` / `PositionedAppend()` via `DataVerificationInfo`
- When `buffered_data_with_checksum_` is additionally true, checksums are accumulated across buffer fills and verified as a batch
- The underlying `FSWritableFile` can verify the checksum against the data received, detecting corruption in transit

### Rate Limiter Integration

WritableFileWriter requests tokens from the rate limiter before writing:

```cpp
// DecideRateLimiterPriority (writable_file_writer.h:349-351)
// Determines priority: operation-specific or file-level default
Env::IOPriority DecideRateLimiterPriority(
    Env::IOPriority writable_file_io_priority,
    Env::IOPriority op_rate_limiter_priority);
```

### Direct I/O Alignment

**Direct I/O** bypasses the OS page cache, requiring:
- **Read/write offsets** must be aligned to block size (typically 512 bytes or 4KB)
- **Buffer addresses** must be aligned (handled by `AlignedBuffer`)
- **I/O sizes** must be multiples of alignment

**`WriteDirect()`** (`writable_file_writer.h:356-358`):
1. Pads buffer to alignment boundary
2. Writes entire buffer to file at `next_write_offset_` via `PositionedAppend()`
3. Retains partial tail (leftover data past the last page boundary) via `buf_.RefitTail()`, moving it to the start of the buffer for re-writing with the next batch
4. Advances `next_write_offset_` by the whole-page portion only

**⚠️ INVARIANT**: For direct I/O, writes are always aligned. Partial blocks at the end are retained in the buffer and re-written with the next batch (`writable_file_writer.h:150-152`).

---

## 3. RandomAccessFileReader

**`RandomAccessFileReader`** (`file/random_access_file_reader.h`) wraps `FSRandomAccessFile` for random read operations with buffering, rate limiting, and statistics.

### Key Responsibilities

1. **Random Reads**: Single and multi-read operations
2. **Direct I/O Alignment**: Handles buffer alignment for direct reads
3. **Rate Limiting**: Compaction read throttling
4. **Statistics**: Tracks read latency and throughput
5. **Async Reads**: Asynchronous read support with callbacks

### Read Operations

**`Read()`** (`random_access_file_reader.h:166-168`):
```cpp
IOStatus Read(const IOOptions& opts, uint64_t offset, size_t n,
              Slice* result, char* scratch, AlignedBuf* aligned_buf,
              IODebugContext* dbg = nullptr) const;
```

**Parameters**:
- `offset`, `n`: Read range
- `scratch`: Caller-provided buffer (used in non-direct I/O)
- `aligned_buf`: For direct I/O, receives ownership of aligned buffer
- `result`: Output slice pointing to data

**Behavior**:
- **Non-direct I/O**: Uses mmap or reads into `scratch`
- **Direct I/O**: Allocates aligned buffer internally; if `aligned_buf != nullptr`, transfers ownership; otherwise copies to `scratch`

**⚠️ INVARIANT**: In direct I/O mode with `aligned_buf` provided, `scratch` can be null. The aligned buffer is returned via `aligned_buf`, and `result` points into it (`random_access_file_reader.h:156-165`).

**`MultiRead()`** (`random_access_file_reader.h:175-177`):
```cpp
IOStatus MultiRead(const IOOptions& opts, FSReadRequest* reqs,
                   size_t num_reqs, AlignedBuf* aligned_buf,
                   IODebugContext* dbg = nullptr) const;
```

**Requirements** (`random_access_file_reader.h:170-174`):
- `num_reqs > 0`
- Requests must NOT overlap
- Offsets must be increasing
- In non-direct I/O mode, `aligned_buf` should be null
- In direct I/O mode, `aligned_buf` stores the aligned buffer allocated inside `MultiRead`; result Slices in reqs refer to `aligned_buf`

**Merging and Alignment**: The implementation uses helper functions:
- `Align()` (`random_access_file_reader.h:31`): Aligns read request to buffer alignment
- `TryMerge()` (`random_access_file_reader.h:39`): Merges overlapping requests

### Async Reads

**`ReadAsync()`** (`random_access_file_reader.h:193-196`):
Submits asynchronous read request with callback:

```cpp
IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                   std::function<void(FSReadRequest&, void*)> cb,
                   void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                   AlignedBuf* aligned_buf, IODebugContext* dbg = nullptr);
```

The callback is invoked when I/O completes:

```cpp
void ReadAsyncCallback(FSReadRequest& req, void* cb_arg);
```

**`ReadAsyncInfo`** (`random_access_file_reader.h:92-117`) stores async request state:
- Callback function and argument
- Start time for latency tracking
- Original user parameters (for direct I/O buffer handling)
- Internal aligned buffer (for direct I/O)

---

## 4. SequentialFileReader

**`SequentialFileReader`** (`file/sequence_file_reader.h`) wraps `FSSequentialFile` for sequential reads (used in WAL replay, MANIFEST reading, and log reading).

### Key Responsibilities

1. **Sequential Reads**: Forward-only read operations
2. **Readahead**: Optional readahead wrapper for streaming reads
3. **Rate Limiting**: Integrated support for throttling reads via `RateLimiter*` (see TODOs at `sequence_file_reader.h:66-67,82-83` about migrating all call sites to pass rate limiter)
4. **Statistics**: Read tracking

### Internal State

```cpp
// sequence_file_reader.h:54-59
std::string file_name_;
FSSequentialFilePtr file_;
std::atomic<size_t> offset_{0};  // Current read offset
std::vector<std::shared_ptr<EventListener>> listeners_;
RateLimiter* rate_limiter_;
bool verify_and_reconstruct_read_;
```

**⚠️ INVARIANT**: `offset_` tracking is not fully reliable: in direct I/O mode, `offset_` is incremented by `n` before the read completes (`sequence_file_reader.cc:53`); in non-direct I/O, `Skip()` delegates to `file_->Skip(n)` without updating `offset_` (`sequence_file_reader.cc:144-149`). Non-direct `Read()` updates `offset_` only inside the notification path.

### Read Operations

**`Read()`** (`sequence_file_reader.h:110-111`):
```cpp
IOStatus Read(size_t n, Slice* result, char* scratch,
              Env::IOPriority rate_limiter_priority);
```

Reads up to `n` bytes into `scratch`, returning actual data via `result`.

**Rate limiter charging** (`sequence_file_reader.h:100-106`):
- Charges rate limiter for `n` bytes even if fewer bytes are read (e.g., at EOF)
- Caller should cap `n` to file size to avoid overcharging
- `Env::IO_TOTAL` priority bypasses rate limiting

**`Skip()`** (`sequence_file_reader.h:113`):
Advances position by `n` bytes without reading data. In direct I/O mode, updates `offset_` directly. In non-direct mode, delegates to `file_->Skip(n)` without updating `offset_`.

### Readahead Wrapper

**`NewReadaheadSequentialFile()`** (`sequence_file_reader.h:124-125`):
Creates a wrapper that prefetches additional data with every read. Returns the original file unchanged when `readahead_size <= GetRequiredBufferAlignment()` (i.e., readahead too small to be useful). Useful for streaming scenarios like log replay.

---

## 5. FilePrefetchBuffer

**`FilePrefetchBuffer`** (`file/file_prefetch_buffer.h`) is a smart buffer that implements adaptive readahead for sequential and semi-random access patterns.

**⚠️ INVARIANT**: `FilePrefetchBuffer` is incompatible with prefetching from mmap-backed `RandomAccessFileReader`s. Callers commonly gate it with `!use_mmap_reads` for the `enable` parameter (`file_prefetch_buffer.h:184-186`).

### Architecture

**Multi-Buffer Design** (`file_prefetch_buffer.h:141-164`):
- `bufs_`: Buffers containing prefetched data (deque of `BufferInfo*`)
- `free_bufs_`: Available buffers with no data (deque of `BufferInfo*`)
- `overlap_buf_`: Temporary buffer for handling data split across multiple buffers

```
FilePrefetchBuffer (num_buffers_ = 3)
  bufs_:        [Buf0: 0-64KB]  [Buf1: 64KB-128KB]
  free_bufs_:   [Buf2: empty]
  overlap_buf_: (temporary, used when request spans bufs)
```

### BufferInfo

**`BufferInfo`** (`file_prefetch_buffer.h:63-132`) represents a single buffer:
```cpp
AlignedBuffer buffer_;              // Actual data
uint64_t offset_;                   // File offset of buffer start
size_t async_req_len_;              // Requested async read length
bool async_read_in_progress_;       // Async I/O in flight
void* io_handle_;                   // Platform-specific async I/O handle
IOHandleDeleter del_fn_;            // Handle cleanup function
uint64_t initial_end_offset_;       // For readahead size tuning
```

**⚠️ INVARIANT**: When `async_read_in_progress_ == true`, the buffer contents are NOT valid until the async I/O completes. Only main thread can set this flag; callback can update buffer but not the flag (`file_prefetch_buffer.h:78-80`).

### Readahead Parameters

**`ReadaheadParams`** (`file_prefetch_buffer.h:37-61`):
- `initial_readahead_size`: Starting readahead amount
- `max_readahead_size`: Maximum readahead (typically multiple of initial)
- `implicit_auto_readahead`: RocksDB automatically enables readahead after detecting sequential access
- `num_file_reads_for_auto_readahead`: Number of sequential reads before enabling (typically 2)
- `num_buffers`: Number of prefetch buffers (1 = synchronous, >1 = asynchronous)

**Exponential Readahead**: When sequential access is detected, readahead size doubles up to `max_readahead_size`.

**Adaptive Decrease** (`file_prefetch_buffer.h:367-395`):
If prefetched data is not used (accessed via cache instead), readahead size is decreased by `DEFAULT_DECREMENT` (8KB) to avoid wasted prefetching.

### Read Paths

**1. Synchronous Prefetch** (`num_buffers_ == 1`):

```
TryReadFromCache(offset, n)
    ↓
Data in buffer? → Return data
    ↓ No
IsEligibleForPrefetch? (sequential, enough reads)
    ↓ Yes
PrefetchInternal() → Read(offset, n + readahead_size)
    ↓
Return requested data
```

**2. Asynchronous Prefetch** (`num_buffers_ > 1`):

```
PrefetchAsync(offset, n)
    ↓
Data in buffer? → Return Status::OK + data
    ↓ No
Submit async read → ReadAsync()
    ↓
Return Status::TryAgain

[Later] TryReadFromCache polls:
    ↓
Poll() → Check async I/O completion
    ↓ Complete
Return data
```

**⚠️ INVARIANT**: For `num_buffers_ > 1`, data may span two buffers. In this case, data is copied to `overlap_buf_` and returned from there (`file_prefetch_buffer.h:162-164`).

### Key Operations

**`Prefetch()`** (`file_prefetch_buffer.h:312-313`):
Explicitly loads data into buffer. User-initiated, not adaptive.

**`PrefetchAsync()`** (`file_prefetch_buffer.h:325-326`):
Submits async read request. Returns `Status::OK` if data already in buffer, `Status::TryAgain` if async request submitted.

**`TryReadFromCache()`** (`file_prefetch_buffer.h:341-343`):
Main API for retrieving data. Handles:
- Returning data if in buffer
- Triggering prefetch if sequential access detected
- Polling async I/O
- Updating readahead size based on access pattern

### Async I/O Lifecycle

1. **Submit**: `ReadAsync()` calls `reader->ReadAsync()`, stores `io_handle_` in `BufferInfo`
2. **Fallback**: If `ReadAsync()` returns `NotSupported` (e.g., io_uring failed to initialize), transparently falls back to a synchronous `Read()` so the buffer is populated inline (`file_prefetch_buffer.cc:163-175`)
3. **Poll**: `PollIfNeeded()` calls `fs_->Poll()` to check completion
3. **Complete**: Callback `PrefetchAsyncCallback()` is invoked, buffer is filled
4. **Cleanup**: `DestroyAndClearIOHandle()` releases `io_handle_`

**Abort Handling** (`file_prefetch_buffer.h:234-246`):
Destructor aborts pending async I/O via `fs_->AbortIO()` to avoid use-after-free.

**⚠️ INVARIANT**: `io_handle_` must be released via `del_fn_` before destruction. The deleter is provided by the FileSystem implementation (`file_prefetch_buffer.h:545-552`).

### FS Buffer Reuse Optimization

**`UseFSBuffer()`** (`file_prefetch_buffer.h:505-510`):
When FileSystem supports `kFSBuffer`, avoid copying by reusing the buffer allocated by the FS. This optimization is only enabled when all of the following conditions are met (`file_prefetch_buffer.h:505-509`):
- `reader->file() != nullptr`
- `!reader->use_direct_io()` (not direct I/O)
- `fs_ != nullptr`
- FileSystem supports `kFSBuffer`
- `num_buffers_ == 1` (synchronous mode only)

```cpp
// file_prefetch_buffer.h:523-543
IOStatus FSBufferDirectRead(RandomAccessFileReader* reader, ...) {
  FSReadRequest read_req;
  read_req.scratch = nullptr;  // FS allocates buffer
  IOStatus s = reader->MultiRead(..., &read_req, ...);
  buf->buffer_.SetBuffer(read_req.result, std::move(read_req.fs_scratch));
  // No copy! Buffer ownership transferred
}
```

---

## 6. RateLimiter

**`RateLimiter`** (`include/rocksdb/rate_limiter.h`) controls I/O rate for compaction and flush to prevent interference with foreground operations.

### API

```cpp
class RateLimiter {
  enum class OpType { kRead, kWrite };
  enum class Mode { kReadsOnly, kWritesOnly, kAllIo };

  void SetBytesPerSecond(int64_t bytes_per_second);
  void Request(int64_t bytes, Env::IOPriority pri, Statistics* stats,
               OpType op_type);
  // Caps bytes to GetSingleBurstBytes(), adjusts for alignment, then calls Request().
  // This is the API actually used by WritableFileWriter and RandomAccessFileReader.
  size_t RequestToken(size_t bytes, size_t alignment,
                      Env::IOPriority io_priority, Statistics* stats,
                      OpType op_type);
  int64_t GetSingleBurstBytes() const;
  int64_t GetTotalBytesThrough(Env::IOPriority pri) const;
};
```

### GenericRateLimiter Implementation

**`GenericRateLimiter`** (`util/rate_limiter_impl.h`) uses token bucket algorithm:

**State** (`rate_limiter_impl.h:123-148`):
```cpp
std::atomic<int64_t> rate_bytes_per_sec_;      // Target rate
std::atomic<int64_t> refill_bytes_per_period_; // Tokens per refill
int64_t available_bytes_;                      // Current tokens
int64_t next_refill_us_;                       // Next refill time
std::deque<Req*> queue_[Env::IO_TOTAL];        // Per-priority queues
int32_t fairness_;                             // Fairness parameter (line 143)
bool auto_tuned_;                              // Auto-tuning enabled (line 150)
```

**Algorithm**:
- Tokens are added every `refill_period_us` (default: 100ms)
- Each I/O request consumes tokens proportional to bytes
- If tokens unavailable, request blocks until refill

**Fairness** (field at line 143):
`Env::IO_USER` is always iterated first. For the remaining priorities (HIGH, MID, LOW), iteration order is randomized using `rnd_.OneIn(fairness_)` to probabilistically prevent starvation (`rate_limiter.cc:234-270`).

**⚠️ INVARIANT**: `Request()` asserts `bytes <= GetSingleBurstBytes()` in debug builds only (`rate_limiter.cc:124`). Callers should use `RequestToken()` which caps the request to `GetSingleBurstBytes()` and adjusts for alignment before calling `Request()` (`rate_limiter.cc:20-35`).

### Rate Limiter Modes

- **`kWritesOnly`**: Only throttle writes (flush/compaction output) - **default**
- **`kReadsOnly`**: Only throttle reads (compaction input)
- **`kAllIo`**: Throttle both reads and writes

**Usage Example**:
```cpp
// Create rate limiter: 10 MB/s, 100ms refill period, fairness=10
std::shared_ptr<RateLimiter> rate_limiter(
    NewGenericRateLimiter(10 << 20, 100 * 1000, 10));

DBOptions options;
options.rate_limiter = rate_limiter;
```

---

## 7. File Metadata

### FileDescriptor

**`FileDescriptor`** (`db/version_edit.h:183-199`) identifies an SST file:
```cpp
struct FileDescriptor {
  mutable PinnedTableReader pinned_reader;  // Cached table reader
  uint64_t packed_number_and_path_id;       // File number + path ID
  uint64_t file_size;                       // Size in bytes
  SequenceNumber smallest_seqno;            // Min sequence number
  SequenceNumber largest_seqno;             // Max sequence number
};
```

**File Number** (`version_edit.h:126`):
- 62-bit file number (low bits, masked by `kFileNumberMask = 0x3FFFFFFFFFFFFFFF`)
- 2-bit path ID (high bits)
- Encoded via `PackFileNumberAndPathId()` (`version_edit.h:135`)

**⚠️ INVARIANT**: File numbers are globally unique within a RocksDB instance. They are assigned via `VersionSet::next_file_number_` and never reused.

### File Naming

SST files are named: `{file_number:06d}.sst` (zero-padded to 6 digits, e.g., `000042.sst`). The `path_id` selects which directory from `db_paths`/`cf_paths` to place the file in, but is NOT encoded in the filename (`file/filename.cc:66-75,141-151`).

BLOB files: `{file_number:06d}.blob` (same zero-padded format)

### File Paths

RocksDB supports multiple data directories via `DBOptions::db_paths` and per-column-family `ColumnFamilyOptions::cf_paths`:
```cpp
DBOptions options;
options.db_paths = {{"/fast_ssd", 100GB}, {"/slow_hdd", 1TB}};
```

`cf_paths` controls SST placement per column family and falls back to `db_paths` when empty (`options.h:320-332`).

Files are placed based on level and size policies.

---

## 8. Directory Operations

### Directory Fsync

After creating a new file, RocksDB fsyncs the parent directory to ensure the directory entry is durable (required for crash consistency on most filesystems).

**FSDirectory** interface (`file_system.h:1437-1469`):
```cpp
class FSDirectory {
 public:
  virtual IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) = 0;
  virtual IOStatus FsyncWithDirOptions(
      const IOOptions& options, IODebugContext* dbg,
      const DirFsyncOptions& dir_fsync_options);
  virtual IOStatus Close(const IOOptions& options, IODebugContext* dbg);
};
```

**Usage Pattern**:
1. Create file (e.g., new SST)
2. Write and sync file
3. Close file
4. **Fsync directory** (ensures directory entry is persistent)

**`DirFsyncOptions`** (`file_system.h:160-177`):
Directory fsync behavior is controlled by `DirFsyncOptions::reason`:
- `kNewFileSynced`: New file created and synced
- `kFileRenamed`: File was renamed (e.g., CURRENT, IDENTITY, options files)
- `kDirRenamed`: Directory was renamed
- `kFileDeleted`: File was deleted
- `kDefault`: Default behavior

**Btrfs-specific optimizations** (`env/io_posix.cc:1946-1970`):
- `kNewFileSynced`: Directory fsync skipped (not needed on btrfs)
- `kFileRenamed`: Instead of dir fsync, opens and fsyncs the renamed file directly
- Other reasons: Fall back to standard directory fsync

**`force_dir_fsync`** (`file_system.h:125-127`):
The `IOOptions::force_dir_fsync` field exists but is not consulted by current `PosixDirectory` implementation; `DirFsyncOptions::reason` is used instead.

**⚠️ INVARIANT**: Directory fsync must be performed AFTER file creation and sync for crash consistency. Failure to do so may result in the file "disappearing" after crash, even if the file data was synced.

---

## 9. Composite Env Pattern

**Problem**: Legacy code uses monolithic `Env` API, but modern code uses separate `Env` (for threading/time) and `FileSystem` (for file I/O).

**Solution**: **`CompositeEnv`** (`env/composite_env_wrapper.h:21-27`) delegates file ops to `FileSystem` and time ops to `SystemClock`. Subclasses provide thread-management APIs. **`CompositeEnvWrapper`** extends this by forwarding thread-management methods to a target `Env` (`composite_env_wrapper.h:327-340`).

### Architecture

```
Legacy RocksDB Code (uses Env interface)
  |
CompositeEnv
  - Forwards file ops -> FileSystem
  - Forwards time ops -> SystemClock
  - Thread management -> subclass or target Env
  |
  +---------------------+------------------------+
  |                                               |
  FileSystem                          SystemClock / Env
  (file operations)                   (time, threading)
```

### Configuration

There is no public `Options::file_system` field. The public API is `DBOptions::env` (`options.h:716-719`). To use a custom `FileSystem`, compose it into an `Env` via `NewCompositeEnv()` (`env.h:2064-2066`):

```cpp
auto fs = std::make_shared<MyCustomFileSystem>();
std::unique_ptr<Env> env = NewCompositeEnv(fs);
DBOptions options;
options.env = env.get();
```

### Wrapper Pattern

**Wrapper Classes** (`env/composite_env.cc:22-100`):
- `CompositeSequentialFileWrapper`: Wraps `FSSequentialFile` as `SequentialFile`
- `CompositeRandomAccessFileWrapper`: Wraps `FSRandomAccessFile` as `RandomAccessFile`
- `CompositeWritableFileWrapper`: Wraps `FSWritableFile` as `WritableFile`

**Forwarding** (`composite_env.cc:28-46` example):
```cpp
class CompositeSequentialFileWrapper : public SequentialFile {
  std::unique_ptr<FSSequentialFile> target_;

  Status Read(size_t n, Slice* result, char* scratch) override {
    IOOptions io_opts;
    IODebugContext dbg;
    return target_->Read(n, io_opts, result, scratch, &dbg);
    // Converts Env API → FileSystem API
  }
};
```

**⚠️ INVARIANT**: CompositeEnv allows users to specify `Options::env` (for threading) and `Options::file_system` separately. If only `env` is set, it uses env's internal filesystem (`composite_env_wrapper.h:11-20`).

---

## 10. Error Handling

### IOStatus

**`IOStatus`** (`include/rocksdb/io_status.h`) extends `Status` with I/O-specific error information:

```cpp
class IOStatus : public Status {
  enum IOErrorScope {
    kIOErrorScopeFileSystem,  // Filesystem-level error
    kIOErrorScopeFile,        // File-level error
    kIOErrorScopeRange,       // Range-level error (e.g., single block)
  };

  void SetRetryable(bool retryable);    // Can retry operation
  void SetDataLoss(bool data_loss);     // Data was lost
  void SetScope(IOErrorScope scope);    // Error scope

  bool GetRetryable() const;
  bool GetDataLoss() const;
  IOErrorScope GetScope() const;
};
```

### Error Categories

**Common Errors**:
- `IOStatus::IOError()`: Generic I/O error (disk full, permission denied, etc.)
- `IOStatus::NoSpace()`: Out of disk space (subcode: `kNoSpace`)
- `IOStatus::PathNotFound()`: File/directory not found (subcode: `kPathNotFound`)
- `IOStatus::IOFenced()`: I/O fenced (for distributed systems, subcode: `kIOFenced`)
- `IOStatus::Corruption()`: Data corruption detected
- `IOStatus::TimedOut()`: Operation timed out

**Retryable Errors** (`io_status.h:52, 58`):
Some errors can be retried (e.g., temporary network failure). Set via `SetRetryable(true)`.

**Data Loss** (`io_status.h:53, 59`):
Indicates unrecoverable data loss. Set via `SetDataLoss(true)`.

**Error Scope** (`io_status.h:33-37, 54, 60`):
- **kIOErrorScopeFileSystem**: Affects entire filesystem (e.g., disk failure)
- **kIOErrorScopeFile**: Affects single file (e.g., corrupted file)
- **kIOErrorScopeRange**: Affects byte range (e.g., single corrupted block)

**⚠️ INVARIANT**: `IOStatus` must be checked after every I/O operation. Ignoring errors can lead to data loss or corruption. Use `.PermitUncheckedError()` (inherited from `Status`) explicitly to document intentionally ignored errors.

### Verify and Reconstruct Read

**`verify_and_reconstruct_read`** (`file_system.h:133-143`):
When set in `IOOptions`, requests the filesystem to:
1. Detect corruption via stronger checksums
2. Reconstruct data from redundant copies (e.g., RAID, erasure coding)
3. Return reconstructed data if possible

**Use Case**: After detecting corruption in a normal read, re-read with this flag for recovery.

**⚠️ INVARIANT**: FileSystem must support `kVerifyAndReconstructRead` in `FSSupportedOps`, otherwise this flag is ignored (`file_system.h:141-142`).

### Error Handling in Writers/Readers

**WritableFileWriter** (`writable_file_writer.h:308-327`):
- Sets `seen_error_` flag on first error
- All subsequent operations fail fast until `reset_seen_error()` called
- Prevents cascading errors from corrupting file state

**RandomAccessFileReader / SequentialFileReader**:
- Errors are returned immediately to caller
- No internal error state (reads are stateless)
- Callers must handle errors appropriately

---

## Summary

RocksDB's File I/O infrastructure provides:

1. **Abstraction**: `Env`/`FileSystem` decouple RocksDB from OS-specific implementations
2. **Performance**: Direct I/O, buffering, prefetching, and alignment handling
3. **Control**: Rate limiting for background I/O to avoid impacting foreground operations
4. **Reliability**: Checksums, error handling, directory fsync for crash consistency
5. **Flexibility**: Configurable via `EnvOptions`, `FileOptions`, `IOOptions`
6. **Observability**: Statistics, histograms, event listeners for monitoring

**Key Files**:
- `include/rocksdb/env.h`, `include/rocksdb/file_system.h`: Public APIs
- `file/writable_file_writer.{h,cc}`: Buffered write path
- `file/random_access_file_reader.{h,cc}`: Random read path
- `file/sequence_file_reader.{h,cc}`: Sequential read path
- `file/file_prefetch_buffer.{h,cc}`: Adaptive readahead
- `util/rate_limiter_impl.h`: Token bucket rate limiter
- `include/rocksdb/io_status.h`: I/O error handling
- `env/composite_env.cc`: Env/FileSystem adapter

**Design Principles**:
- **Layering**: High-level wrappers (Writers/Readers) → Mid-level abstractions (FileSystem) → Platform implementations (PosixFileSystem)
- **Safety**: Thread-safe, error propagation, resource cleanup (RAII)
- **Efficiency**: Zero-copy when possible, aligned I/O, batching, async I/O
- **Compatibility**: Support both buffered and direct I/O, multiple platforms (Linux, Windows, macOS)
