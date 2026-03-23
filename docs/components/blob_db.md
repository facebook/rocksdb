# RocksDB Integrated BlobDB

## Overview

**Integrated BlobDB** is RocksDB's LSM-external value storage system. Instead of storing large values directly in SST files, BlobDB separates them into dedicated blob files and stores lightweight **BlobIndex** references in the LSM tree. This reduces write amplification for large values while maintaining efficient point lookups and range scans.

**Key Benefits:**
- **Lower Write Amplification**: Large values written once to blob files, not repeatedly during compaction
- **Better Cache Efficiency**: SST files contain only keys + BlobIndex (small), more keys fit in block cache
- **Configurable Trade-offs**: Control blob extraction threshold (`min_blob_size`) and GC aggressiveness

### Architecture Diagram

```
APPLICATION: Put(key, large_value)
  |
  v
WRITE PATH (Flush/Compaction)
  BlobFileBuilder::Add(key, value)
    if (value.size() >= min_blob_size):
      - Write to blob file: <key, compressed_value>
      - Get blob_offset (points to value start)
      - Encode BlobIndex: [file_num|offset|size|comp]
  |
  +---------------------------+
  |                           |
  v                           v
SST FILE (LSM TREE)         BLOB FILE (EXTERNAL)
  key1 -> BlobIndex1          [Header: 30B]
  key2 -> BlobIndex2          [Record1: key1 + value1 + CRC]
  key3 -> inline_value        [Record2: key2 + value2 + CRC]
  (BlobIndex = 30-40 bytes)   [Footer: 32B]
  |                           |
  +------ Read large_value ---+
  |
  v
READ PATH
  BlobSource::GetBlob(file_num, offset, size)
    - Check blob_cache
    - Get BlobFileReader from blob_file_cache
    - Read record: verify CRCs (if verify_checksums)
    - Decompress if needed
    - Populate blob_cache
  |
  Compaction GC trigger
  |
  v
GARBAGE COLLECTION
  CompactionIterator::GarbageCollectBlobIfNeeded()
    if (blob_file_num < cutoff_file_num):  // Old file
      - Fetch blob from old file
      - Write to new blob file via BlobFileBuilder
      - Update BlobIndex to new location
```

---

## 1. Blob File Format

**Files:** `db/blob/blob_log_format.h:27-147`

### File Structure

A blob file consists of a header, variable number of records, and an optional footer:

```
Blob file layout:
  BlobLogHeader (30 bytes)
  BlobLogRecord 0 (header + key + value)
  BlobLogRecord 1 (header + key + value)
  ...
  BlobLogRecord N (header + key + value)
  BlobLogFooter (32 bytes) [OPTIONAL]
```

**⚠️ INVARIANT:** Footer only present when blob file is properly closed. Incomplete files (crash during write) lack footer. (`blob_log_format.h:75`)

### BlobLogHeader (30 bytes)

**File:** `db/blob/blob_log_format.h:43-63`

```cpp
struct BlobLogHeader {
  uint32_t version;           // kVersion1 = 1
  uint32_t column_family_id;  // Which CF this file belongs to
  CompressionType compression;  // Applied to ALL blobs in this file
  bool has_ttl;               // Whether file contains TTL data (unused in integrated mode)
  ExpirationRange expiration_range;  // Pair<uint64_t, uint64_t> (unused in integrated BlobDB)
};
// Note: magic_number (0x00248f37 = 2395959) is encoded in header but not a struct field
```

**Key Fields:**
- **`compression`**: Applies uniformly to all blobs in file (can't mix compression types)
- **`column_family_id`**: Required for crash recovery (maps blob files to CFs)
- **`version`**: Format version (currently kVersion1 = 1)

### BlobLogRecord (32-byte header + variable data)

**File:** `db/blob/blob_log_format.h:118-147`

```
BlobLogRecord layout:
  key_size (Fixed64, 8 bytes)
  value_size (Fixed64, 8 bytes, compressed size if compressed)
  expiration (Fixed64, 8 bytes, 0 in integrated mode)
  header_crc (Fixed32, 4 bytes)
  blob_crc (Fixed32, 4 bytes)
  ---
  key (variable, full user key)
  value (variable, potentially compressed)
```

**CRC Coverage** (`blob_log_format.h:110-111`):
- **`header_crc`**: Covers first 24 bytes `(key_size, value_size, expiration)` — detects header corruption. Uses `crc32c::Mask()`.
- **`blob_crc`**: Covers `(key + value)` — computed via `crc32c::Extend` over key then value, then masked. Detects data corruption.

**⚠️ CRITICAL INVARIANT:** `BlobIndex.offset` points to the **value start**, not the record start. To read a full record given a BlobIndex:

```cpp
// blob_log_format.h:125-128
uint64_t adjustment = BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size);
// Returns kHeaderSize + key_size = 32 + key_size
uint64_t record_offset = blob_index.offset() - adjustment;
```

This design enables **zero-copy value reads** when the key is already known (no need to parse the key from the record).

### BlobLogFooter (32 bytes, optional)

**File:** `db/blob/blob_log_format.h:82-92`

```cpp
struct BlobLogFooter {
  uint64_t blob_count;        // Total number of blobs in file
  ExpirationRange expiration_range;  // Actual min/max expiration (pair of uint64_t)
  uint32_t crc;               // CRC of footer fields
};
// Note: magic_number (0x00248f37) is encoded in footer but not a struct field
```

**Usage:** Footer enables fast metadata queries (blob count) without scanning the entire file. Present only when `BlobFileBuilder::Finish()` is called successfully.

---

## 2. BlobIndex Encoding

**Files:** `db/blob/blob_index.h:17-197`

Instead of storing large values in SST files, RocksDB stores a **BlobIndex** (a small reference) with value type `kTypeBlobIndex`. Three BlobIndex types exist:

### kBlob (type = 1) — Primary Format for Integrated BlobDB

**File:** `db/blob/blob_index.h:162-173`

```
kBlob encoding:
  type: 1 byte (kBlob = 1)
  file_number: varint64 (blob file number, e.g., 000123)
  offset: varint64 (points to VALUE START in blob file)
  size: varint64 (compressed blob size, as stored on disk)
  compression: 1 byte (CompressionType of the blob)
```

**Encoding** (`blob_index.h:162`):
```cpp
static void EncodeBlob(std::string* dst, uint64_t file_number,
                       uint64_t offset, uint64_t size,
                       CompressionType compression_type);
```

**Decoding** (`blob_index.h:94-120`):
```cpp
Status DecodeFrom(Slice slice);  // Parses type, fields (takes Slice by value)
```

**⚠️ INVARIANT:** `offset` always points to **value start**, not record start. This is critical for read path correctness.

**Typical Size:** 30-40 bytes (vs. potentially megabytes for large values).

### kInlinedTTL (type = 0) — Not Used in Integrated BlobDB

```
kInlinedTTL encoding:
  type: 1 byte (kInlinedTTL = 0)
  expiration: varint64
  value: variable (inline value, not externalized)
```

Used only in legacy BlobDB for small values with TTL.

### kBlobTTL (type = 2) — Not Used in Integrated BlobDB

```
kBlobTTL encoding:
  type: 1 byte (kBlobTTL = 2)
  expiration: varint64
  file_number: varint64
  offset: varint64
  size: varint64
  compression: 1 byte
```

For externalized blobs with TTL (not used in integrated mode).

---

## 3. Write Path: Blob File Creation

**Files:** `db/blob/blob_file_builder.h`, `db/blob/blob_file_builder.cc:109-365`

### When BlobFileBuilder is Used

BlobDB is integrated into the LSM tree flush and compaction paths:

1. **FlushJob** (`db/flush_job.cc`): Creates `BlobFileBuilder` if `enable_blob_files` && output level ≥ `blob_file_starting_level`
2. **CompactionJob** (`db/compaction/compaction_job.cc`): Creates `BlobFileBuilder` for output SST files

### BlobFileBuilder::Add() Flow

**File:** `db/blob/blob_file_builder.cc:109-166`

```cpp
Status BlobFileBuilder::Add(const Slice& key, const Slice& value,
                             std::string* blob_index) {
  // 1. Size Check
  if (value.size() < min_blob_size_) {
    return Status::OK();  // Value stays inline in SST
  }

  // 2. Open blob file if needed (first Add or after size limit)
  OpenBlobFileIfNeeded();  // Creates <cf_path>/<file_number>.blob

  // 3. Compression
  Slice blob = value;
  Slice compressed_blob;
  if (blob_compressor_) {
    LegacyForceBuiltinCompression(*blob_compressor_, &blob_compressor_wa_,
                                  blob, &compressed_blob);
    blob = compressed_blob;
  }

  // 4. Write record to blob file
  uint64_t key_offset, blob_offset;
  writer_->AddRecord(*write_options_, key, blob, &key_offset, &blob_offset);

  // 5. Encode BlobIndex (points to value start)
  BlobIndex::EncodeBlob(blob_index, blob_file_number_, blob_offset,
                        blob.size(), blob_compression_type_);

  // 6. Check file size limit
  if (file_writer->GetFileSize() >= blob_file_size_) {
    CloseBlobFile();  // Start new file on next Add
  }

  return Status::OK();
}
```

**⚠️ INVARIANT:** Compression is **always** stored even if compressed size is larger than uncompressed. (`blob_file_builder.cc:279` comment: "Currently we always compress regardless of compressed size.")

### Blob File Lifecycle

**Creation** (`blob_file_builder.cc:179-264`):
1. Generate file number via `file_number_generator_()`
2. Path: `<cf_path>/<file_number>.blob`
3. Write `BlobLogHeader` with CF ID, compression type, no TTL
4. Initialize compressor if `blob_compression_type != kNoCompression`

**Closure** (`blob_file_builder.cc:321-365`):
1. Write `BlobLogFooter` with blob count, expiration range (zeros), footer CRC
2. Sync file if `write_options->sync == true`
3. Create `BlobFileAddition` for MANIFEST:
   - `blob_file_number`, `total_blob_count`, `total_blob_bytes`
   - `checksum_method`, `checksum_value` (file-level checksum)
4. Return `BlobFileAddition` to caller (FlushJob/CompactionJob)

**⚠️ CRITICAL:** `BlobFileBuilder::Finish()` must be called explicitly. If not called (crash/error), file has no footer.

### Blob Cache Prepopulation

**File:** `db/blob/blob_file_builder.cc:400-431`

When `prepopulate_blob_cache == kFlushOnly` and flush is happening:
- Inserts each blob into `blob_cache` with `Cache::Priority::BOTTOM`
- Cache key: `OffsetableCacheKey(db_id, db_session_id, file_number).WithOffset(offset)`
- **Goal:** Warm cache for recently flushed data (likely to be read soon)

---

## 4. Read Path: Blob Retrieval

**Files:** `db/blob/blob_source.h`, `db/blob/blob_source.cc:158-250`

### BlobSource: Unified Caching Gateway

`BlobSource` is the central coordinator for all blob reads. It manages two-level caching:

```
BlobSource
  - blob_cache         (TypedCache<BlobContents>)  [Uncompressed blobs]
  - blob_file_cache    (Cache<BlobFileReader>)     [Open file handles]
  - lowest_used_cache_tier
```

### BlobSource::GetBlob() Flow

**File:** `db/blob/blob_source.cc:158-258`

```cpp
Status BlobSource::GetBlob(const ReadOptions& read_options,
                            const Slice& user_key,
                            uint64_t file_number, uint64_t offset,
                            uint64_t file_size,
                            uint64_t value_size,
                            CompressionType compression_type,
                            FilePrefetchBuffer* prefetch_buffer,
                            PinnableSlice* value,
                            uint64_t* bytes_read) {
  // 1. Build cache key (file_size accepted but unused)
  CacheKey cache_key = GetCacheKey(file_number, file_size, offset);
  // Uses: OffsetableCacheKey(db_id, db_session_id, file_number).WithOffset(offset)

  // 2. Check blob_cache (uncompressed blobs)
  if (blob_cache_) {
    Status s = GetBlobFromCache(cache_key, &blob_handle);
    if (s.ok()) {
      PinCachedBlob(&blob_handle, value);  // Zero-copy transfer
      RecordTick(statistics_, BLOB_DB_CACHE_HIT);
      return s;
    }
  }

  // 3. Check read tier constraint
  if (read_options.read_tier == kBlockCacheTier) {
    return Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
  }

  // 4. Get BlobFileReader from blob_file_cache
  CacheHandleGuard<BlobFileReader> blob_file_reader;
  s = blob_file_cache_->GetBlobFileReader(read_options, file_number,
                                          &blob_file_reader);

  // 5. Read from file
  std::unique_ptr<BlobContents> blob_contents;
  uint64_t read_size;
  s = blob_file_reader.GetValue()->GetBlob(
      read_options, user_key, offset, value_size, compression_type,
      prefetch_buffer, allocator, &blob_contents, &read_size);

  *bytes_read = read_size;

  // 6. Populate blob_cache
  if (blob_cache_ && read_options.fill_cache) {
    s = PutBlobIntoCache(cache_key, &blob_contents, &blob_handle);
  }

  // 7. Transfer ownership to PinnableSlice (zero-copy)
  if (blob_handle.IsReady()) {
    PinCachedBlob(&blob_handle, value);
  } else {
    PinOwnedBlob(&blob_contents, value);
  }

  return Status::OK();
}
```

**⚠️ INVARIANT:** Blob cache key uniqueness: Each blob is uniquely identified by `(db_id, db_session_id, file_number, offset)`. (`blob_source.h:141-145`)

### Zero-Copy Optimization

**File:** `db/blob/blob_source.cc:113-130`

Instead of copying blob data from cache to output:
1. Transfer cache handle ownership to `PinnableSlice`
2. `PinnableSlice::PinSlice()` with custom deleter releases cache handle when done
3. Blob memory stays pinned in cache until `PinnableSlice` destroyed

**Benefits:** Eliminates large memory copies for multi-megabyte blobs.

### BlobFileReader::GetBlob()

**File:** `db/blob/blob_file_reader.cc:306-400`

```cpp
Status BlobFileReader::GetBlob(const ReadOptions& read_options,
                                const Slice& user_key,
                                uint64_t offset, uint64_t value_size,
                                CompressionType compression_type,
                                FilePrefetchBuffer* prefetch_buffer,
                                MemoryAllocator* allocator,
                                std::unique_ptr<BlobContents>* result,
                                uint64_t* bytes_read) const {
  // 1. Calculate read offset and size
  // BlobIndex offset points to value start; if verify_checksums is set,
  // we need to read the full record (header + key + value) to verify CRCs.
  // Otherwise, we read just the value.
  uint64_t key_size = user_key.size();
  uint64_t adjustment = read_options.verify_checksums
      ? BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size)  // 32 + key_size
      : 0;
  uint64_t record_offset = offset - adjustment;
  uint64_t record_size = value_size + adjustment;

  // 2. Read data (via prefetch buffer or direct I/O)
  Slice record_slice;
  // ... read record_size bytes starting at record_offset ...

  // 3. Verify CRCs (only when verify_checksums is true)
  if (read_options.verify_checksums) {
    VerifyBlob(record_slice, user_key, value_size);
    // VerifyBlob checks:
    //   - header_crc via DecodeHeaderFrom (covers key_size, value_size, expiration)
    //   - blob_crc via CheckBlobCRC (covers key + value)
    //   - key_size match, value_size match, key content match
  }

  // 4. Extract value slice and decompress if needed
  Slice value_slice(record_slice.data() + adjustment, value_size);
  UncompressBlobIfNeeded(value_slice, compression_type, allocator, result);

  *bytes_read = record_size;
  return Status::OK();
}
```

**⚠️ INVARIANT:** Multi-blob reads (`MultiGetBlob()`) require offsets sorted ascending. (`blob_file_reader.h:54`)

---

## 5. Blob Garbage Collection

**Files:** `db/blob/blob_garbage_meter.h`, `db/blob/blob_garbage_meter.cc`, `db/compaction/compaction_iterator.cc:1170-1267`

### Garbage Collection Problem

When a key is updated or deleted:
- SST entry is updated/tombstoned during compaction (removed from LSM tree)
- **But blob file entry remains** (blob files are immutable)
- Over time, blob files accumulate **garbage** (unreferenced blobs)

**Solution:** Relocate live blobs from old files to new files during compaction.

### BlobGarbageMeter: Tracking Garbage

**File:** `db/blob/blob_garbage_meter.cc:14-98`

Tracks which blobs flow **into** and **out of** compaction:

```cpp
class BlobGarbageMeter {
  // Per-file inflow/outflow tracking
  std::unordered_map<uint64_t, BlobInOutFlow> flows_;

  Status ProcessInFlow(const Slice& key, const Slice& value);
  Status ProcessOutFlow(const Slice& key, const Slice& value);
};

class BlobInOutFlow {
  BlobStats in_flow_;   // Blobs referenced by compaction input (count + bytes)
  BlobStats out_flow_;  // Blobs referenced by compaction output (count + bytes)
  // Garbage = in_flow - out_flow
};
```

**Garbage Calculation:**
```cpp
uint64_t garbage_bytes = flow.GetGarbageBytes();  // in_flow.bytes - out_flow.bytes
uint64_t garbage_count = flow.GetGarbageCount();  // in_flow.count - out_flow.count
```

**Size Includes Full Record** (`blob_garbage_meter.cc:93-95`):
```cpp
*bytes = blob_index.size() +
         BlobLogRecord::CalculateAdjustmentForRecordHeader(ikey.user_key.size());
// This accounts for header (32B) + key + value
```

**⚠️ INVARIANT:** Garbage tracking only applies to blob files with **both** inflow and outflow. Files with outflow but no inflow are ignored (newly created files).

### GC Decision Logic

**File:** `db/compaction/compaction_iterator.cc:1170-1267`

```cpp
void CompactionIterator::GarbageCollectBlobIfNeeded() {
  // Precondition: assert(ikey_.type == kTypeBlobIndex)

  // Path 1: Integrated BlobDB GC
  if (compaction_->enable_blob_garbage_collection()) {
    // 1. Parse BlobIndex
    BlobIndex blob_index;
    blob_index.DecodeFrom(value_);

    // 2. Age cutoff check
    if (blob_index.file_number() >= blob_garbage_collection_cutoff_file_number_) {
      return;  // Blob file is too new, skip GC
    }

    // 3. Fetch blob from old file
    PinnableSlice blob_value;
    uint64_t bytes_read;
    Status s = blob_fetcher_->FetchBlob(user_key(), blob_index, prefetch_buffer,
                                         &blob_value, &bytes_read);
    if (!s.ok()) {
      status_ = s;  // Set error status (not validity_info_)
      return;
    }

    // 4. Update stats
    ++iter_stats_.num_blobs_read;
    iter_stats_.total_blob_bytes_read += bytes_read;
    ++iter_stats_.num_blobs_relocated;
    iter_stats_.total_blob_bytes_relocated += blob_index.size();

    // 5. Try to re-extract into new blob file immediately
    value_ = blob_value;
    if (ExtractLargeValueIfNeededImpl()) {
      return;  // Value re-extracted to new blob file, type stays kTypeBlobIndex
    }

    // 6. If not re-extracted (value < min_blob_size), convert to inline value
    ikey_.type = kTypeValue;
    // Update internal key to reflect type change
  }

  // Path 2: Stacked BlobDB GC (legacy, via compaction_filter_)
  // ...
}
```

**GC Cutoff Calculation:**
```cpp
// File: db/compaction/compaction_iterator.cc (ComputeBlobGarbageCollectionCutoffFileNumber)
const auto& blob_files = storage_info->GetBlobFiles();  // sorted by file number
size_t cutoff_index = blob_garbage_collection_age_cutoff * blob_files.size();
uint64_t cutoff_file_number = blob_files[cutoff_index]->GetBlobFileNumber();
// Example: age_cutoff = 0.25, num_files = 100 → cutoff_index = 25
// Blobs in files at indices 0-24 (oldest 25%) are eligible for GC
// (i.e., file_number < cutoff_file_number)
```

**⚠️ INVARIANT:** Relocated blobs get new BlobIndex (new file number + offset). Old blob becomes garbage.

### Blob File Deletion

**File:** `db/blob/blob_file_meta.h:94-167`

```cpp
class BlobFileMetaData {
  std::shared_ptr<SharedBlobFileMetaData> shared_meta_;  // Immutable metadata (shared across versions)
  uint64_t garbage_blob_count_;          // Mutable, updated by GC
  uint64_t garbage_blob_bytes_;          // Mutable, updated by GC
  LinkedSsts linked_ssts_;  // = std::unordered_set<uint64_t>, SSTs referencing this blob file
};
```

**Deletion Trigger:**
- Blob file deleted when `SharedBlobFileMetaData` refcount → 0
- Happens when no version references it (all SSTs referencing it are deleted)
- Executed by `DeleteScheduler` (rate-limited to avoid I/O spikes)

**⚠️ INVARIANTS** (`blob_file_meta.h:159-160`):
```cpp
assert(garbage_blob_count_ <= shared_meta_->GetTotalBlobCount());
assert(garbage_blob_bytes_ <= shared_meta_->GetTotalBlobBytes());
```

---

## 6. Compaction Integration

**Files:** `db/compaction/compaction_iterator.cc:1159-1267`, `db/compaction/compaction_job.cc`

### Blob Handling in CompactionIterator

```
CompactionIterator::PrepareOutput()
  |
  +-- ikey_.type == kTypeValue?
  |     ExtractLargeValueIfNeeded()
  |       Calls ExtractLargeValueIfNeededImpl()
  |         if (value.size() >= min_blob_size)
  |           - blob_file_builder_->Add(key, value, &blob_index)
  |           - value_ = blob_index
  |         else: returns false (keep inline)
  |       If extracted: ikey_.type = kTypeBlobIndex
  |
  +-- ikey_.type == kTypeBlobIndex?
        GarbageCollectBlobIfNeeded()
          if (file_number < cutoff)
            - Fetch blob from old file
            - Try ExtractLargeValueIfNeededImpl() [within same call]
                Success: re-extracted to new blob file, stays kTypeBlobIndex
                Failure (too small): ikey_.type = kTypeValue (inlined)
          else: keep BlobIndex as-is (passthrough)
```

**Key Points:**
1. **Extraction:** New inline values (from user writes or GC) extracted to blob files
2. **GC Relocation:** Old BlobIndex → fetch blob → re-extract to new file
3. **Passthrough:** Recent BlobIndex (file_number ≥ cutoff) passed through unchanged

---

## 7. Configuration Options

**Files:** `include/rocksdb/advanced_options.h:1014-1134`

### Primary Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enable_blob_files` | bool | `false` | Master switch for blob separation |
| `min_blob_size` | uint64_t | `0` | Min uncompressed value size for blob (0 = all large values) |
| `blob_file_size` | uint64_t | `256 MB` | Max blob file size before rotation |
| `blob_compression_type` | CompressionType | `kNoCompression` | Compression for blob files |
| `enable_blob_garbage_collection` | bool | `false` | Enable blob GC during compaction |
| `blob_garbage_collection_age_cutoff` | double | `0.25` | Relocate blobs in oldest 25% of files |
| `blob_garbage_collection_force_threshold` | double | `1.0` | Force GC if garbage ratio exceeds (1.0 = disabled) |
| `blob_compaction_readahead_size` | uint64_t | `0` | Readahead for blob file reads during compaction |
| `blob_file_starting_level` | int | `0` | LSM level to start blob extraction (0 = all levels) |
| `blob_cache` | std::shared_ptr<Cache> | `nullptr` | Dedicated cache for uncompressed blobs |
| `prepopulate_blob_cache` | PrepopulateBlobCache | `kDisable` | Warm cache on flush (kFlushOnly) |

**All options except `blob_cache` are dynamically changeable via `SetOptions()` API.** `blob_cache` must be set before `DB::Open()`.

### Configuration Examples

**Basic Setup:**
```cpp
Options options;
options.enable_blob_files = true;
options.min_blob_size = 4096;         // Extract values ≥ 4KB
options.blob_file_size = 256 << 20;   // 256 MB per blob file
options.blob_compression_type = kLZ4Compression;
```

**With Garbage Collection:**
```cpp
options.enable_blob_garbage_collection = true;
options.blob_garbage_collection_age_cutoff = 0.25;  // GC oldest 25% of files
```

**With Blob Cache:**
```cpp
options.blob_cache = NewLRUCache(512 << 20);  // 512 MB blob cache
options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;
```

---

## 8. Blob File Lifecycle Summary

### Creation
1. **Trigger:** Flush or compaction with `enable_blob_files = true`
2. **File Path:** `<cf_path>/<file_number>.blob`
3. **Writer:** `BlobFileBuilder` accumulates blobs until `blob_file_size` limit
4. **MANIFEST:** `BlobFileAddition` logged with checksum, blob count

### Reference Tracking
- **`BlobFileMetaData.linked_ssts_`**: Set of SST file numbers referencing this blob file
- Updated on SST creation/deletion
- Prevents premature deletion

### Garbage Accumulation
- Keys updated/deleted → SST entries removed → blob references lost
- `BlobFileMetaData.garbage_blob_count_` incremented
- GC relocates live blobs when `garbage_ratio` or `age` threshold exceeded

### Deletion
- **Condition:** No version references blob file (all linked SSTs deleted)
- **Executor:** `DeleteScheduler` (rate-limited, default 64 MB/s)
- **Cleanup:** Both blob file and metadata removed

---

## 9. Performance Considerations

### Write Amplification

**Without BlobDB:**
```
1 MB value written 10 times (10 MB user writes)
→ 10 MB L0, 10 MB L1, 10 MB L2, ..., 10 MB L6
→ Total write amp = ~50x (500 MB disk writes for 10 MB user data)
```

**With BlobDB:**
```
1 MB value written 10 times (10 MB user writes)
→ 10 MB blob file (once)
→ 10 × 40 bytes BlobIndex in LSM tree = 400 bytes
→ 400 bytes L0, 400 bytes L1, ..., 400 bytes L6
→ Total write amp = ~2x (10 MB + 2 KB vs. 10 MB user data)
```

**⚠️ Trade-off:** Lower write amp, but **higher read amp** for point lookups (two I/Os: SST + blob file).

### Read Performance

**Optimization Strategies:**
1. **Blob Cache:** Reduce duplicate blob reads (critical for hot blobs)
2. **Prepopulation:** `kFlushOnly` warms cache with recently flushed data
3. **Readahead:** `blob_compaction_readahead_size` for sequential GC reads
4. **MultiGet Batching:** `MultiGetBlob()` batches I/O for multiple blobs

**⚠️ Hot Path Warning:** BlobDB adds latency for uncached reads. Use `min_blob_size` carefully (typically ≥ 4KB).

### Cache Efficiency

**Block Cache Benefits:**
```
Without BlobDB: 1 MB value + 100 byte key = 1.0001 MB per entry
With BlobDB:    40 byte BlobIndex + 100 byte key = 140 bytes per entry
→ ~7000× more keys fit in same cache size
```

Better key density → higher block cache hit rate → fewer SST reads.

---

## 10. Important Invariants Summary

| Invariant | Location | Impact |
|-----------|----------|--------|
| ⚠️ `BlobIndex.offset` points to value start, not record start | `blob_log_format.h:122-128` | Read path correctness |
| ⚠️ Footer only present in properly closed blob files | `blob_log_format.h:75` | Recovery logic must handle missing footers |
| ⚠️ `header_crc` covers `(key_len, val_len, expiration)` | `blob_log_format.h:110` | Detects header corruption |
| ⚠️ `blob_crc` covers `(key + value)` | `blob_log_format.h:111` | Detects data corruption |
| ⚠️ Compression stored even if larger than uncompressed | `blob_file_builder.cc:279` | Storage overhead possible |
| ⚠️ `garbage_blob_count ≤ total_blob_count` | `blob_file_meta.h:159` | Metadata consistency |
| ⚠️ `garbage_blob_bytes ≤ total_blob_bytes` | `blob_file_meta.h:160` | Metadata consistency |
| ⚠️ `MultiGetBlob()` offsets must be sorted ascending | `blob_file_reader.h:54` | I/O batching correctness |
| ⚠️ Integrated BlobDB doesn't use TTL | `blob_file_reader.cc:186-188` | Simplifies format, expiration = 0 |
| ⚠️ Blob cache key = `(db_id, db_session_id, file_number, offset)` | `blob_source.h:141-145` | Cache uniqueness |
| ⚠️ Relocated blobs get new BlobIndex | `compaction_iterator.cc:1201-1237` | GC correctness |
| ⚠️ `BlobFileBuilder::Finish()` must be called explicitly | `blob_file_builder.h:75` | Crash recovery |

---

## Key Code References

| Component | Primary Files | Key Functions |
|-----------|--------------|---------------|
| **Format** | `db/blob/blob_log_format.h:27-147` | Header/Record/Footer structs |
| **Index Encoding** | `db/blob/blob_index.h:17-197` | `EncodeBlob()`, `DecodeFrom()` |
| **Write Path** | `db/blob/blob_file_builder.cc:109-365` | `Add()`, `OpenBlobFile()`, `Finish()` |
| **Read Path** | `db/blob/blob_source.cc:158-250` | `GetBlob()`, `MultiGetBlob()` |
| **File Reader** | `db/blob/blob_file_reader.cc:28-84` | `Create()`, `GetBlob()` |
| **GC Metering** | `db/blob/blob_garbage_meter.cc:14-98` | `ProcessInFlow()`, `ProcessOutFlow()` |
| **GC Logic** | `db/compaction/compaction_iterator.cc:1170-1267` | `GarbageCollectBlobIfNeeded()` |
| **Lifecycle** | `db/blob/blob_file_meta.h:26-167` | `BlobFileMetaData`, `SharedBlobFileMetaData` |
| **Options** | `include/rocksdb/advanced_options.h:1014-1134` | All blob options |
| **Compaction Integration** | `db/compaction/compaction_iterator.cc:1159` | `ExtractLargeValueIfNeeded()` |

---

## Related Components

- **[Write Flow](write_flow.md)**: WriteBatch → WAL → MemTable → Flush (where BlobDB extraction happens)
- **[Compaction](compaction.md)**: CompactionIterator integration, GC logic
- **[SST Table Format](sst_table_format.md)**: How BlobIndex is stored in SST data blocks
- **[Cache](cache.md)**: BlobCache implementation, zero-copy pinning
- **[File I/O](file_io.md)**: FileSystem, WritableFileWriter, RandomAccessFile usage
