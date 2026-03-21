# RocksDB Compression

## Overview

RocksDB supports multiple compression algorithms to reduce storage footprint and I/O bandwidth. Compression operates at the block level for SST files, with optional support for WAL and blob files. Users can configure different compression algorithms per LSM level, enabling tradeoffs between CPU cost and storage savings across the data lifecycle.

### Key Characteristics

- **Multiple algorithms**: Snappy, Zlib, LZ4, LZ4HC, ZSTD, BZip2, Xpress (Windows), kNoCompression
- **Per-level compression**: Different algorithms for L0-Ln and bottommost level
- **Dictionary compression**: ZSTD dictionary training for improved ratios on repetitive data
- **Block-level granularity**: Each data/index/filter block compressed independently
- **Context reuse**: Compression/decompression contexts cached per-core for ZSTD
- **Compressed secondary cache**: Optional caching of compressed blocks
- **Parallel compression**: Multi-threaded compression during flush/compaction (ZSTD, Zlib)
- **Adaptive compression**: Skip compression if ratio below threshold (`max_compressed_bytes_per_kb`)

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  COMPRESSION CONFIGURATION                                      │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ ColumnFamilyOptions                                  │     │
│  │  ├─ compression: CompressionType (default: Snappy)   │     │
│  │  ├─ compression_per_level: vector<CompressionType>   │     │
│  │  ├─ bottommost_compression: CompressionType          │     │
│  │  ├─ compression_opts: CompressionOptions             │     │
│  │  └─ bottommost_compression_opts: CompressionOptions  │     │
│  └──────────────────────────────────────────────────────┘     │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ DBOptions                                            │     │
│  │  └─ wal_compression: CompressionType                 │     │
│  └──────────────────────────────────────────────────────┘     │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ BlockBasedTableOptions                               │     │
│  │  └─ block_cache: supports compressed secondary cache│     │
│  └──────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
                         │
                         v
┌─────────────────────────────────────────────────────────────────┐
│  COMPRESSION PIPELINE (Write Path)                             │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ BlockBasedTableBuilder (table/block_based/           │     │
│  │                          block_based_table_builder.cc)│     │
│  │                                                       │     │
│  │  1. Buffer uncompressed block data                   │     │
│  │  2. [Optional] Sample for dictionary training        │     │
│  │     └─ Buffered in memory until max_dict_buffer_bytes│     │
│  │  3. Build dictionary (if enabled)                    │     │
│  │     └─ ZDICT_trainFromBuffer() or                    │     │
│  │        ZDICT_finalizeDictionary()                    │     │
│  │  4. CompressBlock() for each block:                  │     │
│  │     ├─ Get Compressor from CompressionManager        │     │
│  │     ├─ Parallel compression (if enabled)             │     │
│  │     ├─ Apply dictionary (ZSTD only)                  │     │
│  │     └─ Check compression ratio threshold             │     │
│  │  5. Compute checksum (after compression)             │     │
│  │  6. Write compressed block + trailer to SST          │     │
│  └──────────────────────────────────────────────────────┘     │
└────────────────────────┬───────────────────────────────────────┘
                         │
                         v
┌─────────────────────────────────────────────────────────────────┐
│  COMPRESSION ALGORITHMS (util/compression.h, .cc)              │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ Snappy (default)                                     │     │
│  │  • Fast compression/decompression (~500 MB/s each)   │     │
│  │  • Modest ratio (~2-3x for typical data)             │     │
│  │  • No configuration (level ignored)                  │     │
│  │  • Use for: Hot data, low CPU budget                 │     │
│  └──────────────────────────────────────────────────────┘     │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ LZ4 / LZ4HC                                          │     │
│  │  • LZ4: Extremely fast (~400 MB/s comp, 2 GB/s dec) │     │
│  │  • LZ4HC: High compression variant (slow encode)     │     │
│  │  • level < 0 → acceleration = abs(level) (faster)    │     │
│  │  • Use for: Hot reads, cold writes (LZ4HC)           │     │
│  └──────────────────────────────────────────────────────┘     │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ ZSTD (recommended for cold data)                     │     │
│  │  • Excellent ratio (4-6x typical, up to 10x+)        │     │
│  │  • Fast decompression (~400 MB/s)                    │     │
│  │  • Configurable level (1-22, default=3)              │     │
│  │  • Dictionary training support                       │     │
│  │  • Parallel compression support                      │     │
│  │  • Context caching (per-core ZSTD_CCtx/DCtx)         │     │
│  │  • Use for: Bottommost level, cold tiers             │     │
│  └──────────────────────────────────────────────────────┘     │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ Zlib                                                 │     │
│  │  • Good ratio (~3-4x), slower than Snappy            │     │
│  │  • Configurable level (0-9) and strategy             │     │
│  │  • Use for: Legacy compatibility                     │     │
│  └──────────────────────────────────────────────────────┘     │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ BZip2                                                │     │
│  │  • High ratio, very slow                             │     │
│  │  • Use for: Archival, rarely-read data               │     │
│  └──────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
                         │
                         v
┌─────────────────────────────────────────────────────────────────┐
│  DECOMPRESSION PIPELINE (Read Path)                            │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ BlockBasedTableReader (table/block_based/            │     │
│  │                        block_based_table_reader.cc)  │     │
│  │                                                       │     │
│  │  1. Read compressed block from SST                   │     │
│  │  2. Verify checksum (before decompression)           │     │
│  │  3. Check compressed secondary cache                 │     │
│  │  4. Get Decompressor from CompressionManager         │     │
│  │     └─ Load dictionary from cache if needed          │     │
│  │  5. Get per-core UncompressionContext (ZSTD)         │     │
│  │  6. DecompressBlock() → uncompressed data            │     │
│  │  7. Insert into block cache (uncompressed)           │     │
│  │  8. [Optional] Insert into compressed secondary cache│     │
│  └──────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1. Compression Types and Selection

**Files:** `include/rocksdb/compression_type.h`, `util/compression.h`, `util/compression.cc`

### CompressionType Enum

`CompressionType` is an unsigned char enum representing compression algorithms (`include/rocksdb/compression_type.h:18`):

| Value | Name | Availability | Typical Ratio | Speed (comp/decomp) |
|-------|------|--------------|---------------|---------------------|
| 0x00 | kNoCompression | Always | 1.0x | N/A |
| 0x01 | kSnappyCompression | `#ifdef SNAPPY` | 2-3x | Fast/Fast |
| 0x02 | kZlibCompression | `#ifdef ZLIB` | 3-4x | Medium/Medium |
| 0x03 | kBZip2Compression | `#ifdef BZIP2` | 4-5x | Slow/Slow |
| 0x04 | kLZ4Compression | `#ifdef LZ4` | 2-3x | Very Fast/Very Fast |
| 0x05 | kLZ4HCCompression | `#ifdef LZ4` | 3-4x | Slow/Very Fast |
| 0x06 | kXpressCompression | Windows only | 2-3x | Fast/Fast |
| 0x07 | kZSTD | `#ifdef ZSTD` | 4-6x | Medium/Fast |
| 0x80-0xFE | kCustomCompression* | User-defined | Varies | Varies |

⚠️ **INVARIANT**: CompressionType values are part of the persistent SST format and **must never change**. Each block stores its compression type in the block trailer (`compression_type.h:19`).

### Per-Level Compression Configuration

RocksDB determines compression type based on level and compaction output position:

```cpp
// include/rocksdb/advanced_options.h:534
std::vector<CompressionType> compression_per_level;

// include/rocksdb/options.h:227
CompressionType bottommost_compression = kDisableCompressionOption;
```

**Compression selection logic** (`db/compaction/compaction.cc`):

1. **Bottommost level** (last level with data): Use `bottommost_compression` if set, otherwise `compression_per_level[last]`
2. **Other levels**: Use `compression_per_level[level]`, or `compression` if vector is empty
3. **Flush output (L0)**: Use `compression` or `compression_per_level[0]`

⚠️ **INVARIANT**: With `level_compaction_dynamic_level_bytes=true`, `compression_per_level[0]` determines L0 compression, but `compression_per_level[i]` (i>0) applies to base level + i - 1, **not** physical level i (`advanced_options.h:514`).

**Example configuration** for 7-level LSM:

```cpp
options.compression = kSnappyCompression;  // Default for all levels
options.compression_per_level = {
  kNoCompression,      // L0: No compression (short-lived)
  kLZ4Compression,     // L1: Fast compression
  kLZ4Compression,     // L2-L5: Fast compression
  kZSTD                // L6: High compression (last level)
};
options.bottommost_compression = kZSTD;  // Override last level
```

**Note on vector size** (`advanced_options.h:527`): If `compression_per_level.size() < num_levels`, undefined levels use the last element. Example: `{kNoCompression, kSnappyCompression}` for 7 levels → L2-L6 all use Snappy.

---

## 2. CompressionOptions and Dictionary Compression

**Files:** `include/rocksdb/compression_type.h:169`, `util/compression.h`, `table/block_based/block_based_table_builder.cc`

### CompressionOptions Structure

```cpp
struct CompressionOptions {
  static constexpr int kDefaultCompressionLevel = 32767;

  // Algorithm-specific parameters
  int window_bits = -14;           // Zlib only
  int level = kDefaultCompressionLevel;  // ZSTD, Zlib, LZ4, LZ4HC
  int strategy = 0;                // Zlib only

  // Dictionary compression
  uint32_t max_dict_bytes = 0;              // Dictionary size (0 = disabled)
  uint32_t zstd_max_train_bytes = 0;        // Training data size for ZSTD
  uint64_t max_dict_buffer_bytes = 0;       // Max buffering for sampling
  bool use_zstd_dict_trainer = true;        // Train vs finalize

  // Parallel compression
  uint32_t parallel_threads = 1;   // 1 = disabled

  // Compression quality threshold
  int max_compressed_bytes_per_kb = 896;  // ~12.5% min savings

  // ZSTD checksum
  bool checksum = false;           // Enable ZSTD frame checksum

  bool enabled = false;            // For bottommost_compression_opts only
};
```

**Compression level interpretation**:

| Algorithm | level=kDefaultCompressionLevel | level > 0 | level < 0 |
|-----------|-------------------------------|-----------|-----------|
| ZSTD | 3 (ZSTD_CLEVEL_DEFAULT) | ZSTD level 1-22 | Invalid |
| Zlib | 6 (Z_DEFAULT_COMPRESSION) | Zlib level 0-9 | Invalid |
| LZ4 | 1 (default acceleration) | Invalid | acceleration = abs(level) |
| LZ4HC | 9 (default) | LZ4HC level 1-12 | Invalid |
| Snappy | Ignored | Ignored | Ignored |

⚠️ **INVARIANT**: For LZ4, negative `level` configures `acceleration`, where higher absolute value → faster but lower ratio. This negation ensures decreasing `level` favors speed (`compression_type.h:191`).

### ZSTD Dictionary Compression

**Purpose**: Improve compression ratio for repetitive data (e.g., JSON with similar schemas, time-series with common patterns).

**Training process** (`table/block_based/block_based_table_builder.cc`):

```
1. Buffer uncompressed block data (up to max_dict_buffer_bytes)
2. Sample up to zstd_max_train_bytes for training
3. Train dictionary:
   a. If use_zstd_dict_trainer=true:
      ZDICT_trainFromBuffer() → max_dict_bytes dictionary
   b. If use_zstd_dict_trainer=false:
      ZDICT_finalizeDictionary() → max_dict_bytes dictionary
      (Faster training, potentially lower quality)
4. Create ZSTD_CDict (digested compression dictionary)
5. Compress remaining blocks with dictionary
```

**Dictionary storage**:
- Stored in SST file as `kCompressionDictionary` block (before index block)
- Cached in block cache with `CacheEntryRole::kOtherBlock`
- On read, dictionary loaded into `DecompressorDict` and cached

**Dictionary reuse** (`util/compression.h:217`):

```cpp
struct CompressionDict {
  std::string dict_;                // Raw dictionary bytes
  #ifdef ZSTD
  ZSTD_CDict* zstd_cdict_;         // Digested for fast compression
  #endif
};

struct DecompressorDict {
  std::unique_ptr<Decompressor> decompressor_;  // Owns dictionary
  size_t memory_usage_;                         // For cache accounting
};
```

⚠️ **INVARIANT**: Dictionary must be finalized before compressing any block with it. All blocks in an SST share the same dictionary (`block_based_table_builder.cc`).

**Configuration example**:

```cpp
options.compression_opts.max_dict_bytes = 64 * 1024;         // 64KB dictionary
options.compression_opts.zstd_max_train_bytes = 100 * 1024 * 1024;  // 100MB training
options.compression_opts.max_dict_buffer_bytes = 0;          // No limit (use target_file_size)
options.compression_opts.use_zstd_dict_trainer = true;       // High-quality training
```

**Buffering limits**:
1. `max_dict_buffer_bytes` (user limit, 0 = unlimited)
2. `target_file_size_base` (compaction limit to avoid multi-file dictionaries)
3. Block cache insertion failure (`Status::MemoryLimit`)

When limit hit → finalize dictionary with buffered data, stop buffering, compress remaining blocks.

---

## 3. Block-Level Compression

**Files:** `table/block_based/block_based_table_builder.cc`, `table/block_based/block_based_table_reader.cc`

### Compression Write Path

**BlockBasedTableBuilder::CompressBlock()** (`block_based_table_builder.cc`):

```cpp
// Simplified compression logic
Status CompressBlock(Slice uncompressed_data,
                     CompressionType compression_type,
                     const CompressionDict& compression_dict,
                     std::string* compressed_output,
                     uint32_t* compression_format_version) {
  // 1. Get Compressor from CompressionManager
  Compressor* compressor = GetCompressor(compression_type);

  // 2. Parallel compression (if enabled and supported)
  if (compression_opts.parallel_threads > 1 && SupportsParallelCompression(type)) {
    return compressor->CompressBlockParallel(...);
  }

  // 3. Serial compression with optional dictionary
  return compressor->CompressBlock(uncompressed_data,
                                   compressed_output,
                                   compression_dict);
}
```

**Compression ratio check** (`compression_type.h:289`):

After compressing a block, RocksDB checks `max_compressed_bytes_per_kb`:

```cpp
if (compressed_size > uncompressed_size * max_compressed_bytes_per_kb / 1024) {
  // Compression ratio insufficient → store uncompressed
  // Block trailer records compression_type = kNoCompression
  return kNoCompression;
}
```

Default `max_compressed_bytes_per_kb = 896` → minimum 1.14:1 ratio (12.5% savings).

**Parallel compression** (`block_based_table_builder.cc`):

Enabled when:
- `compression_opts.parallel_threads > 1`
- Compression type supports parallelization (ZSTD, Zlib)
- BlockBasedTable with no user-defined index
- `partition_filters=false` or `decouple_partitioned_filters=true`

Parallel workers compress blocks concurrently using ring buffer coordination. SST size may inflate vs. target_file_size due to in-flight uncompressed data.

### Block Trailer Format

Each block ends with a 5-byte trailer (`table/format.h`):

```
[compression_type: 1 byte][checksum: 4 bytes]
```

⚠️ **INVARIANT**: Checksum is computed **after** compression on write, verified **before** decompression on read. This detects both storage corruption and decompression errors.

### Decompression Read Path

**UncompressBlockData()** (`table/block_based/block_based_table_reader.cc`):

```cpp
// Simplified decompression logic
Status UncompressBlockData(const BlockContents& compressed_block,
                           BlockContents* uncompressed_block,
                           const UncompressionDict& dict) {
  // 1. Get Decompressor from CompressionManager
  Decompressor* decompressor = GetDecompressor(compression_type);

  // 2. Get cached ZSTD uncompression context (if ZSTD)
  ZSTDUncompressCachedData zstd_ctx =
      CompressionContextCache::Instance()->GetCachedZSTDUncompressData();

  // 3. Decompress
  Status s = decompressor->DecompressBlock(compressed_data,
                                           uncompressed_size,
                                           uncompressed_output,
                                           dict);

  // 4. Return context to cache
  if (zstd_ctx.GetCacheIndex() >= 0) {
    CompressionContextCache::Instance()->ReturnCachedZSTDUncompressData(idx);
  }

  return s;
}
```

---

## 4. Compression Context Caching

**Files:** `util/compression_context_cache.h`, `util/compression_context_cache.cc`

### Per-Core Context Caching

ZSTD contexts (`ZSTD_CCtx`, `ZSTD_DCtx`) are expensive to allocate. RocksDB caches one per CPU core to amortize allocation cost across decompression operations.

**Architecture**:

```cpp
// util/compression_context_cache.cc
class CompressionContextCache {
 public:
  static CompressionContextCache* Instance();  // Singleton

  ZSTDUncompressCachedData GetCachedZSTDUncompressData();
  void ReturnCachedZSTDUncompressData(int64_t idx);

 private:
  CoreLocalArray<ZSTDCachedData> per_core_uncompr_;  // One per CPU core
};

struct ZSTDCachedData {
  ZSTDUncompressCachedData uncomp_cached_data_;  // ZSTD_DCtx wrapper
  std::atomic<void*> zstd_uncomp_sentinel_;      // CAS for lock-free acquire

  ZSTDUncompressCachedData GetUncompressData(int64_t idx) {
    void* expected = &uncomp_cached_data_;
    if (zstd_uncomp_sentinel_.compare_exchange_strong(expected, SentinelValue)) {
      // Successfully acquired cached context
      result.InitFromCache(uncomp_cached_data_, idx);
    } else {
      // Cache busy → create temporary context
      result.CreateIfNeeded();
    }
    return result;
  }
};
```

**Workflow**:
1. **Acquire**: `GetCachedZSTDUncompressData()` tries CAS to acquire per-core context
   - Success → return cached `ZSTD_DCtx` with cache index
   - Failure → allocate temporary `ZSTD_DCtx` (cache index = -1)
2. **Use**: Decompressor uses `ZSTD_DCtx` for decompression
3. **Release**: `ReturnCachedZSTDUncompressData(idx)` returns context to cache (only if idx ≥ 0)

⚠️ **INVARIANT**: Compression contexts (`ZSTD_CCtx`) are **not cached** because `BlockBasedTableBuilder` creates one per SST file and reuses it across all blocks (`compression_context_cache.cc:23`).

**Memory overhead**: ~100-200 KB per CPU core for ZSTD decompression contexts.

---

## 5. Compressed Block Cache

**Files:** `include/rocksdb/cache.h`, `cache/compressed_secondary_cache.h`

### Compressed Secondary Cache

RocksDB supports a two-tier cache architecture:
1. **Primary cache**: Uncompressed blocks (fast access, high memory cost)
2. **Compressed secondary cache**: Compressed blocks (slower access, lower memory cost)

**Workflow**:

```
Read path with compressed secondary cache:
1. Lookup in primary cache (uncompressed)
   ├─ HIT → return uncompressed block
   └─ MISS ↓
2. Lookup in compressed secondary cache
   ├─ HIT → decompress → insert into primary → return
   └─ MISS ↓
3. Read from SST (already compressed)
4. Decompress
5. Insert into primary cache
6. [Optional] Insert into compressed secondary cache
```

**Configuration**:

```cpp
CompressedSecondaryCacheOptions opts;
opts.capacity = 512 * 1024 * 1024;  // 512MB compressed cache
opts.num_shard_bits = 6;
opts.compression_type = kZSTD;      // Re-compress for cache (may differ from SST)
opts.compress_format_version = 2;

std::shared_ptr<SecondaryCache> compressed_cache =
    NewCompressedSecondaryCache(opts);

BlockBasedTableOptions table_opts;
table_opts.block_cache = NewCache(
    1024 * 1024 * 1024,  // 1GB primary cache
    6,                   // num_shard_bits
    false,               // strict_capacity_limit
    0,                   // high_pri_pool_ratio
    nullptr,
    kDefaultToAdaptiveMutex,
    kDontChargeCacheMetadata,
    compressed_cache     // Attach compressed secondary cache
);
```

**Insertion policy**:

The `Insert` method accepts a `force_insert` boolean parameter that controls when blocks are inserted into the compressed secondary cache. The decision of when to set this flag is made by the caller based on caching policies.

⚠️ **INVARIANT**: Compressed secondary cache stores blocks **already compressed** (from SST). RocksDB may re-compress with different `compression_type` for cache, independent of SST compression (`compressed_secondary_cache.h`).

**Performance tradeoff**:
- **Pro**: Effective cache capacity increases 3-5x (typical compression ratio)
- **Con**: Decompression CPU cost on cache hit (mitigated by per-core context caching)

---

## 6. WAL Compression

**Files:** `include/rocksdb/options.h:1481`, `db/wal_manager.cc`

### Write-Ahead Log Compression

RocksDB supports compressing WAL records to reduce write amplification and log storage.

**Configuration**:

```cpp
options.wal_compression = kZSTD;  // Default: kNoCompression
```

**Supported algorithms**: ZSTD only (as of RocksDB 7.x).

**WAL record format** with compression:

```
[sequence_number: varint64][count: varint32]
[compressed_batch: varint32 size + compressed WriteBatch]
[checksum: 4 bytes]
```

**Compression workflow**:
1. Serialize `WriteBatch` to uncompressed buffer
2. Compress buffer with `StreamingCompress` (ZSTD)
3. Write compressed record to WAL
4. On recovery: Decompress each record with `StreamingUncompress`

⚠️ **INVARIANT**: WAL compression is **stateless** per record. Each WAL record is compressed independently (no dictionary, no inter-record dependencies) to ensure crash recovery can start from any valid record (`wal_manager.cc`).

**Performance considerations**:
- **Write latency**: +10-30% due to compression CPU cost (mitigated by batching)
- **WAL size**: -50% to -70% for typical workloads
- **Recovery time**: +10-20% due to decompression (linear in WAL size)

**Recommendation**: Enable for write-heavy workloads where WAL write amplification is significant (e.g., NVMe SSDs with high IOPS).

---

## 7. Blob Compression

**Files:** `include/rocksdb/advanced_options.h:1056`, `db/blob/blob_file_builder.cc`

### Blob File Compression

BlobDB separates large values (blobs) into blob files, with optional per-blob compression.

**Configuration**:

```cpp
options.enable_blob_files = true;
options.min_blob_size = 4096;              // Values ≥4KB → blob files
options.blob_compression_type = kZSTD;     // Default: kNoCompression
```

**Blob record format**:

```
[key_size: varint32][key: bytes]
[value_size: varint32][value: compressed bytes]
[expiration: varint64][compression_type: 1 byte]
[checksum: 4 bytes]
```

**Compression workflow**:
1. Check `value.size() >= min_blob_size`
2. Compress value with `blob_compression_type`
3. Write compressed blob record to blob file
4. Write blob reference (file number + offset + size) to SST

⚠️ **INVARIANT**: Blob compression is **independent** of SST compression. A blob-ified value is compressed in the blob file, while its reference in the SST may be in a compressed data block (`blob_file_builder.cc`).

**Performance tradeoff**:
- **Pro**: Reduced blob file size → lower storage cost, faster scans of blob files
- **Con**: Decompression cost on blob read (not amortized across multiple values like SST blocks)

**Recommendation**: Use ZSTD for cold blobs (e.g., media metadata, logs). Use kNoCompression for hot blobs accessed frequently.

---

## 8. Compression and Checksums

**Files:** `table/format.cc`, `table/block_based/block_based_table_builder.cc`

### Checksum Order Invariants

⚠️ **INVARIANT**: Checksums are **always** computed on compressed data, never uncompressed data.

**Write path** (`block_based_table_builder.cc`):

```
1. Compress block → compressed_data
2. Compute checksum on compressed_data → checksum
3. Write [compressed_data][compression_type: 1 byte][checksum: 4 bytes]
```

**Read path** (`block_based_table_reader.cc`):

```
1. Read [compressed_data][compression_type][checksum]
2. Verify checksum on compressed_data
   └─ If mismatch → return Status::Corruption
3. Decompress compressed_data → uncompressed_data
```

**Rationale**: Checksum on compressed data detects:
- Storage corruption (bit flips, torn writes)
- Decompression errors (buffer overruns, invalid compressed stream)

Checksum on uncompressed data would only detect storage corruption, missing decompression errors.

### ZSTD Frame Checksum

ZSTD supports an **additional** optional checksum inside the compressed frame:

```cpp
options.compression_opts.checksum = true;  // Enable ZSTD frame checksum
```

**Frame format** with checksum:

```
[ZSTD frame header][compressed blocks][frame checksum: 4 bytes]
```

**Dual checksum**:
1. **RocksDB block checksum**: On entire compressed frame (RocksDB-controlled)
2. **ZSTD frame checksum**: Inside compressed frame (ZSTD-controlled)

⚠️ **INVARIANT**: Enabling ZSTD frame checksum increases compressed size by 4 bytes per block and adds CPU cost during compression/decompression. Only enable if paranoid data integrity required (`compression_type.h:302`).

---

## 9. Performance Tradeoffs and Benchmarking

### Compression Algorithm Selection Guide

| Workload | Recommended Compression | Rationale |
|----------|------------------------|-----------|
| Hot data (L0-L2) | LZ4 or Snappy | Fast read/write, acceptable ratio |
| Warm data (L3-L5) | Snappy or LZ4HC | Balanced ratio and speed |
| Cold data (L6/bottommost) | ZSTD (level 3-9) | High ratio, read-optimized |
| Archival/backup | ZSTD (level 15-22) or BZip2 | Maximum ratio, rare reads |
| Write-heavy SSD | kNoCompression (L0-L1) + ZSTD (L2+) | Reduce write amplification |
| Read-heavy HDD | ZSTD (all levels) | Reduce I/O bandwidth |

### CPU vs. Storage Tradeoff

**Compression CPU cost** (relative to kNoCompression):

| Algorithm | Compression | Decompression | Typical Ratio |
|-----------|-------------|---------------|---------------|
| Snappy | 1.0x | 1.0x | 2-3x |
| LZ4 | 0.5x | 0.3x | 2-3x |
| LZ4HC | 5.0x | 0.3x | 3-4x |
| ZSTD (level 3) | 2.0x | 0.8x | 4-5x |
| ZSTD (level 9) | 10.0x | 0.8x | 5-6x |
| Zlib (level 6) | 3.0x | 2.0x | 3-4x |
| BZip2 | 20.0x | 10.0x | 4-5x |

**Dictionary compression** (ZSTD only):

| Data Characteristics | Dictionary Benefit | Configuration |
|---------------------|-------------------|---------------|
| Repetitive schemas (JSON, logs) | +20-50% ratio improvement | max_dict_bytes=64KB, zstd_max_train_bytes=100MB |
| Random data (encrypted, compressed) | 0% (no benefit) | max_dict_bytes=0 (disabled) |
| Mixed workload | +10-20% ratio improvement | max_dict_bytes=32KB, zstd_max_train_bytes=50MB |

**Memory cost**:
- Dictionary training: `max_dict_buffer_bytes` (charged to block cache)
- Dictionary storage: ~`max_dict_bytes` per SST (typically 16-64 KB)
- Decompression context cache: ~150 KB per CPU core (ZSTD only)

### Benchmarking Compression

**db_bench examples**:

```bash
# Baseline: no compression
./db_bench --benchmarks=fillseq --compression_type=none \
  --num=10000000 --value_size=1024

# ZSTD with dictionary
./db_bench --benchmarks=fillseq --compression_type=zstd \
  --compression_max_dict_bytes=65536 \
  --compression_zstd_max_train_bytes=104857600 \
  --num=10000000 --value_size=1024

# Per-level compression
./db_bench --benchmarks=fillrandom,compact,readrandom \
  --compression_type=lz4 \
  --compression_per_level=none:lz4:lz4:lz4:lz4:lz4:zstd \
  --bottommost_compression=zstd \
  --num=100000000 --value_size=1024 --cache_size=1073741824

# Parallel compression
./db_bench --benchmarks=fillrandom --compression_type=zstd \
  --compression_parallel_threads=8 \
  --num=10000000 --value_size=4096
```

**Key metrics**:
- `rocksdb.block.compression.bytes.total`: Total bytes compressed
- `rocksdb.block.compression.bytes.after`: Compressed size
- `rocksdb.compression.times.nanos`: Compression CPU time
- `rocksdb.decompression.times.nanos`: Decompression CPU time
- SST file sizes (from `ldb manifest_dump` or `sst_dump`)

**Compression ratio calculation**:

```
Compression ratio = uncompressed_size / compressed_size
Savings = 1 - (compressed_size / uncompressed_size)
```

**Read amplification with compression**:

| Scenario | Uncompressed | Compressed (4x ratio) | Savings |
|----------|--------------|----------------------|---------|
| Read 1MB block from SSD | 1MB I/O | 256KB I/O | 74% less I/O |
| Decompress 1MB block (ZSTD) | 0 CPU | ~2.5ms CPU @ 400 MB/s | +2.5ms latency |

**Recommendation**: Profile with production-like data. Compression effectiveness varies dramatically based on data entropy and access patterns.

---

## 10. Common Pitfalls and Best Practices

### Pitfalls

1. **Using ZSTD without dictionary for structured data**: Loses 20-50% compression potential
   - **Solution**: Enable dictionary with `max_dict_bytes=64KB`, `zstd_max_train_bytes=100MB`

2. **Enabling parallel compression for Snappy/LZ4**: No throughput gain, wastes memory
   - **Solution**: Only enable `parallel_threads > 1` for ZSTD/Zlib

3. **Setting `max_dict_buffer_bytes < max_dict_bytes`**: Dictionary too small or fails to build
   - **Solution**: Set `max_dict_buffer_bytes = 0` (unlimited) or `>> max_dict_bytes`

4. **Compressing L0 with ZSTD**: Adds write latency for short-lived data
   - **Solution**: Use `compression_per_level={kNoCompression, kLZ4, ..., kZSTD}`

5. **Forgetting to benchmark**: Assuming compression always improves performance
   - **Solution**: Benchmark with `db_bench` on production-like data before production deployment

6. **Mismatched `compression_opts` and `bottommost_compression_opts`**: Inconsistent configuration
   - **Solution**: Explicitly set `bottommost_compression_opts.enabled=true` and configure all fields

### Best Practices

1. **Start with defaults**: `compression=kSnappyCompression`, then optimize if storage is bottleneck
2. **Use per-level compression**: `kNoCompression` or `kLZ4` for hot levels, `kZSTD` for bottommost
3. **Enable ZSTD dictionary for structured data**: JSON, protobufs, logs with repetitive schemas
4. **Monitor compression ratio**: If ratio < 1.5:1, compression may not be worth CPU cost
5. **Tune `max_compressed_bytes_per_kb`**: Raise to 1024 (disable threshold) for testing, then lower to 800-900 if compression ratio inconsistent
6. **Profile decompression**: If >10% of read CPU, consider LZ4/Snappy for hot data
7. **Test with production data**: Synthetic benchmarks may not reflect real compression ratios

### Debugging Compression Issues

**Check effective compression type**:

```bash
# Dump SST metadata
ldb manifest_dump --db=/path/to/db | grep compression

# Dump block metadata
sst_dump --file=/path/to/sst --command=scan --output_hex | grep compression_type
```

**Verify dictionary usage**:

```bash
# Check for compression dictionary block
sst_dump --file=/path/to/sst --command=raw | grep kCompressionDictionary
```

**Measure actual compression ratio**:

```bash
# Compare SST size to raw data size
ldb dump --db=/path/to/db --count_only  # Get key count
du -sh /path/to/db/*.sst                # Get SST sizes
# Ratio = (key_count * (key_size + value_size)) / sum(sst_sizes)
```

---

## Summary

RocksDB's compression system balances storage efficiency and CPU cost through:
- **Per-level compression**: Optimize hot vs. cold data independently
- **Dictionary compression**: Improve ratio for structured/repetitive data (ZSTD)
- **Context caching**: Amortize ZSTD context allocation across operations
- **Compressed secondary cache**: Extend effective cache capacity 3-5x
- **Adaptive compression**: Skip compression for incompressible blocks
- **Parallel compression**: Scale compression throughput with CPU cores (ZSTD, Zlib)

**Key invariants**:
- Compression type stored in every block trailer (persistent format)
- Checksum on compressed data, verified before decompression
- Dictionary shared across all blocks in an SST file
- ZSTD contexts cached per CPU core, not per SST file

**Configuration checklist**:
1. Choose algorithm per level (`compression`, `compression_per_level`, `bottommost_compression`)
2. Configure ZSTD dictionary if applicable (`max_dict_bytes`, `zstd_max_train_bytes`)
3. Tune compression quality (`level`, `max_compressed_bytes_per_kb`)
4. Enable parallel compression for ZSTD/Zlib if CPU available (`parallel_threads`)
5. Consider compressed secondary cache for memory-constrained deployments
6. Benchmark with `db_bench` on production-like data before deployment
