# SST Table Format

## Overview

SST (Sorted String Table) files are the on-disk storage format for RocksDB. The dominant format is the block-based table, where data is organized into fixed-size blocks with prefix-compressed keys, bloom filters for fast negative lookups, and a hierarchical index for efficient seeking.

### SST File Layout

```
[data block 0]
[data block 1]
...
[data block N-1]
[filter block]                   (full filter or partitioned filter + filter index)
[meta block: properties]
[meta block: compression dictionary]
[meta block: range deletions]
[metaindex block]                (maps meta block names -> BlockHandles)
[index block]                    (binary search, hash, or partitioned index)
[Footer]                         (fixed size, at end of file)
```

Each block is followed by a 5-byte trailer: `[compression_type:1][checksum:4]` (`kBlockTrailerSize = 5`).

---

## 1. Structural Primitives

**Files:** `table/format.h`

### BlockHandle

A pointer to a block extent within an SST file:

| Field | Type | Encoding |
|-------|------|----------|
| `offset_` | `uint64_t` | varint64 |
| `size_` | `uint64_t` | varint64 (payload size, excludes trailer) |

Max encoded length: 20 bytes. Null handle = `{0, 0}`.

### IndexValue

Value stored in index blocks. Contains a `BlockHandle` plus an optional `first_internal_key`. Supports delta encoding: for consecutive blocks, the offset is elided since `offset == prev_offset + prev_size + kBlockTrailerSize`.

### Footer

Fixed-size structure at the tail of every SST file:

| Field | Description |
|-------|-------------|
| `table_magic_number_` | Identifies table type (block-based, plain, cuckoo) |
| `format_version_` | Wire format version (2-7 for read, 2-7 for write) |
| `checksum_type_` | CRC32c, xxHash, xxHash64, or none |
| `metaindex_handle_` | BlockHandle to metaindex block |
| `index_handle_` | BlockHandle to top-level index (format_version < 6) |
| `base_context_checksum_` | For context-aware checksums (format_version >= 6) |

Footer size: 48 bytes (version 0) or 53 bytes (version 1+).

### Block Trailer

Every block on disk has a trailer: `compression_type (1 byte) + checksum (4 bytes)`.

### Format Version Features

| Version | Feature |
|---------|---------|
| < 6 | Index handle in footer |
| >= 6 | Context-aware checksums, unique ID in footer, index handle moves to metaindex |
| >= 7 | Compression manager name stored |

---

## 2. Block Encoding

**Files:** `table/block_based/block_builder.h`, `table/block_based/block.h`

### Block Format (on disk)

```
[entry 0] [entry 1] ... [entry N-1]
[restart_offset_0: uint32] [restart_offset_1: uint32] ... [restart_offset_M: uint32]
[num_restarts: uint32]
```

Each entry uses prefix compression relative to the previous key:

```
shared_bytes:   varint32   -- bytes shared with previous key
unshared_bytes: varint32   -- non-shared key bytes
value_length:   varint32   -- value length
key_delta:      char[unshared_bytes]   -- non-shared key suffix
value:          char[value_length]
```

### Restart Points

Every `block_restart_interval` entries (default 16), a **restart point** is recorded. At restart points, `shared_bytes = 0` (full key, no prefix compression). Restart points are stored as uint32 offsets at the end of the block, enabling binary search.

### Separated KV Storage

When `use_separated_kv_storage` is enabled, keys are written sequentially first, then values in a separate section. Value offsets are stored as varints only at restart points; intermediate offsets are computed incrementally.

### Block Builder

`BlockBuilder` (`table/block_based/block_builder.h`) constructs blocks:

| Method | Description |
|--------|-------------|
| `Add(key, value)` | Append with prefix compression |
| `Finish()` | Write restart array, return block Slice |
| `Reset()` | Clear for reuse |
| `CurrentSizeEstimate()` | Running size estimate |

Constructor parameters control: `block_restart_interval`, `use_delta_encoding`, `use_value_delta_encoding` (for index blocks), `index_type` (binary search or binary+hash).

### Block Reader

`Block` (`table/block_based/block.h`) is the parsed in-memory representation. Key fields:

| Field | Description |
|-------|-------------|
| `contents_` | Raw `BlockContents` |
| `restart_offset_` | Offset of restart array |
| `num_restarts_` | Count of restart points |
| `data_block_hash_index_` | Optional hash index for fast point lookups |
| `values_section_` | Pointer to separated values (if enabled) |

### Block Iterators

| Iterator | Block Type | Value Type | Special Features |
|----------|-----------|------------|------------------|
| `DataBlockIter` | Data blocks | `Slice` | Hash index seek, read-amp bitmap |
| `IndexBlockIter` | Index blocks | `IndexValue` | Delta-decoded handles, first_key support, interpolation search |
| `MetaBlockIter` | Meta blocks | `Slice` | Bytewise comparator, restart interval = 1 |

**DataBlockIter** (`block.h:758`): Supports `SeekForGet()` using `DataBlockHashIndex` for O(1) point lookups within a block. Caches previous entries for efficient `Prev()`.

**IndexBlockIter** (`block.h:904`): Handles delta-encoded index values where only the size portion of BlockHandle is stored for non-restart entries (offset computed by accumulation from previous handle).

---

## 3. BlockBasedTableBuilder (SST Writer)

**Files:** `table/block_based/block_based_table_builder.h`, `table/block_based/block_based_table_builder.cc`

### What It Does

Builds a complete SST file by accumulating sorted key-value pairs into data blocks, then writing metadata blocks, index, and footer.

### Public API

| Method | Description |
|--------|-------------|
| `Add(key, value)` | Add a KV pair. Keys must be in sorted order. |
| `Finish()` | Finalize: flush last data block, write filter/index/properties/range-del/footer |
| `Abandon()` | Discard without finishing |
| `NumEntries()` | Count of Add() calls |
| `FileSize()` | Actual file size (or estimate during parallel compression) |
| `GetTableProperties()` | Collected table properties |

### Build Workflow

```
Add(k1,v1) -> Add(k2,v2) -> ... -> Finish()

Within Add():
  - Append to current BlockBuilder
  - If block size >= target: Flush() -> write data block to file
  - Add key to filter builder
  - Add entry to index builder

Finish() sequence:
  1. Flush remaining data block
  2. WriteFilterBlock()
  3. WriteIndexBlock()
  4. WritePropertiesBlock()
  5. WriteCompressionDictBlock()
  6. WriteRangeDelBlock()
  7. Write metaindex block
  8. WriteFooter()
```

### Buffered vs Unbuffered

The builder starts in **buffered mode** (`kBuffered`) where KV pairs are held in memory for sampling (e.g., compression ratio estimation). It transitions to unbuffered mode via `MaybeEnterUnbuffered()` based on size thresholds.

### Parallel Compression

Optional: background workers (`BGWorker()`) compress and write blocks concurrently. `EstimatedFileSize()` provides size estimates when `FileSize()` is unreliable due to out-of-order block completion.

---

## 4. BlockBasedTable (SST Reader)

**Files:** `table/block_based/block_based_table_reader.h`, `table/block_based/block_based_table_reader.cc`

### What It Does

Reads SST files. Provides point lookups, range iteration, bloom filter checks, and checksum verification.

### Opening

`BlockBasedTable::Open()` is the static factory:
1. Read footer from end of file
2. Read metaindex block
3. Read table properties
4. Optionally prefetch and cache index and filter blocks
5. Create the appropriate `IndexReader` and `FilterBlockReader`

### Point Lookup Flow

```
Get(key)
  |
  v
Check bloom filter (KeyMayMatch?)
  |  no -> return NotFound
  |  yes
  v
Seek index to find data block containing key
  |
  v
RetrieveBlock():
  check block cache -> check compressed cache -> read from file -> decompress
  |
  v
Seek within data block for key
  |
  v
Return value (or resolve blob reference)
```

### Rep Struct

Internal state for an open table reader (`block_based_table_reader.h:604`):

| Field | Description |
|-------|-------------|
| `footer` | Parsed Footer |
| `file` | `RandomAccessFileReader` |
| `index_reader` | Polymorphic index (BinarySearch/Hash/Partitioned) |
| `filter` | Filter block reader |
| `table_properties` | Table properties |
| `filter_type` | `kNoFilter` / `kFullFilter` / `kPartitionedFilter` |
| `index_type` | `kBinarySearch` / `kHashSearch` / `kTwoLevelIndexSearch` |
| `global_seqno` | Sequence number override for ingested files |
| `base_cache_key` | For constructing block cache keys |

### Block Retrieval Pipeline

`RetrieveBlock()` -> check uncompressed block cache -> check compressed cache -> read from file -> decompress -> insert into cache(s).

---

## 5. Index Structures

**Files:** `table/block_based/index_builder.h`, `table/block_based/index_reader.h`

### Index Types

| Type | Enum | Description |
|------|------|-------------|
| Binary Search | `kBinarySearch` | Default. One index block with sorted separator keys. O(log N) seek. |
| Hash Search | `kHashSearch` | Binary search + prefix hash for faster prefix-based seeks. Extra metablocks store prefix metadata. |
| Partitioned (Two-Level) | `kTwoLevelIndexSearch` | Large SSTs. Multiple index partitions + a top-level index. Reduces memory for large files. |

### ShortenedIndexBuilder (Binary Search)

Default index builder. Key optimizations:
- **Key shortening**: Computes the shortest separator between adjacent data blocks rather than storing the full last key. Reduces index size.
- **Optional sequence number stripping**: Maintains two BlockBuilders (with/without seqnums). At `Finish()`, chooses the smaller one if seqnums were never needed for disambiguation.
- **`include_first_key`**: Stores each data block's first internal key in the index value, enabling skip of data block reads when the key is outside the block's range.

### PartitionedIndexBuilder (Two-Level)

For large SSTs, the index is split into partitions:

```
[index partition 0] [index partition 1] ... [index partition P]
[top-level index]   <- maps separator keys to partition BlockHandles
```

Each partition is built with `ShortenedIndexBuilder`. `ShouldCutFilterBlock()` signals aligned filter partitioning.

`Finish()` returns `Status::Incomplete()` for each partition, then `Status::OK()` for the top-level index. The caller writes each partition block and records its handle.

---

## 6. Filter Blocks

**Files:** `table/block_based/filter_block.h`, `table/block_based/full_filter_block.h`, `table/block_based/partitioned_filter_block.h`

### Filter Types

| Type | Description |
|------|-------------|
| Full Filter | Single bloom filter for the entire SST file. One contiguous block. |
| Partitioned Filter | Multiple filter partitions aligned with index partitions. Two-level lookup. |

### Full Filter

**Builder** (`FullFilterBlockBuilder`): Collects all keys during table building, delegates to `FilterBitsBuilder` (typically bloom or ribbon filter) to produce the filter bits.

**Reader** (`FullFilterBlockReader`): Wraps `ParsedFullFilterBlock`. Methods:
- `KeyMayMatch(key)` -- false positive possible, false negative impossible
- `PrefixMayMatch(prefix)` -- prefix-based filter check
- `KeysMayMatch(range)` -- batch version for MultiGet

### Partitioned Filter

**Builder** (`PartitionedFilterBlockBuilder`): Extends `FullFilterBlockBuilder`. Cuts filter partitions aligned with index partitions. Each partition is a full filter block. A top-level index maps separator keys to filter partition handles.

**Reader** (`PartitionedFilterBlockReader`): Two-level lookup:
1. Seek top-level index to find filter partition
2. Load filter partition (from cache or file)
3. Query filter within partition

Maintains `filter_map_` of pinned partitions. `CacheDependencies()` pre-loads all filter partitions.

---

## 7. TableCache

**Files:** `db/table_cache.h`, `db/table_cache.cc`

### What It Does

TableCache manages the caching of `TableReader` objects to avoid repeatedly opening the same SST file. Thread-safe with internal synchronization.

### Key Methods

| Method | Description |
|--------|-------------|
| `FindTable()` | Core lookup. Checks pinned reader on FileMetaData -> checks cache -> opens file and creates TableReader |
| `Get()` | Point lookup on specific SST. Finds table reader, applies filter, seeks to key. Integrates with row cache. |
| `MultiGet()` | Batched point lookups |
| `MultiGetFilter()` | Bloom filter check only (before reading data blocks) |
| `NewIterator()` | Iterator over an SST file. Compaction may create uncached readers. |
| `Evict()` / `ReleaseObsolete()` | Cache eviction for deleted files |
| `GetTableProperties()` | Retrieve table properties without full read |

### Caching Layers

1. **Pinned reader on FileMetaData**: Fastest path. `FileDescriptor::pinned_reader` stores an atomic pointer to a `TableReader` with its cache handle. Bypasses cache lookup entirely.
2. **Table cache** (LRU/Clock): Caches `TableReader` objects keyed by file number. Configured via `max_open_files`.
3. **Row cache** (optional): Caches individual row results keyed by `row_cache_id + fd_number + seq_no + user_key`.

### Immortal Tables

When `table_cache` capacity >= `kInfiniteCapacity`, `immortal_tables_` is set true. Table readers are never evicted and are pinned on `FileMetaData` for zero-overhead access.

### Loader Mutex

A striped mutex (`loader_mutex_`) prevents duplicate concurrent file opens for the same file number. Multiple threads requesting the same file wait on the same mutex shard.

---

## Block Types

Defined in `table/block_based/block_type.h`:

| Type | Content |
|------|---------|
| `kData` | User key-value data |
| `kIndex` | Index entries (separator -> BlockHandle) |
| `kFilter` | Bloom/ribbon filter bits |
| `kFilterPartitionIndex` | Top-level index for partitioned filters |
| `kProperties` | Table properties (num entries, raw sizes, etc.) |
| `kCompressionDictionary` | Shared compression dictionary |
| `kRangeDeletion` | Range tombstone entries |
| `kMetaIndex` | Maps meta block names to BlockHandles |
| `kHashIndexPrefixes` / `kHashIndexMetadata` | Hash search support data |

## Interactions With Other Components

- **Write Path** (see [write_path.md](write_path.md)): FlushJob and CompactionJob use `BlockBasedTableBuilder` to create SST files.
- **Read Path** (see [flush_and_read_path.md](flush_and_read_path.md)): `Version::Get()` uses `TableCache` to access `BlockBasedTable::Get()` for point lookups. Iterators use `BlockBasedTable::NewIterator()`.
- **Cache** (see [cache.md](cache.md)): Data blocks, index blocks, and filter blocks are cached in the block cache. TableCache uses the block cache for table reader management.
- **Version Management** (see [version_management.md](version_management.md)): `FileMetaData` stores per-file metadata including pinned table reader references.
- **Compaction** (see [compaction.md](compaction.md)): `MergingIterator` reads from multiple SST files via their iterators during compaction.
