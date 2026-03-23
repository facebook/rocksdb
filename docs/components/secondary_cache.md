# Secondary Cache and Row Cache

This document describes RocksDB's secondary cache tier and row cache mechanisms. For the primary block cache (LRU, HyperClockCache), see [cache.md](cache.md).

## Overview

RocksDB supports multiple caching mechanisms beyond the primary block cache:

1. **Secondary Cache**: A second-tier cache behind the primary block cache, typically storing compressed blocks or using non-volatile storage
2. **Row Cache**: A per-row cache for point lookups that stores serialized `GetContext` replay logs (sequences of `ValueType` + length-prefixed values, with optional timestamps)

## Secondary Cache Architecture

### Concept

The secondary cache provides a second tier of caching behind the primary block cache. When a block is evicted from the primary cache, it can be demoted to the secondary cache. When a lookup misses in the primary cache but hits in the secondary cache, the block is promoted back to the primary cache.

```
Primary Block Cache (LRU) -- Fast, uncompressed
  (BlockBasedTableReader data)
    | Evict/Promote
    v
Secondary Cache (Compressed) -- Slower, compressed
  (Compressed blocks, may use NVM)
```

**Key characteristics:**
- On **primary cache eviction**: Blocks can be demoted to secondary cache (admission policy controls this)
- On **primary cache miss**: Secondary cache is checked; on hit, block is promoted to primary cache
- **Transparent to users**: Block cache lookups automatically query the secondary cache on misses

### SecondaryCache Interface

Defined in `include/rocksdb/secondary_cache.h`:

```cpp
class SecondaryCache {
  // Insert an object into the secondary cache
  virtual Status Insert(const Slice& key, Cache::ObjectPtr obj,
                        const Cache::CacheItemHelper* helper,
                        bool force_insert) = 0;

  // Insert pre-saved (compressed) data
  virtual Status InsertSaved(const Slice& key, const Slice& saved,
                             CompressionType type, CacheTier source) = 0;

  // Lookup a key in the secondary cache
  virtual std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CacheItemHelper* helper,
      Cache::CreateContext* create_context, bool wait, bool advise_erase,
      Statistics* stats, bool& kept_in_sec_cache) = 0;

  // Erase an entry
  virtual void Erase(const Slice& key) = 0;

  // Wait for multiple lookups to complete
  virtual void WaitAll(std::vector<SecondaryCacheResultHandle*> handles) = 0;

  // Whether the secondary cache supports force-erase (dummy entry promotion)
  virtual bool SupportForceErase() const = 0;

  // Capacity management (default: NotSupported)
  virtual Status SetCapacity(size_t capacity);
  virtual Status GetCapacity(size_t& capacity);
  virtual Status Deflate(size_t decrease);  // Reduce capacity
  virtual Status Inflate(size_t increase);  // Increase capacity
};
```

**Key methods:**

- **`Insert()`**: Suggests inserting an object into the secondary cache. The secondary cache may choose to ignore the insertion based on its admission policy. The caller retains ownership of the object during the call.

- **`InsertSaved()`**: Inserts pre-saved/compressed data directly, useful for warming up the cache from auxiliary sources without first creating objects.

- **`Lookup()`**: Looks up a key. Returns a handle that may not be ready immediately if `wait=false`. The `advise_erase` parameter hints that the primary cache will store this entry, so the secondary cache can optionally erase it.

- **`WaitAll()`**: Efficiently waits for multiple async lookups to complete, used for batch operations like MultiGet.

- **`SupportForceErase()`**: Returns whether this cache supports force-erase semantics, enabling the dummy-entry promotion strategy in `CacheWithSecondaryAdapter`.

- **`SetCapacity()`/`GetCapacity()`**: Get or set the secondary cache capacity. Default implementation returns `NotSupported`.

- **`Deflate()`/`Inflate()`**: Decrease or increase the secondary cache capacity by a given amount. Used by `CacheWithSecondaryAdapter` for memory budget distribution when `distribute_cache_res=true`. Default implementation returns `NotSupported`.

**⚠️ INVARIANT**: `Insert()` returning `Status::OK()` does NOT guarantee the item was cached. The secondary cache may reject insertions based on admission policy or capacity constraints.

**⚠️ INVARIANT**: After a successful `Lookup()`, calling `Value()` and taking ownership is required to avoid memory leaks in case of a cache hit.

### SecondaryCacheResultHandle

Returned by `Lookup()`, represents the result of a secondary cache lookup:

```cpp
class SecondaryCacheResultHandle {
  virtual bool IsReady() = 0;        // Is the lookup complete?
  virtual void Wait() = 0;            // Block until ready
  virtual Cache::ObjectPtr Value() = 0;  // Get the value (nullptr if miss)
  virtual size_t Size() = 0;          // Get the charge
};
```

**Lifecycle:**
1. **Pending state** (`IsReady() == false`): Lookup in progress, must not destroy handle
2. **Ready + not found** (`IsReady() == true`, `Value() == nullptr`): No match or error
3. **Ready + found** (`IsReady() == true`, `Value() != nullptr`): Entry loaded, caller owns object

## CacheWithSecondaryAdapter

Defined in `cache/secondary_cache_adapter.cc`, this adapter integrates a SecondaryCache with a primary Cache.

### Integration with Primary Cache

```cpp
class CacheWithSecondaryAdapter : public CacheWrapper {
  CacheWithSecondaryAdapter(
      std::shared_ptr<Cache> target,
      std::shared_ptr<SecondaryCache> secondary_cache,
      TieredAdmissionPolicy adm_policy,
      bool distribute_cache_res);
};
```

**How it works:**
1. **On primary cache eviction**: `EvictionHandler()` intercepts evictions and calls `secondary_cache_->Insert()` based on admission policy
2. **On primary cache miss**: `Lookup()` queries the secondary cache and promotes hits back to primary cache
3. **Dummy entries**: For secondary caches supporting `SupportForceErase()`, a dummy entry (0 charge) is inserted in the primary cache to track recent access while keeping the real data standalone

### Promotion Strategy

When promoting from secondary to primary cache:

```
Secondary cache hit -> Create object from saved data -> Insert into primary cache
                                                     |
                                                     v
                                        Two promotion modes:
                                          1. Regular insertion
                                          2. Standalone + dummy
```

**Standalone + dummy mode** (when `SupportForceErase()` returns true):
- Creates a standalone handle (not subject to eviction quota)
- Inserts a 0-charge dummy entry to track recent use
- Avoids reading from storage again even if the cache is full

**⚠️ INVARIANT**: Standalone handles created during promotion must eventually be released. They are not subject to eviction and will leak memory if not released by the caller.

### Memory Budget Distribution

When `distribute_cache_res=true` (used by `NewTieredCache()`):

```
Total Capacity = 100 MB
compressed_secondary_ratio = 0.3 (30%)

Primary cache capacity = 100 MB (total)
  - Reserved for secondary: 30 MB (via CacheReservationManager)
  - Usable for blocks: 70 MB

Secondary cache capacity = 30 MB
```

**How it works:**
1. Primary cache is initially sized to the full budget
2. A `ConcurrentCacheReservationManager` reserves space on behalf of the secondary cache
3. When placeholder entries (e.g., WriteBufferManager reservations) are inserted:
   - Full charge goes to primary cache
   - Secondary cache is deflated proportionally
   - Primary reservation is reduced to compensate

**Example** (70/30 ratio, 100MB total, inserting 10MB placeholder):
```
Before:
Primary usable: 70MB, Secondary: 30MB, Placeholder: 0MB

After inserting 10MB placeholder:
1. Placeholder added to primary: 70MB → 60MB usable, 10MB placeholder
2. Secondary deflated by 3MB: 30MB → 27MB
3. Primary reservation reduced by 3MB: 30MB → 27MB reserved
Result: Primary usable: 63MB, Placeholder: 10MB, Secondary: 27MB
```

This prevents double-charging memory reservations across both cache tiers.

## CompressedSecondaryCache

Defined in `cache/compressed_secondary_cache.cc`, stores compressed blocks in memory.

### Architecture

```cpp
class CompressedSecondaryCache : public SecondaryCache {
  CompressedSecondaryCache(const CompressedSecondaryCacheOptions& opts);
};
```

**Internal structure:**
- Backed by an LRU cache for storing compressed blocks
- Uses `cache_res_mgr_` for Deflate/Inflate operations when distributing memory budgets
- Supports compression types: Snappy, Zlib, LZ4, ZSTD, etc.

### Data Format

Entries are stored as a **tagged value** with a 2-byte header followed by the saved block data (which may or may not be compressed):

```
Source (1B) | Type (1B) | Saved/Compressed Block Data
```

- **Source (`CacheTier`)**: Which tier is responsible for compression/decompression
  - `kVolatileCompressedTier`: Compressed by CompressedSecondaryCache
  - `kVolatileTier`: Already compressed from primary cache
- **Type (`CompressionType`)**: Compression algorithm used (`kNoCompression` when data is uncompressed — e.g., compression was disabled, rejected, or skipped for a specific role)

The tagged value is stored differently depending on `enable_custom_split_merge`:
- **`enable_custom_split_merge=false`** (default): Stored as a `LengthPrefixedSlice` (varint64 size prefix + tagged value)
- **`enable_custom_split_merge=true`**: Stored as a chain of `CacheValueChunk` objects (linked list of fixed-size chunks) to reduce internal fragmentation in the allocator

### Insertion Flow

```cpp
Status Insert(const Slice& key, Cache::ObjectPtr value,
              const Cache::CacheItemHelper* helper, bool force_insert) {
  if (!force_insert && MaybeInsertDummy(key)) {
    return Status::OK();  // Insert dummy placeholder on first eviction
  }
  return InsertInternal(key, value, helper, kNoCompression,
                        CacheTier::kVolatileCompressedTier);
}
```

**Admission policy (dummy-based):**
1. **First eviction**: Insert a dummy entry (nullptr value, 0 charge) to track the key
2. **Second eviction**: If dummy exists, insert the real compressed data (second-chance admission)

**⚠️ INVARIANT**: Dummy entries have `value == nullptr` and `charge == 0`. They must be distinguished from real entries to avoid dereferencing null pointers.

### Compression

```cpp
Status InsertInternal(...) {
  // Save uncompressed data
  (*helper->saveto_cb)(value, 0, data_size_original, data_ptr);

  // Compress if configured
  if (compressor_ && from_type == kNoCompression) {
    s = compressor_->CompressBlock(uncompressed_data,
                                   compressed_buf,
                                   &compressed_size, &to_type);
    if (to_type == kNoCompression) {
      // Compression rejected/failed - keep original
    } else {
      // Use compressed version
    }
  }

  // Insert into internal LRU cache
  cache_->Insert(key, data, helper, charge);
}
```

**Compression decision:**
- Attempts compression only if the original data is uncompressed (`from_type == kNoCompression`)
- Rejects compression if compressed size ≥ original size
- Configured roles can skip compression via `do_not_compress_roles`

### Lookup Flow

```cpp
std::unique_ptr<SecondaryCacheResultHandle> Lookup(...) {
  Cache::Handle* lru_handle = cache_->Lookup(key);
  if (lru_handle == nullptr) return nullptr;  // Miss

  void* handle_value = cache_->Value(lru_handle);
  if (handle_value == nullptr) {
    // Dummy entry - return nullptr to indicate miss
    RecordTick(stats, COMPRESSED_SECONDARY_CACHE_DUMMY_HITS);
    return nullptr;
  }

  // Decompress if needed
  if (source == CacheTier::kVolatileCompressedTier && type != kNoCompression) {
    decompressor_->DecompressBlock(compressed_data, uncompressed_buf);
  }

  // Create object using helper's create_cb
  helper->create_cb(saved, type, source, create_context, &result_value, &result_charge);

  if (advise_erase) {
    cache_->Release(lru_handle, erase_if_last_ref=true);
    cache_->Insert(key, nullptr, helper, 0);  // Replace with dummy
  } else {
    kept_in_sec_cache = true;
  }

  return CompressedSecondaryCacheResultHandle(result_value, result_charge);
}
```

**⚠️ INVARIANT**: When `advise_erase=true`, the entry is erased and replaced with a dummy after a successful lookup. This prevents the secondary cache from holding entries that are now in the primary cache.

### Statistics

Tracked in `Statistics` (Tickers):
- `COMPRESSED_SECONDARY_CACHE_HITS`: Successful lookups
- `COMPRESSED_SECONDARY_CACHE_DUMMY_HITS`: Lookups that hit dummy entries

Tracked in `PerfContext` (per-thread counters):
- `compressed_sec_cache_insert_real_count`: Real entries inserted
- `compressed_sec_cache_insert_dummy_count`: Dummy entries inserted
- `compressed_sec_cache_uncompressed_bytes`: Total uncompressed data size
- `compressed_sec_cache_compressed_bytes`: Total compressed data size

**Compression ratio** can be calculated as:
```
ratio = compressed_bytes / uncompressed_bytes
```

## TieredCache (Three-Tier Architecture)

Defined in `cache/tiered_secondary_cache.{h,cc}`, implements a three-tier cache hierarchy.

### Architecture

```
Primary Cache (Uncompressed)    -- Tier 1: Fast, in-memory
    | Evict/Promote
    v
Compressed Secondary (CompressedSC)  -- Tier 2: In-memory, compressed
    | Promote only
    v
NVM Secondary (e.g., Flash)     -- Tier 3: Persistent, slower
```

**Created via `NewTieredCache()`:**

```cpp
struct TieredCacheOptions {
  ShardedCacheOptions* cache_opts;           // Primary cache options
  PrimaryCacheType cache_type;               // LRU or HyperClockCache
  TieredAdmissionPolicy adm_policy;          // Admission policy
  CompressedSecondaryCacheOptions comp_cache_opts;  // Compressed tier options
  size_t total_capacity;                     // Total memory budget
  double compressed_secondary_ratio;         // Ratio for compressed tier
  std::shared_ptr<SecondaryCache> nvm_sec_cache;  // Optional NVM tier
};

std::shared_ptr<Cache> NewTieredCache(const TieredCacheOptions& opts);
```

### TieredSecondaryCache

Wraps a compressed secondary cache and an NVM secondary cache:

```cpp
class TieredSecondaryCache : public SecondaryCacheWrapper {
  TieredSecondaryCache(
      std::shared_ptr<SecondaryCache> comp_sec_cache,
      std::shared_ptr<SecondaryCache> nvm_sec_cache,
      TieredAdmissionPolicy adm_policy);
};
```

**Lookup flow:**
1. Try compressed secondary cache first
2. On miss, try NVM secondary cache
3. On NVM hit, optionally promote to compressed secondary cache

```cpp
std::unique_ptr<SecondaryCacheResultHandle> Lookup(...) {
  // Try compressed tier first
  auto result = target()->Lookup(key, helper, create_context, wait,
                                 advise_erase, stats, kept_in_sec_cache);
  if (result) return result;  // Hit in compressed tier

  // Miss in compressed tier, try NVM tier
  // Wrap create_context to intercept object creation and optionally
  // insert into compressed tier on promotion
  return nvm_sec_cache_->Lookup(key, outer_helper, wrapped_ctx, wait,
                                advise_erase, stats, kept_in_sec_cache);
}
```

**Promotion from NVM tier:**
- Uses `MaybeInsertAndCreate()` callback to intercept object creation
- On NVM hit, promotion to compressed tier only occurs when `!advise_erase && type != kNoCompression`
- Even when promotion is attempted, `CompressedSecondaryCache::InsertSaved()` applies its own admission logic: it silently skips insertion when `type == kNoCompression` or `enable_custom_split_merge == true`, and uses dummy-on-first-insert admission (first promotion creates only a placeholder; subsequent promotions insert real data)

**⚠️ INVARIANT**: `TieredSecondaryCache::Insert()` is a no-op (returns OK but does nothing). With `kAdmPolicyThreeQueue`, `CacheWithSecondaryAdapter::EvictionHandler()` also skips calling `Insert()`, so no demotion happens from primary cache at all. With other admission policies, demotion from primary to compressed tier happens via `CacheWithSecondaryAdapter::EvictionHandler()` calling `CompressedSecondaryCache::Insert()` directly.

### Admission Policies

Defined in `TieredAdmissionPolicy`:

| Policy | Description | Use Case |
|--------|-------------|----------|
| `kAdmPolicyAuto` | Auto-select based on configuration | Default |
| `kAdmPolicyPlaceholder` | Insert dummy on first eviction, real data on second | Compressed secondary only |
| `kAdmPolicyAllowCacheHits` | Same as Placeholder, but also demote cache hits | Compressed secondary only |
| `kAdmPolicyAllowAll` | Demote all evictions immediately | Compressed secondary only |
| `kAdmPolicyThreeQueue` | Three-tier admission policy | NVM + compressed tiers |

**kAdmPolicyThreeQueue** (for three-tier caches):
- **On primary eviction**: No demotion to secondary caches
- **On SST read miss**: Insert compressed block directly into NVM tier via `InsertSaved()`
- **On NVM tier hit**: Promote to primary cache, optionally insert into compressed tier
- **On compressed tier hit**: Promote to primary cache

**Example flow:**
```
1. Block read from SST (miss in all caches)
   → Insert compressed block into NVM tier

2. Later access (miss in primary, hit in NVM)
   → Decompress and promote to primary
   → If !advise_erase and data is compressed: attempt InsertSaved() into compressed tier
     (first hit typically creates only a dummy placeholder due to admission policy)

3. Another NVM hit for same key (dummy exists in compressed tier)
   → InsertSaved() inserts real compressed data into compressed tier

4. Future access (hit in compressed tier)
   → Decompress and promote to primary
```

**⚠️ INVARIANT**: With `kAdmPolicyThreeQueue`, blocks are written to NVM tier during SST reads via `InsertSaved()`, not via eviction from the primary cache.

## Row Cache

Row cache is a separate cache for storing serialized `GetContext` replay logs to speed up point lookups (`Get` and `MultiGet` operations). Each cached entry is a sequence of `ValueType` bytes plus length-prefixed values (and optional timestamps for user-defined timestamp columns), which captures merge operations and other `GetContext` state.

### Purpose

```
Without row cache:
Get(key) → Lookup in block cache → If miss, read SST block → Parse block → Extract value

With row cache:
Get(key) → Lookup in row cache → If hit, return value immediately
        → If miss, look up in block cache → ... → Save result in row cache
```

**Benefits:**
- Bypasses block cache lookup, block decompression, and binary search within blocks
- Stores the serialized result for a specific key, not the entire block
- Useful for workloads with high point lookup rate and small values

**Tradeoffs:**
- Duplicates data between row cache and block cache
- Less efficient for range scans (block cache is better)
- Incompatible with certain features (e.g., DeleteRange, bloom filters benefit is reduced)
- Disabled when the caller needs the returned sequence number (`NeedToReadSequence()` returns true), since row cache does not currently store sequence numbers

### Configuration

Configured via `DBOptions::row_cache`:

```cpp
struct DBOptions {
  // Used to speed up Get() queries.
  // NOTE: does not work with DeleteRange() yet.
  // Default: nullptr (disabled)
  std::shared_ptr<RowCache> row_cache = nullptr;
};
```

**Creating a row cache:**

```cpp
LRUCacheOptions opts;
opts.capacity = 512 * 1024 * 1024;  // 512 MB
options.row_cache = opts.MakeSharedRowCache();
```

**⚠️ IMPORTANT**: Row cache uses the same Cache interface as block cache, but **must not have a secondary cache attached**. `RowCache` is currently just an alias for `Cache`, but in the future may become a separate class.

### Row Cache Key Construction

Defined in `db/table_cache.cc:CreateRowCacheKeyPrefix()`:

```cpp
uint64_t CreateRowCacheKeyPrefix(const ReadOptions& options,
                                 const FileDescriptor& fd,
                                 const Slice& internal_key,
                                 GetContext* get_context,
                                 IterKey& row_cache_key) {
  // Key format:
  // row_cache_id_ (varint) + file_number (varint) + cache_entry_seq_no (varint) + user_key

  row_cache_key.TrimAppend(row_cache_key.Size(), row_cache_id_.data(),
                           row_cache_id_.size());
  AppendVarint64(&row_cache_key, fd_number);
  AppendVarint64(&row_cache_key, cache_entry_seq_no);
  // user_key appended later

  return cache_entry_seq_no == 0 ? 0 : cache_entry_seq_no - 1;
}
```

**Key components:**
1. **`row_cache_id_`**: Unique ID for this DB instance (disambiguates shared caches)
2. **`file_number`**: SST file number containing the key
3. **`cache_entry_seq_no`**: Visibility sequence number for snapshots
   - 0 if no snapshot or snapshot > file's largest seqno
   - 1 + internal_key's seqno otherwise
4. **`user_key`**: The actual user key

**Snapshot handling:**

```cpp
uint64_t cache_entry_seq_no = 0;
if (options.snapshot != nullptr &&
    (get_context->has_callback() ||
     options.snapshot->GetSequenceNumber() <= fd.largest_seqno)) {
  cache_entry_seq_no = 1 + GetInternalKeySeqno(internal_key);
}
```

**Why include sequence number?**
- Cached rows are specific to a snapshot view
- Different snapshots may see different values for the same user key
- Incrementing by 1 distinguishes from 0 (no snapshot)

**⚠️ INVARIANT**: The row cache key includes the file number, so cached entries become stale when files are compacted away. There is no active invalidation; entries simply miss and get repopulated.

### Row Cache Lookup and Insertion

**Lookup** (`TableCache::GetFromRowCache()`):

```cpp
bool GetFromRowCache(const Slice& user_key, IterKey& row_cache_key,
                     size_t prefix_size, GetContext* get_context,
                     Status* read_status, SequenceNumber seq_no) {
  row_cache_key.TrimAppend(prefix_size, user_key.data(), user_key.size());
  RowCacheInterface row_cache{ioptions_.row_cache.get()};

  if (auto row_handle = row_cache.Lookup(row_cache_key.GetUserKey())) {
    Cleanable value_pinner;
    row_cache.RegisterReleaseAsCleanup(row_handle, value_pinner);

    // Replay the "get context log" (serialized operations)
    *read_status = replayGetContextLog(*row_cache.Value(row_handle), user_key,
                                       get_context, &value_pinner, seq_no);
    RecordTick(ioptions_.stats, ROW_CACHE_HIT);
    return true;
  } else {
    RecordTick(ioptions_.stats, ROW_CACHE_MISS);
    return false;
  }
}
```

**Insertion** (`TableCache::Get()`):

```cpp
Status TableCache::Get(...) {
  std::string row_cache_entry_buffer;

  // Check row cache
  if (ioptions_.row_cache && !get_context->NeedToReadSequence()) {
    auto user_key = ExtractUserKey(k);
    uint64_t cache_entry_seq_no = CreateRowCacheKeyPrefix(..., row_cache_key);
    done = GetFromRowCache(user_key, row_cache_key, ..., &s, cache_entry_seq_no);
    if (!done) {
      row_cache_entry = &row_cache_entry_buffer;  // Enable recording
    }
  }

  if (!done && s.ok()) {
    // Perform table read, recording operations in row_cache_entry
    get_context->SetReplayLog(row_cache_entry);
    s = t->Get(options, k, get_context, ...);
    get_context->SetReplayLog(nullptr);
  }

  // Insert into row cache if something was found
  if (!done && s.ok() && row_cache_entry && !row_cache_entry->empty()) {
    RowCacheInterface row_cache{ioptions_.row_cache.get()};
    size_t charge = row_cache_entry->capacity() + sizeof(std::string);
    auto row_ptr = new std::string(std::move(*row_cache_entry));
    Status rcs = row_cache.Insert(row_cache_key.GetUserKey(), row_ptr, charge);
    if (!rcs.ok()) {
      delete row_ptr;  // Insertion failed (cache full)
    }
  }

  return s;
}
```

**Replay log format:**
- The row cache stores a serialized "replay log" of GetContext operations
- On cache hit, `replayGetContextLog()` replays these operations to reconstruct the result
- This captures merge operations, timestamps, and other GetContext state

**⚠️ INVARIANT**: Row cache entries include ownership of the cached string. On insertion failure, the caller must delete the string to avoid memory leaks.

### When Row Cache Helps

**Good use cases:**
- High rate of Get() operations for the same hot keys
- Small to medium-sized values (large values waste cache capacity)
- Workloads with high temporal locality
- Applications that can tolerate some staleness (entries keyed by file number become stale after compaction, though this does not affect correctness — stale entries simply miss and get repopulated)

**Poor use cases:**
- Range scans (use block cache instead)
- Large values (row cache not size-efficient)
- Workloads using DeleteRange() (not supported)
- Applications requiring strong consistency (snapshot sequence numbers add overhead)
- Workloads that need returned sequence numbers (row cache is bypassed when `NeedToReadSequence()` is true)

### Row Cache vs Block Cache

| Aspect | Row Cache | Block Cache |
|--------|-----------|-------------|
| **Granularity** | Serialized per-key replay logs | Entire data blocks (~4-32 KB) |
| **Lookup cost** | Direct hash lookup | Block lookup + binary search in block |
| **Space efficiency** | Lower (stores individual rows) | Higher (multiple rows per block) |
| **Range scan support** | No (must look up each key) | Yes (entire block cached) |
| **Best for** | Point lookups, hot keys | General read workloads |
| **Memory overhead** | Higher per row | Lower per row |
| **Invalidation** | Implicit (file number in key) | Explicit (cache eviction) |
| **Snapshot support** | Yes (seq no in key) | Yes (block-level) |
| **Secondary cache support** | Not allowed | Supported |

**Combined usage:**
- Row cache checked **before** block cache
- On row cache miss, fall through to block cache
- Results from block cache are inserted into row cache
- This creates a two-level caching hierarchy independent of the primary/secondary block cache tiers

## Statistics and Monitoring

### Secondary Cache Statistics

**Primary counters** (from `Tickers`):

```cpp
SECONDARY_CACHE_HITS             // Total hits in secondary cache
SECONDARY_CACHE_FILTER_HITS      // Secondary cache hits for filter blocks
SECONDARY_CACHE_INDEX_HITS       // Secondary cache hits for index blocks
SECONDARY_CACHE_DATA_HITS        // Secondary cache hits for data blocks
```

**Compressed secondary cache counters:**

```cpp
COMPRESSED_SECONDARY_CACHE_HITS           // Successful lookups
COMPRESSED_SECONDARY_CACHE_DUMMY_HITS     // Lookups hitting dummy entries
COMPRESSED_SECONDARY_CACHE_PROMOTIONS     // Promotions from NVM to compressed tier
COMPRESSED_SECONDARY_CACHE_PROMOTION_SKIPS // Skipped promotions
```

**PerfContext counters** (per-thread, used in benchmarking):

```cpp
secondary_cache_hit_count                     // Total secondary cache hits
block_cache_standalone_handle_count           // Standalone handles created
block_cache_real_handle_count                 // Real handles inserted into primary cache
compressed_sec_cache_insert_real_count        // Real entries inserted
compressed_sec_cache_insert_dummy_count       // Dummy entries inserted
compressed_sec_cache_uncompressed_bytes       // Bytes before compression
compressed_sec_cache_compressed_bytes         // Bytes after compression
```

**Calculating metrics:**

```cpp
// Compressed secondary cache compression ratio (from PerfContext)
compression_ratio = compressed_sec_cache_compressed_bytes /
                    compressed_sec_cache_uncompressed_bytes

// Dummy hit rate (measures admission effectiveness)
dummy_hit_rate = COMPRESSED_SECONDARY_CACHE_DUMMY_HITS /
                 (COMPRESSED_SECONDARY_CACHE_HITS + COMPRESSED_SECONDARY_CACHE_DUMMY_HITS)
```

### Row Cache Statistics

```cpp
ROW_CACHE_HIT         // Row cache lookups that found the key
ROW_CACHE_MISS        // Row cache lookups that missed
```

**Hit rate:**

```cpp
row_cache_hit_rate = ROW_CACHE_HIT / (ROW_CACHE_HIT + ROW_CACHE_MISS)
```

**Checking row cache effectiveness:**
- Compare Get() latency with vs without row cache
- Monitor `ROW_CACHE_HIT` rate; low hit rate indicates poor benefit
- Check cache capacity usage via `Cache::GetUsage()`

## Performance Considerations

### Secondary Cache

**Compression overhead:**
- Decompression on every secondary cache hit adds CPU cost
- Use faster compression algorithms (LZ4, Snappy) for lower latency
- Use higher compression (ZSTD) for better compression ratio if CPU is available

**Memory budget:**
- With `distribute_cache_res=true`, choose `compressed_secondary_ratio` based on workload
- Higher ratio → more compressed cache → better hit rate but less primary cache
- Typical values: 0.2 - 0.5 (20-50% of total budget)

**Admission policy:**
- `kAdmPolicyPlaceholder`: Reduces pollution, only caches frequently accessed blocks
- `kAdmPolicyAllowAll`: Caches everything, higher hit rate but more evictions
- `kAdmPolicyThreeQueue`: For NVM tier, balances memory and flash wear

**Async lookups:**
- Use `StartAsyncLookup()` + `WaitAll()` for MultiGet when using secondary caches that actually defer lookup work (e.g., NVM-backed caches)
- Note: `CompressedSecondaryCache` ignores the `wait` parameter, returns always-ready handles, and its `WaitAll()` is a no-op — async patterns provide no benefit for the built-in compressed cache alone
- Sync lookups (`wait=true`) block the calling thread

### Row Cache

**When to use:**
- Point lookup latency is critical
- Working set of hot keys fits in cache
- Values are small to medium-sized

**Sizing:**
- Larger row cache → higher hit rate but less memory for block cache
- Monitor hit rate; if < 20%, consider disabling or shrinking
- Typical: 10-30% of total cache budget for read-heavy workloads

**Avoiding pitfalls:**
- Don't use with range scans (wasted memory)
- Don't use with DeleteRange() (unsupported)
- Don't attach a secondary cache to row cache (not supported, will break in Phase 2)

## Example Configurations

### Compressed Secondary Cache Only

```cpp
LRUCacheOptions cache_opts;
cache_opts.capacity = 8ULL << 30;  // 8 GB primary cache

CompressedSecondaryCacheOptions sec_cache_opts;
sec_cache_opts.capacity = 2ULL << 30;  // 2 GB compressed secondary
sec_cache_opts.compression_type = kLZ4Compression;

cache_opts.secondary_cache = NewCompressedSecondaryCache(sec_cache_opts);

BlockBasedTableOptions table_options;
table_options.block_cache = cache_opts.MakeSharedCache();
options.table_factory.reset(NewBlockBasedTableFactory(table_options));
```

### Tiered Cache (Three-Tier)

```cpp
TieredCacheOptions tiered_opts;
tiered_opts.total_capacity = 10ULL << 30;  // 10 GB total
tiered_opts.compressed_secondary_ratio = 0.3;  // 30% for compressed tier
tiered_opts.cache_type = PrimaryCacheType::kCacheTypeHCC;  // HyperClockCache
tiered_opts.adm_policy = TieredAdmissionPolicy::kAdmPolicyThreeQueue;

HyperClockCacheOptions primary_opts(0 /* capacity, overridden by total_capacity */,
                                     0 /* estimated_entry_charge */);
tiered_opts.cache_opts = &primary_opts;

tiered_opts.comp_cache_opts.compression_type = kZSTD;

// Optional: Add NVM tier (e.g., local flash cache)
tiered_opts.nvm_sec_cache = my_nvm_cache;

BlockBasedTableOptions table_options;
table_options.block_cache = NewTieredCache(tiered_opts);
options.table_factory.reset(NewBlockBasedTableFactory(table_options));
```

### Row Cache

```cpp
LRUCacheOptions row_cache_opts;
row_cache_opts.capacity = 1ULL << 30;  // 1 GB row cache
options.row_cache = row_cache_opts.MakeSharedRowCache();

LRUCacheOptions block_cache_opts;
block_cache_opts.capacity = 8ULL << 30;  // 8 GB block cache

BlockBasedTableOptions table_options;
table_options.block_cache = block_cache_opts.MakeSharedCache();
options.table_factory.reset(NewBlockBasedTableFactory(table_options));
```

### Combined: Tiered Block Cache + Row Cache

```cpp
// Tiered block cache
TieredCacheOptions tiered_opts;
tiered_opts.total_capacity = 8ULL << 30;
tiered_opts.compressed_secondary_ratio = 0.25;
LRUCacheOptions primary_opts;
tiered_opts.cache_opts = &primary_opts;

BlockBasedTableOptions table_options;
table_options.block_cache = NewTieredCache(tiered_opts);
options.table_factory.reset(NewBlockBasedTableFactory(table_options));

// Row cache
LRUCacheOptions row_cache_opts;
row_cache_opts.capacity = 512ULL << 20;  // 512 MB row cache
options.row_cache = row_cache_opts.MakeSharedRowCache();

// Lookup order: row cache → primary block cache → compressed secondary cache
```

## Source Code References

### Secondary Cache Interface
- `include/rocksdb/secondary_cache.h` - SecondaryCache and SecondaryCacheResultHandle interfaces
- `cache/secondary_cache_adapter.cc` - CacheWithSecondaryAdapter implementation
- `cache/compressed_secondary_cache.cc` - CompressedSecondaryCache implementation
- `cache/tiered_secondary_cache.{h,cc}` - TieredSecondaryCache implementation

### Row Cache Integration
- `include/rocksdb/options.h` - DBOptions::row_cache configuration
- `include/rocksdb/cache.h` - RowCache type alias and MakeSharedRowCache factory
- `db/table_cache.cc` - Row cache key construction and Get() integration
  - `CreateRowCacheKeyPrefix()` - Key construction (line ~400)
  - `GetFromRowCache()` - Lookup (line ~442)
  - `TableCache::Get()` - Insertion (line ~473)

### Cache Statistics
- `include/rocksdb/statistics.h` - Ticker definitions (SECONDARY_CACHE_*, ROW_CACHE_*, COMPRESSED_SECONDARY_CACHE_*)
- `include/rocksdb/perf_context.h` - PerfContext counter field definitions

### Tiered Cache Configuration
- `include/rocksdb/cache.h` - TieredCacheOptions, TieredAdmissionPolicy, NewTieredCache(), UpdateTieredCache()
- `cache/secondary_cache_adapter.cc:NewTieredCache()` - Factory function (line ~651)
- `cache/secondary_cache_adapter.cc:UpdateTieredCache()` - Dynamic update (line ~728)
