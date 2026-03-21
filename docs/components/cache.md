# Cache

## Overview

RocksDB's cache subsystem manages in-memory caching of SST file blocks (data, index, filter) to reduce disk I/O. Two primary cache implementations exist: LRUCache (traditional, mutex-based) and HyperClockCache (lock-free, recommended for high concurrency). An optional secondary cache tier supports compressed in-memory or flash-based caching. CacheReservationManager enables non-block-cache memory consumers to participate in the cache's capacity budget.

### Architecture

```
                    +-------------------+
                    |  Cache (abstract)  |  include/rocksdb/advanced_cache.h
                    |  Insert/Lookup/... |
                    +---------+---------+
                              |
              +---------------+------------------+
              |               |                  |
    +---------+------+  +-----+--------+  +------+--------------+
    |   LRUCache     |  | HyperClock   |  | CacheWithSecondary  |
    | (mutex per     |  |   Cache      |  |     Adapter          |
    |  shard)        |  | (lock-free)  |  |   (CacheWrapper)     |
    +----------------+  +--------------+  +---------+------------+
                                                    |
                                            +-------+-------+
                                            | SecondaryCache |
                                            |  (abstract)    |
                                            +-------+-------+
                                                    |
                                          +---------+----------+
                                          | CompressedSecondary|
                                          |     Cache          |
                                          +--------------------+
```

---

## 1. Cache Interface

**Files:** `include/rocksdb/advanced_cache.h`, `include/rocksdb/cache.h`

### Abstract Base

`Cache` (extends `Customizable`) maps 16-byte keys to opaque objects (`ObjectPtr = void*`), tracks reference counts, and evicts unreferenced entries to stay within capacity.

### Core Methods

| Method | Description |
|--------|-------------|
| `Insert(key, obj, helper, charge, handle**, priority)` | Insert mapping; takes ownership of obj on success |
| `Lookup(key, helper, create_context, priority, stats)` | Find entry; optionally queries secondary cache |
| `Ref(handle)` | Increment reference count |
| `Release(handle, erase_if_last_ref)` | Decrement refcount; optionally erase on last ref |
| `Value(handle)` | Get object pointer from handle |
| `Erase(key)` | Remove entry (deferred if still referenced) |
| `SetCapacity(size_t)` | Dynamically change capacity |
| `StartAsyncLookup(async_handle)` | Begin async lookup for secondary cache |
| `WaitAll(handles, count)` | Wait for async lookups to complete |

### CacheItemHelper

Struct with function pointers for cache entry lifecycle:
- `del_cb` -- destructor callback
- `size_cb` / `saveto_cb` -- serialization for secondary cache
- `create_cb` -- deserialization from secondary cache
- `CacheEntryRole` -- classification for monitoring

### Priority Levels

| Priority | Eviction Order | Use Case |
|----------|---------------|----------|
| `HIGH` | Evicted last | Index blocks, filter blocks |
| `LOW` | Default | Data blocks |
| `BOTTOM` | Evicted first | Speculative prefetch |

### CacheEntryRole

Classifies entries for monitoring and memory tracking:
`kDataBlock`, `kFilterBlock`, `kFilterMetaBlock`, `kIndexBlock`, `kOtherBlock`, `kWriteBuffer`, `kFilterConstruction`, `kBlockBasedTableReader`, `kFileMetadata`, `kBlobValue`, `kBlobCache`, `kMisc`

---

## 2. LRUCache

**Files:** `cache/lru_cache.h`, `cache/lru_cache.cc`

### Design

Sharded LRU cache. Each shard has its own mutex, hash table, and LRU list. `class LRUCache : public ShardedCache<LRUCacheShard>`.

### LRUHandle

Variable-length heap-allocated entry with embedded key data. State machine:

| State | Condition | Location |
|-------|-----------|----------|
| Referenced + in cache | `refs >= 1 && in_cache` | NOT on LRU list |
| Unreferenced + in cache | `refs == 0 && in_cache` | ON LRU list (evictable) |
| Referenced + not in cache | `refs >= 1 && !in_cache` | Erased but held by caller |

Key fields: `value`, `helper`, `total_charge`, `hash`, `refs`, priority flags.

### Three-Tier LRU List

Single doubly-linked list with marker pointers dividing it into three priority regions:

```
[HIGH priority entries] [LOW priority entries] [BOTTOM priority entries]
  ^                       ^                       ^
  lru_.prev (newest)      lru_low_pri_            lru_bottom_pri_
                                                  lru_.next (oldest, evicted first)
```

- `EvictFromLRU()` removes entries from the tail (oldest) until enough space is freed
- `MaintainPoolSize()` spills entries when high-pri pool ratio is exceeded
- Entries with cache hits are promoted to high-pri pool on next insertion

### LRU Cache Options

| Option | Default | Description |
|--------|---------|-------------|
| `capacity` | 0 | Total cache capacity |
| `num_shard_bits` | -1 (auto) | Log2 of shard count |
| `high_pri_pool_ratio` | 0.5 | Fraction for high-priority entries |
| `low_pri_pool_ratio` | 0.0 | Fraction for low-priority entries |
| `strict_capacity_limit` | false | Fail Insert when over capacity |
| `use_adaptive_mutex` | platform | Adaptive mutexes for shard locking |

---

## 3. HyperClockCache

**Files:** `cache/clock_cache.h`, `cache/clock_cache.cc`

### Design

Lock-free alternative to LRUCache, optimized for high-concurrency block cache workloads. **Now generally recommended over LRUCache.**

### Key Differences from LRU

| Aspect | LRUCache | HyperClockCache |
|--------|----------|-----------------|
| Concurrency | Mutex per shard | Lock/wait-free (atomics) |
| Lookup cost | Mutex + LRU list update | Single atomic fetch_add |
| Eviction | Strict LRU (oldest first) | CLOCK with countdown aging |
| Key size | Variable length | Fixed 16 bytes only |
| Table resizing | Dynamic | Fixed (FixedHCC) or auto-grow (AutoHCC) |

### ClockHandle State

All state encoded in a single atomic 64-bit `meta` word:

```
| acquire counter (30b) | release counter (30b) | hit (1b) | occupied (1b) | shareable (1b) | visible (1b) |
```

- **Refcount** = acquire_counter - release_counter
- **Countdown clock** = acquire_counter (when refcount == 0)
- Initial countdown based on priority: HIGH=3, LOW=2, BOTTOM=1

### Table Variants

**FixedHyperClockTable**: Pre-allocated open-addressing hash table. Table size fixed at creation based on `estimated_entry_charge`. Load factor capped at 0.7.

**AutoHyperClockTable** (recommended): Uses anonymous mmap for dynamic growth. Chaining with interleaved head/entry slots. Linear hashing for growth without moving entries. Load factor up to 0.60.

### Eviction Algorithm

Parallel CLOCK sweep: threads atomically increment `clock_pointer_` by 4 (batch). For each slot:
- Unreferenced + countdown > 0: decrement countdown (aging)
- Unreferenced + countdown == 0: evict
- Referenced: skip

`eviction_effort_cap` (default 30) limits CPU waste when most entries are pinned.

### HyperClockCache Options

| Option | Default | Description |
|--------|---------|-------------|
| `estimated_entry_charge` | 0 | 0 = auto-resize (recommended); >0 = fixed table |
| `min_avg_entry_charge` | 450 | Lower bound on average charge (auto-resize mode) |
| `eviction_effort_cap` | 30 | Limits CPU spent on eviction under thrashing |

---

## 4. Secondary Cache

**Files:** `include/rocksdb/secondary_cache.h`, `cache/secondary_cache_adapter.h`

### Interface

`SecondaryCache` (abstract, extends `Customizable`) provides a second-tier cache for evicted primary cache entries. Supports async lookups.

| Method | Description |
|--------|-------------|
| `Insert(key, obj, helper)` | Serialize and store evicted entry |
| `Lookup(key, helper, create_context, wait)` | Async-capable lookup, returns `SecondaryCacheResultHandle` |
| `Erase(key)` | Remove entry |
| `WaitAll(handles)` | Batch wait for async lookups |

### CacheWithSecondaryAdapter

`CacheWrapper` that integrates primary + secondary cache:

- **Insert**: primary cache; on eviction, demotes to secondary
- **Lookup**: primary first; on miss, queries secondary and promotes result
- **Admission policy** (`TieredAdmissionPolicy`):
  - `kAdmPolicyPlaceholder` -- insert placeholder on first eviction, full entry on second hit
  - `kAdmPolicyAllowCacheHits` -- also force-insert primary hits into secondary
  - `kAdmPolicyAllowAll` -- admit all evicted blocks

---

## 5. CompressedSecondaryCache

**Files:** `cache/compressed_secondary_cache.h`, `cache/compressed_secondary_cache.cc`

### What It Does

Stores compressed blocks in memory using an internal LRU cache. Uses LZ4 compression by default.

### Two-Hit Admission Protocol

Prevents cache pollution from one-time accesses:

1. **On primary eviction**: if no dummy in secondary -> insert dummy (size 0); if dummy exists -> insert compressed block
2. **On secondary lookup hit**: if no dummy in primary -> insert dummy in primary, keep in secondary; if dummy in primary -> promote to primary, erase from secondary

### Options

| Option | Description |
|--------|-------------|
| `compression_type` | Compression algorithm (default LZ4) |
| `enable_custom_split_merge` | Split compressed values into jemalloc-friendly chunks |
| `do_not_compress_roles` | Roles stored uncompressed (default: `{kFilterBlock}`) |

---

## 6. CacheReservationManager

**Files:** `cache/cache_reservation_manager.h`

### What It Does

Allows non-block-cache memory consumers (WriteBufferManager, filter construction, compression dictionaries) to "reserve" capacity in the block cache by inserting dummy entries (256KB each). This ensures their memory is accounted for in the cache's capacity limit.

### Key Methods

| Method | Description |
|--------|-------------|
| `UpdateCacheReservation(new_size)` | Adjust dummy entries to match new total |
| `MakeCacheReservation(size, handle*)` | RAII-style reservation (released on handle destruction) |
| `GetTotalReservedCacheSize()` | Actual reserved (multiple of 256KB) |

### Delayed Decrease

When `delayed_decrease` is enabled, dummy entries are not released until memory drops to 3/4 of reserved. Avoids churn from oscillating usage.

### ConcurrentCacheReservationManager

Thread-safe wrapper using `std::mutex`. RAII handles lock the mutex on destruction.

---

## 7. Block Cache Keys

**Files:** `cache/cache_key.h`, `table/block_based/block_cache.h`

### CacheKey

Fixed 16-byte key: `file_num_etc64_ (8B) + offset_etc64_ (8B)`.

### OffsetableCacheKey

Generates per-file cache keys from `(db_id, db_session_id, file_number)`. Block offsets within a file derive keys via XOR: `CacheKey(file_num_etc64_, offset_etc64_ ^ offset)`.

Properties:
- Stable across DB open/close, backup/restore
- Globally unique with very high probability
- Fast (single XOR on hot path)
- 8-byte common prefix per file for efficient prefix operations

### Block Type Wrappers

Thin wrapper classes over `Block` that add `kCacheEntryRole` and `kBlockType`:

| Wrapper | Role | Block Type |
|---------|------|------------|
| `Block_kData` | `kDataBlock` | `kData` |
| `Block_kIndex` | `kIndexBlock` | `kIndex` |
| `Block_kFilterPartitionIndex` | `kFilterMetaBlock` | `kFilterPartitionIndex` |
| `Block_kRangeDeletion` | `kOtherBlock` | `kRangeDeletion` |
| `Block_kMetaIndex` | `kOtherBlock` | `kMetaIndex` |

### BlockCreateContext

Extends `Cache::CreateContext` with block-specific parameters for reconstruction from secondary cache: `table_options`, `decompressor`, `protection_bytes_per_key`, restart intervals.

---

## Tiered Cache

**Files:** `include/rocksdb/cache.h`

`NewTieredCache()` creates a 2-tier cache combining a primary (LRU or HCC) with a compressed secondary cache. Memory reservation is distributed proportionally between tiers.

```cpp
TieredCacheOptions opts;
opts.cache_type = PrimaryCacheType::kCacheTypeHCC;
opts.capacity = total_capacity;
opts.compressed_secondary_ratio = 0.3;  // 30% for compressed tier
auto cache = NewTieredCache(opts);
```

---

## Interactions With Other Components

- **SST Format** (see [sst_table_format.md](sst_table_format.md)): Data blocks, index blocks, and filter blocks are cached. `BlockBasedTable::RetrieveBlock()` is the main cache consumer.
- **Read Path** (see [flush_and_read_path.md](flush_and_read_path.md)): Point lookups and iterators access blocks through the cache. `TableCache` caches table reader objects.
- **Write Path** (see [write_path.md](write_path.md)): `WriteBufferManager` uses `CacheReservationManager` to account memtable memory against cache capacity.
- **Compaction** (see [compaction.md](compaction.md)): Compaction may bypass block cache (`fill_cache=false`) to avoid polluting with cold data.
- **File I/O** (see [file_io_and_blob.md](file_io_and_blob.md)): `RandomAccessFileReader` reads blocks from disk when cache misses occur.
