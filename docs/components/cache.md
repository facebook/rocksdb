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

`Cache` (extends `Customizable`) maps variable-length `Slice` keys to opaque objects (`ObjectPtr = void*`), tracks reference counts, and evicts unreferenced entries to stay at or below a configured capacity target. Note: with `strict_capacity_limit=false` (the default), inserts may succeed even when the cache exceeds its target capacity. Only HyperClockCache restricts keys to exactly 16 bytes.

### Core Methods

| Method | Description |
|--------|-------------|
| `Insert(key, obj, helper, charge, handle**, priority, compressed, type)` | Insert mapping; takes ownership of obj on success. Optional `compressed`/`type` allow also caching compressed data. |
| `CreateStandalone(key, obj, helper, charge, allow_uncharged)` | Create an entry not findable via Lookup (e.g. for memory charging or secondary cache promotion) |
| `Lookup(key, helper, create_context, priority, stats)` | Find entry; optionally queries secondary cache |
| `Ref(handle)` | Increment reference count |
| `Release(handle, erase_if_last_ref)` | Decrement refcount; optionally erase on last ref |
| `Value(handle)` | Get object pointer from handle |
| `Erase(key)` | Remove entry (deferred if still referenced) |
| `SetCapacity(size_t)` | Dynamically change capacity |
| `SetStrictCapacityLimit(bool)` | Set whether Insert fails when over capacity |
| `HasStrictCapacityLimit()` | Query strict capacity limit setting |
| `GetCapacity()` | Returns maximum configured capacity |
| `GetUsage()` / `GetUsage(handle)` | Returns memory size for all entries or a specific entry |
| `GetPinnedUsage()` | Returns memory size for entries in use by the system |
| `GetCharge(handle)` | Returns the charge for a specific entry |
| `GetCacheItemHelper(handle)` | Returns the helper for a specific entry |
| `StartAsyncLookup(async_handle)` | Begin async lookup for secondary cache |
| `WaitAll(handles, count)` | Wait for async lookups to complete |

### CacheItemHelper

Struct with function pointers for cache entry lifecycle:
- `del_cb` -- destructor callback
- `size_cb` / `saveto_cb` -- serialization for secondary cache
- `create_cb` -- deserialization from secondary cache
- `CacheEntryRole` -- classification for monitoring
- `without_secondary_compat` -- pointer to an equivalent helper without secondary cache support; used to prevent re-insertion into secondary cache when a promoted entry's secondary copy is intentionally kept

### Priority Levels

| Priority | Eviction Order | Use Case |
|----------|---------------|----------|
| `HIGH` | Evicted last | Index blocks, filter blocks |
| `LOW` | Default | Data blocks |
| `BOTTOM` | Evicted first | BlobDB blob values, compaction block cache warming |

### CacheEntryRole

Classifies entries for monitoring and memory tracking:
`kDataBlock`, `kFilterBlock`, `kFilterMetaBlock`, `kDeprecatedFilterBlock`, `kIndexBlock`, `kOtherBlock`, `kWriteBuffer`, `kCompressionDictionaryBuildingBuffer`, `kFilterConstruction`, `kBlockBasedTableReader`, `kFileMetadata`, `kBlobValue`, `kBlobCache`, `kMisc`

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

### Standalone Handles

`Cache::CreateStandalone()` creates entries that are **not** inserted into the hash table and thus cannot be found by `Lookup()`. They are used for:
- **Memory charging**: reserving capacity without a findable entry (e.g. `CacheReservationManager`)
- **Secondary cache promotion**: on the first secondary cache hit (no dummy in primary), the adapter creates a standalone handle to return to the caller while inserting a dummy to record recent use. This avoids evicting useful primary entries to make room for a potentially one-time access.

In `LRUHandle`, the `IM_IS_STANDALONE` flag marks such entries.

### Three-Tier LRU List

Single doubly-linked list with marker pointers dividing it into three priority regions:

```
[HIGH priority entries] [LOW priority entries] [BOTTOM priority entries]
  ^                       ^                       ^
  lru_.prev (newest)      lru_low_pri_            lru_bottom_pri_
                                                  lru_.next (oldest, evicted first)
```

- `EvictFromLRU()` removes entries from the head (`lru_.next`, oldest) until enough space is freed
- `MaintainPoolSize()` spills high-pri entries into the low-pri pool and low-pri entries into the bottom-pri pool when their respective pool ratios are exceeded
- On `LRU_Insert`, entries with cache hits (`HasHit()`) are inserted into the highest available priority pool; entries without hits are inserted into the highest pool whose priority does not exceed the entry's own priority

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

Lock-free alternative to LRUCache, optimized for high-concurrency block cache workloads. **Now generally recommended over LRUCache.** Note: HyperClockCache is **not a general-purpose `Cache`** -- it can only be used for `BlockBasedTableOptions::block_cache`. It is not appropriate for row cache or table cache uses.

### Key Differences from LRU

| Aspect | LRUCache | HyperClockCache |
|--------|----------|-----------------|
| Concurrency | Mutex per shard | Fixed table: fully lock/wait-free; Auto table: rare waits on some insert/erase paths |
| Lookup cost | Mutex + LRU list update | Most Lookup() and essentially all Release() are a single atomic fetch_add |
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

**FixedHyperClockTable**: Pre-allocated open-addressing hash table. Table size fixed at creation based on `estimated_entry_charge`. Target load factor is 0.7 (`kLoadFactor`), with a strict upper bound of 0.84 (`kStrictLoadFactor`) to avoid degenerate performance when actual entry sizes differ from estimates.

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
| `Insert(key, obj, helper, force_insert)` | Serialize and store evicted entry; `force_insert` bypasses admission policy |
| `InsertSaved(key, saved, type, source)` | Insert from saved/persistable data (e.g. for warming from compressed blocks) |
| `Lookup(key, helper, create_context, wait, advise_erase, stats, kept_in_sec_cache)` | Async-capable lookup; `advise_erase` hints that primary will cache the entry; `kept_in_sec_cache` output indicates if entry remains in secondary |
| `SupportForceErase()` | Whether the implementation supports advised erase on Lookup |
| `Erase(key)` | Remove entry |
| `WaitAll(handles)` | Batch wait for async lookups |
| `SetCapacity(capacity)` / `GetCapacity(capacity)` | Dynamically adjust or query capacity |
| `Deflate(decrease)` / `Inflate(increase)` | Lightweight temporary capacity adjustments (single-shard) |

### CacheWithSecondaryAdapter

`CacheWrapper` that integrates primary + secondary cache:

- **Insert**: primary cache; on eviction, demotes to secondary (except under `kAdmPolicyThreeQueue`, where evictions are not demoted)
- **Lookup**: primary first; on miss, queries secondary and promotes result
- **Admission policy** (`TieredAdmissionPolicy`):
  - `kAdmPolicyAuto` -- automatically select the admission policy
  - `kAdmPolicyPlaceholder` -- insert placeholder on first eviction, full entry on second hit
  - `kAdmPolicyAllowCacheHits` -- also force-insert primary hits into secondary
  - `kAdmPolicyThreeQueue` -- three independent tiers (primary, compressed, NVM); evictions are not demoted, lower tiers are warmed via `InsertSaved()`
  - `kAdmPolicyAllowAll` -- admit all evicted blocks

---

## 5. CompressedSecondaryCache

**Files:** `cache/compressed_secondary_cache.h`, `cache/compressed_secondary_cache.cc`

### What It Does

Stores blocks in memory using an internal LRU cache, compressing most entries with LZ4 by default. Entries in `do_not_compress_roles` (default: `{kFilterBlock}`) are stored uncompressed. Compression can also fall back to `kNoCompression` if the compressed output is not smaller than the original.

### Two-Hit Admission Protocol

Prevents cache pollution from one-time accesses:

1. **On primary eviction**: if no dummy in secondary -> insert dummy (size 0); if dummy exists -> insert compressed block
2. **On secondary lookup hit with `advise_erase`**: erases the real secondary entry and inserts a dummy placeholder in its place (not a plain erase)
3. **On secondary hit with no dummy in primary** (`!found_dummy_entry`): the adapter creates a **standalone handle** (not inserted into the hash table) and inserts a dummy into primary cache to record recent use; the entry is kept in secondary cache
4. **On secondary hit with dummy in primary** (`found_dummy_entry`): the adapter promotes the full entry into primary cache and erases it from secondary

### Options

`CompressedSecondaryCacheOptions` inherits all `LRUCacheOptions` (capacity, sharding, strict capacity, allocator, metadata charging, etc.) plus:

| Option | Description |
|--------|-------------|
| `compression_type` | Compression algorithm (default LZ4) |
| `compression_opts` | Options specific to the compression algorithm |
| `enable_custom_split_merge` | Split compressed values into jemalloc-friendly chunks |
| `do_not_compress_roles` | Roles stored uncompressed (default: `{kFilterBlock}`) |

Note: `InsertSaved()` is a no-op when `type == kNoCompression` or when `enable_custom_split_merge` is true.

---

## 6. CacheReservationManager

**Files:** `cache/cache_reservation_manager.h`

### What It Does

Allows non-block-cache memory consumers (WriteBufferManager, filter construction, compression dictionaries, file metadata, table readers, blob cache) to "reserve" capacity in the block cache by inserting dummy entries (256KB each). This ensures their memory is accounted for in the cache's capacity limit. The tiered-cache adapter also uses reservations to distribute budget between primary and secondary caches.

### Key Methods

| Method | Description |
|--------|-------------|
| `UpdateCacheReservation(new_size)` | Adjust dummy entries to match new total |
| `UpdateCacheReservation(memory_used_delta, increase)` | Adjust by a delta (increase or decrease) |
| `MakeCacheReservation(size, handle*)` | RAII-style reservation (released on handle destruction) |
| `GetTotalReservedCacheSize()` | Actual reserved (multiple of 256KB) |
| `GetTotalMemoryUsed()` | Latest total memory used as indicated by caller |

### Thread Safety

`CacheReservationManagerImpl<R>` is **not thread-safe**, except that `GetTotalReservedCacheSize()` can be called without external synchronization (it reads an atomic).

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

**Files:** `include/rocksdb/cache.h`, `cache/tiered_secondary_cache.h`, `cache/secondary_cache_adapter.cc`

`NewTieredCache()` creates a 2-tier (or 3-tier) cache combining a primary (LRU or HCC) with a compressed secondary cache. Memory reservation is distributed proportionally between tiers. `UpdateTieredCache()` allows dynamically adjusting capacity and compressed secondary ratio.

```cpp
TieredCacheOptions opts;
opts.cache_type = PrimaryCacheType::kCacheTypeHCC;
opts.capacity = total_capacity;
opts.compressed_secondary_ratio = 0.3;  // 30% for compressed tier
// Optional NVM tier:
// opts.nvm_sec_cache = nvm_cache;
// opts.adm_policy = TieredAdmissionPolicy::kAdmPolicyThreeQueue;
auto cache = NewTieredCache(opts);
```

### TieredSecondaryCache

When `nvm_sec_cache` is provided with `kAdmPolicyThreeQueue`, a `TieredSecondaryCache` stacks a compressed secondary cache on top of a non-volatile (local flash) cache. The admission policy warms the NVM tier via `InsertSaved()` from compressed blocks read from SSTs. Evicted blocks from the primary are **not** demoted -- they are simply discarded. Promotion happens on hits in the NVM tier, flowing through the compressed tier and into the primary cache.

---

## Interactions With Other Components

- **SST Format** (see [sst_table_format.md](sst_table_format.md)): Data blocks, index blocks, and filter blocks are cached. `BlockBasedTable::RetrieveBlock()` is the main cache consumer.
- **Read Path** (see [flush_and_read_path.md](flush_and_read_path.md)): Point lookups and iterators access blocks through the cache. `TableCache` caches table reader objects.
- **Write Path** (see [write_path.md](write_path.md)): `WriteBufferManager` uses `CacheReservationManager` to account memtable memory against cache capacity.
- **Compaction** (see [compaction.md](compaction.md)): Compaction may bypass block cache (`fill_cache=false`) to avoid polluting with cold data.
- **File I/O** (see [file_io_and_blob.md](file_io_and_blob.md)): `RandomAccessFileReader` reads blocks from disk when cache misses occur.
