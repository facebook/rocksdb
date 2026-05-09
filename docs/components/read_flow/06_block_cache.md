# Block Cache Integration

**Files:** `table/block_based/block_based_table_reader.cc`, `table/block_based/block_cache.cc`, `cache/cache_key.h`, `cache/typed_cache.h`, `include/rocksdb/cache.h`

## Multi-Tier Cache Architecture

The read path uses a tiered caching strategy:

Step 1: **Uncompressed block cache** (primary) -- `LRUCache` or `HyperClockCache` holding decompressed data blocks, index blocks, and filter blocks. This is the fastest tier.

Step 2: **Compressed/secondary cache** -- `CompressedSecondaryCache` or a user-provided `SecondaryCache` implementation. Holds compressed blocks that were evicted from the primary cache.

Step 3: **Disk I/O** -- Read the block from the SST file via `ReadBlockContents()`, verify checksum, decompress if needed.

The `RetrieveBlock()` method in `BlockBasedTableReader` orchestrates this lookup chain.

## Cache Key Construction

Cache keys must be globally unique across all SST files and databases sharing a cache.

**Per-file base key:** `OffsetableCacheKey` is constructed from `db_id`, `db_session_id`, and `file_number` (see `SetupBaseCacheKey()` in `block_based_table_reader.cc`). This provides global uniqueness with high probability.

**Per-block key derivation:** `GetCacheKey()` XORs the base key with `block_handle.offset() >> 2`. This produces a 16-byte fixed-size key (two `uint64_t` values) with an 8-byte common prefix per file. The XOR operation is very fast on the hot path.

**Stability:** SST files with `db_session_id` and `orig_file_number` properties (newer SSTs) have stable cache keys across DB open/close and backup/restore. Older SSTs lacking these properties use a fallback key that is NOT stable across restarts.

**Key Invariant:** Cache keys must be globally unique. Collisions would cause incorrect data returns.

## Cache Miss Handling

When `RetrieveBlock()` misses in the cache (see `MaybeReadBlockAndLoadToCache()` in `block_based_table_reader.cc`):

Step 1: **Read from disk** -- `BlockFetcher` reads the block from the SST file. If `ReadOptions::async_io` is enabled and a `FilePrefetchBuffer` is available, uses `ReadAsyncBlockContents()` for asynchronous I/O. Otherwise, uses synchronous `ReadBlockContents()`.

Step 2: **Verify checksum** -- Checksum is verified during the fetch, before decompression.

Step 3: **Decompress** -- If the block is compressed (`compression_type != kNoCompression`), decompress it. The compressed data is retained separately if a secondary cache is configured.

Step 4: **Insert into cache** -- `PutDataBlockToCache()` inserts:
- Uncompressed block into the primary cache
- Compressed block into the secondary cache (if configured)

Step 5: **Fallback (no caching)** -- When `ReadOptions::fill_cache` is false or the block has no owned memory, the block is read via `ReadAndParseBlockFromFile()` and returned directly without cache insertion.

## Cache Admission Policy

Three conditions must be met for cache insertion (see `BlockBasedTableReader` in `block_based_table_reader.cc`):

1. `ReadOptions::fill_cache == true` (default)
2. `ReadOptions::read_tier != kBlockCacheTier` (no I/O restriction)
3. Block has owned memory (`block_holder->own_bytes()`)

## Priority Levels

Cache entries are inserted with different eviction priorities:

| Priority | Eviction Order | Use Case |
|----------|---------------|----------|
| `Cache::Priority::HIGH` | Evicted last | Index and filter blocks (when `cache_index_and_filter_blocks_with_high_priority` is true) |
| `Cache::Priority::LOW` | Default | Data blocks, table properties |
| `Cache::Priority::BOTTOM` | Evicted first | Speculative prefetch |

This priority scheme ensures index and filter blocks (frequently reused across many lookups) stay in cache longer than data blocks.

## CacheItemHelper Lifecycle

Each cached block type has a `CacheItemHelper` (see `CacheItemHelper` in `cache/typed_cache.h`) providing lifecycle callbacks:

| Callback | Purpose |
|----------|---------|
| `del_cb` | Destructor: frees block memory |
| `size_cb` | Returns serialization size for secondary cache |
| `saveto_cb` | Serializes block to secondary cache format |
| `create_cb` | Deserializes block from secondary cache (includes decompression) |

## ReadTier Option

`ReadOptions::read_tier` controls which tiers are accessible:

| Tier | Behavior |
|------|----------|
| `kReadAllTier` (default) | Search cache and disk |
| `kBlockCacheTier` | Cache only -- return `Status::Incomplete()` on cache miss |
| `kPersistedTier` | Get/MultiGet only: skip memtables when `has_unpersisted_data_` is true. Returns `Status::NotSupported()` for iterators. |

`kBlockCacheTier` is useful for latency-sensitive reads where a cache miss should fail fast rather than blocking on disk I/O.

## Block Types in Cache

Three types of blocks are cached:

| Block Type | Always Cached | Notes |
|------------|--------------|-------|
| Data blocks | When `fill_cache=true` | Contains key-value pairs |
| Index blocks | When `cache_index_and_filter_blocks=true` | Top-level only; partition blocks always use block cache |
| Filter blocks | When `cache_index_and_filter_blocks=true` | Top-level only; partition blocks always use block cache |

When `cache_index_and_filter_blocks` is false (default), top-level index and filter blocks are loaded when the table reader opens and pinned in memory for the table reader's lifetime, bypassing the cache entirely. However, index and filter **partition** blocks (used with `kTwoLevelIndexSearch` or `partition_filters=true`) always go through the block cache regardless of this setting.
