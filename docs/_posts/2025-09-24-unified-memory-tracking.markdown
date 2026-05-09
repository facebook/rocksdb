---
title: Unified Memory Tracking
layout: post
author: hx235
category: blog
---

## Context / Problem
Modern RocksDB deployments often run in environments with strict memory constraints—cloud VMs, containers, or hosts with hundreds of DB instances. Unpredictable memory usage can lead to out-of-memory (OOM) errors, degraded performance, or even service outages.
Historically, while the block cache was the main source of memory usage, other components—such as memtables, table readers, file metadata, and temporary buffers—could consume significant memory outside the block cache’s control. This made it difficult for users to set a single memory limit and guarantee resource usage stays within expectations.

## Goal
The goal of recent memory tracking work in RocksDB is to enable users to cap the total memory usage of RocksDB instances under a single, configurable limit—the block cache capacity. This is achieved by:
- **Tracking and charging** all major memory consumers (memtables, table readers, file metadata, compression buffers, filter construction) to the block cache.
- **Evicting** data blocks or other memory when the total tracked usage exceeds the configured limit.
- **Providing a fixed memory footprint** for RocksDB, making it easier to run in resource-constrained environments and avoid OOMs.

## Memtable Memory Charging
A major source of memory usage in RocksDB is the memtable. To ensure memtable memory is tracked and capped under a single limit, RocksDB provides the WriteBufferManager (WBM). When WBM is configured with a block cache, memtable memory usage is charged to the block cache. This helps prevent OOM errors and simplifies resource management.

```cpp
std::shared_ptr<Cache> cache = HyperClockCacheOptions(capacity).MakeSharedCache();;
DBOptions db_options;
db_options.write_buffer_manager = std::make_shared<WriteBufferManager>(.., cache);
```

## Other Memory Charging
Beyond memtables, RocksDB allows users to control memory charging for other internal roles using the cache_usage_options API. This provides fine-grained control over how memory is tracked for components like table readers, file metadata, compression dictionary buffers (`CompressionOptions::max_dict_buffer_bytes:`) and filter construction.

```cpp
struct CacheEntryRoleOptions {
  enum class Decision {
    kEnabled,
    kDisabled,
    kFallback,
  };
  Decision charged = Decision::kFallback;
};
struct CacheUsageOptions {
  CacheEntryRoleOptions options;
  std::map<CacheEntryRole, CacheEntryRoleOptions> options_overrides;
};

...
BlockBasedTableOptions table_options;
table_options.cache_usage_options.options.charged = CacheEntryRoleOptions::Decision::kFallback;
table_options.cache_usage_options.options_overrides[CacheEntryRole::kTableBuilder] = {
  .charged = CacheEntryRoleOptions::Decision::kEnabled,
};
```

Default (`Decision::kFallback`) behavior for each memory type:
- `CacheEntryRole::kCompressionDictionaryBuildingBuffer`: `kEnabled`
- `CacheEntryRole::kFilterConstruction`: `kDisabled`
- `CacheEntryRole::kBlockBasedTableReader`: `kDisabled`
- `CacheEntryRole::kFileMetadata`: `kDisabled`

## Monitoring and Observability
RocksDB provides built-in statistics to help users monitor memory usage and cache behavior. The `DB::Properties::kBlockCacheEntryStats` exposes detailed statistics about block cache entries, including breakdowns by each `CacheEntryRole`. These statistics are essential for understanding memory consumption and tuning cache configuration.
