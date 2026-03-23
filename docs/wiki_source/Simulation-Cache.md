*Simulation Cache(SimCache)* can help users to predict the block cache performance metrics, e.t. hit, miss, of a specific simulation capacity (memory) under current work load, without actually using that much of memory.
## Motivation
It is able to help users tune their current block cache size, and determine how efficient they are using the memory. Also, it helps understand the cache performance with fast storage.

## Introduction
The basic idea of SimCache is to wrap the normal block cache with a key-only block cache configured with targeted simulation capacity. When insertion happens, we insert the key to both caches, but value is only inserted into normal cache. The size of the value is contributed to the capacities of both caches so that we simulate the behavior of a block cache with simulation capacity without using that much memory because in fact, the real memory usage only includes the total size of keys.

## How to use SimCache
Since SimCache is a wrapper on top of normal block cache. User has to create a block cache first with [NewLRUCache](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/cache.h):
```cpp
std::shared_ptr<rocksdb::Cache> normal_block_cache =
  NewLRUCache(1024 * 1024 * 1024 /* capacity 1GB */);
```
Then wrap the normal_block_cache with [NewSimCache](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/utilities/sim_cache.h) and set the SimCache as the block_cache field of `rocksdb::BlockBasedTableOptions` and then generate the `options.table_factory`:
```cpp
rocksdb::Options options;
rocksdb::BlockBasedTableOptions bbt_opts;
std::shared_ptr<rocksdb::Cache> sim_cache = 
NewSimCache(normal_block_cache, 
  10 * 1024 * 1024 * 1024 /* sim_capacity 10GB */);
bbt_opts.block_cache = sim_cache;
options.table_factory.reset(new BlockBasedTableFactory(bbt_opts));
```
Finally, open the DB with `options`.
Then the HIT/MISS value of SimCache can be acquired by calling
`sim_cache->get_hit_counter()` and `sim_cache->get_miss_counter()`, respectively. Alternatively, if you don't want to store sim_cache and using Rocksdb (>= v4.12), you can get these statistics through Tickers SIM_BLOCK_CACHE_HIT and SIM_BLOCK_CACHE_MISS in [rocksdb::Statistics](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/statistics.h).

## Memory Overhead
People may concern the actual memory usage of SimCache, which can be estimated as:

**sim_capacity \* entry_size / (entry_size + block_size)**,
* 76 <= entry_size (key_size + other) <= 104,
* BlockBasedTableOptions.block_size = 4096 by default but is configurable.

Therefore, by default the actual memory overhead of SimCache is around **sim_capacity \* 2%**.