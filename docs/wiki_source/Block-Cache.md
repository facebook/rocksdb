Block cache is where RocksDB caches data in memory for reads. User can pass in a `Cache` object to a RocksDB instance with a desired capacity (size). A `Cache` object can be shared by multiple RocksDB instances in the same process, allowing users to control the overall cache capacity. The block cache stores uncompressed blocks. Optionally user can set a second block cache storing compressed blocks. Reads will fetch data blocks first from uncompressed block cache, then compressed block cache. The compressed block cache can be a replacement of OS page cache, if [[Direct-IO]] is used.

There are two cache implementations in RocksDB, namely `LRUCache` and `ClockCache`. Both types of the cache are sharded to mitigate lock contention. Capacity is divided evenly to each shard and shards don't share capacity. By default each cache will be sharded into at most 64 shards, which provides a reasonable balance of scalability with parallel reads and locality of block cache metadata with lighter read loads. The minimum capacity for cache shards is 512KB, to limit the risk of random assignments thrashing one shard.

### Usage

Out of box, RocksDB will use LRU-based block cache implementation with 32MB capacity (8MB before version 8.2). This is only recommended for applications that are not heavily dependent on RocksDB read performance but prefer RocksDB to keep a modest memory footprint.
To set a customized block cache, call `NewLRUCache()` or `HyperClockCacheOptions::MakeSharedCache()` to create a cache object, and set it to block based table options.

    std::shared_ptr<Cache> cache = NewLRUCache(capacity);
    BlockBasedTableOptions table_options;
    table_options.block_cache = cache;
    Options options;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

RocksDB will create the default block cache if `block_cache` is set to `nullptr`. To disable block cache completely:

    table_options.no_block_cache = true;

Advanced users can customize Cache behavior with a CacheWrapper, but block cache implementations have become so specialized and evolving that it is not practical for users to build and maintain their own implementation.

### LRU Cache

Out of box, RocksDB will use LRU-based block cache implementation with 32MB capacity. Each shard of the cache maintains its own LRU list and its own hash table for lookup. Synchronization is done via a per-shard mutex. Both lookup and insert to the cache would require a locking mutex of the shard, because the LRU list makes every cache read a likely write to the cache metadata. User can create an LRU cache by calling `NewLRUCache()`. The function provides several useful options to set to the cache:

* `capacity`: Total size of the cache.
* `num_shard_bits`: The number of bits from cache keys to use as shard id. The cache will be sharded into `2^num_shard_bits` shards.
* `strict_capacity_limit`: In rare case, block cache size can go larger than its capacity. This is when ongoing reads or iterations over DB pin blocks in block cache, and the total size of pinned blocks exceeds the capacity. If there are further reads which try to insert blocks into block cache, if `strict_capacity_limit=false`(default), the cache will fail to respect its capacity limit and allow the insertion. This can create undesired OOM error that crashes the DB if the host don't have enough memory. Setting the option to `true` will reject further insertion to the cache and fail the read or iteration, though this can have undesired effects. The option works on per-shard basis, means it is possible one shard is rejecting insert when it is full, while another shard still have extra unpinned space.
* `high_pri_pool_ratio`: The ratio of capacity reserved for high priority blocks. See [[Caching Index, Filter, and Compression Dictionary Blocks|Block-Cache#caching-index-filter-and-compression-dictionary-blocks]] section below for more information.

### HyperClockCache

Due to limitations, this feature is currently for advanced usage only. We hope to make it more generally applicable soon. [Some performance data is here.](http://smalldatum.blogspot.com/2022/10/hyping-hyper-clock-cache-in-rocksdb.html)

### Caching Index, Filter, and Compression Dictionary Blocks

By default index, filter, and compression dictionary blocks (with the exception of the partitions of [partitioned indexes/filters](https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters)) are cached outside of block cache, and users won't be able to control how much memory should be used to cache these blocks, other than setting `max_open_files`. Users can opt to cache index and filter blocks in block cache, which allows for better control of memory used by RocksDB. To cache index, filter, and compression dictionary blocks in block cache:

    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;

Note that the partitions of partitioned indexes/filters are as a rule stored in the block cache, regardless of the value of the above option.

By putting index, filter, and compression dictionary blocks in block cache, these blocks have to compete against data blocks for staying in cache. Although index and filter blocks are being accessed more frequently than data blocks, there are scenarios where these blocks can be thrashing. This is undesired because index and filter blocks tend to be much larger than data blocks, and they are usually of higher value to stay in cache (the latter is also true for compression dictionary blocks). There are two options to tune to mitigate the problem:

* `cache_index_and_filter_blocks_with_high_priority`: Set priority to high for index, filter, and compression dictionary blocks in block cache. For partitioned indexes/filters, this affects the priority of the partitions as well. It only affect `LRUCache` so far, and need to use together with `high_pri_pool_ratio` when calling `NewLRUCache()`. If the feature is enabled, LRU-list in LRU cache will be split into two parts, one for high-pri blocks and one for low-pri blocks. Data blocks will be inserted to the head of low-pri pool. Index, filter, and compression dictionary blocks will be inserted to the head of high-pri pool. If the total usage in the high-pri pool exceed `capacity * high_pri_pool_ratio`, the block at the tail of high-pri pool will overflow to the head of low-pri pool, after which it will compete against data blocks to stay in cache. Eviction will start from the tail of low-pri pool.

* `pin_l0_filter_and_index_blocks_in_cache`: Pin level-0 file's index and filter blocks in block cache, to avoid them from being evicted. Starting with RocksDB version 6.4, this option also affects compression dictionary blocks. Level-0 index and filters are typically accessed more frequently. Also they tend to be smaller in size so hopefully pinning them in cache won't consume too much capacity.

* `pin_top_level_index_and_filter`: only applicable to partitioned indexes/filters. If `true`, the top level of the partitioned index/filter structure will be pinned in the cache, regardless of the LSM tree level (that is, unlike the previous option, this affects files on all LSM tree levels, not just L0).

### Simulated Cache

`SimCache` is an utility to predict cache hit rate if cache capacity or number of shards is changed. It wraps around the real `Cache` object that the DB is using, and runs a shadow LRU cache simulating the given capacity and number of shards, and measure cache hits and misses of the shadow cache. The utility is useful when user wants to open a DB with, say, 4GB cache size, but would like to know what the cache hit rate will become if cache size enlarge to, say, 64GB. To create a simulated cache:

    // This cache is the actual cache use by the DB.
    std::shared_ptr<Cache> cache = NewLRUCache(capacity);
    // This is the simulated cache.
    std::shared_ptr<Cache> sim_cache = NewSimCache(cache, sim_capacity, sim_num_shard_bits);
    BlockBasedTableOptions table_options;
    table_options.block_cache = sim_cache;
    
The extra memory overhead of the simulated cache is less than 2% of `sim_capacity`.

### Statistics

A list of block cache counters can be accessed through `Options.statistics` if it is non-null.   
    
    // total block cache misses
    // REQUIRES: BLOCK_CACHE_MISS == BLOCK_CACHE_INDEX_MISS +
    //                               BLOCK_CACHE_FILTER_MISS +
    //                               BLOCK_CACHE_DATA_MISS;
    BLOCK_CACHE_MISS = 0,
    // total block cache hit
    // REQUIRES: BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT +
    //                              BLOCK_CACHE_FILTER_HIT +
    //                              BLOCK_CACHE_DATA_HIT;
    BLOCK_CACHE_HIT,
    // # of blocks added to block cache.
    BLOCK_CACHE_ADD,
    // # of failures when adding blocks to block cache.
    BLOCK_CACHE_ADD_FAILURES,
    // # of times cache miss when accessing index block from block cache.
    BLOCK_CACHE_INDEX_MISS,
    // # of times cache hit when accessing index block from block cache.
    BLOCK_CACHE_INDEX_HIT,
    // # of times cache miss when accessing filter block from block cache.
    BLOCK_CACHE_FILTER_MISS,
    // # of times cache hit when accessing filter block from block cache.
    BLOCK_CACHE_FILTER_HIT,
    // # of times cache miss when accessing data block from block cache.
    BLOCK_CACHE_DATA_MISS,
    // # of times cache hit when accessing data block from block cache.
    BLOCK_CACHE_DATA_HIT,
    // # of bytes read from cache.
    BLOCK_CACHE_BYTES_READ,
    // # of bytes written into cache.
    BLOCK_CACHE_BYTES_WRITE,

See also: [[Memory-usage-in-RocksDB#block-cache]]