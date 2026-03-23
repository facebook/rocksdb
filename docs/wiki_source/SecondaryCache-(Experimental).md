`SecondaryCache` is the interface for caching blocks on a secondary tier, which can be a non-volatile media or alternate forms of caching such as compressed data. The purpose of the secondary cache is to support other ways of caching the object, such as persistent or compressed data. It can be viewed as an extension of RocksDB’s current volatile block cache. The RocksDB team has created two concrete implementations of `SecondaryCache`, a `Cachelib` based implementation on non-volatile media and a `LRUCache` based implementation (`CompressedSecondaryCache`) on DRAM.      

# APIs
To build a concrete `SecondaryCache`, you need to prepare the implementations of `SecondaryCacheResultHandle`, `SecondaryCache`, and some callbacks. Their details can be found in `secondary_cache.h` and `cache.h`.
* `SecondaryCacheResultHandle` refers to an item in the secondary cache. 
* The callbacks in `CacheItemHelper` must be implemented for saving items to and restoring them from the secondary cache.
* `SecondaryCache` includes the essential abstract functions of the secondary cache.

To integrate the secondary cache with the primary cache, a `SecondaryCache` can be configured by adding a pointer to it in `LRUCacheOptions`.  

# CompressedSecondaryCache
`CompressedSecondaryCache` leverages compression libraries (e.g. LZ4) to compress items and stores the compressed blocks into an `LRUCache`. User can create a secondary cache instance by calling `NewCompressedSecondaryCache`. The options can be passed in respectively or together in `CompressedSecondaryCacheOptions`. The options extend `LRUCacheOptions` by adding the following two options for compression.
* `compression_type`: The compression method (if any) that is used to compress data.
* `compress_format_version`: The format version needed by some compression methods.   
## Example 1:
```
CompressedSecondaryCacheOptions secondary_cache_opts;
secondary_cache_opts.capacity = sec_cache_capacity;
LRUCacheOptions lru_cache_opts(cache_capacity);
lru_cache_opts.secondary_cache = NewCompressedSecondaryCache(secondary_cache_opts);
std::shared_ptr<Cache> cache = NewLRUCache(lru_cache_opts);
```

## Example 2:
```
LRUCacheOptions lru_cache_opts(cache_capacity);
lru_cache_opts.secondary_cache = NewCompressedSecondaryCache(secondary_cache_capacity);
std::shared_ptr<Cache> cache = NewLRUCache(lru_cache_opts);
```

# References
[This blog](http://rocksdb.org/blog/2021/05/27/rocksdb-secondary-cache.html) includes the initial introduction to `SecondaryCache`.