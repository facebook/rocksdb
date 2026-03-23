Here we try to explain how RocksDB uses memory. There are a couple of components in RocksDB that contribute to memory usage:

1. Block cache
2. Indexes and bloom filters
3. Memtables
4. Blocks pinned by iterators

We will describe each of them in turn.

For some ongoing projects that improve memory efficiency. See [[Improving Memory Efficiency|Projects-Being-Developed#improving-memory-efficiency]].

For operational purposes, we've been working towards charging all kinds of memory usage to a single RocksDB memory budget. This started out with the memtables but has been extended to various other kinds of allocations since then. See https://github.com/facebook/rocksdb/blob/68112b3beb885c9ec8bc410e15b05e7e27e3c9ee/include/rocksdb/table.h#L323-L388 for more.

## Block cache

Block cache is where RocksDB caches uncompressed data blocks. You can configure block cache's size by setting block_cache property of BlockBasedTableOptions:

    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = rocksdb::NewLRUCache(1 * 1024 * 1024 * 1024LL);
    rocksdb::Options options;
    options.table_factory.reset(new rocksdb::BlockBasedTableFactory(table_options));

If the data block is not found in block cache, RocksDB reads it from file using buffered IO. That means it also uses the OS's page cache for raw file blocks, usually containing compressed data. In a way, RocksDB's cache is two-tiered: block cache and page cache. Counter-intuitively, decreasing block cache size will not increase IO. The memory saved will likely be used for page cache, so even more data will be cached. However, CPU usage might grow because RocksDB needs to decompress pages it reads from page cache.

To learn how much memory block cache is using, you can call a function GetUsage() on block cache object or call getProperty() on DB object:

    table_options.block_cache->GetUsage();
    db->getProperty("rocksdb.block-cache-usage")

In MongoRocks, you can get the size of block cache by calling

    > db.serverStatus()["rocksdb"]["block-cache-usage"]

## Indexes and filter blocks

Indexes and filter blocks can be big memory users and by default they don't count in memory you allocate for block cache. This can sometimes cause confusion for users: you allocate 10GB for block cache, but RocksDB is using 15GB of memory. The difference is usually explained by index and bloom filter blocks.

We continuously make index and filter more compact. To take advantage of recent improvements, use more recent format version through `BlockBasedTableOptions.format_version`.  Some other newer features need to be explicitly enabled:
* Set `BlockBasedTableOptions.optimize_filters_for_memory` for more jemalloc friendly bloom filter sizing. 
* Also consider to use the new [Ribbon Filter](https://github.com/facebook/rocksdb/blob/003e72b2019c6727c3e89b1d0f85d8fd75f698ac/include/rocksdb/filter_policy.h#L219-L236)

See [[RocksDB Bloom Filter]] for more information on bloom filters.

Here's how you can roughly calculate and manage sizes of index and filter blocks:

1. For each data block we store three information in the index: a key, a offset and size. Therefore, there are two ways you can reduce the size of the index. If you increase block size, the number of blocks will decrease, so the index size will also reduce linearly. By default our block size is 4KB, although we usually run with 16-32KB in production. The second way to reduce the index size is the reduce key size, although that might not be an option for some use-cases.
2. Calculating the size of filter blocks is easy. If you configure bloom filters with 10 bits per key (default, which gives 1% of false positives), the bloom filter size is `number_of_keys * 10 bits`. There's one trick you can play here, though. If you're certain that Get() will mostly find a key you're looking for, you can set `options.optimize_filters_for_hits = true`. With this option turned on, we will not build bloom filters on the last level, which contains 90% of the database. Thus, the memory usage for bloom filters will be 10X less. You will pay one IO for each Get() that doesn't find data in the database, though.

There are two options that configure how much index and filter blocks we fit in memory:

1. If you set `cache_index_and_filter_blocks` to true, index and filter blocks will be stored in block cache, together with all other data blocks. This also means they can be paged out. If your access pattern is very local (i.e. you have some very cold key ranges), this setting might make sense. However, in most cases it will hurt your performance, since you need to have index and filter to access a certain file. An exception to `cache_index_and_filter_blocks=true` is for L0 when setting `pin_l0_filter_and_index_blocks_in_cache=true`, which can be a good compromise setting.
2. If `cache_index_and_filter_blocks` is false (which is default), the number of index/filter blocks is controlled by option `max_open_files`. If you are certain that your ulimit will always be bigger than number of files in the database, we recommend setting `max_open_files` to -1, which means infinity. This option will preload all filter and index blocks and will not need to maintain LRU of files. Setting `max_open_files` to -1 will get you the best possible performance.

However, regardless of options, by default each column family in each database instance will have its own block cache instance, with its own memory limit. To share a single block cache, set `block_cache` in your various `BlockBasedTableOptions` to use the same shared_ptr, or share the same `BlockBasedTableOptions` for your various factories, or share the same BlockBasedTableFactory for `table_factory` in your various `Options` or `ColumnFamilyOptions`, etc.

To learn how much memory is being used by index and filter blocks, you can use RocksDB's GetProperty() API:

    std::string out;
    db->GetProperty("rocksdb.estimate-table-readers-mem", &out);

In MongoRocks, just call this API from the mongo shell:

    > db.serverStatus()["rocksdb"]["estimate-table-readers-mem"]

In [partitioned index/filters](https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters) the indexes and filters for each partition are always stored in block cache. The top-level index can be configured to be stored in heap or block cache via `cache_index_and_filter_blocks`.

## Memtable

You can think of memtables as in-memory write buffers. Each new key-value pair is first written to the memtable. Memtable size is controlled by the option `write_buffer_size`. It's usually not a big memory consumer unless using many column families and/or database instances. However, memtable size inversely affects write amplification: more memory to the memtable yields less write amplification. If you increase your memtable size, be sure to also increase your L1 size! L1 size is controlled by the option `max_bytes_for_level_base`.

To get the current memtable size, you can use:

    std::string out;
    db->GetProperty("rocksdb.cur-size-all-mem-tables", &out);

In MongoRocks, the equivalent call is

    > db.serverStatus()["rocksdb"]["cur-size-all-mem-tables"]

Since version 5.6, you can cost the memory budget of memtables as a part of block cache. Check [[Write Buffer Manager]] for the information.

Similar to block cache, by default memtable sizes are per column family, per database instance. Use a [[Write Buffer Manager]] to cap memtable memory across column families and/or database instances.

## Blocks pinned by iterators

Blocks pinned by iterators usually don't contribute much to the overall memory usage. However, in some cases, when you have 100k read transactions happening simultaneously, it might put a strain on memory. Memory usage for pinned blocks is easy to calculate for most cases. Each iterator pins exactly one data block for each L0 file plus one data block for each L1+ level. So the total memory usage from pinned blocks is approximately `num_iterators * block_size * ((num_levels-1) + num_l0_files)`. However when `ReadOptions.pin_data` is set to true and the underlying SST file format supports it, an iterator could be pinning all iterated blocks through its lifetime. To get the statistics about this pinned memory usage, call GetPinnedUsage() on block cache object or call getProperty() on db object:

        table_options.block_cache->GetPinnedUsage();
        db->getProperties("rocksdb.block-cache-pinned-usage");