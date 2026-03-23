As DB/mem ratio gets larger, the memory footprint of filter/index blocks becomes non-trivial. Although `cache_index_and_filter_blocks` allows storing only a subset of them in block cache, their relatively large size negatively affects the performance by i) occupying the block cache space that could otherwise be used for caching data, ii) increasing the load on the disk storage by loading them into the cache after a miss. Here we illustrate these problems in more detail and explain how partitioning index/filters alleviates the overhead.

## How large are the index/filter blocks?

RocksDB has by default one index/filter block per SST file. The size of the index/filter block varies based on the configuration but for an SST of size 256MB the index/filter block of size 0.5/5MB is typical, which is much larger than the typical data block size of 4-32KB. That is fine when all index/filter blocks fit perfectly into memory and hence are read once per SST lifetime, not so much when they compete with data blocks for the block cache space and are also likely to be re-read many times from the disk.

## What is the big deal with large index/filter blocks?

When index/filter blocks are stored in block cache they are effectively competing with data blocks (as well as with each other) on this scarce resource. A filter of size 5MB is occupying the space that could otherwise be used to cache 1000s of data blocks (of size 4KB). This would result in more cache misses for data blocks. The large index/filter blocks also kick each other out of the block cache more often and exacerbate their own cache miss rate too. This is while only a small part of the index/filter block might have been actually used during its lifetime in the cache.

After the cache miss of an index/filter block, it has to be reloaded from the disk, and its large size is not helping in reducing the IO cost. While a simple point lookup might need at most a couple of data block reads (of size 4KB) one from each layer of LSM, it might end up also loading multiple megabytes of index/filter blocks. If that happens often then the disk is spending more time serving index/filter blocks rather than the actual data blocks.

## What is partitioned index/filters?

With partitioning, the index/filter block of an SST file is partitioned into smaller blocks with an additional top-level index on them. When reading an index/filter, only top-level index is loaded into memory. The partitioned index/filter then uses the top-level index to load on demand into the block cache the partitions that are required to perform the index/filter query. The top-level index, which has much smaller memory footprint, can be stored in heap or block cache depending on the `cache_index_and_filter_blocks` setting.

### Pros:
- Higher cache hit rate: Instead of polluting the cache space with large index/blocks, partitioning allows loading index/filters with much finer granularity and hence making effective use of the cache space.
- Less IO util: Upon a cache miss for an index/filter partition, only one partition requires to be loaded from the disk, which results into much lighter load on disk compared to reading the entire index/filter of the SST file.
- No compromise on index/filters: Without partitioning the alternative approach to reduce memory footprint of index/filters is to sacrifice their accuracy by for example larger data blocks or fewer bloom bits to have smaller index and filters respectively.

### Cons:
- Additional space for the top-level index: its quite small 0.1-1% of index/filter size.
- More disk IO: if the top-level index is not already in cache it would result to one additional IO. To avoid that they can be either stored in heap or stored in cache with hi priority (TODO work)
- Losing spatial locality: if a workload requires frequent, yet random reads from the same SST file, it would result into loading a separate index/filter partition upon each read, which is less efficient than reading the entire index/filter at once. Although we did not observe this pattern in our benchmarks, it is only likely to happen for L0/L1 layers of LSM, for which partitioning can be disabled (TODO work)

## Success stories

### HDD, 100TB DB
In this example, we have a DB of size 86G on HDD and emulate the small memory that is present to a node with 100TB of data by using direct IO (skipping OS file cache) and a very small block cache of size 60MB. Partitioning improves throughput by 11x from 5 op/s to 55 op/s.

> ./db_bench --benchmarks="readwhilewriting[X3],stats" --use_direct_reads=1 -compaction_readahead_size 1048576 --use_existing_db --num=2000000000 --duration 600 --cache_size=62914560 -cache_index_and_filter_blocks=false -statistics -histogram -bloom_bits=10 -target_file_size_base=268435456 -block_size=32768 -threads 32 -partition_filters -partition_indexes -index_per_partition 100 -pin_l0_filter_and_index_blocks_in_cache -benchmark_write_rate_limit 204800 -max_bytes_for_level_base 134217728 -cache_high_pri_pool_ratio 0.9

### SSD, Linkbench
In this example, we have a DB of size 300G on SSD and emulate the small memory that would be available in presence of other DBs on the same node by using direct IO (skipping OS file cache) and block cache of size 6G and 2G. Without partitioning the linkbench throughput drops from 38k tps to 23k when reducing memory from 6G to 2G. With partitioning the throughput drops from 38k to only 30k.

## How to use it?
- `index_type` = `IndexType::kTwoLevelIndexSearch`
  * This is to enable partitioned indexes.
- `NewBloomFilterPolicy(BITS, false)`
  * Use full filters instead of block-based filter.
- `partition_filters` = `true`
  * This is to enable partitioned filters.
- `metadata_block_size` = 4096
  * This is the block size for index partitions.
- `cache_index_and_filter_blocks` = `false` [if you are on <= 5.14]
  * The partitions are stored in the block cache anyway. This is to control the location of top-level indexes (which easily fit into memory): pinned in heap or cached in the block cache. Having them stored in block cache is less experimented with.
- `cache_index_and_filter_blocks` = `true` and `pin_top_level_index_and_filter` = `true` [if you are on >= 5.15]
  * This would put everything in block cache but also pin the top-level indexes, which are quite small.
- `cache_index_and_filter_blocks_with_high_priority` = `true`
  * Recommended setting.
- `pin_l0_filter_and_index_blocks_in_cache` = `true`
  * Recommended setting as this property is extended to the index/filter partitions as well.
  * Use it only if the compaction style is level-based.
  * **Note**: with pinning blocks into the block cache, it could potentially go beyond the capacity if strict_capacity_limit is not set (which is the default case).
- block cache size: if you used to store the filter/index into heap, do not forget to increase the block cache size with the amount of memory that you are saving from the heap.

## Current limitations
- Partitioned filters cannot be enabled without having partitioned index enabled as well.
- We have the same number of filter and index partitions. In other words, whenever an index block is cut, the filter block is cut as well. We might change it in future if it shows to be causing deficiencies.
- The filter block size is determined by when the index block is cut. We will soon extend `metadata_block_size` to enforce the maximum size on both filter and index blocks, i.e., a filter block is cut either when an index block is cut or when its size is about to exceed `metadata_block_size` (TODO).

# Under the hood

Here we present the implementation details for the developers.

## BlockBasedTable Format

You can study the BlockBasedTable format [here](https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format). With partitioning the difference would be that the index block

    [index block]

is stored as

    [index block - partition 1]
    [index block - partition 2]
    ...
    [index block - partition N]
    [index block - top-level index]

and the footer of SST points to the top-level index block (which by itself is an index on index partition blocks). Each individual index block partition conforms the same format as kBinarySearch. The top-level index format also conforms with that of kBinarySearch and hence can be read using the normal data block readers.

Similar structure is used for partitioning the filter blocks. The format of each individual filter  block conforms with that of kFullFilter. The top-level index format conforms with that of kBinarySearch, similar to top-level index of index blocks.

Note that with partitioning the SST inspection tools such sst_dump report the size of top-level index on index/filters rather than the collective size of index/filter blocks.

## Builder

Partitioned index and filters are built by `PartitionedIndexBuilder` and `PartitionedFilterBlockBuilder` respectively. 

`PartitionedIndexBuilder` maintains `sub_index_builder_`, a pointer to `ShortenedIndexBuilder`, to build the current index partition. When determined by `flush_policy_`, the builder saves the pointer along with the last key in the index block, and creates a new active `ShortenedIndexBuilder`. When `::Finish` is called on the builder, it calls `::Finish` on the earliest sub index builder and returns the resulting partition block. Next calls to `PartitionedIndexBuilder::Finish` will also include the offset of previously returned partition on the SST, which is used as the values of the top-level index.  The last call to `PartitionedIndexBuilder::Finish` will finish the top-level index and return that instead. After storing the top-level index on SST, its offset will be used as the offset of the index block.

`PartitionedFilterBlockBuilder` inherits from `FullFilterBlockBuilder` which has a `FilterBitsBuilder` for building bloom filters. It also has a pointer to `PartitionedIndexBuilder` and invokes `ShouldCutFilterBlock` on it to determine when a filter block should be cut (right after when an index block is cut). To cut a filter block, it finishes the `FilterBitsBuilder` and stores the resulting block along with a partitioning key provided by  `PartitionedIndexBuilder::GetPartitionKey()`, and reset the `FilterBitsBuilder` for the next partition. At the end each time `PartitionedFilterBlockBuilder::Finish` is invoked one of the partitions is returned, and also the offset of the previous partition is used to build the top-level index. The last call to `::Finish` will return the top-level index block.

The reasons for making `PartitionedFilterBlockBuilder` depend on `PartitionedIndexBuilder` was to enable an optimization for interleaving index/filter partitions on the SST file. That optimization not being pursed we are likely to cut this dependency in future.

## Reader

Partitioned indexes are read via `PartitionIndexReader` which operates on the top-level index block. When `NewIterator` is invoked a `TwoLevelIterator` on the the top-level index block. This simple implementation is feasible since each index partition has kBinarySearch format which is the same format as data blocks, and thus can be easily plugged as the lower level iterator. If `pin_l0_filter_and_index_blocks_in_cache` is set, the lower level iterators are pinned to `PartitionIndexReader`, so their corresponding index partitions will be pinned in block cache as long as `PartitionIndexReader` is alive. `BlockEntryIteratorState` uses a set of the pinned partition offsets to avoid unpinning an index partition twice.

`PartitionedFilterBlockReader` uses the top-level index to find the offset of the filter partition. It then invokes `GetFilter` on `BlockBasedTable` object to load the `FilterBlockReader` object on the filter partition from the block cache (or load it to the cache if it is not already there) and then releases the `FilterBlockReader` object. To extend `table_options.pin_l0_filter_and_index_blocks_in_cache` to filter partitions, `PartitionedFilterBlockReader` does not release the cache handles for such blocks (i.e., keep them pinned in block cache). It instead maintains `filter_cache_`, a map of pinned `FilterBlockReader`, which is also used to release the cache entries when `PartitionedFilterBlockReader` is destructed.
