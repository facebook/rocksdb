Write buffer manager helps users control the total memory used by memtables across multiple column families and/or DB instances.
Users can enable this control by 2 ways:

1. Limit the total memtable usage across multiple column families and DBs under a threshold.
2. Cost the memtable memory usage to block cache so that memory of RocksDB can be capped by the single limit.

The usage of a write buffer manager is similar to rate_limiter and sst_file_manager. Users can create one write buffer manager object and pass it to all the options of column families or DBs whose memtable size they want to be controlled by this object.

For more details refer, [write_buffer_manager option](https://github.com/facebook/rocksdb/blob/f35f7f2704d9803908de7eb8864f260312d173b4/include/rocksdb/options.h#L761) and [write_buffer_manager.h](
https://github.com/facebook/rocksdb/blob/main/include/rocksdb/write_buffer_manager.h)

## Limit total memory of memtables
A memory limit is given when creating the write buffer manager object. RocksDB will try to limit the total memory to under this limit.

In version 5.6 or higher, a flush will be triggered on one column family of the DB you are inserting to, 
1. If mutable memtable size exceeds about 90% of the limit,
2. If the total memory is over the limit, more aggressive flush may also be triggered only if the mutable memtable size also exceeds 50% of the limit. Both checks are needed because if already more than half memory is being flushed, triggering more flush may not help.

Before version 5.6, a flush will be triggered if the total mutable memtable size exceeds the limit.

In version 5.6 or higher, the total memory is counted as total memory allocated in the arena, even if some of that may not yet be used by memtable. In earlier versions, the memory is counted as memory actually used by memtables.

## Cost memory used in memtable to block cache

Since version 5.6, users can set up RocksDB to cost memory used by memtables to block cache. This can happen no matter whether you enable memtable memory limit or not. This option is added to manage memory (memtables + block cache) under a single limit.

In most cases, blocks that are actually used in block cache are just a smaller percentage than data cached in block cache, so when users enable this feature, the block cache capacity will cover the memory usage for both block cache and memtables. If users also enable `cache_index_and_filter_blocks`, then the three major types of memory of RocksDB (cache_index_and_filter_blocks, memtables and data blocks cached) will be capped by the single cap.

### Implementation:
For every, let's say 1MB memory allocated memtable, WriteBufferManager will put dummy 1MB entries (empty) to the block cache so that the block cache can track the size correctly for memtables and evict blocks to make room if needed. In case the memory used by the memtable shrinks, WriteBufferManager will not immediately remove the dummy blocks but shrink memory cost in the block cache if the total memory used by memtables is less than 3/4 of what we reserve in the block cache. We do this because we don't want to free the memory costed in the block cache immediately when a memtable is freed, as block cache insertion is expensive and might happen again in a very near future due to a memtable usage increase soon. We want to shrink the memory cost in block cache when the memory is unlikely to come back.

To enable this feature,
* pass the block cache you are using to the WriteBufferManager you are going to use.
* still pass the parameter of WriteBufferManager as the maximum memory you want RocksDB to use for memtables.
* set the capacity of your block cache to be the sum of the memory used for cached data blocks and memtables.

`
 WriteBufferManager(size_t buffer_size, std::shared_ptr<Cache> cache = {})

## Stalls
WriteBufferManager provides an option `allow_stall `that can be passed to WriteBufferManager constructor. If set true, it will enable stalling of all writers when memory usage exceeds buffer_size (soft limit). It will wait for flush to complete and memory usage to drop down. Applications can avoid it by setting ```no_slowdown = true``` in ```WriteOptions```

