Perf Context and IO Stats Context can help us understand the performance bottleneck of individual DB operations. Options.statistics stores cumulative statistics for all operations from all threads since the opening of the DB. Perf Context and IO Stat Context look inside individual operations.

This is the header file for perf context: https://github.com/facebook/rocksdb/blob/main/include/rocksdb/perf_context.h 

This is the header file for IO Stats Context: https://github.com/facebook/rocksdb/blob/main/include/rocksdb/iostats_context.h 

The level of profiling for the two is controlled by the same function in this header file: https://github.com/facebook/rocksdb/blob/main/include/rocksdb/perf_level.h

Perf Context and IO Stats Context use the same mechanism. The only difference is that Perf Context measures functions of RocksDB, whereas IO Stats Context measures I/O related calls. These features need to be enabled in the thread where the query to profile is executed. If the profile level is higher than disable, RocksDB will update the counters in a thread-local data structure. After the query, we can read the counters from the structure.

## How to Use Them
Here is a typical example of using using Perf Context and IO Stats Context:

``` 
#include “rocksdb/iostats_context.h”
#include “rocksdb/perf_context.h”

rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);

rocksdb::get_perf_context()->Reset();
rocksdb::get_iostats_context()->Reset();

... // run your query

rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);

... // evaluate or report variables of rocksdb::get_perf_context() and/or rocksdb::get_iostats_context()
```
Note that the same perf level is applied to both of Perf Context and IO Stats Context.

You can also call rocksdb::get_perf_context()->ToString() and rocksdb::get_iostats_context->ToString() for a human-readable report.

## Profile Levels And Costs
As always, there are trade-offs between statistics quantity and overhead, so we have designed several profile levels to choose from:

* kEnableCount will only enable counters. 

* kEnableTimeAndCPUTimeExceptForMutex is introduced since CPU Time counters are introduced. With this level, non-mutex-timing related counters will be enabled, as well as CPU counters.

* kEnableTimeExceptForMutex enables counter stats and most stats of time duration, except when the timing function needs to be called inside a shared mutex.

* kEnableTime further adds stats for mutex acquisition and waiting time.

kEnableCount avoids the more expensive calls to get system times, which results in lower overhead. We often measure all of the operations using this level, and report them when specific counters are abnormal. 

With kEnableTimeExceptForMutex, RocksDB may call the timing function dozens of times for an operation. Our common practice is to turn it on with sampled operations, or when the user requests it. Users need to be careful when choosing sample rates, since the cost of the timing functions varies across different platforms. 

kEnableTime further allows timing within shared mutex, but profiling an operation may slow down other operations. When we suspect that mutex contention is the performance bottleneck, we use this level to verify the problem.

How do we deal with the counters disabled in a level? If a counter is disabled, it won’t be updated.

## Stats
We are giving some typical examples of how to use those stats to solve your problems. We are not introducing all the stats here. A full description of all stats can be found in the header files.

### Perf Context
#### Binary Searchable Costs
`user_key_comparison_count` helps us figure out whether too many comparisons in binary search can be a problem, especially when a more expensive comparator is used. Moreover, since number of comparisons is usually uniform based on the memtable size, the SST file size for Level 0 and size of other levels, an significant increase of the counter can indicate unexpected LSM-tree shape. You may want to check whether flush/compaction can keep up with the write speed.

#### Block Cache and OS Page Cache Efficiency
`block_cache_hit_count` tells us how many times we read data blocks from block cache, and `block_read_count` tells us how many times we have to read blocks from the file system (either block cache is disabled or it is a cache miss). We can evaluate the block cache efficiency by looking at the two counters over time.

`block_read_byte` tells us how many total bytes we read from the file system. It can tell us whether a slow query can be caused by reading large blocks from the file system. Index and bloom filter blocks are usually large blocks. A large block can also be the result of a very large key or value.

In many setting of RocksDB, we rely on OS page cache to reduce I/O from the device. In fact, in the most common hardware setting, we suggest users tune the OS page cache size to be large enough to hold all the data except the last level so that we can limit a ready query to issue no more than one I/O. With this setting, we will issue multiple file system calls but at most one of them end up with reading from the device. To verify whether, it is the case, we can use counter `block_read_time` to see whether total time spent on reading the blocks from file system is expected.

#### Blob Cache

- `blob_cache_hit_count`: how many times we read blobs from blob cache
- `blob_read_count`: how many times we have to read blobs from the file system (either blob cache is disabled or it is a cache miss). We can evaluate the blob cache efficiency by looking at the two counters (`blob_cache_hit_count` and `blob_read_count`) over time.
- `blob_read_byte`: how many total bytes of blob records we read from the file system. It is important when measuring the performance of blobs of different sizes.
- `blob_read_time`: how long we take to read a blob. Note: not applicable to `MultiGetBlob()`.
- `blob_checksum_time`: how long we take to verify a blob.
- `blob_decompress_time`: how long we take to decompress a blob.

#### Tombstones
When deleting a key, RocksDB simply puts a marker, called tombstone to memtable. The original value of the key will not be removed until we compact the files containing the keys with the tombstone. The tombstone may even live longer even after the original value is removed. So if we have lots of consecutive keys deleted, a user may experience slowness when iterating across these tombstones. Counters `internal_delete_skipped_count` tells us how many tombstones we skipped. `internal_key_skipped_count` covers some other keys we skip.

#### Get Break-Down
We can use "get_*" stats to break down time inside one Get() query. The most important two are `get_from_memtable_time` and `get_from_output_files_time`. The counters tell us whether the slowness is caused by memtable, SST tables, or both. `seek_on_memtable_time` can tell us how much of the time is spent on seeking memtables.

#### Write Break-Down
"write_*" stats break down write operations. `write_wal_time`, `write_memtable_time` and `write_delay_time` tell us the time is spent on writing WAL, memtable, or active slow-down. `write_pre_and_post_process_time` mostly means time spent on waiting in the writer queue. If the write is assigned to a commit group but it is not the group leader, `write_pre_and_post_process_time` will also include the time waiting for the group commit leader to finish.

#### Iterator Operations Break-Down
"seek_*" and "find_next_user_entry_time" break down iterator operations. The most interesting one is `seek_child_seek_count`. It tells us how many sub-iterators we have, which mostly means number of sorted runs in the LSM tree.

#### Per-level PerfContext
In order to provide deeper insights into the performance implication of LSM tree structures, per-level PerfContext (`PerfContextByLevel`) was introduced to break down counters by levels. `PerfContext::level_to_perf_context` was added to maintain the mapping from level number to `PerfContextByLevel` object. User can access it directly by calling `*(rocksdb::get_perf_context()->level_to_perf_context))[level_number]`.

By default per-level PerfContext is disabled, `EnablePerLevelPerfContext()`, `DisablePerLevelPerfContext()`, and, `ClearPerLevelPerfContext` can be called to enable/disable/clear it. When per-level PerfContext is enabled, `rocksdb::get_perf_context->ToString()` will also include non-zero per-level perf context counters, in the form of 
```
bloom_filter_useful = 1@level5, 2@level7
```
which means `bloom_filter_useful` was incremented once at level 5 and twice at level 7.

### IO Stats Context
We have counters for time spent on major file system calls. Write related counters are more interesting to look if you see stalls in write paths. It tells us we are slow because of which file system operations, or it's not caused by file system calls.