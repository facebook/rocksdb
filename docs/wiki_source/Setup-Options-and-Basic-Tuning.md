Besides writing code using [Basic Operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations) on RocksDB, you may also be interested in how to tune RocksDB to achieve desired performance. In this page, we introduce how to get an initial set-up, which should work well enough for many use cases.

RocksDB has many configuration options, but most of them can be safely ignored by many users, as the majority of them are for influencing the performance of very specific workloads. For general use, most RocksDB options can be left at their defaults, however, we suggest some options below that every user might like to experiment with for general workloads.

First, you need to think about the options relating to resource limits (see also: [Basic Operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations)):

## Write Buffer Size

This can be set either per Database and/or per Column Family.

### Column Family Write Buffer Size

This is the maximum write buffer size used for the Column Family.

It represents the amount of data to build up in memory (backed by an unsorted log on disk) before converting to a sorted on-disk file. The default is 64 MB.

You need to budget for 2 x your worst case memory use. If you don't have enough memory for this, you should reduce this value. Otherwise, it is not recommended to change this option. For example:

```c++
cf_options.write_buffer_size = 64 << 20;
```

See below for sharing memory across Column Families.

### Database Write Buffer Size

This is the maximum size of all Write Buffers across all Column Families in the database.
It represents the amount of data to build up in memtables across all column families before writing to disk.

By default this feature is disabled (by being set to `0`). You should not need to change it. However, for reference, if you do need to change it to 64 GB for example:

```c++
db_options.db_write_buffer_size = 64 << 30;
```

## Block Cache Size

You can create a Block cache of your chosen the size for caching uncompressed data.

We recommend that this should be about 1/3 of your total memory budget. The remaining free memory can be left for the OS (Operating System) page cache. Leaving a large chunk of memory for OS page cache has the benefit of avoiding tight memory budgeting (see also: [Memory Usage in RocksDB](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB)).

Setting the block cache size requires that we also set table related options, for example if you want an LRU Cache of `128 MB`:

```c++
auto cache = NewLRUCache(128 << 20);

BlockBasedTableOptions table_options;
table_options.block_cache = cache;

auto table_factory = new BlockBasedTableFactory(table_options);
cf_options.table_factory.reset(table_factory);
```

*NOTE*: You should set the same Cache object on all the `table_options` for all the Column Families of all DB's managed by the process. An alternative to achieve this, is to pass the same `table_factory` or `table_options` to all Column Families of all DB's. To learn more about the Block Cache, see: [Block Cache](https://github.com/facebook/rocksdb/wiki/Block-Cache).

## Compression

You can only choose compression types which are supported on your host system. Using compression is a trade-off between CPU, I/O and storage space.

1. `cf_options.compression` controls the compression type used for the first `n-1` levels.
    We recommend to use LZ4 (`kLZ4Compression`), or if not available, to use Snappy (`kSnappyCompression`).

2. `cf_options.bottommost_compression` controls the compression type used for the `nth` level.
    We recommend to use ZStandard (`kZSTD`), or if not available, to use Zlib (`kZlibCompression`).

To learn more about compression, See [[Compression]].

## Bloom Filters
You should only enable this if it suits your Query patterns; If you have many point lookup operations (i.e. `Get()`), then a Bloom Filter can help speed up those operations, conversely if most of your operations are range scans (e.g. `Iterator()`) then the Bloom Filter will not help.

The Bloom Filter uses a number of bits for each key, a good value is `10`, which yields a filter with ~1% false positive rate. Refer to [Bloom Filter Wiki page](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter) for false positive rates with lower number of bits per key.

If `Get()` is a common operation for your queries, you can configure the Bloom Filter, for example with 10 bits per key:

```c++
table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
```
We also recommend 
```c++
table_options.optimize_filters_for_memory = true;
```
for some memory saving.

To learn more about Bloom Filters, see: [Bloom Filter](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter).

## Rate Limits
It can be a good idea to limit the rate of compactions and flushes to smooth I/O operations, one reason for doing this is to avoid the read latency outliers. This can be done by means of the `db_options.rate_limiter` option. Rate limiting is a complex topic, and is covered in [[Rate Limiter]].

**NOTE**: Make sure to pass the same `rate_limiter` object to all the DB's in your process.

## SST File Manager
If you are using flash storage, we recommend users to mount the file system with the [`discard`](http://man7.org/linux/man-pages/man8/mount.8.html) flag in order to improve write amplification.

If you are using flash storage and the `discard` flag, trimming will be employed. Trimming can cause long I/O latencies temporarily if the trim size is very large. The SST File Manager can cap the file deletion speed, so that each trim's size is controlled.

The SST File Manager can be enabled, by setting the `db_options.sst_file_manager` option. Details of the SST File Manager can be seen here: [sst_file_manager_impl.h](https://github.com/facebook/rocksdb/blob/5.14.fb/util/sst_file_manager_impl.h#L28).

## Other General Options
Below are a number of options, where we feel the values set achieve reasonable out-of-box performance for general workloads. We didn't change these options because of the concern of incompatibility or regression when users upgrade their existing RocksDB instance to a newer version. We suggest that users start their new DB projects with these settings:

```c++
cf_options.level_compaction_dynamic_level_bytes = true;
opts.max_background_jobs = 6;
options.bytes_per_sync = 1048576;
options.compaction_pri = kMinOverlappingRatio;
table_options.block_size = 16 * 1024;
table_options.cache_index_and_filter_blocks = true;
table_options.pin_l0_filter_and_index_blocks_in_cache = true;
table_options.format_version = <the latest version>;
```
Default format version usually lags behind the recommended version for compatibility reason. For new use cases, it's not a concern.

Don't feel sad if you have existing services running with the defaults instead of these options. Whilst we believe that these are better than the default options, none of them is likely to bring significant improvements.

## Conclusion and Further Reading

Now you are ready to test your application and see how your initial RocksDB performance looks. Hopefully it will be good enough!

If the performance of RocksDB within your application after the basic set-up described above, is good enough for you, we don't recommend that you tune it further.
As it is common for a workload to change over time, if you expend unnecessary resources upfront to tune RocksDB to be highly performant for your current workload, some modest change in future to that workload may push the performance off a cliff.

On the other hand, if the performance is not good enough for you, you can further tune RocksDB by following the more detailed [Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide). 
