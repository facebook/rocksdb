---
title: "Blob Caching"
layout: post
author:
- gangliao
- ltamasi
category: blog
---

## Overview

RocksDB is an open-source key-value storage engine widely used both at Meta and externally. Our mission is to provide a fast and 
easy to use persistent storage engine for any workload on any hardware platform. RocksDB is the storage engine of choice when it
comes to online services: it is used in relational databases (MySQL/[MyRocks](https://vldb.org/pvldb/vol13/p3217-matsunobu.pdf))
, distributed key-value stores ([ZippyDB](https://engineering.fb.com/2021/08/06/core-data/zippydb/)), indexing, as well as a 
wide range of application services. RocksDB is written in C++ and features highly flexible configuration settings that may be 
tuned to run on a variety of production environments, including pure memory, flash, hard disks, and distributed file systems.

RocksDB implements a data structure called a Log-Structured Merge Tree (LSM tree). In an LSM tree, writes are buffered in memory 
(in memtables) and then persisted in on-disk files called sorted string tables (SST) by flush jobs.  To deal with updated or 
deleted key-values, background compaction jobs continuously merge and rewrite the data in SST files, eliminating any obsolete 
key-values in the process. This repeated rewriting of data leads to write amplification, which is detrimental to flash lifetime 
and consumes a significant amount of bandwidth, especially when the key-values in question are large. Also, in the case of 
write-heavy workloads, compactions might not be able to keep up with the incoming load, creating backpressure that limits write 
throughput and potentially results in write stalls.

BlobDB is essentially RocksDB for large-value use cases. The basic idea, which was proposed in the [WiscKey paper](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) paper, is key-value separation: by storing large values in 
dedicated blob files and storing only small pointers to them in the LSM tree, we avoid copying the values over and over again 
during compaction. This reduces write amplification, which has several potential benefits like improved SSD lifetime, and better 
write and read performance. On the other hand, this comes with the cost of some space amplification due to the presence of blobs 
that are no longer referenced by the LSM tree, which have to be garbage collected. For more details, you can read the previous 
post: [Integrated BlobDB](http://rocksdb.org/blog/2021/05/26/integrated-blob-db.html).

RocksDB originally targeted services using local flash storage; however, in recent years we have been shifting towards 
disaggregated storage, where RocksDB is used over a distributed file system. This trend enables us to scale processing and 
storage separately, leading to efficiency gains; however, it also brings unique challenges since latencies are much higher when 
accessing storage over the network. Because of this, prefetching and caching data is crucial for performance, especially since 
we cannot rely on the operating system to perform these functions. RocksDB has an in-memory [block cache](https://github.com/facebook/rocksdb/wiki/Block-Cache) as well as a local on-disk secondary cache to store frequently used blocks of the SST files; 
however, there is currently no dedicated caching mechanism for blobs. 

In this post, we'd like to introduce our most recent MVP, which gives applications the ability to configure the blob cache to 
hold frequently used blobs. Our goals are to integrate the blob cache with RocksDB’s read APIs, eliminate the above limitations 
and measure the resulting performance gains under various workloads using the benchmarking tools.

## User API

The new blob cache for BlobDB can be configured (on a per-column family basis if needed) simply by using the following options:

* `blob_file_starting_level`: set a certain LSM tree level for blob files starting from. For certain use cases that have a mix 
of short-lived and long-lived values, it might make sense to support extracting large values only during compactions whose 
output level is greater than or equal to a specified LSM tree level (e.g. compactions into L1/L2/... or above). This could 
reduce the space amplification caused by large values that are turned into garbage shortly after being written at the price of 
some write amplification incurred by long-lived values whose extraction to blob files is delayed.
* `blob_cache`: set the specified cache to enable caching for blobs. Either sharing the backend cache with the block cache or 
using a completely separate cache is supported. 
* `prepopulate_blob_cache`: prepopulate warm/hot blobs which are already in memory into blob cache at the time of flush. On a 
flush, the blob that is in memory (in memtables) get flushed to the device. If using Direct IO, additional IO is incurred to 
read this blob back into memory again, which is avoided by enabling this option. This further helps if the workload exhibits 
high temporal locality, where most of the reads go to recently written data. This also helps in case of the remote file system 
since it involves network traffic and higher latencies.
* `blob_garbage_collection_space_amp_limit`: enable customers to directly set a space amplification target (as opposed to a 
per-blob-file-batch garbage threshold). Compared to `blob_garbage_collection_force_threshold`, this option is more user friendly 
and can be used to control the space amplification of blob files. 

For example, the following configuration enables blob caching for the `default` column family, which also uses the secondary 
blob cache to improve read latencies and reduce the amount of network bandwidth, and prepopulates the blob cache with the most 
frequently used blobs:

```c++
Options options;
options.enable_blob_files = true;

// Enable Prepopulate the blob cache
options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;

// Enable the secondary blob cache
CompressedSecondaryCacheOptions secondary_cache_opts;
secondary_cache_opts.compression_type = kSnappyCompression;

LRUCacheOptions lru_cache_ops;
lru_cache_ops.secondary_cache = NewCompressedSecondaryCache(secondary_cache_opts);

// Enable the blob cache
options.blob_cache = NewLRUCache(lru_cache_ops);
```

Inside RocksDB, we designed a new abstraction interface called [BlobSource](https://github.com/facebook/rocksdb/blob/7.5.fb/db/blob/blob_source.h#L26-L136) for blob read logic that gives all users access to blobs, whether they are in the blob cache, 
secondary cache, or (remote) storage. Blobs can be potentially read both while handling user reads (`Get`, `MultiGet`, or 
iterator) and during compaction (while dealing with compaction filters, Merges, or garbage collection) but eventually all blob 
reads go through `Version::GetBlob` or, for MultiGet, `Version::MultiGetBlob` (and then get dispatched to the interface -- 
`BlobSource`).

In addition to the above, we added blob cache tickers, performance context statistics, and DB properties to expose the capacity 
and current usage of the blob cache and monitor its performance.

* Added new DB properties "rocksdb.blob-cache-capacity", "rocksdb.blob-cache-usage", "rocksdb.blob-cache-pinned-usage" to show 
blob cache usage.
* Added new perf context statistics `blob_cache_hit_count`, `blob_read_count`, `blob_read_byte`, `blob_read_time`, 
`blob_checksum_time` and `blob_decompress_time`.
* Added new tickers `BLOB_DB_CACHE_MISS`, `BLOB_DB_CACHE_HIT`, `BLOB_DB_CACHE_ADD`, `BLOB_DB_CACHE_ADD_FAILURES`, 
`BLOB_DB_CACHE_BYTES_READ` and `BLOB_DB_CACHE_BYTES_WRITE`.

For example, the following code snippet shows how to use the new BlobDB tickers to minitor the blob cache behavior:

```c++
options_.statistics = CreateDBStatistics();
Statistics* statistics = options_.statistics.get();
// blob cache operations ...
statistics->getTickerCount(BLOB_DB_CACHE_MISS);
statistics->getTickerCount(BLOB_DB_CACHE_HIT);
statistics->getTickerCount(BLOB_DB_CACHE_ADD);
statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ);
statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE);
```

More examples about how to use the new blob cache API can be found in the [blob_db_test.cc](https://github.com/facebook/rocksdb/blob/7.5.fb/db/blob/blob_source_test.cc).

## Global Memory Limit

