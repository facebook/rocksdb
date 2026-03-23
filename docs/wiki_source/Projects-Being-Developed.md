The page lists major projects being actively developed, or has been planned for future development.

## RocksDB on Remote Storage
### API between RocksDB and underlying storage
We recently completed a major refactoring of the rocksdb::Env class by separating the storage related interfaces into a class of its own, called rocksdb::FileSystem. In the long-term, the storage interfaces in Env will be deprecated and the main purpose of Env will be to abstract core OS functionality that RocksDB needs. The relevant PRs are https://github.com/facebook/rocksdb/pull/5761 and https://github.com/facebook/rocksdb/pull/6552.

Over time, we will implement new functionality enabled by this separation -
1. Richer error handling - A compliant FileSystem implementation can return information about an IO error, such as whether its transient/retryable, permanent data-loss, file scope or entire file system etc. in ```IOStatus```, which will allow RocksDB to do more intelligent error handling.
2. Fail fast - For file systems that allow callers to provide a timeout for an IO, RocksDB can provide provide better SLAs for user reads by providing an option to specify a deadline, and failing a Get/MultiGet as soon as the deadline is exceeded. This is an ongoing project.

### Remote Compaction
Shared storage enables concurrent access to DB files by multiple DB instances. Remote Compaction project utilizes this to enables the user to run background compaction job on a different host.
The feature can be used by setting [`DBOptions.CompactionService`](https://github.com/facebook/rocksdb/blob/2f1984dd459833a92e8bd9c193f11ea82092c314/include/rocksdb/options.h#L1250-L1257), which basically intercept the compaction. On the remote side, the user can call [`DB.OpenAndCompact()`](https://github.com/facebook/rocksdb/blob/3786181a90bd2daeff22bc0f20e0c06adca95bd2/include/rocksdb/db.h#L247) to run the compaction, then return the result to the primary host.

## Tiered Storage
Tiered Storage allows users to store a RocksDB instance in SSD/HDD hybrid environment.
The tentative architecture will arrange hot data and cold data to different set of SST files. When each SST file is created, it passes a hint to FileSystem layer, where the implementation of FileSystem can do proper placement while appears to be under the same directory to the upper layer. 
RocksDB would need to develop proper mechanism to identify and predict cold and hot data, as well as compaction algorithm to work with them. RocksDB can do the coldness prediction based on KV insertion time, historic access, policies defined by users and perhaps hints from users.
We also need to make sure the compaction-based tiering can work with secondary cache which can effectively cache some hot blocks from data in cold tiers.
Some open problems:
1. How to identify/predict hot/cold data?
2. How to alter leveled and universal compaction with tiered storage?
3. How to keep index and filter blocks of files in cold tier in SSD?
4. How we can help users throttle requests that have to be served by the cold tier 
5. How tiered storage works with some features, e.g. TTL compaction, delete triggered compactions, etc?
We are still in early stage of the tiered storage journey, but hope we can release production ready features in stages. https://github.com/facebook/rocksdb/blob/6.24.fb/include/rocksdb/advanced_options.h#L778-L783 is an initial step.



## User Defined Timestamps
See [[User Timestamp (Experimental) | User Timestamp (Experimental)]].
There is also a [Google Doc](https://docs.google.com/document/d/1FcDjOM8-pJzCajCa9waQkox6DKJIWZAHm36buK4wfqs/edit#heading=h.uxub5284i1ti) describing basic API and design.

## Wide Columns
[Wide-column stores](https://en.wikipedia.org/wiki/Wide-column_store) provide a data model that is in between key-value stores and relational databases. In pure KV stores, keys and values are opaque and unstructured from the storage engine’s point of view; on the other hand, RDBMS’s enforce a strict tabular structure with a fixed set of columns. Wide-column stores occupy the middle ground by supporting columns while allowing the set of columns to differ from row to row, which makes them a good match for storing semi-structured data. As part of this project, we plan to add wide-column storage as a core RocksDB feature. See [this page](https://github.com/facebook/rocksdb/wiki/Wide-Columns) for more details.

## BlobDB
Some systems, [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) and [ForestDB](https://ieeexplore.ieee.org/abstract/document/7110563) store large objects (blobs) separately for various reasons. [BlobDB](https://github.com/facebook/rocksdb/wiki/BlobDB) is RocksDB's implementation of key-value separation, providing similar functionality. Large values (blobs) are stored in separate blob files, and only references to them are stored in RocksDB's LSM tree. By separating value storage from the LSM tree, BlobDB provides an alternative way of reducing write amplification, instead of tuning compactions. BlobDB is used in production at Facebook.

## File Checksums

See https://github.com/facebook/rocksdb/wiki/Full-File-Checksum#the-next-step

## Per Key/Value Checksum

## Encryption at Rest
Encryption at Rest is critical to some applications. Right now RocksDB has some supports in this area but they require some major improvements:
1. needs to support transparent key rotation.
2. needs to have a more friendly API for users to connect with Key Management Systems.
3. maybe some finer granularity of encryption keys are needed, e.g. per column family or even per key ranges.
Contributions from the community are highly welcome.

## MultiGet()
See [MultiGet Performance](https://github.com/facebook/rocksdb/wiki/MultiGet-Performance) for background. We have the following related projects in various stages of planning and implementation -
* Support partitioned filter and index - The first phase of MultiGet provided significant performance improvement for full filter block and index, through various techniques such as reusing blocks, reusing index iterators, prefetching CPU cachelines etc. We plan to extend these to partitioned filters and indexes. (planned work done)
* Parallelize file reads in a single level -  Currently MultiGet can parallelize reads to the same SST file. We plan to enhance this by parallelizing reads across all files in a single LSM level, thus benefiting more workloads.
* Deadline/timeouts - Users will be able to specify a deadline for a MultiGet request, and RocksDB will abort the request if the deadline is exceeded. (done)
* Limit cumulative value size - Users will be able to specify an upper limit on the total size of values read by MultiGet, in order to control memory overhead. (done)

## Parallel I/O inside a query
Sometimes multiple independent I/Os are needed to serve a query, especially in MultiGet() and iterators. Since we more towards remote storage, hiding latency becomes more important. MultiGet() section includes an item to expand parallel I/O in MultiGet(). Also, inside iterators, I/Os can be parallelize when they are against different SST files and are independent.

## Bloom Filter Improvements

First phase complete, including
* Fixed flaws in old Bloom implementation for Block-based table with a new implementation (incl [Issue 4120](https://github.com/facebook/rocksdb/issues/4120) and [5857](https://github.com/facebook/rocksdb/issues/5857)), enabled using `format_version=5`. More detail [on wiki](https://github.com/facebook/rocksdb/issues/5857) and [in the main PR](https://github.com/facebook/rocksdb/pull/6007).
* [Allow non-integer bits / key settings](https://github.com/facebook/rocksdb/pull/6092) for finer granularity in control
* [Expose details like LSM level to custom FilterPolicy](https://github.com/facebook/rocksdb/pull/6088), for experimentation

Planned:
* Minimize memory internal fragmentation on generated filters (https://github.com/facebook/rocksdb/pull/6427)
* Investigate use of different bits/key for different levels (as in [Monkey](https://stratos.seas.harvard.edu/files/stratos/files/monkeykeyvaluestore.pdf))
* Investigate use of alternative data structures, most likely based on perfect hashing static functions. See [Xor filter](https://arxiv.org/pdf/1912.08258.pdf), modified with ["fuse graph" construction](https://arxiv.org/pdf/1907.04749.pdf). Or even [sgauss](https://drops.dagstuhl.de/opus/volltexte/2019/11160/pdf/LIPIcs-ESA-2019-39.pdf). We don't expect much difference in query times, but the primary trade-off to be between construction time and memory footprint for a given false positive rate. It's likely that L0 will continue to construct Bloom filters (fast memtable flushes) while compaction will spend more time to generate more compact structures. 
* Re-vamp how filters are configured (based on above developments), probably moving away from bits/key as a proxy for accuracy.

## Improving Testing

### Fuzz Testing
https://github.com/facebook/rocksdb/tree/main/fuzz

## Adaptive Compaction

## Improving RocksDB Backups

## Persistent Cache
The persistent cache is a block level cache on disk (could be local flash or any other non-volatile medium such as NVM/SCM). It can be viewed as an extension of RocksDB’s current block cache. By maintaining a persistent cache that’s an order of magnitude larger than DRAM, fewer reads would be required from the DB storage, which is typically slower (WSF/WS HDD).
Secondary cache is built for this. In order to use it, users are expected to plug in a persistent cache implementation to secondary cache and use it.

## Improving Memory Efficiency
DRAM is identified as an opportunity to achieve higher memory efficiency. It is getting increasingly attractive for a host to operate on denser SSD drives, which forces a lower DRAM/SSD size ratio. For example, users may find it more cost effective to run on "Storage Optimized" EC2 host, whose DRAM/SSD ratio is usually 1:31 (Dec 2020). While RocksDB can functionally operate on such a ratio, RocksDB should push the performance limit to those set-ups.

There are some ongoing or planned projects there:
* [[Projects-Being-Developed#bloom-filter-improvements]] discussed above
* Track and strictly cap all memory usage by RocksDB with one single limit
* Seeking more compact index format
* Make RocksDB more friendly to jemalloc to reduce fragmentation
* Improve partitioned index to reduce the performance issues
* Compress data in block cache. To make better use of DRAM for block cache, compressing it is a straight-forward idea. Compressed cache or relying on OS page cache can achieve some of them, but they don't work well when DRAM/SSD ratio is low. We need to look for a better solution to compress some data there.

Including https://github.com/facebook/rocksdb/issues/6521

## Make Universal Compaction Incremental
Right now, when using universal compaction, users would experience large compactions, e.g. full compactions, in multiple situations. Large compactions are harder to manage: disk space needs to temporarily double, when DB restarts to shutdown in the middle the compaction progress will be lost, and compaction thread pool is harder to configure -- just to name some challenges. It is desirable always break down large compactions to smaller pieces.

Large compactions can be cut by SST file boundaries. However, we face some challenges:
1. since in universal compaction, we often compaction some much smaller sorted runs with some much larger ones. SST files in those smaller sorted runs are the smallest unit to cut but might still generate large compactions.
2. with lower size ratio between sorted runs, we may end up with compacting a lot of more data for overlapping ranges, generating extra write amplification.
Always cutting higher level files to small files might be able to solve the problem, but many small files are harder for users to manage and might generate performance penalty for read. Rather, we would like to take the path of taking slightly write amplification penalty but minimize the impacts on file size. We need to come up with a compaction algorithm, as well as file cutting algorithm, that keeps the balance between the write amplification and SST file management.

## DeleteRange() Performance Improvements
DeleteRange() is a powerful feature, but some performance limitation prevents it from being effective in many use cases. Two important performance improvements include:
1. Iterator needs to skip the deleted range with an internal Seek(), rather than iterating over keys before throwing it away.
2. Every Get() needs to construct a data structure by going through all outstanding range tombstones in the memtable. It's not scalable when DeleteRange() is frequently called.

Contributions from the community are welcome.

## Async API
The discussions on how RocksDB can provide Async API have been around for a long time. It is challenging for several reasons: (1) we need to preserve synchronous API and avoid duplicating code too much; (2) we need to think about best API that can fit users' need; (3) A good tool for async, coroutine support in C++20, is only available in relatively new compliers. But we should still explore this area and see how we can make progress.