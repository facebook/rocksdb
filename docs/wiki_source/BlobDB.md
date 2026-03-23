## Overview

BlobDB is essentially RocksDB for large-value use cases. The basic idea, which was proposed in the [WiscKey paper](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf), is key-value separation: by storing large values in dedicated blob files and storing only small pointers to them in the LSM tree, we avoid copying the values over and over again during compaction. This reduces write amplification, which has several potential benefits like improved SSD lifetime, and better write and read performance. On the other hand, this comes with the cost of some space amplification due to the presence of blobs that are no longer referenced by the LSM tree, which have to be garbage collected.

## Design

> ⚠️ **WARNING:** There are two BlobDB implementations in the codebase: the legacy `StackableDB` based one (see `rocksdb::blob_db::BlobDB`) and the new integrated one (which uses the well-known `rocksdb::DB` interface).
> The legacy implementation is primarily geared towards FIFO/TTL use cases that can tolerate some data loss. It is incompatible with many widely used RocksDB features, for example, `Merge`, column families, checkpoints, backup/restore, transactions etc., and its performance is significantly worse than that of the integrated implementation. 
> Note that the API for this version is not in the public header directory, it is not actively developed, and we expect it to be eventually deprecated. This page focuses on the new integrated BlobDB.

RocksDB's LSM tree works by buffering writes in memtables, which are then persisted in SST files during flush. SST files form a tree, and are continuously merged and rewritten in the background by compactions, which eliminate any obsolete key-values in the process. This repeated rewriting of data leads to write amplification, which is detrimental to flash lifetime and consumes a significant amount of bandwidth, especially when the key-values in question are large. Also, in the case of write-heavy workloads, compactions might not be able to keep up with the incoming load, creating backpressure that limits write throughput and potentially results in write stalls.

![LSM](https://user-images.githubusercontent.com/47607618/161127024-7180217c-6bd3-4c42-9cdc-da64d8267100.png)

To address the above issues, BlobDB uses a technique called key-value separation: instead of storing large values (blobs) in the SST files, it writes them to a dedicated set of blob files, and stores only small pointers to them in the SST files. (Values smaller than a configurable threshold are stored in the LSM tree as usual.) With the blobs stored outside the LSM tree, compactions have to rewrite much less data, which can dramatically reduce overall write amplification and thus improve flash endurance. BlobDB can also provide much higher throughput by reducing or eliminating the backpressure mentioned above, and for many workloads, it can even improve read performance (see our benchmark results [here](http://rocksdb.org/blog/2021/05/26/integrated-blob-db.html)).

With key-value separation, updating or deleting a key-value results in an unreferenced blob in the corresponding blob file. Space occupied by such garbage blobs is reclaimed using garbage collection. BlobDB’s garbage collector is integrated with the LSM tree compaction process, and can be fine-tuned to strike the desired balance between space amplification and write amplification.

![LSM_with_KV_separation](https://user-images.githubusercontent.com/47607618/161127048-5cdc7001-caba-4d6b-a21d-dfef10394e51.png)

Offloading blob file building to RocksDB’s background jobs, i.e. flushes and compactions, has several advantages. It enables BlobDB to provide the same consistency guarantees as RocksDB itself. There are also several performance benefits:

* Similarly to SSTs, any given blob file is written by a single background thread, which eliminates the need for synchronization.
* Blob files can be written using large I/Os; there is no need to flush them after each write like in the case of the old BlobDB for example. This approach is also a better fit for network-based file systems where small writes might be expensive.
* Compressing blobs in the background can improve latencies.
* Blob files are immutable, which enables making blob files a part of the [Version](https://github.com/facebook/rocksdb/wiki/Terminology). This in turn makes the read-path essentially lock-free.
* Similarly to SST files, blob files are sorted by key, which enables performance improvements like using readahead during compaction and iteration.
* When it comes to garbage collection, blobs can be relocated and the corresponding blob references can be updated at the same time, as they are encountered during compaction (without any additional LSM tree operations).
* It opens up the possibility of file format optimizations that involve buffering (like dictionary compression).

## Features

In terms of functionality, BlobDB is near feature parity with vanilla RocksDB. In particular, it supports the following:

* write APIs: `Put`, `Merge`, `Delete`, `SingleDelete`, `DeleteRange`, `Write` with all write options 
* read APIs: `Get`, `MultiGet` (including batched `MultiGet`), iterators, and `GetMergeOperands`
* flush including atomic and manual flush
* compaction (with integrated garbage collection), subcompactions, and the manual compaction APIs `CompactFiles` and `CompactRange`
* WAL and the various recovery modes
* keeping track of blob file metadata (like number and total size of all blobs, as well as number and total size of garbage blobs in each blob file) in the MANIFEST
* snapshots
* per-blob compression and checksums (CRC32c)
* column families
* compaction filters (with a BlobDB-specific optimization)
* checkpoints
* backup/restore
* transactions
* per-file checksums
* SST file manager integration for tracking and rate-limited deletion of blob files
* blob file cache of frequently used blob files
* blob cache of frequently used blobs (including support for cache warming and using a secondary cache)
* statistics
* DB properties
* metadata APIs: `GetColumnFamilyMetaData`, `GetAllColumnFamilyMetaData`, and `GetLiveFilesStorageInfo`
* `EventListener` interface
* direct I/O
* I/O rate limiting
* I/O tracing
* C and Java bindings
* tooling (`ldb` and `sst_dump` integration, `blob_dump` tool) 

The BlobDB-specific aspects of some of these features are detailed below.

## API

### Column family options

BlobDB can be configured (on a per-column family basis if needed) simply by using the following options:

* `enable_blob_files`: set it to `true` to enable key-value separation.
* `min_blob_size`: values at or above this threshold will be written to blob files during flush or compaction.
* `blob_file_size`: the size limit for blob files. (Note that a single flush or (sub)compaction may write multiple blob files.) Since the space is reclaimed in blob file increments, the value of this parameter heavily influences space amplification.
* `blob_compression_type`: the compression type to use for blob files. All blobs in the same file are compressed using the same algorithm.
* `enable_blob_garbage_collection`: set this to `true` to make BlobDB actively relocate valid blobs from the oldest blob files as they are encountered during compaction.
* `blob_garbage_collection_age_cutoff`: the cutoff that the GC logic uses to determine which blob files should be considered “old.” For example, the default value of 0.25 signals to RocksDB that blobs residing in the oldest 25% of blob files should be relocated by GC. This parameter can be tuned to adjust the trade-off between write amplification and space amplification.
* `blob_garbage_collection_force_threshold`: if the ratio of garbage in the oldest blob files exceeds this threshold, targeted compactions are scheduled in order to force garbage collecting the blob files in question, assuming they are all eligible based on the value of `blob_garbage_collection_age_cutoff` above. This can help reduce space amplification in the case of skewed workloads where the affected files would not otherwise be picked up for compaction. This option is currently only supported with leveled compactions.
* `blob_compaction_readahead_size`: when set, BlobDB will prefetch data from blob files in chunks of the configured size during compaction. This can improve compaction performance when the database resides on higher-latency storage like HDDs or remote filesystems.
* `blob_file_starting_level`: enable blob files starting from a certain LSM tree level. For certain use cases that have a mix of short-lived and long-lived values, it might make sense to support extracting large values only during compactions whose output level is greater than or equal to a specified LSM tree level. This could reduce the space amplification caused by large values that are turned into garbage shortly after being written at the price of some write amplification incurred by long-lived values whose extraction to blob files is delayed.
* `blob_cache`: the `Cache` object to use for blobs. Using a dedicated `Cache` object for blobs and using the same object for the block cache and the blob cache are both supported. Note that blobs are less valuable targets for caching than SST data blocks for two reasons: 1) with BlobDB, data blocks containing blob references conceptually form an index structure which has to be consulted before we can read any blob value, and 2) cached blobs represent only a single key-value, while cached data blocks generally contain multiple key-values. Because of this, blobs are inserted into the cache with "bottom" priority, which is lower than the priority of data blocks. Note: using a secondary cache with the blob cache is also supported.
* `prepopulate_blob_cache`: when set to `kFlushOnly`, BlobDB will insert newly written blobs into the blob cache during flush. This can improve performance when reading back these blobs would otherwise be expensive (e.g. when using direct I/O or remote storage), or when the workload has a high temporal locality.

With the exception of `blob_cache`, the above options are all dynamically adjustable via the `SetOptions` API; changing them will affect subsequent flushes and compactions but not ones that are already in progress.

### Compaction filters

As mentioned above, BlobDB also supports compaction filters. Key-value separation actually enables an optimization here: if the compaction filter of an application can make a decision about a key-value solely based on the key, it is unnecessary to read the value from the blob file. Applications can take advantage of this optimization by implementing the new `FilterBlobByKey` method of the `CompactionFilter` interface. This method gets called by RocksDB first whenever it encounters a key-value where the value is stored in a blob file. If this method returns a “final” decision like `kKeep`, `kRemove`, `kChangeValue`, or `kRemoveAndSkipUntil`, RocksDB will honor that decision; on the other hand, if the method returns `kUndetermined`, RocksDB will read the blob from the blob file and call `FilterV2` with the value in the usual fashion.

### Statistics

The integrated implementation supports the tickers `BLOB_DB_BLOB_FILE_BYTES_{READ,WRITTEN}`, `BLOB_DB_BLOB_FILE_SYNCED`, and `BLOB_DB_GC_{NUM_KEYS,BYTES}_RELOCATED`, as well as the histograms `BLOB_DB_BLOB_FILE_{READ,WRITE,SYNC}_MICROS` and `BLOB_DB_(DE)COMPRESSION_MICROS`. Note that the vast majority of the legacy BlobDB's tickers/histograms are not applicable to the new implementation, since they e.g. pertain to calling dedicated BlobDB APIs (which the integrated BlobDB does not have) or are tied to the legacy BlobDB's design of writing blob files synchronously when a write API is called. Such statistics are marked "legacy BlobDB only" in
`statistics.h`.

For integrated BlobDB, we add new tickers `BLOB_DB_CACHE_MISS`, `BLOB_DB_CACHE_HIT`, `BLOB_DB_CACHE_ADD`, `BLOB_DB_CACHE_ADD_FAILURES`, `BLOB_DB_CACHE_BYTES_READ` and `BLOB_DB_CACHE_BYTES_WRITE` for blob cache.

### Performance context

See details [here](https://github.com/facebook/rocksdb/wiki/Perf-Context-and-IO-Stats-Context#blob-cache).

### DB properties

We support the following BlobDB-related properties:

* `rocksdb.num-blob-files`: number of blob files in the current Version.
* `rocksdb.blob-stats`: returns the total number and size of all blob files, as well as the total amount of garbage (in bytes) in the blob files in the current Version and the corresponding space amplification.
* `rocksdb.total-blob-file-size`: the total size of all blob files aggregated across all Versions.
* `rocksdb.live-blob-file-size`: the total size of all blob files in the current Version.
* `rocksdb.live-blob-file-garbage-size`: the total amount of garbage in all blob files in the current Version.
* `rocksdb.estimate-live-data-size`: this is a non-BlobDB specific property that was extended to also consider the live data bytes residing in blob files (which can be computed exactly by subtracting garbage bytes from total bytes and summing over all blob files in the current Version).
* `rocksdb.blob-cache-capacity`: blob cache capacity.
* `rocksdb.blob-cache-usage`: the memory size for the entries residing in blob cache.
* `rocksdb.blob-cache-pinned-usage`: the memory size for the entries being pinned in blob cache.

Note that the properties `rocksdb.blob-cache-capacity`, `rocksdb.blob-cache-usage`, and `rocksdb.blob-cache-pinned-usage` simply return the capacity, usage, and pinned usage of the blob cache object. This means that if you use a shared block/blob cache, the usage values returned will include the memory footprint of both blocks and blobs.

### Metadata APIs

For BlobDB, the `ColumnFamilyMetaData` structure has been extended with the following information:

* a vector of `BlobMetaData` objects, one for each live blob file, which contain the file number, file name and path, file size, total number and size of all blobs in the file, total number and size of all garbage blobs in the file, as well as the file checksum method and checksum value.
* the total number and size of all live blob files.

This information can be retrieved using the `GetColumnFamilyMetaData` API for any given column family. You can also retrieve a consistent view of all column families using the `GetAllColumnFamilyMetaData` API. In addition, the `GetLiveFilesStorageInfo` API also provides information about blob files.

### EventListener interface

We expose the following BlobDB-related information via the `EventListener` interface:

* Job-level information: `FlushJobInfo` and `CompactionJobInfo` contain information about the blob files generated by flush and compaction jobs, respectively. Both structures contain a vector of `BlobFileInfo` objects corresponding to the newly generated blob files; in addition, `CompactionJobInfo` also contains a vector of `BlobFileGarbageInfo` structures that describe the additional amount of unreferenced garbage produced by the compaction job in question.
* File-level information: RocksDB notifies the listener about events related to the lifecycle of any given blob file through the functions `OnBlobFileCreationStarted`, `OnBlobFileCreated`, and `OnBlobFileDeleted`.
* Operation-level information: the `OnFile*Finish` notifications are also supported for blob files.

### Manual compactions

When using the `CompactRange` API, it is possible for users to temporarily override the garbage collection options. Note that these overrides affect only the given manual compaction. An example use case for this would be doing a full key-space manual compaction with the garbage collection age cutoff set to 1.0 (100%) in order to eliminate all garbage both from the LSM tree and the blob files and minimize the space occupied by the database. See more details about these `CompactRange` options [here](https://github.com/facebook/rocksdb/wiki/Manual-Compaction#compactrange).

## Performance tuning

In terms of compaction styles, we recommend using leveled compaction with BlobDB. The rationale behind universal compaction in general is to provide lower write amplification at the expense of higher read amplification; however, according to our benchmarks, BlobDB can provide very low write amp and good read performance with leveled compaction. Therefore, there is really no reason to take the hit in read performance that comes with universal compaction.

In addition to the BlobDB options above, consider tuning the following non-BlobDB specific options:

* `write_buffer_size`: this is the memtable size. You might want to increase it for large-value workloads to ensure that SST and blob files contain a decent number of keys.
* `target_file_size_base`: the target size of SST files. Note that even when using BlobDB, it is important to have an LSM tree with a “nice” shape and multiple levels and files per level to prevent heavy compactions. Since BlobDB extracts and writes large values to blob files, it makes sense to make this parameter significantly smaller than the memtable size, for instance by dividing up the memtable size proportionally based on the ratio of key size to value size.
* `max_bytes_for_level_base`: consider setting this to a multiple (e.g. 8x or 10x) of `target_file_size_base`.
* `compaction_readahead_size`: this is the readahead size for SST files during compactions. Again, it might make sense to set this when the database is on slower storage.
* `writable_file_max_buffer_size`: buffer size used when writing SST and blob files. Increasing it results in larger I/Os, which might be beneficial on certain types of storage.
* `max_background_flushes`, `max_background_compactions`, and corresponding thread pool sizes: one thing to keep in mind here is that while flushes write roughly the same amount of data with or without BlobDB, compactions can be much more lightweight with BlobDB than without
 (unless heavy garbage collection is involved). Thus, it might make sense to allow relatively more parallel flushes, and relatively fewer parallel compactions.
* `high_pri_pool_ratio` and `low_pri_pool_ratio` in `LRUCacheOptions`: ratio of the high and low priority pools in the LRU cache, respectively. When using a shared block/blob cache, having these dedicated pools can improve performance by making it more likely that more valuable SST blocks can remain in the cache, or, looking at it the other way, less valuable blobs get evicted sooner.

## Future work

There is a couple of remaining features that are not yet supported by BlobDB; namely, we don’t currently support secondary instances and ingestion of blob files. Also, some newer features (e.g. tiered storage) might not support BlobDB yet. We will continue to work on closing these gaps.

We also have further plans when it comes to performance. These include improving iterator performance and evolving the blob file format amongst others.
