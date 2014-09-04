# Rocksdb Change Log

### Unreleased

----- Past Releases -----

## 3.5.0 (9/3/2014)
### New Features
* Add include/utilities/write_batch_with_index.h, providing a utilitiy class to query data out of WriteBatch when building it.
* Move BlockBasedTable related options to BlockBasedTableOptions from Options. Change corresponding JNI interface. Options affected include:
  no_block_cache, block_cache, block_cache_compressed, block_size, block_size_deviation, block_restart_interval, filter_policy, whole_key_filtering. filter_policy is changed to shared_ptr from a raw pointer.
* Remove deprecated options: disable_seek_compaction and db_stats_log_interval
* OptimizeForPointLookup() takes one parameter for block cache size. It now builds hash index, bloom filter, and block cache.

### Public API changes
* The Prefix Extractor used with V2 compaction filters is now passed user key to SliceTransform::Transform instead of unparsed RocksDB key.

## 3.4.0 (8/18/2014)
### New Features
* Support Multiple DB paths in universal style compactions
* Add feature of storing plain table index and bloom filter in SST file.
* CompactRange() will never output compacted files to level 0. This used to be the case when all the compaction input files were at level 0.

### Public API changes
* DBOptions.db_paths now is a vector of a DBPath structure which indicates both of path and target size
* NewPlainTableFactory instead of bunch of parameters now accepts PlainTableOptions, which is defined in include/rocksdb/table.h
* Moved include/utilities/*.h to include/rocksdb/utilities/*.h
* Statistics APIs now take uint32_t as type instead of Tickers. Also make two access functions getTickerCount and histogramData const
* Add DB property rocksdb.estimate-num-keys, estimated number of live keys in DB.
* Add DB::GetIntProperty(), which returns DB properties that are integer as uint64_t.
* The Prefix Extractor used with V2 compaction filters is now passed user key to SliceTransform::Transform instead of unparsed RocksDB key.

## 3.3.0 (7/10/2014)
### New Features
* Added JSON API prototype.
* HashLinklist reduces performance outlier caused by skewed bucket by switching data in the bucket from linked list to skip list. Add parameter threshold_use_skiplist in NewHashLinkListRepFactory().
* RocksDB is now able to reclaim storage space more effectively during the compaction process.  This is done by compensating the size of each deletion entry by the 2X average value size, which makes compaction to be triggerred by deletion entries more easily.
* Add TimeOut API to write.  Now WriteOptions have a variable called timeout_hint_us.  With timeout_hint_us set to non-zero, any write associated with this timeout_hint_us may be aborted when it runs longer than the specified timeout_hint_us, and it is guaranteed that any write completes earlier than the specified time-out will not be aborted due to the time-out condition.
* Add a rate_limiter option, which controls total throughput of flush and compaction. The throughput is specified in bytes/sec. Flush always has precedence over compaction when available bandwidth is constrained.

### Public API changes
* Removed NewTotalOrderPlainTableFactory because it is not used and implemented semantically incorrect.

## 3.2.0 (06/20/2014)

### Public API changes
* We removed seek compaction as a concept from RocksDB because:
1) It makes more sense for spinning disk workloads, while RocksDB is primarily designed for flash and memory,
2) It added some complexity to the important code-paths,
3) None of our internal customers were really using it.
Because of that, Options::disable_seek_compaction is now obsolete. It is still a parameter in Options, so it does not break the build, but it does not have any effect. We plan to completely remove it at some point, so we ask users to please remove this option from your code base.
* Add two paramters to NewHashLinkListRepFactory() for logging on too many entries in a hash bucket when flushing.
* Added new option BlockBasedTableOptions::hash_index_allow_collision. When enabled, prefix hash index for block-based table will not store prefix and allow hash collision, reducing memory consumption.

### New Features
* PlainTable now supports a new key encoding: for keys of the same prefix, the prefix is only written once. It can be enabled through encoding_type paramter of NewPlainTableFactory()
* Add AdaptiveTableFactory, which is used to convert from a DB of PlainTable to BlockBasedTabe, or vise versa. It can be created using NewAdaptiveTableFactory()

### Performance Improvements
* Tailing Iterator re-implemeted with ForwardIterator + Cascading Search Hint , see ~20% throughput improvement.

## 3.1.0 (05/21/2014)

### Public API changes
* Replaced ColumnFamilyOptions::table_properties_collectors with ColumnFamilyOptions::table_properties_collector_factories

### New Features
* Hash index for block-based table will be materialized and reconstructed more efficiently. Previously hash index is constructed by scanning the whole table during every table open.
* FIFO compaction style

## 3.0.0 (05/05/2014)

### Public API changes
* Added _LEVEL to all InfoLogLevel enums
* Deprecated ReadOptions.prefix and ReadOptions.prefix_seek. Seek() defaults to prefix-based seek when Options.prefix_extractor is supplied. More detail is documented in https://github.com/facebook/rocksdb/wiki/Prefix-Seek-API-Changes
* MemTableRepFactory::CreateMemTableRep() takes info logger as an extra parameter.

### New Features
* Column family support
* Added an option to use different checksum functions in BlockBasedTableOptions
* Added ApplyToAllCacheEntries() function to Cache

## 2.8.0 (04/04/2014)

* Removed arena.h from public header files.
* By default, checksums are verified on every read from database
* Change default value of several options, including: paranoid_checks=true, max_open_files=5000, level0_slowdown_writes_trigger=20, level0_stop_writes_trigger=24, disable_seek_compaction=true, max_background_flushes=1 and allow_mmap_writes=false
* Added is_manual_compaction to CompactionFilter::Context
* Added "virtual void WaitForJoin()" in class Env. Default operation is no-op.
* Removed BackupEngine::DeleteBackupsNewerThan() function
* Added new option -- verify_checksums_in_compaction
* Changed Options.prefix_extractor from raw pointer to shared_ptr (take ownership)
  Changed HashSkipListRepFactory and HashLinkListRepFactory constructor to not take SliceTransform object (use Options.prefix_extractor implicitly)
* Added Env::GetThreadPoolQueueLen(), which returns the waiting queue length of thread pools
* Added a command "checkconsistency" in ldb tool, which checks
  if file system state matches DB state (file existence and file sizes)
* Separate options related to block based table to a new struct BlockBasedTableOptions.
* WriteBatch has a new function Count() to return total size in the batch, and Data() now returns a reference instead of a copy
* Add more counters to perf context.
* Supports several more DB properties: compaction-pending, background-errors and cur-size-active-mem-table.

### New Features
* If we find one truncated record at the end of the MANIFEST or WAL files,
  we will ignore it. We assume that writers of these records were interrupted
  and that we can safely ignore it.
* A new SST format "PlainTable" is added, which is optimized for memory-only workloads. It can be created through NewPlainTableFactory() or NewTotalOrderPlainTableFactory().
* A new mem table implementation hash linked list optimizing for the case that there are only few keys for each prefix, which can be created through NewHashLinkListRepFactory().
* Merge operator supports a new function PartialMergeMulti() to allow users to do partial merges against multiple operands.
* Now compaction filter has a V2 interface. It buffers the kv-pairs sharing the same key prefix, process them in batches, and return the batched results back to DB. The new interface uses a new structure CompactionFilterContext for the same purpose as CompactionFilter::Context in V1.
* Geo-spatial support for locations and radial-search.

## 2.7.0 (01/28/2014)

### Public API changes

* Renamed `StackableDB::GetRawDB()` to `StackableDB::GetBaseDB()`.
* Renamed `WriteBatch::Data()` `const std::string& Data() const`.
* Renamed class `TableStats` to `TableProperties`.
* Deleted class `PrefixHashRepFactory`. Please use `NewHashSkipListRepFactory()` instead.
* Supported multi-threaded `EnableFileDeletions()` and `DisableFileDeletions()`.
* Added `DB::GetOptions()`.
* Added `DB::GetDbIdentity()`.

### New Features

* Added [BackupableDB](https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB%3F)
* Implemented [TailingIterator](https://github.com/facebook/rocksdb/wiki/Tailing-Iterator), a special type of iterator that
  doesn't create a snapshot (can be used to read newly inserted data)
  and is optimized for doing sequential reads.
* Added property block for table, which allows (1) a table to store
  its metadata and (2) end user to collect and store properties they
  are interested in.
* Enabled caching index and filter block in block cache (turned off by default).
* Supported error report when doing manual compaction.
* Supported additional Linux platform flavors and Mac OS.
* Put with `SliceParts` - Variant of `Put()` that gathers output like `writev(2)`
* Bug fixes and code refactor for compatibility with upcoming Column
  Family feature.

### Performance Improvements

* Huge benchmark performance improvements by multiple efforts. For example, increase in readonly QPS from about 530k in 2.6 release to 1.1 million in 2.7 [1]
* Speeding up a way RocksDB deleted obsolete files - no longer listing the whole directory under a lock -- decrease in p99
* Use raw pointer instead of shared pointer for statistics: [5b825d](https://github.com/facebook/rocksdb/commit/5b825d6964e26ec3b4bb6faa708ebb1787f1d7bd) -- huge increase in performance -- shared pointers are slow
* Optimized locking for `Get()` -- [1fdb3f](https://github.com/facebook/rocksdb/commit/1fdb3f7dc60e96394e3e5b69a46ede5d67fb976c) -- 1.5x QPS increase for some workloads
* Cache speedup - [e8d40c3](https://github.com/facebook/rocksdb/commit/e8d40c31b3cca0c3e1ae9abe9b9003b1288026a9)
* Implemented autovector, which allocates first N elements on stack. Most of vectors in RocksDB are small. Also, we never want to allocate heap objects while holding a mutex. -- [c01676e4](https://github.com/facebook/rocksdb/commit/c01676e46d3be08c3c140361ef1f5884f47d3b3c)
* Lots of efforts to move malloc, memcpy and IO outside of locks
