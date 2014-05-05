# Rocksdb Change Log

## 3.0.0 (05/05/2014)

### Public API changes
* Added _LEVEL to all InfoLogLevel enums
* Deprecated ReadOptions.prefix and ReadOptions.prefix_seek. Seek() defaults to prefix-based seek when Options.prefix_extractor is supplied. More detail is documented in https://github.com/facebook/rocksdb/wiki/Prefix-Seek-API-Changes

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
