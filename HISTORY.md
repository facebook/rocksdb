# Rocksdb Change Log
## 6.10.2 (6/5/2020)
### Bug fix
* Fix false negative from the VerifyChecksum() API when there is a checksum mismatch in an index partition block in a BlockBasedTable format table file (index_type is kTwoLevelIndexSearch).

## 6.10.1 (5/27/2020)
### Bug fix
* Remove "u'<string>'" in TARGETS file.
* Fix db_stress_lib target in buck.

## 6.10 (5/2/2020)
### Behavior Changes
* Disable delete triggered compaction (NewCompactOnDeletionCollectorFactory) in universal compaction mode and num_levels = 1 in order to avoid a corruption bug.

### Bug Fixes
* Fix wrong result being read from ingested file. May happen when a key in the file happen to be prefix of another key also in the file. The issue can further cause more data corruption. The issue exists with rocksdb >= 5.0.0 since DB::IngestExternalFile() was introduced.
* Finish implementation of BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey. It's now ready for use. Significantly reduces read amplification in some setups, especially for iterator seeks.
* Fix a bug by updating CURRENT file so that it points to the correct MANIFEST file after best-efforts recovery.
* Fixed a bug where ColumnFamilyHandle objects were not cleaned up in case an error happened during BlobDB's open after the base DB had been opened.
* Fix a potential undefined behavior caused by trying to dereference nullable pointer (timestamp argument) in DB::MultiGet.
* Fix a bug caused by not including user timestamp in MultiGet LookupKey construction. This can lead to wrong query result since the trailing bytes of a user key, if not shorter than timestamp, will be mistaken for user timestamp.
* Fix a bug caused by using wrong compare function when sorting the input keys of MultiGet with timestamps.
* Upgraded version of bzip library (1.0.6 -> 1.0.8) used with RocksJava to address potential vulnerabilities if an attacker can manipulate compressed data saved and loaded by RocksDB (not normal). See issue #6703.
* Fix consistency checking error swallowing in some cases when options.force_consistency_checks = true.
* Fix possible false NotFound status from batched MultiGet using index type kHashSearch.
* Fix corruption caused by enabling delete triggered compaction (NewCompactOnDeletionCollectorFactory) in universal compaction mode, along with parallel compactions. The bug can result in two parallel compactions picking the same input files, resulting in the DB resurrecting older and deleted versions of some keys.
* Fix a use-after-free bug in best-efforts recovery. column_family_memtables_ needs to point to valid ColumnFamilySet.
* Let best-efforts recovery ignore corrupted files during table loading.
* Fix a bug when making options.bottommost_compression, options.compression_opts and options.bottommost_compression_opts dynamically changeable: the modified values are not written to option files or returned back to users when being queried.
* Fix a bug where index key comparisons were unaccounted in `PerfContext::user_key_comparison_count` for lookups in files written with `format_version >= 3`.
* Fix many bloom.filter statistics not being updated in batch MultiGet.


### Public API Change
* Add a ConfigOptions argument to the APIs dealing with converting options to and from strings and files.  The ConfigOptions is meant to replace some of the options (such as input_strings_escaped and ignore_unknown_options) and allow for more parameters to be passed in the future without changing the function signature.
* Add NewFileChecksumGenCrc32cFactory to the file checksum public API, such that the builtin Crc32c based file checksum generator factory can be used by applications.
* Add IsDirectory to Env and FS to indicate if a path is a directory.
* ldb now uses options.force_consistency_checks = true by default and "--disable_consistency_checks" is added to disable it.
* Add ReadOptions::deadline to allow users to specify a deadline for MultiGet requests

### New Features
* Added support for pipelined & parallel compression optimization for `BlockBasedTableBuilder`. This optimization makes block building, block compression and block appending a pipeline, and uses multiple threads to accelerate block compression. Users can set `CompressionOptions::parallel_threads` greater than 1 to enable compression parallelism. This feature is experimental for now.
* Provide an allocator for memkind to be used with block cache. This is to work with memory technologies (Intel DCPMM is one such technology currently available) that require different libraries for allocation and management (such as PMDK and memkind). The high capacities available make it possible to provision large caches (up to several TBs in size) beyond what is achievable with DRAM.
* Option `max_background_flushes` can be set dynamically using DB::SetDBOptions().
* Added functionality in sst_dump tool to check the compressed file size for different compression levels and print the time spent on compressing files with each compression type. Added arguments `--compression_level_from` and `--compression_level_to` to report size of all compression levels and one compression_type must be specified with it so that it will report compressed sizes of one compression type with different levels.
* Added statistics for redundant insertions into block cache: rocksdb.block.cache.*add.redundant. (There is currently no coordination to ensure that only one thread loads a table block when many threads are trying to access that same table block.)

### Performance Improvements
* Improve performance of batch MultiGet with partitioned filters, by sharing block cache lookups to applicable filter blocks.
* Reduced memory copies when fetching and uncompressing compressed blocks from sst files.

## 6.9.0 (03/29/2020)
### Behavior changes
* Since RocksDB 6.8, ttl-based FIFO compaction can drop a file whose oldest key becomes older than options.ttl while others have not. This fix reverts this and makes ttl-based FIFO compaction use the file's flush time as the criterion. This fix also requires that max_open_files = -1 and compaction_options_fifo.allow_compaction = false to function properly.

### Public API Change
* Fix spelling so that API now has correctly spelled transaction state name `COMMITTED`, while the old misspelled `COMMITED` is still available as an alias.
* Updated default format_version in BlockBasedTableOptions from 2 to 4. SST files generated with the new default can be read by RocksDB versions 5.16 and newer, and use more efficient encoding of keys in index blocks.
* A new parameter `CreateBackupOptions` is added to both `BackupEngine::CreateNewBackup` and `BackupEngine::CreateNewBackupWithMetadata`, you can decrease CPU priority of `BackupEngine`'s background threads by setting `decrease_background_thread_cpu_priority` and `background_thread_cpu_priority` in `CreateBackupOptions`.
* Updated the public API of SST file checksum. Introduce the FileChecksumGenFactory to create the FileChecksumGenerator for each SST file, such that the FileChecksumGenerator is not shared and it can be more general for checksum implementations. Changed the FileChecksumGenerator interface from Value, Extend, and GetChecksum to Update, Finalize, and GetChecksum. Finalize should be only called once after all data is processed to generate the final checksum. Temproal data should be maintained by the FileChecksumGenerator object itself and finally it can return the checksum string.

### Bug Fixes
* Fix a bug where range tombstone blocks in ingested files were cached incorrectly during ingestion. If range tombstones were read from those incorrectly cached blocks, the keys they covered would be exposed.
* Fix a data race that might cause crash when calling DB::GetCreationTimeOfOldestFile() by a small chance. The bug was introduced in 6.6 Release.
* Fix a bug where a boolean value optimize_filters_for_hits was for max threads when calling load table handles after a flush or compaction. The value is correct to 1. The bug should not cause user visible problems.
* Fix a bug which might crash the service when write buffer manager fails to insert the dummy handle to the block cache.

### Performance Improvements
* In CompactRange, for levels starting from 0, if the level does not have any file with any key falling in the specified range, the level is skipped. So instead of always compacting from level 0, the compaction starts from the first level with keys in the specified range until the last such level.
* Reduced memory copy when reading sst footer and blobdb in direct IO mode.
* When restarting a database with large numbers of sst files, large amount of CPU time is spent on getting logical block size of the sst files, which slows down the starting progress, this inefficiency is optimized away with an internal cache for the logical block sizes.

### New Features
* Basic support for user timestamp in iterator. Seek/SeekToFirst/Next and lower/upper bounds are supported. Reverse iteration is not supported. Merge is not considered.
* When file lock failure when the lock is held by the current process, return acquiring time and thread ID in the error message.
* Added a new option, best_efforts_recovery (default: false), to allow database to open in a db dir with missing table files. During best efforts recovery, missing table files are ignored, and database recovers to the most recent state without missing table file. Cross-column-family consistency is not guaranteed even if WAL is enabled.
* options.bottommost_compression, options.compression_opts and options.bottommost_compression_opts are now dynamically changeable.

## 6.8.0 (02/24/2020)
### Java API Changes
* Major breaking changes to Java comparators, toward standardizing on ByteBuffer for performant, locale-neutral operations on keys (#6252).
* Added overloads of common API methods using direct ByteBuffers for keys and values (#2283).

### Bug Fixes
* Fix incorrect results while block-based table uses kHashSearch, together with Prev()/SeekForPrev().
* Fix a bug that prevents opening a DB after two consecutive crash with TransactionDB, where the first crash recovers from a corrupted WAL with kPointInTimeRecovery but the second cannot.
* Fixed issue #6316 that can cause a corruption of the MANIFEST file in the middle when writing to it fails due to no disk space.
* Add DBOptions::skip_checking_sst_file_sizes_on_db_open. It disables potentially expensive checking of all sst file sizes in DB::Open().
* BlobDB now ignores trivially moved files when updating the mapping between blob files and SSTs. This should mitigate issue #6338 where out of order flush/compaction notifications could trigger an assertion with the earlier code.
* Batched MultiGet() ignores IO errors while reading data blocks, causing it to potentially continue looking for a key and returning stale results.
* `WriteBatchWithIndex::DeleteRange` returns `Status::NotSupported`. Previously it returned success even though reads on the batch did not account for range tombstones. The corresponding language bindings now cannot be used. In C, that includes `rocksdb_writebatch_wi_delete_range`, `rocksdb_writebatch_wi_delete_range_cf`, `rocksdb_writebatch_wi_delete_rangev`, and `rocksdb_writebatch_wi_delete_rangev_cf`. In Java, that includes `WriteBatchWithIndex::deleteRange`.
* Assign new MANIFEST file number when caller tries to create a new MANIFEST by calling LogAndApply(..., new_descriptor_log=true). This bug can cause MANIFEST being overwritten during recovery if options.write_dbid_to_manifest = true and there are WAL file(s).

### Performance Improvements
* Perfom readahead when reading from option files. Inside DB, options.log_readahead_size will be used as the readahead size. In other cases, a default 512KB is used.

### Public API Change
* The BlobDB garbage collector now emits the statistics `BLOB_DB_GC_NUM_FILES` (number of blob files obsoleted during GC), `BLOB_DB_GC_NUM_NEW_FILES` (number of new blob files generated during GC), `BLOB_DB_GC_FAILURES` (number of failed GC passes), `BLOB_DB_GC_NUM_KEYS_RELOCATED` (number of blobs relocated during GC), and `BLOB_DB_GC_BYTES_RELOCATED` (total size of blobs relocated during GC). On the other hand, the following statistics, which are not relevant for the new GC implementation, are now deprecated: `BLOB_DB_GC_NUM_KEYS_OVERWRITTEN`, `BLOB_DB_GC_NUM_KEYS_EXPIRED`, `BLOB_DB_GC_BYTES_OVERWRITTEN`, `BLOB_DB_GC_BYTES_EXPIRED`, and `BLOB_DB_GC_MICROS`.
* Disable recycle_log_file_num when an inconsistent recovery modes are requested: kPointInTimeRecovery and kAbsoluteConsistency

### New Features
* Added the checksum for each SST file generated by Flush or Compaction. Added sst_file_checksum_func to Options such that user can plugin their own SST file checksum function via override the FileChecksumFunc class. If user does not set the sst_file_checksum_func, SST file checksum calculation will not be enabled. The checksum information inlcuding uint32_t checksum value and a checksum function name (string). The checksum information is stored in FileMetadata in version store and also logged to MANIFEST. A new tool is added to LDB such that user can dump out a list of file checksum information from MANIFEST (stored in an unordered_map).
* `db_bench` now supports `value_size_distribution_type`, `value_size_min`, `value_size_max` options for generating random variable sized value. Added `blob_db_compression_type` option for BlobDB to enable blob compression.
* Replace RocksDB namespace "rocksdb" with flag "ROCKSDB_NAMESPACE" which if is not defined, defined as "rocksdb" in header file rocksdb_namespace.h.

## 6.7.0 (01/21/2020)
### Public API Change
* Added a rocksdb::FileSystem class in include/rocksdb/file_system.h to encapsulate file creation/read/write operations, and an option DBOptions::file_system to allow a user to pass in an instance of rocksdb::FileSystem. If its a non-null value, this will take precendence over DBOptions::env for file operations. A new API rocksdb::FileSystem::Default() returns a platform default object. The DBOptions::env option and Env::Default() API will continue to be used for threading and other OS related functions, and where DBOptions::file_system is not specified, for file operations. For storage developers who are accustomed to rocksdb::Env, the interface in rocksdb::FileSystem is new and will probably undergo some changes as more storage systems are ported to it from rocksdb::Env. As of now, no env other than Posix has been ported to the new interface.
* A new rocksdb::NewSstFileManager() API that allows the caller to pass in separate Env and FileSystem objects.
* Changed Java API for RocksDB.keyMayExist functions to use Holder<byte[]> instead of StringBuilder, so that retrieved values need not decode to Strings.
* A new `OptimisticTransactionDBOptions` Option that allows users to configure occ validation policy. The default policy changes from kValidateSerial to kValidateParallel to reduce mutex contention.

### Bug Fixes
* Fix a bug that can cause unnecessary bg thread to be scheduled(#6104).
* Fix crash caused by concurrent CF iterations and drops(#6147).
* Fix a race condition for cfd->log_number_ between manifest switch and memtable switch (PR 6249) when number of column families is greater than 1.
* Fix a bug on fractional cascading index when multiple files at the same level contain the same smallest user key, and those user keys are for merge operands. In this case, Get() the exact key may miss some merge operands.
* Delcare kHashSearch index type feature-incompatible with index_block_restart_interval larger than 1.
* Fixed an issue where the thread pools were not resized upon setting `max_background_jobs` dynamically through the `SetDBOptions` interface.
* Fix a bug that can cause write threads to hang when a slowdown/stall happens and there is a mix of writers with WriteOptions::no_slowdown set/unset.
* Fixed an issue where an incorrect "number of input records" value was used to compute the "records dropped" statistics for compactions.
* Fix a regression bug that causes segfault when hash is used, max_open_files != -1 and total order seek is used and switched back.

### New Features
* It is now possible to enable periodic compactions for the base DB when using BlobDB.
* BlobDB now garbage collects non-TTL blobs when `enable_garbage_collection` is set to `true` in `BlobDBOptions`. Garbage collection is performed during compaction: any valid blobs located in the oldest N files (where N is the number of non-TTL blob files multiplied by the value of `BlobDBOptions::garbage_collection_cutoff`) encountered during compaction get relocated to new blob files, and old blob files are dropped once they are no longer needed. Note: we recommend enabling periodic compactions for the base DB when using this feature to deal with the case when some old blob files are kept alive by SSTs that otherwise do not get picked for compaction.
* `db_bench` now supports the `garbage_collection_cutoff` option for BlobDB.
* Introduce ReadOptions.auto_prefix_mode. When set to true, iterator will return the same result as total order seek, but may choose to use prefix seek internally based on seek key and iterator upper bound.
* MultiGet() can use IO Uring to parallelize read from the same SST file. This featuer is by default disabled. It can be enabled with environment variable ROCKSDB_USE_IO_URING.

## 6.6.2 (01/13/2020)
### Bug Fixes
* Fixed a bug where non-L0 compaction input files were not considered to compute the `creation_time` of new compaction outputs.

## 6.6.1 (01/02/2020)
### Bug Fixes
* Fix a bug in WriteBatchWithIndex::MultiGetFromBatchAndDB, which is called by Transaction::MultiGet, that causes due to stale pointer access when the number of keys is > 32
* Fixed two performance issues related to memtable history trimming. First, a new SuperVersion is now created only if some memtables were actually trimmed. Second, trimming is only scheduled if there is at least one flushed memtable that is kept in memory for the purposes of transaction conflict checking.
* BlobDB no longer updates the SST to blob file mapping upon failed compactions.
* Fix a bug in which a snapshot read through an iterator could be affected by a DeleteRange after the snapshot (#6062).
* Fixed a bug where BlobDB was comparing the `ColumnFamilyHandle` pointers themselves instead of only the column family IDs when checking whether an API call uses the default column family or not.
* Delete superversions in BackgroundCallPurge.
* Fix use-after-free and double-deleting files in BackgroundCallPurge().

## 6.6.0 (11/25/2019)
### Bug Fixes
* Fix data corruption caused by output of intra-L0 compaction on ingested file not being placed in correct order in L0.
* Fix a data race between Version::GetColumnFamilyMetaData() and Compaction::MarkFilesBeingCompacted() for access to being_compacted (#6056). The current fix acquires the db mutex during Version::GetColumnFamilyMetaData(), which may cause regression.
* Fix a bug in DBIter that is_blob_ state isn't updated when iterating backward using seek.
* Fix a bug when format_version=3, partitioned filters, and prefix search are used in conjunction. The bug could result into Seek::(prefix) returning NotFound for an existing prefix.
* Revert the feature "Merging iterator to avoid child iterator reseek for some cases (#5286)" since it might cause strong results when reseek happens with a different iterator upper bound.
* Fix a bug causing a crash during ingest external file when background compaction cause severe error (file not found).
* Fix a bug when partitioned filters and prefix search are used in conjunction, ::SeekForPrev could return invalid for an existing prefix. ::SeekForPrev might be called by the user, or internally on ::Prev, or within ::Seek if the return value involves Delete or a Merge operand.
* Fix OnFlushCompleted fired before flush result persisted in MANIFEST when there's concurrent flush job. The bug exists since OnFlushCompleted was introduced in rocksdb 3.8.
* Fixed an sst_dump crash on some plain table SST files.
* Fixed a memory leak in some error cases of opening plain table SST files.
* Fix a bug when a crash happens while calling WriteLevel0TableForRecovery for multiple column families, leading to a column family's log number greater than the first corrutped log number when the DB is being opened in PointInTime recovery mode during next recovery attempt (#5856).

### New Features
* Universal compaction to support options.periodic_compaction_seconds. A full compaction will be triggered if any file is over the threshold.
* `GetLiveFilesMetaData` and `GetColumnFamilyMetaData` now expose the file number of SST files as well as the oldest blob file referenced by each SST.
* A batched MultiGet API (DB::MultiGet()) that supports retrieving keys from multiple column families.
* Full and partitioned filters in the block-based table use an improved Bloom filter implementation, enabled with format_version 5 (or above) because previous releases cannot read this filter. This replacement is faster and more accurate, especially for high bits per key or millions of keys in a single (full) filter. For example, the new Bloom filter has the same false positive rate at 9.55 bits per key as the old one at 10 bits per key, and a lower false positive rate at 16 bits per key than the old one at 100 bits per key.
* Added AVX2 instructions to USE_SSE builds to accelerate the new Bloom filter and XXH3-based hash function on compatible x86_64 platforms (Haswell and later, ~2014).
* Support options.ttl or options.periodic_compaction_seconds with options.max_open_files = -1. File's oldest ancester time and file creation time will be written to manifest. If it is availalbe, this information will be used instead of creation_time and file_creation_time in table properties.
* Setting options.ttl for universal compaction now has the same meaning as setting periodic_compaction_seconds.
* SstFileMetaData also returns file creation time and oldest ancester time.
* The `sst_dump` command line tool `recompress` command now displays how many blocks were compressed and how many were not, in particular how many were not compressed because the compression ratio was not met (12.5% threshold for GoodCompressionRatio), as seen in the `number.block.not_compressed` counter stat since version 6.0.0.
* The block cache usage is now takes into account the overhead of metadata per each entry. This results into more accurate management of memory. A side-effect of this feature is that less items are fit into the block cache of the same size, which would result to higher cache miss rates. This can be remedied by increasing the block cache size or passing kDontChargeCacheMetadata to its constuctor to restore the old behavior.
* When using BlobDB, a mapping is maintained and persisted in the MANIFEST between each SST file and the oldest non-TTL blob file it references.
* `db_bench` now supports and by default issues non-TTL Puts to BlobDB. TTL Puts can be enabled by specifying a non-zero value for the `blob_db_max_ttl_range` command line parameter explicitly.
* `sst_dump` now supports printing BlobDB blob indexes in a human-readable format. This can be enabled by specifying the `decode_blob_index` flag on the command line.
* A number of new information elements are now exposed through the EventListener interface. For flushes, the file numbers of the new SST file and the oldest blob file referenced by the SST are propagated. For compactions, the level, file number, and the oldest blob file referenced are passed to the client for each compaction input and output file.

### Public API Change
* RocksDB release 4.1 or older will not be able to open DB generated by the new release. 4.2 was released on Feb 23, 2016.
* TTL Compactions in Level compaction style now initiate successive cascading compactions on a key range so that it reaches the bottom level quickly on TTL expiry. `creation_time` table property for compaction output files is now set to the minimum of the creation times of all compaction inputs.
* With FIFO compaction style, options.periodic_compaction_seconds will have the same meaning as options.ttl. Whichever stricter will be used. With the default options.periodic_compaction_seconds value with options.ttl's default of 0, RocksDB will give a default of 30 days.
* Added an API GetCreationTimeOfOldestFile(uint64_t* creation_time) to get the file_creation_time of the oldest SST file in the DB.
* FilterPolicy now exposes additional API to make it possible to choose filter configurations based on context, such as table level and compaction style. See `LevelAndStyleCustomFilterPolicy` in db_bloom_filter_test.cc. While most existing custom implementations of FilterPolicy should continue to work as before, those wrapping the return of NewBloomFilterPolicy will require overriding new function `GetBuilderWithContext()`, because calling `GetFilterBitsBuilder()` on the FilterPolicy returned by NewBloomFilterPolicy is no longer supported.
* An unlikely usage of FilterPolicy is no longer supported. Calling GetFilterBitsBuilder() on the FilterPolicy returned by NewBloomFilterPolicy will now cause an assertion violation in debug builds, because RocksDB has internally migrated to a more elaborate interface that is expected to evolve further. Custom implementations of FilterPolicy should work as before, except those wrapping the return of NewBloomFilterPolicy, which will require a new override of a protected function in FilterPolicy.
* NewBloomFilterPolicy now takes bits_per_key as a double instead of an int. This permits finer control over the memory vs. accuracy trade-off in the new Bloom filter implementation and should not change source code compatibility.
* The option BackupableDBOptions::max_valid_backups_to_open is now only used when opening BackupEngineReadOnly. When opening a read/write BackupEngine, anything but the default value logs a warning and is treated as the default. This change ensures that backup deletion has proper accounting of shared files to ensure they are deleted when no longer referenced by a backup.
* Deprecate `snap_refresh_nanos` option.
* Added DisableManualCompaction/EnableManualCompaction to stop and resume manual compaction.
* Add TryCatchUpWithPrimary() to StackableDB in non-LITE mode.
* Add a new Env::LoadEnv() overloaded function to return a shared_ptr to Env.
* Flush sets file name to "(nil)" for OnTableFileCreationCompleted() if the flush does not produce any L0. This can happen if the file is empty thus delete by RocksDB.

### Default Option Changes
* Changed the default value of periodic_compaction_seconds to `UINT64_MAX - 1` which allows RocksDB to auto-tune periodic compaction scheduling. When using the default value, periodic compactions are now auto-enabled if a compaction filter is used. A value of `0` will turn off the feature completely.
* Changed the default value of ttl to `UINT64_MAX - 1` which allows RocksDB to auto-tune ttl value. When using the default value, TTL will be auto-enabled to 30 days, when the feature is supported. To revert the old behavior, you can explicitly set it to 0.

### Performance Improvements
* For 64-bit hashing, RocksDB is standardizing on a slightly modified preview version of XXH3. This function is now used for many non-persisted hashes, along with fastrange64() in place of the modulus operator, and some benchmarks show a slight improvement.
* Level iterator to invlidate the iterator more often in prefix seek and the level is filtered out by prefix bloom.

## 6.5.2 (11/15/2019)
### Bug Fixes
* Fix a assertion failure in MultiGet() when BlockBasedTableOptions::no_block_cache is true and there is no compressed block cache
* Fix a buffer overrun problem in BlockBasedTable::MultiGet() when compression is enabled and no compressed block cache is configured.
* If a call to BackupEngine::PurgeOldBackups or BackupEngine::DeleteBackup suffered a crash, power failure, or I/O error, files could be left over from old backups that could only be purged with a call to GarbageCollect. Any call to PurgeOldBackups, DeleteBackup, or GarbageCollect should now suffice to purge such files.

## 6.5.1 (10/16/2019)
### Bug Fixes
* Revert the feature "Merging iterator to avoid child iterator reseek for some cases (#5286)" since it might cause strange results when reseek happens with a different iterator upper bound.
* Fix a bug in BlockBasedTableIterator that might return incorrect results when reseek happens with a different iterator upper bound.
* Fix a bug when partitioned filters and prefix search are used in conjunction, ::SeekForPrev could return invalid for an existing prefix. ::SeekForPrev might be called by the user, or internally on ::Prev, or within ::Seek if the return value involves Delete or a Merge operand.

## 6.5.0 (9/13/2019)
### Bug Fixes
* Fixed a number of data races in BlobDB.
* Fix a bug where the compaction snapshot refresh feature is not disabled as advertised when `snap_refresh_nanos` is set to 0..
* Fix bloom filter lookups by the MultiGet batching API when BlockBasedTableOptions::whole_key_filtering is false, by checking that a key is in the perfix_extractor domain and extracting the prefix before looking up.
* Fix a bug in file ingestion caused by incorrect file number allocation when the number of column families involved in the ingestion exceeds 2.

### New Features
* Introduced DBOptions::max_write_batch_group_size_bytes to configure maximum limit on number of bytes that are written in a single batch of WAL or memtable write. It is followed when the leader write size is larger than 1/8 of this limit.
* VerifyChecksum() by default will issue readahead. Allow ReadOptions to be passed in to those functions to override the readhead size. For checksum verifying before external SST file ingestion, a new option IngestExternalFileOptions.verify_checksums_readahead_size, is added for this readahead setting.
* When user uses options.force_consistency_check in RocksDb, instead of crashing the process, we now pass the error back to the users without killing the process.
* Add an option `memtable_insert_hint_per_batch` to WriteOptions. If it is true, each WriteBatch will maintain its own insert hints for each memtable in concurrent write. See include/rocksdb/options.h for more details.

### Public API Change
* Added max_write_buffer_size_to_maintain option to better control memory usage of immutable memtables.
* Added a lightweight API GetCurrentWalFile() to get last live WAL filename and size. Meant to be used as a helper for backup/restore tooling in a larger ecosystem such as MySQL with a MyRocks storage engine.
* The MemTable Bloom filter, when enabled, now always uses cache locality. Options::bloom_locality now only affects the PlainTable SST format.

### Performance Improvements
* Improve the speed of the MemTable Bloom filter, reducing the write overhead of enabling it by 1/3 to 1/2, with similar benefit to read performance.

## 6.4.0 (7/30/2019)
### Default Option Change
* LRUCacheOptions.high_pri_pool_ratio is set to 0.5 (previously 0.0) by default, which means that by default midpoint insertion is enabled. The same change is made for the default value of high_pri_pool_ratio argument in NewLRUCache(). When block cache is not explicitly created, the small block cache created by BlockBasedTable will still has this option to be 0.0.
* Change BlockBasedTableOptions.cache_index_and_filter_blocks_with_high_priority's default value from false to true.

### Public API Change
* Filter and compression dictionary blocks are now handled similarly to data blocks with regards to the block cache: instead of storing objects in the cache, only the blocks themselves are cached. In addition, filter and compression dictionary blocks (as well as filter partitions) no longer get evicted from the cache when a table is closed.
* Due to the above refactoring, block cache eviction statistics for filter and compression dictionary blocks are temporarily broken. We plan to reintroduce them in a later phase.
* The semantics of the per-block-type block read counts in the performance context now match those of the generic block_read_count.
* Errors related to the retrieval of the compression dictionary are now propagated to the user.
* db_bench adds a "benchmark" stats_history, which prints out the whole stats history.
* Overload GetAllKeyVersions() to support non-default column family.
* Added new APIs ExportColumnFamily() and CreateColumnFamilyWithImport() to support export and import of a Column Family. https://github.com/facebook/rocksdb/issues/3469
* ldb sometimes uses a string-append merge operator if no merge operator is passed in. This is to allow users to print keys from a DB with a merge operator.
* Replaces old Registra with ObjectRegistry to allow user to create custom object from string, also add LoadEnv() to Env.
* Added new overload of GetApproximateSizes which gets SizeApproximationOptions object and returns a Status. The older overloads are redirecting their calls to this new method and no longer assert if the include_flags doesn't have either of INCLUDE_MEMTABLES or INCLUDE_FILES bits set. It's recommended to use the new method only, as it is more type safe and returns a meaningful status in case of errors.
* LDBCommandRunner::RunCommand() to return the status code as an integer, rather than call exit() using the code.

### New Features
* Add argument `--secondary_path` to ldb to open the database as the secondary instance. This would keep the original DB intact.
* Compression dictionary blocks are now prefetched and pinned in the cache (based on the customer's settings) the same way as index and filter blocks.
* Added DBOptions::log_readahead_size which specifies the number of bytes to prefetch when reading the log. This is mostly useful for reading a remotely located log, as it can save the number of round-trips. If 0 (default), then the prefetching is disabled.
* Added new option in SizeApproximationOptions used with DB::GetApproximateSizes. When approximating the files total size that is used to store a keys range, allow approximation with an error margin of up to total_files_size * files_size_error_margin. This allows to take some shortcuts in files size approximation, resulting in better performance, while guaranteeing the resulting error is within a reasonable margin.
* Support loading custom objects in unit tests. In the affected unit tests, RocksDB will create custom Env objects based on environment variable TEST_ENV_URI. Users need to make sure custom object types are properly registered. For example, a static library should expose a `RegisterCustomObjects` function. By linking the unit test binary with the static library, the unit test can execute this function.

### Performance Improvements
* Reduce iterator key comparison for upper/lower bound check.
* Improve performance of row_cache: make reads with newer snapshots than data in an SST file share the same cache key, except in some transaction cases.
* The compression dictionary is no longer copied to a new object upon retrieval.

### Bug Fixes
* Fix ingested file and directory not being fsync.
* Return TryAgain status in place of Corruption when new tail is not visible to TransactionLogIterator.
* Fixed a regression where the fill_cache read option also affected index blocks.
* Fixed an issue where using cache_index_and_filter_blocks==false affected partitions of partitioned indexes/filters as well.

## 6.3.2 (8/15/2019)
### Public API Change
* The semantics of the per-block-type block read counts in the performance context now match those of the generic block_read_count.

### Bug Fixes
* Fixed a regression where the fill_cache read option also affected index blocks.
* Fixed an issue where using cache_index_and_filter_blocks==false affected partitions of partitioned indexes as well.

## 6.3.1 (7/24/2019)
### Bug Fixes
* Fix auto rolling bug introduced in 6.3.0, which causes segfault if log file creation fails.

## 6.3.0 (6/18/2019)
### Public API Change
* Now DB::Close() will return Aborted() error when there is unreleased snapshot. Users can retry after all snapshots are released.
* Index blocks are now handled similarly to data blocks with regards to the block cache: instead of storing objects in the cache, only the blocks themselves are cached. In addition, index blocks no longer get evicted from the cache when a table is closed, can now use the compressed block cache (if any), and can be shared among multiple table readers.
* Partitions of partitioned indexes no longer affect the read amplification statistics.
* Due to the above refactoring, block cache eviction statistics for indexes are temporarily broken. We plan to reintroduce them in a later phase.
* options.keep_log_file_num will be enforced strictly all the time. File names of all log files will be tracked, which may take significantly amount of memory if options.keep_log_file_num is large and either of options.max_log_file_size or options.log_file_time_to_roll is set.
* Add initial support for Get/Put with user timestamps. Users can specify timestamps via ReadOptions and WriteOptions when calling DB::Get and DB::Put.
* Accessing a partition of a partitioned filter or index through a pinned reference is no longer considered a cache hit.
* Add C bindings for secondary instance, i.e. DBImplSecondary.
* Rate limited deletion of WALs is only enabled if DBOptions::wal_dir is not set, or explicitly set to db_name passed to DB::Open and DBOptions::db_paths is empty, or same as db_paths[0].path

### New Features
* Add an option `snap_refresh_nanos` (default to 0) to periodically refresh the snapshot list in compaction jobs. Assign to 0 to disable the feature.
* Add an option `unordered_write` which trades snapshot guarantees with higher write throughput. When used with WRITE_PREPARED transactions with two_write_queues=true, it offers higher throughput with however no compromise on guarantees.
* Allow DBImplSecondary to remove memtables with obsolete data after replaying MANIFEST and WAL.
* Add an option `failed_move_fall_back_to_copy` (default is true) for external SST ingestion. When `move_files` is true and hard link fails, ingestion falls back to copy if `failed_move_fall_back_to_copy` is true. Otherwise, ingestion reports an error.
* Add command `list_file_range_deletes` in ldb, which prints out tombstones in SST files.

### Performance Improvements
* Reduce binary search when iterator reseek into the same data block.
* DBIter::Next() can skip user key checking if previous entry's seqnum is 0.
* Merging iterator to avoid child iterator reseek for some cases
* Log Writer will flush after finishing the whole record, rather than a fragment.
* Lower MultiGet batching API latency by reading data blocks from disk in parallel

### General Improvements
* Added new status code kColumnFamilyDropped to distinguish between Column Family Dropped and DB Shutdown in progress.
* Improve ColumnFamilyOptions validation when creating a new column family.

### Bug Fixes
* Fix a bug in WAL replay of secondary instance by skipping write batches with older sequence numbers than the current last sequence number.
* Fix flush's/compaction's merge processing logic which allowed `Put`s covered by range tombstones to reappear. Note `Put`s may exist even if the user only ever called `Merge()` due to an internal conversion during compaction to the bottommost level.
* Fix/improve memtable earliest sequence assignment and WAL replay so that WAL entries of unflushed column families will not be skipped after replaying the MANIFEST and increasing db sequence due to another flushed/compacted column family.
* Fix a bug caused by secondary not skipping the beginning of new MANIFEST.
* On DB open, delete WAL trash files left behind in wal_dir

## 6.2.0 (4/30/2019)
### New Features
* Add an option `strict_bytes_per_sync` that causes a file-writing thread to block rather than exceed the limit on bytes pending writeback specified by `bytes_per_sync` or `wal_bytes_per_sync`.
* Improve range scan performance by avoiding per-key upper bound check in BlockBasedTableIterator.
* Introduce Periodic Compaction for Level style compaction. Files are re-compacted periodically and put in the same level.
* Block-based table index now contains exact highest key in the file, rather than an upper bound. This may improve Get() and iterator Seek() performance in some situations, especially when direct IO is enabled and block cache is disabled. A setting BlockBasedTableOptions::index_shortening is introduced to control this behavior. Set it to kShortenSeparatorsAndSuccessor to get the old behavior.
* When reading from option file/string/map, customized envs can be filled according to object registry.
* Improve range scan performance when using explicit user readahead by not creating new table readers for every iterator.
* Add index type BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey. It significantly reduces read amplification in some setups, especially for iterator seeks. It's not fully implemented yet: IO errors are not handled right.

### Public API Change
* Change the behavior of OptimizeForPointLookup(): move away from hash-based block-based-table index, and use whole key memtable filtering.
* Change the behavior of OptimizeForSmallDb(): use a 16MB block cache, put index and filter blocks into it, and cost the memtable size to it. DBOptions.OptimizeForSmallDb() and ColumnFamilyOptions.OptimizeForSmallDb() start to take an optional cache object.
* Added BottommostLevelCompaction::kForceOptimized to avoid double compacting newly compacted files in the bottommost level compaction of manual compaction. Note this option may prohibit the manual compaction to produce a single file in the bottommost level.

### Bug Fixes
* Adjust WriteBufferManager's dummy entry size to block cache from 1MB to 256KB.
* Fix a race condition between WritePrepared::Get and ::Put with duplicate keys.
* Fix crash when memtable prefix bloom is enabled and read/write a key out of domain of prefix extractor.
* Close a WAL file before another thread deletes it.
* Fix an assertion failure `IsFlushPending() == true` caused by one bg thread releasing the db mutex in ~ColumnFamilyData and another thread clearing `flush_requested_` flag.

## 6.1.1 (4/9/2019)
### New Features
* When reading from option file/string/map, customized comparators and/or merge operators can be filled according to object registry.

### Public API Change

### Bug Fixes
* Fix a bug in 2PC where a sequence of txn prepare, memtable flush, and crash could result in losing the prepared transaction.
* Fix a bug in Encryption Env which could cause encrypted files to be read beyond file boundaries.

## 6.1.0 (3/27/2019)
### New Features
* Introduce two more stats levels, kExceptHistogramOrTimers and kExceptTimers.
* Added a feature to perform data-block sampling for compressibility, and report stats to user.
* Add support for trace filtering.
* Add DBOptions.avoid_unnecessary_blocking_io. If true, we avoid file deletion when destroying ColumnFamilyHandle and Iterator. Instead, a job is scheduled to delete the files in background.

### Public API Change
* Remove bundled fbson library.
* statistics.stats_level_ becomes atomic. It is preferred to use statistics.set_stats_level() and statistics.get_stats_level() to access it.
* Introduce a new IOError subcode, PathNotFound, to indicate trying to open a nonexistent file or directory for read.
* Add initial support for multiple db instances sharing the same data in single-writer, multi-reader mode.
* Removed some "using std::xxx" from public headers.

### Bug Fixes
* Fix JEMALLOC_CXX_THROW macro missing from older Jemalloc versions, causing build failures on some platforms.
* Fix SstFileReader not able to open file ingested with write_glbal_seqno=true.

## 6.0.0 (2/19/2019)
### New Features
* Enabled checkpoint on readonly db (DBImplReadOnly).
* Make DB ignore dropped column families while committing results of atomic flush.
* RocksDB may choose to preopen some files even if options.max_open_files != -1. This may make DB open slightly longer.
* For users of dictionary compression with ZSTD v0.7.0+, we now reuse the same digested dictionary when compressing each of an SST file's data blocks for faster compression speeds.
* For all users of dictionary compression who set `cache_index_and_filter_blocks == true`, we now store dictionary data used for decompression in the block cache for better control over memory usage. For users of ZSTD v1.1.4+ who compile with -DZSTD_STATIC_LINKING_ONLY, this includes a digested dictionary, which is used to increase decompression speed.
* Add support for block checksums verification for external SST files before ingestion.
* Introduce stats history which periodically saves Statistics snapshots and added `GetStatsHistory` API to retrieve these snapshots.
* Add a place holder in manifest which indicate a record from future that can be safely ignored.
* Add support for trace sampling.
* Enable properties block checksum verification for block-based tables.
* For all users of dictionary compression, we now generate a separate dictionary for compressing each bottom-level SST file. Previously we reused a single dictionary for a whole compaction to bottom level. The new approach achieves better compression ratios; however, it uses more memory and CPU for buffering/sampling data blocks and training dictionaries.
* Add whole key bloom filter support in memtable.
* Files written by `SstFileWriter` will now use dictionary compression if it is configured in the file writer's `CompressionOptions`.

### Public API Change
* Disallow CompactionFilter::IgnoreSnapshots() = false, because it is not very useful and the behavior is confusing. The filter will filter everything if there is no snapshot declared by the time the compaction starts. However, users can define a snapshot after the compaction starts and before it finishes and this new snapshot won't be repeatable, because after the compaction finishes, some keys may be dropped.
* CompactionPri = kMinOverlappingRatio also uses compensated file size, which boosts file with lots of tombstones to be compacted first.
* Transaction::GetForUpdate is extended with a do_validate parameter with default value of true. If false it skips validating the snapshot before doing the read. Similarly ::Merge, ::Put, ::Delete, and ::SingleDelete are extended with assume_tracked with default value of false. If true it indicates that call is assumed to be after a ::GetForUpdate.
* `TableProperties::num_entries` and `TableProperties::num_deletions` now also account for number of range tombstones.
* Remove geodb, spatial_db, document_db, json_document, date_tiered_db, and redis_lists.
* With "ldb ----try_load_options", when wal_dir specified by the option file doesn't exist, ignore it.
* Change time resolution in FileOperationInfo.
* Deleting Blob files also go through SStFileManager.
* Remove CuckooHash memtable.
* The counter stat `number.block.not_compressed` now also counts blocks not compressed due to poor compression ratio.
* Remove ttl option from `CompactionOptionsFIFO`. The option has been deprecated and ttl in `ColumnFamilyOptions` is used instead.
* Support SST file ingestion across multiple column families via DB::IngestExternalFiles. See the function's comment about atomicity.
* Remove Lua compaction filter.

### Bug Fixes
* Fix a deadlock caused by compaction and file ingestion waiting for each other in the event of write stalls.
* Fix a memory leak when files with range tombstones are read in mmap mode and block cache is enabled
* Fix handling of corrupt range tombstone blocks such that corruptions cannot cause deleted keys to reappear
* Lock free MultiGet
* Fix incorrect `NotFound` point lookup result when querying the endpoint of a file that has been extended by a range tombstone.
* Fix with pipelined write, write leaders's callback failure lead to the whole write group fail.

### Change Default Options
* Change options.compaction_pri's default to kMinOverlappingRatio

## 5.18.0 (11/30/2018)
### New Features
* Introduced `JemallocNodumpAllocator` memory allocator. When being use, block cache will be excluded from core dump.
* Introduced `PerfContextByLevel` as part of `PerfContext` which allows storing perf context at each level. Also replaced `__thread` with `thread_local` keyword for perf_context. Added per-level perf context for bloom filter and `Get` query.
* With level_compaction_dynamic_level_bytes = true, level multiplier may be adjusted automatically when Level 0 to 1 compaction is lagged behind.
* Introduced DB option `atomic_flush`. If true, RocksDB supports flushing multiple column families and atomically committing the result to MANIFEST. Useful when WAL is disabled.
* Added `num_deletions` and `num_merge_operands` members to `TableProperties`.
* Added "rocksdb.min-obsolete-sst-number-to-keep" DB property that reports the lower bound on SST file numbers that are being kept from deletion, even if the SSTs are obsolete.
* Add xxhash64 checksum support
* Introduced `MemoryAllocator`, which lets the user specify custom memory allocator for block based table.
* Improved `DeleteRange` to prevent read performance degradation. The feature is no longer marked as experimental.

### Public API Change
* `DBOptions::use_direct_reads` now affects reads issued by `BackupEngine` on the database's SSTs.
* `NO_ITERATORS` is divided into two counters `NO_ITERATOR_CREATED` and `NO_ITERATOR_DELETE`. Both of them are only increasing now, just as other counters.

### Bug Fixes
* Fix corner case where a write group leader blocked due to write stall blocks other writers in queue with WriteOptions::no_slowdown set.
* Fix in-memory range tombstone truncation to avoid erroneously covering newer keys at a lower level, and include range tombstones in compacted files whose largest key is the range tombstone's start key.
* Properly set the stop key for a truncated manual CompactRange
* Fix slow flush/compaction when DB contains many snapshots. The problem became noticeable to us in DBs with 100,000+ snapshots, though it will affect others at different thresholds.
* Fix the bug that WriteBatchWithIndex's SeekForPrev() doesn't see the entries with the same key.
* Fix the bug where user comparator was sometimes fed with InternalKey instead of the user key. The bug manifests when during GenerateBottommostFiles.
* Fix a bug in WritePrepared txns where if the number of old snapshots goes beyond the snapshot cache size (128 default) the rest will not be checked when evicting a commit entry from the commit cache.
* Fixed Get correctness bug in the presence of range tombstones where merge operands covered by a range tombstone always result in NotFound.
* Start populating `NO_FILE_CLOSES` ticker statistic, which was always zero previously.
* The default value of NewBloomFilterPolicy()'s argument use_block_based_builder is changed to false. Note that this new default may cause large temp memory usage when building very large SST files.

## 5.17.0 (10/05/2018)
### Public API Change
* `OnTableFileCreated` will now be called for empty files generated during compaction. In that case, `TableFileCreationInfo::file_path` will be "(nil)" and `TableFileCreationInfo::file_size` will be zero.
* Add `FlushOptions::allow_write_stall`, which controls whether Flush calls start working immediately, even if it causes user writes to stall, or will wait until flush can be performed without causing write stall (similar to `CompactRangeOptions::allow_write_stall`). Note that the default value is false, meaning we add delay to Flush calls until stalling can be avoided when possible. This is behavior change compared to previous RocksDB versions, where Flush calls didn't check if they might cause stall or not.
* Application using PessimisticTransactionDB is expected to rollback/commit recovered transactions before starting new ones. This assumption is used to skip concurrency control during recovery.
* Expose column family id to `OnCompactionCompleted`.

### New Features
* TransactionOptions::skip_concurrency_control allows pessimistic transactions to skip the overhead of concurrency control. Could be used for optimizing certain transactions or during recovery.

### Bug Fixes
* Avoid creating empty SSTs and subsequently deleting them in certain cases during compaction.
* Sync CURRENT file contents during checkpoint.

## 5.16.3 (10/1/2018)
### Bug Fixes
* Fix crash caused when `CompactFiles` run with `CompactionOptions::compression == CompressionType::kDisableCompressionOption`. Now that setting causes the compression type to be chosen according to the column family-wide compression options.

## 5.16.2 (9/21/2018)
### Bug Fixes
* Fix bug in partition filters with format_version=4.

## 5.16.1 (9/17/2018)
### Bug Fixes
* Remove trace_analyzer_tool from rocksdb_lib target in TARGETS file.
* Fix RocksDB Java build and tests.
* Remove sync point in Block destructor.

## 5.16.0 (8/21/2018)
### Public API Change
* The merge operands are passed to `MergeOperator::ShouldMerge` in the reversed order relative to how they were merged (passed to FullMerge or FullMergeV2) for performance reasons
* GetAllKeyVersions() to take an extra argument of `max_num_ikeys`.
* Using ZSTD dictionary trainer (i.e., setting `CompressionOptions::zstd_max_train_bytes` to a nonzero value) now requires ZSTD version 1.1.3 or later.

### New Features
* Changes the format of index blocks by delta encoding the index values, which are the block handles. This saves the encoding of BlockHandle::offset of the non-head index entries in each restart interval. The feature is backward compatible but not forward compatible. It is disabled by default unless format_version 4 or above is used.
* Add a new tool: trace_analyzer. Trace_analyzer analyzes the trace file generated by using trace_replay API. It can convert the binary format trace file to a human readable txt file, output the statistics of the analyzed query types such as access statistics and size statistics, combining the dumped whole key space file to analyze, support query correlation analyzing, and etc. Current supported query types are: Get, Put, Delete, SingleDelete, DeleteRange, Merge, Iterator (Seek, SeekForPrev only).
* Add hash index support to data blocks, which helps reducing the cpu utilization of point-lookup operations. This feature is backward compatible with the data block created without the hash index. It is disabled by default unless BlockBasedTableOptions::data_block_index_type is set to data_block_index_type = kDataBlockBinaryAndHash.

### Bug Fixes
* Fix a bug in misreporting the estimated partition index size in properties block.

## 5.15.0 (7/17/2018)
### Public API Change
* Remove managed iterator. ReadOptions.managed is not effective anymore.
* For bottommost_compression, a compatible CompressionOptions is added via `bottommost_compression_opts`. To keep backward compatible, a new boolean `enabled` is added to CompressionOptions. For compression_opts, it will be always used no matter what value of `enabled` is. For bottommost_compression_opts, it will only be used when user set `enabled=true`, otherwise, compression_opts will be used for bottommost_compression as default.
* With LRUCache, when high_pri_pool_ratio > 0, midpoint insertion strategy will be enabled to put low-pri items to the tail of low-pri list (the midpoint) when they first inserted into the cache. This is to make cache entries never get hit age out faster, improving cache efficiency when large background scan presents.
* For users of `Statistics` objects created via `CreateDBStatistics()`, the format of the string returned by its `ToString()` method has changed.
* The "rocksdb.num.entries" table property no longer counts range deletion tombstones as entries.

### New Features
* Changes the format of index blocks by storing the key in their raw form rather than converting them to InternalKey. This saves 8 bytes per index key. The feature is backward compatible but not forward compatible. It is disabled by default unless format_version 3 or above is used.
* Avoid memcpy when reading mmap files with OpenReadOnly and max_open_files==-1.
* Support dynamically changing `ColumnFamilyOptions::ttl` via `SetOptions()`.
* Add a new table property, "rocksdb.num.range-deletions", which counts the number of range deletion tombstones in the table.
* Improve the performance of iterators doing long range scans by using readahead, when using direct IO.
* pin_top_level_index_and_filter (default true) in BlockBasedTableOptions can be used in combination with cache_index_and_filter_blocks to prefetch and pin the top-level index of partitioned index and filter blocks in cache. It has no impact when cache_index_and_filter_blocks is false.
* Write properties meta-block at the end of block-based table to save read-ahead IO.

### Bug Fixes
* Fix deadlock with enable_pipelined_write=true and max_successive_merges > 0
* Check conflict at output level in CompactFiles.
* Fix corruption in non-iterator reads when mmap is used for file reads
* Fix bug with prefix search in partition filters where a shared prefix would be ignored from the later partitions. The bug could report an eixstent key as missing. The bug could be triggered if prefix_extractor is set and partition filters is enabled.
* Change default value of `bytes_max_delete_chunk` to 0 in NewSstFileManager() as it doesn't work well with checkpoints.
* Fix a bug caused by not copying the block trailer with compressed SST file, direct IO, prefetcher and no compressed block cache.
* Fix write can stuck indefinitely if enable_pipelined_write=true. The issue exists since pipelined write was introduced in 5.5.0.

## 5.14.0 (5/16/2018)
### Public API Change
* Add a BlockBasedTableOption to align uncompressed data blocks on the smaller of block size or page size boundary, to reduce flash reads by avoiding reads spanning 4K pages.
* The background thread naming convention changed (on supporting platforms) to "rocksdb:<thread pool priority><thread number>", e.g., "rocksdb:low0".
* Add a new ticker stat rocksdb.number.multiget.keys.found to count number of keys successfully read in MultiGet calls
* Touch-up to write-related counters in PerfContext. New counters added: write_scheduling_flushes_compactions_time, write_thread_wait_nanos. Counters whose behavior was fixed or modified: write_memtable_time, write_pre_and_post_process_time, write_delay_time.
* Posix Env's NewRandomRWFile() will fail if the file doesn't exist.
* Now, `DBOptions::use_direct_io_for_flush_and_compaction` only applies to background writes, and `DBOptions::use_direct_reads` applies to both user reads and background reads. This conforms with Linux's `open(2)` manpage, which advises against simultaneously reading a file in buffered and direct modes, due to possibly undefined behavior and degraded performance.
* Iterator::Valid() always returns false if !status().ok(). So, now when doing a Seek() followed by some Next()s, there's no need to check status() after every operation.
* Iterator::Seek()/SeekForPrev()/SeekToFirst()/SeekToLast() always resets status().
* Introduced `CompressionOptions::kDefaultCompressionLevel`, which is a generic way to tell RocksDB to use the compression library's default level. It is now the default value for `CompressionOptions::level`. Previously the level defaulted to -1, which gave poor compression ratios in ZSTD.

### New Features
* Introduce TTL for level compaction so that all files older than ttl go through the compaction process to get rid of old data.
* TransactionDBOptions::write_policy can be configured to enable WritePrepared 2PC transactions. Read more about them in the wiki.
* Add DB properties "rocksdb.block-cache-capacity", "rocksdb.block-cache-usage", "rocksdb.block-cache-pinned-usage" to show block cache usage.
* Add `Env::LowerThreadPoolCPUPriority(Priority)` method, which lowers the CPU priority of background (esp. compaction) threads to minimize interference with foreground tasks.
* Fsync parent directory after deleting a file in delete scheduler.
* In level-based compaction, if bottom-pri thread pool was setup via `Env::SetBackgroundThreads()`, compactions to the bottom level will be delegated to that thread pool.
* `prefix_extractor` has been moved from ImmutableCFOptions to MutableCFOptions, meaning it can be dynamically changed without a DB restart.

### Bug Fixes
* Fsync after writing global seq number to the ingestion file in ExternalSstFileIngestionJob.
* Fix WAL corruption caused by race condition between user write thread and FlushWAL when two_write_queue is not set.
* Fix `BackupableDBOptions::max_valid_backups_to_open` to not delete backup files when refcount cannot be accurately determined.
* Fix memory leak when pin_l0_filter_and_index_blocks_in_cache is used with partitioned filters
* Disable rollback of merge operands in WritePrepared transactions to work around an issue in MyRocks. It can be enabled back by setting TransactionDBOptions::rollback_merge_operands to true.
* Fix wrong results by ReverseBytewiseComparator::FindShortSuccessor()

### Java API Changes
* Add `BlockBasedTableConfig.setBlockCache` to allow sharing a block cache across DB instances.
* Added SstFileManager to the Java API to allow managing SST files across DB instances.

## 5.13.0 (3/20/2018)
### Public API Change
* RocksDBOptionsParser::Parse()'s `ignore_unknown_options` argument will only be effective if the option file shows it is generated using a higher version of RocksDB than the current version.
* Remove CompactionEventListener.

### New Features
* SstFileManager now can cancel compactions if they will result in max space errors. SstFileManager users can also use SetCompactionBufferSize to specify how much space must be leftover during a compaction for auxiliary file functions such as logging and flushing.
* Avoid unnecessarily flushing in `CompactRange()` when the range specified by the user does not overlap unflushed memtables.
* If `ColumnFamilyOptions::max_subcompactions` is set greater than one, we now parallelize large manual level-based compactions.
* Add "rocksdb.live-sst-files-size" DB property to return total bytes of all SST files belong to the latest LSM tree.
* NewSstFileManager to add an argument bytes_max_delete_chunk with default 64MB. With this argument, a file larger than 64MB will be ftruncated multiple times based on this size.

### Bug Fixes
* Fix a leak in prepared_section_completed_ where the zeroed entries would not removed from the map.
* Fix WAL corruption caused by race condition between user write thread and backup/checkpoint thread.

## 5.12.0 (2/14/2018)
### Public API Change
* Iterator::SeekForPrev is now a pure virtual method. This is to prevent user who implement the Iterator interface fail to implement SeekForPrev by mistake.
* Add `include_end` option to make the range end exclusive when `include_end == false` in `DeleteFilesInRange()`.
* Add `CompactRangeOptions::allow_write_stall`, which makes `CompactRange` start working immediately, even if it causes user writes to stall. The default value is false, meaning we add delay to `CompactRange` calls until stalling can be avoided when possible. Note this delay is not present in previous RocksDB versions.
* Creating checkpoint with empty directory now returns `Status::InvalidArgument`; previously, it returned `Status::IOError`.
* Adds a BlockBasedTableOption to turn off index block compression.
* Close() method now returns a status when closing a db.

### New Features
* Improve the performance of iterators doing long range scans by using readahead.
* Add new function `DeleteFilesInRanges()` to delete files in multiple ranges at once for better performance.
* FreeBSD build support for RocksDB and RocksJava.
* Improved performance of long range scans with readahead.
* Updated to and now continuously tested in Visual Studio 2017.

### Bug Fixes
* Fix `DisableFileDeletions()` followed by `GetSortedWalFiles()` to not return obsolete WAL files that `PurgeObsoleteFiles()` is going to delete.
* Fix Handle error return from WriteBuffer() during WAL file close and DB close.
* Fix advance reservation of arena block addresses.
* Fix handling of empty string as checkpoint directory.

## 5.11.0 (01/08/2018)
### Public API Change
* Add `autoTune` and `getBytesPerSecond()` to RocksJava RateLimiter

### New Features
* Add a new histogram stat called rocksdb.db.flush.micros for memtable flush.
* Add "--use_txn" option to use transactional API in db_stress.
* Disable onboard cache for compaction output in Windows platform.
* Improve the performance of iterators doing long range scans by using readahead.

### Bug Fixes
* Fix a stack-use-after-scope bug in ForwardIterator.
* Fix builds on platforms including Linux, Windows, and PowerPC.
* Fix buffer overrun in backup engine for DBs with huge number of files.
* Fix a mislabel bug for bottom-pri compaction threads.
* Fix DB::Flush() keep waiting after flush finish under certain condition.

## 5.10.0 (12/11/2017)
### Public API Change
* When running `make` with environment variable `USE_SSE` set and `PORTABLE` unset, will use all machine features available locally. Previously this combination only compiled SSE-related features.

### New Features
* Provide lifetime hints when writing files on Linux. This reduces hardware write-amp on storage devices supporting multiple streams.
* Add a DB stat, `NUMBER_ITER_SKIP`, which returns how many internal keys were skipped during iterations (e.g., due to being tombstones or duplicate versions of a key).
* Add PerfContext counters, `key_lock_wait_count` and `key_lock_wait_time`, which measure the number of times transactions wait on key locks and total amount of time waiting.

### Bug Fixes
* Fix IOError on WAL write doesn't propagate to write group follower
* Make iterator invalid on merge error.
* Fix performance issue in `IngestExternalFile()` affecting databases with large number of SST files.
* Fix possible corruption to LSM structure when `DeleteFilesInRange()` deletes a subset of files spanned by a `DeleteRange()` marker.

## 5.9.0 (11/1/2017)
### Public API Change
* `BackupableDBOptions::max_valid_backups_to_open == 0` now means no backups will be opened during BackupEngine initialization. Previously this condition disabled limiting backups opened.
* `DBOptions::preserve_deletes` is a new option that allows one to specify that DB should not drop tombstones for regular deletes if they have sequence number larger than what was set by the new API call `DB::SetPreserveDeletesSequenceNumber(SequenceNumber seqnum)`. Disabled by default.
* API call `DB::SetPreserveDeletesSequenceNumber(SequenceNumber seqnum)` was added, users who wish to preserve deletes are expected to periodically call this function to advance the cutoff seqnum (all deletes made before this seqnum can be dropped by DB). It's user responsibility to figure out how to advance the seqnum in the way so the tombstones are kept for the desired period of time, yet are eventually processed in time and don't eat up too much space.
* `ReadOptions::iter_start_seqnum` was added;
if set to something > 0 user will see 2 changes in iterators behavior 1) only keys written with sequence larger than this parameter would be returned and 2) the `Slice` returned by iter->key() now points to the memory that keep User-oriented representation of the internal key, rather than user key. New struct `FullKey` was added to represent internal keys, along with a new helper function `ParseFullKey(const Slice& internal_key, FullKey* result);`.
* Deprecate trash_dir param in NewSstFileManager, right now we will rename deleted files to <name>.trash instead of moving them to trash directory
* Allow setting a custom trash/DB size ratio limit in the SstFileManager, after which files that are to be scheduled for deletion are deleted immediately, regardless of any delete ratelimit.
* Return an error on write if write_options.sync = true and write_options.disableWAL = true to warn user of inconsistent options. Previously we will not write to WAL and not respecting the sync options in this case.

### New Features
* CRC32C is now using the 3-way pipelined SSE algorithm `crc32c_3way` on supported platforms to improve performance. The system will choose to use this algorithm on supported platforms automatically whenever possible. If PCLMULQDQ is not supported it will fall back to the old Fast_CRC32 algorithm.
* `DBOptions::writable_file_max_buffer_size` can now be changed dynamically.
* `DBOptions::bytes_per_sync`, `DBOptions::compaction_readahead_size`, and `DBOptions::wal_bytes_per_sync` can now be changed dynamically, `DBOptions::wal_bytes_per_sync` will flush all memtables and switch to a new WAL file.
* Support dynamic adjustment of rate limit according to demand for background I/O. It can be enabled by passing `true` to the `auto_tuned` parameter in `NewGenericRateLimiter()`. The value passed as `rate_bytes_per_sec` will still be respected as an upper-bound.
* Support dynamically changing `ColumnFamilyOptions::compaction_options_fifo`.
* Introduce `EventListener::OnStallConditionsChanged()` callback. Users can implement it to be notified when user writes are stalled, stopped, or resumed.
* Add a new db property "rocksdb.estimate-oldest-key-time" to return oldest data timestamp. The property is available only for FIFO compaction with compaction_options_fifo.allow_compaction = false.
* Upon snapshot release, recompact bottommost files containing deleted/overwritten keys that previously could not be dropped due to the snapshot. This alleviates space-amp caused by long-held snapshots.
* Support lower bound on iterators specified via `ReadOptions::iterate_lower_bound`.
* Support for differential snapshots (via iterator emitting the sequence of key-values representing the difference between DB state at two different sequence numbers). Supports preserving and emitting puts and regular deletes, doesn't support SingleDeletes, MergeOperator, Blobs and Range Deletes.

### Bug Fixes
* Fix a potential data inconsistency issue during point-in-time recovery. `DB:Open()` will abort if column family inconsistency is found during PIT recovery.
* Fix possible metadata corruption in databases using `DeleteRange()`.

## 5.8.0 (08/30/2017)
### Public API Change
* Users of `Statistics::getHistogramString()` will see fewer histogram buckets and different bucket endpoints.
* `Slice::compare` and BytewiseComparator `Compare` no longer accept `Slice`s containing nullptr.
* `Transaction::Get` and `Transaction::GetForUpdate` variants with `PinnableSlice` added.

### New Features
* Add Iterator::Refresh(), which allows users to update the iterator state so that they can avoid some initialization costs of recreating iterators.
* Replace dynamic_cast<> (except unit test) so people can choose to build with RTTI off. With make, release mode is by default built with -fno-rtti and debug mode is built without it. Users can override it by setting USE_RTTI=0 or 1.
* Universal compactions including the bottom level can be executed in a dedicated thread pool. This alleviates head-of-line blocking in the compaction queue, which cause write stalling, particularly in multi-instance use cases. Users can enable this feature via `Env::SetBackgroundThreads(N, Env::Priority::BOTTOM)`, where `N > 0`.
* Allow merge operator to be called even with a single merge operand during compactions, by appropriately overriding `MergeOperator::AllowSingleOperand`.
* Add `DB::VerifyChecksum()`, which verifies the checksums in all SST files in a running DB.
* Block-based table support for disabling checksums by setting `BlockBasedTableOptions::checksum = kNoChecksum`.

### Bug Fixes
* Fix wrong latencies in `rocksdb.db.get.micros`, `rocksdb.db.write.micros`, and `rocksdb.sst.read.micros`.
* Fix incorrect dropping of deletions during intra-L0 compaction.
* Fix transient reappearance of keys covered by range deletions when memtable prefix bloom filter is enabled.
* Fix potentially wrong file smallest key when range deletions separated by snapshot are written together.

## 5.7.0 (07/13/2017)
### Public API Change
* DB property "rocksdb.sstables" now prints keys in hex form.

### New Features
* Measure estimated number of reads per file. The information can be accessed through DB::GetColumnFamilyMetaData or "rocksdb.sstables" DB property.
* RateLimiter support for throttling background reads, or throttling the sum of background reads and writes. This can give more predictable I/O usage when compaction reads more data than it writes, e.g., due to lots of deletions.
* [Experimental] FIFO compaction with TTL support. It can be enabled by setting CompactionOptionsFIFO.ttl > 0.
* Introduce `EventListener::OnBackgroundError()` callback. Users can implement it to be notified of errors causing the DB to enter read-only mode, and optionally override them.
* Partitioned Index/Filters exiting the experimental mode. To enable partitioned indexes set index_type to kTwoLevelIndexSearch and to further enable partitioned filters set partition_filters to true. To configure the partition size set metadata_block_size.


### Bug Fixes
* Fix discarding empty compaction output files when `DeleteRange()` is used together with subcompactions.

## 5.6.0 (06/06/2017)
### Public API Change
* Scheduling flushes and compactions in the same thread pool is no longer supported by setting `max_background_flushes=0`. Instead, users can achieve this by configuring their high-pri thread pool to have zero threads.
* Replace `Options::max_background_flushes`, `Options::max_background_compactions`, and `Options::base_background_compactions` all with `Options::max_background_jobs`, which automatically decides how many threads to allocate towards flush/compaction.
* options.delayed_write_rate by default take the value of options.rate_limiter rate.
* Replace global variable `IOStatsContext iostats_context` with `IOStatsContext* get_iostats_context()`; replace global variable `PerfContext perf_context` with `PerfContext* get_perf_context()`.

### New Features
* Change ticker/histogram statistics implementations to use core-local storage. This improves aggregation speed compared to our previous thread-local approach, particularly for applications with many threads.
* Users can pass a cache object to write buffer manager, so that they can cap memory usage for memtable and block cache using one single limit.
* Flush will be triggered when 7/8 of the limit introduced by write_buffer_manager or db_write_buffer_size is triggered, so that the hard threshold is hard to hit.
* Introduce WriteOptions.low_pri. If it is true, low priority writes will be throttled if the compaction is behind.
* `DB::IngestExternalFile()` now supports ingesting files into a database containing range deletions.

### Bug Fixes
* Shouldn't ignore return value of fsync() in flush.

## 5.5.0 (05/17/2017)
### New Features
* FIFO compaction to support Intra L0 compaction too with CompactionOptionsFIFO.allow_compaction=true.
* DB::ResetStats() to reset internal stats.
* Statistics::Reset() to reset user stats.
* ldb add option --try_load_options, which will open DB with its own option file.
* Introduce WriteBatch::PopSavePoint to pop the most recent save point explicitly.
* Support dynamically change `max_open_files` option via SetDBOptions()
* Added DB::CreateColumnFamilie() and DB::DropColumnFamilies() to bulk create/drop column families.
* Add debugging function `GetAllKeyVersions` to see internal versions of a range of keys.
* Support file ingestion with universal compaction style
* Support file ingestion behind with option `allow_ingest_behind`
* New option enable_pipelined_write which may improve write throughput in case writing from multiple threads and WAL enabled.

### Bug Fixes
* Fix the bug that Direct I/O uses direct reads for non-SST file

## 5.4.0 (04/11/2017)
### Public API Change
* random_access_max_buffer_size no longer has any effect
* Removed Env::EnableReadAhead(), Env::ShouldForwardRawRequest()
* Support dynamically change `stats_dump_period_sec` option via SetDBOptions().
* Added ReadOptions::max_skippable_internal_keys to set a threshold to fail a request as incomplete when too many keys are being skipped when using iterators.
* DB::Get in place of std::string accepts PinnableSlice, which avoids the extra memcpy of value to std::string in most of cases.
    * PinnableSlice releases the pinned resources that contain the value when it is destructed or when ::Reset() is called on it.
    * The old API that accepts std::string, although discouraged, is still supported.
* Replace Options::use_direct_writes with Options::use_direct_io_for_flush_and_compaction. Read Direct IO wiki for details.
* Added CompactionEventListener and EventListener::OnFlushBegin interfaces.

### New Features
* Memtable flush can be avoided during checkpoint creation if total log file size is smaller than a threshold specified by the user.
* Introduce level-based L0->L0 compactions to reduce file count, so write delays are incurred less often.
* (Experimental) Partitioning filters which creates an index on the partitions. The feature can be enabled by setting partition_filters when using kFullFilter. Currently the feature also requires two-level indexing to be enabled. Number of partitions is the same as the number of partitions for indexes, which is controlled by metadata_block_size.

## 5.3.0 (03/08/2017)
### Public API Change
* Remove disableDataSync option.
* Remove timeout_hint_us option from WriteOptions. The option has been deprecated and has no effect since 3.13.0.
* Remove option min_partial_merge_operands. Partial merge operands will always be merged in flush or compaction if there are more than one.
* Remove option verify_checksums_in_compaction. Compaction will always verify checksum.

### Bug Fixes
* Fix the bug that iterator may skip keys

## 5.2.0 (02/08/2017)
### Public API Change
* NewLRUCache() will determine number of shard bits automatically based on capacity, if the user doesn't pass one. This also impacts the default block cache when the user doesn't explicit provide one.
* Change the default of delayed slowdown value to 16MB/s and further increase the L0 stop condition to 36 files.
* Options::use_direct_writes and Options::use_direct_reads are now ready to use.
* (Experimental) Two-level indexing that partition the index and creates a 2nd level index on the partitions. The feature can be enabled by setting kTwoLevelIndexSearch as IndexType and configuring index_per_partition.

### New Features
* Added new overloaded function GetApproximateSizes that allows to specify if memtable stats should be computed only without computing SST files' stats approximations.
* Added new function GetApproximateMemTableStats that approximates both number of records and size of memtables.
* Add Direct I/O mode for SST file I/O

### Bug Fixes
* RangeSync() should work if ROCKSDB_FALLOCATE_PRESENT is not set
* Fix wrong results in a data race case in Get()
* Some fixes related to 2PC.
* Fix bugs of data corruption in direct I/O

## 5.1.0 (01/13/2017)
* Support dynamically change `delete_obsolete_files_period_micros` option via SetDBOptions().
* Added EventListener::OnExternalFileIngested which will be called when IngestExternalFile() add a file successfully.
* BackupEngine::Open and BackupEngineReadOnly::Open now always return error statuses matching those of the backup Env.

### Bug Fixes
* Fix the bug that if 2PC is enabled, checkpoints may loss some recent transactions.
* When file copying is needed when creating checkpoints or bulk loading files, fsync the file after the file copying.

## 5.0.0 (11/17/2016)
### Public API Change
* Options::max_bytes_for_level_multiplier is now a double along with all getters and setters.
* Support dynamically change `delayed_write_rate` and `max_total_wal_size` options via SetDBOptions().
* Introduce DB::DeleteRange for optimized deletion of large ranges of contiguous keys.
* Support dynamically change `delayed_write_rate` option via SetDBOptions().
* Options::allow_concurrent_memtable_write and Options::enable_write_thread_adaptive_yield are now true by default.
* Remove Tickers::SEQUENCE_NUMBER to avoid confusion if statistics object is shared among RocksDB instance. Alternatively DB::GetLatestSequenceNumber() can be used to get the same value.
* Options.level0_stop_writes_trigger default value changes from 24 to 32.
* New compaction filter API: CompactionFilter::FilterV2(). Allows to drop ranges of keys.
* Removed flashcache support.
* DB::AddFile() is deprecated and is replaced with DB::IngestExternalFile(). DB::IngestExternalFile() remove all the restrictions that existed for DB::AddFile.

### New Features
* Add avoid_flush_during_shutdown option, which speeds up DB shutdown by not flushing unpersisted data (i.e. with disableWAL = true). Unpersisted data will be lost. The options is dynamically changeable via SetDBOptions().
* Add memtable_insert_with_hint_prefix_extractor option. The option is mean to reduce CPU usage for inserting keys into memtable, if keys can be group by prefix and insert for each prefix are sequential or almost sequential. See include/rocksdb/options.h for more details.
* Add LuaCompactionFilter in utilities.  This allows developers to write compaction filters in Lua.  To use this feature, LUA_PATH needs to be set to the root directory of Lua.
* No longer populate "LATEST_BACKUP" file in backup directory, which formerly contained the number of the latest backup. The latest backup can be determined by finding the highest numbered file in the "meta/" subdirectory.

## 4.13.0 (10/18/2016)
### Public API Change
* DB::GetOptions() reflect dynamic changed options (i.e. through DB::SetOptions()) and return copy of options instead of reference.
* Added Statistics::getAndResetTickerCount().

### New Features
* Add DB::SetDBOptions() to dynamic change base_background_compactions and max_background_compactions.
* Added Iterator::SeekForPrev(). This new API will seek to the last key that less than or equal to the target key.

## 4.12.0 (9/12/2016)
### Public API Change
* CancelAllBackgroundWork() flushes all memtables for databases containing writes that have bypassed the WAL (writes issued with WriteOptions::disableWAL=true) before shutting down background threads.
* Merge options source_compaction_factor, max_grandparent_overlap_bytes and expanded_compaction_factor into max_compaction_bytes.
* Remove ImmutableCFOptions.
* Add a compression type ZSTD, which can work with ZSTD 0.8.0 or up. Still keep ZSTDNotFinal for compatibility reasons.

### New Features
* Introduce NewClockCache, which is based on CLOCK algorithm with better concurrent performance in some cases. It can be used to replace the default LRU-based block cache and table cache. To use it, RocksDB need to be linked with TBB lib.
* Change ticker/histogram statistics implementations to accumulate data in thread-local storage, which improves CPU performance by reducing cache coherency costs. Callers of CreateDBStatistics do not need to change anything to use this feature.
* Block cache mid-point insertion, where index and filter block are inserted into LRU block cache with higher priority. The feature can be enabled by setting BlockBasedTableOptions::cache_index_and_filter_blocks_with_high_priority to true and high_pri_pool_ratio > 0 when creating NewLRUCache.

## 4.11.0 (8/1/2016)
### Public API Change
* options.memtable_prefix_bloom_huge_page_tlb_size => memtable_huge_page_size. When it is set, RocksDB will try to allocate memory from huge page for memtable too, rather than just memtable bloom filter.

### New Features
* A tool to migrate DB after options change. See include/rocksdb/utilities/option_change_migration.h.
* Add ReadOptions.background_purge_on_iterator_cleanup. If true, we avoid file deletion when destroying iterators.

## 4.10.0 (7/5/2016)
### Public API Change
* options.memtable_prefix_bloom_bits changes to options.memtable_prefix_bloom_bits_ratio and deprecate options.memtable_prefix_bloom_probes
* enum type CompressionType and PerfLevel changes from char to unsigned char. Value of all PerfLevel shift by one.
* Deprecate options.filter_deletes.

### New Features
* Add avoid_flush_during_recovery option.
* Add a read option background_purge_on_iterator_cleanup to avoid deleting files in foreground when destroying iterators. Instead, a job is scheduled in high priority queue and would be executed in a separate background thread.
* RepairDB support for column families. RepairDB now associates data with non-default column families using information embedded in the SST/WAL files (4.7 or later). For data written by 4.6 or earlier, RepairDB associates it with the default column family.
* Add options.write_buffer_manager which allows users to control total memtable sizes across multiple DB instances.

## 4.9.0 (6/9/2016)
### Public API changes
* Add bottommost_compression option, This option can be used to set a specific compression algorithm for the bottommost level (Last level containing files in the DB).
* Introduce CompactionJobInfo::compression, This field state the compression algorithm used to generate the output files of the compaction.
* Deprecate BlockBaseTableOptions.hash_index_allow_collision=false
* Deprecate options builder (GetOptions()).

### New Features
* Introduce NewSimCache() in rocksdb/utilities/sim_cache.h. This function creates a block cache that is able to give simulation results (mainly hit rate) of simulating block behavior with a configurable cache size.

## 4.8.0 (5/2/2016)
### Public API Change
* Allow preset compression dictionary for improved compression of block-based tables. This is supported for zlib, zstd, and lz4. The compression dictionary's size is configurable via CompressionOptions::max_dict_bytes.
* Delete deprecated classes for creating backups (BackupableDB) and restoring from backups (RestoreBackupableDB). Now, BackupEngine should be used for creating backups, and BackupEngineReadOnly should be used for restorations. For more details, see https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB%3F
* Expose estimate of per-level compression ratio via DB property: "rocksdb.compression-ratio-at-levelN".
* Added EventListener::OnTableFileCreationStarted. EventListener::OnTableFileCreated will be called on failure case. User can check creation status via TableFileCreationInfo::status.

### New Features
* Add ReadOptions::readahead_size. If non-zero, NewIterator will create a new table reader which performs reads of the given size.

## 4.7.0 (4/8/2016)
### Public API Change
* rename options compaction_measure_io_stats to report_bg_io_stats and include flush too.
* Change some default options. Now default options will optimize for server-workloads. Also enable slowdown and full stop triggers for pending compaction bytes. These changes may cause sub-optimal performance or significant increase of resource usage. To avoid these risks, users can open existing RocksDB with options extracted from RocksDB option files. See https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File for how to use RocksDB option files. Or you can call Options.OldDefaults() to recover old defaults. DEFAULT_OPTIONS_HISTORY.md will track change history of default options.

## 4.6.0 (3/10/2016)
### Public API Changes
* Change default of BlockBasedTableOptions.format_version to 2. It means default DB created by 4.6 or up cannot be opened by RocksDB version 3.9 or earlier.
* Added strict_capacity_limit option to NewLRUCache. If the flag is set to true, insert to cache will fail if no enough capacity can be free. Signature of Cache::Insert() is updated accordingly.
* Tickers [NUMBER_DB_NEXT, NUMBER_DB_PREV, NUMBER_DB_NEXT_FOUND, NUMBER_DB_PREV_FOUND, ITER_BYTES_READ] are not updated immediately. The are updated when the Iterator is deleted.
* Add monotonically increasing counter (DB property "rocksdb.current-super-version-number") that increments upon any change to the LSM tree.

### New Features
* Add CompactionPri::kMinOverlappingRatio, a compaction picking mode friendly to write amplification.
* Deprecate Iterator::IsKeyPinned() and replace it with Iterator::GetProperty() with prop_name="rocksdb.iterator.is.key.pinned"

## 4.5.0 (2/5/2016)
### Public API Changes
* Add a new perf context level between kEnableCount and kEnableTime. Level 2 now does not include timers for mutexes.
* Statistics of mutex operation durations will not be measured by default. If you want to have them enabled, you need to set Statistics::stats_level_ to kAll.
* DBOptions::delete_scheduler and NewDeleteScheduler() are removed, please use DBOptions::sst_file_manager and NewSstFileManager() instead

### New Features
* ldb tool now supports operations to non-default column families.
* Add kPersistedTier to ReadTier.  This option allows Get and MultiGet to read only the persited data and skip mem-tables if writes were done with disableWAL = true.
* Add DBOptions::sst_file_manager. Use NewSstFileManager() in include/rocksdb/sst_file_manager.h to create a SstFileManager that can be used to track the total size of SST files and control the SST files deletion rate.

## 4.4.0 (1/14/2016)
### Public API Changes
* Change names in CompactionPri and add a new one.
* Deprecate options.soft_rate_limit and add options.soft_pending_compaction_bytes_limit.
* If options.max_write_buffer_number > 3, writes will be slowed down when writing to the last write buffer to delay a full stop.
* Introduce CompactionJobInfo::compaction_reason, this field include the reason to trigger the compaction.
* After slow down is triggered, if estimated pending compaction bytes keep increasing, slowdown more.
* Increase default options.delayed_write_rate to 2MB/s.
* Added a new parameter --path to ldb tool. --path accepts the name of either MANIFEST, SST or a WAL file. Either --db or --path can be used when calling ldb.

## 4.3.0 (12/8/2015)
### New Features
* CompactionFilter has new member function called IgnoreSnapshots which allows CompactionFilter to be called even if there are snapshots later than the key.
* RocksDB will now persist options under the same directory as the RocksDB database on successful DB::Open, CreateColumnFamily, DropColumnFamily, and SetOptions.
* Introduce LoadLatestOptions() in rocksdb/utilities/options_util.h.  This function can construct the latest DBOptions / ColumnFamilyOptions used by the specified RocksDB intance.
* Introduce CheckOptionsCompatibility() in rocksdb/utilities/options_util.h.  This function checks whether the input set of options is able to open the specified DB successfully.

### Public API Changes
* When options.db_write_buffer_size triggers, only the column family with the largest column family size will be flushed, not all the column families.

## 4.2.0 (11/9/2015)
### New Features
* Introduce CreateLoggerFromOptions(), this function create a Logger for provided DBOptions.
* Add GetAggregatedIntProperty(), which returns the sum of the GetIntProperty of all the column families.
* Add MemoryUtil in rocksdb/utilities/memory.h.  It currently offers a way to get the memory usage by type from a list rocksdb instances.

### Public API Changes
* CompactionFilter::Context includes information of Column Family ID
* The need-compaction hint given by TablePropertiesCollector::NeedCompact() will be persistent and recoverable after DB recovery. This introduces a breaking format change. If you use this experimental feature, including NewCompactOnDeletionCollectorFactory() in the new version, you may not be able to directly downgrade the DB back to version 4.0 or lower.
* TablePropertiesCollectorFactory::CreateTablePropertiesCollector() now takes an option Context, containing the information of column family ID for the file being written.
* Remove DefaultCompactionFilterFactory.


## 4.1.0 (10/8/2015)
### New Features
* Added single delete operation as a more efficient way to delete keys that have not been overwritten.
* Added experimental AddFile() to DB interface that allow users to add files created by SstFileWriter into an empty Database, see include/rocksdb/sst_file_writer.h and DB::AddFile() for more info.
* Added support for opening SST files with .ldb suffix which enables opening LevelDB databases.
* CompactionFilter now supports filtering of merge operands and merge results.

### Public API Changes
* Added SingleDelete() to the DB interface.
* Added AddFile() to DB interface.
* Added SstFileWriter class.
* CompactionFilter has a new method FilterMergeOperand() that RocksDB applies to every merge operand during compaction to decide whether to filter the operand.
* We removed CompactionFilterV2 interfaces from include/rocksdb/compaction_filter.h. The functionality was deprecated already in version 3.13.

## 4.0.0 (9/9/2015)
### New Features
* Added support for transactions.  See include/rocksdb/utilities/transaction.h for more info.
* DB::GetProperty() now accepts "rocksdb.aggregated-table-properties" and "rocksdb.aggregated-table-properties-at-levelN", in which case it returns aggregated table properties of the target column family, or the aggregated table properties of the specified level N if the "at-level" version is used.
* Add compression option kZSTDNotFinalCompression for people to experiment ZSTD although its format is not finalized.
* We removed the need for LATEST_BACKUP file in BackupEngine. We still keep writing it when we create new backups (because of backward compatibility), but we don't read it anymore.

### Public API Changes
* Removed class Env::RandomRWFile and Env::NewRandomRWFile().
* Renamed DBOptions.num_subcompactions to DBOptions.max_subcompactions to make the name better match the actual functionality of the option.
* Added Equal() method to the Comparator interface that can optionally be overwritten in cases where equality comparisons can be done more efficiently than three-way comparisons.
* Previous 'experimental' OptimisticTransaction class has been replaced by Transaction class.

## 3.13.0 (8/6/2015)
### New Features
* RollbackToSavePoint() in WriteBatch/WriteBatchWithIndex
* Add NewCompactOnDeletionCollectorFactory() in utilities/table_properties_collectors, which allows rocksdb to mark a SST file as need-compaction when it observes at least D deletion entries in any N consecutive entries in that SST file.  Note that this feature depends on an experimental NeedCompact() API --- the result of this API will not persist after DB restart.
* Add DBOptions::delete_scheduler. Use NewDeleteScheduler() in include/rocksdb/delete_scheduler.h to create a DeleteScheduler that can be shared among multiple RocksDB instances to control the file deletion rate of SST files that exist in the first db_path.

### Public API Changes
* Deprecated WriteOptions::timeout_hint_us. We no longer support write timeout. If you really need this option, talk to us and we might consider returning it.
* Deprecated purge_redundant_kvs_while_flush option.
* Removed BackupEngine::NewBackupEngine() and NewReadOnlyBackupEngine() that were deprecated in RocksDB 3.8. Please use BackupEngine::Open() instead.
* Deprecated Compaction Filter V2. We are not aware of any existing use-cases. If you use this filter, your compile will break with RocksDB 3.13. Please let us know if you use it and we'll put it back in RocksDB 3.14.
* Env::FileExists now returns a Status instead of a boolean
* Add statistics::getHistogramString() to print detailed distribution of a histogram metric.
* Add DBOptions::skip_stats_update_on_db_open.  When it is on, DB::Open() will run faster as it skips the random reads required for loading necessary stats from SST files to optimize compaction.

## 3.12.0 (7/2/2015)
### New Features
* Added experimental support for optimistic transactions.  See include/rocksdb/utilities/optimistic_transaction.h for more info.
* Added a new way to report QPS from db_bench (check out --report_file and --report_interval_seconds)
* Added a cache for individual rows. See DBOptions::row_cache for more info.
* Several new features on EventListener (see include/rocksdb/listener.h):
 - OnCompationCompleted() now returns per-compaction job statistics, defined in include/rocksdb/compaction_job_stats.h.
 - Added OnTableFileCreated() and OnTableFileDeleted().
* Add compaction_options_universal.enable_trivial_move to true, to allow trivial move while performing universal compaction. Trivial move will happen only when all the input files are non overlapping.

### Public API changes
* EventListener::OnFlushCompleted() now passes FlushJobInfo instead of a list of parameters.
* DB::GetDbIdentity() is now a const function.  If this function is overridden in your application, be sure to also make GetDbIdentity() const to avoid compile error.
* Move listeners from ColumnFamilyOptions to DBOptions.
* Add max_write_buffer_number_to_maintain option
* DB::CompactRange()'s parameter reduce_level is changed to change_level, to allow users to move levels to lower levels if allowed. It can be used to migrate a DB from options.level_compaction_dynamic_level_bytes=false to options.level_compaction_dynamic_level_bytes.true.
* Change default value for options.compaction_filter_factory and options.compaction_filter_factory_v2 to nullptr instead of DefaultCompactionFilterFactory and DefaultCompactionFilterFactoryV2.
* If CancelAllBackgroundWork is called without doing a flush after doing loads with WAL disabled, the changes which haven't been flushed before the call to CancelAllBackgroundWork will be lost.
* WBWIIterator::Entry() now returns WriteEntry instead of `const WriteEntry&`
* options.hard_rate_limit is deprecated.
* When options.soft_rate_limit or options.level0_slowdown_writes_trigger is triggered, the way to slow down writes is changed to: write rate to DB is limited to to options.delayed_write_rate.
* DB::GetApproximateSizes() adds a parameter to allow the estimation to include data in mem table, with default to be not to include. It is now only supported in skip list mem table.
* DB::CompactRange() now accept CompactRangeOptions instead of multiple parameters. CompactRangeOptions is defined in include/rocksdb/options.h.
* CompactRange() will now skip bottommost level compaction for level based compaction if there is no compaction filter, bottommost_level_compaction is introduced in CompactRangeOptions to control when it's possible to skip bottommost level compaction. This mean that if you want the compaction to produce a single file you need to set bottommost_level_compaction to BottommostLevelCompaction::kForce.
* Add Cache.GetPinnedUsage() to get the size of memory occupied by entries that are in use by the system.
* DB:Open() will fail if the compression specified in Options is not linked with the binary. If you see this failure, recompile RocksDB with compression libraries present on your system. Also, previously our default compression was snappy. This behavior is now changed. Now, the default compression is snappy only if it's available on the system. If it isn't we change the default to kNoCompression.
* We changed how we account for memory used in block cache. Previously, we only counted the sum of block sizes currently present in block cache. Now, we count the actual memory usage of the blocks. For example, a block of size 4.5KB will use 8KB memory with jemalloc. This might decrease your memory usage and possibly decrease performance. Increase block cache size if you see this happening after an upgrade.
* Add BackupEngineImpl.options_.max_background_operations to specify the maximum number of operations that may be performed in parallel. Add support for parallelized backup and restore.
* Add DB::SyncWAL() that does a WAL sync without blocking writers.

## 3.11.0 (5/19/2015)
### New Features
* Added a new API Cache::SetCapacity(size_t capacity) to dynamically change the maximum configured capacity of the cache. If the new capacity is less than the existing cache usage, the implementation will try to lower the usage by evicting the necessary number of elements following a strict LRU policy.
* Added an experimental API for handling flashcache devices (blacklists background threads from caching their reads) -- NewFlashcacheAwareEnv
* If universal compaction is used and options.num_levels > 1, compact files are tried to be stored in none-L0 with smaller files based on options.target_file_size_base. The limitation of DB size when using universal compaction is greatly mitigated by using more levels. You can set num_levels = 1 to make universal compaction behave as before. If you set num_levels > 1 and want to roll back to a previous version, you need to compact all files to a big file in level 0 (by setting target_file_size_base to be large and CompactRange(<cf_handle>, nullptr, nullptr, true, 0) and reopen the DB with the same version to rewrite the manifest, and then you can open it using previous releases.
* More information about rocksdb background threads are available in Env::GetThreadList(), including the number of bytes read / written by a compaction job, mem-table size and current number of bytes written by a flush job and many more.  Check include/rocksdb/thread_status.h for more detail.

### Public API changes
* TablePropertiesCollector::AddUserKey() is added to replace TablePropertiesCollector::Add(). AddUserKey() exposes key type, sequence number and file size up to now to users.
* DBOptions::bytes_per_sync used to apply to both WAL and table files. As of 3.11 it applies only to table files. If you want to use this option to sync WAL in the background, please use wal_bytes_per_sync

## 3.10.0 (3/24/2015)
### New Features
* GetThreadStatus() is now able to report detailed thread status, including:
 - Thread Operation including flush and compaction.
 - The stage of the current thread operation.
 - The elapsed time in micros since the current thread operation started.
 More information can be found in include/rocksdb/thread_status.h.  In addition, when running db_bench with --thread_status_per_interval, db_bench will also report thread status periodically.
* Changed the LRU caching algorithm so that referenced blocks (by iterators) are never evicted. This change made parameter removeScanCountLimit obsolete. Because of that NewLRUCache doesn't take three arguments anymore. table_cache_remove_scan_limit option is also removed
* By default we now optimize the compilation for the compilation platform (using -march=native). If you want to build portable binary, use 'PORTABLE=1' before the make command.
* We now allow level-compaction to place files in different paths by
  specifying them in db_paths along with the target_size.
  Lower numbered levels will be placed earlier in the db_paths and higher
  numbered levels will be placed later in the db_paths vector.
* Potentially big performance improvements if you're using RocksDB with lots of column families (100-1000)
* Added BlockBasedTableOptions.format_version option, which allows user to specify which version of block based table he wants. As a general guideline, newer versions have more features, but might not be readable by older versions of RocksDB.
* Added new block based table format (version 2), which you can enable by setting BlockBasedTableOptions.format_version = 2. This format changes how we encode size information in compressed blocks and should help with memory allocations if you're using Zlib or BZip2 compressions.
* MemEnv (env that stores data in memory) is now available in default library build. You can create it by calling NewMemEnv().
* Add SliceTransform.SameResultWhenAppended() to help users determine it is safe to apply prefix bloom/hash.
* Block based table now makes use of prefix bloom filter if it is a full fulter.
* Block based table remembers whether a whole key or prefix based bloom filter is supported in SST files. Do a sanity check when reading the file with users' configuration.
* Fixed a bug in ReadOnlyBackupEngine that deleted corrupted backups in some cases, even though the engine was ReadOnly
* options.level_compaction_dynamic_level_bytes, a feature to allow RocksDB to pick dynamic base of bytes for levels. With this feature turned on, we will automatically adjust max bytes for each level. The goal of this feature is to have lower bound on size amplification. For more details, see comments in options.h.
* Added an abstract base class WriteBatchBase for write batches
* Fixed a bug where we start deleting files of a dropped column families even if there are still live references to it

### Public API changes
* Deprecated skip_log_error_on_recovery and table_cache_remove_scan_count_limit options.
* Logger method logv with log level parameter is now virtual

### RocksJava
* Added compression per level API.
* MemEnv is now available in RocksJava via RocksMemEnv class.
* lz4 compression is now included in rocksjava static library when running `make rocksdbjavastatic`.
* Overflowing a size_t when setting rocksdb options now throws an IllegalArgumentException, which removes the necessity for a developer to catch these Exceptions explicitly.

## 3.9.0 (12/8/2014)

### New Features
* Add rocksdb::GetThreadList(), which in the future will return the current status of all
  rocksdb-related threads.  We will have more code instruments in the following RocksDB
  releases.
* Change convert function in rocksdb/utilities/convenience.h to return Status instead of boolean.
  Also add support for nested options in convert function

### Public API changes
* New API to create a checkpoint added. Given a directory name, creates a new
  database which is an image of the existing database.
* New API LinkFile added to Env. If you implement your own Env class, an
  implementation of the API LinkFile will have to be provided.
* MemTableRep takes MemTableAllocator instead of Arena

### Improvements
* RocksDBLite library now becomes smaller and will be compiled with -fno-exceptions flag.

## 3.8.0 (11/14/2014)

### Public API changes
* BackupEngine::NewBackupEngine() was deprecated; please use BackupEngine::Open() from now on.
* BackupableDB/RestoreBackupableDB have new GarbageCollect() methods, which will clean up files from corrupt and obsolete backups.
* BackupableDB/RestoreBackupableDB have new GetCorruptedBackups() methods which list corrupt backups.

### Cleanup
* Bunch of code cleanup, some extra warnings turned on (-Wshadow, -Wshorten-64-to-32, -Wnon-virtual-dtor)

### New features
* CompactFiles and EventListener, although they are still in experimental state
* Full ColumnFamily support in RocksJava.

## 3.7.0 (11/6/2014)
### Public API changes
* Introduce SetOptions() API to allow adjusting a subset of options dynamically online
* Introduce 4 new convenient functions for converting Options from string: GetColumnFamilyOptionsFromMap(), GetColumnFamilyOptionsFromString(), GetDBOptionsFromMap(), GetDBOptionsFromString()
* Remove WriteBatchWithIndex.Delete() overloads using SliceParts
* When opening a DB, if options.max_background_compactions is larger than the existing low pri pool of options.env, it will enlarge it. Similarly, options.max_background_flushes is larger than the existing high pri pool of options.env, it will enlarge it.

## 3.6.0 (10/7/2014)
### Disk format changes
* If you're using RocksDB on ARM platforms and you're using default bloom filter, there is a disk format change you need to be aware of. There are three steps you need to do when you convert to new release: 1. turn off filter policy, 2. compact the whole database, 3. turn on filter policy

### Behavior changes
* We have refactored our system of stalling writes.  Any stall-related statistics' meanings are changed. Instead of per-write stall counts, we now count stalls per-epoch, where epochs are periods between flushes and compactions. You'll find more information in our Tuning Perf Guide once we release RocksDB 3.6.
* When disableDataSync=true, we no longer sync the MANIFEST file.
* Add identity_as_first_hash property to CuckooTable. SST file needs to be rebuilt to be opened by reader properly.

### Public API changes
* Change target_file_size_base type to uint64_t from int.
* Remove allow_thread_local. This feature was proved to be stable, so we are turning it always-on.

## 3.5.0 (9/3/2014)
### New Features
* Add include/utilities/write_batch_with_index.h, providing a utility class to query data out of WriteBatch when building it.
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
* Added iterate_upper_bound to define the extent upto which the forward iterator will return entries. This will prevent iterating over delete markers and overwritten entries for edge cases where you want to break out the iterator anyways. This may improve performance in case there are a large number of delete markers or overwritten entries.

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
* RocksDB is now able to reclaim storage space more effectively during the compaction process.  This is done by compensating the size of each deletion entry by the 2X average value size, which makes compaction to be triggered by deletion entries more easily.
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
* Add two parameters to NewHashLinkListRepFactory() for logging on too many entries in a hash bucket when flushing.
* Added new option BlockBasedTableOptions::hash_index_allow_collision. When enabled, prefix hash index for block-based table will not store prefix and allow hash collision, reducing memory consumption.

### New Features
* PlainTable now supports a new key encoding: for keys of the same prefix, the prefix is only written once. It can be enabled through encoding_type parameter of NewPlainTableFactory()
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
