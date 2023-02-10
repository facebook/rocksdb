# Rocksdb Change Log
## 7.10.2 (02/10/2023)
### Bug Fixes
* Fixed a bug in DB open/recovery from a compressed WAL that was caused due to incorrect handling of certain record fragments with the same offset within a WAL block.

## 7.10.1 (02/01/2023)
### Bug Fixes
* Fixed a data race on `ColumnFamilyData::flush_reason` caused by concurrent flushes.
* Fixed `DisableManualCompaction()` and `CompactRangeOptions::canceled` to cancel compactions even when they are waiting on conflicting compactions to finish
* Fixed a bug in which a successful `GetMergeOperands()` could transiently return `Status::MergeInProgress()`
* Return the correct error (Status::NotSupported()) to MultiGet caller when ReadOptions::async_io flag is true and IO uring is not enabled. Previously, Status::Corruption() was being returned when the actual failure was lack of async IO support.

## 7.10.0 (01/23/2023)
### Behavior changes
* Make best-efforts recovery verify SST unique ID before Version construction (#10962)
* Introduce `epoch_number` and sort L0 files by `epoch_number` instead of `largest_seqno`. `epoch_number` represents the order of a file being flushed or ingested/imported. Compaction output file will be assigned with the minimum `epoch_number` among input files'. For L0, larger `epoch_number` indicates newer L0 file.

### Bug Fixes
* Fixed a regression in iterator where range tombstones after `iterate_upper_bound` is processed.
* Fixed a memory leak in MultiGet with async_io read option, caused by IO errors during table file open
* Fixed a bug that multi-level FIFO compaction deletes one file in non-L0 even when `CompactionOptionsFIFO::max_table_files_size` is no exceeded since #10348 or 7.8.0.
* Fixed a bug caused by `DB::SyncWAL()` affecting `track_and_verify_wals_in_manifest`. Without the fix, application may see "open error: Corruption: Missing WAL with log number" while trying to open the db. The corruption is a false alarm but prevents DB open (#10892).
* Fixed a BackupEngine bug in which RestoreDBFromLatestBackup would fail if the latest backup was deleted and there is another valid backup available.
* Fix L0 file misorder corruption caused by ingesting files of overlapping seqnos with memtable entries' through introducing `epoch_number`. Before the fix, `force_consistency_checks=true` may catch the corruption before it's exposed to readers, in which case writes returning `Status::Corruption` would be expected. Also replace the previous incomplete fix (#5958) to the same corruption with this new and more complete fix.
* Fixed a bug in LockWAL() leading to re-locking mutex (#11020).
* Fixed a heap use after free bug in async scan prefetching when the scan thread and another thread try to read and load the same seek block into cache.
* Fixed a heap use after free in async scan prefetching if dictionary compression is enabled, in which case sync read of the compression dictionary gets mixed with async prefetching
* Fixed a data race bug of `CompactRange()` under `change_level=true` acts on overlapping range with an ongoing file ingestion for level compaction. This will either result in overlapping file ranges corruption at a certain level caught by `force_consistency_checks=true` or protentially two same keys both with seqno 0 in two different levels (i.e, new data ends up in lower/older level). The latter will be caught by assertion in debug build but go silently and result in read returning wrong result in release build. This fix is general so it also replaced previous fixes to a similar problem for `CompactFiles()` (#4665), general `CompactRange()` and auto compaction (commit 5c64fb6 and 87dfc1d).
* Fixed a bug in compaction output cutting where small output files were produced due to TTL file cutting states were not being updated (#11075).

### New Features
* When an SstPartitionerFactory is configured, CompactRange() now automatically selects for compaction any files overlapping a partition boundary that is in the compaction range, even if no actual entries are in the requested compaction range. With this feature, manual compaction can be used to (re-)establish SST partition points when SstPartitioner changes, without a full compaction.
* Add BackupEngine feature to exclude files from backup that are known to be backed up elsewhere, using `CreateBackupOptions::exclude_files_callback`. To restore the DB, the excluded files must be provided in alternative backup directories using `RestoreOptions::alternate_dirs`.

### Public API Changes
* Substantial changes have been made to the Cache class to support internal development goals. Direct use of Cache class members is discouraged and further breaking modifications are expected in the future. SecondaryCache has some related changes and implementations will need to be updated. (Unlike Cache, SecondaryCache is still intended to support user implementations, and disruptive changes will be avoided.) (#10975)
* Add `MergeOperationOutput::op_failure_scope` for merge operator users to control the blast radius of merge operator failures. Existing merge operator users do not need to make any change to preserve the old behavior

### Performance Improvements
* Updated xxHash source code, which should improve kXXH3 checksum speed, at least on ARM (#11098).
* Improved CPU efficiency of DB reads, from block cache access improvements (#10975).

## 7.9.0 (11/21/2022)
### Performance Improvements
* Fixed an iterator performance regression for delete range users when scanning through a consecutive sequence of range tombstones (#10877).

### Bug Fixes
* Fix memory corruption error in scans if async_io is enabled. Memory corruption happened if there is IOError while reading the data leading to empty buffer and other buffer already in progress of async read goes again for reading.
* Fix failed memtable flush retry bug that could cause wrongly ordered updates, which would surface to writers as `Status::Corruption` in case of `force_consistency_checks=true` (default). It affects use cases that enable both parallel flush (`max_background_flushes > 1` or `max_background_jobs >= 8`) and non-default memtable count (`max_write_buffer_number > 2`).
* Fixed an issue where the `READ_NUM_MERGE_OPERANDS` ticker was not updated when the base key-value or tombstone was read from an SST file.
* Fixed a memory safety bug when using a SecondaryCache with `block_cache_compressed`. `block_cache_compressed` no longer attempts to use SecondaryCache features.
* Fixed a regression in scan for async_io. During seek, valid buffers were getting cleared causing a regression.
* Tiered Storage: fixed excessive keys written to penultimate level in non-debug builds.

### New Features
* Add basic support for user-defined timestamp to Merge (#10819).
* Add stats for ReadAsync time spent and async read errors.
* Basic support for the wide-column data model is now available. Wide-column entities can be stored using the `PutEntity` API, and retrieved using `GetEntity` and the new `columns` API of iterator. For compatibility, the classic APIs `Get` and `MultiGet`, as well as iterator's `value` API return the value of the anonymous default column of wide-column entities; also, `GetEntity` and iterator's `columns` return any plain key-values in the form of an entity which only has the anonymous default column. `Merge` (and `GetMergeOperands`) currently also apply to the default column; any other columns of entities are unaffected by `Merge` operations. Note that some features like compaction filters, transactions, user-defined timestamps, and the SST file writer do not yet support wide-column entities; also, there is currently no `MultiGet`-like API to retrieve multiple entities at once. We plan to gradually close the above gaps and also implement new features like column-level operations (e.g. updating or querying only certain columns of an entity).
* Marked HyperClockCache as a production-ready alternative to LRUCache for the block cache. HyperClockCache greatly improves hot-path CPU efficiency under high parallel load or high contention, with some documented caveats and limitations. As much as 4.5x higher ops/sec vs. LRUCache has been seen in db_bench under high parallel load.
* Add periodic diagnostics to info_log (LOG file) for HyperClockCache block cache if performance is degraded by bad `estimated_entry_charge` option.

### Public API Changes
* Marked `block_cache_compressed` as a deprecated feature. Use SecondaryCache instead.
* Added a `SecondaryCache::InsertSaved()` API, with default implementation depending on `Insert()`. Some implementations might need to add a custom implementation of `InsertSaved()`. (Details in API comments.)

## 7.8.0 (10/22/2022)
### New Features
* `DeleteRange()` now supports user-defined timestamp.
* Provide support for async_io with tailing iterators when ReadOptions.tailing is enabled during scans.
* Tiered Storage: allow data moving up from the last level to the penultimate level if the input level is penultimate level or above.
* Added `DB::Properties::kFastBlockCacheEntryStats`, which is similar to `DB::Properties::kBlockCacheEntryStats`, except returns cached (stale) values in more cases to reduce overhead.
* FIFO compaction now supports migrating from a multi-level DB via DB::Open(). During the migration phase, FIFO compaction picker will:
* picks the sst file with the smallest starting key in the bottom-most non-empty level.
* Note that during the migration phase, the file purge order will only be an approximation of "FIFO" as files in lower-level might sometime contain newer keys than files in upper-level.
* Added an option `ignore_max_compaction_bytes_for_input` to ignore max_compaction_bytes limit when adding files to be compacted from input level. This should help reduce write amplification. The option is enabled by default.
* Tiered Storage: allow data moving up from the last level even if it's a last level only compaction, as long as the penultimate level is empty.
* Add a new option IOOptions.do_not_recurse that can be used by underlying file systems to skip recursing through sub directories and list only files in GetChildren API.
* Add option `preserve_internal_time_seconds` to preserve the time information for the latest data. Which can be used to determine the age of data when `preclude_last_level_data_seconds` is enabled. The time information is attached with SST in table property `rocksdb.seqno.time.map` which can be parsed by tool ldb or sst_dump.

### Bug Fixes
* Fix a bug in io_uring_prep_cancel in AbortIO API for posix which expects sqe->addr to match with read request submitted and wrong paramter was being passed.
* Fixed a regression in iterator performance when the entire DB is a single memtable introduced in #10449. The fix is in #10705 and #10716.
* Fixed an optimistic transaction validation bug caused by DBImpl::GetLatestSequenceForKey() returning non-latest seq for merge (#10724).
* Fixed a bug in iterator refresh which could segfault for DeleteRange users (#10739).
* Fixed a bug causing manual flush with `flush_opts.wait=false` to stall when database has stopped all writes (#10001).
* Fixed a bug in iterator refresh that was not freeing up SuperVersion, which could cause excessive resource pinniung (#10770).
* Fixed a bug where RocksDB could be doing compaction endlessly when allow_ingest_behind is true and the bottommost level is not filled (#10767).
* Fixed a memory safety bug in experimental HyperClockCache (#10768)
* Fixed some cases where `ldb update_manifest` and `ldb unsafe_remove_sst_file` are not usable because they were requiring the DB files to match the existing manifest state (before updating the manifest to match a desired state).

### Performance Improvements
* Try to align the compaction output file boundaries to the next level ones, which can reduce more than 10% compaction load for the default level compaction. The feature is enabled by default, to disable, set `AdvancedColumnFamilyOptions.level_compaction_dynamic_file_size` to false. As a side effect, it can create SSTs larger than the target_file_size (capped at 2x target_file_size) or smaller files.
* Improve RoundRobin TTL compaction, which is going to be the same as normal RoundRobin compaction to move the compaction cursor.
* Fix a small CPU regression caused by a change that UserComparatorWrapper was made Customizable, because Customizable itself has small CPU overhead for initialization.

### Behavior Changes
* Sanitize min_write_buffer_number_to_merge to 1 if atomic flush is enabled to prevent unexpected data loss when WAL is disabled in a multi-column-family setting (#10773).
* With periodic stat dumper waits up every options.stats_dump_period_sec seconds, it won't dump stats for a CF if it has no change in the period, unless 7 periods have been skipped.
* Only periodic stats dumper triggered by options.stats_dump_period_sec will update stats interval. Ones triggered by DB::GetProperty() will not update stats interval and will report based on an interval since the last time stats dump period.

### Public API changes
* Make kXXH3 checksum the new default, because it is faster on common hardware, especially with kCRC32c affected by a performance bug in some versions of clang (https://github.com/facebook/rocksdb/issues/9891). DBs written with this new setting can be read by RocksDB 6.27 and newer.
* Refactor the classes, APIs and data structures for block cache tracing to allow a user provided trace writer to be used. Introduced an abstract BlockCacheTraceWriter class that takes a structured BlockCacheTraceRecord. The BlockCacheTraceWriter implementation can then format and log the record in whatever way it sees fit. The default BlockCacheTraceWriterImpl does file tracing using a user provided TraceWriter. More details in rocksdb/includb/block_cache_trace_writer.h.

## 7.7.0 (09/18/2022)
### Bug Fixes
* Fixed a hang when an operation such as `GetLiveFiles` or `CreateNewBackup` is asked to trigger and wait for memtable flush on a read-only DB. Such indirect requests for memtable flush are now ignored on a read-only DB.
* Fixed bug where `FlushWAL(true /* sync */)` (used by `GetLiveFilesStorageInfo()`, which is used by checkpoint and backup) could cause parallel writes at the tail of a WAL file to never be synced.
* Fix periodic_task unable to re-register the same task type, which may cause `SetOptions()` fail to update periodical_task time like: `stats_dump_period_sec`, `stats_persist_period_sec`.
* Fixed a bug in the rocksdb.prefetched.bytes.discarded stat. It was counting the prefetch buffer size, rather than the actual number of bytes discarded from the buffer.
* Fix bug where the directory containing CURRENT can left unsynced after CURRENT is updated to point to the latest MANIFEST, which leads to risk of unsync data loss of CURRENT.
* Update rocksdb.multiget.io.batch.size stat in non-async MultiGet as well.
* Fix a bug in key range overlap checking with concurrent compactions when user-defined timestamp is enabled. User-defined timestamps should be EXCLUDED when checking if two ranges overlap.
* Fixed a bug where the blob cache prepopulating logic did not consider the secondary cache (see #10603).
* Fixed the rocksdb.num.sst.read.per.level, rocksdb.num.index.and.filter.blocks.read.per.level and rocksdb.num.level.read.per.multiget stats in the MultiGet coroutines

### Public API changes
* Add `rocksdb_column_family_handle_get_id`, `rocksdb_column_family_handle_get_name` to get name, id of column family in C API
* Add a new stat rocksdb.async.prefetch.abort.micros to measure time spent waiting for async prefetch reads to abort

### Java API Changes
* Add CompactionPriority.RoundRobin.
* Revert to using the default metadata charge policy when creating an LRU cache via the Java API.

### Behavior Change
* DBOptions::verify_sst_unique_id_in_manifest is now an on-by-default feature that verifies SST file identity whenever they are opened by a DB, rather than only at DB::Open time.
* Right now, when the option migration tool (OptionChangeMigration()) migrates to FIFO compaction, it compacts all the data into one single SST file and move to L0. This might create a problem for some users: the giant file may be soon deleted to satisfy max_table_files_size, and might cayse the DB to be almost empty. We change the behavior so that the files are cut to be smaller, but these files might not follow the data insertion order. With the change, after the migration, migrated data might not be dropped by insertion order by FIFO compaction.
* When a block is firstly found from `CompressedSecondaryCache`, we just insert a dummy block into the primary cache and don’t erase the block from `CompressedSecondaryCache`. A standalone handle is returned to the caller. Only if the block is found again from `CompressedSecondaryCache` before the dummy block is evicted, we erase the block from `CompressedSecondaryCache` and insert it into the primary cache.
* When a block is firstly evicted from the primary cache to `CompressedSecondaryCache`, we just insert a dummy block in `CompressedSecondaryCache`. Only if it is evicted again before the dummy block is evicted from the cache, it is treated as a hot block and is inserted into `CompressedSecondaryCache`.
* Improved the estimation of memory used by cached blobs by taking into account the size of the object owning the blob value and also the allocator overhead if `malloc_usable_size` is available (see #10583).
* Blob values now have their own category in the cache occupancy statistics, as opposed to being lumped into the "Misc" bucket (see #10601).
* Change the optimize_multiget_for_io experimental ReadOptions flag to default on.

### New Features
*  RocksDB does internal auto prefetching if it notices 2 sequential reads if readahead_size is not specified. New option `num_file_reads_for_auto_readahead` is added in BlockBasedTableOptions which indicates after how many sequential reads internal auto prefetching should be start (default is 2).
* Added new perf context counters `block_cache_standalone_handle_count`, `block_cache_real_handle_count`,`compressed_sec_cache_insert_real_count`, `compressed_sec_cache_insert_dummy_count`, `compressed_sec_cache_uncompressed_bytes`, and `compressed_sec_cache_compressed_bytes`.
* Memory for blobs which are to be inserted into the blob cache is now allocated using the cache's allocator (see #10628 and #10647).
* HyperClockCache is an experimental, lock-free Cache alternative for block cache that offers much improved CPU efficiency under high parallel load or high contention, with some caveats. As much as 4.5x higher ops/sec vs. LRUCache has been seen in db_bench under high parallel load.
* `CompressedSecondaryCacheOptions::enable_custom_split_merge` is added for enabling the custom split and merge feature, which split the compressed value into chunks so that they may better fit jemalloc bins.

### Performance Improvements
* Iterator performance is improved for `DeleteRange()` users. Internally, iterator will skip to the end of a range tombstone when possible, instead of looping through each key and check individually if a key is range deleted.
* Eliminated some allocations and copies in the blob read path. Also, `PinnableSlice` now only points to the blob value and pins the backing resource (cache entry or buffer) in all cases, instead of containing a copy of the blob value. See #10625 and #10647.
* In case of scans with async_io enabled, few optimizations have been added to issue more asynchronous requests in parallel in order to avoid synchronous prefetching.
* `DeleteRange()` users should see improvement in get/iterator performance from mutable memtable (see #10547).

## 7.6.0 (08/19/2022)
### New Features
* Added `prepopulate_blob_cache` to ColumnFamilyOptions. If enabled, prepopulate warm/hot blobs which are already in memory into blob cache at the time of flush. On a flush, the blob that is in memory (in memtables) get flushed to the device. If using Direct IO, additional IO is incurred to read this blob back into memory again, which is avoided by enabling this option. This further helps if the workload exhibits high temporal locality, where most of the reads go to recently written data. This also helps in case of the remote file system since it involves network traffic and higher latencies.
* Support using secondary cache with the blob cache. When creating a blob cache, the user can set a secondary blob cache by configuring `secondary_cache` in LRUCacheOptions.
* Charge memory usage of blob cache when the backing cache of the blob cache and the block cache are different. If an operation reserving memory for blob cache exceeds the avaible space left in the block cache at some point (i.e, causing a cache full under `LRUCacheOptions::strict_capacity_limit` = true), creation will fail with `Status::MemoryLimit()`. To opt in this feature, enable charging `CacheEntryRole::kBlobCache` in `BlockBasedTableOptions::cache_usage_options`.
* Improve subcompaction range partition so that it is likely to be more even. More evenly distribution of subcompaction will improve compaction throughput for some workloads. All input files' index blocks to sample some anchor key points from which we pick positions to partition the input range. This would introduce some CPU overhead in compaction preparation phase, if subcompaction is enabled, but it should be a small fraction of the CPU usage of the whole compaction process. This also brings a behavier change: subcompaction number is much more likely to maxed out than before.
* Add CompactionPri::kRoundRobin, a compaction picking mode that cycles through all the files with a compact cursor in a round-robin manner. This feature is available since 7.5.
* Provide support for subcompactions for user_defined_timestamp.
* Added an option `memtable_protection_bytes_per_key` that turns on memtable per key-value checksum protection. Each memtable entry will be suffixed by a checksum that is computed during writes, and verified in reads/compaction. Detected corruption will be logged and with corruption status returned to user.
* Added a blob-specific cache priority level - bottom level. Blobs are typically lower-value targets for caching than data blocks, since 1) with BlobDB, data blocks containing blob references conceptually form an index structure which has to be consulted before we can read the blob value, and 2) cached blobs represent only a single key-value, while cached data blocks generally contain multiple KVs. The user can specify the new option `low_pri_pool_ratio` in `LRUCacheOptions` to configure the ratio of capacity reserved for low priority cache entries (and therefore the remaining ratio is the space reserved for the bottom level), or configuring the new argument `low_pri_pool_ratio` in `NewLRUCache()` to achieve the same effect.

### Public API changes
* Removed Customizable support for RateLimiter and removed its CreateFromString() and Type() functions.
* `CompactRangeOptions::exclusive_manual_compaction` is now false by default. This ensures RocksDB does not introduce artificial parallelism limitations by default.
* Tiered Storage: change `bottommost_temperture` to `last_level_temperture`. The old option name is kept only for migration, please use the new option. The behavior is changed to apply temperature for the `last_level` SST files only.
* Added a new experimental ReadOption flag called optimize_multiget_for_io, which when set attempts to reduce MultiGet latency by spawning coroutines for keys in multiple levels.

### Bug Fixes
* Fix a bug starting in 7.4.0 in which some fsync operations might be skipped in a DB after any DropColumnFamily on that DB, until it is re-opened. This can lead to data loss on power loss. (For custom FileSystem implementations, this could lead to `FSDirectory::Fsync` or `FSDirectory::Close` after the first `FSDirectory::Close`; Also, valgrind could report call to `close()` with `fd=-1`.)
* Fix a bug where `GenericRateLimiter` could revert the bandwidth set dynamically using `SetBytesPerSecond()` when a user configures a structure enclosing it, e.g., using `GetOptionsFromString()` to configure an `Options` that references an existing `RateLimiter` object.
* Fix race conditions in `GenericRateLimiter`.
* Fix a bug in `FIFOCompactionPicker::PickTTLCompaction` where total_size calculating might cause underflow
* Fix data race bug in hash linked list memtable. With this bug, read request might temporarily miss an old record in the memtable in a race condition to the hash bucket.
* Fix a bug that `best_efforts_recovery` may fail to open the db with mmap read.
* Fixed a bug where blobs read during compaction would pollute the cache.
* Fixed a data race in LRUCache when used with a secondary_cache.
* Fixed a bug where blobs read by iterators would be inserted into the cache even with the `fill_cache` read option set to false.
* Fixed the segfault caused by `AllocateData()` in `CompressedSecondaryCache::SplitValueIntoChunks()` and `MergeChunksIntoValueTest`.
* Fixed a bug in BlobDB where a mix of inlined and blob values could result in an incorrect value being passed to the compaction filter (see #10391).
* Fixed a memory leak bug in stress tests caused by `FaultInjectionSecondaryCache`.

### Behavior Change
* Added checksum handshake during the copying of decompressed WAL fragment. This together with #9875, #10037, #10212, #10114 and #10319 provides end-to-end integrity protection for write batch during recovery.
* To minimize the internal fragmentation caused by the variable size of the compressed blocks in `CompressedSecondaryCache`, the original block is split according to the jemalloc bin size in `Insert()` and then merged back in `Lookup()`.
* PosixLogger is removed and by default EnvLogger will be used for info logging. The behavior of the two loggers should be very similar when using the default Posix Env.
* Remove [min|max]_timestamp from VersionEdit for now since they are not tracked in MANIFEST anyway but consume two empty std::string (up to 64 bytes) for each file. Should they be added back in the future, we should store them more compactly.
* Improve universal tiered storage compaction picker to avoid extra major compaction triggered by size amplification. If `preclude_last_level_data_seconds` is enabled, the size amplification is calculated within non last_level data only which skip the last level and use the penultimate level as the size base.
* If an error is hit when writing to a file (append, sync, etc), RocksDB is more strict with not issuing more operations to it, except closing the file, with exceptions of some WAL file operations in error recovery path.
* A `WriteBufferManager` constructed with `allow_stall == false` will no longer trigger write stall implicitly by thrashing until memtable count limit is reached. Instead, a column family can continue accumulating writes while that CF is flushing, which means memory may increase. Users who prefer stalling writes must now explicitly set `allow_stall == true`.
* Add `CompressedSecondaryCache` into the stress tests.
* Block cache keys have changed, which will cause any persistent caches to miss between versions.

### Performance Improvements
* Instead of constructing `FragmentedRangeTombstoneList` during every read operation, it is now constructed once and stored in immutable memtables. This improves speed of querying range tombstones from immutable memtables.
* When using iterators with the integrated BlobDB implementation, blob cache handles are now released immediately when the iterator's position changes.
* MultiGet can now do more IO in parallel by reading data blocks from SST files in multiple levels, if the optimize_multiget_for_io ReadOption flag is set.

## 7.5.0 (07/15/2022)
### New Features
* Mempurge option flag `experimental_mempurge_threshold` is now a ColumnFamilyOptions and can now be dynamically configured using `SetOptions()`.
* Support backward iteration when `ReadOptions::iter_start_ts` is set.
* Provide support for ReadOptions.async_io with direct_io to improve Seek latency by using async IO to parallelize child iterator seek and doing asynchronous prefetching on sequential scans.
* Added support for blob caching in order to cache frequently used blobs for BlobDB.
  * User can configure the new ColumnFamilyOptions `blob_cache` to enable/disable blob caching.
  * Either sharing the backend cache with the block cache or using a completely separate cache is supported.
  * A new abstraction interface called `BlobSource` for blob read logic gives all users access to blobs, whether they are in the blob cache, secondary cache, or (remote) storage. Blobs can be potentially read both while handling user reads (`Get`, `MultiGet`, or iterator) and during compaction (while dealing with compaction filters, Merges, or garbage collection) but eventually all blob reads go through `Version::GetBlob` or, for MultiGet, `Version::MultiGetBlob` (and then get dispatched to the interface -- `BlobSource`).
* Add experimental tiered compaction feature `AdvancedColumnFamilyOptions::preclude_last_level_data_seconds`, which makes sure the new data inserted within preclude_last_level_data_seconds won't be placed on cold tier (the feature is not complete).

### Public API changes
* Add metadata related structs and functions in C API, including
  * `rocksdb_get_column_family_metadata()` and `rocksdb_get_column_family_metadata_cf()` to obtain `rocksdb_column_family_metadata_t`.
  * `rocksdb_column_family_metadata_t` and its get functions & destroy function.
  * `rocksdb_level_metadata_t` and its and its get functions & destroy function.
  * `rocksdb_file_metadata_t` and its and get functions & destroy functions.
* Add suggest_compact_range() and suggest_compact_range_cf() to C API.
* When using block cache strict capacity limit (`LRUCache` with `strict_capacity_limit=true`), DB operations now fail with Status code `kAborted` subcode `kMemoryLimit` (`IsMemoryLimit()`) instead of `kIncomplete` (`IsIncomplete()`) when the capacity limit is reached, because Incomplete can mean other specific things for some operations. In more detail, `Cache::Insert()` now returns the updated Status code and this usually propagates through RocksDB to the user on failure.
* NewClockCache calls temporarily return an LRUCache (with similar characteristics as the desired ClockCache). This is because ClockCache is being replaced by a new version (the old one had unknown bugs) but this is still under development.
* Add two functions `int ReserveThreads(int threads_to_be_reserved)` and `int ReleaseThreads(threads_to_be_released)` into `Env` class. In the default implementation, both return 0. Newly added `xxxEnv` class that inherits `Env` should implement these two functions for thread reservation/releasing features.
* Add `rocksdb_options_get_prepopulate_blob_cache` and `rocksdb_options_set_prepopulate_blob_cache` to C API.
* Add `prepopulateBlobCache` and `setPrepopulateBlobCache` to Java API.

### Bug Fixes
* Fix a bug in which backup/checkpoint can include a WAL deleted by RocksDB.
* Fix a bug where concurrent compactions might cause unnecessary further write stalling. In some cases, this might cause write rate to drop to minimum.
* Fix a bug in Logger where if dbname and db_log_dir are on different filesystems, dbname creation would fail wrt to db_log_dir path returning an error and fails to open the DB.
* Fix a CPU and memory efficiency issue introduce by https://github.com/facebook/rocksdb/pull/8336 which made InternalKeyComparator configurable as an unintended side effect.

## Behavior Change
* In leveled compaction with dynamic levelling, level multiplier is not anymore adjusted due to oversized L0. Instead, compaction score is adjusted by increasing size level target by adding incoming bytes from upper levels. This would deprioritize compactions from upper levels if more data from L0 is coming. This is to fix some unnecessary full stalling due to drastic change of level targets, while not wasting write bandwidth for compaction while writes are overloaded.
* For track_and_verify_wals_in_manifest, revert to the original behavior before #10087: syncing of live WAL file is not tracked, and we track only the synced sizes of **closed** WALs. (PR #10330).
* WAL compression now computes/verifies checksum during compression/decompression.

### Performance Improvements
* Rather than doing total sort against all files in a level, SortFileByOverlappingRatio() to only find the top 50 files based on score. This can improve write throughput for the use cases where data is loaded in increasing key order and there are a lot of files in one LSM-tree, where applying compaction results is the bottleneck.
* In leveled compaction, L0->L1 trivial move will allow more than one file to be moved in one compaction. This would allow L0 files to be moved down faster when data is loaded in sequential order, making slowdown or stop condition harder to hit. Also seek L0->L1 trivial move when only some files qualify.
* In leveled compaction, try to trivial move more than one files if possible, up to 4 files or max_compaction_bytes. This is to allow higher write throughput for some use cases where data is loaded in sequential order, where appying compaction results is the bottleneck.

## 7.4.0 (06/19/2022)
### Bug Fixes
* Fixed a bug in calculating key-value integrity protection for users of in-place memtable updates. In particular, the affected users would be those who configure `protection_bytes_per_key > 0` on `WriteBatch` or `WriteOptions`, and configure `inplace_callback != nullptr`.
* Fixed a bug where a snapshot taken during SST file ingestion would be unstable.
* Fixed a bug for non-TransactionDB with avoid_flush_during_recovery = true and TransactionDB where in case of crash, min_log_number_to_keep may not change on recovery and persisting a new MANIFEST with advanced log_numbers for some column families, results in "column family inconsistency" error on second recovery. As a solution, RocksDB will persist the new MANIFEST after successfully syncing the new WAL. If a future recovery starts from the new MANIFEST, then it means the new WAL is successfully synced. Due to the sentinel empty write batch at the beginning, kPointInTimeRecovery of WAL is guaranteed to go after this point. If future recovery starts from the old MANIFEST, it means the writing the new MANIFEST failed. We won't have the "SST ahead of WAL" error.
* Fixed a bug where RocksDB DB::Open() may creates and writes to two new MANIFEST files even before recovery succeeds. Now writes to MANIFEST are persisted only after recovery is successful.
* Fix a race condition in WAL size tracking which is caused by an unsafe iterator access after container is changed.
* Fix unprotected concurrent accesses to `WritableFileWriter::filesize_` by `DB::SyncWAL()` and `DB::Put()` in two write queue mode.
* Fix a bug in WAL tracking. Before this PR (#10087), calling `SyncWAL()` on the only WAL file of the db will not log the event in MANIFEST, thus allowing a subsequent `DB::Open` even if the WAL file is missing or corrupted.
* Fix a bug that could return wrong results with `index_type=kHashSearch` and using `SetOptions` to change the `prefix_extractor`.
* Fixed a bug in WAL tracking with wal_compression. WAL compression writes a kSetCompressionType record which is not associated with any sequence number. As result, WalManager::GetSortedWalsOfType() will skip these WALs and not return them to caller, e.g. Checkpoint, Backup, causing the operations to fail.
* Avoid a crash if the IDENTITY file is accidentally truncated to empty. A new DB ID will be written and generated on Open.
* Fixed a possible corruption for users of `manual_wal_flush` and/or `FlushWAL(true /* sync */)`, together with `track_and_verify_wals_in_manifest == true`. For those users, losing unsynced data (e.g., due to power loss) could make future DB opens fail with a `Status::Corruption` complaining about missing WAL data.
* Fixed a bug in `WriteBatchInternal::Append()` where WAL termination point in write batch was not considered and the function appends an incorrect number of checksums.
* Fixed a crash bug introduced in 7.3.0 affecting users of MultiGet with `kDataBlockBinaryAndHash`.

### Public API changes
* Add new API GetUnixTime in Snapshot class which returns the unix time at which Snapshot is taken.
* Add transaction `get_pinned` and `multi_get` to C API.
* Add two-phase commit support to C API.
* Add `rocksdb_transaction_get_writebatch_wi` and `rocksdb_transaction_rebuild_from_writebatch` to C API.
* Add `rocksdb_options_get_blob_file_starting_level` and `rocksdb_options_set_blob_file_starting_level` to C API.
* Add `blobFileStartingLevel` and `setBlobFileStartingLevel` to Java API.
* Add SingleDelete for DB in C API
* Add User Defined Timestamp in C API.
  * `rocksdb_comparator_with_ts_create` to create timestamp aware comparator
  * Put, Get, Delete, SingleDelete, MultiGet APIs has corresponding timestamp aware APIs with suffix `with_ts`
  * And Add C API's for Transaction, SstFileWriter, Compaction as mentioned [here](https://github.com/facebook/rocksdb/wiki/User-defined-Timestamp-(Experimental))
* The contract for implementations of Comparator::IsSameLengthImmediateSuccessor has been updated to work around a design bug in `auto_prefix_mode`.
* The API documentation for `auto_prefix_mode` now notes some corner cases in which it returns different results than `total_order_seek`, due to design bugs that are not easily fixed. Users using built-in comparators and keys at least the size of a fixed prefix length are not affected.
* Obsoleted the NUM_DATA_BLOCKS_READ_PER_LEVEL stat and introduced the NUM_LEVEL_READ_PER_MULTIGET and MULTIGET_COROUTINE_COUNT stats
* Introduced `WriteOptions::protection_bytes_per_key`, which can be used to enable key-value integrity protection for live updates.

### New Features
* Add FileSystem::ReadAsync API in io_tracing
* Add blob garbage collection parameters `blob_garbage_collection_policy` and `blob_garbage_collection_age_cutoff` to both force-enable and force-disable GC, as well as selectively override age cutoff when using CompactRange.
* Add an extra sanity check in `GetSortedWalFiles()` (also used by `GetLiveFilesStorageInfo()`, `BackupEngine`, and `Checkpoint`) to reduce risk of successfully created backup or checkpoint failing to open because of missing WAL file.
* Add a new column family option `blob_file_starting_level` to enable writing blob files during flushes and compactions starting from the specified LSM tree level.
* Add support for timestamped snapshots (#9879)
* Provide support for AbortIO in posix to cancel submitted asynchronous requests using io_uring.
* Add support for rate-limiting batched `MultiGet()` APIs
* Added several new tickers, perf context statistics, and DB properties to BlobDB
  * Added new DB properties "rocksdb.blob-cache-capacity", "rocksdb.blob-cache-usage", "rocksdb.blob-cache-pinned-usage" to show blob cache usage.
  * Added new perf context statistics `blob_cache_hit_count`, `blob_read_count`, `blob_read_byte`, `blob_read_time`, `blob_checksum_time` and `blob_decompress_time`.
  * Added new tickers `BLOB_DB_CACHE_MISS`, `BLOB_DB_CACHE_HIT`, `BLOB_DB_CACHE_ADD`, `BLOB_DB_CACHE_ADD_FAILURES`, `BLOB_DB_CACHE_BYTES_READ` and `BLOB_DB_CACHE_BYTES_WRITE`.

### Behavior changes
* DB::Open(), DB::OpenAsSecondary() will fail if a Logger cannot be created (#9984)
* DB::Write does not hold global `mutex_` if this db instance does not need to switch wal and mem-table (#7516).
* Removed support for reading Bloom filters using obsolete block-based filter format. (Support for writing such filters was dropped in 7.0.) For good read performance on old DBs using these filters, a full compaction is required.
* Per KV checksum in write batch is verified before a write batch is written to WAL to detect any corruption to the write batch (#10114).

### Performance Improvements
* When compiled with folly (Meta-internal integration; experimental in open source build), improve the locking performance (CPU efficiency) of LRUCache by using folly DistributedMutex in place of standard mutex.

## 7.3.0 (05/20/2022)
### Bug Fixes
* Fixed a bug where manual flush would block forever even though flush options had wait=false.
* Fixed a bug where RocksDB could corrupt DBs with `avoid_flush_during_recovery == true` by removing valid WALs, leading to `Status::Corruption` with message like "SST file is ahead of WALs" when attempting to reopen.
* Fixed a bug in async_io path where incorrect length of data is read by FilePrefetchBuffer if data is consumed from two populated buffers and request for more data is sent.
* Fixed a CompactionFilter bug. Compaction filter used to use `Delete` to remove keys, even if the keys should be removed with `SingleDelete`. Mixing `Delete` and `SingleDelete` may cause undefined behavior.
* Fixed a bug in `WritableFileWriter::WriteDirect` and `WritableFileWriter::WriteDirectWithChecksum`. The rate_limiter_priority specified in ReadOptions was not passed to the RateLimiter when requesting a token.
* Fixed a bug which might cause process crash when I/O error happens when reading an index block in MultiGet().

### New Features
* DB::GetLiveFilesStorageInfo is ready for production use.
* Add new stats PREFETCHED_BYTES_DISCARDED which records number of prefetched bytes discarded by RocksDB FilePrefetchBuffer on destruction and POLL_WAIT_MICROS records wait time for FS::Poll API completion.
* RemoteCompaction supports table_properties_collector_factories override on compaction worker.
* Start tracking SST unique id in MANIFEST, which will be used to verify with SST properties during DB open to make sure the SST file is not overwritten or misplaced. A db option `verify_sst_unique_id_in_manifest` is introduced to enable/disable the verification, if enabled all SST files will be opened during DB-open to verify the unique id (default is false), so it's recommended to use it with `max_open_files = -1` to pre-open the files.
* Added the ability to concurrently read data blocks from multiple files in a level in batched MultiGet. This can be enabled by setting the async_io option in ReadOptions. Using this feature requires a FileSystem that supports ReadAsync (PosixFileSystem is not supported yet for this), and for RocksDB to be compiled with folly and c++20.
* Charge memory usage of file metadata. RocksDB holds one file metadata structure in-memory per on-disk table file. If an operation reserving memory for file metadata exceeds the avaible space left in the block
cache at some point (i.e, causing a cache full under `LRUCacheOptions::strict_capacity_limit` = true), creation will fail with `Status::MemoryLimit()`. To opt in this feature,  enable charging `CacheEntryRole::kFileMetadata` in `BlockBasedTableOptions::cache_usage_options`.

### Public API changes
* Add rollback_deletion_type_callback to TransactionDBOptions so that write-prepared transactions know whether to issue a Delete or SingleDelete to cancel a previous key written during prior prepare phase. The PR aims to prevent mixing SingleDeletes and Deletes for the same key that can lead to undefined behaviors for write-prepared transactions.
* EXPERIMENTAL: Add new API AbortIO in file_system to abort the read requests submitted asynchronously.
* CompactionFilter::Decision has a new value: kRemoveWithSingleDelete. If CompactionFilter returns this decision, then CompactionIterator will use `SingleDelete` to mark a key as removed.
* Renamed CompactionFilter::Decision::kRemoveWithSingleDelete to kPurge since the latter sounds more general and hides the implementation details of how compaction iterator handles keys.
* Added ability to specify functions for Prepare and Validate to OptionsTypeInfo.  Added methods to OptionTypeInfo to set the functions via an API.  These methods are intended for RocksDB plugin developers for configuration management.
* Added a new immutable db options, enforce_single_del_contracts. If set to false (default is true), compaction will NOT fail due to a single delete followed by a delete for the same key. The purpose of this temporay option is to help existing use cases migrate.
* Introduce `BlockBasedTableOptions::cache_usage_options` and use that to replace `BlockBasedTableOptions::reserve_table_builder_memory` and  `BlockBasedTableOptions::reserve_table_reader_memory`.
* Changed `GetUniqueIdFromTableProperties` to return a 128-bit unique identifier, which will be the standard size now. The old functionality (192-bit) is available from `GetExtendedUniqueIdFromTableProperties`. Both functions are no longer "experimental" and are ready for production use.
* In IOOptions, mark `prio` as deprecated for future removal.
* In `file_system.h`, mark `IOPriority` as deprecated for future removal.
* Add an option, `CompressionOptions::use_zstd_dict_trainer`, to indicate whether zstd dictionary trainer should be used for generating zstd compression dictionaries. The default value of this option is true for backward compatibility. When this option is set to false, zstd API `ZDICT_finalizeDictionary` is used to generate compression dictionaries.
* Seek API which positions itself every LevelIterator on the correct data block in the correct SST file which can be parallelized if ReadOptions.async_io option is enabled.
* Add new stat number_async_seek in PerfContext that indicates number of async calls made by seek to prefetch data.
* Add support for user-defined timestamps to read only DB.

### Bug Fixes
* RocksDB calls FileSystem::Poll API during FilePrefetchBuffer destruction which impacts performance as it waits for read requets completion which is not needed anymore. Calling FileSystem::AbortIO to abort those requests instead fixes that performance issue.
* Fixed unnecessary block cache contention when queries within a MultiGet batch and across parallel batches access the same data block, which previously could cause severely degraded performance in this unusual case. (In more typical MultiGet cases, this fix is expected to yield a small or negligible performance improvement.)

### Behavior changes
* Enforce the existing contract of SingleDelete so that SingleDelete cannot be mixed with Delete because it leads to undefined behavior. Fix a number of unit tests that violate the contract but happen to pass.
* ldb `--try_load_options` default to true if `--db` is specified and not creating a new DB, the user can still explicitly disable that by `--try_load_options=false` (or explicitly enable that by `--try_load_options`).
* During Flush write or Compaction write/read, the WriteController is used to determine whether DB writes are stalled or slowed down. The priority (Env::IOPriority) can then be determined accordingly and be passed in IOOptions to the file system.

### Performance Improvements
* Avoid calling malloc_usable_size() in LRU Cache's mutex.
* Reduce DB mutex holding time when finding obsolete files to delete. When a file is trivial moved to another level, the internal files will be referenced twice internally and sometimes opened twice too. If a deletion candidate file is not the last reference, we need to destroy the reference and close the file but not deleting the file. Right now we determine it by building a set of all live files. With the improvement, we check the file against all live LSM-tree versions instead.

## 7.2.0 (04/15/2022)
### Bug Fixes
* Fixed bug which caused rocksdb failure in the situation when rocksdb was accessible using UNC path
* Fixed a race condition when 2PC is disabled and WAL tracking in the MANIFEST is enabled. The race condition is between two background flush threads trying to install flush results, causing a WAL deletion not tracked in the MANIFEST. A future DB open may fail.
* Fixed a heap use-after-free race with DropColumnFamily.
* Fixed a bug that `rocksdb.read.block.compaction.micros` cannot track compaction stats (#9722).
* Fixed `file_type`, `relative_filename` and `directory` fields returned by `GetLiveFilesMetaData()`, which were added in inheriting from `FileStorageInfo`.
* Fixed a bug affecting `track_and_verify_wals_in_manifest`. Without the fix, application may see "open error: Corruption: Missing WAL with log number" while trying to open the db. The corruption is a false alarm but prevents DB open (#9766).
* Fix segfault in FilePrefetchBuffer with async_io as it doesn't wait for pending jobs to complete on destruction.
* Fix ERROR_HANDLER_AUTORESUME_RETRY_COUNT stat whose value was set wrong in portal.h
* Fixed a bug for non-TransactionDB with avoid_flush_during_recovery = true and TransactionDB where in case of crash, min_log_number_to_keep may not change on recovery and persisting a new MANIFEST with advanced log_numbers for some column families, results in "column family inconsistency" error on second recovery. As a solution the corrupted WALs whose numbers are larger than the corrupted wal and smaller than the new WAL will be moved to archive folder.
* Fixed a bug in RocksDB DB::Open() which may creates and writes to two new MANIFEST files even before recovery succeeds. Now writes to MANIFEST are persisted only after recovery is successful.

### New Features
* For db_bench when --seed=0 or --seed is not set then it uses the current time as the seed value. Previously it used the value 1000.
* For db_bench when --benchmark lists multiple tests and each test uses a seed for a RNG then the seeds across tests will no longer be repeated.
* Added an option to dynamically charge an updating estimated memory usage of block-based table reader to block cache if block cache available. To enable this feature, set `BlockBasedTableOptions::reserve_table_reader_memory = true`.
* Add new stat ASYNC_READ_BYTES that calculates number of bytes read during async read call and users can check if async code path is being called by RocksDB internal automatic prefetching for sequential reads.
* Enable async prefetching if ReadOptions.readahead_size is set along with ReadOptions.async_io in FilePrefetchBuffer.
* Add event listener support on remote compaction compactor side.
* Added a dedicated integer DB property `rocksdb.live-blob-file-garbage-size` that exposes the total amount of garbage in the blob files in the current version.
* RocksDB does internal auto prefetching if it notices sequential reads. It starts with readahead size `initial_auto_readahead_size` which now can be configured through BlockBasedTableOptions.
* Add a merge operator that allows users to register specific aggregation function so that they can does aggregation using different aggregation types for different keys. See comments in include/rocksdb/utilities/agg_merge.h for actual usage. The feature is experimental and the format is subject to change and we won't provide a migration tool.
* Meta-internal / Experimental: Improve CPU performance by replacing many uses of std::unordered_map with folly::F14FastMap when RocksDB is compiled together with Folly.
* Experimental: Add CompressedSecondaryCache, a concrete implementation of rocksdb::SecondaryCache, that integrates with compression libraries (e.g. LZ4) to hold compressed blocks.

### Behavior changes
* Disallow usage of commit-time-write-batch for write-prepared/write-unprepared transactions if TransactionOptions::use_only_the_last_commit_time_batch_for_recovery is false to prevent two (or more) uncommitted versions of the same key in the database. Otherwise, bottommost compaction may violate the internal key uniqueness invariant of SSTs if the sequence numbers of both internal keys are zeroed out (#9794).
* Make DB::GetUpdatesSince() return NotSupported early for write-prepared/write-unprepared transactions, as the API contract indicates.

### Public API changes
* Exposed APIs to examine results of block cache stats collections in a structured way. In particular, users of `GetMapProperty()` with property `kBlockCacheEntryStats` can now use the functions in `BlockCacheEntryStatsMapKeys` to find stats in the map.
* Add `fail_if_not_bottommost_level` to IngestExternalFileOptions so that ingestion will fail if the file(s) cannot be ingested to the bottommost level.
* Add output parameter `is_in_sec_cache` to `SecondaryCache::Lookup()`. It is to indicate whether the handle is possibly erased from the secondary cache after the Lookup.

## 7.1.0 (03/23/2022)
### New Features
* Allow WriteBatchWithIndex to index a WriteBatch that includes keys with user-defined timestamps. The index itself does not have timestamp.
* Add support for user-defined timestamps to write-committed transaction without API change. The `TransactionDB` layer APIs do not allow timestamps because we require that all user-defined-timestamps-aware operations go through the `Transaction` APIs.
* Added BlobDB options to `ldb`
* `BlockBasedTableOptions::detect_filter_construct_corruption` can now be dynamically configured using `DB::SetOptions`.
* Automatically recover from retryable read IO errors during backgorund flush/compaction.
* Experimental support for preserving file Temperatures through backup and restore, and for updating DB metadata for outside changes to file Temperature (`UpdateManifestForFilesState` or `ldb update_manifest --update_temperatures`).
* Experimental support for async_io in ReadOptions which is used by FilePrefetchBuffer to prefetch some of the data asynchronously,  if reads are sequential and auto readahead is enabled by rocksdb internally.

### Bug Fixes
* Fixed a major performance bug in which Bloom filters generated by pre-7.0 releases are not read by early 7.0.x releases (and vice-versa) due to changes to FilterPolicy::Name() in #9590. This can severely impact read performance and read I/O on upgrade or downgrade with existing DB, but not data correctness.
* Fixed a data race on `versions_` between `DBImpl::ResumeImpl()` and threads waiting for recovery to complete (#9496)
* Fixed a bug caused by race among flush, incoming writes and taking snapshots. Queries to snapshots created with these race condition can return incorrect result, e.g. resurfacing deleted data.
* Fixed a bug that DB flush uses `options.compression` even `options.compression_per_level` is set.
* Fixed a bug that DisableManualCompaction may assert when disable an unscheduled manual compaction.
* Fix a race condition when cancel manual compaction with `DisableManualCompaction`. Also DB close can cancel the manual compaction thread.
* Fixed a potential timer crash when open close DB concurrently.
* Fixed a race condition for `alive_log_files_` in non-two-write-queues mode. The race is between the write_thread_ in WriteToWAL() and another thread executing `FindObsoleteFiles()`. The race condition will be caught if `__glibcxx_requires_nonempty` is enabled.
* Fixed a bug that `Iterator::Refresh()` reads stale keys after DeleteRange() performed.
* Fixed a race condition when disable and re-enable manual compaction.
* Fixed automatic error recovery failure in atomic flush.
* Fixed a race condition when mmaping a WritableFile on POSIX.

### Public API changes
* Added pure virtual FilterPolicy::CompatibilityName(), which is needed for fixing major performance bug involving FilterPolicy naming in SST metadata without affecting Customizable aspect of FilterPolicy. This change only affects those with their own custom or wrapper FilterPolicy classes.
* `options.compression_per_level` is dynamically changeable with `SetOptions()`.
* Added `WriteOptions::rate_limiter_priority`. When set to something other than `Env::IO_TOTAL`, the internal rate limiter (`DBOptions::rate_limiter`) will be charged at the specified priority for writes associated with the API to which the `WriteOptions` was provided. Currently the support covers automatic WAL flushes, which happen during live updates (`Put()`, `Write()`, `Delete()`, etc.) when `WriteOptions::disableWAL == false` and `DBOptions::manual_wal_flush == false`.
* Add DB::OpenAndTrimHistory API. This API will open DB and trim data to the timestamp specified by trim_ts (The data with timestamp larger than specified trim bound will be removed). This API should only be used at a timestamp-enabled column families recovery. If the column family doesn't have timestamp enabled, this API won't trim any data on that column family. This API is not compatible with avoid_flush_during_recovery option.
* Remove BlockBasedTableOptions.hash_index_allow_collision which already takes no effect.

## 7.0.0 (02/20/2022)
### Bug Fixes
* Fixed a major bug in which batched MultiGet could return old values for keys deleted by DeleteRange when memtable Bloom filter is enabled (memtable_prefix_bloom_size_ratio > 0). (The fix includes a substantial MultiGet performance improvement in the unusual case of both memtable_whole_key_filtering and prefix_extractor.)
* Fixed more cases of EventListener::OnTableFileCreated called with OK status, file_size==0, and no SST file kept. Now the status is Aborted.
* Fixed a read-after-free bug in `DB::GetMergeOperands()`.
* Fix a data loss bug for 2PC write-committed transaction caused by concurrent transaction commit and memtable switch (#9571).
* Fixed NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL, NUM_DATA_BLOCKS_READ_PER_LEVEL, and NUM_SST_READ_PER_LEVEL stats to be reported once per MultiGet batch per level.

### Performance Improvements
* Mitigated the overhead of building the file location hash table used by the online LSM tree consistency checks, which can improve performance for certain workloads (see #9351).
* Switched to using a sorted `std::vector` instead of `std::map` for storing the metadata objects for blob files, which can improve performance for certain workloads, especially when the number of blob files is high.
* DisableManualCompaction() doesn't have to wait scheduled manual compaction to be executed in thread-pool to cancel the job.

### Public API changes
* Require C++17 compatible compiler (GCC >= 7, Clang >= 5, Visual Studio >= 2017) for compiling RocksDB and any code using RocksDB headers. See #9388.
* Added `ReadOptions::rate_limiter_priority`. When set to something other than `Env::IO_TOTAL`, the internal rate limiter (`DBOptions::rate_limiter`) will be charged at the specified priority for file reads associated with the API to which the `ReadOptions` was provided.
* Remove HDFS support from main repo.
* Remove librados support from main repo.
* Remove obsolete backupable_db.h and type alias `BackupableDBOptions`. Use backup_engine.h and `BackupEngineOptions`. Similar renamings are in the C and Java APIs.
* Removed obsolete utility_db.h and `UtilityDB::OpenTtlDB`. Use db_ttl.h and `DBWithTTL::Open`.
* Remove deprecated API DB::AddFile from main repo.
* Remove deprecated API ObjectLibrary::Register() and the (now obsolete) Regex public API. Use ObjectLibrary::AddFactory() with PatternEntry instead.
* Remove deprecated option DBOption::table_cache_remove_scan_count_limit.
* Remove deprecated API AdvancedColumnFamilyOptions::soft_rate_limit.
* Remove deprecated API AdvancedColumnFamilyOptions::hard_rate_limit.
* Remove deprecated API DBOption::base_background_compactions.
* Remove deprecated API DBOptions::purge_redundant_kvs_while_flush.
* Remove deprecated overloads of API DB::CompactRange.
* Remove deprecated option DBOptions::skip_log_error_on_recovery.
* Remove ReadOptions::iter_start_seqnum which has been deprecated.
* Remove DBOptions::preserved_deletes and DB::SetPreserveDeletesSequenceNumber().
* Remove deprecated API AdvancedColumnFamilyOptions::rate_limit_delay_max_milliseconds.
* Removed timestamp from WriteOptions. Accordingly, added to DB APIs Put, Delete, SingleDelete, etc. accepting an additional argument 'timestamp'. Added Put, Delete, SingleDelete, etc to WriteBatch accepting an additional argument 'timestamp'. Removed WriteBatch::AssignTimestamps(vector<Slice>) API. Renamed WriteBatch::AssignTimestamp() to WriteBatch::UpdateTimestamps() with clarified comments.
* Changed type of cache buffer passed to `Cache::CreateCallback` from `void*` to `const void*`.
* Significant updates to FilterPolicy-related APIs and configuration:
  * Remove public API support for deprecated, inefficient block-based filter (use_block_based_builder=true).
    * Old code and configuration strings that would enable it now quietly enable full filters instead, though any built-in FilterPolicy can still read block-based filters. This includes changing the longstanding default behavior of the Java API.
    * Remove deprecated FilterPolicy::CreateFilter() and FilterPolicy::KeyMayMatch()
    * Remove `rocksdb_filterpolicy_create()` from C API, as the only C API support for custom filter policies is now obsolete.
    * If temporary memory usage in full filter creation is a problem, consider using partitioned filters, smaller SST files, or setting reserve_table_builder_memory=true.
  * Remove support for "filter_policy=experimental_ribbon" configuration
  string. Use something like "filter_policy=ribbonfilter:10" instead.
  * Allow configuration string like "filter_policy=bloomfilter:10" without
  bool, to minimize acknowledgement of obsolete block-based filter.
  * Made FilterPolicy Customizable. Configuration of filter_policy is now accurately saved in OPTIONS file and can be loaded with LoadOptionsFromFile. (Loading an OPTIONS file generated by a previous version only enables reading and using existing filters, not generating new filters. Previously, no filter_policy would be configured from a saved OPTIONS file.)
  * Change meaning of nullptr return from GetBuilderWithContext() from "use
    block-based filter" to "generate no filter in this case."
    * Also, when user specifies bits_per_key < 0.5, we now round this down
    to "no filter" because we expect a filter with >= 80% FP rate is
    unlikely to be worth the CPU cost of accessing it (esp with
    cache_index_and_filter_blocks=1 or partition_filters=1).
    * bits_per_key >= 0.5 and < 1.0 is still rounded up to 1.0 (for 62% FP
    rate)
  * Remove class definitions for FilterBitsBuilder and FilterBitsReader from
    public API, so these can evolve more easily as implementation details.
    Custom FilterPolicy can still decide what kind of built-in filter to use
    under what conditions.
  * Also removed deprecated functions
    * FilterPolicy::GetFilterBitsBuilder()
    * NewExperimentalRibbonFilterPolicy()
  * Remove default implementations of
    * FilterPolicy::GetBuilderWithContext()
* Remove default implementation of Name() from FileSystemWrapper.
* Rename `SizeApproximationOptions.include_memtabtles` to `SizeApproximationOptions.include_memtables`.
* Remove deprecated option DBOptions::max_mem_compaction_level.
* Return Status::InvalidArgument from ObjectRegistry::NewObject if a factory exists but the object ould not be created (returns NotFound if the factory is missing).
* Remove deprecated overloads of API DB::GetApproximateSizes.
* Remove deprecated option DBOptions::new_table_reader_for_compaction_inputs.
* Add Transaction::SetReadTimestampForValidation() and Transaction::SetCommitTimestamp(). Default impl returns NotSupported().
* Add support for decimal patterns to ObjectLibrary::PatternEntry
* Remove deprecated remote compaction APIs `CompactionService::Start()` and `CompactionService::WaitForComplete()`. Please use `CompactionService::StartV2()`, `CompactionService::WaitForCompleteV2()` instead, which provides the same information plus extra data like priority, db_id, etc.
* `ColumnFamilyOptions::OldDefaults` and `DBOptions::OldDefaults` are marked deprecated, as they are no longer maintained.
* Add subcompaction callback APIs: `OnSubcompactionBegin()` and `OnSubcompactionCompleted()`.
* Add file Temperature information to `FileOperationInfo` in event listener API.
* Change the type of SizeApproximationFlags from enum to enum class. Also update the signature of DB::GetApproximateSizes API from uint8_t to SizeApproximationFlags.
* Add Temperature hints information from RocksDB in API `NewSequentialFile()`. backup and checkpoint operations need to open the source files with `NewSequentialFile()`, which will have the temperature hints. Other operations are not covered.

### Behavior Changes
* Disallow the combination of DBOptions.use_direct_io_for_flush_and_compaction == true and DBOptions.writable_file_max_buffer_size == 0. This combination can cause WritableFileWriter::Append() to loop forever, and it does not make much sense in direct IO.
* `ReadOptions::total_order_seek` no longer affects `DB::Get()`. The original motivation for this interaction has been obsolete since RocksDB has been able to detect whether the current prefix extractor is compatible with that used to generate table files, probably RocksDB 5.14.0.

## New Features
* Introduced an option `BlockBasedTableOptions::detect_filter_construct_corruption` for detecting corruption during Bloom Filter (format_version >= 5) and Ribbon Filter construction.
* Improved the SstDumpTool to read the comparator from table properties and use it to read the SST File.
* Extended the column family statistics in the info log so the total amount of garbage in the blob files and the blob file space amplification factor are also logged. Also exposed the blob file space amp via the `rocksdb.blob-stats` DB property.
* Introduced the API rocksdb_create_dir_if_missing in c.h that calls underlying file system's CreateDirIfMissing API to create the directory.
* Added last level and non-last level read statistics: `LAST_LEVEL_READ_*`, `NON_LAST_LEVEL_READ_*`.
* Experimental: Add support for new APIs ReadAsync in FSRandomAccessFile that reads the data asynchronously and Poll API in FileSystem that checks if requested read request has completed or not. ReadAsync takes a callback function. Poll API checks for completion of read IO requests and  should call callback functions to indicate completion of read requests.

## 6.29.0 (01/21/2022)
Note: The next release will be major release 7.0. See https://github.com/facebook/rocksdb/issues/9390 for more info.
### Public API change
* Added values to `TraceFilterType`: `kTraceFilterIteratorSeek`, `kTraceFilterIteratorSeekForPrev`, and `kTraceFilterMultiGet`. They can be set in `TraceOptions` to filter out the operation types after which they are named.
* Added `TraceOptions::preserve_write_order`. When enabled it  guarantees write records are traced in the same order they are logged to WAL and applied to the DB. By default it is disabled (false) to match the legacy behavior and prevent regression.
* Made the Env class extend the Customizable class.  Implementations need to be registered with the ObjectRegistry and to implement a Name() method in order to be created via this method.
* `Options::OldDefaults` is marked deprecated, as it is no longer maintained.
* Add ObjectLibrary::AddFactory and ObjectLibrary::PatternEntry classes.  This method and associated class are the preferred mechanism for registering factories with the ObjectLibrary going forward.  The ObjectLibrary::Register method, which uses regular expressions and may be problematic, is deprecated and will be in a future release.
* Changed `BlockBasedTableOptions::block_size` from `size_t` to `uint64_t`.
* Added API warning against using `Iterator::Refresh()` together with `DB::DeleteRange()`, which are incompatible and have always risked causing the refreshed iterator to return incorrect results.
* Made `AdvancedColumnFamilyOptions.bottommost_temperature` dynamically changeable with `SetOptions()`.

### Behavior Changes
* `DB::DestroyColumnFamilyHandle()` will return Status::InvalidArgument() if called with `DB::DefaultColumnFamily()`.
* On 32-bit platforms, mmap reads are no longer quietly disabled, just discouraged.

### New Features
* Added `Options::DisableExtraChecks()` that can be used to improve peak write performance by disabling checks that should not be necessary in the absence of software logic errors or CPU+memory hardware errors. (Default options are slowly moving toward some performance overheads for extra correctness checking.)

### Performance Improvements
* Improved read performance when a prefix extractor is used (Seek, Get, MultiGet), even compared to version 6.25 baseline (see bug fix below), by optimizing the common case of prefix extractor compatible with table file and unchanging.

### Bug Fixes
* Fix a bug that FlushMemTable may return ok even flush not succeed.
* Fixed a bug of Sync() and Fsync() not using `fcntl(F_FULLFSYNC)` on OS X and iOS.
* Fixed a significant performance regression in version 6.26 when a prefix extractor is used on the read path (Seek, Get, MultiGet). (Excessive time was spent in SliceTransform::AsString().)
* Fixed a race condition in SstFileManagerImpl error recovery code that can cause a crash during process shutdown.

### New Features
* Added RocksJava support for MacOS universal binary (ARM+x86)

## 6.28.0 (2021-12-17)
### New Features
* Introduced 'CommitWithTimestamp' as a new tag. Currently, there is no API for user to trigger a write with this tag to the WAL. This is part of the efforts to support write-commited transactions with user-defined timestamps.
* Introduce SimulatedHybridFileSystem which can help simulating HDD latency in db_bench. Tiered Storage latency simulation can be enabled using -simulate_hybrid_fs_file (note that it doesn't work if db_bench is interrupted in the middle). -simulate_hdd can also be used to simulate all files on HDD.

### Bug Fixes
* Fixed a bug in rocksdb automatic implicit prefetching which got broken because of new feature adaptive_readahead and internal prefetching got disabled when iterator moves from one file to next.
* Fixed a bug in TableOptions.prepopulate_block_cache which causes segmentation fault when used with TableOptions.partition_filters = true and TableOptions.cache_index_and_filter_blocks = true.
* Fixed a bug affecting custom memtable factories which are not registered with the `ObjectRegistry`. The bug could result in failure to save the OPTIONS file.
* Fixed a bug causing two duplicate entries to be appended to a file opened in non-direct mode and tracked by `FaultInjectionTestFS`.
* Fixed a bug in TableOptions.prepopulate_block_cache to support block-based filters also.
* Block cache keys no longer use `FSRandomAccessFile::GetUniqueId()` (previously used when available), so a filesystem recycling unique ids can no longer lead to incorrect result or crash (#7405). For files generated by RocksDB >= 6.24, the cache keys are stable across DB::Open and DB directory move / copy / import / export / migration, etc. Although collisions are still theoretically possible, they are (a) impossible in many common cases, (b) not dependent on environmental factors, and (c) much less likely than a CPU miscalculation while executing RocksDB.
* Fixed a bug in C bindings causing iterator to return incorrect result (#9343).

### Behavior Changes
* MemTableList::TrimHistory now use allocated bytes when max_write_buffer_size_to_maintain > 0(default in TrasactionDB, introduced in PR#5022) Fix #8371.

### Public API change
* Extend WriteBatch::AssignTimestamp and AssignTimestamps API so that both functions can accept an optional `checker` argument that performs additional checking on timestamp sizes.
* Introduce a new EventListener callback that will be called upon the end of automatic error recovery.
* Add IncreaseFullHistoryTsLow API so users can advance each column family's full_history_ts_low seperately.
* Add GetFullHistoryTsLow API so users can query current full_history_low value of specified column family.

### Performance Improvements
* Replaced map property `TableProperties::properties_offsets`  with uint64_t property `external_sst_file_global_seqno_offset` to save table properties's memory.
* Block cache accesses are faster by RocksDB using cache keys of fixed size (16 bytes).

### Java API Changes
* Removed Java API `TableProperties.getPropertiesOffsets()` as it exposed internal details to external users.

## 6.27.0 (2021-11-19)
### New Features
* Added new ChecksumType kXXH3 which is faster than kCRC32c on almost all x86\_64 hardware.
* Added a new online consistency check for BlobDB which validates that the number/total size of garbage blobs does not exceed the number/total size of all blobs in any given blob file.
* Provided support for tracking per-sst user-defined timestamp information in MANIFEST.
* Added new option "adaptive_readahead" in ReadOptions. For iterators, RocksDB does auto-readahead on noticing sequential reads and by enabling this option, readahead_size of current file (if reads are sequential) will be carried forward to next file instead of starting from the scratch at each level (except L0 level files). If reads are not sequential it will fall back to 8KB. This option is applicable only for RocksDB internal prefetch buffer and isn't supported with underlying file system prefetching.
* Added the read count and read bytes related stats to Statistics for tiered storage hot, warm, and cold file reads.
* Added an option to dynamically charge an updating estimated memory usage of block-based table building to block cache if block cache available. It currently only includes charging memory usage of constructing (new) Bloom Filter and Ribbon Filter to block cache. To enable this feature, set `BlockBasedTableOptions::reserve_table_builder_memory = true`.
* Add a new API OnIOError in listener.h that notifies listeners when an IO error occurs during FileSystem operation along with filename, status etc.
* Added compaction readahead support for blob files to the integrated BlobDB implementation, which can improve compaction performance when the database resides on higher-latency storage like HDDs or remote filesystems. Readahead can be configured using the column family option `blob_compaction_readahead_size`.

### Bug Fixes
* Prevent a `CompactRange()` with `CompactRangeOptions::change_level == true` from possibly causing corruption to the LSM state (overlapping files within a level) when run in parallel with another manual compaction. Note that setting `force_consistency_checks == true` (the default) would cause the DB to enter read-only mode in this scenario and return `Status::Corruption`, rather than committing any corruption.
* Fixed a bug in CompactionIterator when write-prepared transaction is used. A released earliest write conflict snapshot may cause assertion failure in dbg mode and unexpected key in opt mode.
* Fix ticker WRITE_WITH_WAL("rocksdb.write.wal"), this bug is caused by a bad extra `RecordTick(stats_, WRITE_WITH_WAL)` (at 2 place), this fix remove the extra `RecordTick`s and fix the corresponding test case.
* EventListener::OnTableFileCreated was previously called with OK status and file_size==0 in cases of no SST file contents written (because there was no content to add) and the empty file deleted before calling the listener. Now the status is Aborted.
* Fixed a bug in CompactionIterator when write-preared transaction is used. Releasing earliest_snapshot during compaction may cause a SingleDelete to be output after a PUT of the same user key whose seq has been zeroed.
* Added input sanitization on negative bytes passed into `GenericRateLimiter::Request`.
* Fixed an assertion failure in CompactionIterator when write-prepared transaction is used. We prove that certain operations can lead to a Delete being followed by a SingleDelete (same user key). We can drop the SingleDelete.
* Fixed a bug of timestamp-based GC which can cause all versions of a key under full_history_ts_low to be dropped. This bug will be triggered when some of the ikeys' timestamps are lower than full_history_ts_low, while others are newer.
* In some cases outside of the DB read and compaction paths, SST block checksums are now checked where they were not before.
* Explicitly check for and disallow the `BlockBasedTableOptions` if insertion into one of {`block_cache`, `block_cache_compressed`, `persistent_cache`} can show up in another of these. (RocksDB expects to be able to use the same key for different physical data among tiers.)
* Users who configured a dedicated thread pool for bottommost compactions by explicitly adding threads to the `Env::Priority::BOTTOM` pool will no longer see RocksDB schedule automatic compactions exceeding the DB's compaction concurrency limit. For details on per-DB compaction concurrency limit, see API docs of `max_background_compactions` and `max_background_jobs`.
* Fixed a bug of background flush thread picking more memtables to flush and prematurely advancing column family's log_number.
* Fixed an assertion failure in ManifestTailer.
* Fixed a bug that could, with WAL enabled, cause backups, checkpoints, and `GetSortedWalFiles()` to fail randomly with an error like `IO error: 001234.log: No such file or directory`

### Behavior Changes
* `NUM_FILES_IN_SINGLE_COMPACTION` was only counting the first input level files, now it's including all input files.
* `TransactionUtil::CheckKeyForConflicts` can also perform conflict-checking based on user-defined timestamps in addition to sequence numbers.
* Removed `GenericRateLimiter`'s minimum refill bytes per period previously enforced.

### Public API change
* When options.ttl is used with leveled compaction with compactinon priority kMinOverlappingRatio, files exceeding half of TTL value will be prioritized more, so that by the time TTL is reached, fewer extra compactions will be scheduled to clear them up. At the same time, when compacting files with data older than half of TTL, output files may be cut off based on those files' boundaries, in order for the early TTL compaction to work properly.
* Made FileSystem and RateLimiter extend the Customizable class and added a CreateFromString method.  Implementations need to be registered with the ObjectRegistry and to implement a Name() method in order to be created via this method.
* Clarified in API comments that RocksDB is not exception safe for callbacks and custom extensions. An exception propagating into RocksDB can lead to undefined behavior, including data loss, unreported corruption, deadlocks, and more.
* Marked `WriteBufferManager` as `final` because it is not intended for extension.
* Removed unimportant implementation details from table_properties.h
* Add API `FSDirectory::FsyncWithDirOptions()`, which provides extra information like directory fsync reason in `DirFsyncOptions`. File system like btrfs is using that to skip directory fsync for creating a new file, or when renaming a file, fsync the target file instead of the directory, which improves the `DB::Open()` speed by ~20%.
* `DB::Open()` is not going be blocked by obsolete file purge if `DBOptions::avoid_unnecessary_blocking_io` is set to true.
* In builds where glibc provides `gettid()`, info log ("LOG" file) lines now print a system-wide thread ID from `gettid()` instead of the process-local `pthread_self()`. For all users, the thread ID format is changed from hexadecimal to decimal integer.
* In builds where glibc provides `pthread_setname_np()`, the background thread names no longer contain an ID suffix. For example, "rocksdb:bottom7" (and all other threads in the `Env::Priority::BOTTOM` pool) are now named "rocksdb:bottom". Previously large thread pools could breach the name size limit (e.g., naming "rocksdb:bottom10" would fail).
* Deprecating `ReadOptions::iter_start_seqnum` and `DBOptions::preserve_deletes`, please try using user defined timestamp feature instead. The options will be removed in a future release, currently it logs a warning message when using.

### Performance Improvements
* Released some memory related to filter construction earlier in `BlockBasedTableBuilder` for `FullFilter` and `PartitionedFilter` case (#9070)

### Behavior Changes
* `NUM_FILES_IN_SINGLE_COMPACTION` was only counting the first input level files, now it's including all input files.

## 6.26.0 (2021-10-20)
### Bug Fixes
* Fixes a bug in directed IO mode when calling MultiGet() for blobs in the same blob file. The bug is caused by not sorting the blob read requests by file offsets.
* Fix the incorrect disabling of SST rate limited deletion when the WAL and DB are in different directories. Only WAL rate limited deletion should be disabled if its in a different directory.
* Fix `DisableManualCompaction()` to cancel compactions even when they are waiting on automatic compactions to drain due to `CompactRangeOptions::exclusive_manual_compactions == true`.
* Fix contract of `Env::ReopenWritableFile()` and `FileSystem::ReopenWritableFile()` to specify any existing file must not be deleted or truncated.
* Fixed bug in calls to `IngestExternalFiles()` with files for multiple column families. The bug could have introduced a delay in ingested file keys becoming visible after `IngestExternalFiles()` returned. Furthermore, mutations to ingested file keys while they were invisible could have been dropped (not necessarily immediately).
* Fixed a possible race condition impacting users of `WriteBufferManager` who constructed it with `allow_stall == true`. The race condition led to undefined behavior (in our experience, typically a process crash).
* Fixed a bug where stalled writes would remain stalled forever after the user calls `WriteBufferManager::SetBufferSize()` with `new_size == 0` to dynamically disable memory limiting.
* Make `DB::close()` thread-safe.
* Fix a bug in atomic flush where one bg flush thread will wait forever for a preceding bg flush thread to commit its result to MANIFEST but encounters an error which is mapped to a soft error (DB not stopped).
* Fix a bug in `BackupEngine` where some internal callers of `GenericRateLimiter::Request()` do not honor `bytes <= GetSingleBurstBytes()`.

### New Features
* Print information about blob files when using "ldb list_live_files_metadata"
* Provided support for SingleDelete with user defined timestamp.
* Experimental new function DB::GetLiveFilesStorageInfo offers essentially a unified version of other functions like GetLiveFiles, GetLiveFilesChecksumInfo, and GetSortedWalFiles. Checkpoints and backups could show small behavioral changes and/or improved performance as they now use this new API.
* Add remote compaction read/write bytes statistics: `REMOTE_COMPACT_READ_BYTES`, `REMOTE_COMPACT_WRITE_BYTES`.
* Introduce an experimental feature to dump out the blocks from block cache and insert them to the secondary cache to reduce the cache warmup time (e.g., used while migrating DB instance). More information are in `class CacheDumper` and `CacheDumpedLoader` at `rocksdb/utilities/cache_dump_load.h` Note that, this feature is subject to the potential change in the future, it is still experimental.
* Introduced a new BlobDB configuration option `blob_garbage_collection_force_threshold`, which can be used to trigger compactions targeting the SST files which reference the oldest blob files when the ratio of garbage in those blob files meets or exceeds the specified threshold. This can reduce space amplification with skewed workloads where the affected SST files might not otherwise get picked up for compaction.
* Added EXPERIMENTAL support for table file (SST) unique identifiers that are stable and universally unique, available with new function `GetUniqueIdFromTableProperties`. Only SST files from RocksDB >= 6.24 support unique IDs.
* Added `GetMapProperty()` support for "rocksdb.dbstats" (`DB::Properties::kDBStats`). As a map property, it includes DB-level internal stats accumulated over the DB's lifetime, such as user write related stats and uptime.

### Public API change
* Made SystemClock extend the Customizable class and added a CreateFromString method.  Implementations need to be registered with the ObjectRegistry and to implement a Name() method in order to be created via this method.
* Made SliceTransform extend the Customizable class and added a CreateFromString method.  Implementations need to be registered with the ObjectRegistry and to implement a Name() method in order to be created via this method.  The Capped and Prefixed transform classes return a short name (no length); use GetId for the fully qualified name.
* Made FileChecksumGenFactory, SstPartitionerFactory, TablePropertiesCollectorFactory, and WalFilter extend the Customizable class and added a CreateFromString method.
* Some fields of SstFileMetaData are deprecated for compatibility with new base class FileStorageInfo.
* Add `file_temperature` to `IngestExternalFileArg` such that when ingesting SST files, we are able to indicate the temperature of the this batch of files.
* If `DB::Close()` failed with a non aborted status, calling `DB::Close()` again will return the original status instead of Status::OK.
* Add CacheTier to advanced_options.h to describe the cache tier we used. Add a `lowest_used_cache_tier` option to `DBOptions` (immutable) and pass it to BlockBasedTableReader. By default it is `CacheTier::kNonVolatileBlockTier`, which means, we always use both block cache (kVolatileTier) and secondary cache (kNonVolatileBlockTier). By set it to `CacheTier::kVolatileTier`, the DB will not use the secondary cache.
* Even when options.max_compaction_bytes is hit, compaction output files are only cut when it aligns with grandparent files' boundaries. options.max_compaction_bytes could be slightly violated with the change, but the violation is no more than one target SST file size, which is usually much smaller.

### Performance Improvements
* Improved CPU efficiency of building block-based table (SST) files (#9039 and #9040).

### Java API Changes
* Add Java API bindings for new integrated BlobDB options
* `keyMayExist()` supports ByteBuffer.
* Fix multiget throwing Null Pointer Exception for num of keys > 70k (https://github.com/facebook/rocksdb/issues/8039).

## 6.25.0 (2021-09-20)
### Bug Fixes
* Allow secondary instance to refresh iterator. Assign read seq after referencing SuperVersion.
* Fixed a bug of secondary instance's last_sequence going backward, and reads on the secondary fail to see recent updates from the primary.
* Fixed a bug that could lead to duplicate DB ID or DB session ID in POSIX environments without /proc/sys/kernel/random/uuid.
* Fix a race in DumpStats() with column family destruction due to not taking a Ref on each entry while iterating the ColumnFamilySet.
* Fix a race in item ref counting in LRUCache when promoting an item from the SecondaryCache.
* Fix a race in BackupEngine if RateLimiter is reconfigured during concurrent Restore operations.
* Fix a bug on POSIX in which failure to create a lock file (e.g. out of space) can prevent future LockFile attempts in the same process on the same file from succeeding.
* Fix a bug that backup_rate_limiter and restore_rate_limiter in BackupEngine could not limit read rates.
* Fix the implementation of `prepopulate_block_cache = kFlushOnly` to only apply to flushes rather than to all generated files.
* Fix WAL log data corruption when using DBOptions.manual_wal_flush(true) and WriteOptions.sync(true) together. The sync WAL should work with locked log_write_mutex_.
* Add checks for validity of the IO uring completion queue entries, and fail the BlockBasedTableReader MultiGet sub-batch if there's an invalid completion
* Add an interface RocksDbIOUringEnable() that, if defined by the user, will allow them to enable/disable the use of IO uring by RocksDB
* Fix the bug that when direct I/O is used and MultiRead() returns a short result, RandomAccessFileReader::MultiRead() still returns full size buffer, with returned short value together with some data in original buffer. This bug is unlikely cause incorrect results, because (1) since FileSystem layer is expected to retry on short result, returning short results is only possible when asking more bytes in the end of the file, which RocksDB doesn't do when using MultiRead(); (2) checksum is unlikely to match.

### New Features
* RemoteCompaction's interface now includes `db_name`, `db_id`, `session_id`, which could help the user uniquely identify compaction job between db instances and sessions.
* Added a ticker statistic, "rocksdb.verify_checksum.read.bytes", reporting how many bytes were read from file to serve `VerifyChecksum()` and `VerifyFileChecksums()` queries.
* Added ticker statistics, "rocksdb.backup.read.bytes" and "rocksdb.backup.write.bytes", reporting how many bytes were read and written during backup.
* Added properties for BlobDB: `rocksdb.num-blob-files`, `rocksdb.blob-stats`, `rocksdb.total-blob-file-size`, and `rocksdb.live-blob-file-size`. The existing property `rocksdb.estimate_live-data-size` was also extended to include live bytes residing in blob files.
* Added two new RateLimiter IOPriorities: `Env::IO_USER`,`Env::IO_MID`. `Env::IO_USER` will have superior priority over all other RateLimiter IOPriorities without being subject to fair scheduling constraint.
* `SstFileWriter` now supports `Put`s and `Delete`s with user-defined timestamps. Note that the ingestion logic itself is not timestamp-aware yet.
* Allow a single write batch to include keys from multiple column families whose timestamps' formats can differ. For example, some column families may disable timestamp, while others enable timestamp.
* Add compaction priority information in RemoteCompaction, which can be used to schedule high priority job first.
* Added new callback APIs `OnBlobFileCreationStarted`,`OnBlobFileCreated`and `OnBlobFileDeleted` in `EventListener` class of listener.h. It notifies listeners during creation/deletion of individual blob files in Integrated BlobDB. It also log blob file creation finished event and deletion event in LOG file.
* Batch blob read requests for `DB::MultiGet` using `MultiRead`.
* Add support for fallback to local compaction, the user can return `CompactionServiceJobStatus::kUseLocal` to instruct RocksDB to run the compaction locally instead of waiting for the remote compaction result.
* Add built-in rate limiter's implementation of `RateLimiter::GetTotalPendingRequest(int64_t* total_pending_requests, const Env::IOPriority pri)` for the total number of requests that are pending for bytes in the rate limiter.
* Charge memory usage during data buffering, from which training samples are gathered for dictionary compression, to block cache. Unbuffering data can now be triggered if the block cache becomes full and `strict_capacity_limit=true` for the block cache, in addition to existing conditions that can trigger unbuffering.

### Public API change
* Remove obsolete implementation details FullKey and ParseFullKey from public API
* Change `SstFileMetaData::size` from `size_t` to `uint64_t`.
* Made Statistics extend the Customizable class and added a CreateFromString method.  Implementations of Statistics need to be registered with the ObjectRegistry and to implement a Name() method in order to be created via this method.
* Extended `FlushJobInfo` and `CompactionJobInfo` in listener.h to provide information about the blob files generated by a flush/compaction and garbage collected during compaction in Integrated BlobDB. Added struct members `blob_file_addition_infos` and `blob_file_garbage_infos` that contain this information.
* Extended parameter `output_file_names` of `CompactFiles` API to also include paths of the blob files generated by the compaction in Integrated BlobDB.
* Most `BackupEngine` functions now return `IOStatus` instead of `Status`. Most existing code should be compatible with this change but some calls might need to be updated.
* Add a new field `level_at_creation` in `TablePropertiesCollectorFactory::Context` to capture the level at creating the SST file (i.e, table), of which the properties are being collected.

### Miscellaneous
* Add a paranoid check where in case FileSystem layer doesn't fill the buffer but returns succeed, checksum is unlikely to match even if buffer contains a previous block. The byte modified is not useful anyway, so it isn't expected to change any behavior when FileSystem is satisfying its contract.

## 6.24.0 (2021-08-20)
### Bug Fixes
* If the primary's CURRENT file is missing or inaccessible, the secondary instance should not hang repeatedly trying to switch to a new MANIFEST. It should instead return the error code encountered while accessing the file.
* Restoring backups with BackupEngine is now a logically atomic operation, so that if a restore operation is interrupted, DB::Open on it will fail. Using BackupEngineOptions::sync (default) ensures atomicity even in case of power loss or OS crash.
* Fixed a race related to the destruction of `ColumnFamilyData` objects. The earlier logic unlocked the DB mutex before destroying the thread-local `SuperVersion` pointers, which could result in a process crash if another thread managed to get a reference to the `ColumnFamilyData` object.
* Removed a call to `RenameFile()` on a non-existent info log file ("LOG") when opening a new DB. Such a call was guaranteed to fail though did not impact applications since we swallowed the error. Now we also stopped swallowing errors in renaming "LOG" file.
* Fixed an issue where `OnFlushCompleted` was not called for atomic flush.
* Fixed a bug affecting the batched `MultiGet` API when used with keys spanning multiple column families and `sorted_input == false`.
* Fixed a potential incorrect result in opt mode and assertion failures caused by releasing snapshot(s) during compaction.
* Fixed passing of BlobFileCompletionCallback to Compaction job and Atomic flush job which was default paramter (nullptr). BlobFileCompletitionCallback is internal callback that manages addition of blob files to SSTFileManager.
* Fixed MultiGet not updating the block_read_count and block_read_byte PerfContext counters.

### New Features
* Made the EventListener extend the Customizable class.
* EventListeners that have a non-empty Name() and that are registered with the ObjectRegistry can now be serialized to/from the OPTIONS file.
* Insert warm blocks (data blocks, uncompressed dict blocks, index and filter blocks) in Block cache during flush under option BlockBasedTableOptions.prepopulate_block_cache. Previously it was enabled for only data blocks.
* BlockBasedTableOptions.prepopulate_block_cache can be dynamically configured using DB::SetOptions.
* Add CompactionOptionsFIFO.age_for_warm, which allows RocksDB to move old files to warm tier in FIFO compactions. Note that file temperature is still an experimental feature.
* Add a comment to suggest btrfs user to disable file preallocation by setting `options.allow_fallocate=false`.
* Fast forward option in Trace replay changed to double type to allow replaying at a lower speed, by settings the value between 0 and 1. This option can be set via `ReplayOptions` in `Replayer::Replay()`, or via `--trace_replay_fast_forward` in db_bench.
* Add property `LiveSstFilesSizeAtTemperature` to retrieve sst file size at different temperature.
* Added a stat rocksdb.secondary.cache.hits.
* Added a PerfContext counter secondary_cache_hit_count.
* The integrated BlobDB implementation now supports the tickers `BLOB_DB_BLOB_FILE_BYTES_READ`, `BLOB_DB_GC_NUM_KEYS_RELOCATED`, and `BLOB_DB_GC_BYTES_RELOCATED`, as well as the histograms `BLOB_DB_COMPRESSION_MICROS` and `BLOB_DB_DECOMPRESSION_MICROS`.
* Added hybrid configuration of Ribbon filter and Bloom filter where some LSM levels use Ribbon for memory space efficiency and some use Bloom for speed. See NewRibbonFilterPolicy. This also changes the default behavior of NewRibbonFilterPolicy to use Bloom for flushes under Leveled and Universal compaction and Ribbon otherwise. The C API function `rocksdb_filterpolicy_create_ribbon` is unchanged but adds new `rocksdb_filterpolicy_create_ribbon_hybrid`.

### Public API change
* Added APIs to decode and replay trace file via Replayer class. Added `DB::NewDefaultReplayer()` to create a default Replayer instance. Added `TraceReader::Reset()` to restart reading a trace file. Created trace_record.h, trace_record_result.h and utilities/replayer.h files to access the decoded Trace records, replay them, and query the actual operation results.
* Added Configurable::GetOptionsMap to the public API for use in creating new Customizable classes.
* Generalized bits_per_key parameters in C API from int to double for greater configurability. Although this is a compatible change for existing C source code, anything depending on C API signatures, such as foreign function interfaces, will need to be updated.

### Performance Improvements
* Try to avoid updating DBOptions if `SetDBOptions()` does not change any option value.

### Behavior Changes
* `StringAppendOperator` additionally accepts a string as the delimiter.
* BackupEngineOptions::sync (default true) now applies to restoring backups in addition to creating backups. This could slow down restores, but ensures they are fully persisted before returning OK. (Consider increasing max_background_operations to improve performance.)

## 6.23.0 (2021-07-16)
### Behavior Changes
* Obsolete keys in the bottommost level that were preserved for a snapshot will now be cleaned upon snapshot release in all cases. This form of compaction (snapshot release triggered compaction) previously had an artificial limitation that multiple tombstones needed to be present.
### Bug Fixes
* Blob file checksums are now printed in hexadecimal format when using the `manifest_dump` `ldb` command.
* `GetLiveFilesMetaData()` now populates the `temperature`, `oldest_ancester_time`, and `file_creation_time` fields of its `LiveFileMetaData` results when the information is available. Previously these fields always contained zero indicating unknown.
* Fix mismatches of OnCompaction{Begin,Completed} in case of DisableManualCompaction().
* Fix continuous logging of an existing background error on every user write
* Fix a bug that `Get()` return Status::OK() and an empty value for non-existent key when `read_options.read_tier = kBlockCacheTier`.
* Fix a bug that stat in `get_context` didn't accumulate to statistics when query is failed.
* Fixed handling of DBOptions::wal_dir with LoadLatestOptions() or ldb --try_load_options on a copied or moved DB. Previously, when the WAL directory is same as DB directory (default), a copied or moved DB would reference the old path of the DB as the WAL directory, potentially corrupting both copies. Under this change, the wal_dir from DB::GetOptions() or LoadLatestOptions() may now be empty, indicating that the current DB directory is used for WALs. This is also a subtle API change.

### New Features
* ldb has a new feature, `list_live_files_metadata`, that shows the live SST files, as well as their LSM storage level and the column family they belong to.
* The new BlobDB implementation now tracks the amount of garbage in each blob file in the MANIFEST.
* Integrated BlobDB now supports Merge with base values (Put/Delete etc.).
* RemoteCompaction supports sub-compaction, the job_id in the user interface is changed from `int` to `uint64_t` to support sub-compaction id.
* Expose statistics option in RemoteCompaction worker.

### Public API change
* Added APIs to the Customizable class to allow developers to create their own Customizable classes.  Created the utilities/customizable_util.h file to contain helper methods for developing new Customizable classes.
* Change signature of SecondaryCache::Name().  Make SecondaryCache customizable and add SecondaryCache::CreateFromString method.

## 6.22.0 (2021-06-18)
### Behavior Changes
* Added two additional tickers, MEMTABLE_PAYLOAD_BYTES_AT_FLUSH and MEMTABLE_GARBAGE_BYTES_AT_FLUSH. These stats can be used to estimate the ratio of "garbage" (outdated) bytes in the memtable that are discarded at flush time.
* Added API comments clarifying safe usage of Disable/EnableManualCompaction and EventListener callbacks for compaction.
### Bug Fixes
* fs_posix.cc GetFreeSpace() always report disk space available to root even when running as non-root.  Linux defaults often have disk mounts with 5 to 10 percent of total space reserved only for root.  Out of space could result for non-root users.
* Subcompactions are now disabled when user-defined timestamps are used, since the subcompaction boundary picking logic is currently not timestamp-aware, which could lead to incorrect results when different subcompactions process keys that only differ by timestamp.
* Fix an issue that `DeleteFilesInRange()` may cause ongoing compaction reports corruption exception, or ASSERT for debug build. There's no actual data loss or corruption that we find.
* Fixed confusingly duplicated output in LOG for periodic stats ("DUMPING STATS"), including "Compaction Stats" and "File Read Latency Histogram By Level".
* Fixed performance bugs in background gathering of block cache entry statistics, that could consume a lot of CPU when there are many column families with a shared block cache.

### New Features
* Marked the Ribbon filter and optimize_filters_for_memory features as production-ready, each enabling memory savings for Bloom-like filters. Use `NewRibbonFilterPolicy` in place of `NewBloomFilterPolicy` to use Ribbon filters instead of Bloom, or `ribbonfilter` in place of `bloomfilter` in configuration string.
* Allow `DBWithTTL` to use `DeleteRange` api just like other DBs. `DeleteRangeCF()` which executes `WriteBatchInternal::DeleteRange()` has been added to the handler in `DBWithTTLImpl::Write()` to implement it.
* Add BlockBasedTableOptions.prepopulate_block_cache.  If enabled, it prepopulate warm/hot data blocks which are already in memory into block cache at the time of flush. On a flush, the data block that is in memory (in memtables) get flushed to the device. If using Direct IO, additional IO is incurred to read this data back into memory again, which is avoided by enabling this option and it also helps with Distributed FileSystem. More details in include/rocksdb/table.h.
* Added a `cancel` field to `CompactRangeOptions`, allowing individual in-process manual range compactions to be cancelled.

### New Features
* Added BlobMetaData to the ColumnFamilyMetaData to return information about blob files

### Public API change
* Added GetAllColumnFamilyMetaData API to retrieve the ColumnFamilyMetaData about all column families.

## 6.21.0 (2021-05-21)
### Bug Fixes
* Fixed a bug in handling file rename error in distributed/network file systems when the server succeeds but client returns error. The bug can cause CURRENT file to point to non-existing MANIFEST file, thus DB cannot be opened.
* Fixed a bug where ingested files were written with incorrect boundary key metadata. In rare cases this could have led to a level's files being wrongly ordered and queries for the boundary keys returning wrong results.
* Fixed a data race between insertion into memtables and the retrieval of the DB properties `rocksdb.cur-size-active-mem-table`, `rocksdb.cur-size-all-mem-tables`, and `rocksdb.size-all-mem-tables`.
* Fixed the false-positive alert when recovering from the WAL file. Avoid reporting "SST file is ahead of WAL" on a newly created empty column family, if the previous WAL file is corrupted.
* Fixed a bug where `GetLiveFiles()` output included a non-existent file called "OPTIONS-000000". Backups and checkpoints, which use `GetLiveFiles()`, failed on DBs impacted by this bug. Read-write DBs were impacted when the latest OPTIONS file failed to write and `fail_if_options_file_error == false`. Read-only DBs were impacted when no OPTIONS files existed.
* Handle return code by io_uring_submit_and_wait() and io_uring_wait_cqe().
* In the IngestExternalFile() API, only try to sync the ingested file if the file is linked and the FileSystem/Env supports reopening a writable file.
* Fixed a bug that `AdvancedColumnFamilyOptions.max_compaction_bytes` is under-calculated for manual compaction (`CompactRange()`). Manual compaction is split to multiple compactions if the compaction size exceed the `max_compaction_bytes`. The bug creates much larger compaction which size exceed the user setting. On the other hand, larger manual compaction size can increase the subcompaction parallelism, you can tune that by setting `max_compaction_bytes`.

### Behavior Changes
* Due to the fix of false-postive alert of "SST file is ahead of WAL", all the CFs with no SST file (CF empty) will bypass the consistency check. We fixed a false-positive, but introduced a very rare true-negative which will be triggered in the following conditions: A CF with some delete operations in the last a few queries which will result in an empty CF (those are flushed to SST file and a compaction triggered which combines this file and all other SST files and generates an empty CF, or there is another reason to write a manifest entry for this CF after a flush that generates no SST file from an empty CF). The deletion entries are logged in a WAL and this WAL was corrupted, while the CF's log number points to the next WAL (due to the flush). Therefore, the DB can only recover to the point without these trailing deletions and cause the inconsistent DB status.

### New Features
* Add new option allow_stall passed during instance creation of WriteBufferManager. When allow_stall is set, WriteBufferManager will stall all writers shared across multiple DBs and columns if memory usage goes beyond specified WriteBufferManager::buffer_size (soft limit). Stall will be cleared when memory is freed after flush and memory usage goes down below buffer_size.
* Allow `CompactionFilter`s to apply in more table file creation scenarios such as flush and recovery. For compatibility, `CompactionFilter`s by default apply during compaction. Users can customize this behavior by overriding `CompactionFilterFactory::ShouldFilterTableFileCreation()`.
* Added more fields to FilterBuildingContext with LSM details, for custom filter policies that vary behavior based on where they are in the LSM-tree.
* Added DB::Properties::kBlockCacheEntryStats for querying statistics on what percentage of block cache is used by various kinds of blocks, etc. using DB::GetProperty and DB::GetMapProperty. The same information is now dumped to info LOG periodically according to `stats_dump_period_sec`.
* Add an experimental Remote Compaction feature, which allows the user to run Compaction on a different host or process. The feature is still under development, currently only works on some basic use cases. The interface will be changed without backward/forward compatibility support.
* RocksDB would validate total entries read in flush, and compare with counter inserted into it. If flush_verify_memtable_count = true (default), flush will fail. Otherwise, only log to info logs.
* Add `TableProperties::num_filter_entries`, which can be used with `TableProperties::filter_size` to calculate the effective bits per filter entry (unique user key or prefix) for a table file.

### Performance Improvements
* BlockPrefetcher is used by iterators to prefetch data if they anticipate more data to be used in future. It is enabled implicitly by rocksdb. Added change to take in account read pattern if reads are sequential. This would disable prefetching for random reads in MultiGet and iterators as readahead_size is increased exponential doing large prefetches.

### Public API change
* Removed a parameter from TableFactory::NewTableBuilder, which should not be called by user code because TableBuilder is not a public API.
* Removed unused structure `CompactionFilterContext`.
* The `skip_filters` parameter to SstFileWriter is now considered deprecated. Use `BlockBasedTableOptions::filter_policy` to control generation of filters.
* ClockCache is known to have bugs that could lead to crash or corruption, so should not be used until fixed. Use NewLRUCache instead.
* Added a new pure virtual function `ApplyToAllEntries` to `Cache`, to replace `ApplyToAllCacheEntries`. Custom `Cache` implementations must add an implementation. Because this function is for gathering statistics, an empty implementation could be acceptable for some applications.
* Added the ObjectRegistry to the ConfigOptions class.  This registry instance will be used to find any customizable loadable objects during initialization.
* Expanded the ObjectRegistry functionality to allow nested ObjectRegistry instances.  Added methods to register a set of functions with the registry/library as a group.
* Deprecated backupable_db.h and BackupableDBOptions in favor of new versions with appropriate names: backup_engine.h and BackupEngineOptions. Old API compatibility is preserved.

### Default Option Change
* When options.arena_block_size <= 0 (default value 0), still use writer_buffer_size / 8 but cap to 1MB. Too large alloation size might not be friendly to allocator and might cause performance issues in extreme cases.

### Build
* By default, try to build with liburing. For make, if ROCKSDB_USE_IO_URING is not set, treat as enable, which means RocksDB will try to build with liburing. Users can disable it with ROCKSDB_USE_IO_URING=0. For cmake, add WITH_LIBURING to control it, with default on.

## 6.20.0 (2021-04-16)
### Behavior Changes
* `ColumnFamilyOptions::sample_for_compression` now takes effect for creation of all block-based tables. Previously it only took effect for block-based tables created by flush.
* `CompactFiles()` can no longer compact files from lower level to up level, which has the risk to corrupt DB (details: #8063). The validation is also added to all compactions.
* Fixed some cases in which DB::OpenForReadOnly() could write to the filesystem. If you want a Logger with a read-only DB, you must now set DBOptions::info_log yourself, such as using CreateLoggerFromOptions().
* get_iostats_context() will never return nullptr. If thread-local support is not available, and user does not opt-out iostats context, then compilation will fail. The same applies to perf context as well.
* Added support for WriteBatchWithIndex::NewIteratorWithBase when overwrite_key=false.  Previously, this combination was not supported and would assert or return nullptr.
* Improve the behavior of WriteBatchWithIndex for Merge operations.  Now more operations may be stored in order to return the correct merged result.

### Bug Fixes
* Use thread-safe `strerror_r()` to get error messages.
* Fixed a potential hang in shutdown for a DB whose `Env` has high-pri thread pool disabled (`Env::GetBackgroundThreads(Env::Priority::HIGH) == 0`)
* Made BackupEngine thread-safe and added documentation comments to clarify what is safe for multiple BackupEngine objects accessing the same backup directory.
* Fixed crash (divide by zero) when compression dictionary is applied to a file containing only range tombstones.
* Fixed a backward iteration bug with partitioned filter enabled: not including the prefix of the last key of the previous filter partition in current filter partition can cause wrong iteration result.
* Fixed a bug that allowed `DBOptions::max_open_files` to be set with a non-negative integer with `ColumnFamilyOptions::compaction_style = kCompactionStyleFIFO`.

### Performance Improvements
* On ARM platform, use `yield` instead of `wfe` to relax cpu to gain better performance.

### Public API change
* Added `TableProperties::slow_compression_estimated_data_size` and `TableProperties::fast_compression_estimated_data_size`. When `ColumnFamilyOptions::sample_for_compression > 0`, they estimate what `TableProperties::data_size` would have been if the "fast" or "slow" (see `ColumnFamilyOptions::sample_for_compression` API doc for definitions) compression had been used instead.
* Update DB::StartIOTrace and remove Env object from the arguments as its redundant and DB already has Env object that is passed down to IOTracer::StartIOTrace
* Added `FlushReason::kWalFull`, which is reported when a memtable is flushed due to the WAL reaching its size limit; those flushes were previously reported as `FlushReason::kWriteBufferManager`. Also, changed the reason for flushes triggered by the write buffer manager to `FlushReason::kWriteBufferManager`; they were previously reported as `FlushReason::kWriteBufferFull`.
* Extend file_checksum_dump ldb command and DB::GetLiveFilesChecksumInfo API for IntegratedBlobDB and get checksum of blob files along with SST files.

### New Features
* Added the ability to open BackupEngine backups as read-only DBs, using BackupInfo::name_for_open and env_for_open provided by BackupEngine::GetBackupInfo() with include_file_details=true.
* Added BackupEngine support for integrated BlobDB, with blob files shared between backups when table files are shared. Because of current limitations, blob files always use the kLegacyCrc32cAndFileSize naming scheme, and incremental backups must read and checksum all blob files in a DB, even for files that are already backed up.
* Added an optional output parameter to BackupEngine::CreateNewBackup(WithMetadata) to return the BackupID of the new backup.
* Added BackupEngine::GetBackupInfo / GetLatestBackupInfo for querying individual backups.
* Made the Ribbon filter a long-term supported feature in terms of the SST schema(compatible with version >= 6.15.0) though the API for enabling it is expected to change.

## 6.19.0 (2021-03-21)
### Bug Fixes
* Fixed the truncation error found in APIs/tools when dumping block-based SST files in a human-readable format. After fix, the block-based table can be fully dumped as a readable file.
* When hitting a write slowdown condition, no write delay (previously 1 millisecond) is imposed until `delayed_write_rate` is actually exceeded, with an initial burst allowance of 1 millisecond worth of bytes. Also, beyond the initial burst allowance, `delayed_write_rate` is now more strictly enforced, especially with multiple column families.

### Public API change
* Changed default `BackupableDBOptions::share_files_with_checksum` to `true` and deprecated `false` because of potential for data loss. Note that accepting this change in behavior can temporarily increase backup data usage because files are not shared between backups using the two different settings. Also removed obsolete option kFlagMatchInterimNaming.
* Add a new option BlockBasedTableOptions::max_auto_readahead_size. RocksDB does auto-readahead for iterators on noticing more than two reads for a table file if user doesn't provide readahead_size. The readahead starts at 8KB and doubles on every additional read upto max_auto_readahead_size and now max_auto_readahead_size can be configured dynamically as well. Found that 256 KB readahead size provides the best performance, based on experiments, for auto readahead. Experiment data is in PR #3282. If value is set 0 then no automatic prefetching will be done by rocksdb. Also changing the value will only affect files opened after the change.
* Add suppport to extend DB::VerifyFileChecksums API to also verify blob files checksum.
* When using the new BlobDB, the amount of data written by flushes/compactions is now broken down into table files and blob files in the compaction statistics; namely, Write(GB) denotes the amount of data written to table files, while Wblob(GB) means the amount of data written to blob files.
* New default BlockBasedTableOptions::format_version=5 to enable new Bloom filter implementation by default, compatible with RocksDB versions >= 6.6.0.
* Add new SetBufferSize API to WriteBufferManager to allow dynamic management of memory allotted to all write buffers.  This allows user code to adjust memory monitoring provided by WriteBufferManager as process memory needs change datasets grow and shrink.
* Clarified the required semantics of Read() functions in FileSystem and Env APIs. Please ensure any custom implementations are compliant.
* For the new integrated BlobDB implementation, compaction statistics now include the amount of data read from blob files during compaction (due to garbage collection or compaction filters). Write amplification metrics have also been extended to account for data read from blob files.
* Add EqualWithoutTimestamp() to Comparator.
* Extend support to track blob files in SSTFileManager whenever a blob file is created/deleted. Blob files will be scheduled to delete via SSTFileManager and SStFileManager will now take blob files in account while calculating size and space limits along with SST files.
* Add new Append and PositionedAppend API with checksum handoff to legacy Env.

### New Features
* Support compaction filters for the new implementation of BlobDB. Add `FilterBlobByKey()` to `CompactionFilter`. Subclasses can override this method so that compaction filters can determine whether the actual blob value has to be read during compaction. Use a new `kUndetermined` in `CompactionFilter::Decision` to indicated that further action is necessary for compaction filter to make a decision.
* Add support to extend retrieval of checksums for blob files from the MANIFEST when checkpointing. During backup, rocksdb can detect corruption in blob files  during file copies.
* Add new options for db_bench --benchmarks: flush, waitforcompaction, compact0, compact1.
* Add an option to BackupEngine::GetBackupInfo to include the name and size of each backed-up file. Especially in the presence of file sharing among backups, this offers detailed insight into backup space usage.
* Enable backward iteration on keys with user-defined timestamps.
* Add statistics and info log for error handler: counters for bg error, bg io error, bg retryable io error, auto resume count, auto resume total retry number, and auto resume sucess; Histogram for auto resume retry count in each recovery call. Note that, each auto resume attempt will have one or multiple retries.

### Behavior Changes
* During flush, only WAL sync retryable IO error is mapped to hard error, which will stall the writes. When WAL is used but only SST file write has retryable IO error, it will be mapped to soft error and write will not be affected.

## 6.18.0 (2021-02-19)
### Behavior Changes
* When retryable IO error occurs during compaction, it is mapped to soft error and set the BG error. However, auto resume is not called to clean the soft error since compaction will reschedule by itself. In this change, When retryable IO error occurs during compaction, BG error is not set. User will be informed the error via EventHelper.
* Introduce a new trace file format for query tracing and replay and trace file version is bump up to 0.2. A payload map is added as the first portion of the payload. We will not have backward compatible issues when adding new entries to trace records. Added the iterator_upper_bound and iterator_lower_bound in Seek and SeekForPrev tracing function. Added them as the new payload member for iterator tracing.

### New Features
* Add support for key-value integrity protection in live updates from the user buffers provided to `WriteBatch` through the write to RocksDB's in-memory update buffer (memtable). This is intended to detect some cases of in-memory data corruption, due to either software or hardware errors. Users can enable protection by constructing their `WriteBatch` with `protection_bytes_per_key == 8`.
* Add support for updating `full_history_ts_low` option in manual compaction, which is for old timestamp data GC.
* Add a mechanism for using Makefile to build external plugin code into the RocksDB libraries/binaries. This intends to simplify compatibility and distribution for plugins (e.g., special-purpose `FileSystem`s) whose source code resides outside the RocksDB repo. See "plugin/README.md" for developer details, and "PLUGINS.md" for a listing of available plugins.
* Added memory pre-fetching for experimental Ribbon filter, which especially optimizes performance with batched MultiGet.
* A new, experimental version of BlobDB (key-value separation) is now available. The new implementation is integrated into the RocksDB core, i.e. it is accessible via the usual `rocksdb::DB` API, as opposed to the separate `rocksdb::blob_db::BlobDB` interface used by the earlier version, and can be configured on a per-column family basis using the configuration options `enable_blob_files`, `min_blob_size`, `blob_file_size`, `blob_compression_type`, `enable_blob_garbage_collection`, and `blob_garbage_collection_age_cutoff`. It extends RocksDB's consistency guarantees to blobs, and offers more features and better performance. Note that some features, most notably `Merge`, compaction filters, and backup/restore are not yet supported, and there is no support for migrating a database created by the old implementation.

### Bug Fixes
* Since 6.15.0, `TransactionDB` returns error `Status`es from calls to `DeleteRange()` and calls to `Write()` where the `WriteBatch` contains a range deletion. Previously such operations may have succeeded while not providing the expected transactional guarantees. There are certain cases where range deletion can still be used on such DBs; see the API doc on `TransactionDB::DeleteRange()` for details.
* `OptimisticTransactionDB` now returns error `Status`es from calls to `DeleteRange()` and calls to `Write()` where the `WriteBatch` contains a range deletion. Previously such operations may have succeeded while not providing the expected transactional guarantees.
* Fix `WRITE_PREPARED`, `WRITE_UNPREPARED` TransactionDB `MultiGet()` may return uncommitted data with snapshot.
* In DB::OpenForReadOnly, if any error happens while checking Manifest file path, it was overridden by Status::NotFound. It has been fixed and now actual error is returned.

### Public API Change
* Added a "only_mutable_options" flag to the ConfigOptions.  When this flag is "true", the Configurable functions and convenience methods (such as GetDBOptionsFromString) will only deal with options that are marked as mutable.  When this flag is true, only options marked as mutable can be configured (a Status::InvalidArgument will be returned) and options not marked as mutable will not be returned or compared.  The default is "false", meaning to compare all options.
* Add new Append and PositionedAppend APIs to FileSystem to bring the data verification information (data checksum information) from upper layer (e.g., WritableFileWriter) to the storage layer. In this way, the customized FileSystem is able to verify the correctness of data being written to the storage on time. Add checksum_handoff_file_types to DBOptions. User can use this option to control which file types (Currently supported file tyes: kWALFile, kTableFile, kDescriptorFile.) should use the new Append and PositionedAppend APIs to handoff the verification information. Currently, RocksDB only use crc32c to calculate the checksum for write handoff.
* Add an option, `CompressionOptions::max_dict_buffer_bytes`, to limit the in-memory buffering for selecting samples for generating/training a dictionary. The limit is currently loosely adhered to.


## 6.17.0 (2021-01-15)
### Behavior Changes
* When verifying full file checksum with `DB::VerifyFileChecksums()`, we now fail with `Status::InvalidArgument` if the name of the checksum generator used for verification does not match the name of the checksum generator used for protecting the file when it was created.
* Since RocksDB does not continue write the same file if a file write fails for any reason, the file scope write IO error is treated the same as retryable IO error. More information about error handling of file scope IO error is included in `ErrorHandler::SetBGError`.

### Bug Fixes
* Version older than 6.15 cannot decode VersionEdits `WalAddition` and `WalDeletion`, fixed this by changing the encoded format of them to be ignorable by older versions.
* Fix a race condition between DB startups and shutdowns in managing the periodic background worker threads. One effect of this race condition could be the process being terminated.

### Public API Change
* Add a public API WriteBufferManager::dummy_entries_in_cache_usage() which reports the size of dummy entries stored in cache (passed to WriteBufferManager). Dummy entries are used to account for DataBlocks.
* Add a SystemClock class that contains the time-related methods from Env.  The original methods in Env may be deprecated in a future release.  This class will allow easier testing, development, and expansion of time-related features.
* Add a public API GetRocksBuildProperties and GetRocksBuildInfoAsString to get properties about the current build.  These properties may include settings related to the GIT settings (branch, timestamp).  This change also sets the "build date" based on the GIT properties, rather than the actual build time, thereby enabling more reproducible builds.

## 6.16.0 (2020-12-18)
### Behavior Changes
* Attempting to write a merge operand without explicitly configuring `merge_operator` now fails immediately, causing the DB to enter read-only mode. Previously, failure was deferred until the `merge_operator` was needed by a user read or a background operation.

### Bug Fixes
* Truncated WALs ending in incomplete records can no longer produce gaps in the recovered data when `WALRecoveryMode::kPointInTimeRecovery` is used. Gaps are still possible when WALs are truncated exactly on record boundaries; for complete protection, users should enable `track_and_verify_wals_in_manifest`.
* Fix a bug where compressed blocks read by MultiGet are not inserted into the compressed block cache when use_direct_reads = true.
* Fixed the issue of full scanning on obsolete files when there are too many outstanding compactions with ConcurrentTaskLimiter enabled.
* Fixed the logic of populating native data structure for `read_amp_bytes_per_bit` during OPTIONS file parsing on big-endian architecture. Without this fix, original code introduced in PR7659, when running on big-endian machine, can mistakenly store read_amp_bytes_per_bit (an uint32) in little endian format. Future access to `read_amp_bytes_per_bit` will give wrong values. Little endian architecture is not affected.
* Fixed prefix extractor with timestamp issues.
* Fixed a bug in atomic flush: in two-phase commit mode, the minimum WAL log number to keep is incorrect.
* Fixed a bug related to checkpoint in PR7789: if there are multiple column families, and the checkpoint is not opened as read only, then in rare cases, data loss may happen in the checkpoint. Since backup engine relies on checkpoint, it may also be affected.
* When ldb --try_load_options is used with the --column_family option, the ColumnFamilyOptions for the specified column family was not loaded from the OPTIONS file. Fix it so its loaded from OPTIONS and then overridden with command line overrides.

### New Features
* User defined timestamp feature supports `CompactRange` and `GetApproximateSizes`.
* Support getting aggregated table properties (kAggregatedTableProperties and kAggregatedTablePropertiesAtLevel) with DB::GetMapProperty, for easier access to the data in a structured format.
* Experimental option BlockBasedTableOptions::optimize_filters_for_memory now works with experimental Ribbon filter (as well as Bloom filter).

### Public API Change
* Deprecated public but rarely-used FilterBitsBuilder::CalculateNumEntry, which is replaced with ApproximateNumEntries taking a size_t parameter and returning size_t.
* To improve portability the functions `Env::GetChildren` and `Env::GetChildrenFileAttributes` will no longer return entries for the special directories `.` or `..`.
* Added a new option `track_and_verify_wals_in_manifest`. If `true`, the log numbers and sizes of the synced WALs are tracked in MANIFEST, then during DB recovery, if a synced WAL is missing from disk, or the WAL's size does not match the recorded size in MANIFEST, an error will be reported and the recovery will be aborted. Note that this option does not work with secondary instance.
* `rocksdb_approximate_sizes` and `rocksdb_approximate_sizes_cf` in the C API now requires an error pointer (`char** errptr`) for receiving any error.
* All overloads of DB::GetApproximateSizes now return Status, so that any failure to obtain the sizes is indicated to the caller.

## 6.15.0 (2020-11-13)
### Bug Fixes
* Fixed a bug in the following combination of features: indexes with user keys (`format_version >= 3`), indexes are partitioned (`index_type == kTwoLevelIndexSearch`), and some index partitions are pinned in memory (`BlockBasedTableOptions::pin_l0_filter_and_index_blocks_in_cache`). The bug could cause keys to be truncated when read from the index leading to wrong read results or other unexpected behavior.
* Fixed a bug when indexes are partitioned (`index_type == kTwoLevelIndexSearch`), some index partitions are pinned in memory (`BlockBasedTableOptions::pin_l0_filter_and_index_blocks_in_cache`), and partitions reads could be mixed between block cache and directly from the file (e.g., with `enable_index_compression == 1` and `mmap_read == 1`, partitions that were stored uncompressed due to poor compression ratio would be read directly from the file via mmap, while partitions that were stored compressed would be read from block cache). The bug could cause index partitions to be mistakenly considered empty during reads leading to wrong read results.
* Since 6.12, memtable lookup should report unrecognized value_type as corruption (#7121).
* Since 6.14, fix false positive flush/compaction `Status::Corruption` failure when `paranoid_file_checks == true` and range tombstones were written to the compaction output files.
* Since 6.14, fix a bug that could cause a stalled write to crash with mixed of slowdown and no_slowdown writes (`WriteOptions.no_slowdown=true`).
* Fixed a bug which causes hang in closing DB when refit level is set in opt build. It was because ContinueBackgroundWork() was called in assert statement which is a no op. It was introduced in 6.14.
* Fixed a bug which causes Get() to return incorrect result when a key's merge operand is applied twice. This can occur if the thread performing Get() runs concurrently with a background flush thread and another thread writing to the MANIFEST file (PR6069).
* Reverted a behavior change silently introduced in 6.14.2, in which the effects of the `ignore_unknown_options` flag (used in option parsing/loading functions) changed.
* Reverted a behavior change silently introduced in 6.14, in which options parsing/loading functions began returning `NotFound` instead of `InvalidArgument` for option names not available in the present version.
* Fixed MultiGet bugs it doesn't return valid data with user defined timestamp.
* Fixed a potential bug caused by evaluating `TableBuilder::NeedCompact()` before `TableBuilder::Finish()` in compaction job. For example, the `NeedCompact()` method of `CompactOnDeletionCollector` returned by built-in `CompactOnDeletionCollectorFactory` requires `BlockBasedTable::Finish()` to return the correct result. The bug can cause a compaction-generated file not to be marked for future compaction based on deletion ratio.
* Fixed a seek issue with prefix extractor and timestamp.
* Fixed a bug of encoding and parsing BlockBasedTableOptions::read_amp_bytes_per_bit as a 64-bit integer.
* Fixed a bug of a recovery corner case, details in PR7621.

### Public API Change
* Deprecate `BlockBasedTableOptions::pin_l0_filter_and_index_blocks_in_cache` and `BlockBasedTableOptions::pin_top_level_index_and_filter`. These options still take effect until users migrate to the replacement APIs in `BlockBasedTableOptions::metadata_cache_options`. Migration guidance can be found in the API comments on the deprecated options.
* Add new API `DB::VerifyFileChecksums` to verify SST file checksum with corresponding entries in the MANIFEST if present. Current implementation requires scanning and recomputing file checksums.

### Behavior Changes
* The dictionary compression settings specified in `ColumnFamilyOptions::compression_opts` now additionally affect files generated by flush and compaction to non-bottommost level. Previously those settings at most affected files generated by compaction to bottommost level, depending on whether `ColumnFamilyOptions::bottommost_compression_opts` overrode them. Users who relied on dictionary compression settings in `ColumnFamilyOptions::compression_opts` affecting only the bottommost level can keep the behavior by moving their dictionary settings to `ColumnFamilyOptions::bottommost_compression_opts` and setting its `enabled` flag.
* When the `enabled` flag is set in `ColumnFamilyOptions::bottommost_compression_opts`, those compression options now take effect regardless of the value in `ColumnFamilyOptions::bottommost_compression`. Previously, those compression options only took effect when `ColumnFamilyOptions::bottommost_compression != kDisableCompressionOption`. Now, they additionally take effect when `ColumnFamilyOptions::bottommost_compression == kDisableCompressionOption` (such a setting causes bottommost compression type to fall back to `ColumnFamilyOptions::compression_per_level` if configured, and otherwise fall back to `ColumnFamilyOptions::compression`).

### New Features
* An EXPERIMENTAL new Bloom alternative that saves about 30% space compared to Bloom filters, with about 3-4x construction time and similar query times is available using NewExperimentalRibbonFilterPolicy.

## 6.14 (2020-10-09)
### Bug fixes
* Fixed a bug after a `CompactRange()` with `CompactRangeOptions::change_level` set fails due to a conflict in the level change step, which caused all subsequent calls to `CompactRange()` with `CompactRangeOptions::change_level` set to incorrectly fail with a `Status::NotSupported("another thread is refitting")` error.
* Fixed a bug that the bottom most level compaction could still be a trivial move even if `BottommostLevelCompaction.kForce` or `kForceOptimized` is set.

### Public API Change
* The methods to create and manage EncrypedEnv have been changed.  The EncryptionProvider is now passed to NewEncryptedEnv as a shared pointer, rather than a raw pointer.  Comparably, the CTREncryptedProvider now takes a shared pointer, rather than a reference, to a BlockCipher.  CreateFromString methods have been added to BlockCipher and EncryptionProvider to provide a single API by which different ciphers and providers can be created, respectively.
* The internal classes (CTREncryptionProvider, ROT13BlockCipher, CTRCipherStream) associated with the EncryptedEnv have been moved out of the public API.  To create a CTREncryptionProvider, one can either use EncryptionProvider::NewCTRProvider, or EncryptionProvider::CreateFromString("CTR").  To create a new ROT13BlockCipher, one can either use BlockCipher::NewROT13Cipher or BlockCipher::CreateFromString("ROT13").
* The EncryptionProvider::AddCipher method has been added to allow keys to be added to an EncryptionProvider.  This API will allow future providers to support multiple cipher keys.
* Add a new option "allow_data_in_errors". When this new option is set by users, it allows users to opt-in to get error messages containing corrupted keys/values. Corrupt keys, values will be logged in the messages, logs, status etc. that will help users with the useful information regarding affected data. By default value of this option is set false to prevent users data to be exposed in the messages so currently, data will be redacted from logs, messages, status by default.
* AdvancedColumnFamilyOptions::force_consistency_checks is now true by default, for more proactive DB corruption detection at virtually no cost (estimated two extra CPU cycles per million on a major production workload). Corruptions reported by these checks now mention "force_consistency_checks" in case a false positive corruption report is suspected and the option needs to be disabled (unlikely). Since existing column families have a saved setting for force_consistency_checks, only new column families will pick up the new default.

### General Improvements
* The settings of the DBOptions and ColumnFamilyOptions are now managed by Configurable objects (see New Features).  The same convenience methods to configure these options still exist but the backend implementation has been unified under a common implementation.

### New Features

* Methods to configure serialize, and compare -- such as TableFactory -- are exposed directly through the Configurable base class (from which these objects inherit).  This change will allow for better and more thorough configuration management and retrieval in the future.  The options for a Configurable object can be set via the ConfigureFromMap, ConfigureFromString, or ConfigureOption method.  The serialized version of the options of an object can be retrieved via the GetOptionString, ToString, or GetOption methods.  The list of options supported by an object can be obtained via the GetOptionNames method.  The "raw" object (such as the BlockBasedTableOption) for an option may be retrieved via the GetOptions method.  Configurable options can be compared via the AreEquivalent method.  The settings within a Configurable object may be validated via the ValidateOptions method.  The object may be intialized (at which point only mutable options may be updated) via the PrepareOptions method.
* Introduce options.check_flush_compaction_key_order with default value to be true. With this option, during flush and compaction, key order will be checked when writing to each SST file. If the order is violated, the flush or compaction will fail.
* Added is_full_compaction to CompactionJobStats, so that the information is available through the EventListener interface.
* Add more stats for MultiGet in Histogram to get number of data blocks, index blocks, filter blocks and sst files read from file system per level.
* SST files have a new table property called db_host_id, which is set to the hostname by default. A new option in DBOptions, db_host_id, allows the property value to be overridden with a user specified string, or disable it completely by making the option string empty.
* Methods to create customizable extensions -- such as TableFactory -- are exposed directly through the Customizable base class (from which these objects inherit).  This change will allow these Customizable classes to be loaded and configured in a standard way (via CreateFromString).  More information on how to write and use Customizable classes is in the customizable.h header file.

## 6.13 (2020-09-12)
### Bug fixes
* Fix a performance regression introduced in 6.4 that makes a upper bound check for every Next() even if keys are within a data block that is within the upper bound.
* Fix a possible corruption to the LSM state (overlapping files within a level) when a `CompactRange()` for refitting levels (`CompactRangeOptions::change_level == true`) and another manual compaction are executed in parallel.
* Sanitize `recycle_log_file_num` to zero when the user attempts to enable it in combination with `WALRecoveryMode::kTolerateCorruptedTailRecords`. Previously the two features were allowed together, which compromised the user's configured crash-recovery guarantees.
* Fix a bug where a level refitting in CompactRange() might race with an automatic compaction that puts the data to the target level of the refitting. The bug has been there for years.
* Fixed a bug in version 6.12 in which BackupEngine::CreateNewBackup could fail intermittently with non-OK status when backing up a read-write DB configured with a DBOptions::file_checksum_gen_factory.
* Fix useless no-op compactions scheduled upon snapshot release when options.disable-auto-compactions = true.
* Fix a bug when max_write_buffer_size_to_maintain is set, immutable flushed memtable destruction is delayed until the next super version is installed. A memtable is not added to delete list because of its reference hold by super version and super version doesn't switch because of empt delete list. So memory usage keeps on increasing beyond write_buffer_size + max_write_buffer_size_to_maintain.
* Avoid converting MERGES to PUTS when allow_ingest_behind is true.
* Fix compression dictionary sampling together with `SstFileWriter`. Previously, the dictionary would be trained/finalized immediately with zero samples. Now, the whole `SstFileWriter` file is buffered in memory and then sampled.
* Fix a bug with `avoid_unnecessary_blocking_io=1` and creating backups (BackupEngine::CreateNewBackup) or checkpoints (Checkpoint::Create). With this setting and WAL enabled, these operations could randomly fail with non-OK status.
* Fix a bug in which bottommost compaction continues to advance the underlying InternalIterator to skip tombstones even after shutdown.

### New Features
* A new field `std::string requested_checksum_func_name` is added to `FileChecksumGenContext`, which enables the checksum factory to create generators for a suite of different functions.
* Added a new subcommand, `ldb unsafe_remove_sst_file`, which removes a lost or corrupt SST file from a DB's metadata. This command involves data loss and must not be used on a live DB.

### Performance Improvements
* Reduce thread number for multiple DB instances by re-using one global thread for statistics dumping and persisting.
* Reduce write-amp in heavy write bursts in `kCompactionStyleLevel` compaction style with `level_compaction_dynamic_level_bytes` set.
* BackupEngine incremental backups no longer read DB table files that are already saved to a shared part of the backup directory, unless `share_files_with_checksum` is used with `kLegacyCrc32cAndFileSize` naming (discouraged).
  * For `share_files_with_checksum`, we are confident there is no regression (vs. pre-6.12) in detecting DB or backup corruption at backup creation time, mostly because the old design did not leverage this extra checksum computation for detecting inconsistencies at backup creation time.
  * For `share_table_files` without "checksum" (not recommended), there is a regression in detecting fundamentally unsafe use of the option, greatly mitigated by file size checking (under "Behavior Changes"). Almost no reason to use `share_files_with_checksum=false` should remain.
  * `DB::VerifyChecksum` and `BackupEngine::VerifyBackup` with checksum checking are still able to catch corruptions that `CreateNewBackup` does not.

### Public API Change
* Expose kTypeDeleteWithTimestamp in EntryType and update GetEntryType() accordingly.
* Added file_checksum and file_checksum_func_name to TableFileCreationInfo, which can pass the table file checksum information through the OnTableFileCreated callback during flush and compaction.
* A warning is added to `DB::DeleteFile()` API describing its known problems and deprecation plan.
* Add a new stats level, i.e. StatsLevel::kExceptTickers (PR7329) to exclude tickers even if application passes a non-null Statistics object.
* Added a new status code IOStatus::IOFenced() for the Env/FileSystem to indicate that writes from this instance are fenced off. Like any other background error, this error is returned to the user in Put/Merge/Delete/Flush calls and can be checked using Status::IsIOFenced().

### Behavior Changes
* File abstraction `FSRandomAccessFile.Prefetch()` default return status is changed from `OK` to `NotSupported`. If the user inherited file doesn't implement prefetch, RocksDB will create internal prefetch buffer to improve read performance.
* When retryabel IO error happens during Flush (manifest write error is excluded) and WAL is disabled, originally it is mapped to kHardError. Now,it is mapped to soft error. So DB will not stall the writes unless the memtable is full. At the same time, when auto resume is triggered to recover the retryable IO error during Flush, SwitchMemtable is not called to avoid generating to many small immutable memtables. If WAL is enabled, no behavior changes.
* When considering whether a table file is already backed up in a shared part of backup directory, BackupEngine would already query the sizes of source (DB) and pre-existing destination (backup) files. BackupEngine now uses these file sizes to detect corruption, as at least one of (a) old backup, (b) backup in progress, or (c) current DB is corrupt if there's a size mismatch.

### Others
* Error in prefetching partitioned index blocks will not be swallowed. It will fail the query and return the IOError users.

## 6.12 (2020-07-28)
### Public API Change
* Encryption file classes now exposed for inheritance in env_encryption.h
* File I/O listener is extended to cover more I/O operations. Now class `EventListener` in listener.h contains new callback functions: `OnFileFlushFinish()`, `OnFileSyncFinish()`, `OnFileRangeSyncFinish()`, `OnFileTruncateFinish()`, and ``OnFileCloseFinish()``.
* `FileOperationInfo` now reports `duration` measured by `std::chrono::steady_clock` and `start_ts` measured by `std::chrono::system_clock` instead of start and finish timestamps measured by `system_clock`. Note that `system_clock` is called before `steady_clock` in program order at operation starts.
* `DB::GetDbSessionId(std::string& session_id)` is added. `session_id` stores a unique identifier that gets reset every time the DB is opened. This DB session ID should be unique among all open DB instances on all hosts, and should be unique among re-openings of the same or other DBs. This identifier is recorded in the LOG file on the line starting with "DB Session ID:".
* `DB::OpenForReadOnly()` now returns `Status::NotFound` when the specified DB directory does not exist. Previously the error returned depended on the underlying `Env`. This change is available in all 6.11 releases as well.
* A parameter `verify_with_checksum` is added to `BackupEngine::VerifyBackup`, which is false by default. If it is ture, `BackupEngine::VerifyBackup` verifies checksums and file sizes of backup files. Pass `false` for `verify_with_checksum` to maintain the previous behavior and performance of `BackupEngine::VerifyBackup`, by only verifying sizes of backup files.

### Behavior Changes
* Best-efforts recovery ignores CURRENT file completely. If CURRENT file is missing during recovery, best-efforts recovery still proceeds with MANIFEST file(s).
* In best-efforts recovery, an error that is not Corruption or IOError::kNotFound or IOError::kPathNotFound will be overwritten silently. Fix this by checking all non-ok cases and return early.
* When `file_checksum_gen_factory` is set to `GetFileChecksumGenCrc32cFactory()`, BackupEngine will compare the crc32c checksums of table files computed when creating a backup to the expected checksums stored in the DB manifest, and will fail `CreateNewBackup()` on mismatch (corruption). If the `file_checksum_gen_factory` is not set or set to any other customized factory, there is no checksum verification to detect if SST files in a DB are corrupt when read, copied, and independently checksummed by BackupEngine.
* When a DB sets `stats_dump_period_sec > 0`, either as the initial value for DB open or as a dynamic option change, the first stats dump is staggered in the following X seconds, where X is an integer in `[0, stats_dump_period_sec)`. Subsequent stats dumps are still spaced `stats_dump_period_sec` seconds apart.
* When the paranoid_file_checks option is true, a hash is generated of all keys and values are generated when the SST file is written, and then the values are read back in to validate the file.  A corruption is signaled if the two hashes do not match.

### Bug fixes
* Compressed block cache was automatically disabled with read-only DBs by mistake. Now it is fixed: compressed block cache will be in effective with read-only DB too.
* Fix a bug of wrong iterator result if another thread finishes an update and a DB flush between two statement.
* Disable file deletion after MANIFEST write/sync failure until db re-open or Resume() so that subsequent re-open will not see MANIFEST referencing deleted SSTs.
* Fix a bug when index_type == kTwoLevelIndexSearch in PartitionedIndexBuilder to update FlushPolicy to point to internal key partitioner when it changes from user-key mode to internal-key mode in index partition.
* Make compaction report InternalKey corruption while iterating over the input.
* Fix a bug which may cause MultiGet to be slow because it may read more data than requested, but this won't affect correctness. The bug was introduced in 6.10 release.
* Fail recovery and report once hitting a physical log record checksum mismatch, while reading MANIFEST. RocksDB should not continue processing the MANIFEST any further.
* Fixed a bug in size-amp-triggered and periodic-triggered universal compaction, where the compression settings for the first input level were used rather than the compression settings for the output (bottom) level.

### New Features
* DB identity (`db_id`) and DB session identity (`db_session_id`) are added to table properties and stored in SST files. SST files generated from SstFileWriter and Repairer have DB identity “SST Writer” and “DB Repairer”, respectively. Their DB session IDs are generated in the same way as `DB::GetDbSessionId`. The session ID for SstFileWriter (resp., Repairer) resets every time `SstFileWriter::Open` (resp., `Repairer::Run`) is called.
* Added experimental option BlockBasedTableOptions::optimize_filters_for_memory for reducing allocated memory size of Bloom filters (~10% savings with Jemalloc) while preserving the same general accuracy. To have an effect, the option requires format_version=5 and malloc_usable_size. Enabling this option is forward and backward compatible with existing format_version=5.
* `BackupableDBOptions::share_files_with_checksum_naming` is added with new default behavior for naming backup files with `share_files_with_checksum`, to address performance and backup integrity issues. See API comments for details.
* Added auto resume function to automatically recover the DB from background Retryable IO Error. When retryable IOError happens during flush and WAL write, the error is mapped to Hard Error and DB will be in read mode. When retryable IO Error happens during compaction, the error will be mapped to Soft Error. DB is still in write/read mode. Autoresume function will create a thread for a DB to call DB->ResumeImpl() to try the recover for Retryable IO Error during flush and WAL write. Compaction will be rescheduled by itself if retryable IO Error happens. Auto resume may also cause other Retryable IO Error during the recovery, so the recovery will fail. Retry the auto resume may solve the issue, so we use max_bgerror_resume_count to decide how many resume cycles will be tried in total. If it is <=0, auto resume retryable IO Error is disabled. Default is INT_MAX, which will lead to a infinit auto resume. bgerror_resume_retry_interval decides the time interval between two auto resumes.
* Option `max_subcompactions` can be set dynamically using DB::SetDBOptions().
* Added experimental ColumnFamilyOptions::sst_partitioner_factory to define determine the partitioning of sst files. This helps compaction to split the files on interesting boundaries (key prefixes) to make propagation of sst files less write amplifying (covering the whole key space).

### Performance Improvements
* Eliminate key copies for internal comparisons while accessing ingested block-based tables.
* Reduce key comparisons during random access in all block-based tables.
* BackupEngine avoids unnecessary repeated checksum computation for backing up a table file to the `shared_checksum` directory when using `share_files_with_checksum_naming = kUseDbSessionId` (new default), except on SST files generated before this version of RocksDB, which fall back on using `kLegacyCrc32cAndFileSize`.

## 6.11 (2020-06-12)
### Bug Fixes
* Fix consistency checking error swallowing in some cases when options.force_consistency_checks = true.
* Fix possible false NotFound status from batched MultiGet using index type kHashSearch.
* Fix corruption caused by enabling delete triggered compaction (NewCompactOnDeletionCollectorFactory) in universal compaction mode, along with parallel compactions. The bug can result in two parallel compactions picking the same input files, resulting in the DB resurrecting older and deleted versions of some keys.
* Fix a use-after-free bug in best-efforts recovery. column_family_memtables_ needs to point to valid ColumnFamilySet.
* Let best-efforts recovery ignore corrupted files during table loading.
* Fix corrupt key read from ingested file when iterator direction switches from reverse to forward at a key that is a prefix of another key in the same file. It is only possible in files with a non-zero global seqno.
* Fix abnormally large estimate from GetApproximateSizes when a range starts near the end of one SST file and near the beginning of another. Now GetApproximateSizes consistently and fairly includes the size of SST metadata in addition to data blocks, attributing metadata proportionally among the data blocks based on their size.
* Fix potential file descriptor leakage in PosixEnv's IsDirectory() and NewRandomAccessFile().
* Fix false negative from the VerifyChecksum() API when there is a checksum mismatch in an index partition block in a BlockBasedTable format table file (index_type is kTwoLevelIndexSearch).
* Fix sst_dump to return non-zero exit code if the specified file is not a recognized SST file or fails requested checks.
* Fix incorrect results from batched MultiGet for duplicate keys, when the duplicate key matches the largest key of an SST file and the value type for the key in the file is a merge value.

### Public API Change
* Flush(..., column_family) may return Status::ColumnFamilyDropped() instead of Status::InvalidArgument() if column_family is dropped while processing the flush request.
* BlobDB now explicitly disallows using the default column family's storage directories as blob directory.
* DeleteRange now returns `Status::InvalidArgument` if the range's end key comes before its start key according to the user comparator. Previously the behavior was undefined.
* ldb now uses options.force_consistency_checks = true by default and "--disable_consistency_checks" is added to disable it.
* DB::OpenForReadOnly no longer creates files or directories if the named DB does not exist, unless create_if_missing is set to true.
* The consistency checks that validate LSM state changes (table file additions/deletions during flushes and compactions) are now stricter, more efficient, and no longer optional, i.e. they are performed even if `force_consistency_checks` is `false`.
* Disable delete triggered compaction (NewCompactOnDeletionCollectorFactory) in universal compaction mode and num_levels = 1 in order to avoid a corruption bug.
* `pin_l0_filter_and_index_blocks_in_cache` no longer applies to L0 files larger than `1.5 * write_buffer_size` to give more predictable memory usage. Such L0 files may exist due to intra-L0 compaction, external file ingestion, or user dynamically changing `write_buffer_size` (note, however, that files that are already pinned will continue being pinned, even after such a dynamic change).
* In point-in-time wal recovery mode, fail database recovery in case of IOError while reading the WAL to avoid data loss.
* A new method `Env::LowerThreadPoolCPUPriority(Priority, CpuPriority)` is added to `Env` to be able to lower to a specific priority such as `CpuPriority::kIdle`.

### New Features
* sst_dump to add a new --readahead_size argument. Users can specify read size when scanning the data. Sst_dump also tries to prefetch tail part of the SST files so usually some number of I/Os are saved there too.
* Generate file checksum in SstFileWriter if Options.file_checksum_gen_factory is set. The checksum and checksum function name are stored in ExternalSstFileInfo after the sst file write is finished.
* Add a value_size_soft_limit in read options which limits the cumulative value size of keys read in batches in MultiGet. Once the cumulative value size of found keys exceeds read_options.value_size_soft_limit, all the remaining keys are returned with status Abort without further finding their values. By default the value_size_soft_limit is std::numeric_limits<uint64_t>::max().
* Enable SST file ingestion with file checksum information when calling IngestExternalFiles(const std::vector<IngestExternalFileArg>& args). Added files_checksums and files_checksum_func_names to IngestExternalFileArg such that user can ingest the sst files with their file checksum information. Added verify_file_checksum to IngestExternalFileOptions (default is True). To be backward compatible, if DB does not enable file checksum or user does not provide checksum information (vectors of files_checksums and files_checksum_func_names are both empty), verification of file checksum is always sucessful. If DB enables file checksum, DB will always generate the checksum for each ingested SST file during Prepare stage of ingestion and store the checksum in Manifest, unless verify_file_checksum is False and checksum information is provided by the application. In this case, we only verify the checksum function name and directly store the ingested checksum in Manifest. If verify_file_checksum is set to True, DB will verify the ingested checksum and function name with the genrated ones. Any mismatch will fail the ingestion. Note that, if IngestExternalFileOptions::write_global_seqno is True, the seqno will be changed in the ingested file. Therefore, the checksum of the file will be changed. In this case, a new checksum will be generated after the seqno is updated and be stored in the Manifest.

### Performance Improvements
* Eliminate redundant key comparisons during random access in block-based tables.

## 6.10 (2020-05-02)
### Bug Fixes
* Fix wrong result being read from ingested file. May happen when a key in the file happen to be prefix of another key also in the file. The issue can further cause more data corruption. The issue exists with rocksdb >= 5.0.0 since DB::IngestExternalFile() was introduced.
* Finish implementation of BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey. It's now ready for use. Significantly reduces read amplification in some setups, especially for iterator seeks.
* Fix a bug by updating CURRENT file so that it points to the correct MANIFEST file after best-efforts recovery.
* Fixed a bug where ColumnFamilyHandle objects were not cleaned up in case an error happened during BlobDB's open after the base DB had been opened.
* Fix a potential undefined behavior caused by trying to dereference nullable pointer (timestamp argument) in DB::MultiGet.
* Fix a bug caused by not including user timestamp in MultiGet LookupKey construction. This can lead to wrong query result since the trailing bytes of a user key, if not shorter than timestamp, will be mistaken for user timestamp.
* Fix a bug caused by using wrong compare function when sorting the input keys of MultiGet with timestamps.
* Upgraded version of bzip library (1.0.6 -> 1.0.8) used with RocksJava to address potential vulnerabilities if an attacker can manipulate compressed data saved and loaded by RocksDB (not normal). See issue #6703.

### Public API Change
* Add a ConfigOptions argument to the APIs dealing with converting options to and from strings and files.  The ConfigOptions is meant to replace some of the options (such as input_strings_escaped and ignore_unknown_options) and allow for more parameters to be passed in the future without changing the function signature.
* Add NewFileChecksumGenCrc32cFactory to the file checksum public API, such that the builtin Crc32c based file checksum generator factory can be used by applications.
* Add IsDirectory to Env and FS to indicate if a path is a directory.

### New Features
* Added support for pipelined & parallel compression optimization for `BlockBasedTableBuilder`. This optimization makes block building, block compression and block appending a pipeline, and uses multiple threads to accelerate block compression. Users can set `CompressionOptions::parallel_threads` greater than 1 to enable compression parallelism. This feature is experimental for now.
* Provide an allocator for memkind to be used with block cache. This is to work with memory technologies (Intel DCPMM is one such technology currently available) that require different libraries for allocation and management (such as PMDK and memkind). The high capacities available make it possible to provision large caches (up to several TBs in size) beyond what is achievable with DRAM.
* Option `max_background_flushes` can be set dynamically using DB::SetDBOptions().
* Added functionality in sst_dump tool to check the compressed file size for different compression levels and print the time spent on compressing files with each compression type. Added arguments `--compression_level_from` and `--compression_level_to` to report size of all compression levels and one compression_type must be specified with it so that it will report compressed sizes of one compression type with different levels.
* Added statistics for redundant insertions into block cache: rocksdb.block.cache.*add.redundant. (There is currently no coordination to ensure that only one thread loads a table block when many threads are trying to access that same table block.)

### Bug Fixes
* Fix a bug when making options.bottommost_compression, options.compression_opts and options.bottommost_compression_opts dynamically changeable: the modified values are not written to option files or returned back to users when being queried.
* Fix a bug where index key comparisons were unaccounted in `PerfContext::user_key_comparison_count` for lookups in files written with `format_version >= 3`.
* Fix many bloom.filter statistics not being updated in batch MultiGet.

### Performance Improvements
* Improve performance of batch MultiGet with partitioned filters, by sharing block cache lookups to applicable filter blocks.
* Reduced memory copies when fetching and uncompressing compressed blocks from sst files.

## 6.9.0 (2020-03-29)
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

## 6.8.0 (2020-02-24)
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

## 6.7.0 (2020-01-21)
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

## 6.6.2 (2020-01-13)
### Bug Fixes
* Fixed a bug where non-L0 compaction input files were not considered to compute the `creation_time` of new compaction outputs.

## 6.6.1 (2020-01-02)
### Bug Fixes
* Fix a bug in WriteBatchWithIndex::MultiGetFromBatchAndDB, which is called by Transaction::MultiGet, that causes due to stale pointer access when the number of keys is > 32
* Fixed two performance issues related to memtable history trimming. First, a new SuperVersion is now created only if some memtables were actually trimmed. Second, trimming is only scheduled if there is at least one flushed memtable that is kept in memory for the purposes of transaction conflict checking.
* BlobDB no longer updates the SST to blob file mapping upon failed compactions.
* Fix a bug in which a snapshot read through an iterator could be affected by a DeleteRange after the snapshot (#6062).
* Fixed a bug where BlobDB was comparing the `ColumnFamilyHandle` pointers themselves instead of only the column family IDs when checking whether an API call uses the default column family or not.
* Delete superversions in BackgroundCallPurge.
* Fix use-after-free and double-deleting files in BackgroundCallPurge().

## 6.6.0 (2019-11-25)
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

## 6.5.2 (2019-11-15)
### Bug Fixes
* Fix a assertion failure in MultiGet() when BlockBasedTableOptions::no_block_cache is true and there is no compressed block cache
* Fix a buffer overrun problem in BlockBasedTable::MultiGet() when compression is enabled and no compressed block cache is configured.
* If a call to BackupEngine::PurgeOldBackups or BackupEngine::DeleteBackup suffered a crash, power failure, or I/O error, files could be left over from old backups that could only be purged with a call to GarbageCollect. Any call to PurgeOldBackups, DeleteBackup, or GarbageCollect should now suffice to purge such files.

## 6.5.1 (2019-10-16)
### Bug Fixes
* Revert the feature "Merging iterator to avoid child iterator reseek for some cases (#5286)" since it might cause strange results when reseek happens with a different iterator upper bound.
* Fix a bug in BlockBasedTableIterator that might return incorrect results when reseek happens with a different iterator upper bound.
* Fix a bug when partitioned filters and prefix search are used in conjunction, ::SeekForPrev could return invalid for an existing prefix. ::SeekForPrev might be called by the user, or internally on ::Prev, or within ::Seek if the return value involves Delete or a Merge operand.

## 6.5.0 (2019-09-13)
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

## 6.4.0 (2019-07-30)
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

## 6.3.2 (2019-08-15)
### Public API Change
* The semantics of the per-block-type block read counts in the performance context now match those of the generic block_read_count.

### Bug Fixes
* Fixed a regression where the fill_cache read option also affected index blocks.
* Fixed an issue where using cache_index_and_filter_blocks==false affected partitions of partitioned indexes as well.

## 6.3.1 (2019-07-24)
### Bug Fixes
* Fix auto rolling bug introduced in 6.3.0, which causes segfault if log file creation fails.

## 6.3.0 (2019-06-18)
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

## 6.2.0 (2019-04-30)
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

## 6.1.1 (2019-04-09)
### New Features
* When reading from option file/string/map, customized comparators and/or merge operators can be filled according to object registry.

### Public API Change

### Bug Fixes
* Fix a bug in 2PC where a sequence of txn prepare, memtable flush, and crash could result in losing the prepared transaction.
* Fix a bug in Encryption Env which could cause encrypted files to be read beyond file boundaries.

## 6.1.0 (2019-03-27)
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

## 6.0.0 (2019-02-19)
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

## 5.18.0 (2018-11-30)
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

## 5.17.0 (2018-10-05)
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

## 5.16.3 (2018-10-01)
### Bug Fixes
* Fix crash caused when `CompactFiles` run with `CompactionOptions::compression == CompressionType::kDisableCompressionOption`. Now that setting causes the compression type to be chosen according to the column family-wide compression options.

## 5.16.2 (2018-09-21)
### Bug Fixes
* Fix bug in partition filters with format_version=4.

## 5.16.1 (2018-09-17)
### Bug Fixes
* Remove trace_analyzer_tool from rocksdb_lib target in TARGETS file.
* Fix RocksDB Java build and tests.
* Remove sync point in Block destructor.

## 5.16.0 (2018-08-21)
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

## 5.15.0 (2018-07-17)
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

## 5.14.0 (2018-05-16)
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

## 5.13.0 (2018-03-20)
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

## 5.12.0 (2018-02-14)
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

## 5.11.0 (2018-01-08)
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

## 5.10.0 (2017-12-11)
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

## 5.9.0 (2017-11-01)
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

## 5.8.0 (2017-08-30)
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

## 5.7.0 (2017-07-13)
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

## 5.6.0 (2017-06-06)
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

## 5.5.0 (2017-05-17)
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

## 5.4.0 (2017-04-11)
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

## 5.3.0 (2017-03-08)
### Public API Change
* Remove disableDataSync option.
* Remove timeout_hint_us option from WriteOptions. The option has been deprecated and has no effect since 3.13.0.
* Remove option min_partial_merge_operands. Partial merge operands will always be merged in flush or compaction if there are more than one.
* Remove option verify_checksums_in_compaction. Compaction will always verify checksum.

### Bug Fixes
* Fix the bug that iterator may skip keys

## 5.2.0 (2017-02-08)
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

## 5.1.0 (2017-01-13)
* Support dynamically change `delete_obsolete_files_period_micros` option via SetDBOptions().
* Added EventListener::OnExternalFileIngested which will be called when IngestExternalFile() add a file successfully.
* BackupEngine::Open and BackupEngineReadOnly::Open now always return error statuses matching those of the backup Env.

### Bug Fixes
* Fix the bug that if 2PC is enabled, checkpoints may loss some recent transactions.
* When file copying is needed when creating checkpoints or bulk loading files, fsync the file after the file copying.

## 5.0.0 (2016-11-17)
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

## 4.13.0 (2016-10-18)
### Public API Change
* DB::GetOptions() reflect dynamic changed options (i.e. through DB::SetOptions()) and return copy of options instead of reference.
* Added Statistics::getAndResetTickerCount().

### New Features
* Add DB::SetDBOptions() to dynamic change base_background_compactions and max_background_compactions.
* Added Iterator::SeekForPrev(). This new API will seek to the last key that less than or equal to the target key.

## 4.12.0 (2016-09-12)
### Public API Change
* CancelAllBackgroundWork() flushes all memtables for databases containing writes that have bypassed the WAL (writes issued with WriteOptions::disableWAL=true) before shutting down background threads.
* Merge options source_compaction_factor, max_grandparent_overlap_bytes and expanded_compaction_factor into max_compaction_bytes.
* Remove ImmutableCFOptions.
* Add a compression type ZSTD, which can work with ZSTD 0.8.0 or up. Still keep ZSTDNotFinal for compatibility reasons.

### New Features
* Introduce NewClockCache, which is based on CLOCK algorithm with better concurrent performance in some cases. It can be used to replace the default LRU-based block cache and table cache. To use it, RocksDB need to be linked with TBB lib.
* Change ticker/histogram statistics implementations to accumulate data in thread-local storage, which improves CPU performance by reducing cache coherency costs. Callers of CreateDBStatistics do not need to change anything to use this feature.
* Block cache mid-point insertion, where index and filter block are inserted into LRU block cache with higher priority. The feature can be enabled by setting BlockBasedTableOptions::cache_index_and_filter_blocks_with_high_priority to true and high_pri_pool_ratio > 0 when creating NewLRUCache.

## 4.11.0 (2016-08-01)
### Public API Change
* options.memtable_prefix_bloom_huge_page_tlb_size => memtable_huge_page_size. When it is set, RocksDB will try to allocate memory from huge page for memtable too, rather than just memtable bloom filter.

### New Features
* A tool to migrate DB after options change. See include/rocksdb/utilities/option_change_migration.h.
* Add ReadOptions.background_purge_on_iterator_cleanup. If true, we avoid file deletion when destroying iterators.

## 4.10.0 (2016-07-05)
### Public API Change
* options.memtable_prefix_bloom_bits changes to options.memtable_prefix_bloom_bits_ratio and deprecate options.memtable_prefix_bloom_probes
* enum type CompressionType and PerfLevel changes from char to unsigned char. Value of all PerfLevel shift by one.
* Deprecate options.filter_deletes.

### New Features
* Add avoid_flush_during_recovery option.
* Add a read option background_purge_on_iterator_cleanup to avoid deleting files in foreground when destroying iterators. Instead, a job is scheduled in high priority queue and would be executed in a separate background thread.
* RepairDB support for column families. RepairDB now associates data with non-default column families using information embedded in the SST/WAL files (4.7 or later). For data written by 4.6 or earlier, RepairDB associates it with the default column family.
* Add options.write_buffer_manager which allows users to control total memtable sizes across multiple DB instances.

## 4.9.0 (2016-06-09)
### Public API changes
* Add bottommost_compression option, This option can be used to set a specific compression algorithm for the bottommost level (Last level containing files in the DB).
* Introduce CompactionJobInfo::compression, This field state the compression algorithm used to generate the output files of the compaction.
* Deprecate BlockBaseTableOptions.hash_index_allow_collision=false
* Deprecate options builder (GetOptions()).

### New Features
* Introduce NewSimCache() in rocksdb/utilities/sim_cache.h. This function creates a block cache that is able to give simulation results (mainly hit rate) of simulating block behavior with a configurable cache size.

## 4.8.0 (2016-05-02)
### Public API Change
* Allow preset compression dictionary for improved compression of block-based tables. This is supported for zlib, zstd, and lz4. The compression dictionary's size is configurable via CompressionOptions::max_dict_bytes.
* Delete deprecated classes for creating backups (BackupableDB) and restoring from backups (RestoreBackupableDB). Now, BackupEngine should be used for creating backups, and BackupEngineReadOnly should be used for restorations. For more details, see https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB%3F
* Expose estimate of per-level compression ratio via DB property: "rocksdb.compression-ratio-at-levelN".
* Added EventListener::OnTableFileCreationStarted. EventListener::OnTableFileCreated will be called on failure case. User can check creation status via TableFileCreationInfo::status.

### New Features
* Add ReadOptions::readahead_size. If non-zero, NewIterator will create a new table reader which performs reads of the given size.

## 4.7.0 (2016-04-08)
### Public API Change
* rename options compaction_measure_io_stats to report_bg_io_stats and include flush too.
* Change some default options. Now default options will optimize for server-workloads. Also enable slowdown and full stop triggers for pending compaction bytes. These changes may cause sub-optimal performance or significant increase of resource usage. To avoid these risks, users can open existing RocksDB with options extracted from RocksDB option files. See https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File for how to use RocksDB option files. Or you can call Options.OldDefaults() to recover old defaults. DEFAULT_OPTIONS_HISTORY.md will track change history of default options.

## 4.6.0 (2016-03-10)
### Public API Changes
* Change default of BlockBasedTableOptions.format_version to 2. It means default DB created by 4.6 or up cannot be opened by RocksDB version 3.9 or earlier.
* Added strict_capacity_limit option to NewLRUCache. If the flag is set to true, insert to cache will fail if no enough capacity can be free. Signature of Cache::Insert() is updated accordingly.
* Tickers [NUMBER_DB_NEXT, NUMBER_DB_PREV, NUMBER_DB_NEXT_FOUND, NUMBER_DB_PREV_FOUND, ITER_BYTES_READ] are not updated immediately. The are updated when the Iterator is deleted.
* Add monotonically increasing counter (DB property "rocksdb.current-super-version-number") that increments upon any change to the LSM tree.

### New Features
* Add CompactionPri::kMinOverlappingRatio, a compaction picking mode friendly to write amplification.
* Deprecate Iterator::IsKeyPinned() and replace it with Iterator::GetProperty() with prop_name="rocksdb.iterator.is.key.pinned"

## 4.5.0 (2016-02-05)
### Public API Changes
* Add a new perf context level between kEnableCount and kEnableTime. Level 2 now does not include timers for mutexes.
* Statistics of mutex operation durations will not be measured by default. If you want to have them enabled, you need to set Statistics::stats_level_ to kAll.
* DBOptions::delete_scheduler and NewDeleteScheduler() are removed, please use DBOptions::sst_file_manager and NewSstFileManager() instead

### New Features
* ldb tool now supports operations to non-default column families.
* Add kPersistedTier to ReadTier.  This option allows Get and MultiGet to read only the persited data and skip mem-tables if writes were done with disableWAL = true.
* Add DBOptions::sst_file_manager. Use NewSstFileManager() in include/rocksdb/sst_file_manager.h to create a SstFileManager that can be used to track the total size of SST files and control the SST files deletion rate.

## 4.4.0 (2016-01-14)
### Public API Changes
* Change names in CompactionPri and add a new one.
* Deprecate options.soft_rate_limit and add options.soft_pending_compaction_bytes_limit.
* If options.max_write_buffer_number > 3, writes will be slowed down when writing to the last write buffer to delay a full stop.
* Introduce CompactionJobInfo::compaction_reason, this field include the reason to trigger the compaction.
* After slow down is triggered, if estimated pending compaction bytes keep increasing, slowdown more.
* Increase default options.delayed_write_rate to 2MB/s.
* Added a new parameter --path to ldb tool. --path accepts the name of either MANIFEST, SST or a WAL file. Either --db or --path can be used when calling ldb.

## 4.3.0 (2015-12-08)
### New Features
* CompactionFilter has new member function called IgnoreSnapshots which allows CompactionFilter to be called even if there are snapshots later than the key.
* RocksDB will now persist options under the same directory as the RocksDB database on successful DB::Open, CreateColumnFamily, DropColumnFamily, and SetOptions.
* Introduce LoadLatestOptions() in rocksdb/utilities/options_util.h.  This function can construct the latest DBOptions / ColumnFamilyOptions used by the specified RocksDB intance.
* Introduce CheckOptionsCompatibility() in rocksdb/utilities/options_util.h.  This function checks whether the input set of options is able to open the specified DB successfully.

### Public API Changes
* When options.db_write_buffer_size triggers, only the column family with the largest column family size will be flushed, not all the column families.

## 4.2.0 (2015-11-09)
### New Features
* Introduce CreateLoggerFromOptions(), this function create a Logger for provided DBOptions.
* Add GetAggregatedIntProperty(), which returns the sum of the GetIntProperty of all the column families.
* Add MemoryUtil in rocksdb/utilities/memory.h.  It currently offers a way to get the memory usage by type from a list rocksdb instances.

### Public API Changes
* CompactionFilter::Context includes information of Column Family ID
* The need-compaction hint given by TablePropertiesCollector::NeedCompact() will be persistent and recoverable after DB recovery. This introduces a breaking format change. If you use this experimental feature, including NewCompactOnDeletionCollectorFactory() in the new version, you may not be able to directly downgrade the DB back to version 4.0 or lower.
* TablePropertiesCollectorFactory::CreateTablePropertiesCollector() now takes an option Context, containing the information of column family ID for the file being written.
* Remove DefaultCompactionFilterFactory.


## 4.1.0 (2015-10-08)
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

## 4.0.0 (2015-09-09)
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

## 3.13.0 (2015-08-06)
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

## 3.12.0 (2015-07-02)
### New Features
* Added experimental support for optimistic transactions.  See include/rocksdb/utilities/optimistic_transaction.h for more info.
* Added a new way to report QPS from db_bench (check out --report_file and --report_interval_seconds)
* Added a cache for individual rows. See DBOptions::row_cache for more info.
* Several new features on EventListener (see include/rocksdb/listener.h):
 - OnCompactionCompleted() now returns per-compaction job statistics, defined in include/rocksdb/compaction_job_stats.h.
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

## 3.11.0 (2015-05-19)
### New Features
* Added a new API Cache::SetCapacity(size_t capacity) to dynamically change the maximum configured capacity of the cache. If the new capacity is less than the existing cache usage, the implementation will try to lower the usage by evicting the necessary number of elements following a strict LRU policy.
* Added an experimental API for handling flashcache devices (blacklists background threads from caching their reads) -- NewFlashcacheAwareEnv
* If universal compaction is used and options.num_levels > 1, compact files are tried to be stored in none-L0 with smaller files based on options.target_file_size_base. The limitation of DB size when using universal compaction is greatly mitigated by using more levels. You can set num_levels = 1 to make universal compaction behave as before. If you set num_levels > 1 and want to roll back to a previous version, you need to compact all files to a big file in level 0 (by setting target_file_size_base to be large and CompactRange(<cf_handle>, nullptr, nullptr, true, 0) and reopen the DB with the same version to rewrite the manifest, and then you can open it using previous releases.
* More information about rocksdb background threads are available in Env::GetThreadList(), including the number of bytes read / written by a compaction job, mem-table size and current number of bytes written by a flush job and many more.  Check include/rocksdb/thread_status.h for more detail.

### Public API changes
* TablePropertiesCollector::AddUserKey() is added to replace TablePropertiesCollector::Add(). AddUserKey() exposes key type, sequence number and file size up to now to users.
* DBOptions::bytes_per_sync used to apply to both WAL and table files. As of 3.11 it applies only to table files. If you want to use this option to sync WAL in the background, please use wal_bytes_per_sync

## 3.10.0 (2015-03-24)
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

## 3.9.0 (2014-12-08)

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

## 3.8.0 (2014-11-14)

### Public API changes
* BackupEngine::NewBackupEngine() was deprecated; please use BackupEngine::Open() from now on.
* BackupableDB/RestoreBackupableDB have new GarbageCollect() methods, which will clean up files from corrupt and obsolete backups.
* BackupableDB/RestoreBackupableDB have new GetCorruptedBackups() methods which list corrupt backups.

### Cleanup
* Bunch of code cleanup, some extra warnings turned on (-Wshadow, -Wshorten-64-to-32, -Wnon-virtual-dtor)

### New features
* CompactFiles and EventListener, although they are still in experimental state
* Full ColumnFamily support in RocksJava.

## 3.7.0 (2014-11-06)
### Public API changes
* Introduce SetOptions() API to allow adjusting a subset of options dynamically online
* Introduce 4 new convenient functions for converting Options from string: GetColumnFamilyOptionsFromMap(), GetColumnFamilyOptionsFromString(), GetDBOptionsFromMap(), GetDBOptionsFromString()
* Remove WriteBatchWithIndex.Delete() overloads using SliceParts
* When opening a DB, if options.max_background_compactions is larger than the existing low pri pool of options.env, it will enlarge it. Similarly, options.max_background_flushes is larger than the existing high pri pool of options.env, it will enlarge it.

## 3.6.0 (2014-10-07)
### Disk format changes
* If you're using RocksDB on ARM platforms and you're using default bloom filter, there is a disk format change you need to be aware of. There are three steps you need to do when you convert to new release: 1. turn off filter policy, 2. compact the whole database, 3. turn on filter policy

### Behavior changes
* We have refactored our system of stalling writes.  Any stall-related statistics' meanings are changed. Instead of per-write stall counts, we now count stalls per-epoch, where epochs are periods between flushes and compactions. You'll find more information in our Tuning Perf Guide once we release RocksDB 3.6.
* When disableDataSync=true, we no longer sync the MANIFEST file.
* Add identity_as_first_hash property to CuckooTable. SST file needs to be rebuilt to be opened by reader properly.

### Public API changes
* Change target_file_size_base type to uint64_t from int.
* Remove allow_thread_local. This feature was proved to be stable, so we are turning it always-on.

## 3.5.0 (2014-09-03)
### New Features
* Add include/utilities/write_batch_with_index.h, providing a utility class to query data out of WriteBatch when building it.
* Move BlockBasedTable related options to BlockBasedTableOptions from Options. Change corresponding JNI interface. Options affected include:
  no_block_cache, block_cache, block_cache_compressed, block_size, block_size_deviation, block_restart_interval, filter_policy, whole_key_filtering. filter_policy is changed to shared_ptr from a raw pointer.
* Remove deprecated options: disable_seek_compaction and db_stats_log_interval
* OptimizeForPointLookup() takes one parameter for block cache size. It now builds hash index, bloom filter, and block cache.

### Public API changes
* The Prefix Extractor used with V2 compaction filters is now passed user key to SliceTransform::Transform instead of unparsed RocksDB key.

## 3.4.0 (2014-08-18)
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

## 3.3.0 (2014-07-10)
### New Features
* Added JSON API prototype.
* HashLinklist reduces performance outlier caused by skewed bucket by switching data in the bucket from linked list to skip list. Add parameter threshold_use_skiplist in NewHashLinkListRepFactory().
* RocksDB is now able to reclaim storage space more effectively during the compaction process.  This is done by compensating the size of each deletion entry by the 2X average value size, which makes compaction to be triggered by deletion entries more easily.
* Add TimeOut API to write.  Now WriteOptions have a variable called timeout_hint_us.  With timeout_hint_us set to non-zero, any write associated with this timeout_hint_us may be aborted when it runs longer than the specified timeout_hint_us, and it is guaranteed that any write completes earlier than the specified time-out will not be aborted due to the time-out condition.
* Add a rate_limiter option, which controls total throughput of flush and compaction. The throughput is specified in bytes/sec. Flush always has precedence over compaction when available bandwidth is constrained.

### Public API changes
* Removed NewTotalOrderPlainTableFactory because it is not used and implemented semantically incorrect.

## 3.2.0 (2014-06-20)

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

## 3.1.0 (2014-05-21)

### Public API changes
* Replaced ColumnFamilyOptions::table_properties_collectors with ColumnFamilyOptions::table_properties_collector_factories

### New Features
* Hash index for block-based table will be materialized and reconstructed more efficiently. Previously hash index is constructed by scanning the whole table during every table open.
* FIFO compaction style

## 3.0.0 (2014-05-05)

### Public API changes
* Added _LEVEL to all InfoLogLevel enums
* Deprecated ReadOptions.prefix and ReadOptions.prefix_seek. Seek() defaults to prefix-based seek when Options.prefix_extractor is supplied. More detail is documented in https://github.com/facebook/rocksdb/wiki/Prefix-Seek-API-Changes
* MemTableRepFactory::CreateMemTableRep() takes info logger as an extra parameter.

### New Features
* Column family support
* Added an option to use different checksum functions in BlockBasedTableOptions
* Added ApplyToAllCacheEntries() function to Cache

## 2.8.0 (2014-04-04)

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

## 2.7.0 (2014-01-28)

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
