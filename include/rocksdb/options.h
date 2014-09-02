// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_ROCKSDB_INCLUDE_OPTIONS_H_
#define STORAGE_ROCKSDB_INCLUDE_OPTIONS_H_

#include <stddef.h>
#include <string>
#include <memory>
#include <vector>
#include <stdint.h>

#include "rocksdb/version.h"
#include "rocksdb/universal_compaction.h"

namespace rocksdb {

class Cache;
class CompactionFilter;
class CompactionFilterFactory;
class CompactionFilterFactoryV2;
class Comparator;
class Env;
enum InfoLogLevel : unsigned char;
class FilterPolicy;
class Logger;
class MergeOperator;
class Snapshot;
class TableFactory;
class MemTableRepFactory;
class TablePropertiesCollectorFactory;
class RateLimiter;
class Slice;
class SliceTransform;
class Statistics;
class InternalKeyComparator;

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
enum CompressionType : char {
  // NOTE: do not change the values of existing entries, as these are
  // part of the persistent format on disk.
  kNoCompression = 0x0, kSnappyCompression = 0x1, kZlibCompression = 0x2,
  kBZip2Compression = 0x3, kLZ4Compression = 0x4, kLZ4HCCompression = 0x5
};

enum CompactionStyle : char {
  kCompactionStyleLevel = 0x0,      // level based compaction style
  kCompactionStyleUniversal = 0x1,  // Universal compaction style
  kCompactionStyleFIFO = 0x2,       // FIFO compaction style
};

struct CompactionOptionsFIFO {
  // once the total sum of table files reaches this, we will delete the oldest
  // table file
  // Default: 1GB
  uint64_t max_table_files_size;

  CompactionOptionsFIFO() : max_table_files_size(1 * 1024 * 1024 * 1024) {}
};

// Compression options for different compression algorithms like Zlib
struct CompressionOptions {
  int window_bits;
  int level;
  int strategy;
  CompressionOptions() : window_bits(-14), level(-1), strategy(0) {}
  CompressionOptions(int wbits, int _lev, int _strategy)
      : window_bits(wbits), level(_lev), strategy(_strategy) {}
};

enum UpdateStatus {    // Return status For inplace update callback
  UPDATE_FAILED   = 0, // Nothing to update
  UPDATED_INPLACE = 1, // Value updated inplace
  UPDATED         = 2, // No inplace update. Merged value set
};

struct DbPath {
  std::string path;
  uint64_t target_size;  // Target size of total files under the path, in byte.

  DbPath() : target_size(0) {}
  DbPath(const std::string& p, uint64_t t) : path(p), target_size(t) {}
};

struct Options;

struct ColumnFamilyOptions {
  // Some functions that make it easier to optimize RocksDB

  // Use this if you don't need to keep the data sorted, i.e. you'll never use
  // an iterator, only Put() and Get() API calls
  ColumnFamilyOptions* OptimizeForPointLookup(
      uint64_t block_cache_size_mb);

  // Default values for some parameters in ColumnFamilyOptions are not
  // optimized for heavy workloads and big datasets, which means you might
  // observe write stalls under some conditions. As a starting point for tuning
  // RocksDB options, use the following two functions:
  // * OptimizeLevelStyleCompaction -- optimizes level style compaction
  // * OptimizeUniversalStyleCompaction -- optimizes universal style compaction
  // Universal style compaction is focused on reducing Write Amplification
  // Factor for big data sets, but increases Space Amplification. You can learn
  // more about the different styles here:
  // https://github.com/facebook/rocksdb/wiki/Rocksdb-Architecture-Guide
  // Make sure to also call IncreaseParallelism(), which will provide the
  // biggest performance gains.
  // Note: we might use more memory than memtable_memory_budget during high
  // write rate period
  ColumnFamilyOptions* OptimizeLevelStyleCompaction(
      uint64_t memtable_memory_budget = 512 * 1024 * 1024);
  ColumnFamilyOptions* OptimizeUniversalStyleCompaction(
      uint64_t memtable_memory_budget = 512 * 1024 * 1024);

  // -------------------
  // Parameters that affect behavior

  // Comparator used to define the order of keys in the table.
  // Default: a comparator that uses lexicographic byte-wise ordering
  //
  // REQUIRES: The client must ensure that the comparator supplied
  // here has the same name and orders keys *exactly* the same as the
  // comparator provided to previous open calls on the same DB.
  const Comparator* comparator;

  // REQUIRES: The client must provide a merge operator if Merge operation
  // needs to be accessed. Calling Merge on a DB without a merge operator
  // would result in Status::NotSupported. The client must ensure that the
  // merge operator supplied here has the same name and *exactly* the same
  // semantics as the merge operator provided to previous open calls on
  // the same DB. The only exception is reserved for upgrade, where a DB
  // previously without a merge operator is introduced to Merge operation
  // for the first time. It's necessary to specify a merge operator when
  // openning the DB in this case.
  // Default: nullptr
  std::shared_ptr<MergeOperator> merge_operator;

  // A single CompactionFilter instance to call into during compaction.
  // Allows an application to modify/delete a key-value during background
  // compaction.
  //
  // If the client requires a new compaction filter to be used for different
  // compaction runs, it can specify compaction_filter_factory instead of this
  // option.  The client should specify only one of the two.
  // compaction_filter takes precedence over compaction_filter_factory if
  // client specifies both.
  //
  // If multithreaded compaction is being used, the supplied CompactionFilter
  // instance may be used from different threads concurrently and so should be
  // thread-safe.
  //
  // Default: nullptr
  const CompactionFilter* compaction_filter;

  // This is a factory that provides compaction filter objects which allow
  // an application to modify/delete a key-value during background compaction.
  //
  // A new filter will be created on each compaction run.  If multithreaded
  // compaction is being used, each created CompactionFilter will only be used
  // from a single thread and so does not need to be thread-safe.
  //
  // Default: a factory that doesn't provide any object
  std::shared_ptr<CompactionFilterFactory> compaction_filter_factory;

  // Version TWO of the compaction_filter_factory
  // It supports rolling compaction
  //
  // Default: a factory that doesn't provide any object
  std::shared_ptr<CompactionFilterFactoryV2> compaction_filter_factory_v2;

  // -------------------
  // Parameters that affect performance

  // Amount of data to build up in memory (backed by an unsorted log
  // on disk) before converting to a sorted on-disk file.
  //
  // Larger values increase performance, especially during bulk loads.
  // Up to max_write_buffer_number write buffers may be held in memory
  // at the same time,
  // so you may wish to adjust this parameter to control memory usage.
  // Also, a larger write buffer will result in a longer recovery time
  // the next time the database is opened.
  //
  // Default: 4MB
  size_t write_buffer_size;

  // The maximum number of write buffers that are built up in memory.
  // The default and the minimum number is 2, so that when 1 write buffer
  // is being flushed to storage, new writes can continue to the other
  // write buffer.
  // Default: 2
  int max_write_buffer_number;

  // The minimum number of write buffers that will be merged together
  // before writing to storage.  If set to 1, then
  // all write buffers are fushed to L0 as individual files and this increases
  // read amplification because a get request has to check in all of these
  // files. Also, an in-memory merge may result in writing lesser
  // data to storage if there are duplicate records in each of these
  // individual write buffers.  Default: 1
  int min_write_buffer_number_to_merge;

  // Compress blocks using the specified compression algorithm.  This
  // parameter can be changed dynamically.
  //
  // Default: kSnappyCompression, which gives lightweight but fast
  // compression.
  //
  // Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
  //    ~200-500MB/s compression
  //    ~400-800MB/s decompression
  // Note that these speeds are significantly faster than most
  // persistent storage speeds, and therefore it is typically never
  // worth switching to kNoCompression.  Even if the input data is
  // incompressible, the kSnappyCompression implementation will
  // efficiently detect that and will switch to uncompressed mode.
  CompressionType compression;

  // Different levels can have different compression policies. There
  // are cases where most lower levels would like to quick compression
  // algorithm while the higher levels (which have more data) use
  // compression algorithms that have better compression but could
  // be slower. This array, if non nullptr, should have an entry for
  // each level of the database. This array, if non nullptr, overides the
  // value specified in the previous field 'compression'. The caller is
  // reponsible for allocating memory and initializing the values in it
  // before invoking Open(). The caller is responsible for freeing this
  // array and it could be freed anytime after the return from Open().
  // This could have been a std::vector but that makes the equivalent
  // java/C api hard to construct.
  std::vector<CompressionType> compression_per_level;

  // different options for compression algorithms
  CompressionOptions compression_opts;

  // If non-nullptr, use the specified function to determine the
  // prefixes for keys.  These prefixes will be placed in the filter.
  // Depending on the workload, this can reduce the number of read-IOP
  // cost for scans when a prefix is passed via ReadOptions to
  // db.NewIterator().  For prefix filtering to work properly,
  // "prefix_extractor" and "comparator" must be such that the following
  // properties hold:
  //
  // 1) key.starts_with(prefix(key))
  // 2) Compare(prefix(key), key) <= 0.
  // 3) If Compare(k1, k2) <= 0, then Compare(prefix(k1), prefix(k2)) <= 0
  // 4) prefix(prefix(key)) == prefix(key)
  //
  // Default: nullptr
  std::shared_ptr<const SliceTransform> prefix_extractor;

  // Number of levels for this database
  int num_levels;

  // Number of files to trigger level-0 compaction. A value <0 means that
  // level-0 compaction will not be triggered by number of files at all.
  //
  // Default: 4
  int level0_file_num_compaction_trigger;

  // Soft limit on number of level-0 files. We start slowing down writes at this
  // point. A value <0 means that no writing slow down will be triggered by
  // number of files in level-0.
  int level0_slowdown_writes_trigger;

  // Maximum number of level-0 files.  We stop writes at this point.
  int level0_stop_writes_trigger;

  // Maximum level to which a new compacted memtable is pushed if it
  // does not create overlap.  We try to push to level 2 to avoid the
  // relatively expensive level 0=>1 compactions and to avoid some
  // expensive manifest file operations.  We do not push all the way to
  // the largest level since that can generate a lot of wasted disk
  // space if the same key space is being repeatedly overwritten.
  int max_mem_compaction_level;

  // Target file size for compaction.
  // target_file_size_base is per-file size for level-1.
  // Target file size for level L can be calculated by
  // target_file_size_base * (target_file_size_multiplier ^ (L-1))
  // For example, if target_file_size_base is 2MB and
  // target_file_size_multiplier is 10, then each file on level-1 will
  // be 2MB, and each file on level 2 will be 20MB,
  // and each file on level-3 will be 200MB.

  // by default target_file_size_base is 2MB.
  int target_file_size_base;
  // by default target_file_size_multiplier is 1, which means
  // by default files in different levels will have similar size.
  int target_file_size_multiplier;

  // Control maximum total data size for a level.
  // max_bytes_for_level_base is the max total for level-1.
  // Maximum number of bytes for level L can be calculated as
  // (max_bytes_for_level_base) * (max_bytes_for_level_multiplier ^ (L-1))
  // For example, if max_bytes_for_level_base is 20MB, and if
  // max_bytes_for_level_multiplier is 10, total data size for level-1
  // will be 20MB, total file size for level-2 will be 200MB,
  // and total file size for level-3 will be 2GB.

  // by default 'max_bytes_for_level_base' is 10MB.
  uint64_t max_bytes_for_level_base;
  // by default 'max_bytes_for_level_base' is 10.
  int max_bytes_for_level_multiplier;

  // Different max-size multipliers for different levels.
  // These are multiplied by max_bytes_for_level_multiplier to arrive
  // at the max-size of each level.
  // Default: 1
  std::vector<int> max_bytes_for_level_multiplier_additional;

  // Maximum number of bytes in all compacted files.  We avoid expanding
  // the lower level file set of a compaction if it would make the
  // total compaction cover more than
  // (expanded_compaction_factor * targetFileSizeLevel()) many bytes.
  int expanded_compaction_factor;

  // Maximum number of bytes in all source files to be compacted in a
  // single compaction run. We avoid picking too many files in the
  // source level so that we do not exceed the total source bytes
  // for compaction to exceed
  // (source_compaction_factor * targetFileSizeLevel()) many bytes.
  // Default:1, i.e. pick maxfilesize amount of data as the source of
  // a compaction.
  int source_compaction_factor;

  // Control maximum bytes of overlaps in grandparent (i.e., level+2) before we
  // stop building a single file in a level->level+1 compaction.
  int max_grandparent_overlap_factor;

  // Puts are delayed 0-1 ms when any level has a compaction score that exceeds
  // soft_rate_limit. This is ignored when == 0.0.
  // CONSTRAINT: soft_rate_limit <= hard_rate_limit. If this constraint does not
  // hold, RocksDB will set soft_rate_limit = hard_rate_limit
  // Default: 0 (disabled)
  double soft_rate_limit;

  // Puts are delayed 1ms at a time when any level has a compaction score that
  // exceeds hard_rate_limit. This is ignored when <= 1.0.
  // Default: 0 (disabled)
  double hard_rate_limit;

  // Max time a put will be stalled when hard_rate_limit is enforced. If 0, then
  // there is no limit.
  // Default: 1000
  unsigned int rate_limit_delay_max_milliseconds;

  // size of one block in arena memory allocation.
  // If <= 0, a proper value is automatically calculated (usually 1/10 of
  // writer_buffer_size).
  //
  // There are two additonal restriction of the The specified size:
  // (1) size should be in the range of [4096, 2 << 30] and
  // (2) be the multiple of the CPU word (which helps with the memory
  // alignment).
  //
  // We'll automatically check and adjust the size number to make sure it
  // conforms to the restrictions.
  //
  // Default: 0
  size_t arena_block_size;

  // Disable automatic compactions. Manual compactions can still
  // be issued on this column family
  bool disable_auto_compactions;

  // Purge duplicate/deleted keys when a memtable is flushed to storage.
  // Default: true
  bool purge_redundant_kvs_while_flush;

  // The compaction style. Default: kCompactionStyleLevel
  CompactionStyle compaction_style;

  // If true, compaction will verify checksum on every read that happens
  // as part of compaction
  // Default: true
  bool verify_checksums_in_compaction;

  // The options needed to support Universal Style compactions
  CompactionOptionsUniversal compaction_options_universal;

  // The options for FIFO compaction style
  CompactionOptionsFIFO compaction_options_fifo;

  // Use KeyMayExist API to filter deletes when this is true.
  // If KeyMayExist returns false, i.e. the key definitely does not exist, then
  // the delete is a noop. KeyMayExist only incurs in-memory look up.
  // This optimization avoids writing the delete to storage when appropriate.
  // Default: false
  bool filter_deletes;

  // An iteration->Next() sequentially skips over keys with the same
  // user-key unless this option is set. This number specifies the number
  // of keys (with the same userkey) that will be sequentially
  // skipped before a reseek is issued.
  // Default: 8
  uint64_t max_sequential_skip_in_iterations;

  // This is a factory that provides MemTableRep objects.
  // Default: a factory that provides a skip-list-based implementation of
  // MemTableRep.
  std::shared_ptr<MemTableRepFactory> memtable_factory;

  // This is a factory that provides TableFactory objects.
  // Default: a block-based table factory that provides a default
  // implementation of TableBuilder and TableReader with default
  // BlockBasedTableOptions.
  std::shared_ptr<TableFactory> table_factory;

  // Block-based table related options are moved to BlockBasedTableOptions.
  // Related options that were originally here but now moved include:
  //   no_block_cache
  //   block_cache
  //   block_cache_compressed
  //   block_size
  //   block_size_deviation
  //   block_restart_interval
  //   filter_policy
  //   whole_key_filtering
  // If you'd like to customize some of these options, you will need to
  // use NewBlockBasedTableFactory() to construct a new table factory.

  // This option allows user to to collect their own interested statistics of
  // the tables.
  // Default: empty vector -- no user-defined statistics collection will be
  // performed.
  typedef std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      TablePropertiesCollectorFactories;
  TablePropertiesCollectorFactories table_properties_collector_factories;

  // Allows thread-safe inplace updates. If this is true, there is no way to
  // achieve point-in-time consistency using snapshot or iterator (assuming
  // concurrent updates).
  // If inplace_callback function is not set,
  //   Put(key, new_value) will update inplace the existing_value iff
  //   * key exists in current memtable
  //   * new sizeof(new_value) <= sizeof(existing_value)
  //   * existing_value for that key is a put i.e. kTypeValue
  // If inplace_callback function is set, check doc for inplace_callback.
  // Default: false.
  bool inplace_update_support;

  // Number of locks used for inplace update
  // Default: 10000, if inplace_update_support = true, else 0.
  size_t inplace_update_num_locks;

  // existing_value - pointer to previous value (from both memtable and sst).
  //                  nullptr if key doesn't exist
  // existing_value_size - pointer to size of existing_value).
  //                       nullptr if key doesn't exist
  // delta_value - Delta value to be merged with the existing_value.
  //               Stored in transaction logs.
  // merged_value - Set when delta is applied on the previous value.

  // Applicable only when inplace_update_support is true,
  // this callback function is called at the time of updating the memtable
  // as part of a Put operation, lets say Put(key, delta_value). It allows the
  // 'delta_value' specified as part of the Put operation to be merged with
  // an 'existing_value' of the key in the database.

  // If the merged value is smaller in size that the 'existing_value',
  // then this function can update the 'existing_value' buffer inplace and
  // the corresponding 'existing_value'_size pointer, if it wishes to.
  // The callback should return UpdateStatus::UPDATED_INPLACE.
  // In this case. (In this case, the snapshot-semantics of the rocksdb
  // Iterator is not atomic anymore).

  // If the merged value is larger in size than the 'existing_value' or the
  // application does not wish to modify the 'existing_value' buffer inplace,
  // then the merged value should be returned via *merge_value. It is set by
  // merging the 'existing_value' and the Put 'delta_value'. The callback should
  // return UpdateStatus::UPDATED in this case. This merged value will be added
  // to the memtable.

  // If merging fails or the application does not wish to take any action,
  // then the callback should return UpdateStatus::UPDATE_FAILED.

  // Please remember that the original call from the application is Put(key,
  // delta_value). So the transaction log (if enabled) will still contain (key,
  // delta_value). The 'merged_value' is not stored in the transaction log.
  // Hence the inplace_callback function should be consistent across db reopens.

  // Default: nullptr
  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value);

  // if prefix_extractor is set and bloom_bits is not 0, create prefix bloom
  // for memtable
  uint32_t memtable_prefix_bloom_bits;

  // number of hash probes per key
  uint32_t memtable_prefix_bloom_probes;

  // Page size for huge page TLB for bloom in memtable. If <=0, not allocate
  // from huge page TLB but from malloc.
  // Need to reserve huge pages for it to be allocated. For example:
  //      sysctl -w vm.nr_hugepages=20
  // See linux doc Documentation/vm/hugetlbpage.txt

  size_t memtable_prefix_bloom_huge_page_tlb_size;

  // Control locality of bloom filter probes to improve cache miss rate.
  // This option only applies to memtable prefix bloom and plaintable
  // prefix bloom. It essentially limits every bloom checking to one cache line.
  // This optimization is turned off when set to 0, and positive number to turn
  // it on.
  // Default: 0
  uint32_t bloom_locality;

  // Maximum number of successive merge operations on a key in the memtable.
  //
  // When a merge operation is added to the memtable and the maximum number of
  // successive merges is reached, the value of the key will be calculated and
  // inserted into the memtable instead of the merge operation. This will
  // ensure that there are never more than max_successive_merges merge
  // operations in the memtable.
  //
  // Default: 0 (disabled)
  size_t max_successive_merges;

  // The number of partial merge operands to accumulate before partial
  // merge will be performed. Partial merge will not be called
  // if the list of values to merge is less than min_partial_merge_operands.
  //
  // If min_partial_merge_operands < 2, then it will be treated as 2.
  //
  // Default: 2
  uint32_t min_partial_merge_operands;

  // Create ColumnFamilyOptions with default values for all fields
  ColumnFamilyOptions();
  // Create ColumnFamilyOptions from Options
  explicit ColumnFamilyOptions(const Options& options);

  void Dump(Logger* log) const;
};

struct DBOptions {
  // Some functions that make it easier to optimize RocksDB

  // By default, RocksDB uses only one background thread for flush and
  // compaction. Calling this function will set it up such that total of
  // `total_threads` is used. Good value for `total_threads` is the number of
  // cores. You almost definitely want to call this function if your system is
  // bottlenecked by RocksDB.
  DBOptions* IncreaseParallelism(int total_threads = 16);

  // If true, the database will be created if it is missing.
  // Default: false
  bool create_if_missing;

  // If true, missing column families will be automatically created.
  // Default: false
  bool create_missing_column_families;

  // If true, an error is raised if the database already exists.
  // Default: false
  bool error_if_exists;

  // If true, the implementation will do aggressive checking of the
  // data it is processing and will stop early if it detects any
  // errors.  This may have unforeseen ramifications: for example, a
  // corruption of one DB entry may cause a large number of entries to
  // become unreadable or for the entire DB to become unopenable.
  // If any of the  writes to the database fails (Put, Delete, Merge, Write),
  // the database will switch to read-only mode and fail all other
  // Write operations.
  // Default: true
  bool paranoid_checks;

  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  Env* env;

  // Use to control write rate of flush and compaction. Flush has higher
  // priority than compaction. Rate limiting is disabled if nullptr.
  // If rate limiter is enabled, bytes_per_sync is set to 1MB by default.
  // Default: nullptr
  std::shared_ptr<RateLimiter> rate_limiter;

  // Any internal progress/error information generated by the db will
  // be written to info_log if it is non-nullptr, or to a file stored
  // in the same directory as the DB contents if info_log is nullptr.
  // Default: nullptr
  std::shared_ptr<Logger> info_log;

  InfoLogLevel info_log_level;

  // Number of open files that can be used by the DB.  You may need to
  // increase this if your database has a large working set. Value -1 means
  // files opened are always kept open. You can estimate number of files based
  // on target_file_size_base and target_file_size_multiplier for level-based
  // compaction. For universal-style compaction, you can usually set it to -1.
  // Default: 5000
  int max_open_files;

  // Once write-ahead logs exceed this size, we will start forcing the flush of
  // column families whose memtables are backed by the oldest live WAL file
  // (i.e. the ones that are causing all the space amplification). If set to 0
  // (default), we will dynamically choose the WAL size limit to be
  // [sum of all write_buffer_size * max_write_buffer_number] * 2
  // Default: 0
  uint64_t max_total_wal_size;

  // If non-null, then we should collect metrics about database operations
  // Statistics objects should not be shared between DB instances as
  // it does not use any locks to prevent concurrent updates.
  std::shared_ptr<Statistics> statistics;

  // If true, then the contents of data files are not synced
  // to stable storage. Their contents remain in the OS buffers till the
  // OS decides to flush them. This option is good for bulk-loading
  // of data. Once the bulk-loading is complete, please issue a
  // sync to the OS to flush all dirty buffesrs to stable storage.
  // Default: false
  bool disableDataSync;

  // If true, then every store to stable storage will issue a fsync.
  // If false, then every store to stable storage will issue a fdatasync.
  // This parameter should be set to true while storing data to
  // filesystem like ext3 that can lose files after a reboot.
  // Default: false
  bool use_fsync;

  // A list of paths where SST files can be put into, with its target size.
  // Newer data is placed into paths specified earlier in the vector while
  // older data gradually moves to paths specified later in the vector.
  //
  // For example, you have a flash device with 10GB allocated for the DB,
  // as well as a hard drive of 2TB, you should config it to be:
  //   [{"/flash_path", 10GB}, {"/hard_drive", 2TB}]
  //
  // The system will try to guarantee data under each path is close to but
  // not larger than the target size. But current and future file sizes used
  // by determining where to place a file are based on best-effort estimation,
  // which means there is a chance that the actual size under the directory
  // is slightly more than target size under some workloads. User should give
  // some buffer room for those cases.
  //
  // If none of the paths has sufficient room to place a file, the file will
  // be placed to the last path anyway, despite to the target size.
  //
  // Placing newer data to ealier paths is also best-efforts. User should
  // expect user files to be placed in higher levels in some extreme cases.
  //
  // If left empty, only one path will be used, which is db_name passed when
  // opening the DB.
  // Default: empty
  std::vector<DbPath> db_paths;

  // This specifies the info LOG dir.
  // If it is empty, the log files will be in the same dir as data.
  // If it is non empty, the log files will be in the specified dir,
  // and the db data dir's absolute path will be used as the log file
  // name's prefix.
  std::string db_log_dir;

  // This specifies the absolute dir path for write-ahead logs (WAL).
  // If it is empty, the log files will be in the same dir as data,
  //   dbname is used as the data dir by default
  // If it is non empty, the log files will be in kept the specified dir.
  // When destroying the db,
  //   all log files in wal_dir and the dir itself is deleted
  std::string wal_dir;

  // The periodicity when obsolete files get deleted. The default
  // value is 6 hours. The files that get out of scope by compaction
  // process will still get automatically delete on every compaction,
  // regardless of this setting
  uint64_t delete_obsolete_files_period_micros;

  // Maximum number of concurrent background compaction jobs, submitted to
  // the default LOW priority thread pool.
  // If you're increasing this, also consider increasing number of threads in
  // LOW priority thread pool. For more information, see
  // Env::SetBackgroundThreads
  // Default: 1
  int max_background_compactions;

  // Maximum number of concurrent background memtable flush jobs, submitted to
  // the HIGH priority thread pool.
  //
  // By default, all background jobs (major compaction and memtable flush) go
  // to the LOW priority pool. If this option is set to a positive number,
  // memtable flush jobs will be submitted to the HIGH priority pool.
  // It is important when the same Env is shared by multiple db instances.
  // Without a separate pool, long running major compaction jobs could
  // potentially block memtable flush jobs of other db instances, leading to
  // unnecessary Put stalls.
  //
  // If you're increasing this, also consider increasing number of threads in
  // HIGH priority thread pool. For more information, see
  // Env::SetBackgroundThreads
  // Default: 1
  int max_background_flushes;

  // Specify the maximal size of the info log file. If the log file
  // is larger than `max_log_file_size`, a new info log file will
  // be created.
  // If max_log_file_size == 0, all logs will be written to one
  // log file.
  size_t max_log_file_size;

  // Time for the info log file to roll (in seconds).
  // If specified with non-zero value, log file will be rolled
  // if it has been active longer than `log_file_time_to_roll`.
  // Default: 0 (disabled)
  size_t log_file_time_to_roll;

  // Maximal info log files to be kept.
  // Default: 1000
  size_t keep_log_file_num;

  // manifest file is rolled over on reaching this limit.
  // The older manifest file be deleted.
  // The default value is MAX_INT so that roll-over does not take place.
  uint64_t max_manifest_file_size;

  // Number of shards used for table cache.
  int table_cache_numshardbits;

  // During data eviction of table's LRU cache, it would be inefficient
  // to strictly follow LRU because this piece of memory will not really
  // be released unless its refcount falls to zero. Instead, make two
  // passes: the first pass will release items with refcount = 1,
  // and if not enough space releases after scanning the number of
  // elements specified by this parameter, we will remove items in LRU
  // order.
  int table_cache_remove_scan_count_limit;

  // The following two fields affect how archived logs will be deleted.
  // 1. If both set to 0, logs will be deleted asap and will not get into
  //    the archive.
  // 2. If WAL_ttl_seconds is 0 and WAL_size_limit_MB is not 0,
  //    WAL files will be checked every 10 min and if total size is greater
  //    then WAL_size_limit_MB, they will be deleted starting with the
  //    earliest until size_limit is met. All empty files will be deleted.
  // 3. If WAL_ttl_seconds is not 0 and WAL_size_limit_MB is 0, then
  //    WAL files will be checked every WAL_ttl_secondsi / 2 and those that
  //    are older than WAL_ttl_seconds will be deleted.
  // 4. If both are not 0, WAL files will be checked every 10 min and both
  //    checks will be performed with ttl being first.
  uint64_t WAL_ttl_seconds;
  uint64_t WAL_size_limit_MB;

  // Number of bytes to preallocate (via fallocate) the manifest
  // files.  Default is 4mb, which is reasonable to reduce random IO
  // as well as prevent overallocation for mounts that preallocate
  // large amounts of data (such as xfs's allocsize option).
  size_t manifest_preallocation_size;

  // Data being read from file storage may be buffered in the OS
  // Default: true
  bool allow_os_buffer;

  // Allow the OS to mmap file for reading sst tables. Default: false
  bool allow_mmap_reads;

  // Allow the OS to mmap file for writing. Default: false
  bool allow_mmap_writes;

  // Disable child process inherit open files. Default: true
  bool is_fd_close_on_exec;

  // Skip log corruption error on recovery (If client is ok with
  // losing most recent changes)
  // Default: false
  bool skip_log_error_on_recovery;

  // if not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
  // Default: 3600 (1 hour)
  unsigned int stats_dump_period_sec;

  // If set true, will hint the underlying file system that the file
  // access pattern is random, when a sst file is opened.
  // Default: true
  bool advise_random_on_open;

  // Specify the file access pattern once a compaction is started.
  // It will be applied to all input files of a compaction.
  // Default: NORMAL
  enum {
    NONE,
    NORMAL,
    SEQUENTIAL,
    WILLNEED
  } access_hint_on_compaction_start;

  // Use adaptive mutex, which spins in the user space before resorting
  // to kernel. This could reduce context switch when the mutex is not
  // heavily contended. However, if the mutex is hot, we could end up
  // wasting spin time.
  // Default: false
  bool use_adaptive_mutex;

  // Allow RocksDB to use thread local storage to optimize performance.
  // Default: true
  bool allow_thread_local;

  // Create DBOptions with default values for all fields
  DBOptions();
  // Create DBOptions from Options
  explicit DBOptions(const Options& options);

  void Dump(Logger* log) const;

  // Allows OS to incrementally sync files to disk while they are being
  // written, asynchronously, in the background.
  // Issue one request for every bytes_per_sync written. 0 turns it off.
  // Default: 0
  //
  // You may consider using rate_limiter to regulate write rate to device.
  // When rate limiter is enabled, it automatically enables bytes_per_sync
  // to 1MB.
  uint64_t bytes_per_sync;
};

// Options to control the behavior of a database (passed to DB::Open)
struct Options : public DBOptions, public ColumnFamilyOptions {
  // Create an Options object with default values for all fields.
  Options() :
    DBOptions(),
    ColumnFamilyOptions() {}

  Options(const DBOptions& db_options,
          const ColumnFamilyOptions& column_family_options)
      : DBOptions(db_options), ColumnFamilyOptions(column_family_options) {}

  void Dump(Logger* log) const;

  // Set appropriate parameters for bulk loading.
  // The reason that this is a function that returns "this" instead of a
  // constructor is to enable chaining of multiple similar calls in the future.
  //

  // All data will be in level 0 without any automatic compaction.
  // It's recommended to manually call CompactRange(NULL, NULL) before reading
  // from the database, because otherwise the read can be very slow.
  Options* PrepareForBulkLoad();
};

//
// An application can issue a read request (via Get/Iterators) and specify
// if that read should process data that ALREADY resides on a specified cache
// level. For example, if an application specifies kBlockCacheTier then the
// Get call will process data that is already processed in the memtable or
// the block cache. It will not page in data from the OS cache or data that
// resides in storage.
enum ReadTier {
  kReadAllTier = 0x0,    // data in memtable, block cache, OS cache or storage
  kBlockCacheTier = 0x1  // data in memtable or block cache
};

// Options that control read operations
struct ReadOptions {
  // If true, all data read from underlying storage will be
  // verified against corresponding checksums.
  // Default: true
  bool verify_checksums;

  // Should the "data block"/"index block"/"filter block" read for this
  // iteration be cached in memory?
  // Callers may wish to set this field to false for bulk scans.
  // Default: true
  bool fill_cache;

  // If this option is set and memtable implementation allows, Seek
  // might only return keys with the same prefix as the seek-key
  //
  // ! DEPRECATED: prefix_seek is on by default when prefix_extractor
  // is configured
  // bool prefix_seek;

  // If "snapshot" is non-nullptr, read as of the supplied snapshot
  // (which must belong to the DB that is being read and which must
  // not have been released).  If "snapshot" is nullptr, use an impliicit
  // snapshot of the state at the beginning of this read operation.
  // Default: nullptr
  const Snapshot* snapshot;

  // If "prefix" is non-nullptr, and ReadOptions is being passed to
  // db.NewIterator, only return results when the key begins with this
  // prefix.  This field is ignored by other calls (e.g., Get).
  // Options.prefix_extractor must also be set, and
  // prefix_extractor.InRange(prefix) must be true.  The iterator
  // returned by NewIterator when this option is set will behave just
  // as if the underlying store did not contain any non-matching keys,
  // with two exceptions.  Seek() only accepts keys starting with the
  // prefix, and SeekToLast() is not supported.  prefix filter with this
  // option will sometimes reduce the number of read IOPs.
  // Default: nullptr
  //
  // ! DEPRECATED
  // const Slice* prefix;

  // Specify if this read request should process data that ALREADY
  // resides on a particular cache. If the required data is not
  // found at the specified cache, then Status::Incomplete is returned.
  // Default: kReadAllTier
  ReadTier read_tier;

  // Specify to create a tailing iterator -- a special iterator that has a
  // view of the complete database (i.e. it can also be used to read newly
  // added data) and is optimized for sequential reads. It will return records
  // that were inserted into the database after the creation of the iterator.
  // Default: false
  // Not supported in ROCKSDB_LITE mode!
  bool tailing;

  // Enable a total order seek regardless of index format (e.g. hash index)
  // used in the table. Some table format (e.g. plain table) may not support
  // this option.
  bool total_order_seek;

  ReadOptions()
      : verify_checksums(true),
        fill_cache(true),
        snapshot(nullptr),
        read_tier(kReadAllTier),
        tailing(false),
        total_order_seek(false) {}
  ReadOptions(bool cksum, bool cache)
      : verify_checksums(cksum),
        fill_cache(cache),
        snapshot(nullptr),
        read_tier(kReadAllTier),
        tailing(false),
        total_order_seek(false) {}
};

// Options that control write operations
struct WriteOptions {
  // If true, the write will be flushed from the operating system
  // buffer cache (by calling WritableFile::Sync()) before the write
  // is considered complete.  If this flag is true, writes will be
  // slower.
  //
  // If this flag is false, and the machine crashes, some recent
  // writes may be lost.  Note that if it is just the process that
  // crashes (i.e., the machine does not reboot), no writes will be
  // lost even if sync==false.
  //
  // In other words, a DB write with sync==false has similar
  // crash semantics as the "write()" system call.  A DB write
  // with sync==true has similar crash semantics to a "write()"
  // system call followed by "fdatasync()".
  //
  // Default: false
  bool sync;

  // If true, writes will not first go to the write ahead log,
  // and the write may got lost after a crash.
  bool disableWAL;

  // If non-zero, then associated write waiting longer than the specified
  // time MAY be aborted and returns Status::TimedOut. A write that takes
  // less than the specified time is guaranteed to not fail with
  // Status::TimedOut.
  //
  // The number of times a write call encounters a timeout is recorded in
  // Statistics.WRITE_TIMEDOUT
  //
  // Default: 0
  uint64_t timeout_hint_us;

  // If true and if user is trying to write to column families that don't exist
  // (they were dropped),  ignore the write (don't return an error). If there
  // are multiple writes in a WriteBatch, other writes will succeed.
  // Default: false
  bool ignore_missing_column_families;

  WriteOptions()
      : sync(false),
        disableWAL(false),
        timeout_hint_us(0),
        ignore_missing_column_families(false) {}
};

// Options that control flush operations
struct FlushOptions {
  // If true, the flush will wait until the flush is done.
  // Default: true
  bool wait;

  FlushOptions() : wait(true) {}
};

// Get options based on some guidelines. Now only tune parameter based on
// flush/compaction and fill default parameters for other parameters.
// total_write_buffer_limit: budget for memory spent for mem tables
// read_amplification_threshold: comfortable value of read amplification
// write_amplification_threshold: comfortable value of write amplification.
// target_db_size: estimated total DB size.
extern Options GetOptions(size_t total_write_buffer_limit,
                          int read_amplification_threshold = 8,
                          int write_amplification_threshold = 32,
                          uint64_t target_db_size = 68719476736 /* 64GB */);
}  // namespace rocksdb

#endif  // STORAGE_ROCKSDB_INCLUDE_OPTIONS_H_
