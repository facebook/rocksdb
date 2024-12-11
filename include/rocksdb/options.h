// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/advanced_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/customizable.h"
#include "rocksdb/data_structure.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/listener.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/types.h"
#include "rocksdb/universal_compaction.h"
#include "rocksdb/version.h"
#include "rocksdb/write_buffer_manager.h"

#ifdef max
#undef max
#endif

namespace ROCKSDB_NAMESPACE {

class Cache;
class CompactionFilter;
class CompactionFilterFactory;
class Comparator;
class ConcurrentTaskLimiter;
class Env;
enum InfoLogLevel : unsigned char;
class SstFileManager;
class FilterPolicy;
class Logger;
class MergeOperator;
class Snapshot;
class MemTableRepFactory;
class RateLimiter;
class Slice;
class Statistics;
class InternalKeyComparator;
class WalFilter;
class FileSystem;

struct Options;
struct DbPath;

using FileTypeSet = SmallEnumSet<FileType, FileType::kBlobFile>;

struct ColumnFamilyOptions : public AdvancedColumnFamilyOptions {
  // The function recovers options to a previous version. Only 4.6 or later
  // versions are supported.
  // NOT MAINTAINED: This function has not been and is not maintained.
  // DEPRECATED: This function might be removed in a future release.
  // In general, defaults are changed to suit broad interests. Opting
  // out of a change on upgrade should be deliberate and considered.
  ColumnFamilyOptions* OldDefaults(int rocksdb_major_version = 4,
                                   int rocksdb_minor_version = 6);

  // Some functions that make it easier to optimize RocksDB
  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  // An optional cache object is passed in to be used as the block cache
  ColumnFamilyOptions* OptimizeForSmallDb(
      std::shared_ptr<Cache>* cache = nullptr);

  // Use this if you don't need to keep the data sorted, i.e. you'll never use
  // an iterator, only Put() and Get() API calls
  //
  ColumnFamilyOptions* OptimizeForPointLookup(uint64_t block_cache_size_mb);

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
  const Comparator* comparator = BytewiseComparator();

  // REQUIRES: The client must provide a merge operator if Merge operation
  // needs to be accessed. Calling Merge on a DB without a merge operator
  // would result in Status::NotSupported. The client must ensure that the
  // merge operator supplied here has the same name and *exactly* the same
  // semantics as the merge operator provided to previous open calls on
  // the same DB. The only exception is reserved for upgrade, where a DB
  // previously without a merge operator is introduced to Merge operation
  // for the first time. It's necessary to specify a merge operator when
  // opening the DB in this case.
  // Default: nullptr
  std::shared_ptr<MergeOperator> merge_operator = nullptr;

  // A single CompactionFilter instance to call into during compaction.
  // Allows an application to modify/delete a key-value during background
  // compaction.
  //
  // If the client requires a new `CompactionFilter` to be used for different
  // compaction runs and/or requires a `CompactionFilter` for table file
  // creations outside of compaction, it can specify compaction_filter_factory
  // instead of this option.  The client should specify only one of the two.
  // compaction_filter takes precedence over compaction_filter_factory if
  // client specifies both.
  //
  // If multithreaded compaction is being used, the supplied CompactionFilter
  // instance may be used from different threads concurrently and so should be
  // thread-safe.
  //
  // Default: nullptr
  const CompactionFilter* compaction_filter = nullptr;

  // This is a factory that provides `CompactionFilter` objects which allow
  // an application to modify/delete a key-value during table file creation.
  //
  // Unlike the `compaction_filter` option, which is used when compaction
  // creates a table file, this factory allows using a `CompactionFilter` when a
  // table file is created for various reasons. The factory can decide what
  // `TableFileCreationReason`s use a `CompactionFilter`. For compatibility, by
  // default the decision is to use a `CompactionFilter` for
  // `TableFileCreationReason::kCompaction` only.
  //
  // Each thread of work involving creating table files will create a new
  // `CompactionFilter` when it will be used according to the above
  // `TableFileCreationReason`-based decision. This allows the application to
  // know about the different ongoing threads of work and makes it unnecessary
  // for `CompactionFilter` to provide thread-safety.
  //
  // Default: nullptr
  std::shared_ptr<CompactionFilterFactory> compaction_filter_factory = nullptr;

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
  // Note that write_buffer_size is enforced per column family.
  // See db_write_buffer_size for sharing memory across column families.
  //
  // Default: 64MB
  //
  // Dynamically changeable through SetOptions() API
  size_t write_buffer_size = 64 << 20;

  // Compress blocks using the specified compression algorithm.
  //
  // Default: kSnappyCompression, if it's supported. If snappy is not linked
  // with the library, the default is kNoCompression.
  //
  // Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
  //    ~200-500MB/s compression
  //    ~400-800MB/s decompression
  //
  // Note that these speeds are significantly faster than most
  // persistent storage speeds, and therefore it is typically never
  // worth switching to kNoCompression.  Even if the input data is
  // incompressible, the kSnappyCompression implementation will
  // efficiently detect that and will switch to uncompressed mode.
  //
  // If you do not set `compression_opts.level`, or set it to
  // `CompressionOptions::kDefaultCompressionLevel`, we will attempt to pick the
  // default corresponding to `compression` as follows:
  //
  // - kZSTD: 3
  // - kZlibCompression: Z_DEFAULT_COMPRESSION (currently -1)
  // - kLZ4HCCompression: 0
  // - kLZ4: -1 (i.e., `acceleration=1`; see `CompressionOptions::level` doc)
  // - For all others, we do not specify a compression level
  //
  // Dynamically changeable through SetOptions() API
  CompressionType compression;

  // Compression algorithm that will be used for the bottommost level that
  // contain files. The behavior for num_levels = 1 is not well defined.
  // Right now, with num_levels = 1,  all compaction outputs will use
  // bottommost_compression and all flush outputs still use options.compression,
  // but the behavior is subject to change.
  //
  // Default: kDisableCompressionOption (Disabled)
  CompressionType bottommost_compression = kDisableCompressionOption;

  // different options for compression algorithms used by bottommost_compression
  // if it is enabled. To enable it, please see the definition of
  // CompressionOptions. Behavior for num_levels = 1 is the same as
  // options.bottommost_compression.
  CompressionOptions bottommost_compression_opts;

  // different options for compression algorithms
  CompressionOptions compression_opts;

  // Number of files to trigger level-0 compaction. A value <0 means that
  // level-0 compaction will not be triggered by number of files at all.
  //
  // Universal compaction: RocksDB will try to keep the number of sorted runs
  //   no more than this number. If CompactionOptionsUniversal::max_read_amp is
  //   set, then this option will be used only as a trigger to look for
  //   compaction. CompactionOptionsUniversal::max_read_amp will be the limit
  //   on the number of sorted runs.
  //
  // Default: 4
  //
  // Dynamically changeable through SetOptions() API
  int level0_file_num_compaction_trigger = 4;

  // If non-nullptr, use the specified function to put keys in contiguous
  // groups called "prefixes". These prefixes are used to place one
  // representative entry for the group into the Bloom filter
  // rather than an entry for each key (see whole_key_filtering).
  // Under certain conditions, this enables optimizing some range queries
  // (Iterators) in addition to some point lookups (Get/MultiGet).
  //
  // Together `prefix_extractor` and `comparator` must satisfy one essential
  // property for valid prefix filtering of range queries:
  //   If Compare(k1, k2) <= 0 and Compare(k2, k3) <= 0 and
  //      InDomain(k1) and InDomain(k3) and prefix(k1) == prefix(k3),
  //   Then InDomain(k2) and prefix(k2) == prefix(k1)
  //
  // In other words, all keys with the same prefix must be in a contiguous
  // group by comparator order, and cannot be interrupted by keys with no
  // prefix ("out of domain"). (This makes it valid to conclude that no
  // entries within some bounds are present if the upper and lower bounds
  // have a common prefix and no entries with that same prefix are present.)
  //
  // Some other properties are recommended but not strictly required. Under
  // most sensible comparators, the following will need to hold true to
  // satisfy the essential property above:
  // * "Prefix is a prefix": key.starts_with(prefix(key))
  // * "Prefixes preserve ordering": If Compare(k1, k2) <= 0, then
  //   Compare(prefix(k1), prefix(k2)) <= 0
  //
  // The next two properties ensure that seeking to a prefix allows
  // enumerating all entries with that prefix:
  // * "Prefix starts the group": Compare(prefix(key), key) <= 0
  // * "Prefix idempotent": prefix(prefix(key)) == prefix(key)
  //
  // Default: nullptr
  std::shared_ptr<const SliceTransform> prefix_extractor = nullptr;

  // Control maximum total data size for a level.
  // max_bytes_for_level_base is the max total for level-1.
  // Maximum number of bytes for level L can be calculated as
  // (max_bytes_for_level_base) * (max_bytes_for_level_multiplier ^ (L-1))
  // For example, if max_bytes_for_level_base is 200MB, and if
  // max_bytes_for_level_multiplier is 10, total data size for level-1
  // will be 200MB, total file size for level-2 will be 2GB,
  // and total file size for level-3 will be 20GB.
  //
  // Default: 256MB.
  //
  // Dynamically changeable through SetOptions() API
  uint64_t max_bytes_for_level_base = 256 * 1048576;

  // Deprecated.
  uint64_t snap_refresh_nanos = 0;

  // Disable automatic compactions. Manual compactions can still
  // be issued on this column family
  //
  // Dynamically changeable through SetOptions() API
  bool disable_auto_compactions = false;

  // This is a factory that provides TableFactory objects.
  // Default: a block-based table factory that provides a default
  // implementation of TableBuilder and TableReader with default
  // BlockBasedTableOptions.
  std::shared_ptr<TableFactory> table_factory;

  // A list of paths where SST files for this column family
  // can be put into, with its target size. Similar to db_paths,
  // newer data is placed into paths specified earlier in the
  // vector while older data gradually moves to paths specified
  // later in the vector.
  // Note that, if a path is supplied to multiple column
  // families, it would have files and total size from all
  // the column families combined. User should provision for the
  // total size(from all the column families) in such cases.
  //
  // If left empty, db_paths will be used.
  // Default: empty
  std::vector<DbPath> cf_paths;

  // Compaction concurrent thread limiter for the column family.
  // If non-nullptr, use given concurrent thread limiter to control
  // the max outstanding compaction tasks. Limiter can be shared with
  // multiple column families across db instances.
  //
  // Default: nullptr
  std::shared_ptr<ConcurrentTaskLimiter> compaction_thread_limiter = nullptr;

  // If non-nullptr, use the specified factory for a function to determine the
  // partitioning of sst files. This helps compaction to split the files
  // on interesting boundaries (key prefixes) to make propagation of sst
  // files less write amplifying (covering the whole key space).
  // THE FEATURE IS STILL EXPERIMENTAL
  //
  // Default: nullptr
  std::shared_ptr<SstPartitionerFactory> sst_partitioner_factory = nullptr;

  // RocksDB will try to flush the current memtable after the number of range
  // deletions is >= this limit. For workloads with many range
  // deletions, limiting the number of range deletions in memtable can help
  // prevent performance degradation and/or OOM caused by too many range
  // tombstones in a single memtable.
  //
  // Default: 0 (disabled)
  //
  // Dynamically changeable through SetOptions() API
  uint32_t memtable_max_range_deletions = 0;

  // EXPERIMENTAL
  // When > 0, RocksDB attempts to erase some block cache entries for files
  // that have become obsolete, which means they are about to be deleted.
  // To avoid excessive tracking, this "uncaching" process is iterative and
  // speculative, meaning it could incur extra background CPU effort if the
  // file's blocks are generally not cached. A larger number indicates more
  // willingness to spend CPU time to maximize block cache hit rates by
  // erasing known-obsolete entries.
  //
  // When uncache_aggressiveness=1, block cache entries for an obsolete file
  // are only erased until any attempted erase operation fails because the
  // block is not cached. Then no further attempts are made to erase cached
  // blocks for that file.
  //
  // For larger values, erasure is attempted until evidence incidates that the
  // chance of success is < 0.99^(a-1), where a = uncache_aggressiveness. For
  // example:
  // 2 -> Attempt only while expecting >= 99% successful/useful erasure
  // 11 -> 90%
  // 69 -> 50%
  // 110 -> 33%
  // 230 -> 10%
  // 460 -> 1%
  // 690 -> 0.1%
  // 1000 -> 1 in 23000
  // 10000 -> Always (for all practical purposes)
  // NOTE: UINT32_MAX and nearby values could take additional special meanings
  // in the future.
  //
  // Pinned cache entries (guaranteed present) are always erased if
  // uncache_aggressiveness > 0, but are not used in predicting the chances of
  // successful erasure of non-pinned entries.
  //
  // NOTE: In the case of copied DBs (such as Checkpoints) sharing a block
  // cache, it is possible that a file becoming obsolete doesn't mean its
  // block cache entries (shared among copies) are obsolete. Such a scenerio
  // is the best case for uncache_aggressiveness = 0.
  //
  // When using allow_mmap_reads=true, this option is ignored (no un-caching).
  //
  // Once validated in production, the default will likely change to something
  // around 300.
  uint32_t uncache_aggressiveness = 0;

  // Create ColumnFamilyOptions with default values for all fields
  ColumnFamilyOptions();
  // Create ColumnFamilyOptions from Options
  explicit ColumnFamilyOptions(const Options& options);

  void Dump(Logger* log) const;
};

enum class WALRecoveryMode : char {
  // Original levelDB recovery
  //
  // We tolerate the last record in any log to be incomplete due to a crash
  // while writing it. Zeroed bytes from preallocation are also tolerated in the
  // trailing data of any log.
  //
  // Use case: Applications for which updates, once applied, must not be rolled
  // back even after a crash-recovery. In this recovery mode, RocksDB guarantees
  // this as long as `WritableFile::Append()` writes are durable. In case the
  // user needs the guarantee in more situations (e.g., when
  // `WritableFile::Append()` writes to page cache, but the user desires this
  // guarantee in face of power-loss crash-recovery), RocksDB offers various
  // mechanisms to additionally invoke `WritableFile::Sync()` in order to
  // strengthen the guarantee.
  //
  // This differs from `kPointInTimeRecovery` in that, in case a corruption is
  // detected during recovery, this mode will refuse to open the DB. Whereas,
  // `kPointInTimeRecovery` will stop recovery just before the corruption since
  // that is a valid point-in-time to which to recover.
  kTolerateCorruptedTailRecords = 0x00,
  // Recover from clean shutdown
  // We don't expect to find any corruption in the WAL
  // Use case : This is ideal for unit tests and rare applications that
  // can require high consistency guarantee
  kAbsoluteConsistency = 0x01,
  // Recover to point-in-time consistency (default)
  // We stop the WAL playback on discovering WAL inconsistency
  // Use case : Ideal for systems that have disk controller cache like
  // hard disk, SSD without super capacitor that store related data
  kPointInTimeRecovery = 0x02,
  // Recovery after a disaster
  // We ignore any corruption in the WAL and try to salvage as much data as
  // possible
  // Use case : Ideal for last ditch effort to recover data or systems that
  // operate with low grade unrelated data
  kSkipAnyCorruptedRecords = 0x03,
};

struct DbPath {
  std::string path;
  uint64_t target_size;  // Target size of total files under the path, in byte.

  DbPath() : target_size(0) {}
  DbPath(const std::string& p, uint64_t t) : path(p), target_size(t) {}
};

extern const char* kHostnameForDbHostId;

enum class CompactionServiceJobStatus : char {
  kSuccess,
  kFailure,
  kUseLocal,
};

struct CompactionServiceJobInfo {
  std::string db_name;
  std::string db_id;
  std::string db_session_id;
  uint64_t job_id;  // job_id is only unique within the current DB and session,
                    // restart DB will reset the job_id. `db_id` and
                    // `db_session_id` could help you build unique id across
                    // different DBs and sessions.

  Env::Priority priority;

  // Additional Compaction Details that can be useful in the CompactionService
  CompactionReason compaction_reason;
  bool is_full_compaction;
  bool is_manual_compaction;
  bool bottommost_level;

  CompactionServiceJobInfo(std::string db_name_, std::string db_id_,
                           std::string db_session_id_, uint64_t job_id_,
                           Env::Priority priority_,
                           CompactionReason compaction_reason_,
                           bool is_full_compaction_, bool is_manual_compaction_,
                           bool bottommost_level_)
      : db_name(std::move(db_name_)),
        db_id(std::move(db_id_)),
        db_session_id(std::move(db_session_id_)),
        job_id(job_id_),
        priority(priority_),
        compaction_reason(compaction_reason_),
        is_full_compaction(is_full_compaction_),
        is_manual_compaction(is_manual_compaction_),
        bottommost_level(bottommost_level_) {}
};

struct CompactionServiceScheduleResponse {
  std::string scheduled_job_id;  // Generated outside of primary host, unique
                                 // across different DBs and sessions
  CompactionServiceJobStatus status;
  CompactionServiceScheduleResponse(std::string scheduled_job_id_,
                                    CompactionServiceJobStatus status_)
      : scheduled_job_id(scheduled_job_id_), status(status_) {}
  explicit CompactionServiceScheduleResponse(CompactionServiceJobStatus status_)
      : status(status_) {}
};

// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class CompactionService : public Customizable {
 public:
  static const char* Type() { return "CompactionService"; }

  // Returns the name of this compaction service.
  const char* Name() const override = 0;

  // Schedule compaction to be processed remotely.
  virtual CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& /*info*/,
      const std::string& /*compaction_service_input*/) {
    CompactionServiceScheduleResponse response(
        CompactionServiceJobStatus::kUseLocal);
    return response;
  }

  // Wait for the scheduled compaction to finish from the remote worker
  virtual CompactionServiceJobStatus Wait(
      const std::string& /*scheduled_job_id*/, std::string* /*result*/) {
    return CompactionServiceJobStatus::kUseLocal;
  }

  // Optional callback function upon Installation.
  virtual void OnInstallation(const std::string& /*scheduled_job_id*/,
                              CompactionServiceJobStatus /*status*/) {}

  ~CompactionService() override = default;
};

struct DBOptions {
  // The function recovers options to the option as in version 4.6.
  // NOT MAINTAINED: This function has not been and is not maintained.
  // DEPRECATED: This function might be removed in a future release.
  // In general, defaults are changed to suit broad interests. Opting
  // out of a change on upgrade should be deliberate and considered.
  DBOptions* OldDefaults(int rocksdb_major_version = 4,
                         int rocksdb_minor_version = 6);

  // Some functions that make it easier to optimize RocksDB

  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  // An optional cache object is passed in for the memory of the
  // memtable to cost to
  DBOptions* OptimizeForSmallDb(std::shared_ptr<Cache>* cache = nullptr);

  // By default, RocksDB uses only one background thread for flush and
  // compaction. Calling this function will set it up such that total of
  // `total_threads` is used. Good value for `total_threads` is the number of
  // cores. You almost definitely want to call this function if your system is
  // bottlenecked by RocksDB.
  DBOptions* IncreaseParallelism(int total_threads = 16);

  // If true, the database will be created if it is missing.
  // Default: false
  bool create_if_missing = false;

  // If true, missing column families will be automatically created on
  // DB::Open().
  // Default: false
  bool create_missing_column_families = false;

  // If true, an error is raised if the database already exists.
  // Default: false
  bool error_if_exists = false;

  // If true, RocksDB does some pro-active and generally inexpensive checks
  // for DB or data corruption, on top of usual protections such as block
  // checksums. True also enters a read-only mode when a DB write fails;
  // see DB::Resume().
  //
  // As most workloads value data correctness over availability, this option
  // is on by default. Note that the name of this old option is potentially
  // misleading, and other options and operations go further in proactive
  // checking for corruption, including
  // * paranoid_file_checks
  // * paranoid_memory_checks
  // * DB::VerifyChecksum()
  //
  // Default: true
  bool paranoid_checks = true;

  // DEPRECATED: This option might be removed in a future release.
  //
  // If true, during memtable flush, RocksDB will validate total entries
  // read in flush, and compare with counter inserted into it.
  //
  // The option is here to turn the feature off in case this new validation
  // feature has a bug. The option may be removed in the future once the
  // feature is stable.
  //
  // Default: true
  bool flush_verify_memtable_count = true;

  // DEPRECATED: This option might be removed in a future release.
  //
  // If true, during compaction, RocksDB will count the number of entries
  // read and compare it against the number of entries in the compaction
  // input files. This is intended to add protection against corruption
  // during compaction. Note that
  // - this verification is not done for compactions during which a compaction
  // filter returns kRemoveAndSkipUntil, and
  // - the number of range deletions is not verified.
  //
  // The option is here to turn the feature off in case this new validation
  // feature has a bug. The option may be removed in the future once the
  // feature is stable.
  //
  // Default: true
  bool compaction_verify_record_count = true;

  // If true, the log numbers and sizes of the synced WALs are tracked
  // in MANIFEST. During DB recovery, if a synced WAL is missing
  // from disk, or the WAL's size does not match the recorded size in
  // MANIFEST, an error will be reported and the recovery will be aborted.
  //
  // This is one additional protection against WAL corruption besides the
  // per-WAL-entry checksum.
  //
  // Note that this option does not work with secondary instance.
  // Currently, only syncing closed WALs are tracked. Calling `DB::SyncWAL()`,
  // etc. or writing with `WriteOptions::sync=true` to sync the live WAL is not
  // tracked for performance/efficiency reasons.
  //
  // Default: false
  bool track_and_verify_wals_in_manifest = false;

  // If true, verifies the SST unique id between MANIFEST and actual file
  // each time an SST file is opened. This check ensures an SST file is not
  // overwritten or misplaced. A corruption error will be reported if mismatch
  // detected, but only when MANIFEST tracks the unique id, which starts from
  // RocksDB version 7.3. Although the tracked internal unique id is related
  // to the one returned by GetUniqueIdFromTableProperties, that is subject to
  // change.
  // NOTE: verification is currently only done on SST files using block-based
  // table format.
  //
  // Setting to false should only be needed in case of unexpected problems.
  //
  // Although an early version of this option opened all SST files for
  // verification on DB::Open, that is no longer guaranteed. However, as
  // documented in an above option, if max_open_files is -1, DB will open all
  // files on DB::Open().
  //
  // Default: true
  bool verify_sst_unique_id_in_manifest = true;

  // Use the specified object to interact with the environment,
  // e.g. to read/write files, schedule background work, etc.
  // Default: Env::Default()
  Env* env = Env::Default();

  // Limits internal file read/write bandwidth:
  //
  // - Flush requests write bandwidth at `Env::IOPriority::IO_HIGH`
  // - Compaction requests read and write bandwidth at
  //   `Env::IOPriority::IO_LOW`
  // - Reads associated with a `ReadOptions` can be charged at
  //   `ReadOptions::rate_limiter_priority` (see that option's API doc for usage
  //   and limitations).
  // - Writes associated with a `WriteOptions` can be charged at
  //   `WriteOptions::rate_limiter_priority` (see that option's API doc for
  //   usage and limitations).
  //
  // Rate limiting is disabled if nullptr. If rate limiter is enabled,
  // bytes_per_sync is set to 1MB by default.
  //
  // Default: nullptr
  std::shared_ptr<RateLimiter> rate_limiter = nullptr;

  // Use to track SST files and control their file deletion rate.
  //
  // Features:
  //  - Throttle the deletion rate of the SST files.
  //  - Keep track the total size of all SST files.
  //  - Set a maximum allowed space limit for SST files that when reached
  //    the DB wont do any further flushes or compactions and will set the
  //    background error.
  //  - Can be shared between multiple dbs.
  // Limitations:
  //  - Only track and throttle deletes of SST files in
  //    first db_path (db_name if db_paths is empty).
  //
  // Default: nullptr
  std::shared_ptr<SstFileManager> sst_file_manager = nullptr;

  // Any internal progress/error information generated by the db will
  // be written to info_log if it is non-nullptr, or to a file stored
  // in the same directory as the DB contents if info_log is nullptr.
  // Default: nullptr
  std::shared_ptr<Logger> info_log = nullptr;

  // Minimum level for sending log messages to info_log. The default is
  // INFO_LEVEL when RocksDB is compiled in release mode, and DEBUG_LEVEL
  // when it is compiled in debug mode.
  InfoLogLevel info_log_level = Logger::kDefaultLogLevel;

  // Number of open files that can be used by the DB.  You may need to
  // increase this if your database has a large working set. Value -1 means
  // files opened are always kept open. You can estimate number of files based
  // on target_file_size_base and target_file_size_multiplier for level-based
  // compaction. For universal-style compaction, you can usually set it to -1.
  //
  // A high value or -1 for this option can cause high memory usage.
  // See BlockBasedTableOptions::cache_usage_options to constrain
  // memory usage in case of block based table format.
  //
  // Default: -1
  //
  // Dynamically changeable through SetDBOptions() API.
  int max_open_files = -1;

  // If max_open_files is -1, DB will open all files on DB::Open(). You can
  // use this option to increase the number of threads used to open the files.
  // Default: 16
  int max_file_opening_threads = 16;

  // Once write-ahead logs exceed this size, we will start forcing the flush of
  // column families whose memtables are backed by the oldest live WAL file
  // (i.e. the ones that are causing all the space amplification). If set to 0
  // (default), we will dynamically choose the WAL size limit to be
  // [sum of all write_buffer_size * max_write_buffer_number] * 4
  //
  // For example, with 15 column families, each with
  // write_buffer_size = 128 MB
  // max_write_buffer_number = 6
  // max_total_wal_size will be calculated to be [15 * 128MB * 6] * 4 = 45GB
  //
  // The RocksDB wiki has some discussion about how the WAL interacts
  // with memtables and flushing of column families.
  // https://github.com/facebook/rocksdb/wiki/Column-Families
  //
  // This option takes effect only when there are more than one column
  // family as otherwise the wal size is dictated by the write_buffer_size.
  //
  // Default: 0
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t max_total_wal_size = 0;

  // If non-null, then we should collect metrics about database operations
  std::shared_ptr<Statistics> statistics = nullptr;

  // By default, writes to stable storage use fdatasync (on platforms
  // where this function is available). If this option is true,
  // fsync is used instead.
  //
  // fsync and fdatasync are equally safe for our purposes and fdatasync is
  // faster, so it is rarely necessary to set this option. It is provided
  // as a workaround for kernel/filesystem bugs, such as one that affected
  // fdatasync with ext4 in kernel versions prior to 3.7.
  bool use_fsync = false;

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
  // Placing newer data to earlier paths is also best-efforts. User should
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
  std::string db_log_dir = "";

  // This specifies the absolute dir path for write-ahead logs (WAL).
  // If it is empty, the log files will be in the same dir as data,
  //   dbname is used as the data dir by default
  // If it is non empty, the log files will be in kept the specified dir.
  // When destroying the db,
  //   all log files in wal_dir and the dir itself is deleted
  std::string wal_dir = "";

  // The periodicity when obsolete files get deleted. The default
  // value is 6 hours. The files that get out of scope by compaction
  // process will still get automatically delete on every compaction,
  // regardless of this setting
  //
  // Default: 6 hours
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t delete_obsolete_files_period_micros = 6ULL * 60 * 60 * 1000000;

  // Maximum number of concurrent background jobs (compactions and flushes).
  //
  // Default: 2
  //
  // Dynamically changeable through SetDBOptions() API.
  int max_background_jobs = 2;

  // DEPRECATED: RocksDB automatically decides this based on the
  // value of max_background_jobs. For backwards compatibility we will set
  // `max_background_jobs = max_background_compactions + max_background_flushes`
  // in the case where user sets at least one of `max_background_compactions` or
  // `max_background_flushes` (we replace -1 by 1 in case one option is unset).
  //
  // Maximum number of concurrent background compaction jobs, submitted to
  // the default LOW priority thread pool.
  //
  // If you're increasing this, also consider increasing number of threads in
  // LOW priority thread pool. For more information, see
  // Env::SetBackgroundThreads
  //
  // Default: -1
  //
  // Dynamically changeable through SetDBOptions() API.
  int max_background_compactions = -1;

  // This value represents the maximum number of threads that will
  // concurrently perform a compaction job by breaking it into multiple,
  // smaller ones that are run simultaneously.
  // Default: 1 (i.e. no subcompactions)
  //
  // Dynamically changeable through SetDBOptions() API.
  uint32_t max_subcompactions = 1;

  // DEPRECATED: RocksDB automatically decides this based on the
  // value of max_background_jobs. For backwards compatibility we will set
  // `max_background_jobs = max_background_compactions + max_background_flushes`
  // in the case where user sets at least one of `max_background_compactions` or
  // `max_background_flushes`.
  //
  // Maximum number of concurrent background memtable flush jobs, submitted by
  // default to the HIGH priority thread pool. If the HIGH priority thread pool
  // is configured to have zero threads, flush jobs will share the LOW priority
  // thread pool with compaction jobs.
  //
  // It is important to use both thread pools when the same Env is shared by
  // multiple db instances. Without a separate pool, long running compaction
  // jobs could potentially block memtable flush jobs of other db instances,
  // leading to unnecessary Put stalls.
  //
  // If you're increasing this, also consider increasing number of threads in
  // HIGH priority thread pool. For more information, see
  // Env::SetBackgroundThreads
  // Default: -1
  int max_background_flushes = -1;

  // Specify the maximal size of the info log file. If the log file
  // is larger than `max_log_file_size`, a new info log file will
  // be created.
  // If max_log_file_size == 0, all logs will be written to one
  // log file.
  size_t max_log_file_size = 0;

  // Time for the info log file to roll (in seconds).
  // If specified with non-zero value, log file will be rolled
  // if it has been active longer than `log_file_time_to_roll`.
  // Default: 0 (disabled)
  size_t log_file_time_to_roll = 0;

  // Maximal info log files to be kept.
  // Default: 1000
  size_t keep_log_file_num = 1000;

  // Recycle log files.
  // If non-zero, we will reuse previously written log files for new
  // logs, overwriting the old data.  The value indicates how many
  // such files we will keep around at any point in time for later
  // use.  This is more efficient because the blocks are already
  // allocated and fdatasync does not need to update the inode after
  // each write.
  // Default: 0
  size_t recycle_log_file_num = 0;

  // manifest file is rolled over on reaching this limit.
  // The older manifest file be deleted.
  // The default value is 1GB so that the manifest file can grow, but not
  // reach the limit of storage capacity.
  uint64_t max_manifest_file_size = 1024 * 1024 * 1024;

  // Number of shards used for table cache.
  int table_cache_numshardbits = 6;

  // The following two fields affect when WALs will be archived and deleted.
  //
  // When both are zero, obsolete WALs will not be archived and will be deleted
  // immediately. Otherwise, obsolete WALs will be archived prior to deletion.
  //
  // When `WAL_size_limit_MB` is nonzero, archived WALs starting with the
  // earliest will be deleted until the total size of the archive falls below
  // this limit. All empty WALs will be deleted.
  //
  // When `WAL_ttl_seconds` is nonzero, archived WALs older than
  // `WAL_ttl_seconds` will be deleted.
  //
  // When only `WAL_ttl_seconds` is nonzero, the frequency at which archived
  // WALs are deleted is every `WAL_ttl_seconds / 2` seconds. When only
  // `WAL_size_limit_MB` is nonzero, the deletion frequency is every ten
  // minutes. When both are nonzero, the deletion frequency is the minimum of
  // those two values.
  uint64_t WAL_ttl_seconds = 0;
  uint64_t WAL_size_limit_MB = 0;

  // Number of bytes to preallocate (via fallocate) the manifest
  // files.  Default is 4mb, which is reasonable to reduce random IO
  // as well as prevent overallocation for mounts that preallocate
  // large amounts of data (such as xfs's allocsize option).
  size_t manifest_preallocation_size = 4 * 1024 * 1024;

  // Allow the OS to mmap file for reading sst tables.
  // Not recommended for 32-bit OS.
  // When the option is set to true and compression is disabled, the blocks
  // will not be copied and will be read directly from the mmap-ed memory
  // area, and the block will not be inserted into the block cache. However,
  // checksums will still be checked if ReadOptions.verify_checksums is set
  // to be true. It means a checksum check every time a block is read, more
  // than the setup where the option is set to false and the block cache is
  // used. The common use of the options is to run RocksDB on ramfs, where
  // checksum verification is usually not needed.
  // Default: false
  bool allow_mmap_reads = false;

  // Allow the OS to mmap file for writing.
  // DB::SyncWAL() only works if this is set to false.
  // Default: false
  bool allow_mmap_writes = false;

  // Enable direct I/O mode for read/write
  // they may or may not improve performance depending on the use case
  //
  // Files will be opened in "direct I/O" mode
  // which means that data r/w from the disk will not be cached or
  // buffered. The hardware buffer of the devices may however still
  // be used. Memory mapped files are not impacted by these parameters.

  // Use O_DIRECT for user and compaction reads.
  // Default: false
  bool use_direct_reads = false;

  // Use O_DIRECT for writes in background flush and compactions.
  // Default: false
  bool use_direct_io_for_flush_and_compaction = false;

  // If false, fallocate() calls are bypassed, which disables file
  // preallocation. The file space preallocation is used to increase the file
  // write/append performance. By default, RocksDB preallocates space for WAL,
  // SST, Manifest files, the extra space is truncated when the file is written.
  // Warning: if you're using btrfs, we would recommend setting
  // `allow_fallocate=false` to disable preallocation. As on btrfs, the extra
  // allocated space cannot be freed, which could be significant if you have
  // lots of files. More details about this limitation:
  // https://github.com/btrfs/btrfs-dev-docs/blob/471c5699336e043114d4bca02adcd57d9dab9c44/data-extent-reference-counts.md
  bool allow_fallocate = true;

  // Disable child process inherit open files. Default: true
  bool is_fd_close_on_exec = true;

  // if not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
  //
  // Default: 600 (10 min)
  //
  // Dynamically changeable through SetDBOptions() API.
  unsigned int stats_dump_period_sec = 600;

  // if not zero, dump rocksdb.stats to RocksDB every stats_persist_period_sec
  // Default: 600
  unsigned int stats_persist_period_sec = 600;

  // If true, automatically persist stats to a hidden column family (column
  // family name: ___rocksdb_stats_history___) every
  // stats_persist_period_sec seconds; otherwise, write to an in-memory
  // struct. User can query through `GetStatsHistory` API.
  // If user attempts to create a column family with the same name on a DB
  // which have previously set persist_stats_to_disk to true, the column family
  // creation will fail, but the hidden column family will survive, as well as
  // the previously persisted statistics.
  // When peristing stats to disk, the stat name will be limited at 100 bytes.
  // Default: false
  bool persist_stats_to_disk = false;

  // if not zero, periodically take stats snapshots and store in memory, the
  // memory size for stats snapshots is capped at stats_history_buffer_size
  // Default: 1MB
  size_t stats_history_buffer_size = 1024 * 1024;

  // If set true, will hint the underlying file system that the file
  // access pattern is random, when a sst file is opened.
  // Default: true
  bool advise_random_on_open = true;

  // Amount of data to build up in memtables across all column
  // families before writing to disk.
  //
  // This is distinct from write_buffer_size, which enforces a limit
  // for a single memtable.
  //
  // This feature is disabled by default. Specify a non-zero value
  // to enable it.
  //
  // Default: 0 (disabled)
  size_t db_write_buffer_size = 0;

  // The memory usage of memtable will report to this object. The same object
  // can be passed into multiple DBs and it will track the sum of size of all
  // the DBs. If the total size of all live memtables of all the DBs exceeds
  // a limit, a flush will be triggered in the next DB to which the next write
  // is issued, as long as there is one or more column family not already
  // flushing.
  //
  // If the object is only passed to one DB, the behavior is the same as
  // db_write_buffer_size. When write_buffer_manager is set, the value set will
  // override db_write_buffer_size.
  //
  // This feature is disabled by default. Specify a non-zero value
  // to enable it.
  //
  // Default: null
  std::shared_ptr<WriteBufferManager> write_buffer_manager = nullptr;

  // If non-zero, we perform bigger reads when doing compaction. If you're
  // running RocksDB on spinning disks, you should set this to at least 2MB.
  // That way RocksDB's compaction is doing sequential instead of random reads.
  //
  // Default: 2MB
  //
  // Dynamically changeable through SetDBOptions() API.
  size_t compaction_readahead_size = 2 * 1024 * 1024;

  // This is a maximum buffer size that is used by WinMmapReadableFile in
  // unbuffered disk I/O mode. We need to maintain an aligned buffer for
  // reads. We allow the buffer to grow until the specified value and then
  // for bigger requests allocate one shot buffers. In unbuffered mode we
  // always bypass read-ahead buffer at ReadaheadRandomAccessFile
  // When read-ahead is required we then make use of compaction_readahead_size
  // value and always try to read ahead. With read-ahead we always
  // pre-allocate buffer to the size instead of growing it up to a limit.
  //
  // This option is currently honored only on Windows
  //
  // Default: 1 Mb
  //
  // Special value: 0 - means do not maintain per instance buffer. Allocate
  //                per request buffer and avoid locking.
  size_t random_access_max_buffer_size = 1024 * 1024;

  // This is the maximum buffer size that is used by WritableFileWriter.
  // With direct IO, we need to maintain an aligned buffer for writes.
  // We allow the buffer to grow until it's size hits the limit in buffered
  // IO and fix the buffer size when using direct IO to ensure alignment of
  // write requests if the logical sector size is unusual
  //
  // Default: 1024 * 1024 (1 MB)
  //
  // Dynamically changeable through SetDBOptions() API.
  size_t writable_file_max_buffer_size = 1024 * 1024;

  // Use adaptive mutex, which spins in the user space before resorting
  // to kernel. This could reduce context switch when the mutex is not
  // heavily contended. However, if the mutex is hot, we could end up
  // wasting spin time.
  // Default: false
  bool use_adaptive_mutex = false;

  // Create DBOptions with default values for all fields
  DBOptions();
  // Create DBOptions from Options
  explicit DBOptions(const Options& options);

  void Dump(Logger* log) const;

  // Allows OS to incrementally sync files to disk while they are being
  // written, asynchronously, in the background. This operation can be used
  // to smooth out write I/Os over time. Users shouldn't rely on it for
  // persistence guarantee.
  // Issue one request for every bytes_per_sync written. 0 turns it off.
  //
  // You may consider using rate_limiter to regulate write rate to device.
  // When rate limiter is enabled, it automatically enables bytes_per_sync
  // to 1MB.
  //
  // This option applies to table files
  //
  // Default: 0, turned off
  //
  // Note: DOES NOT apply to WAL files. See wal_bytes_per_sync instead
  // Dynamically changeable through SetDBOptions() API.
  uint64_t bytes_per_sync = 0;

  // Same as bytes_per_sync, but applies to WAL files
  // This does not gaurantee the WALs are synced in the order of creation. New
  // WAL can be synced while an older WAL doesn't. Therefore upon system crash,
  // this hole in the WAL data can create partial data loss.
  //
  // Default: 0, turned off
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t wal_bytes_per_sync = 0;

  // When true, guarantees WAL files have at most `wal_bytes_per_sync`
  // bytes submitted for writeback at any given time, and SST files have at most
  // `bytes_per_sync` bytes pending writeback at any given time. This can be
  // used to handle cases where processing speed exceeds I/O speed during file
  // generation, which can lead to a huge sync when the file is finished, even
  // with `bytes_per_sync` / `wal_bytes_per_sync` properly configured.
  //
  //  - If `sync_file_range` is supported it achieves this by waiting for any
  //    prior `sync_file_range`s to finish before proceeding. In this way,
  //    processing (compression, etc.) can proceed uninhibited in the gap
  //    between `sync_file_range`s, and we block only when I/O falls behind.
  //  - Otherwise the `WritableFile::Sync` method is used. Note this mechanism
  //    always blocks, thus preventing the interleaving of I/O and processing.
  //
  // Note: Enabling this option does not provide any additional persistence
  // guarantees, as it may use `sync_file_range`, which does not write out
  // metadata.
  //
  // Default: false
  bool strict_bytes_per_sync = false;

  // A vector of EventListeners whose callback functions will be called
  // when specific RocksDB event happens.
  std::vector<std::shared_ptr<EventListener>> listeners;

  // If true, then the status of the threads involved in this DB will
  // be tracked and available via GetThreadList() API.
  //
  // Default: false
  bool enable_thread_tracking = false;

  // The limited write rate to DB if soft_pending_compaction_bytes_limit or
  // level0_slowdown_writes_trigger is triggered, or we are writing to the
  // last mem table allowed and we allow more than 3 mem tables. It is
  // calculated using size of user write requests before compression.
  // RocksDB may decide to slow down more if the compaction still
  // gets behind further.
  // If the value is 0, we will infer a value from `rater_limiter` value
  // if it is not empty, or 16MB if `rater_limiter` is empty. Note that
  // if users change the rate in `rate_limiter` after DB is opened,
  // `delayed_write_rate` won't be adjusted.
  //
  // Unit: byte per second.
  //
  // Default: 0
  //
  // Dynamically changeable through SetDBOptions() API.
  uint64_t delayed_write_rate = 0;

  // By default, a single write thread queue is maintained. The thread gets
  // to the head of the queue becomes write batch group leader and responsible
  // for writing to WAL and memtable for the batch group.
  //
  // If enable_pipelined_write is true, separate write thread queue is
  // maintained for WAL write and memtable write. A write thread first enter WAL
  // writer queue and then memtable writer queue. Pending thread on the WAL
  // writer queue thus only have to wait for previous writers to finish their
  // WAL writing but not the memtable writing. Enabling the feature may improve
  // write throughput and reduce latency of the prepare phase of two-phase
  // commit.
  //
  // Default: false
  bool enable_pipelined_write = false;

  // Setting unordered_write to true trades higher write throughput with
  // relaxing the immutability guarantee of snapshots. This violates the
  // repeatability one expects from ::Get from a snapshot, as well as
  // ::MultiGet and Iterator's consistent-point-in-time view property.
  // If the application cannot tolerate the relaxed guarantees, it can implement
  // its own mechanisms to work around that and yet benefit from the higher
  // throughput. Using TransactionDB with WRITE_PREPARED write policy and
  // two_write_queues=true is one way to achieve immutable snapshots despite
  // unordered_write.
  //
  // By default, i.e., when it is false, rocksdb does not advance the sequence
  // number for new snapshots unless all the writes with lower sequence numbers
  // are already finished. This provides the immutability that we expect from
  // snapshots. Moreover, since Iterator and MultiGet internally depend on
  // snapshots, the snapshot immutability results into Iterator and MultiGet
  // offering consistent-point-in-time view. If set to true, although
  // Read-Your-Own-Write property is still provided, the snapshot immutability
  // property is relaxed: the writes issued after the snapshot is obtained (with
  // larger sequence numbers) will be still not visible to the reads from that
  // snapshot, however, there still might be pending writes (with lower sequence
  // number) that will change the state visible to the snapshot after they are
  // landed to the memtable.
  //
  // Default: false
  bool unordered_write = false;

  // If true, allow multi-writers to update mem tables in parallel.
  // Only some memtable_factory-s support concurrent writes; currently it
  // is implemented only for SkipListFactory.  Concurrent memtable writes
  // are not compatible with inplace_update_support or filter_deletes.
  // It is strongly recommended to set enable_write_thread_adaptive_yield
  // if you are going to use this feature.
  //
  // Default: true
  bool allow_concurrent_memtable_write = true;

  // If true, threads synchronizing with the write batch group leader will
  // wait for up to write_thread_max_yield_usec before blocking on a mutex.
  // This can substantially improve throughput for concurrent workloads,
  // regardless of whether allow_concurrent_memtable_write is enabled.
  //
  // Default: true
  bool enable_write_thread_adaptive_yield = true;

  // The maximum limit of number of bytes that are written in a single batch
  // of WAL or memtable write. It is followed when the leader write size
  // is larger than 1/8 of this limit.
  //
  // Default: 1 MB
  uint64_t max_write_batch_group_size_bytes = 1 << 20;

  // The maximum number of microseconds that a write operation will use
  // a yielding spin loop to coordinate with other write threads before
  // blocking on a mutex.  (Assuming write_thread_slow_yield_usec is
  // set properly) increasing this value is likely to increase RocksDB
  // throughput at the expense of increased CPU usage.
  //
  // Default: 100
  uint64_t write_thread_max_yield_usec = 100;

  // The latency in microseconds after which a std::this_thread::yield
  // call (sched_yield on Linux) is considered to be a signal that
  // other processes or threads would like to use the current core.
  // Increasing this makes writer threads more likely to take CPU
  // by spinning, which will show up as an increase in the number of
  // involuntary context switches.
  //
  // Default: 3
  uint64_t write_thread_slow_yield_usec = 3;

  // If true, then DB::Open() will not update the statistics used to optimize
  // compaction decision by loading table properties from many files.
  // Turning off this feature will improve DBOpen time especially in
  // disk environment.
  //
  // Default: false
  bool skip_stats_update_on_db_open = false;

  // If true, then DB::Open() will not fetch and check sizes of all sst files.
  // This may significantly speed up startup if there are many sst files,
  // especially when using non-default Env with expensive GetFileSize().
  // We'll still check that all required sst files exist.
  // If paranoid_checks is false, this option is ignored, and sst files are
  // not checked at all.
  //
  // Default: false
  bool skip_checking_sst_file_sizes_on_db_open = false;

  // Recovery mode to control the consistency while replaying WAL
  // Default: kPointInTimeRecovery
  WALRecoveryMode wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  // if set to false then recovery will fail when a prepared
  // transaction is encountered in the WAL
  bool allow_2pc = false;

  // A global cache for table-level rows.
  // Used to speed up Get() queries.
  // NOTE: does not work with DeleteRange() yet.
  // Default: nullptr (disabled)
  std::shared_ptr<RowCache> row_cache = nullptr;

  // A filter object supplied to be invoked while processing write-ahead-logs
  // (WALs) during recovery. The filter provides a way to inspect log
  // records, ignoring a particular record or skipping replay.
  // The filter is invoked at startup and is invoked from a single-thread
  // currently.
  WalFilter* wal_filter = nullptr;

  // DEPRECATED: This option might be removed in a future release.
  //
  // If true, then DB::Open, CreateColumnFamily, DropColumnFamily, and
  // SetOptions will fail if options file is not properly persisted.
  //
  // DEFAULT: true
  bool fail_if_options_file_error = true;

  // If true, then print malloc stats together with rocksdb.stats
  // when printing to LOG.
  // DEFAULT: false
  bool dump_malloc_stats = false;

  // By default RocksDB replay WAL logs and flush them on DB open, which may
  // create very small SST files. If this option is enabled, RocksDB will try
  // to avoid (but not guarantee not to) flush during recovery. Also, existing
  // WAL logs will be kept, so that if crash happened before flush, we still
  // have logs to recover from.
  //
  // DEFAULT: false
  bool avoid_flush_during_recovery = false;

  // By default RocksDB will flush all memtables on DB close if there are
  // unpersisted data (i.e. with WAL disabled) The flush can be skip to speedup
  // DB close. Unpersisted data WILL BE LOST.
  //
  // DEFAULT: false
  //
  // Dynamically changeable through SetDBOptions() API.
  bool avoid_flush_during_shutdown = false;

  // Set this option to true during creation of database if you want
  // to be able to ingest behind (call IngestExternalFile() skipping keys
  // that already exist, rather than overwriting matching keys).
  // Setting this option to true has the following effects:
  // 1) Disable some internal optimizations around SST file compression.
  // 2) Reserve the last level for ingested files only.
  // 3) Compaction will not include any file from the last level.
  // Note that only Universal Compaction supports allow_ingest_behind.
  // `num_levels` should be >= 3 if this option is turned on.
  //
  //
  // DEFAULT: false
  // Immutable.
  bool allow_ingest_behind = false;

  // If enabled it uses two queues for writes, one for the ones with
  // disable_memtable and one for the ones that also write to memtable. This
  // allows the memtable writes not to lag behind other writes. It can be used
  // to optimize MySQL 2PC in which only the commits, which are serial, write to
  // memtable.
  bool two_write_queues = false;

  // If true WAL is not flushed automatically after each write. Instead it
  // relies on manual invocation of FlushWAL to write the WAL buffer to its
  // file.
  bool manual_wal_flush = false;

  // If enabled WAL records will be compressed before they are written. Only
  // ZSTD (= kZSTD) is supported (until streaming support is adapted for other
  // compression types). Compressed WAL records will be read in supported
  // versions (>= RocksDB 7.4.0 for ZSTD) regardless of this setting when
  // the WAL is read.
  CompressionType wal_compression = kNoCompression;

  // Set to true to re-instate an old behavior of keeping complete, synced WAL
  // files open for write until they are collected for deletion by a
  // background thread. This should not be needed unless there is a
  // performance issue with file Close(), but setting it to true means that
  // Checkpoint might call LinkFile on a WAL still open for write, which might
  // be unsupported on some FileSystem implementations. As this is intended as
  // a temporary kill switch, it is already DEPRECATED.
  bool background_close_inactive_wals = false;

  // If true, RocksDB supports flushing multiple column families and committing
  // their results atomically to MANIFEST. Note that it is not
  // necessary to set atomic_flush to true if WAL is always enabled since WAL
  // allows the database to be restored to the last persistent state in WAL.
  // This option is useful when there are column families with writes NOT
  // protected by WAL.
  // For manual flush, application has to specify which column families to
  // flush atomically in DB::Flush.
  // For auto-triggered flush, RocksDB atomically flushes ALL column families.
  //
  // Currently, any WAL-enabled writes after atomic flush may be replayed
  // independently if the process crashes later and tries to recover.
  bool atomic_flush = false;

  // If true, working thread may avoid doing unnecessary and long-latency
  // operation (such as deleting obsolete files directly or deleting memtable)
  // and will instead schedule a background job to do it.
  // Use it if you're latency-sensitive.
  // If set to true, takes precedence over
  // ReadOptions::background_purge_on_iterator_cleanup.
  bool avoid_unnecessary_blocking_io = false;

  // The DB unique ID can be saved in the DB manifest (preferred, this option)
  // or an IDENTITY file (historical, deprecated), or both. If this option is
  // set to false (old behavior), then write_identity_file must be set to true.
  // The manifest is preferred because
  // 1. The IDENTITY file is not checksummed, so it is not as safe against
  //    corruption.
  // 2. The IDENTITY file may or may not be copied with the DB (e.g. not
  //    copied by BackupEngine), so is not reliable for the provenance of a DB.
  // This option might eventually be obsolete and removed as Identity files
  // are phased out.
  bool write_dbid_to_manifest = true;

  // It is expected that the Identity file will be obsoleted by recording
  // DB ID in the manifest (see write_dbid_to_manifest). Setting this to true
  // maintains the historical behavior of writing an Identity file, while
  // setting to false is expected to be the future default. This option might
  // eventually be obsolete and removed as Identity files are phased out.
  bool write_identity_file = true;

  // Historically, when prefix_extractor != nullptr, iterators have an
  // unfortunate default semantics of *possibly* only returning data
  // within the same prefix. To avoid "spooky action at a distance," iterator
  // bounds should come from the instantiation or seeking of the iterator,
  // not from a mutable column family option.
  //
  // When set to true, it is as if every iterator is created with
  // total_order_seek=true and only auto_prefix_mode=true and
  // prefix_same_as_start=true can take advantage of prefix seek optimizations.
  bool prefix_seek_opt_in_only = false;

  // The number of bytes to prefetch when reading the log. This is mostly useful
  // for reading a remotely located log, as it can save the number of
  // round-trips. If 0, then the prefetching is disabled.
  //
  // Default: 0
  size_t log_readahead_size = 0;

  // If user does NOT provide the checksum generator factory, the file checksum
  // will NOT be used. A new file checksum generator object will be created
  // when a SST file is created. Therefore, each created FileChecksumGenerator
  // will only be used from a single thread and so does not need to be
  // thread-safe.
  //
  // Default: nullptr
  std::shared_ptr<FileChecksumGenFactory> file_checksum_gen_factory = nullptr;

  // By default, RocksDB will attempt to detect any data losses or corruptions
  // in DB files and return an error to the user, either at DB::Open time or
  // later during DB operation. The exception to this policy is the WAL file,
  // whose recovery is controlled by the wal_recovery_mode option.
  //
  // Best-efforts recovery (this option set to true) signals a preference for
  // opening the DB to any point-in-time valid state for each column family,
  // including the empty/new state, versus the default of returning non-WAL
  // data losses to the user as errors. In terms of RocksDB user data, this
  // is like applying WALRecoveryMode::kPointInTimeRecovery to each column
  // family rather than just the WAL.
  //
  // The behavior changes in the presence of "AtomicGroup"s in the MANIFEST,
  // which is currently only the case when `atomic_flush == true`. In that
  // case, all pre-existing CFs must recover the atomic group in order for
  // that group to be applied in an all-or-nothing manner. This means that
  // unused/inactive CF(s) with invalid filesystem state can block recovery of
  // all other CFs at an atomic group.
  //
  // Best-efforts recovery (BER) is specifically designed to recover a DB with
  // files that are missing or truncated to some smaller size, such as the
  // result of an incomplete DB "physical" (FileSystem) copy. BER can also
  // detect when an SST file has been replaced with a different one of the
  // same size (assuming SST unique IDs are tracked in DB manifest).
  // BER is not yet designed to produce a usable DB from other corruptions to
  // DB files (which should generally be detectable by DB::VerifyChecksum()),
  // and BER does not yet attempt to recover any WAL files.
  //
  // For example, if an SST or blob file referenced by the MANIFEST is missing,
  // BER might be able to find a set of files corresponding to an old "point in
  // time" version of the column family, possibly from an older MANIFEST
  // file.
  // Besides complete "point in time" version, an incomplete version with
  // only a suffix of L0 files missing can also be recovered to if the
  // versioning history doesn't include an atomic flush.  From the users'
  // perspective, missing a suffix of L0 files means missing the
  // user's most recently written data. So the remaining available files still
  // presents a valid point in time view, although for some previous time. It's
  // not done for atomic flush because that guarantees a consistent view across
  // column families. We cannot guarantee that if recovering an incomplete
  // version.
  // Some other kinds of DB files (e.g. CURRENT, LOCK, IDENTITY) are
  // either ignored or replaced with BER, or quietly fixed regardless of BER
  // setting. BER does require at least one valid MANIFEST to recover to a
  // non-trivial DB state, unlike `ldb repair`.
  //
  // Default: false
  bool best_efforts_recovery = false;

  // It defines how many times DB::Resume() is called by a separate thread when
  // background retryable IO Error happens. When background retryable IO
  // Error happens, SetBGError is called to deal with the error. If the error
  // can be auto-recovered (e.g., retryable IO Error during Flush or WAL write),
  // then db resume is called in background to recover from the error. If this
  // value is 0 or negative, DB::Resume() will not be called automatically.
  //
  // Default: INT_MAX
  int max_bgerror_resume_count = INT_MAX;

  // If max_bgerror_resume_count is >= 2, db resume is called multiple times.
  // This option decides how long to wait to retry the next resume if the
  // previous resume fails and satisfy redo resume conditions.
  //
  // Default: 1000000 (microseconds).
  uint64_t bgerror_resume_retry_interval = 1000000;

  // It allows user to opt-in to get error messages containing corrupted
  // keys/values. Corrupt keys, values will be logged in the
  // messages/logs/status that will help users with the useful information
  // regarding affected data. By default value is set false to prevent users
  // data to be exposed in the logs/messages etc.
  //
  // Default: false
  bool allow_data_in_errors = false;

  // A string identifying the machine hosting the DB. This
  // will be written as a property in every SST file written by the DB (or
  // by offline writers such as SstFileWriter and RepairDB). It can be useful
  // for troubleshooting in memory corruption caused by a failing host when
  // writing a file, by tracing back to the writing host. These corruptions
  // may not be caught by the checksum since they happen before checksumming.
  // If left as default, the table writer will substitute it with the actual
  // hostname when writing the SST file. If set to an empty string, the
  // property will not be written to the SST file.
  //
  // Default: hostname
  std::string db_host_id = kHostnameForDbHostId;

  // Use this if your DB want to enable checksum handoff for specific file
  // types writes. Make sure that the File_system you use support the
  // crc32c checksum verification
  // Currently supported file tyes: kWALFile, kTableFile, kDescriptorFile.
  // NOTE: currently RocksDB only generates crc32c based checksum for the
  // handoff. If the storage layer has different checksum support, user
  // should enble this set as empty. Otherwise,it may cause unexpected
  // write failures.
  FileTypeSet checksum_handoff_file_types;

  // EXPERIMENTAL
  // CompactionService is a feature allows the user to run compactions on a
  // different host or process, which offloads the background load from the
  // primary host.
  // It's an experimental feature, the interface will be changed without
  // backward/forward compatibility support for now. Some known issues are still
  // under development.
  std::shared_ptr<CompactionService> compaction_service = nullptr;

  // It indicates, which lowest cache tier we want to
  // use for a certain DB. Currently we support volatile_tier and
  // non_volatile_tier. They are layered. By setting it to kVolatileTier, only
  // the block cache (current implemented volatile_tier) is used. So
  // cache entries will not spill to secondary cache (current
  // implemented non_volatile_tier), and block cache lookup misses will not
  // lookup in the secondary cache. When kNonVolatileBlockTier is used, we use
  // both block cache and secondary cache.
  //
  // Default: kNonVolatileBlockTier
  CacheTier lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier;

  // DEPRECATED: This option might be removed in a future release.
  //
  // If set to false, when compaction or flush sees a SingleDelete followed by
  // a Delete for the same user key, compaction job will not fail.
  // Otherwise, compaction job will fail.
  // This is a temporary option to help existing use cases migrate, and
  // will be removed in a future release.
  // Warning: do not set to false unless you are trying to migrate existing
  // data in which the contract of single delete
  // (https://github.com/facebook/rocksdb/wiki/Single-Delete) is not enforced,
  // thus has Delete mixed with SingleDelete for the same user key. Violation
  // of the contract leads to undefined behaviors with high possibility of data
  // inconsistency, e.g. deleted old data become visible again, etc.
  bool enforce_single_del_contracts = true;

  // Implementing off-peak duration awareness in RocksDB. In this context,
  // "off-peak time" signifies periods characterized by significantly less read
  // and write activity compared to other times. By leveraging this knowledge,
  // we can prevent low-priority tasks, such as TTL-based compactions, from
  // competing with read and write operations during peak hours. Essentially, we
  // preprocess these tasks during the preceding off-peak period, just before
  // the next peak cycle begins. For example, if the TTL is configured for 25
  // days, we may compact the files during the off-peak hours of the 24th day.
  //
  // Time of the day in UTC, start_time-end_time inclusive.
  // Format - HH:mm-HH:mm (00:00-23:59)
  // If the start time > end time, it will be considered that the time period
  // spans to the next day (e.g., 23:30-04:00). To make an entire day off-peak,
  // use "0:00-23:59". To make an entire day have no offpeak period, leave
  // this field blank. Default: Empty string (no offpeak).
  std::string daily_offpeak_time_utc = "";

  // EXPERIMENTAL

  // When a RocksDB database is opened in follower mode, this option
  // is set by the user to request the frequency of the follower
  // attempting to refresh its view of the leader. RocksDB may choose to
  // trigger catch ups more frequently if it detects any changes in the
  // database state.
  // Default every 10s.
  uint64_t follower_refresh_catchup_period_ms = 10000;

  // For a given catch up attempt, this option specifies the number of times
  // to tail the MANIFEST and try to install a new, consistent  version before
  // giving up. Though it should be extremely rare, the catch up may fail if
  // the leader is mutating the LSM at a very high rate and the follower is
  // unable to get a consistent view.
  // Default to 10 attempts
  uint64_t follower_catchup_retry_count = 10;

  // Time to wait between consecutive catch up attempts
  // Default 100ms
  uint64_t follower_catchup_retry_wait_ms = 100;

  // When DB files other than SST, blob and WAL files are created, use this
  // filesystem temperature. (See also `wal_write_temperature` and various
  // `*_temperature` CF options.) When not `kUnknown`, this overrides any
  // temperature set by OptimizeForManifestWrite functions.
  Temperature metadata_write_temperature = Temperature::kUnknown;

  // Use this filesystem temperature when creating WAL files. When not
  // `kUnknown`, this overrides any temperature set by OptimizeForLogWrite
  // functions.
  Temperature wal_write_temperature = Temperature::kUnknown;
  // End EXPERIMENTAL
};

// Options to control the behavior of a database (passed to DB::Open)
struct Options : public DBOptions, public ColumnFamilyOptions {
  // Create an Options object with default values for all fields.
  Options() : DBOptions(), ColumnFamilyOptions() {}

  Options(const DBOptions& db_options,
          const ColumnFamilyOptions& column_family_options)
      : DBOptions(db_options), ColumnFamilyOptions(column_family_options) {}

  // Change to some default settings from an older version.
  // NOT MAINTAINED: This function has not been and is not maintained.
  // DEPRECATED: This function might be removed in a future release.
  // In general, defaults are changed to suit broad interests. Opting
  // out of a change on upgrade should be deliberate and considered.
  Options* OldDefaults(int rocksdb_major_version = 4,
                       int rocksdb_minor_version = 6);

  void Dump(Logger* log) const;

  void DumpCFOptions(Logger* log) const;

  // Some functions that make it easier to optimize RocksDB

  // Set appropriate parameters for bulk loading.
  // The reason that this is a function that returns "this" instead of a
  // constructor is to enable chaining of multiple similar calls in the future.
  //

  // All data will be in level 0 without any automatic compaction.
  // It's recommended to manually call CompactRange(NULL, NULL) before reading
  // from the database, because otherwise the read can be very slow.
  Options* PrepareForBulkLoad();

  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  Options* OptimizeForSmallDb();

  // Disable some checks that should not be necessary in the absence of
  // software logic errors or CPU+memory hardware errors. This can improve
  // write speeds but is only recommended for temporary use. Does not
  // change protection against corrupt storage (e.g. verify_checksums).
  Options* DisableExtraChecks();
};

// An application can issue a read request (via Get/Iterators) and specify
// if that read should process data that ALREADY resides on a specified cache
// level. For example, if an application specifies kBlockCacheTier then the
// Get call will process data that is already processed in the memtable or
// the block cache. It will not page in data from the OS cache or data that
// resides in storage.
enum ReadTier {
  kReadAllTier = 0x0,     // data in memtable, block cache, OS cache or storage
  kBlockCacheTier = 0x1,  // data in memtable or block cache
  kPersistedTier = 0x2,   // persisted data.  When WAL is disabled, this option
                          // will skip data in memtable.
                          // Note that this ReadTier currently only supports
                          // Get and MultiGet and does not support iterators.
  kMemtableTier = 0x3     // data in memtable. used for memtable-only iterators.
};

// Options that control read operations
struct ReadOptions {
  // *** BEGIN options relevant to point lookups as well as scans ***

  // If "snapshot" is non-nullptr, read as of the supplied snapshot
  // (which must belong to the DB that is being read and which must
  // not have been released).  If "snapshot" is nullptr, use an implicit
  // snapshot of the state at the beginning of this read operation.
  const Snapshot* snapshot = nullptr;

  // Timestamp of operation. Read should return the latest data visible to the
  // specified timestamp. All timestamps of the same database must be of the
  // same length and format. The user is responsible for providing a customized
  // compare function via Comparator to order <key, timestamp> tuples.
  // For iterator, iter_start_ts is the lower bound (older) and timestamp
  // serves as the upper bound. Versions of the same record that fall in
  // the timestamp range will be returned. If iter_start_ts is nullptr,
  // only the most recent version visible to timestamp is returned.
  // The user-specified timestamp feature is still under active development,
  // and the API is subject to change.
  const Slice* timestamp = nullptr;
  const Slice* iter_start_ts = nullptr;

  // Deadline for completing an API call (Get/MultiGet/Seek/Next for now)
  // in microseconds.
  // It should be set to microseconds since epoch, i.e, gettimeofday or
  // equivalent plus allowed duration in microseconds. The best way is to use
  // env->NowMicros() + some timeout.
  // This is best efforts. The call may exceed the deadline if there is IO
  // involved and the file system doesn't support deadlines, or due to
  // checking for deadline periodically rather than for every key if
  // processing a batch
  std::chrono::microseconds deadline = std::chrono::microseconds::zero();

  // A timeout in microseconds to be passed to the underlying FileSystem for
  // reads. As opposed to deadline, this determines the timeout for each
  // individual file read request. If a MultiGet/Get/Seek/Next etc call
  // results in multiple reads, each read can last up to io_timeout us.
  std::chrono::microseconds io_timeout = std::chrono::microseconds::zero();

  // Specify if this read request should process data that ALREADY
  // resides on a particular cache. If the required data is not
  // found at the specified cache, then Status::Incomplete is returned.
  ReadTier read_tier = kReadAllTier;

  // For file reads associated with this option, charge the internal rate
  // limiter (see `DBOptions::rate_limiter`) at the specified priority. The
  // special value `Env::IO_TOTAL` disables charging the rate limiter.
  //
  // The rate limiting is bypassed no matter this option's value for file reads
  // on plain tables (these can exist when `ColumnFamilyOptions::table_factory`
  // is a `PlainTableFactory`) and cuckoo tables (these can exist when
  // `ColumnFamilyOptions::table_factory` is a `CuckooTableFactory`).
  //
  // The bytes charged to rate limiter may not exactly match the file read bytes
  // since there are some seemingly insignificant reads, like for file
  // headers/footers, that we currently do not charge to rate limiter.
  Env::IOPriority rate_limiter_priority = Env::IO_TOTAL;

  // It limits the maximum cumulative value size of the keys in batch while
  // reading through MultiGet. Once the cumulative value size exceeds this
  // soft limit then all the remaining keys are returned with status Aborted.
  uint64_t value_size_soft_limit = std::numeric_limits<uint64_t>::max();

  // When the number of merge operands applied exceeds this threshold
  // during a successful query, the operation will return a special OK
  // Status with subcode kMergeOperandThresholdExceeded. Currently only applies
  // to point lookups and is disabled by default.
  std::optional<size_t> merge_operand_count_threshold;

  // If true, all data read from underlying storage will be
  // verified against corresponding checksums.
  bool verify_checksums = true;

  // Should the "data block"/"index block" read for this iteration be placed in
  // block cache?
  // Callers may wish to set this field to false for bulk scans.
  // This would help not to the change eviction order of existing items in the
  // block cache.
  bool fill_cache = true;

  // If true, range tombstones handling will be skipped in key lookup paths.
  // For DB instances that don't use DeleteRange() calls, this setting can
  // be used to optimize the read performance.
  // Note that, if this assumption (of no previous DeleteRange() calls) is
  // broken, stale keys could be served in read paths.
  bool ignore_range_deletions = false;

  // If async_io is enabled, RocksDB will prefetch some of data asynchronously.
  // RocksDB apply it if reads are sequential and its internal automatic
  // prefetching.
  bool async_io = false;

  // Experimental
  //
  // If async_io is set, then this flag controls whether we read SST files
  // in multiple levels asynchronously. Enabling this flag can help reduce
  // MultiGet latency by maximizing the number of SST files read in
  // parallel if the keys in the MultiGet batch are in different levels. It
  // comes at the expense of slightly higher CPU overhead.
  bool optimize_multiget_for_io = true;

  // *** END options relevant to point lookups (as well as scans) ***
  // *** BEGIN options only relevant to iterators or scans ***

  // RocksDB does auto-readahead for iterators on noticing more than two reads
  // for a table file. The readahead starts at 8KB and doubles on every
  // additional read up to 256KB.
  // This option can help if most of the range scans are large, and if it is
  // determined that a larger readahead than that enabled by auto-readahead is
  // needed.
  // Using a large readahead size (> 2MB) can typically improve the performance
  // of forward iteration on spinning disks.
  size_t readahead_size = 0;

  // A threshold for the number of keys that can be skipped before failing an
  // iterator seek as incomplete. The default value of 0 should be used to
  // never fail a request as incomplete, even on skipping too many keys.
  uint64_t max_skippable_internal_keys = 0;

  // `iterate_lower_bound` defines the smallest key at which the backward
  // iterator can return an entry. Once the bound is passed, Valid() will be
  // false. `iterate_lower_bound` is inclusive ie the bound value is a valid
  // entry.
  //
  // If prefix_extractor is not null, the Seek target and `iterate_lower_bound`
  // need to have the same prefix. This is because ordering is not guaranteed
  // outside of prefix domain.
  //
  // In case of user_defined timestamp, if enabled, iterate_lower_bound should
  // point to key without timestamp part.
  const Slice* iterate_lower_bound = nullptr;

  // "iterate_upper_bound" defines the extent up to which the forward iterator
  // can return entries. Once the bound is reached, Valid() will be false.
  // "iterate_upper_bound" is exclusive ie the bound value is
  // not a valid entry. If prefix_extractor is not null:
  // 1. If options.auto_prefix_mode = true, iterate_upper_bound will be used
  //    to infer whether prefix iterating (e.g. applying prefix bloom filter)
  //    can be used within RocksDB. This is done by comparing
  //    iterate_upper_bound with the seek key.
  // 2. If options.auto_prefix_mode = false, iterate_upper_bound only takes
  //    effect if it shares the same prefix as the seek key. If
  //    iterate_upper_bound is outside the prefix of the seek key, then keys
  //    returned outside the prefix range will be undefined, just as if
  //    iterate_upper_bound = null.
  // If iterate_upper_bound is not null, SeekToLast() will position the iterator
  // at the first key smaller than iterate_upper_bound.
  //
  // In case of user_defined timestamp, if enabled, iterate_upper_bound should
  // point to key without timestamp part.
  const Slice* iterate_upper_bound = nullptr;

  // Specify to create a tailing iterator -- a special iterator that has a
  // view of the complete database (i.e. it can also be used to read newly
  // added data) and is optimized for sequential reads. It will return records
  // that were inserted into the database after the creation of the iterator.
  bool tailing = false;

  // This options is not used anymore. It was to turn on a functionality that
  // has been removed. DEPRECATED
  bool managed = false;

  // Enable a total order seek regardless of index format (e.g. hash index)
  // used in the table. Some table format (e.g. plain table) may not support
  // this option.
  // If true when calling Get(), we also skip prefix bloom when reading from
  // block based table, which only affects Get() performance.
  bool total_order_seek = false;

  // When true, by default use total_order_seek = true, and RocksDB can
  // selectively enable prefix seek mode if won't generate a different result
  // from total_order_seek, based on seek key, and iterator upper bound.
  // BUG: Using Comparator::IsSameLengthImmediateSuccessor and
  // SliceTransform::FullLengthEnabled to enable prefix mode in cases where
  // prefix of upper bound differs from prefix of seek key has a flaw.
  // If present in the DB, "short keys" (shorter than "full length" prefix)
  // can be omitted from auto_prefix_mode iteration when they would be present
  // in total_order_seek iteration, regardless of whether the short keys are
  // "in domain" of the prefix extractor. This is not an issue if no short
  // keys are added to DB or are not expected to be returned by such
  // iterators. (We are also assuming the new condition on
  // IsSameLengthImmediateSuccessor is satisfied; see its BUG section).
  // A bug example is in DBTest2::AutoPrefixMode1, search for "BUG".
  bool auto_prefix_mode = false;

  // Enforce that the iterator only iterates over the same prefix as the seek.
  // This makes the iterator bounds dependent on the column family's current
  // prefix_extractor, which is mutable. When SST files have been built with
  // the same prefix extractor, prefix filtering optimizations will be used
  // for both Seek and SeekForPrev.
  bool prefix_same_as_start = false;

  // Keep the blocks loaded by the iterator pinned in memory as long as the
  // iterator is not deleted, If used when reading from tables created with
  // BlockBasedTableOptions::use_delta_encoding = false,
  // Iterator's property "rocksdb.iterator.is-key-pinned" is guaranteed to
  // return 1.
  bool pin_data = false;

  // For iterators, RocksDB does auto-readahead on noticing more than two
  // sequential reads for a table file if user doesn't provide readahead_size.
  // The readahead starts at 8KB and doubles on every additional read upto
  // max_auto_readahead_size only when reads are sequential. However at each
  // level, if iterator moves over next file, readahead_size starts again from
  // 8KB.
  //
  // By enabling this option, RocksDB will do some enhancements for
  // prefetching the data.
  bool adaptive_readahead = false;

  // If true, when PurgeObsoleteFile is called in CleanupIteratorState, we
  // schedule a background job in the flush job queue and delete obsolete files
  // in background.
  bool background_purge_on_iterator_cleanup = false;

  // A callback to determine whether relevant keys for this scan exist in a
  // given table based on the table's properties. The callback is passed the
  // properties of each table during iteration. If the callback returns false,
  // the table will not be scanned. This option only affects Iterators and has
  // no impact on point lookups.
  // Default: empty (every table will be scanned)
  std::function<bool(const TableProperties&)> table_filter;

  // If auto_readahead_size is set to true, it will auto tune the readahead_size
  // during scans internally based on block cache data when block cache is
  // enabled, iteration upper bound when `iterate_upper_bound != nullptr` and
  // prefix when `prefix_same_as_start == true`
  //
  // Besides enabling block cache, it
  // also requires `iterate_upper_bound != nullptr` or  `prefix_same_as_start ==
  // true` for this option to take effect
  //
  // To be specific, it does the following:
  // (1) When `iterate_upper_bound`
  // is specified, trim the readahead so the readahead does not exceed iteration
  // upper bound
  // (2) When `prefix_same_as_start` is set to true, trim the
  // readahead so data blocks containing keys that are not in the same prefix as
  // the seek key in `Seek()` are not prefetched
  //  - Limition: `Seek(key)` instead of `SeekToFirst()` needs to be called in
  //  order for this trimming to take effect
  //
  // NOTE: - Used for forward Scans only.
  //       - If there is a backward scans, this option will be
  //          disabled internally and won't be enabled again if the forward scan
  //          is issued again.
  //
  // Default: true
  bool auto_readahead_size = true;

  // When set, the iterator may defer loading and/or preparing the value when
  // moving to a different entry (i.e. during SeekToFirst/SeekToLast/Seek/
  // SeekForPrev/Next/Prev operations). This can be used to save on I/O and/or
  // CPU when the values associated with certain keys may not be used by the
  // application. See also IteratorBase::PrepareValue().
  //
  // Note: this option currently only applies to 1) large values stored in blob
  // files using BlobDB and 2) multi-column-family iterators (CoalescingIterator
  // and AttributeGroupIterator). Otherwise, it has no effect.
  //
  // Default: false
  bool allow_unprepared_value = false;

  // *** END options only relevant to iterators or scans ***

  // *** BEGIN options for RocksDB internal use only ***

  // EXPERIMENTAL
  Env::IOActivity io_activity = Env::IOActivity::kUnknown;

  // *** END options for RocksDB internal use only ***

  ReadOptions() {}
  ReadOptions(bool _verify_checksums, bool _fill_cache);
  explicit ReadOptions(Env::IOActivity _io_activity);
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
  bool sync = false;

  // If true, writes will not first go to the write ahead log,
  // and the write may get lost after a crash. The backup engine
  // relies on write-ahead logs to back up the memtable, so if
  // you disable write-ahead logs, you must create backups with
  // flush_before_backup=true to avoid losing unflushed memtable data.
  // Default: false
  bool disableWAL = false;

  // If true and if user is trying to write to column families that don't exist
  // (they were dropped),  ignore the write (don't return an error). If there
  // are multiple writes in a WriteBatch, other writes will succeed.
  // Default: false
  bool ignore_missing_column_families = false;

  // If true and we need to wait or sleep for the write request, fails
  // immediately with Status::Incomplete().
  // Default: false
  bool no_slowdown = false;

  // If true, this write request is of lower priority if compaction is
  // behind. In this case, no_slowdown = true, the request will be canceled
  // immediately with Status::Incomplete() returned. Otherwise, it will be
  // slowed down. The slowdown value is determined by RocksDB to guarantee
  // it introduces minimum impacts to high priority writes.
  //
  // Default: false
  bool low_pri = false;

  // If true, this writebatch will maintain the last insert positions of each
  // memtable as hints in concurrent write. It can improve write performance
  // in concurrent writes if keys in one writebatch are sequential. In
  // non-concurrent writes (when concurrent_memtable_writes is false) this
  // option will be ignored.
  //
  // Default: false
  bool memtable_insert_hint_per_batch = false;

  // For writes associated with this option, charge the internal rate
  // limiter (see `DBOptions::rate_limiter`) at the specified priority. The
  // special value `Env::IO_TOTAL` disables charging the rate limiter.
  //
  // Currently the support covers automatic WAL flushes, which happen during
  // live updates (`Put()`, `Write()`, `Delete()`, etc.)
  // when `WriteOptions::disableWAL == false`
  // and `DBOptions::manual_wal_flush == false`.
  //
  // Only `Env::IO_USER` and `Env::IO_TOTAL` are allowed
  // due to implementation constraints.
  //
  // Default: `Env::IO_TOTAL`
  Env::IOPriority rate_limiter_priority = Env::IO_TOTAL;

  // `protection_bytes_per_key` is the number of bytes used to store
  // protection information for each key entry. Currently supported values are
  // zero (disabled) and eight.
  //
  // Default: zero (disabled).
  size_t protection_bytes_per_key = 0;

  // For RocksDB internal use only
  //
  // Default: Env::IOActivity::kUnknown.
  Env::IOActivity io_activity = Env::IOActivity::kUnknown;

  WriteOptions() {}
  explicit WriteOptions(Env::IOActivity _io_activity);
  explicit WriteOptions(
      Env::IOPriority _rate_limiter_priority,
      Env::IOActivity _io_activity = Env::IOActivity::kUnknown);
};

// Options that control flush operations
struct FlushOptions {
  // If true, the flush will wait until the flush is done.
  // Default: true
  bool wait;
  // If true, the flush would proceed immediately even it means writes will
  // stall for the duration of the flush; if false the operation will wait
  // until it's possible to do flush w/o causing stall or until required flush
  // is performed by someone else (foreground call or background thread).
  // Default: false
  bool allow_write_stall;

  FlushOptions() : wait(true), allow_write_stall(false) {}
};

// Create a Logger from provided DBOptions
Status CreateLoggerFromOptions(const std::string& dbname,
                               const DBOptions& options,
                               std::shared_ptr<Logger>* logger);

// CompactionOptions are used in CompactFiles() call.
struct CompactionOptions {
  // DEPRECATED: this option is unsafe because it allows the user to set any
  // `CompressionType` while always using `CompressionOptions` from the
  // `ColumnFamilyOptions`. As a result the `CompressionType` and
  // `CompressionOptions` can easily be inconsistent.
  //
  // Compaction output compression type
  //
  // Default: `kDisableCompressionOption`
  //
  // If set to `kDisableCompressionOption`, RocksDB will choose compression type
  // according to the `ColumnFamilyOptions`. RocksDB takes into account the
  // output level in case the `ColumnFamilyOptions` has level-specific settings.
  CompressionType compression;

  // Compaction will create files of size `output_file_size_limit`.
  // Default: MAX, which means that compaction will create a single file
  uint64_t output_file_size_limit;

  // If > 0, it will replace the option in the DBOptions for this compaction.
  uint32_t max_subcompactions;

  CompactionOptions()
      : compression(kDisableCompressionOption),
        output_file_size_limit(std::numeric_limits<uint64_t>::max()),
        max_subcompactions(0) {}
};

// For level based compaction, we can configure if we want to skip/force
// bottommost level compaction.
enum class BottommostLevelCompaction {
  // Skip bottommost level compaction.
  kSkip,
  // Only compact bottommost level if there is a compaction filter.
  // This is the default option.
  // Similar to kForceOptimized, when compacting bottommost level, avoid
  // double-compacting files
  // created in the same manual compaction.
  kIfHaveCompactionFilter,
  // Always compact bottommost level.
  kForce,
  // Always compact bottommost level but in bottommost level avoid
  // double-compacting files created in the same compaction.
  kForceOptimized,
};

// For manual compaction, we can configure if we want to skip/force garbage
// collection of blob files.
enum class BlobGarbageCollectionPolicy {
  // Force blob file garbage collection.
  kForce,
  // Skip blob file garbage collection.
  kDisable,
  // Inherit blob file garbage collection policy from ColumnFamilyOptions.
  kUseDefault,
};

// CompactRangeOptions is used by CompactRange() call.
struct CompactRangeOptions {
  // If true, no other compaction will run at the same time as this
  // manual compaction.
  //
  // Default: false
  bool exclusive_manual_compaction = false;

  // If true, compacted files will be moved to the minimum level capable
  // of holding the data or given level (specified non-negative target_level).
  bool change_level = false;
  // If change_level is true and target_level have non-negative value, compacted
  // files will be moved to target_level.
  int target_level = -1;
  // Compaction outputs will be placed in options.db_paths[target_path_id].
  // Behavior is undefined if target_path_id is out of range.
  uint32_t target_path_id = 0;
  // By default level based compaction will only compact the bottommost level
  // if there is a compaction filter
  BottommostLevelCompaction bottommost_level_compaction =
      BottommostLevelCompaction::kIfHaveCompactionFilter;
  // If true, will execute immediately even if doing so would cause the DB to
  // enter write stall mode. Otherwise, it'll sleep until load is low enough.
  bool allow_write_stall = false;
  // If > 0, it will replace the option in the DBOptions for this compaction.
  uint32_t max_subcompactions = 0;
  // Set user-defined timestamp low bound, the data with older timestamp than
  // low bound maybe GCed by compaction. Default: nullptr
  const Slice* full_history_ts_low = nullptr;

  // Allows cancellation of an in-progress manual compaction.
  //
  // Cancellation can be delayed waiting on automatic compactions when used
  // together with `exclusive_manual_compaction == true`.
  std::atomic<bool>* canceled = nullptr;
  // NOTE: Calling DisableManualCompaction() overwrites the uer-provided
  // canceled variable in CompactRangeOptions.
  // Typically, when CompactRange is being called in one thread (t1) with
  // canceled = false, and DisableManualCompaction is being called in the
  // other thread (t2), manual compaction is disabled normally, even if the
  // compaction iterator may still scan a few items before *canceled is
  // set to true

  // If set to kForce, RocksDB will override enable_blob_file_garbage_collection
  // to true; if set to kDisable, RocksDB will override it to false, and
  // kUseDefault leaves the setting in effect. This enables customers to both
  // force-enable and force-disable GC when calling CompactRange.
  BlobGarbageCollectionPolicy blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kUseDefault;

  // If set to < 0 or > 1, RocksDB leaves blob_garbage_collection_age_cutoff
  // from ColumnFamilyOptions in effect. Otherwise, it will override the
  // user-provided setting. This enables customers to selectively override the
  // age cutoff.
  double blob_garbage_collection_age_cutoff = -1;
};

// IngestExternalFileOptions is used by IngestExternalFile()
struct IngestExternalFileOptions {
  // Can be set to true to move the files instead of copying them.
  // The input files will be unlinked after successful ingestion.
  // The implementation depends on hard links (LinkFile) instead of traditional
  // move (RenameFile) to maximize the chances to restore to the original
  // state upon failure.
  bool move_files = false;
  // Same as move_files except that input files will NOT be unlinked.
  // Only one of `move_files` and `link_files` can be set at the same time.
  bool link_files = false;
  // If set to true, ingestion falls back to copy when hard linking fails.
  // This applies to both `move_files` and `link_files`.
  bool failed_move_fall_back_to_copy = true;
  // If set to false, an ingested file keys could appear in existing snapshots
  // that where created before the file was ingested.
  bool snapshot_consistency = true;
  // If set to false, IngestExternalFile() will fail if the file key range
  // overlaps with existing keys or tombstones or output of ongoing compaction
  // during file ingestion in the DB (the conditions under which a global_seqno
  // must be assigned to the ingested file).
  bool allow_global_seqno = true;
  // If set to false and the file key range overlaps with the memtable key range
  // (memtable flush required), IngestExternalFile will fail.
  bool allow_blocking_flush = true;
  // Set to true if you would like duplicate keys in the file being ingested
  // to be skipped rather than overwriting existing data under that key.
  // Use case: back-fill of some historical data in the database without
  // over-writing existing newer version of data.
  // This option could only be used if the DB has been running
  // with allow_ingest_behind=true since the dawn of time.
  // All files will be ingested at the bottommost level with seqno=0.
  bool ingest_behind = false;
  // DEPRECATED - Set to true if you would like to write global_seqno to
  // the external SST file on ingestion for backward compatibility before
  // RocksDB 5.16.0. Such old versions of RocksDB expect any global_seqno to
  // be written to the SST file rather than recorded in the DB manifest.
  // This functionality was deprecated because (a) random writes might be
  // costly or unsupported on some FileSystems, and (b) the file checksum
  // changes with such a write.
  bool write_global_seqno = false;
  // Set to true if you would like to verify the checksums of each block of the
  // external SST file before ingestion.
  // Warning: setting this to true causes slowdown in file ingestion because
  // the external SST file has to be read.
  bool verify_checksums_before_ingest = false;
  // When verify_checksums_before_ingest = true, RocksDB uses default
  // readahead setting to scan the file while verifying checksums before
  // ingestion.
  // Users can override the default value using this option.
  // Using a large readahead size (> 2MB) can typically improve the performance
  // of forward iteration on spinning disks.
  size_t verify_checksums_readahead_size = 0;
  // Set to TRUE if user wants to verify the sst file checksum of ingested
  // files. The DB checksum function will generate the checksum of each
  // ingested file (if file_checksum_gen_factory is set) and compare the
  // checksum function name and checksum with the ingested checksum information.
  //
  // If this option is set to True: 1) if DB does not enable checksum
  // (file_checksum_gen_factory == nullptr), the ingested checksum information
  // will be ignored; 2) If DB enable the checksum function, we calculate the
  // sst file checksum after the file is moved or copied and compare the
  // checksum and checksum name. If checksum or checksum function name does
  // not match, ingestion will be failed. If the verification is successful,
  // checksum and checksum function name will be stored in Manifest.
  // If this option is set to FALSE, 1) if DB does not enable checksum,
  // the ingested checksum information will be ignored; 2) if DB enable the
  // checksum, we only verify the ingested checksum function name and we
  // trust the ingested checksum. If the checksum function name matches, we
  // store the checksum in Manifest. DB does not calculate the checksum during
  // ingestion. However, if no checksum information is provided with the
  // ingested files, DB will generate the checksum and store in the Manifest.
  bool verify_file_checksum = true;
  // Set to TRUE if user wants file to be ingested to the last level. An
  // error of Status::TryAgain() will be returned if a file cannot fit in the
  // last level when calling
  // DB::IngestExternalFile()/DB::IngestExternalFiles(). The user should clear
  // the last level in the overlapping range before re-attempt.
  //
  // ingest_behind takes precedence over fail_if_not_bottommost_level.
  //
  // XXX: "bottommost" is obsolete/confusing terminology to refer to last level
  bool fail_if_not_bottommost_level = false;
  // EXPERIMENTAL
  // Enables ingestion of files not generated by SstFileWriter. When true:
  // - Allows files to be ingested when their cf_id doesn't match the CF they
  //   are being ingested into.
  // REQUIREMENTS:
  // - Ingested files must not overlap with existing keys.
  // - `write_global_seqno` must be false.
  // - All keys in ingested files should have sequence number 0. We fail
  // ingestion if any sequence numbers is non-zero.
  // WARNING: If a DB contains ingested files generated by another DB/CF,
  // RepairDB() may not recover these files correctly, potentially leading to
  // data loss.
  bool allow_db_generated_files = false;

  // Controls whether data and metadata blocks (e.g. index, filter) read during
  // file ingestion will be added to block cache.
  // Users may wish to set this to false when bulk loading into a CF that is not
  // available for reads yet.
  // When ingesting to multiple families, this option should be the same across
  // ingestion options.
  bool fill_cache = true;
};

enum TraceFilterType : uint64_t {
  // Trace all the operations
  kTraceFilterNone = 0x0,
  // Do not trace the get operations
  kTraceFilterGet = 0x1 << 0,
  // Do not trace the write operations
  kTraceFilterWrite = 0x1 << 1,
  // Do not trace the `Iterator::Seek()` operations
  kTraceFilterIteratorSeek = 0x1 << 2,
  // Do not trace the `Iterator::SeekForPrev()` operations
  kTraceFilterIteratorSeekForPrev = 0x1 << 3,
  // Do not trace the `MultiGet()` operations
  kTraceFilterMultiGet = 0x1 << 4,
};

// TraceOptions is used for StartTrace
struct TraceOptions {
  // To avoid the trace file size grows large than the storage space,
  // user can set the max trace file size in Bytes. Default is 64GB
  uint64_t max_trace_file_size = uint64_t{64} * 1024 * 1024 * 1024;
  // Specify trace sampling option, i.e. capture one per how many requests.
  // Default to 1 (capture every request).
  uint64_t sampling_frequency = 1;
  // Note: The filtering happens before sampling.
  uint64_t filter = kTraceFilterNone;
  // When true, the order of write records in the trace will match the order of
  // the corresponding write records in the WAL and applied to the DB. There may
  // be a performance penalty associated with preserving this ordering.
  //
  // Default: false. This means write records in the trace may be in an order
  // different from the WAL's order.
  bool preserve_write_order = false;
};

// ImportColumnFamilyOptions is used by ImportColumnFamily()
struct ImportColumnFamilyOptions {
  // Can be set to true to move the files instead of copying them.
  bool move_files = false;
};

// Options used with DB::GetApproximateSizes()
struct SizeApproximationOptions {
  // Defines whether the returned size should include the recently written
  // data in the memtables. If set to false, include_files must be true.
  bool include_memtables = false;
  // Defines whether the returned size should include data serialized to disk.
  // If set to false, include_memtables must be true.
  bool include_files = true;
  // When approximating the files total size that is used to store a keys range
  // using DB::GetApproximateSizes, allow approximation with an error margin of
  // up to total_files_size * files_size_error_margin. This allows to take some
  // shortcuts in files size approximation, resulting in better performance,
  // while guaranteeing the resulting error is within a reasonable margin.
  // E.g., if the value is 0.1, then the error margin of the returned files size
  // approximation will be within 10%.
  // If the value is non-positive - a more precise yet more CPU intensive
  // estimation is performed.
  double files_size_error_margin = -1.0;
};

struct CompactionServiceOptionsOverride {
  Env* env = Env::Default();
  std::shared_ptr<FileChecksumGenFactory> file_checksum_gen_factory = nullptr;

  const Comparator* comparator = BytewiseComparator();
  std::shared_ptr<MergeOperator> merge_operator = nullptr;
  const CompactionFilter* compaction_filter = nullptr;
  std::shared_ptr<CompactionFilterFactory> compaction_filter_factory = nullptr;
  std::shared_ptr<const SliceTransform> prefix_extractor = nullptr;
  std::shared_ptr<TableFactory> table_factory;
  std::shared_ptr<SstPartitionerFactory> sst_partitioner_factory = nullptr;

  // Only subsets of events are triggered in remote compaction worker, like:
  // `OnTableFileCreated`, `OnTableFileCreationStarted`,
  // `ShouldBeNotifiedOnFileIO` `OnSubcompactionBegin`,
  // `OnSubcompactionCompleted`, etc. Worth mentioning, `OnCompactionBegin` and
  // `OnCompactionCompleted` won't be triggered. They will be triggered on the
  // primary DB side.
  std::vector<std::shared_ptr<EventListener>> listeners;

  // statistics is used to collect DB operation metrics, the metrics won't be
  // returned to CompactionService primary host, to collect that, the user needs
  // to set it here.
  std::shared_ptr<Statistics> statistics = nullptr;

  // Only compaction generated SST files use this user defined table properties
  // collector.
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      table_properties_collector_factories;
};

struct OpenAndCompactOptions {
  // Allows cancellation of an in-progress compaction.
  std::atomic<bool>* canceled = nullptr;
};

struct LiveFilesStorageInfoOptions {
  // Whether to populate FileStorageInfo::file_checksum* or leave blank
  bool include_checksum_info = false;
  // Flushes memtables if total size in bytes of live WAL files is >= this
  // number (and DB is not read-only).
  // Default: always force a flush without checking sizes.
  uint64_t wal_size_for_flush = 0;
};

struct WaitForCompactOptions {
  // A boolean to abort waiting in case of a pause (PauseBackgroundWork()
  // called) If true, Status::Aborted will be returned immediately. If false,
  // ContinueBackgroundWork() must be called to resume the background jobs.
  // Otherwise, jobs that were queued, but not scheduled yet may never finish
  // and WaitForCompact() may wait indefinitely (if timeout is set, it will
  // expire and return Status::TimedOut).
  bool abort_on_pause = false;

  // A boolean to flush all column families before starting to wait.
  bool flush = false;

  // A boolean to wait for purge to complete
  bool wait_for_purge = false;

  // A boolean to call Close() after waiting is done. By the time Close() is
  // called here, there should be no background jobs in progress and no new
  // background jobs should be added. DB may not have been closed if Close()
  // returned Aborted status due to unreleased snapshots in the system. See
  // comments in DB::Close() for details.
  bool close_db = false;

  // Timeout in microseconds for waiting for compaction to complete.
  // Status::TimedOut will be returned if timeout expires.
  // when timeout == 0, WaitForCompact() will wait as long as there's background
  // work to finish.
  std::chrono::microseconds timeout = std::chrono::microseconds::zero();
};

}  // namespace ROCKSDB_NAMESPACE
