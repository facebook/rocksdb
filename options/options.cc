//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/options.h"

#include <cinttypes>
#include <limits>

#include "file/filename.h"
#include "logging/logging.h"
#include "monitoring/statistics.h"
#include "options/db_options.h"
#include "options/options_helper.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/wal_filter.h"
#include "table/block_based/block_based_table_factory.h"
#include "test_util/sync_point.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {
namespace {
const uint64_t kDefaultTtl = 0xfffffffffffffffe;
const uint64_t kDefaultPeriodicCompSecs = 0xfffffffffffffffe;

Status CheckCompressionSupported(const ColumnFamilyOptions& cf_options) {
  if (!cf_options.compression_per_level.empty()) {
    for (size_t level = 0; level < cf_options.compression_per_level.size();
         ++level) {
      if (!CompressionTypeSupported(cf_options.compression_per_level[level])) {
        return Status::InvalidArgument(
            "Compression type " +
            CompressionTypeToString(cf_options.compression_per_level[level]) +
            " is not linked with the binary.");
      }
    }
  } else {
    if (!CompressionTypeSupported(cf_options.compression)) {
      return Status::InvalidArgument(
          "Compression type " +
          CompressionTypeToString(cf_options.compression) +
          " is not linked with the binary.");
    }
  }
  if (cf_options.compression_opts.zstd_max_train_bytes > 0) {
    if (!ZSTD_TrainDictionarySupported()) {
      return Status::InvalidArgument(
          "zstd dictionary trainer cannot be used because ZSTD 1.1.3+ "
          "is not linked with the binary.");
    }
    if (cf_options.compression_opts.max_dict_bytes == 0) {
      return Status::InvalidArgument(
          "The dictionary size limit (`CompressionOptions::max_dict_bytes`) "
          "should be nonzero if we're using zstd's dictionary generator.");
    }
  }

  if (!CompressionTypeSupported(cf_options.blob_compression_type)) {
    std::ostringstream oss;
    oss << "The specified blob compression type "
        << CompressionTypeToString(cf_options.blob_compression_type)
        << " is not available.";

    return Status::InvalidArgument(oss.str());
  }

  return Status::OK();
}

Status CheckConcurrentWritesSupported(const ColumnFamilyOptions& cf_options) {
  if (cf_options.inplace_update_support) {
    return Status::InvalidArgument(
        "In-place memtable updates (inplace_update_support) is not compatible "
        "with concurrent writes (allow_concurrent_memtable_write)");
  }
  if (!cf_options.memtable_factory->IsInsertConcurrentlySupported()) {
    return Status::InvalidArgument(
        "Memtable doesn't concurrent writes (allow_concurrent_memtable_write)");
  }
  return Status::OK();
}

Status CheckCFPathsSupported(const DBOptions& db_options,
                             const ColumnFamilyOptions& cf_options) {
  // More than one cf_paths are supported only in universal
  // and level compaction styles. This function also checks the case
  // in which cf_paths is not specified, which results in db_paths
  // being used.
  if ((cf_options.compaction_style != kCompactionStyleUniversal) &&
      (cf_options.compaction_style != kCompactionStyleLevel)) {
    if (cf_options.cf_paths.size() > 1) {
      return Status::NotSupported(
          "More than one CF paths are only supported in "
          "universal and level compaction styles. ");
    } else if (cf_options.cf_paths.empty() && db_options.db_paths.size() > 1) {
      return Status::NotSupported(
          "More than one DB paths are only supported in "
          "universal and level compaction styles. ");
    }
  }
  return Status::OK();
}
}  // anonymous namespace
AdvancedColumnFamilyOptions::AdvancedColumnFamilyOptions() {
  assert(memtable_factory.get() != nullptr);
}

AdvancedColumnFamilyOptions::AdvancedColumnFamilyOptions(const Options& options)
    : max_write_buffer_number(options.max_write_buffer_number),
      min_write_buffer_number_to_merge(
          options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          options.max_write_buffer_number_to_maintain),
      max_write_buffer_size_to_maintain(
          options.max_write_buffer_size_to_maintain),
      inplace_update_support(options.inplace_update_support),
      inplace_update_num_locks(options.inplace_update_num_locks),
      inplace_callback(options.inplace_callback),
      memtable_prefix_bloom_size_ratio(
          options.memtable_prefix_bloom_size_ratio),
      memtable_whole_key_filtering(options.memtable_whole_key_filtering),
      memtable_huge_page_size(options.memtable_huge_page_size),
      memtable_insert_with_hint_prefix_extractor(
          options.memtable_insert_with_hint_prefix_extractor),
      bloom_locality(options.bloom_locality),
      arena_block_size(options.arena_block_size),
      compression_per_level(options.compression_per_level),
      num_levels(options.num_levels),
      level0_slowdown_writes_trigger(options.level0_slowdown_writes_trigger),
      level0_stop_writes_trigger(options.level0_stop_writes_trigger),
      target_file_size_base(options.target_file_size_base),
      target_file_size_multiplier(options.target_file_size_multiplier),
      level_compaction_dynamic_level_bytes(
          options.level_compaction_dynamic_level_bytes),
      max_bytes_for_level_multiplier(options.max_bytes_for_level_multiplier),
      max_bytes_for_level_multiplier_additional(
          options.max_bytes_for_level_multiplier_additional),
      max_compaction_bytes(options.max_compaction_bytes),
      soft_pending_compaction_bytes_limit(
          options.soft_pending_compaction_bytes_limit),
      hard_pending_compaction_bytes_limit(
          options.hard_pending_compaction_bytes_limit),
      compaction_style(options.compaction_style),
      compaction_pri(options.compaction_pri),
      compaction_options_universal(options.compaction_options_universal),
      compaction_options_fifo(options.compaction_options_fifo),
      max_sequential_skip_in_iterations(
          options.max_sequential_skip_in_iterations),
      memtable_factory(options.memtable_factory),
      table_properties_collector_factories(
          options.table_properties_collector_factories),
      max_successive_merges(options.max_successive_merges),
      optimize_filters_for_hits(options.optimize_filters_for_hits),
      paranoid_file_checks(options.paranoid_file_checks),
      force_consistency_checks(options.force_consistency_checks),
      report_bg_io_stats(options.report_bg_io_stats),
      ttl(options.ttl),
      periodic_compaction_seconds(options.periodic_compaction_seconds),
      sample_for_compression(options.sample_for_compression),
      enable_blob_files(options.enable_blob_files),
      min_blob_size(options.min_blob_size),
      blob_file_size(options.blob_file_size),
      blob_compression_type(options.blob_compression_type),
      enable_blob_garbage_collection(options.enable_blob_garbage_collection),
      blob_garbage_collection_age_cutoff(
          options.blob_garbage_collection_age_cutoff),
      blob_garbage_collection_force_threshold(
          options.blob_garbage_collection_force_threshold),
      blob_compaction_readahead_size(options.blob_compaction_readahead_size) {
  assert(memtable_factory.get() != nullptr);
  if (max_bytes_for_level_multiplier_additional.size() <
      static_cast<unsigned int>(num_levels)) {
    max_bytes_for_level_multiplier_additional.resize(num_levels, 1);
  }
}

ColumnFamilyOptions::ColumnFamilyOptions()
    : compression(Snappy_Supported() ? kSnappyCompression : kNoCompression),
      table_factory(
          std::shared_ptr<TableFactory>(new BlockBasedTableFactory())) {}

ColumnFamilyOptions::ColumnFamilyOptions(const Options& options)
    : ColumnFamilyOptions(*static_cast<const ColumnFamilyOptions*>(&options)) {}

Status ColumnFamilyOptions::Sanitize(const DBOptions& db_opts) {
  size_t clamp_max = std::conditional<
      sizeof(size_t) == 4, std::integral_constant<size_t, 0xffffffff>,
      std::integral_constant<uint64_t, 64ull << 30>>::type::value;
  OptionTypeInfo::ClipToRange(&write_buffer_size,
                              (static_cast<size_t>(64)) << 10, clamp_max);
  // if user sets arena_block_size, we trust user to use this value. Otherwise,
  // calculate a proper value from writer_buffer_size;
  if (arena_block_size <= 0) {
    arena_block_size = std::min(size_t{1024 * 1024}, write_buffer_size / 8);

    // Align up to 4k
    const size_t align = 4 * 1024;
    arena_block_size = ((arena_block_size + align - 1) / align) * align;
  }
  min_write_buffer_number_to_merge =
      std::min(min_write_buffer_number_to_merge, max_write_buffer_number - 1);
  if (min_write_buffer_number_to_merge < 1) {
    min_write_buffer_number_to_merge = 1;
  }

  if (num_levels < 1) {
    num_levels = 1;
  }
  if (compaction_style == kCompactionStyleLevel && num_levels < 2) {
    num_levels = 2;
  }

  if (compaction_style == kCompactionStyleUniversal &&
      db_opts.allow_ingest_behind && num_levels < 3) {
    num_levels = 3;
  }

  if (max_write_buffer_number < 2) {
    max_write_buffer_number = 2;
  }
  // fall back max_write_buffer_number_to_maintain if
  // max_write_buffer_size_to_maintain is not set
  if (max_write_buffer_size_to_maintain < 0) {
    max_write_buffer_size_to_maintain =
        max_write_buffer_number * static_cast<int64_t>(write_buffer_size);
  } else if (max_write_buffer_size_to_maintain == 0 &&
             max_write_buffer_number_to_maintain < 0) {
    max_write_buffer_number_to_maintain = max_write_buffer_number;
  }
  // bloom filter size shouldn't exceed 1/4 of memtable size.
  if (memtable_prefix_bloom_size_ratio > 0.25) {
    memtable_prefix_bloom_size_ratio = 0.25;
  } else if (memtable_prefix_bloom_size_ratio < 0) {
    memtable_prefix_bloom_size_ratio = 0;
  }

  if (!prefix_extractor) {
    assert(memtable_factory);
    Slice name = memtable_factory->Name();
    if (name.compare("HashSkipListRepFactory") == 0 ||
        name.compare("HashLinkListRepFactory") == 0) {
      memtable_factory = std::make_shared<SkipListFactory>();
    }
  }

  if (compaction_style == kCompactionStyleFIFO) {
    num_levels = 1;
    // since we delete level0 files in FIFO compaction when there are too many
    // of them, these options don't really mean anything
    level0_slowdown_writes_trigger = std::numeric_limits<int>::max();
    level0_stop_writes_trigger = std::numeric_limits<int>::max();
  }

  if (max_bytes_for_level_multiplier <= 0) {
    max_bytes_for_level_multiplier = 1;
  }

  if (level0_file_num_compaction_trigger == 0) {
    ROCKS_LOG_WARN(db_opts.info_log,
                   "level0_file_num_compaction_trigger cannot be 0");
    level0_file_num_compaction_trigger = 1;
  }

  if (level0_stop_writes_trigger < level0_slowdown_writes_trigger ||
      level0_slowdown_writes_trigger < level0_file_num_compaction_trigger) {
    ROCKS_LOG_WARN(db_opts.info_log,
                   "This condition must be satisfied: "
                   "level0_stop_writes_trigger(%d) >= "
                   "level0_slowdown_writes_trigger(%d) >= "
                   "level0_file_num_compaction_trigger(%d)",
                   level0_stop_writes_trigger, level0_slowdown_writes_trigger,
                   level0_file_num_compaction_trigger);
    if (level0_slowdown_writes_trigger < level0_file_num_compaction_trigger) {
      level0_slowdown_writes_trigger = level0_file_num_compaction_trigger;
    }
    if (level0_stop_writes_trigger < level0_slowdown_writes_trigger) {
      level0_stop_writes_trigger = level0_slowdown_writes_trigger;
    }
    ROCKS_LOG_WARN(db_opts.info_log,
                   "Adjust the value to "
                   "level0_stop_writes_trigger(%d)"
                   "level0_slowdown_writes_trigger(%d)"
                   "level0_file_num_compaction_trigger(%d)",
                   level0_stop_writes_trigger, level0_slowdown_writes_trigger,
                   level0_file_num_compaction_trigger);
  }

  if (soft_pending_compaction_bytes_limit == 0) {
    soft_pending_compaction_bytes_limit = hard_pending_compaction_bytes_limit;
  } else if (hard_pending_compaction_bytes_limit > 0 &&
             soft_pending_compaction_bytes_limit >
                 hard_pending_compaction_bytes_limit) {
    soft_pending_compaction_bytes_limit = hard_pending_compaction_bytes_limit;
  }

  if (cf_paths.empty()) {
    cf_paths = db_opts.db_paths;
  }

  if (level_compaction_dynamic_level_bytes) {
    if (compaction_style != kCompactionStyleLevel) {
      ROCKS_LOG_WARN(db_opts.info_log,
                     "level_compaction_dynamic_level_bytes only makes sense"
                     "for level-based compaction");
      level_compaction_dynamic_level_bytes = false;
    } else if (cf_paths.size() > 1U) {
      // we don't yet know how to make both of this feature and multiple
      // DB path work.
      ROCKS_LOG_WARN(db_opts.info_log,
                     "multiple cf_paths/db_paths and"
                     "level_compaction_dynamic_level_bytes"
                     "can't be used together");
      level_compaction_dynamic_level_bytes = false;
    }
  }

  if (max_compaction_bytes == 0) {
    max_compaction_bytes = target_file_size_base * 25;
  }

  bool is_block_based_table =
      (table_factory->IsInstanceOf(TableFactory::kBlockBasedTableName()));

  const uint64_t kAdjustedTtl = 30 * 24 * 60 * 60;
  if (ttl == kDefaultTtl) {
    if (is_block_based_table && compaction_style != kCompactionStyleFIFO) {
      ttl = kAdjustedTtl;
    } else {
      ttl = 0;
    }
  }

  const uint64_t kAdjustedPeriodicCompSecs = 30 * 24 * 60 * 60;

  // Turn on periodic compactions and set them to occur once every 30 days if
  // compaction filters are used and periodic_compaction_seconds is set to the
  // default value.
  if (compaction_style != kCompactionStyleFIFO) {
    if ((compaction_filter != nullptr ||
         compaction_filter_factory != nullptr) &&
        periodic_compaction_seconds == kDefaultPeriodicCompSecs &&
        is_block_based_table) {
      periodic_compaction_seconds = kAdjustedPeriodicCompSecs;
    }
  } else {
    // compaction_style == kCompactionStyleFIFO
    if (ttl == 0) {
      if (is_block_based_table) {
        if (periodic_compaction_seconds == kDefaultPeriodicCompSecs) {
          periodic_compaction_seconds = kAdjustedPeriodicCompSecs;
        }
        ttl = periodic_compaction_seconds;
      }
    } else if (periodic_compaction_seconds != 0) {
      ttl = std::min(ttl, periodic_compaction_seconds);
    }
  }

  // TTL compactions would work similar to Periodic Compactions in Universal in
  // most of the cases. So, if ttl is set, execute the periodic compaction
  // codepath.
  if (compaction_style == kCompactionStyleUniversal && ttl != 0) {
    if (periodic_compaction_seconds != 0) {
      periodic_compaction_seconds = std::min(ttl, periodic_compaction_seconds);
    } else {
      periodic_compaction_seconds = ttl;
    }
  }

  if (periodic_compaction_seconds == kDefaultPeriodicCompSecs) {
    periodic_compaction_seconds = 0;
  }

#ifndef ROCKSDB_LITE
  auto cf_cfg = CFOptionsAsConfigurable(*this);
  return cf_cfg->SanitizeOptions(db_opts, *this);
#else
  return Status::OK();
#endif  // ROCKSDB_LITE
}

Status ColumnFamilyOptions::Validate(const DBOptions& db_opts) const {
  Status s;
  s = CheckCompressionSupported(*this);
  if (s.ok() && db_opts.allow_concurrent_memtable_write) {
    s = CheckConcurrentWritesSupported(*this);
  }
  if (s.ok() && db_opts.unordered_write && max_successive_merges != 0) {
    s = Status::InvalidArgument(
        "max_successive_merges > 0 is incompatible with unordered_write");
  }
  if (s.ok()) {
    s = CheckCFPathsSupported(db_opts, *this);
  }
  if (!s.ok()) {
    return s;
  }

  if (ttl > 0 && ttl != kDefaultTtl) {
    if (!table_factory->IsInstanceOf(TableFactory::kBlockBasedTableName())) {
      return Status::NotSupported(
          "TTL is only supported in Block-Based Table format. ");
    }
  }

  if (periodic_compaction_seconds > 0 &&
      periodic_compaction_seconds != kDefaultPeriodicCompSecs) {
    if (!table_factory->IsInstanceOf(TableFactory::kBlockBasedTableName())) {
      return Status::NotSupported(
          "Periodic Compaction is only supported in "
          "Block-Based Table format. ");
    }
  }

  if (enable_blob_garbage_collection) {
    if (blob_garbage_collection_age_cutoff < 0.0 ||
        blob_garbage_collection_age_cutoff > 1.0) {
      return Status::InvalidArgument(
          "The age cutoff for blob garbage collection should be in the range "
          "[0.0, 1.0].");
    }
    if (blob_garbage_collection_force_threshold < 0.0 ||
        blob_garbage_collection_force_threshold > 1.0) {
      return Status::InvalidArgument(
          "The garbage ratio threshold for forcing blob garbage collection "
          "should be in the range [0.0, 1.0].");
    }
  }

  if (compaction_style == kCompactionStyleFIFO &&
      db_opts.max_open_files != -1 && ttl > 0) {
    return Status::NotSupported(
        "FIFO compaction only supported with max_open_files = -1.");
  }
#ifndef ROCKSDB_LITE
  auto cf_cfg = CFOptionsAsConfigurable(*this);
  s = cf_cfg->ValidateOptions(db_opts, *this);
#else
  s = table_factory->ValidateOptions(db_opts, *this);
#endif
  return s;
}

DBOptions::DBOptions() {}
DBOptions::DBOptions(const Options& options)
    : DBOptions(*static_cast<const DBOptions*>(&options)) {}

void DBOptions::Dump(Logger* log) const {
    ImmutableDBOptions(*this).Dump(log);
    MutableDBOptions(*this).Dump(log);
}  // DBOptions::Dump

Status DBOptions::Validate(const ColumnFamilyOptions& cf_opts) const {
  if (db_paths.size() > 4) {
    return Status::NotSupported(
        "More than four DB paths are not supported yet. ");
  }

  if (allow_mmap_reads && use_direct_reads) {
    // Protect against assert in PosixMMapReadableFile constructor
    return Status::NotSupported(
        "If memory mapped reads (allow_mmap_reads) are enabled "
        "then direct I/O reads (use_direct_reads) must be disabled. ");
  }

  if (allow_mmap_writes && use_direct_io_for_flush_and_compaction) {
    return Status::NotSupported(
        "If memory mapped writes (allow_mmap_writes) are enabled "
        "then direct I/O writes (use_direct_io_for_flush_and_compaction) must "
        "be disabled. ");
  }

  if (keep_log_file_num == 0) {
    return Status::InvalidArgument("keep_log_file_num must be greater than 0");
  }

  if (unordered_write && !allow_concurrent_memtable_write) {
    return Status::InvalidArgument(
        "unordered_write is incompatible with "
        "!allow_concurrent_memtable_write");
  }

  if (unordered_write && enable_pipelined_write) {
    return Status::InvalidArgument(
        "unordered_write is incompatible with enable_pipelined_write");
  }

  if (atomic_flush && enable_pipelined_write) {
    return Status::InvalidArgument(
        "atomic_flush is incompatible with enable_pipelined_write");
  }

  // TODO remove this restriction
  if (atomic_flush && best_efforts_recovery) {
    return Status::InvalidArgument(
        "atomic_flush is currently incompatible with best-efforts recovery");
  }
  if (use_direct_io_for_flush_and_compaction &&
      0 == writable_file_max_buffer_size) {
    return Status::InvalidArgument(
        "writes in direct IO require writable_file_max_buffer_size > 0");
  }
  
#ifndef ROCKSDB_LITE
  auto db_cfg = DBOptionsAsConfigurable(*this);
  return db_cfg->ValidateOptions(*this, cf_opts);
#else
  (void)cf_opts;
  return Status::OK();
#endif
}

Status DBOptions::Sanitize(const std::string& dbname, bool read_only) {
  if (env == nullptr) {
    env = Env::Default();
  }

  // max_open_files means an "infinite" open files.
  if (max_open_files != -1) {
    int max_max_open_files = port::GetMaxOpenFiles();
    if (max_max_open_files == -1) {
      max_max_open_files = 0x400000;
    }
    OptionTypeInfo::ClipToRange(&max_open_files, 20, max_max_open_files);
    TEST_SYNC_POINT_CALLBACK("SanitizeOptions::AfterChangeMaxOpenFiles",
                             &max_open_files);
  }

  if (info_log == nullptr && !read_only) {
    Status s = CreateLoggerFromOptions(dbname, *this, &info_log);
    if (!s.ok()) {
      // No place suitable for logging
      info_log = nullptr;
    }
  }

  if (!write_buffer_manager) {
    write_buffer_manager.reset(new WriteBufferManager(db_write_buffer_size));
  }
  if (rate_limiter.get() != nullptr) {
    if (bytes_per_sync == 0) {
      bytes_per_sync = 1024 * 1024;
    }
  }

  if (delayed_write_rate == 0) {
    if (rate_limiter.get() != nullptr) {
      delayed_write_rate = rate_limiter->GetBytesPerSecond();
    }
    if (delayed_write_rate == 0) {
      delayed_write_rate = 16 * 1024 * 1024;
    }
  }

  if (WAL_ttl_seconds > 0 || WAL_size_limit_MB > 0) {
    recycle_log_file_num = false;
  }

  if (recycle_log_file_num &&
      (wal_recovery_mode == WALRecoveryMode::kTolerateCorruptedTailRecords ||
       wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery ||
       wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency)) {
    // - kTolerateCorruptedTailRecords is inconsistent with recycle log file
    //   feature. WAL recycling expects recovery success upon encountering a
    //   corrupt record at the point where new data ends and recycled data
    //   remains at the tail. However, `kTolerateCorruptedTailRecords` must fail
    //   upon encountering any such corrupt record, as it cannot differentiate
    //   between this and a real corruption, which would cause committed updates
    //   to be truncated -- a violation of the recovery guarantee.
    // - kPointInTimeRecovery and kAbsoluteConsistency are incompatible with
    //   recycle log file feature temporarily due to a bug found introducing a
    //   hole in the recovered data
    //   (https://github.com/facebook/rocksdb/pull/7252#issuecomment-673766236).
    //   Besides this bug, we believe the features are fundamentally compatible.
    recycle_log_file_num = 0;
  }

  if (db_paths.size() == 0) {
    db_paths.emplace_back(dbname, std::numeric_limits<uint64_t>::max());
  } else if (wal_dir.empty()) {
    // Use dbname as default
    wal_dir = dbname;
  }
  if (!wal_dir.empty()) {
    // If there is a wal_dir already set, check to see if the wal_dir is the
    // same as the dbname AND the same as the db_path[0] (which must exist from
    // a few lines ago). If the wal_dir matches both of these values, then clear
    // the wal_dir value, which will make wal_dir == dbname.  Most likely this
    // condition was the result of reading an old options file where we forced
    // wal_dir to be set (to dbname).
    auto npath = NormalizePath(dbname + "/");
    if (npath == NormalizePath(wal_dir + "/") &&
        npath == NormalizePath(db_paths[0].path + "/")) {
      wal_dir.clear();
    }
  }

  if (!wal_dir.empty() && wal_dir.back() == '/') {
    wal_dir = wal_dir.substr(0, wal_dir.size() - 1);
  }

  if (use_direct_reads && compaction_readahead_size == 0) {
    TEST_SYNC_POINT_CALLBACK("SanitizeOptions:direct_io", nullptr);
    compaction_readahead_size = 1024 * 1024 * 2;
  }

  // Force flush on DB open if 2PC is enabled, since with 2PC we have no
  // guarantee that consecutive log files have consecutive sequence id, which
  // make recovery complicated.
  if (allow_2pc) {
    avoid_flush_during_recovery = false;
  }

  if (!paranoid_checks) {
    skip_checking_sst_file_sizes_on_db_open = true;
    ROCKS_LOG_INFO(info_log, "file size check will be skipped during open.");
  }

  // Supported wal compression types
  if (wal_compression != kNoCompression &&
      wal_compression != kZSTD) {
    wal_compression = kNoCompression;
    ROCKS_LOG_WARN(info_log,
                   "wal_compression is disabled since only zstd is supported");
  }

  if (!paranoid_checks) {
    skip_checking_sst_file_sizes_on_db_open = true;
    ROCKS_LOG_INFO(info_log,
                   "file size check will be skipped during open.");
  }

#ifndef ROCKSDB_LITE
  auto db_cfg = DBOptionsAsConfigurable(*this);
  return db_cfg->SanitizeOptions(dbname, read_only, *this);
#else
  return Status::OK();
#endif  // ROCKSDB_LITE
}

void ColumnFamilyOptions::Dump(Logger* log) const {
  ROCKS_LOG_HEADER(log, "              Options.comparator: %s",
                   comparator->Name());
  ROCKS_LOG_HEADER(log, "          Options.merge_operator: %s",
                   merge_operator ? merge_operator->Name() : "None");
  ROCKS_LOG_HEADER(log, "       Options.compaction_filter: %s",
                   compaction_filter ? compaction_filter->Name() : "None");
  ROCKS_LOG_HEADER(
      log, "       Options.compaction_filter_factory: %s",
      compaction_filter_factory ? compaction_filter_factory->Name() : "None");
  ROCKS_LOG_HEADER(
      log, " Options.sst_partitioner_factory: %s",
      sst_partitioner_factory ? sst_partitioner_factory->Name() : "None");
  ROCKS_LOG_HEADER(log, "        Options.memtable_factory: %s",
                   memtable_factory->Name());
  ROCKS_LOG_HEADER(log, "           Options.table_factory: %s",
                   table_factory->Name());
  ROCKS_LOG_HEADER(log, "           table_factory options: %s",
                   table_factory->GetPrintableOptions().c_str());
  ROCKS_LOG_HEADER(log, "       Options.write_buffer_size: %" ROCKSDB_PRIszt,
                   write_buffer_size);
  ROCKS_LOG_HEADER(log, " Options.max_write_buffer_number: %d",
                   max_write_buffer_number);
  if (!compression_per_level.empty()) {
    for (unsigned int i = 0; i < compression_per_level.size(); i++) {
      ROCKS_LOG_HEADER(
          log, "       Options.compression[%d]: %s", i,
          CompressionTypeToString(compression_per_level[i]).c_str());
    }
    } else {
      ROCKS_LOG_HEADER(log, "         Options.compression: %s",
                       CompressionTypeToString(compression).c_str());
    }
    ROCKS_LOG_HEADER(
        log, "                 Options.bottommost_compression: %s",
        bottommost_compression == kDisableCompressionOption
            ? "Disabled"
            : CompressionTypeToString(bottommost_compression).c_str());
    ROCKS_LOG_HEADER(
        log, "      Options.prefix_extractor: %s",
        prefix_extractor == nullptr ? "nullptr" : prefix_extractor->Name());
    ROCKS_LOG_HEADER(log,
                     "  Options.memtable_insert_with_hint_prefix_extractor: %s",
                     memtable_insert_with_hint_prefix_extractor == nullptr
                         ? "nullptr"
                         : memtable_insert_with_hint_prefix_extractor->Name());
    ROCKS_LOG_HEADER(log, "            Options.num_levels: %d", num_levels);
    ROCKS_LOG_HEADER(log, "       Options.min_write_buffer_number_to_merge: %d",
                     min_write_buffer_number_to_merge);
    ROCKS_LOG_HEADER(log, "    Options.max_write_buffer_number_to_maintain: %d",
                     max_write_buffer_number_to_maintain);
    ROCKS_LOG_HEADER(log,
                     "    Options.max_write_buffer_size_to_maintain: %" PRIu64,
                     max_write_buffer_size_to_maintain);
    ROCKS_LOG_HEADER(
        log, "           Options.bottommost_compression_opts.window_bits: %d",
        bottommost_compression_opts.window_bits);
    ROCKS_LOG_HEADER(
        log, "                 Options.bottommost_compression_opts.level: %d",
        bottommost_compression_opts.level);
    ROCKS_LOG_HEADER(
        log, "              Options.bottommost_compression_opts.strategy: %d",
        bottommost_compression_opts.strategy);
    ROCKS_LOG_HEADER(
        log,
        "        Options.bottommost_compression_opts.max_dict_bytes: "
        "%" PRIu32,
        bottommost_compression_opts.max_dict_bytes);
    ROCKS_LOG_HEADER(
        log,
        "        Options.bottommost_compression_opts.zstd_max_train_bytes: "
        "%" PRIu32,
        bottommost_compression_opts.zstd_max_train_bytes);
    ROCKS_LOG_HEADER(
        log,
        "        Options.bottommost_compression_opts.parallel_threads: "
        "%" PRIu32,
        bottommost_compression_opts.parallel_threads);
    ROCKS_LOG_HEADER(
        log, "                 Options.bottommost_compression_opts.enabled: %s",
        bottommost_compression_opts.enabled ? "true" : "false");
    ROCKS_LOG_HEADER(
        log,
        "        Options.bottommost_compression_opts.max_dict_buffer_bytes: "
        "%" PRIu64,
        bottommost_compression_opts.max_dict_buffer_bytes);
    ROCKS_LOG_HEADER(log, "           Options.compression_opts.window_bits: %d",
                     compression_opts.window_bits);
    ROCKS_LOG_HEADER(log, "                 Options.compression_opts.level: %d",
                     compression_opts.level);
    ROCKS_LOG_HEADER(log, "              Options.compression_opts.strategy: %d",
                     compression_opts.strategy);
    ROCKS_LOG_HEADER(
        log,
        "        Options.compression_opts.max_dict_bytes: %" PRIu32,
        compression_opts.max_dict_bytes);
    ROCKS_LOG_HEADER(log,
                     "        Options.compression_opts.zstd_max_train_bytes: "
                     "%" PRIu32,
                     compression_opts.zstd_max_train_bytes);
    ROCKS_LOG_HEADER(log,
                     "        Options.compression_opts.parallel_threads: "
                     "%" PRIu32,
                     compression_opts.parallel_threads);
    ROCKS_LOG_HEADER(log,
                     "                 Options.compression_opts.enabled: %s",
                     compression_opts.enabled ? "true" : "false");
    ROCKS_LOG_HEADER(log,
                     "        Options.compression_opts.max_dict_buffer_bytes: "
                     "%" PRIu64,
                     compression_opts.max_dict_buffer_bytes);
    ROCKS_LOG_HEADER(log, "     Options.level0_file_num_compaction_trigger: %d",
                     level0_file_num_compaction_trigger);
    ROCKS_LOG_HEADER(log, "         Options.level0_slowdown_writes_trigger: %d",
                     level0_slowdown_writes_trigger);
    ROCKS_LOG_HEADER(log, "             Options.level0_stop_writes_trigger: %d",
                     level0_stop_writes_trigger);
    ROCKS_LOG_HEADER(
        log, "                  Options.target_file_size_base: %" PRIu64,
        target_file_size_base);
    ROCKS_LOG_HEADER(log, "            Options.target_file_size_multiplier: %d",
                     target_file_size_multiplier);
    ROCKS_LOG_HEADER(
        log, "               Options.max_bytes_for_level_base: %" PRIu64,
        max_bytes_for_level_base);
    ROCKS_LOG_HEADER(log, "Options.level_compaction_dynamic_level_bytes: %d",
                     level_compaction_dynamic_level_bytes);
    ROCKS_LOG_HEADER(log, "         Options.max_bytes_for_level_multiplier: %f",
                     max_bytes_for_level_multiplier);
    for (size_t i = 0; i < max_bytes_for_level_multiplier_additional.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "Options.max_bytes_for_level_multiplier_addtl[%" ROCKSDB_PRIszt
               "]: %d",
          i, max_bytes_for_level_multiplier_additional[i]);
    }
    ROCKS_LOG_HEADER(
        log, "      Options.max_sequential_skip_in_iterations: %" PRIu64,
        max_sequential_skip_in_iterations);
    ROCKS_LOG_HEADER(
        log, "                   Options.max_compaction_bytes: %" PRIu64,
        max_compaction_bytes);
    ROCKS_LOG_HEADER(
        log,
        "                       Options.arena_block_size: %" ROCKSDB_PRIszt,
        arena_block_size);
    ROCKS_LOG_HEADER(log,
                     "  Options.soft_pending_compaction_bytes_limit: %" PRIu64,
                     soft_pending_compaction_bytes_limit);
    ROCKS_LOG_HEADER(log,
                     "  Options.hard_pending_compaction_bytes_limit: %" PRIu64,
                     hard_pending_compaction_bytes_limit);
    ROCKS_LOG_HEADER(log, "               Options.disable_auto_compactions: %d",
                     disable_auto_compactions);

    const auto& it_compaction_style =
        compaction_style_to_string.find(compaction_style);
    std::string str_compaction_style;
    if (it_compaction_style == compaction_style_to_string.end()) {
      assert(false);
      str_compaction_style = "unknown_" + std::to_string(compaction_style);
    } else {
      str_compaction_style = it_compaction_style->second;
    }
    ROCKS_LOG_HEADER(log,
                     "                       Options.compaction_style: %s",
                     str_compaction_style.c_str());

    const auto& it_compaction_pri =
        compaction_pri_to_string.find(compaction_pri);
    std::string str_compaction_pri;
    if (it_compaction_pri == compaction_pri_to_string.end()) {
      assert(false);
      str_compaction_pri = "unknown_" + std::to_string(compaction_pri);
    } else {
      str_compaction_pri = it_compaction_pri->second;
    }
    ROCKS_LOG_HEADER(log,
                     "                         Options.compaction_pri: %s",
                     str_compaction_pri.c_str());
    ROCKS_LOG_HEADER(log,
                     "Options.compaction_options_universal.size_ratio: %u",
                     compaction_options_universal.size_ratio);
    ROCKS_LOG_HEADER(log,
                     "Options.compaction_options_universal.min_merge_width: %u",
                     compaction_options_universal.min_merge_width);
    ROCKS_LOG_HEADER(log,
                     "Options.compaction_options_universal.max_merge_width: %u",
                     compaction_options_universal.max_merge_width);
    ROCKS_LOG_HEADER(
        log,
        "Options.compaction_options_universal."
        "max_size_amplification_percent: %u",
        compaction_options_universal.max_size_amplification_percent);
    ROCKS_LOG_HEADER(
        log,
        "Options.compaction_options_universal.compression_size_percent: %d",
        compaction_options_universal.compression_size_percent);
    const auto& it_compaction_stop_style = compaction_stop_style_to_string.find(
        compaction_options_universal.stop_style);
    std::string str_compaction_stop_style;
    if (it_compaction_stop_style == compaction_stop_style_to_string.end()) {
      assert(false);
      str_compaction_stop_style =
          "unknown_" + std::to_string(compaction_options_universal.stop_style);
    } else {
      str_compaction_stop_style = it_compaction_stop_style->second;
    }
    ROCKS_LOG_HEADER(log,
                     "Options.compaction_options_universal.stop_style: %s",
                     str_compaction_stop_style.c_str());
    ROCKS_LOG_HEADER(
        log, "Options.compaction_options_fifo.max_table_files_size: %" PRIu64,
        compaction_options_fifo.max_table_files_size);
    ROCKS_LOG_HEADER(log,
                     "Options.compaction_options_fifo.allow_compaction: %d",
                     compaction_options_fifo.allow_compaction);
    std::ostringstream collector_info;
    for (const auto& collector_factory : table_properties_collector_factories) {
      collector_info << collector_factory->ToString() << ';';
    }
    ROCKS_LOG_HEADER(
        log, "                  Options.table_properties_collectors: %s",
        collector_info.str().c_str());
    ROCKS_LOG_HEADER(log,
                     "                  Options.inplace_update_support: %d",
                     inplace_update_support);
    ROCKS_LOG_HEADER(
        log,
        "                Options.inplace_update_num_locks: %" ROCKSDB_PRIszt,
        inplace_update_num_locks);
    // TODO: easier config for bloom (maybe based on avg key/value size)
    ROCKS_LOG_HEADER(
        log, "              Options.memtable_prefix_bloom_size_ratio: %f",
        memtable_prefix_bloom_size_ratio);
    ROCKS_LOG_HEADER(log,
                     "              Options.memtable_whole_key_filtering: %d",
                     memtable_whole_key_filtering);

    ROCKS_LOG_HEADER(log, "  Options.memtable_huge_page_size: %" ROCKSDB_PRIszt,
                     memtable_huge_page_size);
    ROCKS_LOG_HEADER(log,
                     "                          Options.bloom_locality: %d",
                     bloom_locality);

    ROCKS_LOG_HEADER(
        log,
        "                   Options.max_successive_merges: %" ROCKSDB_PRIszt,
        max_successive_merges);
    ROCKS_LOG_HEADER(log,
                     "               Options.optimize_filters_for_hits: %d",
                     optimize_filters_for_hits);
    ROCKS_LOG_HEADER(log, "               Options.paranoid_file_checks: %d",
                     paranoid_file_checks);
    ROCKS_LOG_HEADER(log, "               Options.force_consistency_checks: %d",
                     force_consistency_checks);
    ROCKS_LOG_HEADER(log, "               Options.report_bg_io_stats: %d",
                     report_bg_io_stats);
    ROCKS_LOG_HEADER(log, "                              Options.ttl: %" PRIu64,
                     ttl);
    ROCKS_LOG_HEADER(log,
                     "         Options.periodic_compaction_seconds: %" PRIu64,
                     periodic_compaction_seconds);
    ROCKS_LOG_HEADER(log, "                      Options.enable_blob_files: %s",
                     enable_blob_files ? "true" : "false");
    ROCKS_LOG_HEADER(
        log, "                          Options.min_blob_size: %" PRIu64,
        min_blob_size);
    ROCKS_LOG_HEADER(
        log, "                         Options.blob_file_size: %" PRIu64,
        blob_file_size);
    ROCKS_LOG_HEADER(log, "                  Options.blob_compression_type: %s",
                     CompressionTypeToString(blob_compression_type).c_str());
    ROCKS_LOG_HEADER(log, "         Options.enable_blob_garbage_collection: %s",
                     enable_blob_garbage_collection ? "true" : "false");
    ROCKS_LOG_HEADER(log, "     Options.blob_garbage_collection_age_cutoff: %f",
                     blob_garbage_collection_age_cutoff);
    ROCKS_LOG_HEADER(log, "Options.blob_garbage_collection_force_threshold: %f",
                     blob_garbage_collection_force_threshold);
    ROCKS_LOG_HEADER(
        log, "         Options.blob_compaction_readahead_size: %" PRIu64,
        blob_compaction_readahead_size);
}  // ColumnFamilyOptions::Dump

void Options::Dump(Logger* log) const {
  DBOptions::Dump(log);
  ColumnFamilyOptions::Dump(log);
}   // Options::Dump

void Options::DumpCFOptions(Logger* log) const {
  ColumnFamilyOptions::Dump(log);
}  // Options::DumpCFOptions

Status Options::Validate() const {
  Status s = DBOptions::Validate(*this);
  if (s.ok()) {
    s = ColumnFamilyOptions::Validate(*this);
  }
  return s;
}

Status Options::Sanitize(const std::string& dbname, bool read_only) {
  Status s = DBOptions::Sanitize(dbname, read_only);
  if (s.ok()) {
    s = ColumnFamilyOptions::Sanitize(*this);
  }
  return s;
}

//
// The goal of this method is to create a configuration that
// allows an application to write all files into L0 and
// then do a single compaction to output all files into L1.
Options*
Options::PrepareForBulkLoad()
{
  // never slowdown ingest.
  level0_file_num_compaction_trigger = (1<<30);
  level0_slowdown_writes_trigger = (1<<30);
  level0_stop_writes_trigger = (1<<30);
  soft_pending_compaction_bytes_limit = 0;
  hard_pending_compaction_bytes_limit = 0;

  // no auto compactions please. The application should issue a
  // manual compaction after all data is loaded into L0.
  disable_auto_compactions = true;
  // A manual compaction run should pick all files in L0 in
  // a single compaction run.
  max_compaction_bytes = (static_cast<uint64_t>(1) << 60);

  // It is better to have only 2 levels, otherwise a manual
  // compaction would compact at every possible level, thereby
  // increasing the total time needed for compactions.
  num_levels = 2;

  // Need to allow more write buffers to allow more parallism
  // of flushes.
  max_write_buffer_number = 6;
  min_write_buffer_number_to_merge = 1;

  // When compaction is disabled, more parallel flush threads can
  // help with write throughput.
  max_background_flushes = 4;

  // Prevent a memtable flush to automatically promote files
  // to L1. This is helpful so that all files that are
  // input to the manual compaction are all at L0.
  max_background_compactions = 2;

  // The compaction would create large files in L1.
  target_file_size_base = 256 * 1024 * 1024;
  return this;
}

Options* Options::OptimizeForSmallDb() {
  // 16MB block cache
  std::shared_ptr<Cache> cache = NewLRUCache(16 << 20);

  ColumnFamilyOptions::OptimizeForSmallDb(&cache);
  DBOptions::OptimizeForSmallDb(&cache);
  return this;
}

Options* Options::DisableExtraChecks() {
  // See https://github.com/facebook/rocksdb/issues/9354
  force_consistency_checks = false;
  // Considered but no clear performance impact seen:
  // * check_flush_compaction_key_order
  // * paranoid_checks
  // * flush_verify_memtable_count
  // By current API contract, not including
  // * verify_checksums
  // because checking storage data integrity is a more standard practice.
  return this;
}

Options* Options::OldDefaults(int rocksdb_major_version,
                              int rocksdb_minor_version) {
  ColumnFamilyOptions::OldDefaults(rocksdb_major_version,
                                   rocksdb_minor_version);
  DBOptions::OldDefaults(rocksdb_major_version, rocksdb_minor_version);
  return this;
}

DBOptions* DBOptions::OldDefaults(int rocksdb_major_version,
                                  int rocksdb_minor_version) {
  if (rocksdb_major_version < 4 ||
      (rocksdb_major_version == 4 && rocksdb_minor_version < 7)) {
    max_file_opening_threads = 1;
    table_cache_numshardbits = 4;
  }
  if (rocksdb_major_version < 5 ||
      (rocksdb_major_version == 5 && rocksdb_minor_version < 2)) {
    delayed_write_rate = 2 * 1024U * 1024U;
  } else if (rocksdb_major_version < 5 ||
             (rocksdb_major_version == 5 && rocksdb_minor_version < 6)) {
    delayed_write_rate = 16 * 1024U * 1024U;
  }
  max_open_files = 5000;
  wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OldDefaults(
    int rocksdb_major_version, int rocksdb_minor_version) {
  if (rocksdb_major_version < 5 ||
      (rocksdb_major_version == 5 && rocksdb_minor_version <= 18)) {
    compaction_pri = CompactionPri::kByCompensatedSize;
  }
  if (rocksdb_major_version < 4 ||
      (rocksdb_major_version == 4 && rocksdb_minor_version < 7)) {
    write_buffer_size = 4 << 20;
    target_file_size_base = 2 * 1048576;
    max_bytes_for_level_base = 10 * 1048576;
    soft_pending_compaction_bytes_limit = 0;
    hard_pending_compaction_bytes_limit = 0;
  }
  if (rocksdb_major_version < 5) {
    level0_stop_writes_trigger = 24;
  } else if (rocksdb_major_version == 5 && rocksdb_minor_version < 2) {
    level0_stop_writes_trigger = 30;
  }

  return this;
}

// Optimization functions
DBOptions* DBOptions::OptimizeForSmallDb(std::shared_ptr<Cache>* cache) {
  max_file_opening_threads = 1;
  max_open_files = 5000;

  // Cost memtable to block cache too.
  std::shared_ptr<ROCKSDB_NAMESPACE::WriteBufferManager> wbm =
      std::make_shared<ROCKSDB_NAMESPACE::WriteBufferManager>(
          0, (cache != nullptr) ? *cache : std::shared_ptr<Cache>());
  write_buffer_manager = wbm;

  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForSmallDb(
    std::shared_ptr<Cache>* cache) {
  write_buffer_size = 2 << 20;
  target_file_size_base = 2 * 1048576;
  max_bytes_for_level_base = 10 * 1048576;
  soft_pending_compaction_bytes_limit = 256 * 1048576;
  hard_pending_compaction_bytes_limit = 1073741824ul;

  BlockBasedTableOptions table_options;
  table_options.block_cache =
      (cache != nullptr) ? *cache : std::shared_ptr<Cache>();
  table_options.cache_index_and_filter_blocks = true;
  // Two level iterator to avoid LRU cache imbalance
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_factory.reset(new BlockBasedTableFactory(table_options));

  return this;
}

#ifndef ROCKSDB_LITE
ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForPointLookup(
    uint64_t block_cache_size_mb) {
  BlockBasedTableOptions block_based_options;
  block_based_options.data_block_index_type =
      BlockBasedTableOptions::kDataBlockBinaryAndHash;
  block_based_options.data_block_hash_table_util_ratio = 0.75;
  block_based_options.filter_policy.reset(NewBloomFilterPolicy(10));
  block_based_options.block_cache =
      NewLRUCache(static_cast<size_t>(block_cache_size_mb * 1024 * 1024));
  table_factory.reset(new BlockBasedTableFactory(block_based_options));
  memtable_prefix_bloom_size_ratio = 0.02;
  memtable_whole_key_filtering = true;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeLevelStyleCompaction(
    uint64_t memtable_memory_budget) {
  write_buffer_size = static_cast<size_t>(memtable_memory_budget / 4);
  // merge two memtables when flushing to L0
  min_write_buffer_number_to_merge = 2;
  // this means we'll use 50% extra memory in the worst case, but will reduce
  // write stalls.
  max_write_buffer_number = 6;
  // start flushing L0->L1 as soon as possible. each file on level0 is
  // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
  // memtable_memory_budget.
  level0_file_num_compaction_trigger = 2;
  // doesn't really matter much, but we don't want to create too many files
  target_file_size_base = memtable_memory_budget / 8;
  // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
  max_bytes_for_level_base = memtable_memory_budget;

  // level style compaction
  compaction_style = kCompactionStyleLevel;

  // only compress levels >= 2
  compression_per_level.resize(num_levels);
  for (int i = 0; i < num_levels; ++i) {
    if (i < 2) {
      compression_per_level[i] = kNoCompression;
    } else {
      compression_per_level[i] =
          LZ4_Supported()
              ? kLZ4Compression
              : (Snappy_Supported() ? kSnappyCompression : kNoCompression);
    }
  }
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeUniversalStyleCompaction(
    uint64_t memtable_memory_budget) {
  write_buffer_size = static_cast<size_t>(memtable_memory_budget / 4);
  // merge two memtables when flushing to L0
  min_write_buffer_number_to_merge = 2;
  // this means we'll use 50% extra memory in the worst case, but will reduce
  // write stalls.
  max_write_buffer_number = 6;
  // universal style compaction
  compaction_style = kCompactionStyleUniversal;
  compaction_options_universal.compression_size_percent = 80;
  return this;
}

DBOptions* DBOptions::IncreaseParallelism(int total_threads) {
  max_background_jobs = total_threads;
  env->SetBackgroundThreads(total_threads, Env::LOW);
  env->SetBackgroundThreads(1, Env::HIGH);
  return this;
}

#endif  // !ROCKSDB_LITE

ReadOptions::ReadOptions()
    : snapshot(nullptr),
      iterate_lower_bound(nullptr),
      iterate_upper_bound(nullptr),
      readahead_size(0),
      max_skippable_internal_keys(0),
      read_tier(kReadAllTier),
      verify_checksums(true),
      fill_cache(true),
      tailing(false),
      managed(false),
      total_order_seek(false),
      auto_prefix_mode(false),
      prefix_same_as_start(false),
      pin_data(false),
      background_purge_on_iterator_cleanup(false),
      ignore_range_deletions(false),
      timestamp(nullptr),
      iter_start_ts(nullptr),
      deadline(std::chrono::microseconds::zero()),
      io_timeout(std::chrono::microseconds::zero()),
      value_size_soft_limit(std::numeric_limits<uint64_t>::max()),
      adaptive_readahead(false) {}

ReadOptions::ReadOptions(bool cksum, bool cache)
    : snapshot(nullptr),
      iterate_lower_bound(nullptr),
      iterate_upper_bound(nullptr),
      readahead_size(0),
      max_skippable_internal_keys(0),
      read_tier(kReadAllTier),
      verify_checksums(cksum),
      fill_cache(cache),
      tailing(false),
      managed(false),
      total_order_seek(false),
      auto_prefix_mode(false),
      prefix_same_as_start(false),
      pin_data(false),
      background_purge_on_iterator_cleanup(false),
      ignore_range_deletions(false),
      timestamp(nullptr),
      iter_start_ts(nullptr),
      deadline(std::chrono::microseconds::zero()),
      io_timeout(std::chrono::microseconds::zero()),
      value_size_soft_limit(std::numeric_limits<uint64_t>::max()),
      adaptive_readahead(false) {}

}  // namespace ROCKSDB_NAMESPACE
