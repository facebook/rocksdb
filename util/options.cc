//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <limits>

#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/wal_filter.h"
#include "table/block_based_table_factory.h"
#include "util/compression.h"
#include "util/statistics.h"
#include "util/xfunc.h"

namespace rocksdb {

ColumnFamilyOptions::ColumnFamilyOptions()
    : comparator(BytewiseComparator()),
      merge_operator(nullptr),
      compaction_filter(nullptr),
      compaction_filter_factory(nullptr),
      write_buffer_size(64 << 20),
      max_write_buffer_number(2),
      min_write_buffer_number_to_merge(1),
      max_write_buffer_number_to_maintain(0),
      compression(Snappy_Supported() ? kSnappyCompression : kNoCompression),
      bottommost_compression(kDisableCompressionOption),
      prefix_extractor(nullptr),
      num_levels(7),
      level0_file_num_compaction_trigger(4),
      level0_slowdown_writes_trigger(20),
      level0_stop_writes_trigger(32),
      target_file_size_base(64 * 1048576),
      target_file_size_multiplier(1),
      max_bytes_for_level_base(256 * 1048576),
      level_compaction_dynamic_level_bytes(false),
      max_bytes_for_level_multiplier(10),
      max_bytes_for_level_multiplier_additional(num_levels, 1),
      max_compaction_bytes(0),
      soft_rate_limit(0.0),
      hard_rate_limit(0.0),
      soft_pending_compaction_bytes_limit(64 * 1073741824ull),
      hard_pending_compaction_bytes_limit(256 * 1073741824ull),
      rate_limit_delay_max_milliseconds(1000),
      arena_block_size(0),
      disable_auto_compactions(false),
      purge_redundant_kvs_while_flush(true),
      compaction_style(kCompactionStyleLevel),
      compaction_pri(kByCompensatedSize),
      verify_checksums_in_compaction(true),
      max_sequential_skip_in_iterations(8),
      memtable_factory(std::shared_ptr<SkipListFactory>(new SkipListFactory)),
      table_factory(
          std::shared_ptr<TableFactory>(new BlockBasedTableFactory())),
      inplace_update_support(false),
      inplace_update_num_locks(10000),
      inplace_callback(nullptr),
      memtable_prefix_bloom_size_ratio(0.0),
      memtable_huge_page_size(0),
      memtable_insert_with_hint_prefix_extractor(nullptr),
      bloom_locality(0),
      max_successive_merges(0),
      min_partial_merge_operands(2),
      optimize_filters_for_hits(false),
      paranoid_file_checks(false),
      force_consistency_checks(false),
      report_bg_io_stats(false) {
  assert(memtable_factory.get() != nullptr);
}

ColumnFamilyOptions::ColumnFamilyOptions(const Options& options)
    : comparator(options.comparator),
      merge_operator(options.merge_operator),
      compaction_filter(options.compaction_filter),
      compaction_filter_factory(options.compaction_filter_factory),
      write_buffer_size(options.write_buffer_size),
      max_write_buffer_number(options.max_write_buffer_number),
      min_write_buffer_number_to_merge(
          options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          options.max_write_buffer_number_to_maintain),
      compression(options.compression),
      compression_per_level(options.compression_per_level),
      bottommost_compression(options.bottommost_compression),
      compression_opts(options.compression_opts),
      prefix_extractor(options.prefix_extractor),
      num_levels(options.num_levels),
      level0_file_num_compaction_trigger(
          options.level0_file_num_compaction_trigger),
      level0_slowdown_writes_trigger(options.level0_slowdown_writes_trigger),
      level0_stop_writes_trigger(options.level0_stop_writes_trigger),
      target_file_size_base(options.target_file_size_base),
      target_file_size_multiplier(options.target_file_size_multiplier),
      max_bytes_for_level_base(options.max_bytes_for_level_base),
      level_compaction_dynamic_level_bytes(
          options.level_compaction_dynamic_level_bytes),
      max_bytes_for_level_multiplier(options.max_bytes_for_level_multiplier),
      max_bytes_for_level_multiplier_additional(
          options.max_bytes_for_level_multiplier_additional),
      max_compaction_bytes(options.max_compaction_bytes),
      soft_rate_limit(options.soft_rate_limit),
      soft_pending_compaction_bytes_limit(
          options.soft_pending_compaction_bytes_limit),
      hard_pending_compaction_bytes_limit(
          options.hard_pending_compaction_bytes_limit),
      rate_limit_delay_max_milliseconds(
          options.rate_limit_delay_max_milliseconds),
      arena_block_size(options.arena_block_size),
      disable_auto_compactions(options.disable_auto_compactions),
      purge_redundant_kvs_while_flush(options.purge_redundant_kvs_while_flush),
      compaction_style(options.compaction_style),
      compaction_pri(options.compaction_pri),
      verify_checksums_in_compaction(options.verify_checksums_in_compaction),
      compaction_options_universal(options.compaction_options_universal),
      compaction_options_fifo(options.compaction_options_fifo),
      max_sequential_skip_in_iterations(
          options.max_sequential_skip_in_iterations),
      memtable_factory(options.memtable_factory),
      table_factory(options.table_factory),
      table_properties_collector_factories(
          options.table_properties_collector_factories),
      inplace_update_support(options.inplace_update_support),
      inplace_update_num_locks(options.inplace_update_num_locks),
      inplace_callback(options.inplace_callback),
      memtable_prefix_bloom_size_ratio(
          options.memtable_prefix_bloom_size_ratio),
      memtable_huge_page_size(options.memtable_huge_page_size),
      memtable_insert_with_hint_prefix_extractor(
          options.memtable_insert_with_hint_prefix_extractor),
      bloom_locality(options.bloom_locality),
      max_successive_merges(options.max_successive_merges),
      min_partial_merge_operands(options.min_partial_merge_operands),
      optimize_filters_for_hits(options.optimize_filters_for_hits),
      paranoid_file_checks(options.paranoid_file_checks),
      force_consistency_checks(options.force_consistency_checks),
      report_bg_io_stats(options.report_bg_io_stats) {
  assert(memtable_factory.get() != nullptr);
  if (max_bytes_for_level_multiplier_additional.size() <
      static_cast<unsigned int>(num_levels)) {
    max_bytes_for_level_multiplier_additional.resize(num_levels, 1);
  }
}

DBOptions::DBOptions()
    : create_if_missing(false),
      create_missing_column_families(false),
      error_if_exists(false),
      paranoid_checks(true),
      env(Env::Default()),
      rate_limiter(nullptr),
      sst_file_manager(nullptr),
      info_log(nullptr),
#ifdef NDEBUG
      info_log_level(INFO_LEVEL),
#else
      info_log_level(DEBUG_LEVEL),
#endif  // NDEBUG
      max_open_files(-1),
      max_file_opening_threads(16),
      max_total_wal_size(0),
      statistics(nullptr),
      disableDataSync(false),
      use_fsync(false),
      db_log_dir(""),
      wal_dir(""),
      delete_obsolete_files_period_micros(6ULL * 60 * 60 * 1000000),
      base_background_compactions(1),
      max_background_compactions(1),
      max_subcompactions(1),
      max_background_flushes(1),
      max_log_file_size(0),
      log_file_time_to_roll(0),
      keep_log_file_num(1000),
      recycle_log_file_num(0),
      max_manifest_file_size(std::numeric_limits<uint64_t>::max()),
      table_cache_numshardbits(6),
      WAL_ttl_seconds(0),
      WAL_size_limit_MB(0),
      manifest_preallocation_size(4 * 1024 * 1024),
      allow_mmap_reads(false),
      allow_mmap_writes(false),
      use_direct_reads(false),
      use_direct_writes(false),
      allow_fallocate(true),
      is_fd_close_on_exec(true),
      skip_log_error_on_recovery(false),
      stats_dump_period_sec(600),
      advise_random_on_open(true),
      db_write_buffer_size(0),
      access_hint_on_compaction_start(NORMAL),
      new_table_reader_for_compaction_inputs(false),
      compaction_readahead_size(0),
      random_access_max_buffer_size(1024 * 1024),
      writable_file_max_buffer_size(1024 * 1024),
      use_adaptive_mutex(false),
      bytes_per_sync(0),
      wal_bytes_per_sync(0),
      listeners(),
      enable_thread_tracking(false),
      delayed_write_rate(2 * 1024U * 1024U),
      allow_concurrent_memtable_write(true),
      enable_write_thread_adaptive_yield(true),
      write_thread_max_yield_usec(100),
      write_thread_slow_yield_usec(3),
      skip_stats_update_on_db_open(false),
      wal_recovery_mode(WALRecoveryMode::kPointInTimeRecovery),
      row_cache(nullptr),
#ifndef ROCKSDB_LITE
      wal_filter(nullptr),
#endif  // ROCKSDB_LITE
      fail_if_options_file_error(false),
      dump_malloc_stats(false),
      avoid_flush_during_recovery(false),
      avoid_flush_during_shutdown(false) {
}

DBOptions::DBOptions(const Options& options)
    : create_if_missing(options.create_if_missing),
      create_missing_column_families(options.create_missing_column_families),
      error_if_exists(options.error_if_exists),
      paranoid_checks(options.paranoid_checks),
      env(options.env),
      rate_limiter(options.rate_limiter),
      sst_file_manager(options.sst_file_manager),
      info_log(options.info_log),
      info_log_level(options.info_log_level),
      max_open_files(options.max_open_files),
      max_file_opening_threads(options.max_file_opening_threads),
      max_total_wal_size(options.max_total_wal_size),
      statistics(options.statistics),
      disableDataSync(options.disableDataSync),
      use_fsync(options.use_fsync),
      db_paths(options.db_paths),
      db_log_dir(options.db_log_dir),
      wal_dir(options.wal_dir),
      delete_obsolete_files_period_micros(
          options.delete_obsolete_files_period_micros),
      base_background_compactions(options.base_background_compactions),
      max_background_compactions(options.max_background_compactions),
      max_subcompactions(options.max_subcompactions),
      max_background_flushes(options.max_background_flushes),
      max_log_file_size(options.max_log_file_size),
      log_file_time_to_roll(options.log_file_time_to_roll),
      keep_log_file_num(options.keep_log_file_num),
      recycle_log_file_num(options.recycle_log_file_num),
      max_manifest_file_size(options.max_manifest_file_size),
      table_cache_numshardbits(options.table_cache_numshardbits),
      WAL_ttl_seconds(options.WAL_ttl_seconds),
      WAL_size_limit_MB(options.WAL_size_limit_MB),
      manifest_preallocation_size(options.manifest_preallocation_size),
      allow_mmap_reads(options.allow_mmap_reads),
      allow_mmap_writes(options.allow_mmap_writes),
      use_direct_reads(options.use_direct_reads),
      use_direct_writes(options.use_direct_writes),
      allow_fallocate(options.allow_fallocate),
      is_fd_close_on_exec(options.is_fd_close_on_exec),
      skip_log_error_on_recovery(options.skip_log_error_on_recovery),
      stats_dump_period_sec(options.stats_dump_period_sec),
      advise_random_on_open(options.advise_random_on_open),
      db_write_buffer_size(options.db_write_buffer_size),
      write_buffer_manager(options.write_buffer_manager),
      access_hint_on_compaction_start(options.access_hint_on_compaction_start),
      new_table_reader_for_compaction_inputs(
          options.new_table_reader_for_compaction_inputs),
      compaction_readahead_size(options.compaction_readahead_size),
      random_access_max_buffer_size(options.random_access_max_buffer_size),
      writable_file_max_buffer_size(options.writable_file_max_buffer_size),
      use_adaptive_mutex(options.use_adaptive_mutex),
      bytes_per_sync(options.bytes_per_sync),
      wal_bytes_per_sync(options.wal_bytes_per_sync),
      listeners(options.listeners),
      enable_thread_tracking(options.enable_thread_tracking),
      delayed_write_rate(options.delayed_write_rate),
      allow_concurrent_memtable_write(options.allow_concurrent_memtable_write),
      enable_write_thread_adaptive_yield(
          options.enable_write_thread_adaptive_yield),
      write_thread_max_yield_usec(options.write_thread_max_yield_usec),
      write_thread_slow_yield_usec(options.write_thread_slow_yield_usec),
      skip_stats_update_on_db_open(options.skip_stats_update_on_db_open),
      wal_recovery_mode(options.wal_recovery_mode),
      row_cache(options.row_cache),
#ifndef ROCKSDB_LITE
      wal_filter(options.wal_filter),
#endif  // ROCKSDB_LITE
      fail_if_options_file_error(options.fail_if_options_file_error),
      dump_malloc_stats(options.dump_malloc_stats),
      avoid_flush_during_recovery(options.avoid_flush_during_recovery),
      avoid_flush_during_shutdown(options.avoid_flush_during_shutdown) {
}

static const char* const access_hints[] = {
  "NONE", "NORMAL", "SEQUENTIAL", "WILLNEED"
};

void DBOptions::Dump(Logger* log) const {
    Header(log, "         Options.error_if_exists: %d", error_if_exists);
    Header(log, "       Options.create_if_missing: %d", create_if_missing);
    Header(log, "         Options.paranoid_checks: %d", paranoid_checks);
    Header(log, "                     Options.env: %p", env);
    Header(log, "                Options.info_log: %p", info_log.get());
    Header(log, "          Options.max_open_files: %d", max_open_files);
    Header(log,
        "Options.max_file_opening_threads: %d", max_file_opening_threads);
    Header(log,
        "      Options.max_total_wal_size: %" PRIu64, max_total_wal_size);
    Header(log, "       Options.disableDataSync: %d", disableDataSync);
    Header(log, "             Options.use_fsync: %d", use_fsync);
    Header(log, "     Options.max_log_file_size: %" ROCKSDB_PRIszt,
         max_log_file_size);
    Header(log, "Options.max_manifest_file_size: %" PRIu64,
         max_manifest_file_size);
    Header(log, "     Options.log_file_time_to_roll: %" ROCKSDB_PRIszt,
         log_file_time_to_roll);
    Header(log, "     Options.keep_log_file_num: %" ROCKSDB_PRIszt,
         keep_log_file_num);
    Header(log, "  Options.recycle_log_file_num: %" ROCKSDB_PRIszt,
           recycle_log_file_num);
    Header(log, "      Options.allow_fallocate: %d", allow_fallocate);
    Header(log, "      Options.allow_mmap_reads: %d", allow_mmap_reads);
    Header(log, "     Options.allow_mmap_writes: %d", allow_mmap_writes);
    Header(log, "      Options.use_direct_reads: %d", use_direct_reads);
    Header(log, "     Options.use_direct_writes: %d", use_direct_writes);
    Header(log, "         Options.create_missing_column_families: %d",
        create_missing_column_families);
    Header(log, "                             Options.db_log_dir: %s",
        db_log_dir.c_str());
    Header(log, "                                Options.wal_dir: %s",
        wal_dir.c_str());
    Header(log, "               Options.table_cache_numshardbits: %d",
        table_cache_numshardbits);
    Header(log, "    Options.delete_obsolete_files_period_micros: %" PRIu64,
        delete_obsolete_files_period_micros);
    Header(log, "             Options.base_background_compactions: %d",
           base_background_compactions);
    Header(log, "             Options.max_background_compactions: %d",
        max_background_compactions);
    Header(log, "                     Options.max_subcompactions: %" PRIu32,
        max_subcompactions);
    Header(log, "                 Options.max_background_flushes: %d",
        max_background_flushes);
    Header(log, "                        Options.WAL_ttl_seconds: %" PRIu64,
        WAL_ttl_seconds);
    Header(log, "                      Options.WAL_size_limit_MB: %" PRIu64,
        WAL_size_limit_MB);
    Header(log,
         "            Options.manifest_preallocation_size: %" ROCKSDB_PRIszt,
         manifest_preallocation_size);
    Header(log, "                     Options.is_fd_close_on_exec: %d",
        is_fd_close_on_exec);
    Header(log, "                   Options.stats_dump_period_sec: %u",
        stats_dump_period_sec);
    Header(log, "                   Options.advise_random_on_open: %d",
        advise_random_on_open);
    Header(log,
         "                    Options.db_write_buffer_size: %" ROCKSDB_PRIszt
         "d",
         db_write_buffer_size);
    Header(log, "         Options.access_hint_on_compaction_start: %s",
        access_hints[access_hint_on_compaction_start]);
    Header(log, "  Options.new_table_reader_for_compaction_inputs: %d",
         new_table_reader_for_compaction_inputs);
    Header(log,
         "               Options.compaction_readahead_size: %" ROCKSDB_PRIszt
         "d",
         compaction_readahead_size);
    Header(
        log,
        "               Options.random_access_max_buffer_size: %" ROCKSDB_PRIszt
        "d",
        random_access_max_buffer_size);
    Header(log,
         "              Options.writable_file_max_buffer_size: %" ROCKSDB_PRIszt
         "d",
         writable_file_max_buffer_size);
    Header(log, "                      Options.use_adaptive_mutex: %d",
        use_adaptive_mutex);
    Header(log, "                            Options.rate_limiter: %p",
        rate_limiter.get());
    Header(
        log, "     Options.sst_file_manager.rate_bytes_per_sec: %" PRIi64,
        sst_file_manager ? sst_file_manager->GetDeleteRateBytesPerSecond() : 0);
    Header(log, "                          Options.bytes_per_sync: %" PRIu64,
        bytes_per_sync);
    Header(log, "                      Options.wal_bytes_per_sync: %" PRIu64,
        wal_bytes_per_sync);
    Header(log, "                       Options.wal_recovery_mode: %d",
        wal_recovery_mode);
    Header(log, "                  Options.enable_thread_tracking: %d",
        enable_thread_tracking);
    Header(log, "         Options.allow_concurrent_memtable_write: %d",
           allow_concurrent_memtable_write);
    Header(log, "      Options.enable_write_thread_adaptive_yield: %d",
           enable_write_thread_adaptive_yield);
    Header(log, "             Options.write_thread_max_yield_usec: %" PRIu64,
           write_thread_max_yield_usec);
    Header(log, "            Options.write_thread_slow_yield_usec: %" PRIu64,
           write_thread_slow_yield_usec);
    if (row_cache) {
      Header(log, "                               Options.row_cache: %" PRIu64,
           row_cache->GetCapacity());
    } else {
      Header(log, "                               Options.row_cache: None");
    }
#ifndef ROCKSDB_LITE
    Header(log, "       Options.wal_filter: %s",
           wal_filter ? wal_filter->Name() : "None");
#endif  // ROCKDB_LITE
    Header(log, "                    Options.avoid_flush_during_recovery: %d",
           avoid_flush_during_recovery);
}  // DBOptions::Dump

void ColumnFamilyOptions::Dump(Logger* log) const {
  Header(log, "              Options.comparator: %s", comparator->Name());
  Header(log, "          Options.merge_operator: %s",
      merge_operator ? merge_operator->Name() : "None");
  Header(log, "       Options.compaction_filter: %s",
      compaction_filter ? compaction_filter->Name() : "None");
  Header(log, "       Options.compaction_filter_factory: %s",
      compaction_filter_factory ? compaction_filter_factory->Name() : "None");
  Header(log, "        Options.memtable_factory: %s", memtable_factory->Name());
  Header(log, "           Options.table_factory: %s", table_factory->Name());
  Header(log, "           table_factory options: %s",
      table_factory->GetPrintableTableOptions().c_str());
  Header(log, "       Options.write_buffer_size: %" ROCKSDB_PRIszt,
       write_buffer_size);
  Header(log, " Options.max_write_buffer_number: %d", max_write_buffer_number);
    if (!compression_per_level.empty()) {
      for (unsigned int i = 0; i < compression_per_level.size(); i++) {
        Header(log, "       Options.compression[%d]: %s", i,
            CompressionTypeToString(compression_per_level[i]).c_str());
      }
    } else {
      Header(log, "         Options.compression: %s",
          CompressionTypeToString(compression).c_str());
    }
    Header(log, "                 Options.bottommost_compression: %s",
           bottommost_compression == kDisableCompressionOption
               ? "Disabled"
               : CompressionTypeToString(bottommost_compression).c_str());
    Header(log, "      Options.prefix_extractor: %s",
        prefix_extractor == nullptr ? "nullptr" : prefix_extractor->Name());
    Header(log, "  Options.memtable_insert_with_hint_prefix_extractor: %s",
           memtable_insert_with_hint_prefix_extractor == nullptr
               ? "nullptr"
               : memtable_insert_with_hint_prefix_extractor->Name());
    Header(log, "            Options.num_levels: %d", num_levels);
    Header(log, "       Options.min_write_buffer_number_to_merge: %d",
        min_write_buffer_number_to_merge);
    Header(log, "    Options.max_write_buffer_number_to_maintain: %d",
         max_write_buffer_number_to_maintain);
    Header(log, "           Options.compression_opts.window_bits: %d",
        compression_opts.window_bits);
    Header(log, "                 Options.compression_opts.level: %d",
        compression_opts.level);
    Header(log, "              Options.compression_opts.strategy: %d",
        compression_opts.strategy);
    Header(log,
        "        Options.compression_opts.max_dict_bytes: %" ROCKSDB_PRIszt,
        compression_opts.max_dict_bytes);
    Header(log, "     Options.level0_file_num_compaction_trigger: %d",
        level0_file_num_compaction_trigger);
    Header(log, "         Options.level0_slowdown_writes_trigger: %d",
        level0_slowdown_writes_trigger);
    Header(log, "             Options.level0_stop_writes_trigger: %d",
        level0_stop_writes_trigger);
    Header(log, "                  Options.target_file_size_base: %" PRIu64,
        target_file_size_base);
    Header(log, "            Options.target_file_size_multiplier: %d",
        target_file_size_multiplier);
    Header(log, "               Options.max_bytes_for_level_base: %" PRIu64,
        max_bytes_for_level_base);
    Header(log, "Options.level_compaction_dynamic_level_bytes: %d",
        level_compaction_dynamic_level_bytes);
    Header(log, "         Options.max_bytes_for_level_multiplier: %f",
           max_bytes_for_level_multiplier);
    for (size_t i = 0; i < max_bytes_for_level_multiplier_additional.size();
         i++) {
      Header(log,
          "Options.max_bytes_for_level_multiplier_addtl[%" ROCKSDB_PRIszt
                "]: %d",
           i, max_bytes_for_level_multiplier_additional[i]);
    }
    Header(log, "      Options.max_sequential_skip_in_iterations: %" PRIu64,
        max_sequential_skip_in_iterations);
    Header(log, "                   Options.max_compaction_bytes: %" PRIu64,
           max_compaction_bytes);
    Header(log,
         "                       Options.arena_block_size: %" ROCKSDB_PRIszt,
         arena_block_size);
    Header(log, "  Options.soft_pending_compaction_bytes_limit: %" PRIu64,
           soft_pending_compaction_bytes_limit);
    Header(log, "  Options.hard_pending_compaction_bytes_limit: %" PRIu64,
         hard_pending_compaction_bytes_limit);
    Header(log, "      Options.rate_limit_delay_max_milliseconds: %u",
        rate_limit_delay_max_milliseconds);
    Header(log, "               Options.disable_auto_compactions: %d",
        disable_auto_compactions);
    Header(log, "          Options.verify_checksums_in_compaction: %d",
        verify_checksums_in_compaction);
    Header(log, "                        Options.compaction_style: %d",
        compaction_style);
    Header(log, "                          Options.compaction_pri: %d",
           compaction_pri);
    Header(log, " Options.compaction_options_universal.size_ratio: %u",
        compaction_options_universal.size_ratio);
    Header(log, "Options.compaction_options_universal.min_merge_width: %u",
        compaction_options_universal.min_merge_width);
    Header(log, "Options.compaction_options_universal.max_merge_width: %u",
        compaction_options_universal.max_merge_width);
    Header(log, "Options.compaction_options_universal."
            "max_size_amplification_percent: %u",
        compaction_options_universal.max_size_amplification_percent);
    Header(log,
        "Options.compaction_options_universal.compression_size_percent: %d",
        compaction_options_universal.compression_size_percent);
    Header(log,
        "Options.compaction_options_fifo.max_table_files_size: %" PRIu64,
        compaction_options_fifo.max_table_files_size);
    std::string collector_names;
    for (const auto& collector_factory : table_properties_collector_factories) {
      collector_names.append(collector_factory->Name());
      collector_names.append("; ");
    }
    Header(log, "                  Options.table_properties_collectors: %s",
        collector_names.c_str());
    Header(log, "                  Options.inplace_update_support: %d",
        inplace_update_support);
    Header(log,
         "                Options.inplace_update_num_locks: %" ROCKSDB_PRIszt,
         inplace_update_num_locks);
    Header(log, "              Options.min_partial_merge_operands: %u",
        min_partial_merge_operands);
    // TODO: easier config for bloom (maybe based on avg key/value size)
    Header(log, "              Options.memtable_prefix_bloom_size_ratio: %f",
           memtable_prefix_bloom_size_ratio);

    Header(log, "  Options.memtable_huge_page_size: %" ROCKSDB_PRIszt,
           memtable_huge_page_size);
    Header(log, "                          Options.bloom_locality: %d",
        bloom_locality);

    Header(log,
         "                   Options.max_successive_merges: %" ROCKSDB_PRIszt,
         max_successive_merges);
    Header(log, "               Options.optimize_filters_for_hits: %d",
        optimize_filters_for_hits);
    Header(log, "               Options.paranoid_file_checks: %d",
         paranoid_file_checks);
    Header(log, "               Options.force_consistency_checks: %d",
           force_consistency_checks);
    Header(log, "               Options.report_bg_io_stats: %d",
           report_bg_io_stats);
}  // ColumnFamilyOptions::Dump

void Options::Dump(Logger* log) const {
  DBOptions::Dump(log);
  ColumnFamilyOptions::Dump(log);
}   // Options::Dump

void Options::DumpCFOptions(Logger* log) const {
  ColumnFamilyOptions::Dump(log);
}  // Options::DumpCFOptions

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
  disableDataSync = true;
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
  base_background_compactions = 2;

  // The compaction would create large files in L1.
  target_file_size_base = 256 * 1024 * 1024;
  return this;
}

Options* Options::OptimizeForSmallDb() {
  ColumnFamilyOptions::OptimizeForSmallDb();
  DBOptions::OptimizeForSmallDb();
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
  max_open_files = 5000;
  base_background_compactions = -1;
  wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OldDefaults(
    int rocksdb_major_version, int rocksdb_minor_version) {
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
  }
  compaction_pri = CompactionPri::kByCompensatedSize;

  return this;
}

// Optimization functions
DBOptions* DBOptions::OptimizeForSmallDb() {
  max_file_opening_threads = 1;
  max_open_files = 5000;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForSmallDb() {
  write_buffer_size = 2 << 20;
  target_file_size_base = 2 * 1048576;
  max_bytes_for_level_base = 10 * 1048576;
  soft_pending_compaction_bytes_limit = 256 * 1048576;
  hard_pending_compaction_bytes_limit = 1073741824ul;
  return this;
}

#ifndef ROCKSDB_LITE
ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForPointLookup(
    uint64_t block_cache_size_mb) {
  prefix_extractor.reset(NewNoopTransform());
  BlockBasedTableOptions block_based_options;
  block_based_options.index_type = BlockBasedTableOptions::kHashSearch;
  block_based_options.filter_policy.reset(NewBloomFilterPolicy(10));
  block_based_options.block_cache =
      NewLRUCache(static_cast<size_t>(block_cache_size_mb * 1024 * 1024));
  table_factory.reset(new BlockBasedTableFactory(block_based_options));
  memtable_prefix_bloom_size_ratio = 0.02;
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
      compression_per_level[i] = kSnappyCompression;
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
  max_background_compactions = total_threads - 1;
  max_background_flushes = 1;
  env->SetBackgroundThreads(total_threads, Env::LOW);
  env->SetBackgroundThreads(1, Env::HIGH);
  return this;
}

#endif  // !ROCKSDB_LITE

ReadOptions::ReadOptions()
    : verify_checksums(true),
      fill_cache(true),
      snapshot(nullptr),
      iterate_upper_bound(nullptr),
      read_tier(kReadAllTier),
      tailing(false),
      managed(false),
      total_order_seek(false),
      prefix_same_as_start(false),
      pin_data(false),
      background_purge_on_iterator_cleanup(false),
      readahead_size(0),
      ignore_range_deletions(false) {
  XFUNC_TEST("", "managed_options", managed_options, xf_manage_options,
             reinterpret_cast<ReadOptions*>(this));
}

ReadOptions::ReadOptions(bool cksum, bool cache)
    : verify_checksums(cksum),
      fill_cache(cache),
      snapshot(nullptr),
      iterate_upper_bound(nullptr),
      read_tier(kReadAllTier),
      tailing(false),
      managed(false),
      total_order_seek(false),
      prefix_same_as_start(false),
      pin_data(false),
      background_purge_on_iterator_cleanup(false),
      readahead_size(0),
      ignore_range_deletions(false) {
  XFUNC_TEST("", "managed_options", managed_options, xf_manage_options,
             reinterpret_cast<ReadOptions*>(this));
}

}  // namespace rocksdb
