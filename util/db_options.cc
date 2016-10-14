// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "util/db_options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/wal_filter.h"

namespace rocksdb {

ImmutableDBOptions::ImmutableDBOptions() : ImmutableDBOptions(Options()) {}

ImmutableDBOptions::ImmutableDBOptions(const DBOptions& options)
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
      disable_data_sync(options.disableDataSync),
      use_fsync(options.use_fsync),
      db_paths(options.db_paths),
      db_log_dir(options.db_log_dir),
      wal_dir(options.wal_dir),
      delete_obsolete_files_period_micros(
          options.delete_obsolete_files_period_micros),
      max_subcompactions(options.max_subcompactions),
      max_background_flushes(options.max_background_flushes),
      max_log_file_size(options.max_log_file_size),
      log_file_time_to_roll(options.log_file_time_to_roll),
      keep_log_file_num(options.keep_log_file_num),
      recycle_log_file_num(options.recycle_log_file_num),
      max_manifest_file_size(options.max_manifest_file_size),
      table_cache_numshardbits(options.table_cache_numshardbits),
      wal_ttl_seconds(options.WAL_ttl_seconds),
      wal_size_limit_mb(options.WAL_size_limit_MB),
      manifest_preallocation_size(options.manifest_preallocation_size),
      allow_os_buffer(options.allow_os_buffer),
      allow_mmap_reads(options.allow_mmap_reads),
      allow_mmap_writes(options.allow_mmap_writes),
      allow_fallocate(options.allow_fallocate),
      is_fd_close_on_exec(options.is_fd_close_on_exec),
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
      allow_2pc(options.allow_2pc),
      row_cache(options.row_cache),
#ifndef ROCKSDB_LITE
      wal_filter(options.wal_filter),
#endif  // ROCKSDB_LITE
      fail_if_options_file_error(options.fail_if_options_file_error),
      dump_malloc_stats(options.dump_malloc_stats),
      avoid_flush_during_recovery(options.avoid_flush_during_recovery) {
}

void ImmutableDBOptions::Dump(Logger* log) const {
  Header(log, "                        Options.error_if_exists: %d",
         error_if_exists);
  Header(log, "                      Options.create_if_missing: %d",
         create_if_missing);
  Header(log, "                        Options.paranoid_checks: %d",
         paranoid_checks);
  Header(log, "                                    Options.env: %p", env);
  Header(log, "                               Options.info_log: %p",
         info_log.get());
  Header(log, "                         Options.max_open_files: %d",
         max_open_files);
  Header(log, "               Options.max_file_opening_threads: %d",
         max_file_opening_threads);
  Header(log, "                     Options.max_total_wal_size: %" PRIu64,
         max_total_wal_size);
  Header(log, "                        Options.disableDataSync: %d",
         disable_data_sync);
  Header(log, "                              Options.use_fsync: %d", use_fsync);
  Header(log,
         "                      Options.max_log_file_size: %" ROCKSDB_PRIszt,
         max_log_file_size);
  Header(log, "                 Options.max_manifest_file_size: %" PRIu64,
         max_manifest_file_size);
  Header(log,
         "                  Options.log_file_time_to_roll: %" ROCKSDB_PRIszt,
         log_file_time_to_roll);
  Header(log,
         "                      Options.keep_log_file_num: %" ROCKSDB_PRIszt,
         keep_log_file_num);
  Header(log,
         "                   Options.recycle_log_file_num: %" ROCKSDB_PRIszt,
         recycle_log_file_num);
  Header(log, "                        Options.allow_os_buffer: %d",
         allow_os_buffer);
  Header(log, "                       Options.allow_mmap_reads: %d",
         allow_mmap_reads);
  Header(log, "                        Options.allow_fallocate: %d",
         allow_fallocate);
  Header(log, "                      Options.allow_mmap_writes: %d",
         allow_mmap_writes);
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
  Header(log, "                     Options.max_subcompactions: %" PRIu32,
         max_subcompactions);
  Header(log, "                 Options.max_background_flushes: %d",
         max_background_flushes);
  Header(log, "                        Options.WAL_ttl_seconds: %" PRIu64,
         wal_ttl_seconds);
  Header(log, "                      Options.WAL_size_limit_MB: %" PRIu64,
         wal_size_limit_mb);
  Header(log,
         "            Options.manifest_preallocation_size: %" ROCKSDB_PRIszt,
         manifest_preallocation_size);
  Header(log, "                        Options.allow_os_buffer: %d",
         allow_os_buffer);
  Header(log, "                       Options.allow_mmap_reads: %d",
         allow_mmap_reads);
  Header(log, "                      Options.allow_mmap_writes: %d",
         allow_mmap_writes);
  Header(log, "                    Options.is_fd_close_on_exec: %d",
         is_fd_close_on_exec);
  Header(log, "                  Options.stats_dump_period_sec: %u",
         stats_dump_period_sec);
  Header(log, "                  Options.advise_random_on_open: %d",
         advise_random_on_open);
  Header(log,
         "                   Options.db_write_buffer_size: %" ROCKSDB_PRIszt,
         db_write_buffer_size);
  Header(log, "        Options.access_hint_on_compaction_start: %d",
         static_cast<int>(access_hint_on_compaction_start));
  Header(log, " Options.new_table_reader_for_compaction_inputs: %d",
         new_table_reader_for_compaction_inputs);
  Header(log,
         "              Options.compaction_readahead_size: %" ROCKSDB_PRIszt,
         compaction_readahead_size);
  Header(log,
         "          Options.random_access_max_buffer_size: %" ROCKSDB_PRIszt,
         random_access_max_buffer_size);
  Header(log,
         "          Options.writable_file_max_buffer_size: %" ROCKSDB_PRIszt,
         writable_file_max_buffer_size);
  Header(log, "                     Options.use_adaptive_mutex: %d",
         use_adaptive_mutex);
  Header(log, "                           Options.rate_limiter: %p",
         rate_limiter.get());
  Header(
      log, "    Options.sst_file_manager.rate_bytes_per_sec: %" PRIi64,
      sst_file_manager ? sst_file_manager->GetDeleteRateBytesPerSecond() : 0);
  Header(log, "                         Options.bytes_per_sync: %" PRIu64,
         bytes_per_sync);
  Header(log, "                     Options.wal_bytes_per_sync: %" PRIu64,
         wal_bytes_per_sync);
  Header(log, "                      Options.wal_recovery_mode: %d",
         wal_recovery_mode);
  Header(log, "                 Options.enable_thread_tracking: %d",
         enable_thread_tracking);
  Header(log, "                    Options.delayed_write_rate : %" PRIu64,
         delayed_write_rate);
  Header(log, "        Options.allow_concurrent_memtable_write: %d",
         allow_concurrent_memtable_write);
  Header(log, "     Options.enable_write_thread_adaptive_yield: %d",
         enable_write_thread_adaptive_yield);
  Header(log, "            Options.write_thread_max_yield_usec: %" PRIu64,
         write_thread_max_yield_usec);
  Header(log, "           Options.write_thread_slow_yield_usec: %" PRIu64,
         write_thread_slow_yield_usec);
  if (row_cache) {
    Header(log, "                              Options.row_cache: %" PRIu64,
           row_cache->GetCapacity());
  } else {
    Header(log, "                              Options.row_cache: None");
  }
#ifndef ROCKSDB_LITE
  Header(log, "                             Options.wal_filter: %s",
         wal_filter ? wal_filter->Name() : "None");
#endif  // ROCKDB_LITE
  Header(log, "            Options.avoid_flush_during_recovery: %d",
         avoid_flush_during_recovery);
}

MutableDBOptions::MutableDBOptions()
    : base_background_compactions(1), max_background_compactions(1) {}

MutableDBOptions::MutableDBOptions(const DBOptions& options)
    : base_background_compactions(options.base_background_compactions),
      max_background_compactions(options.max_background_compactions) {}

void MutableDBOptions::Dump(Logger* log) const {
  Header(log, "            Options.base_background_compactions: %d",
         base_background_compactions);
  Header(log, "             Options.max_background_compactions: %d",
         max_background_compactions);
}

}  // namespace rocksdb
