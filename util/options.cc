// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include <limits>

#include "leveldb/cache.h"
#include "leveldb/compaction_filter.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/merge_operator.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      merge_operator(nullptr),
      compaction_filter(nullptr),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(nullptr),
      write_buffer_size(4<<20),
      max_write_buffer_number(2),
      min_write_buffer_number_to_merge(1),
      max_open_files(1000),
      block_size(4096),
      block_restart_interval(16),
      compression(kSnappyCompression),
      filter_policy(nullptr),
      num_levels(7),
      level0_file_num_compaction_trigger(4),
      level0_slowdown_writes_trigger(8),
      level0_stop_writes_trigger(12),
      max_mem_compaction_level(2),
      target_file_size_base(2 * 1048576),
      target_file_size_multiplier(1),
      max_bytes_for_level_base(10 * 1048576),
      max_bytes_for_level_multiplier(10),
      max_bytes_for_level_multiplier_additional(num_levels, 1),
      expanded_compaction_factor(25),
      source_compaction_factor(1),
      max_grandparent_overlap_factor(10),
      disableDataSync(false),
      use_fsync(false),
      db_stats_log_interval(1800),
      db_log_dir(""),
      disable_seek_compaction(false),
      delete_obsolete_files_period_micros(0),
      max_background_compactions(1),
      max_log_file_size(0),
      log_file_time_to_roll(0),
      keep_log_file_num(1000),
      rate_limit(0.0),
      rate_limit_delay_milliseconds(1000),
      max_manifest_file_size(std::numeric_limits<uint64_t>::max()),
      no_block_cache(false),
      table_cache_numshardbits(4),
      disable_auto_compactions(false),
      WAL_ttl_seconds(0),
      manifest_preallocation_size(4 * 1024 * 1024),
      purge_redundant_kvs_while_flush(true),
      allow_os_buffer(true),
      allow_mmap_reads(false),
      allow_mmap_writes(true),
      is_fd_close_on_exec(true),
      skip_log_error_on_recovery(false),
      stats_dump_period_sec(3600),
      block_size_deviation (10),
      advise_random_on_open(true),
      access_hint_on_compaction_start(NORMAL),
      use_adaptive_mutex(false),
      bytes_per_sync(0),
      deletes_check_filter_first(false) {
}

static const char* const access_hints[] = {
  "NONE", "NORMAL", "SEQUENTIAL", "WILLNEED"
};

void
Options::Dump(Logger* log) const
{
    Log(log,"              Options.comparator: %s", comparator->Name());
    Log(log,"          Options.merge_operator: %s",
        merge_operator? merge_operator->Name() : "None");
    Log(log,"       Options.compaction_filter: %s",
        compaction_filter? compaction_filter->Name() : "None");
    Log(log,"         Options.error_if_exists: %d", error_if_exists);
    Log(log,"         Options.paranoid_checks: %d", paranoid_checks);
    Log(log,"                     Options.env: %p", env);
    Log(log,"                Options.info_log: %p", info_log.get());
    Log(log,"       Options.write_buffer_size: %zd", write_buffer_size);
    Log(log," Options.max_write_buffer_number: %d", max_write_buffer_number);
    Log(log,"          Options.max_open_files: %d", max_open_files);
    Log(log,"             Options.block_cache: %p", block_cache.get());
    if (block_cache) {
      Log(log,"      Options.block_cache_size: %zd",
          block_cache->GetCapacity());
    }
    Log(log,"              Options.block_size: %zd", block_size);
    Log(log,"  Options.block_restart_interval: %d", block_restart_interval);
    if (!compression_per_level.empty()) {
      for (unsigned int i = 0; i < compression_per_level.size(); i++) {
          Log(log,"       Options.compression[%d]: %d",
              i, compression_per_level[i]);
       }
    } else {
      Log(log,"         Options.compression: %d", compression);
    }
    Log(log,"         Options.filter_policy: %s",
        filter_policy == nullptr ? "nullptr" : filter_policy->Name());
    Log(log,"            Options.num_levels: %d", num_levels);
    Log(log,"       Options.disableDataSync: %d", disableDataSync);
    Log(log,"             Options.use_fsync: %d", use_fsync);
    Log(log,"     Options.max_log_file_size: %ld", max_log_file_size);
    Log(log,"Options.max_manifest_file_size: %ld",
      max_manifest_file_size);
    Log(log,"     Options.log_file_time_to_roll: %ld", log_file_time_to_roll);
    Log(log,"     Options.keep_log_file_num: %ld", keep_log_file_num);
    Log(log," Options.db_stats_log_interval: %d",
        db_stats_log_interval);
    Log(log,"       Options.allow_os_buffer: %d", allow_os_buffer);
    Log(log,"      Options.allow_mmap_reads: %d", allow_mmap_reads);
    Log(log,"     Options.allow_mmap_writes: %d", allow_mmap_writes);
    Log(log,"       Options.min_write_buffer_number_to_merge: %d",
        min_write_buffer_number_to_merge);
    Log(log,"        Options.purge_redundant_kvs_while_flush: %d",
         purge_redundant_kvs_while_flush);
    Log(log,"           Options.compression_opts.window_bits: %d",
        compression_opts.window_bits);
    Log(log,"                 Options.compression_opts.level: %d",
        compression_opts.level);
    Log(log,"              Options.compression_opts.strategy: %d",
        compression_opts.strategy);
    Log(log,"     Options.level0_file_num_compaction_trigger: %d",
        level0_file_num_compaction_trigger);
    Log(log,"         Options.level0_slowdown_writes_trigger: %d",
        level0_slowdown_writes_trigger);
    Log(log,"             Options.level0_stop_writes_trigger: %d",
        level0_stop_writes_trigger);
    Log(log,"               Options.max_mem_compaction_level: %d",
        max_mem_compaction_level);
    Log(log,"                  Options.target_file_size_base: %d",
        target_file_size_base);
    Log(log,"            Options.target_file_size_multiplier: %d",
        target_file_size_multiplier);
    Log(log,"               Options.max_bytes_for_level_base: %ld",
        max_bytes_for_level_base);
    Log(log,"         Options.max_bytes_for_level_multiplier: %d",
        max_bytes_for_level_multiplier);
    for (int i = 0; i < num_levels; i++) {
      Log(log,"Options.max_bytes_for_level_multiplier_addtl[%d]: %d",
          i, max_bytes_for_level_multiplier_additional[i]);
    }
    Log(log,"             Options.expanded_compaction_factor: %d",
        expanded_compaction_factor);
    Log(log,"               Options.source_compaction_factor: %d",
        source_compaction_factor);
    Log(log,"         Options.max_grandparent_overlap_factor: %d",
        max_grandparent_overlap_factor);
    Log(log,"                             Options.db_log_dir: %s",
        db_log_dir.c_str());
    Log(log,"                Options.disable_seek_compaction: %d",
        disable_seek_compaction);
    Log(log,"                         Options.no_block_cache: %d",
        no_block_cache);
    Log(log,"               Options.table_cache_numshardbits: %d",
        table_cache_numshardbits);
    Log(log,"    Options.delete_obsolete_files_period_micros: %ld",
        delete_obsolete_files_period_micros);
    Log(log,"             Options.max_background_compactions: %d",
        max_background_compactions);
    Log(log,"                             Options.rate_limit: %.2f",
        rate_limit);
    Log(log,"          Options.rate_limit_delay_milliseconds: %d",
        rate_limit_delay_milliseconds);
    Log(log,"               Options.disable_auto_compactions: %d",
        disable_auto_compactions);
    Log(log,"                        Options.WAL_ttl_seconds: %ld",
        WAL_ttl_seconds);
    Log(log,"            Options.manifest_preallocation_size: %ld",
        manifest_preallocation_size);
    Log(log,"         Options.purge_redundant_kvs_while_flush: %d",
        purge_redundant_kvs_while_flush);
    Log(log,"                         Options.allow_os_buffer: %d",
        allow_os_buffer);
    Log(log,"                        Options.allow_mmap_reads: %d",
        allow_mmap_reads);
    Log(log,"                       Options.allow_mmap_writes: %d",
        allow_mmap_writes);
    Log(log,"                     Options.is_fd_close_on_exec: %d",
        is_fd_close_on_exec);
    Log(log,"              Options.skip_log_error_on_recovery: %d",
        skip_log_error_on_recovery);
    Log(log,"                   Options.stats_dump_period_sec: %d",
        stats_dump_period_sec);
    Log(log,"                    Options.block_size_deviation: %d",
        block_size_deviation);
    Log(log,"                   Options.advise_random_on_open: %d",
        advise_random_on_open);
    Log(log,"         Options.access_hint_on_compaction_start: %s",
        access_hints[access_hint_on_compaction_start]);
    Log(log,"                      Options.use_adaptive_mutex: %d",
        use_adaptive_mutex);
    Log(log,"                          Options.bytes_per_sync: %ld",
        bytes_per_sync);
    Log(log,"              Options.deletes_check_filter_first: %d",
        deletes_check_filter_first);
}   // Options::Dump

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

  // no auto compactions please. The application should issue a
  // manual compaction after all data is loaded into L0.
  disable_auto_compactions = true;
  disable_seek_compaction = true;
  disableDataSync = true;

  // A manual compaction run should pick all files in L0 in
  // a single compaction run.
  source_compaction_factor = (1<<30);

  // It is better to have only 2 levels, otherwise a manual
  // compaction would compact at every possible level, thereby
  // increasing the total time needed for compactions.
  num_levels = 2;

  // Prevent a memtable flush to automatically promote files
  // to L1. This is helpful so that all files that are
  // input to the manual compaction are all at L0.
  max_background_compactions = 2;

  // The compaction would create large files in L1.
  target_file_size_base = 256 * 1024 * 1024;
  return this;
}

}  // namespace leveldb
