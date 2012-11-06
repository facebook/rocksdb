// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/cache.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      write_buffer_size(4<<20),
      max_open_files(1000),
      block_cache(NULL),
      block_size(4096),
      block_restart_interval(16),
      compression(kSnappyCompression),
      compression_per_level(NULL),
      filter_policy(NULL),
      num_levels(7),
      level0_file_num_compaction_trigger(4),
      level0_slowdown_writes_trigger(8),
      level0_stop_writes_trigger(12),
      max_mem_compaction_level(2),
      target_file_size_base(2 * 1048576),
      target_file_size_multiplier(1),
      max_bytes_for_level_base(10 * 1048576),
      max_bytes_for_level_multiplier(10),
      expanded_compaction_factor(25),
      max_grandparent_overlap_factor(10),
      statistics(NULL),
      disableDataSync(false),
      use_fsync(false),
      db_stats_log_interval(1800),
      db_log_dir(""),
      disable_seek_compaction(false),
      delete_obsolete_files_period_micros(0),
      max_log_file_size(0),
      rate_limit(0.0), 
      no_block_cache(false),
      table_cache_numshardbits(4),
      CompactionFilter(NULL) {
}

void
Options::Dump(
    Logger * log) const
{
    Log(log,"            Options.comparator: %s", comparator->Name());
    Log(log,"     Options.create_if_missing: %d", create_if_missing);
    Log(log,"       Options.error_if_exists: %d", error_if_exists);
    Log(log,"       Options.paranoid_checks: %d", paranoid_checks);
    Log(log,"                   Options.env: %p", env);
    Log(log,"              Options.info_log: %p", info_log);
    Log(log,"     Options.write_buffer_size: %zd", write_buffer_size);
    Log(log,"        Options.max_open_files: %d", max_open_files);
    Log(log,"           Options.block_cache: %p", block_cache);
    if (block_cache) {
      Log(log,"      Options.block_cache_size: %zd",
          block_cache->GetCapacity());
    }
    Log(log,"            Options.block_size: %zd", block_size);
    Log(log,"Options.block_restart_interval: %d", block_restart_interval);
    if (compression_per_level != NULL) {
       for (int i = 0; i < num_levels; i++){
          Log(log,"       Options.compression[%d]: %d",
              i, compression_per_level[i]);
       }
    } else {
      Log(log,"           Options.compression: %d", compression);
    }
    Log(log,"         Options.filter_policy: %s",
        filter_policy == NULL ? "NULL" : filter_policy->Name());
    Log(log,"            Options.num_levels: %d", num_levels);
    Log(log,"       Options.disableDataSync: %d", disableDataSync);
    Log(log,"             Options.use_fsync: %d", use_fsync);
    Log(log,"     Options.max_log_file_size: %ld", max_log_file_size);
    Log(log," Options.db_stats_log_interval: %d",
        db_stats_log_interval);
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
    Log(log,"               Options.max_bytes_for_level_base: %d",
        max_bytes_for_level_base);
    Log(log,"         Options.max_bytes_for_level_multiplier: %d",
        max_bytes_for_level_multiplier);
    Log(log,"             Options.expanded_compaction_factor: %d",
        expanded_compaction_factor);
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
    Log(log,"                             Options.rate_limit: %.2f",
        rate_limit);
    Log(log,"                       Options.CompactionFilter: %p",
        CompactionFilter);
}   // Options::Dump


}  // namespace leveldb
