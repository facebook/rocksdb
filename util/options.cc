// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"

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
      filter_policy(NULL),
      statistics(NULL),
      disableDataSync(false),
      db_stats_log_interval(1800) {
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
    Log(log,"            Options.block_size: %zd", block_size);
    Log(log,"Options.block_restart_interval: %d", block_restart_interval);
    Log(log,"           Options.compression: %d", compression);
    Log(log,"         Options.filter_policy: %s", filter_policy == NULL ? "NULL" : filter_policy->Name());

}   // Options::Dump


}  // namespace leveldb
