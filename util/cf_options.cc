//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "util/cf_options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <cassert>
#include <limits>
#include <string>
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "util/db_options.h"

namespace rocksdb {

ImmutableCFOptions::ImmutableCFOptions(const Options& options)
    : ImmutableCFOptions(ImmutableDBOptions(options), options) {}

ImmutableCFOptions::ImmutableCFOptions(const ImmutableDBOptions& db_options,
                                       const ColumnFamilyOptions& cf_options)
    : compaction_style(cf_options.compaction_style),
      compaction_pri(cf_options.compaction_pri),
      compaction_options_universal(cf_options.compaction_options_universal),
      compaction_options_fifo(cf_options.compaction_options_fifo),
      prefix_extractor(cf_options.prefix_extractor.get()),
      user_comparator(cf_options.comparator),
      internal_comparator(InternalKeyComparator(cf_options.comparator)),
      merge_operator(cf_options.merge_operator.get()),
      compaction_filter(cf_options.compaction_filter),
      compaction_filter_factory(cf_options.compaction_filter_factory.get()),
      min_write_buffer_number_to_merge(
          cf_options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          cf_options.max_write_buffer_number_to_maintain),
      inplace_update_support(cf_options.inplace_update_support),
      inplace_callback(cf_options.inplace_callback),
      info_log(db_options.info_log.get()),
      statistics(db_options.statistics.get()),
      env(db_options.env),
      allow_mmap_reads(db_options.allow_mmap_reads),
      allow_mmap_writes(db_options.allow_mmap_writes),
      db_paths(db_options.db_paths),
      memtable_factory(cf_options.memtable_factory.get()),
      table_factory(cf_options.table_factory.get()),
      table_properties_collector_factories(
          cf_options.table_properties_collector_factories),
      advise_random_on_open(db_options.advise_random_on_open),
      bloom_locality(cf_options.bloom_locality),
      purge_redundant_kvs_while_flush(
          cf_options.purge_redundant_kvs_while_flush),
      disable_data_sync(db_options.disable_data_sync),
      use_fsync(db_options.use_fsync),
      compression_per_level(cf_options.compression_per_level),
      bottommost_compression(cf_options.bottommost_compression),
      compression_opts(cf_options.compression_opts),
      level_compaction_dynamic_level_bytes(
          cf_options.level_compaction_dynamic_level_bytes),
      access_hint_on_compaction_start(
          db_options.access_hint_on_compaction_start),
      new_table_reader_for_compaction_inputs(
          db_options.new_table_reader_for_compaction_inputs),
      compaction_readahead_size(db_options.compaction_readahead_size),
      num_levels(cf_options.num_levels),
      optimize_filters_for_hits(cf_options.optimize_filters_for_hits),
      force_consistency_checks(cf_options.force_consistency_checks),
      listeners(db_options.listeners),
      row_cache(db_options.row_cache),
      max_subcompactions(db_options.max_subcompactions),
      memtable_insert_with_hint_prefix_extractor(
          cf_options.memtable_insert_with_hint_prefix_extractor.get()) {}

// Multiple two operands. If they overflow, return op1.
uint64_t MultiplyCheckOverflow(uint64_t op1, double op2) {
  if (op1 == 0 || op2 <= 0) {
    return 0;
  }
  if (port::kMaxUint64 / op1 < op2) {
    return op1;
  }
  return static_cast<uint64_t>(op1 * op2);
}

void MutableCFOptions::RefreshDerivedOptions(int num_levels,
                                             CompactionStyle compaction_style) {
  max_file_size.resize(num_levels);
  for (int i = 0; i < num_levels; ++i) {
    if (i == 0 && compaction_style == kCompactionStyleUniversal) {
      max_file_size[i] = ULLONG_MAX;
    } else if (i > 1) {
      max_file_size[i] = MultiplyCheckOverflow(max_file_size[i - 1],
                                               target_file_size_multiplier);
    } else {
      max_file_size[i] = target_file_size_base;
    }
  }
}

uint64_t MutableCFOptions::MaxFileSizeForLevel(int level) const {
  assert(level >= 0);
  assert(level < (int)max_file_size.size());
  return max_file_size[level];
}

void MutableCFOptions::Dump(Logger* log) const {
  // Memtable related options
  Log(log, "                        write_buffer_size: %" ROCKSDB_PRIszt,
      write_buffer_size);
  Log(log, "                  max_write_buffer_number: %d",
      max_write_buffer_number);
  Log(log, "                         arena_block_size: %" ROCKSDB_PRIszt,
      arena_block_size);
  Log(log, "              memtable_prefix_bloom_ratio: %f",
      memtable_prefix_bloom_size_ratio);
  Log(log, "                  memtable_huge_page_size: %" ROCKSDB_PRIszt,
      memtable_huge_page_size);
  Log(log, "                    max_successive_merges: %" ROCKSDB_PRIszt,
      max_successive_merges);
  Log(log, "                 inplace_update_num_locks: %" ROCKSDB_PRIszt,
      inplace_update_num_locks);
  Log(log, "                 disable_auto_compactions: %d",
      disable_auto_compactions);
  Log(log, "      soft_pending_compaction_bytes_limit: %" PRIu64,
      soft_pending_compaction_bytes_limit);
  Log(log, "      hard_pending_compaction_bytes_limit: %" PRIu64,
      hard_pending_compaction_bytes_limit);
  Log(log, "       level0_file_num_compaction_trigger: %d",
      level0_file_num_compaction_trigger);
  Log(log, "           level0_slowdown_writes_trigger: %d",
      level0_slowdown_writes_trigger);
  Log(log, "               level0_stop_writes_trigger: %d",
      level0_stop_writes_trigger);
  Log(log, "                     max_compaction_bytes: %" PRIu64,
      max_compaction_bytes);
  Log(log, "                    target_file_size_base: %" PRIu64,
      target_file_size_base);
  Log(log, "              target_file_size_multiplier: %d",
      target_file_size_multiplier);
  Log(log, "                 max_bytes_for_level_base: %" PRIu64,
      max_bytes_for_level_base);
  Log(log, "           max_bytes_for_level_multiplier: %f",
      max_bytes_for_level_multiplier);
  std::string result;
  char buf[10];
  for (const auto m : max_bytes_for_level_multiplier_additional) {
    snprintf(buf, sizeof(buf), "%d, ", m);
    result += buf;
  }
  if (result.size() >= 2) {
    result.resize(result.size() - 2);
  } else {
    result = "";
  }

  Log(log, "max_bytes_for_level_multiplier_additional: %s", result.c_str());
  Log(log, "           verify_checksums_in_compaction: %d",
      verify_checksums_in_compaction);
  Log(log, "        max_sequential_skip_in_iterations: %" PRIu64,
      max_sequential_skip_in_iterations);
  Log(log, "                     paranoid_file_checks: %d",
      paranoid_file_checks);
  Log(log, "                       report_bg_io_stats: %d", report_bg_io_stats);
  Log(log, "                              compression: %d",
      static_cast<int>(compression));
  Log(log, "               min_partial_merge_operands: %" PRIu32,
      min_partial_merge_operands);
}

}  // namespace rocksdb
