//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "util/cf_options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <limits>
#include <cassert>
#include <string>
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

namespace rocksdb {

ImmutableCFOptions::ImmutableCFOptions(const Options& options)
    : compaction_style(options.compaction_style),
      compaction_options_universal(options.compaction_options_universal),
      compaction_options_fifo(options.compaction_options_fifo),
      prefix_extractor(options.prefix_extractor.get()),
      comparator(options.comparator),
      merge_operator(options.merge_operator.get()),
      compaction_filter(options.compaction_filter),
      compaction_filter_factory(options.compaction_filter_factory.get()),
      inplace_update_support(options.inplace_update_support),
      inplace_callback(options.inplace_callback),
      info_log(options.info_log.get()),
      statistics(options.statistics.get()),
      env(options.env),
      delayed_write_rate(options.delayed_write_rate),
      allow_mmap_reads(options.allow_mmap_reads),
      allow_mmap_writes(options.allow_mmap_writes),
      db_paths(options.db_paths),
      memtable_factory(options.memtable_factory.get()),
      table_factory(options.table_factory.get()),
      table_properties_collector_factories(
          options.table_properties_collector_factories),
      advise_random_on_open(options.advise_random_on_open),
      bloom_locality(options.bloom_locality),
      purge_redundant_kvs_while_flush(options.purge_redundant_kvs_while_flush),
      disable_data_sync(options.disableDataSync),
      use_fsync(options.use_fsync),
      compression_per_level(options.compression_per_level),
      bottommost_compression(options.bottommost_compression),
      compression_opts(options.compression_opts),
      level_compaction_dynamic_level_bytes(
          options.level_compaction_dynamic_level_bytes),
      access_hint_on_compaction_start(options.access_hint_on_compaction_start),
      new_table_reader_for_compaction_inputs(
          options.new_table_reader_for_compaction_inputs),
      compaction_readahead_size(options.compaction_readahead_size),
      num_levels(options.num_levels),
      optimize_filters_for_hits(options.optimize_filters_for_hits),
      listeners(options.listeners),
      row_cache(options.row_cache) {}

// Multiple two operands. If they overflow, return op1.
uint64_t MultiplyCheckOverflow(uint64_t op1, int op2) {
  if (op1 == 0) {
    return 0;
  }
  if (op2 <= 0) {
    return op1;
  }
  uint64_t casted_op2 = (uint64_t) op2;
  if (std::numeric_limits<uint64_t>::max() / op1 < casted_op2) {
    return op1;
  }
  return op1 * casted_op2;
}

void MutableCFOptions::RefreshDerivedOptions(
    const ImmutableCFOptions& ioptions) {
  max_file_size.resize(ioptions.num_levels);
  for (int i = 0; i < ioptions.num_levels; ++i) {
    if (i == 0 && ioptions.compaction_style == kCompactionStyleUniversal) {
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
  Log(log, " memtable_huge_page_size: %" ROCKSDB_PRIszt,
      memtable_huge_page_size);
  Log(log, "                    max_successive_merges: %" ROCKSDB_PRIszt,
      max_successive_merges);
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
  Log(log, "           max_bytes_for_level_multiplier: %d",
      max_bytes_for_level_multiplier);
  std::string result;
  char buf[10];
  for (const auto m : max_bytes_for_level_multiplier_additional) {
    snprintf(buf, sizeof(buf), "%d, ", m);
    result += buf;
  }
  result.resize(result.size() - 2);
  Log(log, "max_bytes_for_level_multiplier_additional: %s", result.c_str());
  Log(log, "           verify_checksums_in_compaction: %d",
      verify_checksums_in_compaction);
  Log(log, "        max_sequential_skip_in_iterations: %" PRIu64,
      max_sequential_skip_in_iterations);
}

}  // namespace rocksdb
