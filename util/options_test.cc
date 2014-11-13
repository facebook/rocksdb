//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <unordered_map>
#include <inttypes.h>
#include <gflags/gflags.h>

#include "rocksdb/options.h"
#include "util/testharness.h"
#include "rocksdb/utilities/convenience.h"

using GFLAGS::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");

namespace rocksdb {

class OptionsTest {};

class StderrLogger : public Logger {
 public:
  virtual void Logv(const char* format, va_list ap) override {
    vprintf(format, ap);
    printf("\n");
  }
};

Options PrintAndGetOptions(size_t total_write_buffer_limit,
                           int read_amplification_threshold,
                           int write_amplification_threshold,
                           uint64_t target_db_size = 68719476736) {
  StderrLogger logger;

  if (FLAGS_enable_print) {
    printf(
        "---- total_write_buffer_limit: %zu "
        "read_amplification_threshold: %d write_amplification_threshold: %d "
        "target_db_size %" PRIu64 " ----\n",
        total_write_buffer_limit, read_amplification_threshold,
        write_amplification_threshold, target_db_size);
  }

  Options options =
      GetOptions(total_write_buffer_limit, read_amplification_threshold,
                 write_amplification_threshold, target_db_size);
  if (FLAGS_enable_print) {
    options.Dump(&logger);
    printf("-------------------------------------\n\n\n");
  }
  return options;
}

TEST(OptionsTest, LooseCondition) {
  Options options;
  PrintAndGetOptions(static_cast<size_t>(10) * 1024 * 1024 * 1024, 100, 100);

  // Less mem table memory budget
  PrintAndGetOptions(32 * 1024 * 1024, 100, 100);

  // Tight read amplification
  options = PrintAndGetOptions(128 * 1024 * 1024, 8, 100);
  ASSERT_EQ(options.compaction_style, kCompactionStyleLevel);

  // Tight write amplification
  options = PrintAndGetOptions(128 * 1024 * 1024, 64, 10);
  ASSERT_EQ(options.compaction_style, kCompactionStyleUniversal);

  // Both tight amplifications
  PrintAndGetOptions(128 * 1024 * 1024, 4, 8);
}

TEST(OptionsTest, GetOptionsFromMapTest) {
  std::unordered_map<std::string, std::string> cf_options_map = {
    {"write_buffer_size", "1"},
    {"max_write_buffer_number", "2"},
    {"min_write_buffer_number_to_merge", "3"},
    {"compression", "kSnappyCompression"},
    {"compression_per_level", "kNoCompression:"
                              "kSnappyCompression:"
                              "kZlibCompression:"
                              "kBZip2Compression:"
                              "kLZ4Compression:"
                              "kLZ4HCCompression"},
    {"compression_opts", "4:5:6"},
    {"num_levels", "7"},
    {"level0_file_num_compaction_trigger", "8"},
    {"level0_slowdown_writes_trigger", "9"},
    {"level0_stop_writes_trigger", "10"},
    {"max_mem_compaction_level", "11"},
    {"target_file_size_base", "12"},
    {"target_file_size_multiplier", "13"},
    {"max_bytes_for_level_base", "14"},
    {"max_bytes_for_level_multiplier", "15"},
    {"max_bytes_for_level_multiplier_additional", "16:17:18"},
    {"expanded_compaction_factor", "19"},
    {"source_compaction_factor", "20"},
    {"max_grandparent_overlap_factor", "21"},
    {"soft_rate_limit", "1.1"},
    {"hard_rate_limit", "2.1"},
    {"arena_block_size", "22"},
    {"disable_auto_compactions", "true"},
    {"purge_redundant_kvs_while_flush", "1"},
    {"compaction_style", "kCompactionStyleLevel"},
    {"verify_checksums_in_compaction", "false"},
    {"compaction_options_fifo", "23"},
    {"filter_deletes", "0"},
    {"max_sequential_skip_in_iterations", "24"},
    {"inplace_update_support", "true"},
    {"inplace_update_num_locks", "25"},
    {"memtable_prefix_bloom_bits", "26"},
    {"memtable_prefix_bloom_probes", "27"},
    {"memtable_prefix_bloom_huge_page_tlb_size", "28"},
    {"bloom_locality", "29"},
    {"max_successive_merges", "30"},
    {"min_partial_merge_operands", "31"}
  };

  std::unordered_map<std::string, std::string> db_options_map = {
    {"create_if_missing", "false"},
    {"create_missing_column_families", "true"},
    {"error_if_exists", "false"},
    {"paranoid_checks", "true"},
    {"max_open_files", "32"},
    {"max_total_wal_size", "33"},
    {"disable_data_sync", "false"},
    {"use_fsync", "true"},
    {"db_log_dir", "/db_log_dir"},
    {"wal_dir", "/wal_dir"},
    {"delete_obsolete_files_period_micros", "34"},
    {"max_background_compactions", "35"},
    {"max_background_flushes", "36"},
    {"max_log_file_size", "37"},
    {"log_file_time_to_roll", "38"},
    {"keep_log_file_num", "39"},
    {"max_manifest_file_size", "40"},
    {"table_cache_numshardbits", "41"},
    {"table_cache_remove_scan_count_limit", "42"},
    {"WAL_ttl_seconds", "43"},
    {"WAL_size_limit_MB", "44"},
    {"manifest_preallocation_size", "45"},
    {"allow_os_buffer", "false"},
    {"allow_mmap_reads", "true"},
    {"allow_mmap_writes", "false"},
    {"is_fd_close_on_exec", "true"},
    {"skip_log_error_on_recovery", "false"},
    {"stats_dump_period_sec", "46"},
    {"advise_random_on_open", "true"},
    {"use_adaptive_mutex", "false"},
    {"bytes_per_sync", "47"},
  };

  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  ASSERT_TRUE(GetColumnFamilyOptionsFromMap(
              base_cf_opt, cf_options_map, &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 1U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 2);
  ASSERT_EQ(new_cf_opt.min_write_buffer_number_to_merge, 3);
  ASSERT_EQ(new_cf_opt.compression, kSnappyCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level.size(), 6U);
  ASSERT_EQ(new_cf_opt.compression_per_level[0], kNoCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[1], kSnappyCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[2], kZlibCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[3], kBZip2Compression);
  ASSERT_EQ(new_cf_opt.compression_per_level[4], kLZ4Compression);
  ASSERT_EQ(new_cf_opt.compression_per_level[5], kLZ4HCCompression);
  ASSERT_EQ(new_cf_opt.compression_opts.window_bits, 4);
  ASSERT_EQ(new_cf_opt.compression_opts.level, 5);
  ASSERT_EQ(new_cf_opt.compression_opts.strategy, 6);
  ASSERT_EQ(new_cf_opt.num_levels, 7);
  ASSERT_EQ(new_cf_opt.level0_file_num_compaction_trigger, 8);
  ASSERT_EQ(new_cf_opt.level0_slowdown_writes_trigger, 9);
  ASSERT_EQ(new_cf_opt.level0_stop_writes_trigger, 10);
  ASSERT_EQ(new_cf_opt.max_mem_compaction_level, 11);
  ASSERT_EQ(new_cf_opt.target_file_size_base, static_cast<uint64_t>(12));
  ASSERT_EQ(new_cf_opt.target_file_size_multiplier, 13);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_base, 14U);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier, 15);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional.size(), 3U);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[0], 16);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[1], 17);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[2], 18);
  ASSERT_EQ(new_cf_opt.expanded_compaction_factor, 19);
  ASSERT_EQ(new_cf_opt.source_compaction_factor, 20);
  ASSERT_EQ(new_cf_opt.max_grandparent_overlap_factor, 21);
  ASSERT_EQ(new_cf_opt.soft_rate_limit, 1.1);
  ASSERT_EQ(new_cf_opt.hard_rate_limit, 2.1);
  ASSERT_EQ(new_cf_opt.arena_block_size, 22U);
  ASSERT_EQ(new_cf_opt.disable_auto_compactions, true);
  ASSERT_EQ(new_cf_opt.purge_redundant_kvs_while_flush, true);
  ASSERT_EQ(new_cf_opt.compaction_style, kCompactionStyleLevel);
  ASSERT_EQ(new_cf_opt.verify_checksums_in_compaction, false);
  ASSERT_EQ(new_cf_opt.compaction_options_fifo.max_table_files_size,
            static_cast<uint64_t>(23));
  ASSERT_EQ(new_cf_opt.filter_deletes, false);
  ASSERT_EQ(new_cf_opt.max_sequential_skip_in_iterations,
            static_cast<uint64_t>(24));
  ASSERT_EQ(new_cf_opt.inplace_update_support, true);
  ASSERT_EQ(new_cf_opt.inplace_update_num_locks, 25U);
  ASSERT_EQ(new_cf_opt.memtable_prefix_bloom_bits, 26U);
  ASSERT_EQ(new_cf_opt.memtable_prefix_bloom_probes, 27U);
  ASSERT_EQ(new_cf_opt.memtable_prefix_bloom_huge_page_tlb_size, 28U);
  ASSERT_EQ(new_cf_opt.bloom_locality, 29U);
  ASSERT_EQ(new_cf_opt.max_successive_merges, 30U);
  ASSERT_EQ(new_cf_opt.min_partial_merge_operands, 31U);

  cf_options_map["write_buffer_size"] = "hello";
  ASSERT_TRUE(!GetColumnFamilyOptionsFromMap(
              base_cf_opt, cf_options_map, &new_cf_opt));
  cf_options_map["write_buffer_size"] = "1";
  ASSERT_TRUE(GetColumnFamilyOptionsFromMap(
              base_cf_opt, cf_options_map, &new_cf_opt));
  cf_options_map["unknown_option"] = "1";
  ASSERT_TRUE(!GetColumnFamilyOptionsFromMap(
              base_cf_opt, cf_options_map, &new_cf_opt));

  DBOptions base_db_opt;
  DBOptions new_db_opt;
  ASSERT_TRUE(GetDBOptionsFromMap(base_db_opt, db_options_map, &new_db_opt));
  ASSERT_EQ(new_db_opt.create_if_missing, false);
  ASSERT_EQ(new_db_opt.create_missing_column_families, true);
  ASSERT_EQ(new_db_opt.error_if_exists, false);
  ASSERT_EQ(new_db_opt.paranoid_checks, true);
  ASSERT_EQ(new_db_opt.max_open_files, 32);
  ASSERT_EQ(new_db_opt.max_total_wal_size, static_cast<uint64_t>(33));
  ASSERT_EQ(new_db_opt.disableDataSync, false);
  ASSERT_EQ(new_db_opt.use_fsync, true);
  ASSERT_EQ(new_db_opt.db_log_dir, "/db_log_dir");
  ASSERT_EQ(new_db_opt.wal_dir, "/wal_dir");
  ASSERT_EQ(new_db_opt.delete_obsolete_files_period_micros,
            static_cast<uint64_t>(34));
  ASSERT_EQ(new_db_opt.max_background_compactions, 35);
  ASSERT_EQ(new_db_opt.max_background_flushes, 36);
  ASSERT_EQ(new_db_opt.max_log_file_size, 37U);
  ASSERT_EQ(new_db_opt.log_file_time_to_roll, 38U);
  ASSERT_EQ(new_db_opt.keep_log_file_num, 39U);
  ASSERT_EQ(new_db_opt.max_manifest_file_size, static_cast<uint64_t>(40));
  ASSERT_EQ(new_db_opt.table_cache_numshardbits, 41);
  ASSERT_EQ(new_db_opt.table_cache_remove_scan_count_limit, 42);
  ASSERT_EQ(new_db_opt.WAL_ttl_seconds, static_cast<uint64_t>(43));
  ASSERT_EQ(new_db_opt.WAL_size_limit_MB, static_cast<uint64_t>(44));
  ASSERT_EQ(new_db_opt.manifest_preallocation_size, 45U);
  ASSERT_EQ(new_db_opt.allow_os_buffer, false);
  ASSERT_EQ(new_db_opt.allow_mmap_reads, true);
  ASSERT_EQ(new_db_opt.allow_mmap_writes, false);
  ASSERT_EQ(new_db_opt.is_fd_close_on_exec, true);
  ASSERT_EQ(new_db_opt.skip_log_error_on_recovery, false);
  ASSERT_EQ(new_db_opt.stats_dump_period_sec, 46U);
  ASSERT_EQ(new_db_opt.advise_random_on_open, true);
  ASSERT_EQ(new_db_opt.use_adaptive_mutex, false);
  ASSERT_EQ(new_db_opt.bytes_per_sync, static_cast<uint64_t>(47));
}

TEST(OptionsTest, GetOptionsFromStringTest) {
  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  ASSERT_TRUE(GetColumnFamilyOptionsFromString(base_cf_opt, "", &new_cf_opt));
  ASSERT_TRUE(GetColumnFamilyOptionsFromString(base_cf_opt,
              "write_buffer_size=5", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 5U);
  ASSERT_TRUE(GetColumnFamilyOptionsFromString(base_cf_opt,
              "write_buffer_size=6;", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 6U);
  ASSERT_TRUE(GetColumnFamilyOptionsFromString(base_cf_opt,
              "  write_buffer_size =  7  ", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 7U);
  ASSERT_TRUE(GetColumnFamilyOptionsFromString(base_cf_opt,
              "  write_buffer_size =  8 ; ", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 8U);
  ASSERT_TRUE(GetColumnFamilyOptionsFromString(base_cf_opt,
              "write_buffer_size=9;max_write_buffer_number=10", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 9U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 10);
  ASSERT_TRUE(GetColumnFamilyOptionsFromString(base_cf_opt,
              "write_buffer_size=11; max_write_buffer_number  =  12 ;",
              &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 11U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 12);
  // Wrong name "max_write_buffer_number_"
  ASSERT_TRUE(!GetColumnFamilyOptionsFromString(base_cf_opt,
              "write_buffer_size=13;max_write_buffer_number_=14;",
              &new_cf_opt));
  // Wrong key/value pair
  ASSERT_TRUE(!GetColumnFamilyOptionsFromString(base_cf_opt,
              "write_buffer_size=13;max_write_buffer_number;", &new_cf_opt));
  // Error Paring value
  ASSERT_TRUE(!GetColumnFamilyOptionsFromString(base_cf_opt,
              "write_buffer_size=13;max_write_buffer_number=;", &new_cf_opt));
  // Missing option name
  ASSERT_TRUE(!GetColumnFamilyOptionsFromString(base_cf_opt,
              "write_buffer_size=13; =100;", &new_cf_opt));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);
  return rocksdb::test::RunAllTests();
}
