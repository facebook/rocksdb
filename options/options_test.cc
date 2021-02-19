//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cctype>
#include <cinttypes>
#include <cstring>
#include <unordered_map>

#include "cache/lru_cache.h"
#include "cache/sharded_cache.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/leveldb_options.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "table/block_based/filter_policy_internal.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"
#include "utilities/merge_operators/bytesxor.h"

#ifndef GFLAGS
bool FLAGS_enable_print = false;
#else
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
DEFINE_bool(enable_print, false, "Print options generated to console.");
#endif  // GFLAGS

namespace ROCKSDB_NAMESPACE {

class OptionsTest : public testing::Test {};

#ifndef ROCKSDB_LITE  // GetOptionsFromMap is not supported in ROCKSDB_LITE
TEST_F(OptionsTest, GetOptionsFromMapTest) {
  std::unordered_map<std::string, std::string> cf_options_map = {
      {"write_buffer_size", "1"},
      {"max_write_buffer_number", "2"},
      {"min_write_buffer_number_to_merge", "3"},
      {"max_write_buffer_number_to_maintain", "99"},
      {"max_write_buffer_size_to_maintain", "-99999"},
      {"compression", "kSnappyCompression"},
      {"compression_per_level",
       "kNoCompression:"
       "kSnappyCompression:"
       "kZlibCompression:"
       "kBZip2Compression:"
       "kLZ4Compression:"
       "kLZ4HCCompression:"
       "kXpressCompression:"
       "kZSTD:"
       "kZSTDNotFinalCompression"},
      {"bottommost_compression", "kLZ4Compression"},
      {"bottommost_compression_opts", "5:6:7:8:10:true"},
      {"compression_opts", "4:5:6:7:8:true"},
      {"num_levels", "8"},
      {"level0_file_num_compaction_trigger", "8"},
      {"level0_slowdown_writes_trigger", "9"},
      {"level0_stop_writes_trigger", "10"},
      {"target_file_size_base", "12"},
      {"target_file_size_multiplier", "13"},
      {"max_bytes_for_level_base", "14"},
      {"level_compaction_dynamic_level_bytes", "true"},
      {"max_bytes_for_level_multiplier", "15.0"},
      {"max_bytes_for_level_multiplier_additional", "16:17:18"},
      {"max_compaction_bytes", "21"},
      {"soft_rate_limit", "1.1"},
      {"hard_rate_limit", "2.1"},
      {"hard_pending_compaction_bytes_limit", "211"},
      {"arena_block_size", "22"},
      {"disable_auto_compactions", "true"},
      {"compaction_style", "kCompactionStyleLevel"},
      {"compaction_pri", "kOldestSmallestSeqFirst"},
      {"verify_checksums_in_compaction", "false"},
      {"compaction_options_fifo", "23"},
      {"max_sequential_skip_in_iterations", "24"},
      {"inplace_update_support", "true"},
      {"report_bg_io_stats", "true"},
      {"compaction_measure_io_stats", "false"},
      {"inplace_update_num_locks", "25"},
      {"memtable_prefix_bloom_size_ratio", "0.26"},
      {"memtable_whole_key_filtering", "true"},
      {"memtable_huge_page_size", "28"},
      {"bloom_locality", "29"},
      {"max_successive_merges", "30"},
      {"min_partial_merge_operands", "31"},
      {"prefix_extractor", "fixed:31"},
      {"optimize_filters_for_hits", "true"},
      {"enable_blob_files", "true"},
      {"min_blob_size", "1K"},
      {"blob_file_size", "1G"},
      {"blob_compression_type", "kZSTD"},
      {"enable_blob_garbage_collection", "true"},
      {"blob_garbage_collection_age_cutoff", "0.5"},
  };

  std::unordered_map<std::string, std::string> db_options_map = {
      {"create_if_missing", "false"},
      {"create_missing_column_families", "true"},
      {"error_if_exists", "false"},
      {"paranoid_checks", "true"},
      {"track_and_verify_wals_in_manifest", "true"},
      {"max_open_files", "32"},
      {"max_total_wal_size", "33"},
      {"use_fsync", "true"},
      {"db_log_dir", "/db_log_dir"},
      {"wal_dir", "/wal_dir"},
      {"delete_obsolete_files_period_micros", "34"},
      {"max_background_compactions", "35"},
      {"max_background_flushes", "36"},
      {"max_log_file_size", "37"},
      {"log_file_time_to_roll", "38"},
      {"keep_log_file_num", "39"},
      {"recycle_log_file_num", "5"},
      {"max_manifest_file_size", "40"},
      {"table_cache_numshardbits", "41"},
      {"WAL_ttl_seconds", "43"},
      {"WAL_size_limit_MB", "44"},
      {"manifest_preallocation_size", "45"},
      {"allow_mmap_reads", "true"},
      {"allow_mmap_writes", "false"},
      {"use_direct_reads", "false"},
      {"use_direct_io_for_flush_and_compaction", "false"},
      {"is_fd_close_on_exec", "true"},
      {"skip_log_error_on_recovery", "false"},
      {"stats_dump_period_sec", "46"},
      {"stats_persist_period_sec", "57"},
      {"persist_stats_to_disk", "false"},
      {"stats_history_buffer_size", "69"},
      {"advise_random_on_open", "true"},
      {"use_adaptive_mutex", "false"},
      {"new_table_reader_for_compaction_inputs", "true"},
      {"compaction_readahead_size", "100"},
      {"random_access_max_buffer_size", "3145728"},
      {"writable_file_max_buffer_size", "314159"},
      {"bytes_per_sync", "47"},
      {"wal_bytes_per_sync", "48"},
      {"strict_bytes_per_sync", "true"},
  };

  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  ConfigOptions exact, loose;
  exact.input_strings_escaped = false;
  exact.ignore_unknown_options = false;
  exact.sanity_level = ConfigOptions::kSanityLevelExactMatch;
  loose.sanity_level = ConfigOptions::kSanityLevelLooselyCompatible;

  loose.input_strings_escaped = false;
  loose.ignore_unknown_options = true;
  ASSERT_OK(GetColumnFamilyOptionsFromMap(exact, base_cf_opt, cf_options_map,
                                          &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 1U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 2);
  ASSERT_EQ(new_cf_opt.min_write_buffer_number_to_merge, 3);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number_to_maintain, 99);
  ASSERT_EQ(new_cf_opt.max_write_buffer_size_to_maintain, -99999);
  ASSERT_EQ(new_cf_opt.compression, kSnappyCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level.size(), 9U);
  ASSERT_EQ(new_cf_opt.compression_per_level[0], kNoCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[1], kSnappyCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[2], kZlibCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[3], kBZip2Compression);
  ASSERT_EQ(new_cf_opt.compression_per_level[4], kLZ4Compression);
  ASSERT_EQ(new_cf_opt.compression_per_level[5], kLZ4HCCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[6], kXpressCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[7], kZSTD);
  ASSERT_EQ(new_cf_opt.compression_per_level[8], kZSTDNotFinalCompression);
  ASSERT_EQ(new_cf_opt.compression_opts.window_bits, 4);
  ASSERT_EQ(new_cf_opt.compression_opts.level, 5);
  ASSERT_EQ(new_cf_opt.compression_opts.strategy, 6);
  ASSERT_EQ(new_cf_opt.compression_opts.max_dict_bytes, 7u);
  ASSERT_EQ(new_cf_opt.compression_opts.zstd_max_train_bytes, 8u);
  ASSERT_EQ(new_cf_opt.compression_opts.parallel_threads,
            CompressionOptions().parallel_threads);
  ASSERT_EQ(new_cf_opt.compression_opts.enabled, true);
  ASSERT_EQ(new_cf_opt.bottommost_compression, kLZ4Compression);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.window_bits, 5);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.level, 6);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.strategy, 7);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.max_dict_bytes, 8u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.zstd_max_train_bytes, 10u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.parallel_threads,
            CompressionOptions().parallel_threads);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.enabled, true);
  ASSERT_EQ(new_cf_opt.num_levels, 8);
  ASSERT_EQ(new_cf_opt.level0_file_num_compaction_trigger, 8);
  ASSERT_EQ(new_cf_opt.level0_slowdown_writes_trigger, 9);
  ASSERT_EQ(new_cf_opt.level0_stop_writes_trigger, 10);
  ASSERT_EQ(new_cf_opt.target_file_size_base, static_cast<uint64_t>(12));
  ASSERT_EQ(new_cf_opt.target_file_size_multiplier, 13);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_base, 14U);
  ASSERT_EQ(new_cf_opt.level_compaction_dynamic_level_bytes, true);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier, 15.0);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional.size(), 3U);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[0], 16);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[1], 17);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[2], 18);
  ASSERT_EQ(new_cf_opt.max_compaction_bytes, 21);
  ASSERT_EQ(new_cf_opt.hard_pending_compaction_bytes_limit, 211);
  ASSERT_EQ(new_cf_opt.arena_block_size, 22U);
  ASSERT_EQ(new_cf_opt.disable_auto_compactions, true);
  ASSERT_EQ(new_cf_opt.compaction_style, kCompactionStyleLevel);
  ASSERT_EQ(new_cf_opt.compaction_pri, kOldestSmallestSeqFirst);
  ASSERT_EQ(new_cf_opt.compaction_options_fifo.max_table_files_size,
            static_cast<uint64_t>(23));
  ASSERT_EQ(new_cf_opt.max_sequential_skip_in_iterations,
            static_cast<uint64_t>(24));
  ASSERT_EQ(new_cf_opt.inplace_update_support, true);
  ASSERT_EQ(new_cf_opt.inplace_update_num_locks, 25U);
  ASSERT_EQ(new_cf_opt.memtable_prefix_bloom_size_ratio, 0.26);
  ASSERT_EQ(new_cf_opt.memtable_whole_key_filtering, true);
  ASSERT_EQ(new_cf_opt.memtable_huge_page_size, 28U);
  ASSERT_EQ(new_cf_opt.bloom_locality, 29U);
  ASSERT_EQ(new_cf_opt.max_successive_merges, 30U);
  ASSERT_TRUE(new_cf_opt.prefix_extractor != nullptr);
  ASSERT_EQ(new_cf_opt.optimize_filters_for_hits, true);
  ASSERT_EQ(std::string(new_cf_opt.prefix_extractor->Name()),
            "rocksdb.FixedPrefix.31");
  ASSERT_EQ(new_cf_opt.enable_blob_files, true);
  ASSERT_EQ(new_cf_opt.min_blob_size, 1ULL << 10);
  ASSERT_EQ(new_cf_opt.blob_file_size, 1ULL << 30);
  ASSERT_EQ(new_cf_opt.blob_compression_type, kZSTD);
  ASSERT_EQ(new_cf_opt.enable_blob_garbage_collection, true);
  ASSERT_EQ(new_cf_opt.blob_garbage_collection_age_cutoff, 0.5);

  cf_options_map["write_buffer_size"] = "hello";
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(exact, base_cf_opt, cf_options_map,
                                           &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  cf_options_map["write_buffer_size"] = "1";
  ASSERT_OK(GetColumnFamilyOptionsFromMap(exact, base_cf_opt, cf_options_map,
                                          &new_cf_opt));

  cf_options_map["unknown_option"] = "1";
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(exact, base_cf_opt, cf_options_map,
                                           &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // ignore_unknown_options=true;input_strings_escaped=false
  ASSERT_OK(GetColumnFamilyOptionsFromMap(loose, base_cf_opt, cf_options_map,
                                          &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(loose, base_cf_opt, new_cf_opt));
  ASSERT_NOK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  DBOptions base_db_opt;
  DBOptions new_db_opt;
  ASSERT_OK(
      GetDBOptionsFromMap(exact, base_db_opt, db_options_map, &new_db_opt));
  ASSERT_EQ(new_db_opt.create_if_missing, false);
  ASSERT_EQ(new_db_opt.create_missing_column_families, true);
  ASSERT_EQ(new_db_opt.error_if_exists, false);
  ASSERT_EQ(new_db_opt.paranoid_checks, true);
  ASSERT_EQ(new_db_opt.track_and_verify_wals_in_manifest, true);
  ASSERT_EQ(new_db_opt.max_open_files, 32);
  ASSERT_EQ(new_db_opt.max_total_wal_size, static_cast<uint64_t>(33));
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
  ASSERT_EQ(new_db_opt.recycle_log_file_num, 5U);
  ASSERT_EQ(new_db_opt.max_manifest_file_size, static_cast<uint64_t>(40));
  ASSERT_EQ(new_db_opt.table_cache_numshardbits, 41);
  ASSERT_EQ(new_db_opt.WAL_ttl_seconds, static_cast<uint64_t>(43));
  ASSERT_EQ(new_db_opt.WAL_size_limit_MB, static_cast<uint64_t>(44));
  ASSERT_EQ(new_db_opt.manifest_preallocation_size, 45U);
  ASSERT_EQ(new_db_opt.allow_mmap_reads, true);
  ASSERT_EQ(new_db_opt.allow_mmap_writes, false);
  ASSERT_EQ(new_db_opt.use_direct_reads, false);
  ASSERT_EQ(new_db_opt.use_direct_io_for_flush_and_compaction, false);
  ASSERT_EQ(new_db_opt.is_fd_close_on_exec, true);
  ASSERT_EQ(new_db_opt.skip_log_error_on_recovery, false);
  ASSERT_EQ(new_db_opt.stats_dump_period_sec, 46U);
  ASSERT_EQ(new_db_opt.stats_persist_period_sec, 57U);
  ASSERT_EQ(new_db_opt.persist_stats_to_disk, false);
  ASSERT_EQ(new_db_opt.stats_history_buffer_size, 69U);
  ASSERT_EQ(new_db_opt.advise_random_on_open, true);
  ASSERT_EQ(new_db_opt.use_adaptive_mutex, false);
  ASSERT_EQ(new_db_opt.new_table_reader_for_compaction_inputs, true);
  ASSERT_EQ(new_db_opt.compaction_readahead_size, 100);
  ASSERT_EQ(new_db_opt.random_access_max_buffer_size, 3145728);
  ASSERT_EQ(new_db_opt.writable_file_max_buffer_size, 314159);
  ASSERT_EQ(new_db_opt.bytes_per_sync, static_cast<uint64_t>(47));
  ASSERT_EQ(new_db_opt.wal_bytes_per_sync, static_cast<uint64_t>(48));
  ASSERT_EQ(new_db_opt.strict_bytes_per_sync, true);

  db_options_map["max_open_files"] = "hello";
  Status s =
      GetDBOptionsFromMap(exact, base_db_opt, db_options_map, &new_db_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(loose, base_db_opt, new_db_opt));

  // unknow options should fail parsing without ignore_unknown_options = true
  db_options_map["unknown_db_option"] = "1";
  s = GetDBOptionsFromMap(exact, base_db_opt, db_options_map, &new_db_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));

  ASSERT_OK(
      GetDBOptionsFromMap(loose, base_db_opt, db_options_map, &new_db_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(loose, base_db_opt, new_db_opt));
  ASSERT_NOK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));
}
#endif  // !ROCKSDB_LITE

#ifndef ROCKSDB_LITE  // GetColumnFamilyOptionsFromString is not supported in
                      // ROCKSDB_LITE
TEST_F(OptionsTest, GetColumnFamilyOptionsFromStringTest) {
  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;

  base_cf_opt.table_factory.reset();
  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt, "",
                                             &new_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "write_buffer_size=5", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 5U);
  ASSERT_TRUE(new_cf_opt.table_factory == nullptr);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "write_buffer_size=6;", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 6U);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "  write_buffer_size =  7  ", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 7U);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "  write_buffer_size =  8 ; ", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 8U);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=9;max_write_buffer_number=10", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 9U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 10);
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=11; max_write_buffer_number  =  12 ;", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 11U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 12);
  // Wrong name "max_write_buffer_number_"
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=13;max_write_buffer_number_=14;", &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  // Comparator from object registry
  std::string kCompName = "reverse_comp";
  ObjectLibrary::Default()->Register<const Comparator>(
      kCompName,
      [](const std::string& /*name*/,
         std::unique_ptr<const Comparator>* /*guard*/,
         std::string* /* errmsg */) { return ReverseBytewiseComparator(); });

  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                             "comparator=" + kCompName + ";",
                                             &new_cf_opt));
  ASSERT_EQ(new_cf_opt.comparator, ReverseBytewiseComparator());

  // MergeOperator from object registry
  std::unique_ptr<BytesXOROperator> bxo(new BytesXOROperator());
  std::string kMoName = bxo->Name();
  ObjectLibrary::Default()->Register<MergeOperator>(
      kMoName,
      [](const std::string& /*name*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /* errmsg */) {
        guard->reset(new BytesXOROperator());
        return guard->get();
      });

  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                             "merge_operator=" + kMoName + ";",
                                             &new_cf_opt));
  ASSERT_EQ(kMoName, std::string(new_cf_opt.merge_operator->Name()));

  // Wrong key/value pair
  Status s = GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=13;max_write_buffer_number;", &new_cf_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  // Error Parsing value
  s = GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=13;max_write_buffer_number=;", &new_cf_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  // Missing option name
  s = GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "write_buffer_size=13; =100;", &new_cf_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  const uint64_t kilo = 1024UL;
  const uint64_t mega = 1024 * kilo;
  const uint64_t giga = 1024 * mega;
  const uint64_t tera = 1024 * giga;

  // Units (k)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "max_write_buffer_number=15K", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 15 * kilo);
  // Units (m)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "max_write_buffer_number=16m;inplace_update_num_locks=17M", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 16 * mega);
  ASSERT_EQ(new_cf_opt.inplace_update_num_locks, 17u * mega);
  // Units (g)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=18g;prefix_extractor=capped:8;"
      "arena_block_size=19G",
      &new_cf_opt));

  ASSERT_EQ(new_cf_opt.write_buffer_size, 18 * giga);
  ASSERT_EQ(new_cf_opt.arena_block_size, 19 * giga);
  ASSERT_TRUE(new_cf_opt.prefix_extractor.get() != nullptr);
  std::string prefix_name(new_cf_opt.prefix_extractor->Name());
  ASSERT_EQ(prefix_name, "rocksdb.CappedPrefix.8");

  // Units (t)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt, "write_buffer_size=20t;arena_block_size=21T",
      &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 20 * tera);
  ASSERT_EQ(new_cf_opt.arena_block_size, 21 * tera);

  // Nested block based table options
  // Empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={};arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  // Non-empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_cache=1M;block_size=4;};"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  // Last one
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_cache=1M;block_size=4;}",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  // Mismatch curly braces
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={{{block_size=4;};"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  // Unexpected chars after closing curly brace
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_size=4;}};"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_size=4;}xdfa;"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_size=4;}xdfa",
      &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  // Invalid block based table option
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={xx_block_size=4;}",
      &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                             "optimize_filters_for_hits=true",
                                             &new_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                             "optimize_filters_for_hits=false",
                                             &new_cf_opt));

  ASSERT_NOK(GetColumnFamilyOptionsFromString(config_options, base_cf_opt,
                                              "optimize_filters_for_hits=junk",
                                              &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opt,
                                                  new_cf_opt));

  // Nested plain table options
  // Empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "plain_table_factory={};arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  ASSERT_EQ(std::string(new_cf_opt.table_factory->Name()), "PlainTable");
  // Non-empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "plain_table_factory={user_key_len=66;bloom_bits_per_key=20;};"
      "arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  ASSERT_EQ(std::string(new_cf_opt.table_factory->Name()), "PlainTable");

  // memtable factory
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "memtable=skip_list:10;arena_block_size=1024",
      &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.memtable_factory != nullptr);
  ASSERT_EQ(std::string(new_cf_opt.memtable_factory->Name()), "SkipListFactory");
}

TEST_F(OptionsTest, CompressionOptionsFromString) {
  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  ConfigOptions config_options;
  std::string opts_str;
  config_options.ignore_unknown_options = false;
  CompressionOptions dflt;
  // Test with some optional values removed....
  ASSERT_OK(
      GetColumnFamilyOptionsFromString(config_options, ColumnFamilyOptions(),
                                       "compression_opts=3:4:5; "
                                       "bottommost_compression_opts=4:5:6:7",
                                       &base_cf_opt));
  ASSERT_EQ(base_cf_opt.compression_opts.window_bits, 3);
  ASSERT_EQ(base_cf_opt.compression_opts.level, 4);
  ASSERT_EQ(base_cf_opt.compression_opts.strategy, 5);
  ASSERT_EQ(base_cf_opt.compression_opts.max_dict_bytes, dflt.max_dict_bytes);
  ASSERT_EQ(base_cf_opt.compression_opts.zstd_max_train_bytes,
            dflt.zstd_max_train_bytes);
  ASSERT_EQ(base_cf_opt.compression_opts.parallel_threads,
            dflt.parallel_threads);
  ASSERT_EQ(base_cf_opt.compression_opts.enabled, dflt.enabled);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.window_bits, 4);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.level, 5);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.strategy, 6);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.max_dict_bytes, 7u);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.zstd_max_train_bytes,
            dflt.zstd_max_train_bytes);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.parallel_threads,
            dflt.parallel_threads);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.enabled, dflt.enabled);

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(),
      "compression_opts=4:5:6:7:8:9:true; "
      "bottommost_compression_opts=5:6:7:8:9:false",
      &base_cf_opt));
  ASSERT_EQ(base_cf_opt.compression_opts.window_bits, 4);
  ASSERT_EQ(base_cf_opt.compression_opts.level, 5);
  ASSERT_EQ(base_cf_opt.compression_opts.strategy, 6);
  ASSERT_EQ(base_cf_opt.compression_opts.max_dict_bytes, 7u);
  ASSERT_EQ(base_cf_opt.compression_opts.zstd_max_train_bytes, 8u);
  ASSERT_EQ(base_cf_opt.compression_opts.parallel_threads, 9u);
  ASSERT_EQ(base_cf_opt.compression_opts.enabled, true);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.window_bits, 5);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.level, 6);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.strategy, 7);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.max_dict_bytes, 8u);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.zstd_max_train_bytes, 9u);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.parallel_threads,
            dflt.parallel_threads);
  ASSERT_EQ(base_cf_opt.bottommost_compression_opts.enabled, false);

  ASSERT_OK(
      GetStringFromColumnFamilyOptions(config_options, base_cf_opt, &opts_str));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(), opts_str, &new_cf_opt));
  ASSERT_EQ(new_cf_opt.compression_opts.window_bits, 4);
  ASSERT_EQ(new_cf_opt.compression_opts.level, 5);
  ASSERT_EQ(new_cf_opt.compression_opts.strategy, 6);
  ASSERT_EQ(new_cf_opt.compression_opts.max_dict_bytes, 7u);
  ASSERT_EQ(new_cf_opt.compression_opts.zstd_max_train_bytes, 8u);
  ASSERT_EQ(new_cf_opt.compression_opts.parallel_threads, 9u);
  ASSERT_EQ(new_cf_opt.compression_opts.enabled, true);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.window_bits, 5);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.level, 6);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.strategy, 7);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.max_dict_bytes, 8u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.zstd_max_train_bytes, 9u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.parallel_threads,
            dflt.parallel_threads);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.enabled, false);

  // Test as struct values
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(),
      "compression_opts={window_bits=5; level=6; strategy=7; max_dict_bytes=8;"
      "zstd_max_train_bytes=9;parallel_threads=10;enabled=true}; "
      "bottommost_compression_opts={window_bits=4; level=5; strategy=6;"
      " max_dict_bytes=7;zstd_max_train_bytes=8;parallel_threads=9;"
      "enabled=false}; ",
      &new_cf_opt));
  ASSERT_EQ(new_cf_opt.compression_opts.window_bits, 5);
  ASSERT_EQ(new_cf_opt.compression_opts.level, 6);
  ASSERT_EQ(new_cf_opt.compression_opts.strategy, 7);
  ASSERT_EQ(new_cf_opt.compression_opts.max_dict_bytes, 8u);
  ASSERT_EQ(new_cf_opt.compression_opts.zstd_max_train_bytes, 9u);
  ASSERT_EQ(new_cf_opt.compression_opts.parallel_threads, 10u);
  ASSERT_EQ(new_cf_opt.compression_opts.enabled, true);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.window_bits, 4);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.level, 5);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.strategy, 6);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.max_dict_bytes, 7u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.zstd_max_train_bytes, 8u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.parallel_threads, 9u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.enabled, false);

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "compression_opts={window_bits=4; strategy=5;};"
      "bottommost_compression_opts={level=6; strategy=7;}",
      &new_cf_opt));
  ASSERT_EQ(new_cf_opt.compression_opts.window_bits, 4);
  ASSERT_EQ(new_cf_opt.compression_opts.strategy, 5);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.level, 6);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.strategy, 7);

  ASSERT_EQ(new_cf_opt.compression_opts.level,
            base_cf_opt.compression_opts.level);
  ASSERT_EQ(new_cf_opt.compression_opts.max_dict_bytes,
            base_cf_opt.compression_opts.max_dict_bytes);
  ASSERT_EQ(new_cf_opt.compression_opts.zstd_max_train_bytes,
            base_cf_opt.compression_opts.zstd_max_train_bytes);
  ASSERT_EQ(new_cf_opt.compression_opts.parallel_threads,
            base_cf_opt.compression_opts.parallel_threads);
  ASSERT_EQ(new_cf_opt.compression_opts.enabled,
            base_cf_opt.compression_opts.enabled);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.window_bits,
            base_cf_opt.bottommost_compression_opts.window_bits);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.max_dict_bytes,
            base_cf_opt.bottommost_compression_opts.max_dict_bytes);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.zstd_max_train_bytes,
            base_cf_opt.bottommost_compression_opts.zstd_max_train_bytes);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.parallel_threads,
            base_cf_opt.bottommost_compression_opts.parallel_threads);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.enabled,
            base_cf_opt.bottommost_compression_opts.enabled);

  // Test a few individual struct values
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, base_cf_opt,
      "compression_opts.enabled=false; "
      "bottommost_compression_opts.enabled=true; ",
      &new_cf_opt));
  ASSERT_EQ(new_cf_opt.compression_opts.enabled, false);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.enabled, true);

  // Now test some illegal values
  ConfigOptions ignore;
  ignore.ignore_unknown_options = true;
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(),
      "compression_opts=5:6:7:8:9:x:false", &base_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      ignore, ColumnFamilyOptions(), "compression_opts=5:6:7:8:9:x:false",
      &base_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(),
      "compression_opts=1:2:3:4:5:6:true:8", &base_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      ignore, ColumnFamilyOptions(), "compression_opts=1:2:3:4:5:6:true:8",
      &base_cf_opt));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(),
      "compression_opts=1:2:3:4:5:6:true:8:9", &base_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      ignore, ColumnFamilyOptions(), "compression_opts=1:2:3:4:5:6:true:8:9",
      &base_cf_opt));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(), "compression_opts={unknown=bad;}",
      &base_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(ignore, ColumnFamilyOptions(),
                                             "compression_opts={unknown=bad;}",
                                             &base_cf_opt));
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, ColumnFamilyOptions(), "compression_opts.unknown=bad",
      &base_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(ignore, ColumnFamilyOptions(),
                                             "compression_opts.unknown=bad",
                                             &base_cf_opt));
}

TEST_F(OptionsTest, OldInterfaceTest) {
  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  ConfigOptions exact;

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      base_cf_opt,
      "write_buffer_size=18;prefix_extractor=capped:8;"
      "arena_block_size=19",
      &new_cf_opt));

  ASSERT_EQ(new_cf_opt.write_buffer_size, 18);
  ASSERT_EQ(new_cf_opt.arena_block_size, 19);
  ASSERT_TRUE(new_cf_opt.prefix_extractor.get() != nullptr);

  // And with a bad option
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      base_cf_opt,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={xx_block_size=4;}",
      &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  std::unordered_map<std::string, std::string> cf_options_map = {
      {"write_buffer_size", "1"},
      {"max_write_buffer_number", "2"},
      {"min_write_buffer_number_to_merge", "3"},
  };
  ASSERT_OK(
      GetColumnFamilyOptionsFromMap(base_cf_opt, cf_options_map, &new_cf_opt));
  cf_options_map["unknown_option"] = "1";
  ASSERT_NOK(
      GetColumnFamilyOptionsFromMap(base_cf_opt, cf_options_map, &new_cf_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromMap(base_cf_opt, cf_options_map,
                                          &new_cf_opt, true, true));

  DBOptions base_db_opt;
  DBOptions new_db_opt;
  std::unordered_map<std::string, std::string> db_options_map = {
      {"create_if_missing", "false"},
      {"create_missing_column_families", "true"},
      {"error_if_exists", "false"},
      {"paranoid_checks", "true"},
      {"track_and_verify_wals_in_manifest", "true"},
      {"max_open_files", "32"},
  };
  ASSERT_OK(GetDBOptionsFromMap(base_db_opt, db_options_map, &new_db_opt));
  ASSERT_EQ(new_db_opt.create_if_missing, false);
  ASSERT_EQ(new_db_opt.create_missing_column_families, true);
  ASSERT_EQ(new_db_opt.error_if_exists, false);
  ASSERT_EQ(new_db_opt.paranoid_checks, true);
  ASSERT_EQ(new_db_opt.track_and_verify_wals_in_manifest, true);
  ASSERT_EQ(new_db_opt.max_open_files, 32);
  db_options_map["unknown_option"] = "1";
  Status s = GetDBOptionsFromMap(base_db_opt, db_options_map, &new_db_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));
  ASSERT_OK(GetDBOptionsFromMap(base_db_opt, db_options_map, &new_db_opt, true,
                                true));
  ASSERT_OK(GetDBOptionsFromString(
      base_db_opt,
      "create_if_missing=false;error_if_exists=false;max_open_files=42;",
      &new_db_opt));
  ASSERT_EQ(new_db_opt.create_if_missing, false);
  ASSERT_EQ(new_db_opt.error_if_exists, false);
  ASSERT_EQ(new_db_opt.max_open_files, 42);
  s = GetDBOptionsFromString(
      base_db_opt,
      "create_if_missing=false;error_if_exists=false;max_open_files=42;"
      "unknown_option=1;",
      &new_db_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));
}

#endif  // !ROCKSDB_LITE

#ifndef ROCKSDB_LITE  // GetBlockBasedTableOptionsFromString is not supported
TEST_F(OptionsTest, GetBlockBasedTableOptionsFromString) {
  BlockBasedTableOptions table_opt;
  BlockBasedTableOptions new_opt;
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;

  // make sure default values are overwritten by something else
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "cache_index_and_filter_blocks=1;index_type=kHashSearch;"
      "checksum=kxxHash;hash_index_allow_collision=1;"
      "block_cache=1M;block_cache_compressed=1k;block_size=1024;"
      "block_size_deviation=8;block_restart_interval=4;"
      "format_version=5;whole_key_filtering=1;"
      "filter_policy=bloomfilter:4.567:false;"
      // A bug caused read_amp_bytes_per_bit to be a large integer in OPTIONS
      // file generated by 6.10 to 6.14. Though bug is fixed in these releases,
      // we need to handle the case of loading OPTIONS file generated before the
      // fix.
      "read_amp_bytes_per_bit=17179869185;",
      &new_opt));
  ASSERT_TRUE(new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(new_opt.index_type, BlockBasedTableOptions::kHashSearch);
  ASSERT_EQ(new_opt.checksum, ChecksumType::kxxHash);
  ASSERT_TRUE(new_opt.hash_index_allow_collision);
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 1024UL*1024UL);
  ASSERT_TRUE(new_opt.block_cache_compressed != nullptr);
  ASSERT_EQ(new_opt.block_cache_compressed->GetCapacity(), 1024UL);
  ASSERT_EQ(new_opt.block_size, 1024UL);
  ASSERT_EQ(new_opt.block_size_deviation, 8);
  ASSERT_EQ(new_opt.block_restart_interval, 4);
  ASSERT_EQ(new_opt.format_version, 5U);
  ASSERT_EQ(new_opt.whole_key_filtering, true);
  ASSERT_TRUE(new_opt.filter_policy != nullptr);
  const BloomFilterPolicy* bfp =
      dynamic_cast<const BloomFilterPolicy*>(new_opt.filter_policy.get());
  EXPECT_EQ(bfp->GetMillibitsPerKey(), 4567);
  EXPECT_EQ(bfp->GetWholeBitsPerKey(), 5);
  EXPECT_EQ(bfp->GetMode(), BloomFilterPolicy::kAutoBloom);
  // Verify that only the lower 32bits are stored in
  // new_opt.read_amp_bytes_per_bit.
  EXPECT_EQ(1U, new_opt.read_amp_bytes_per_bit);

  // unknown option
  Status s = GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "cache_index_and_filter_blocks=1;index_type=kBinarySearch;"
      "bad_option=1",
      &new_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_EQ(static_cast<bool>(table_opt.cache_index_and_filter_blocks),
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.index_type, new_opt.index_type);

  // unrecognized index type
  s = GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "cache_index_and_filter_blocks=1;index_type=kBinarySearchXX", &new_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.index_type, new_opt.index_type);

  // unrecognized checksum type
  ASSERT_NOK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "cache_index_and_filter_blocks=1;checksum=kxxHashXX", &new_opt));
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.index_type, new_opt.index_type);

  // unrecognized filter policy name
  s = GetBlockBasedTableOptionsFromString(config_options, table_opt,
                                          "cache_index_and_filter_blocks=1;"
                                          "filter_policy=bloomfilterxx:4:true",
                                          &new_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.filter_policy, new_opt.filter_policy);

  // unrecognized filter policy config
  s = GetBlockBasedTableOptionsFromString(config_options, table_opt,
                                          "cache_index_and_filter_blocks=1;"
                                          "filter_policy=bloomfilter:4",
                                          &new_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.filter_policy, new_opt.filter_policy);

  // Experimental Ribbon filter policy
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt, "filter_policy=experimental_ribbon:5.678;",
      &new_opt));
  ASSERT_TRUE(new_opt.filter_policy != nullptr);
  bfp = dynamic_cast<const BloomFilterPolicy*>(new_opt.filter_policy.get());
  EXPECT_EQ(bfp->GetMillibitsPerKey(), 5678);
  EXPECT_EQ(bfp->GetMode(), BloomFilterPolicy::kStandard128Ribbon);

  // Check block cache options are overwritten when specified
  // in new format as a struct.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "block_cache={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;high_pri_pool_ratio=0.5;};"
      "block_cache_compressed={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;high_pri_pool_ratio=0.5;}",
      &new_opt));
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 1024UL*1024UL);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache)->GetNumShardBits(), 4);
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), true);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(
                new_opt.block_cache)->GetHighPriPoolRatio(), 0.5);
  ASSERT_TRUE(new_opt.block_cache_compressed != nullptr);
  ASSERT_EQ(new_opt.block_cache_compressed->GetCapacity(), 1024UL*1024UL);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache_compressed)->GetNumShardBits(), 4);
  ASSERT_EQ(new_opt.block_cache_compressed->HasStrictCapacityLimit(), true);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(
                new_opt.block_cache_compressed)->GetHighPriPoolRatio(),
                0.5);

  // Set only block cache capacity. Check other values are
  // reset to default values.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "block_cache={capacity=2M};"
      "block_cache_compressed={capacity=2M}",
      &new_opt));
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 2*1024UL*1024UL);
  // Default values
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache)->GetNumShardBits(),
                GetDefaultCacheShardBits(new_opt.block_cache->GetCapacity()));
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), false);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache)
                ->GetHighPriPoolRatio(),
            0.5);
  ASSERT_TRUE(new_opt.block_cache_compressed != nullptr);
  ASSERT_EQ(new_opt.block_cache_compressed->GetCapacity(), 2*1024UL*1024UL);
  // Default values
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache_compressed)->GetNumShardBits(),
                GetDefaultCacheShardBits(
                    new_opt.block_cache_compressed->GetCapacity()));
  ASSERT_EQ(new_opt.block_cache_compressed->HasStrictCapacityLimit(), false);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache_compressed)
                ->GetHighPriPoolRatio(),
            0.5);

  // Set couple of block cache options.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "block_cache={num_shard_bits=5;high_pri_pool_ratio=0.5;};"
      "block_cache_compressed={num_shard_bits=5;"
      "high_pri_pool_ratio=0.0;}",
      &new_opt));
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 0);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache)->GetNumShardBits(), 5);
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), false);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(
                new_opt.block_cache)->GetHighPriPoolRatio(), 0.5);
  ASSERT_TRUE(new_opt.block_cache_compressed != nullptr);
  ASSERT_EQ(new_opt.block_cache_compressed->GetCapacity(), 0);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache_compressed)->GetNumShardBits(), 5);
  ASSERT_EQ(new_opt.block_cache_compressed->HasStrictCapacityLimit(), false);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache_compressed)
                ->GetHighPriPoolRatio(),
            0.0);

  // Set couple of block cache options.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      config_options, table_opt,
      "block_cache={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;};"
      "block_cache_compressed={capacity=1M;num_shard_bits=4;"
      "strict_capacity_limit=true;}",
      &new_opt));
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 1024UL*1024UL);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache)->GetNumShardBits(), 4);
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), true);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache)
                ->GetHighPriPoolRatio(),
            0.5);
  ASSERT_TRUE(new_opt.block_cache_compressed != nullptr);
  ASSERT_EQ(new_opt.block_cache_compressed->GetCapacity(), 1024UL*1024UL);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache_compressed)->GetNumShardBits(), 4);
  ASSERT_EQ(new_opt.block_cache_compressed->HasStrictCapacityLimit(), true);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache_compressed)
                ->GetHighPriPoolRatio(),
            0.5);
}
#endif  // !ROCKSDB_LITE


#ifndef ROCKSDB_LITE  // GetPlainTableOptionsFromString is not supported
TEST_F(OptionsTest, GetPlainTableOptionsFromString) {
  PlainTableOptions table_opt;
  PlainTableOptions new_opt;
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  // make sure default values are overwritten by something else
  ASSERT_OK(GetPlainTableOptionsFromString(
      config_options, table_opt,
      "user_key_len=66;bloom_bits_per_key=20;hash_table_ratio=0.5;"
      "index_sparseness=8;huge_page_tlb_size=4;encoding_type=kPrefix;"
      "full_scan_mode=true;store_index_in_file=true",
      &new_opt));
  ASSERT_EQ(new_opt.user_key_len, 66u);
  ASSERT_EQ(new_opt.bloom_bits_per_key, 20);
  ASSERT_EQ(new_opt.hash_table_ratio, 0.5);
  ASSERT_EQ(new_opt.index_sparseness, 8);
  ASSERT_EQ(new_opt.huge_page_tlb_size, 4);
  ASSERT_EQ(new_opt.encoding_type, EncodingType::kPrefix);
  ASSERT_TRUE(new_opt.full_scan_mode);
  ASSERT_TRUE(new_opt.store_index_in_file);

  // unknown option
  Status s = GetPlainTableOptionsFromString(
      config_options, table_opt,
      "user_key_len=66;bloom_bits_per_key=20;hash_table_ratio=0.5;"
      "bad_option=1",
      &new_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  // unrecognized EncodingType
  s = GetPlainTableOptionsFromString(
      config_options, table_opt,
      "user_key_len=66;bloom_bits_per_key=20;hash_table_ratio=0.5;"
      "encoding_type=kPrefixXX",
      &new_opt);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
}
#endif  // !ROCKSDB_LITE

#ifndef ROCKSDB_LITE  // GetMemTableRepFactoryFromString is not supported
TEST_F(OptionsTest, GetMemTableRepFactoryFromString) {
  std::unique_ptr<MemTableRepFactory> new_mem_factory = nullptr;

  ASSERT_OK(GetMemTableRepFactoryFromString("skip_list", &new_mem_factory));
  ASSERT_OK(GetMemTableRepFactoryFromString("skip_list:16", &new_mem_factory));
  ASSERT_EQ(std::string(new_mem_factory->Name()), "SkipListFactory");
  ASSERT_NOK(GetMemTableRepFactoryFromString("skip_list:16:invalid_opt",
                                             &new_mem_factory));

  ASSERT_OK(GetMemTableRepFactoryFromString("prefix_hash", &new_mem_factory));
  ASSERT_OK(GetMemTableRepFactoryFromString("prefix_hash:1000",
                                            &new_mem_factory));
  ASSERT_EQ(std::string(new_mem_factory->Name()), "HashSkipListRepFactory");
  ASSERT_NOK(GetMemTableRepFactoryFromString("prefix_hash:1000:invalid_opt",
                                             &new_mem_factory));

  ASSERT_OK(GetMemTableRepFactoryFromString("hash_linkedlist",
                                            &new_mem_factory));
  ASSERT_OK(GetMemTableRepFactoryFromString("hash_linkedlist:1000",
                                            &new_mem_factory));
  ASSERT_EQ(std::string(new_mem_factory->Name()), "HashLinkListRepFactory");
  ASSERT_NOK(GetMemTableRepFactoryFromString("hash_linkedlist:1000:invalid_opt",
                                             &new_mem_factory));

  ASSERT_OK(GetMemTableRepFactoryFromString("vector", &new_mem_factory));
  ASSERT_OK(GetMemTableRepFactoryFromString("vector:1024", &new_mem_factory));
  ASSERT_EQ(std::string(new_mem_factory->Name()), "VectorRepFactory");
  ASSERT_NOK(GetMemTableRepFactoryFromString("vector:1024:invalid_opt",
                                             &new_mem_factory));

  ASSERT_NOK(GetMemTableRepFactoryFromString("cuckoo", &new_mem_factory));
  // CuckooHash memtable is already removed.
  ASSERT_NOK(GetMemTableRepFactoryFromString("cuckoo:1024", &new_mem_factory));

  ASSERT_NOK(GetMemTableRepFactoryFromString("bad_factory", &new_mem_factory));
}
#endif  // !ROCKSDB_LITE

#ifndef ROCKSDB_LITE  // GetOptionsFromString is not supported in RocksDB Lite
TEST_F(OptionsTest, GetOptionsFromStringTest) {
  Options base_options, new_options;
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;

  base_options.write_buffer_size = 20;
  base_options.min_write_buffer_number_to_merge = 15;
  BlockBasedTableOptions block_based_table_options;
  block_based_table_options.cache_index_and_filter_blocks = true;
  base_options.table_factory.reset(
      NewBlockBasedTableFactory(block_based_table_options));

  // Register an Env with object registry.
  const static char* kCustomEnvName = "CustomEnv";
  class CustomEnv : public EnvWrapper {
   public:
    explicit CustomEnv(Env* _target) : EnvWrapper(_target) {}
  };

  ObjectLibrary::Default()->Register<Env>(
      kCustomEnvName,
      [](const std::string& /*name*/, std::unique_ptr<Env>* /*env_guard*/,
         std::string* /* errmsg */) {
        static CustomEnv env(Env::Default());
        return &env;
      });

  ASSERT_OK(GetOptionsFromString(
      config_options, base_options,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_cache=1M;block_size=4;};"
      "compression_opts=4:5:6;create_if_missing=true;max_open_files=1;"
      "bottommost_compression_opts=5:6:7;create_if_missing=true;max_open_files="
      "1;"
      "rate_limiter_bytes_per_sec=1024;env=CustomEnv",
      &new_options));

  ASSERT_EQ(new_options.compression_opts.window_bits, 4);
  ASSERT_EQ(new_options.compression_opts.level, 5);
  ASSERT_EQ(new_options.compression_opts.strategy, 6);
  ASSERT_EQ(new_options.compression_opts.max_dict_bytes, 0u);
  ASSERT_EQ(new_options.compression_opts.zstd_max_train_bytes, 0u);
  ASSERT_EQ(new_options.compression_opts.parallel_threads, 1u);
  ASSERT_EQ(new_options.compression_opts.enabled, false);
  ASSERT_EQ(new_options.bottommost_compression, kDisableCompressionOption);
  ASSERT_EQ(new_options.bottommost_compression_opts.window_bits, 5);
  ASSERT_EQ(new_options.bottommost_compression_opts.level, 6);
  ASSERT_EQ(new_options.bottommost_compression_opts.strategy, 7);
  ASSERT_EQ(new_options.bottommost_compression_opts.max_dict_bytes, 0u);
  ASSERT_EQ(new_options.bottommost_compression_opts.zstd_max_train_bytes, 0u);
  ASSERT_EQ(new_options.bottommost_compression_opts.parallel_threads, 1u);
  ASSERT_EQ(new_options.bottommost_compression_opts.enabled, false);
  ASSERT_EQ(new_options.write_buffer_size, 10U);
  ASSERT_EQ(new_options.max_write_buffer_number, 16);
  const auto new_bbto =
      new_options.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(new_bbto, nullptr);
  ASSERT_EQ(new_bbto->block_cache->GetCapacity(), 1U << 20);
  ASSERT_EQ(new_bbto->block_size, 4U);
  // don't overwrite block based table options
  ASSERT_TRUE(new_bbto->cache_index_and_filter_blocks);

  ASSERT_EQ(new_options.create_if_missing, true);
  ASSERT_EQ(new_options.max_open_files, 1);
  ASSERT_TRUE(new_options.rate_limiter.get() != nullptr);
  Env* newEnv = new_options.env;
  ASSERT_OK(Env::LoadEnv(kCustomEnvName, &newEnv));
  ASSERT_EQ(newEnv, new_options.env);

  config_options.ignore_unknown_options = false;
  // Test a bad value for a DBOption returns a failure
  base_options.dump_malloc_stats = false;
  base_options.write_buffer_size = 1024;
  Options bad_options = new_options;
  Status s = GetOptionsFromString(config_options, base_options,
                                  "create_if_missing=XX;dump_malloc_stats=true",
                                  &bad_options);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_EQ(bad_options.dump_malloc_stats, false);

  bad_options = new_options;
  s = GetOptionsFromString(config_options, base_options,
                           "write_buffer_size=XX;dump_malloc_stats=true",
                           &bad_options);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsInvalidArgument());

  ASSERT_EQ(bad_options.dump_malloc_stats, false);

  // Test a bad value for a TableFactory Option returns a failure
  bad_options = new_options;
  s = GetOptionsFromString(config_options, base_options,
                           "write_buffer_size=16;dump_malloc_stats=true"
                           "block_based_table_factory={block_size=XX;};",
                           &bad_options);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_EQ(bad_options.dump_malloc_stats, false);
  ASSERT_EQ(bad_options.write_buffer_size, 1024);

  config_options.ignore_unknown_options = true;
  ASSERT_OK(GetOptionsFromString(config_options, base_options,
                                 "create_if_missing=XX;dump_malloc_stats=true;"
                                 "write_buffer_size=XX;"
                                 "block_based_table_factory={block_size=XX;};",
                                 &bad_options));
  ASSERT_EQ(bad_options.create_if_missing, base_options.create_if_missing);
  ASSERT_EQ(bad_options.dump_malloc_stats, true);
  ASSERT_EQ(bad_options.write_buffer_size, base_options.write_buffer_size);

  // Test the old interface
  ASSERT_OK(GetOptionsFromString(
      base_options,
      "write_buffer_size=22;max_write_buffer_number=33;max_open_files=44;",
      &new_options));
  ASSERT_EQ(new_options.write_buffer_size, 22U);
  ASSERT_EQ(new_options.max_write_buffer_number, 33);
  ASSERT_EQ(new_options.max_open_files, 44);
}

TEST_F(OptionsTest, DBOptionsSerialization) {
  Options base_options, new_options;
  Random rnd(301);
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;

  // Phase 1: Make big change in base_options
  test::RandomInitDBOptions(&base_options, &rnd);

  // Phase 2: obtain a string from base_option
  std::string base_options_file_content;
  ASSERT_OK(GetStringFromDBOptions(config_options, base_options,
                                   &base_options_file_content));

  // Phase 3: Set new_options from the derived string and expect
  //          new_options == base_options
  ASSERT_OK(GetDBOptionsFromString(config_options, DBOptions(),
                                   base_options_file_content, &new_options));
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(config_options, base_options,
                                                  new_options));
}

TEST_F(OptionsTest, OptionsComposeDecompose) {
  // build an Options from DBOptions + CFOptions, then decompose it to verify
  // we get same constituent options.
  DBOptions base_db_opts;
  ColumnFamilyOptions base_cf_opts;
  ConfigOptions
      config_options;  // Use default for ignore(false) and check (exact)
  config_options.input_strings_escaped = false;

  Random rnd(301);
  test::RandomInitDBOptions(&base_db_opts, &rnd);
  test::RandomInitCFOptions(&base_cf_opts, base_db_opts, &rnd);

  Options base_opts(base_db_opts, base_cf_opts);
  DBOptions new_db_opts(base_opts);
  ColumnFamilyOptions new_cf_opts(base_opts);

  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(config_options, base_db_opts,
                                                  new_db_opts));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_cf_opts,
                                                  new_cf_opts));
  delete new_cf_opts.compaction_filter;
}

TEST_F(OptionsTest, ColumnFamilyOptionsSerialization) {
  Options options;
  ColumnFamilyOptions base_opt, new_opt;
  Random rnd(302);
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;

  // Phase 1: randomly assign base_opt
  // custom type options
  test::RandomInitCFOptions(&base_opt, options, &rnd);

  // Phase 2: obtain a string from base_opt
  std::string base_options_file_content;
  ASSERT_OK(GetStringFromColumnFamilyOptions(config_options, base_opt,
                                             &base_options_file_content));

  // Phase 3: Set new_opt from the derived string and expect
  //          new_opt == base_opt
  ASSERT_OK(
      GetColumnFamilyOptionsFromString(config_options, ColumnFamilyOptions(),
                                       base_options_file_content, &new_opt));
  ASSERT_OK(
      RocksDBOptionsParser::VerifyCFOptions(config_options, base_opt, new_opt));
  if (base_opt.compaction_filter) {
    delete base_opt.compaction_filter;
  }
}

TEST_F(OptionsTest, CheckBlockBasedTableOptions) {
  ColumnFamilyOptions cf_opts;
  DBOptions db_opts;
  ConfigOptions config_opts;

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_opts, cf_opts, "prefix_extractor=capped:8", &cf_opts));
  ASSERT_OK(TableFactory::CreateFromString(config_opts, "BlockBasedTable",
                                           &cf_opts.table_factory));
  ASSERT_NE(cf_opts.table_factory.get(), nullptr);
  ASSERT_TRUE(cf_opts.table_factory->IsInstanceOf(
      TableFactory::kBlockBasedTableName()));
  auto bbto = cf_opts.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_OK(cf_opts.table_factory->ConfigureFromString(
      config_opts,
      "block_cache={capacity=1M;num_shard_bits=4;};"
      "block_size_deviation=101;"
      "block_restart_interval=0;"
      "index_block_restart_interval=5;"
      "partition_filters=true;"
      "index_type=kHashSearch;"
      "no_block_cache=1;"));
  ASSERT_NE(bbto, nullptr);
  ASSERT_EQ(bbto->block_cache.get(), nullptr);
  ASSERT_EQ(bbto->block_size_deviation, 0);
  ASSERT_EQ(bbto->block_restart_interval, 1);
  ASSERT_EQ(bbto->index_block_restart_interval, 1);
  ASSERT_FALSE(bbto->partition_filters);
  ASSERT_OK(TableFactory::CreateFromString(config_opts, "BlockBasedTable",
                                           &cf_opts.table_factory));
  bbto = cf_opts.table_factory->GetOptions<BlockBasedTableOptions>();

  ASSERT_OK(cf_opts.table_factory->ConfigureFromString(config_opts,
                                                       "no_block_cache=0;"));
  ASSERT_NE(bbto->block_cache.get(), nullptr);
  ASSERT_OK(cf_opts.table_factory->ValidateOptions(db_opts, cf_opts));
}

TEST_F(OptionsTest, MutableTableOptions) {
  ConfigOptions config_options;
  std::shared_ptr<TableFactory> bbtf;
  bbtf.reset(NewBlockBasedTableFactory());
  auto bbto = bbtf->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(bbto, nullptr);
  ASSERT_FALSE(bbtf->IsPrepared());
  ASSERT_OK(bbtf->ConfigureOption(config_options, "block_align", "true"));
  ASSERT_OK(bbtf->ConfigureOption(config_options, "block_size", "1024"));
  ASSERT_EQ(bbto->block_align, true);
  ASSERT_EQ(bbto->block_size, 1024);
  ASSERT_OK(bbtf->PrepareOptions(config_options));
  ASSERT_TRUE(bbtf->IsPrepared());
  config_options.mutable_options_only = true;
  ASSERT_OK(bbtf->ConfigureOption(config_options, "block_size", "1024"));
  ASSERT_EQ(bbto->block_align, true);
  ASSERT_NOK(bbtf->ConfigureOption(config_options, "block_align", "false"));
  ASSERT_OK(bbtf->ConfigureOption(config_options, "block_size", "2048"));
  ASSERT_EQ(bbto->block_align, true);
  ASSERT_EQ(bbto->block_size, 2048);

  ColumnFamilyOptions cf_opts;
  cf_opts.table_factory = bbtf;
  ASSERT_NOK(GetColumnFamilyOptionsFromString(
      config_options, cf_opts, "block_based_table_factory.block_align=false",
      &cf_opts));
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, cf_opts, "block_based_table_factory.block_size=8192",
      &cf_opts));
  ASSERT_EQ(bbto->block_align, true);
  ASSERT_EQ(bbto->block_size, 8192);
}

TEST_F(OptionsTest, MutableCFOptions) {
  ConfigOptions config_options;
  ColumnFamilyOptions cf_opts;

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      config_options, cf_opts,
      "paranoid_file_checks=true; block_based_table_factory.block_align=false; "
      "block_based_table_factory.block_size=8192;",
      &cf_opts));
  ASSERT_TRUE(cf_opts.paranoid_file_checks);
  ASSERT_NE(cf_opts.table_factory.get(), nullptr);
  const auto bbto = cf_opts.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(bbto, nullptr);
  ASSERT_EQ(bbto->block_size, 8192);
  ASSERT_EQ(bbto->block_align, false);
  std::unordered_map<std::string, std::string> unused_opts;
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts, {{"paranoid_file_checks", "false"}}, &cf_opts));
  ASSERT_EQ(cf_opts.paranoid_file_checks, false);

  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"block_based_table_factory.block_size", "16384"}}, &cf_opts));
  ASSERT_EQ(bbto, cf_opts.table_factory->GetOptions<BlockBasedTableOptions>());
  ASSERT_EQ(bbto->block_size, 16384);

  config_options.mutable_options_only = true;
  // Force consistency checks is not mutable
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts, {{"force_consistency_checks", "true"}},
      &cf_opts));

  // Attempt to change the table.  It is not mutable, so this should fail and
  // leave the original intact
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts, {{"table_factory", "PlainTable"}}, &cf_opts));
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts, {{"table_factory.id", "PlainTable"}}, &cf_opts));
  ASSERT_NE(cf_opts.table_factory.get(), nullptr);
  ASSERT_EQ(bbto, cf_opts.table_factory->GetOptions<BlockBasedTableOptions>());

  // Change the block size.  Should update the value in the current table
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"block_based_table_factory.block_size", "8192"}}, &cf_opts));
  ASSERT_EQ(bbto, cf_opts.table_factory->GetOptions<BlockBasedTableOptions>());
  ASSERT_EQ(bbto->block_size, 8192);

  // Attempt to turn off block cache fails, as this option is not mutable
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"block_based_table_factory.no_block_cache", "true"}}, &cf_opts));
  ASSERT_EQ(bbto, cf_opts.table_factory->GetOptions<BlockBasedTableOptions>());

  // Attempt to change the block size via a config string/map.  Should update
  // the current value
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"block_based_table_factory", "{block_size=32768}"}}, &cf_opts));
  ASSERT_EQ(bbto, cf_opts.table_factory->GetOptions<BlockBasedTableOptions>());
  ASSERT_EQ(bbto->block_size, 32768);

  // Attempt to change the block size and no cache through the map.  Should
  // fail, leaving the old values intact
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
      config_options, cf_opts,
      {{"block_based_table_factory",
        "{block_size=16384; no_block_cache=true}"}},
      &cf_opts));
  ASSERT_EQ(bbto, cf_opts.table_factory->GetOptions<BlockBasedTableOptions>());
  ASSERT_EQ(bbto->block_size, 32768);
}

#endif  // !ROCKSDB_LITE

Status StringToMap(
    const std::string& opts_str,
    std::unordered_map<std::string, std::string>* opts_map);

#ifndef ROCKSDB_LITE  // StringToMap is not supported in ROCKSDB_LITE
TEST_F(OptionsTest, StringToMapTest) {
  std::unordered_map<std::string, std::string> opts_map;
  // Regular options
  ASSERT_OK(StringToMap("k1=v1;k2=v2;k3=v3", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "v2");
  ASSERT_EQ(opts_map["k3"], "v3");
  // Value with '='
  opts_map.clear();
  ASSERT_OK(StringToMap("k1==v1;k2=v2=;", &opts_map));
  ASSERT_EQ(opts_map["k1"], "=v1");
  ASSERT_EQ(opts_map["k2"], "v2=");
  // Overwrriten option
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k1=v2;k3=v3", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v2");
  ASSERT_EQ(opts_map["k3"], "v3");
  // Empty value
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2=;k3=v3;k4=", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_TRUE(opts_map.find("k2") != opts_map.end());
  ASSERT_EQ(opts_map["k2"], "");
  ASSERT_EQ(opts_map["k3"], "v3");
  ASSERT_TRUE(opts_map.find("k4") != opts_map.end());
  ASSERT_EQ(opts_map["k4"], "");
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2=;k3=v3;k4=   ", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_TRUE(opts_map.find("k2") != opts_map.end());
  ASSERT_EQ(opts_map["k2"], "");
  ASSERT_EQ(opts_map["k3"], "v3");
  ASSERT_TRUE(opts_map.find("k4") != opts_map.end());
  ASSERT_EQ(opts_map["k4"], "");
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2=;k3=", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_TRUE(opts_map.find("k2") != opts_map.end());
  ASSERT_EQ(opts_map["k2"], "");
  ASSERT_TRUE(opts_map.find("k3") != opts_map.end());
  ASSERT_EQ(opts_map["k3"], "");
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2=;k3=;", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_TRUE(opts_map.find("k2") != opts_map.end());
  ASSERT_EQ(opts_map["k2"], "");
  ASSERT_TRUE(opts_map.find("k3") != opts_map.end());
  ASSERT_EQ(opts_map["k3"], "");
  // Regular nested options
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2={nk1=nv1;nk2=nv2};k3=v3", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "nk1=nv1;nk2=nv2");
  ASSERT_EQ(opts_map["k3"], "v3");
  // Multi-level nested options
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2={nk1=nv1;nk2={nnk1=nnk2}};"
                        "k3={nk1={nnk1={nnnk1=nnnv1;nnnk2;nnnv2}}};k4=v4",
                        &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "nk1=nv1;nk2={nnk1=nnk2}");
  ASSERT_EQ(opts_map["k3"], "nk1={nnk1={nnnk1=nnnv1;nnnk2;nnnv2}}");
  ASSERT_EQ(opts_map["k4"], "v4");
  // Garbage inside curly braces
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2={dfad=};k3={=};k4=v4",
                        &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "dfad=");
  ASSERT_EQ(opts_map["k3"], "=");
  ASSERT_EQ(opts_map["k4"], "v4");
  // Empty nested options
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2={};", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "");
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2={{{{}}}{}{}};", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "{{{}}}{}{}");
  // With random spaces
  opts_map.clear();
  ASSERT_OK(StringToMap("  k1 =  v1 ; k2= {nk1=nv1; nk2={nnk1=nnk2}}  ; "
                        "k3={  {   } }; k4= v4  ",
                        &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "nk1=nv1; nk2={nnk1=nnk2}");
  ASSERT_EQ(opts_map["k3"], "{   }");
  ASSERT_EQ(opts_map["k4"], "v4");

  // Empty key
  ASSERT_NOK(StringToMap("k1=v1;k2=v2;=", &opts_map));
  ASSERT_NOK(StringToMap("=v1;k2=v2", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2v2;", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2=v2;fadfa", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2=v2;;", &opts_map));
  // Mismatch curly braces
  ASSERT_NOK(StringToMap("k1=v1;k2={;k3=v3", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{};k3=v3", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={}};k3=v3", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{}{}}};k3=v3", &opts_map));
  // However this is valid!
  opts_map.clear();
  ASSERT_OK(StringToMap("k1=v1;k2=};k3=v3", &opts_map));
  ASSERT_EQ(opts_map["k1"], "v1");
  ASSERT_EQ(opts_map["k2"], "}");
  ASSERT_EQ(opts_map["k3"], "v3");

  // Invalid chars after closing curly brace
  ASSERT_NOK(StringToMap("k1=v1;k2={{}}{};k3=v3", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{}}cfda;k3=v3", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{}}  cfda;k3=v3", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{}}  cfda", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{}}{}", &opts_map));
  ASSERT_NOK(StringToMap("k1=v1;k2={{dfdl}adfa}{}", &opts_map));
}
#endif  // ROCKSDB_LITE

#ifndef ROCKSDB_LITE  // StringToMap is not supported in ROCKSDB_LITE
TEST_F(OptionsTest, StringToMapRandomTest) {
  std::unordered_map<std::string, std::string> opts_map;
  // Make sure segfault is not hit by semi-random strings

  std::vector<std::string> bases = {
      "a={aa={};tt={xxx={}}};c=defff",
      "a={aa={};tt={xxx={}}};c=defff;d={{}yxx{}3{xx}}",
      "abc={{}{}{}{{{}}}{{}{}{}{}{}{}{}"};

  for (std::string base : bases) {
    for (int rand_seed = 301; rand_seed < 401; rand_seed++) {
      Random rnd(rand_seed);
      for (int attempt = 0; attempt < 10; attempt++) {
        std::string str = base;
        // Replace random position to space
        size_t pos = static_cast<size_t>(
            rnd.Uniform(static_cast<int>(base.size())));
        str[pos] = ' ';
        Status s = StringToMap(str, &opts_map);
        ASSERT_TRUE(s.ok() || s.IsInvalidArgument());
        opts_map.clear();
      }
    }
  }

  // Random Construct a string
  std::vector<char> chars = {'{', '}', ' ', '=', ';', 'c'};
  for (int rand_seed = 301; rand_seed < 1301; rand_seed++) {
    Random rnd(rand_seed);
    int len = rnd.Uniform(30);
    std::string str = "";
    for (int attempt = 0; attempt < len; attempt++) {
      // Add a random character
      size_t pos = static_cast<size_t>(
          rnd.Uniform(static_cast<int>(chars.size())));
      str.append(1, chars[pos]);
    }
    Status s = StringToMap(str, &opts_map);
    ASSERT_TRUE(s.ok() || s.IsInvalidArgument());
    s = StringToMap("name=" + str, &opts_map);
    ASSERT_TRUE(s.ok() || s.IsInvalidArgument());
    opts_map.clear();
  }
}

TEST_F(OptionsTest, GetStringFromCompressionType) {
  std::string res;

  ASSERT_OK(GetStringFromCompressionType(&res, kNoCompression));
  ASSERT_EQ(res, "kNoCompression");

  ASSERT_OK(GetStringFromCompressionType(&res, kSnappyCompression));
  ASSERT_EQ(res, "kSnappyCompression");

  ASSERT_OK(GetStringFromCompressionType(&res, kDisableCompressionOption));
  ASSERT_EQ(res, "kDisableCompressionOption");

  ASSERT_OK(GetStringFromCompressionType(&res, kLZ4Compression));
  ASSERT_EQ(res, "kLZ4Compression");

  ASSERT_OK(GetStringFromCompressionType(&res, kZlibCompression));
  ASSERT_EQ(res, "kZlibCompression");

  ASSERT_NOK(
      GetStringFromCompressionType(&res, static_cast<CompressionType>(-10)));
}

TEST_F(OptionsTest, OnlyMutableDBOptions) {
  std::string opt_str;
  Random rnd(302);
  ConfigOptions cfg_opts;
  DBOptions db_opts;
  DBOptions mdb_opts;
  std::unordered_set<std::string> m_names;
  std::unordered_set<std::string> a_names;

  test::RandomInitDBOptions(&db_opts, &rnd);
  auto db_config = DBOptionsAsConfigurable(db_opts);

  // Get all of the DB Option names (mutable or not)
  ASSERT_OK(db_config->GetOptionNames(cfg_opts, &a_names));

  // Get only the mutable options from db_opts and set those in mdb_opts
  cfg_opts.mutable_options_only = true;

  // Get only the Mutable DB Option names
  ASSERT_OK(db_config->GetOptionNames(cfg_opts, &m_names));
  ASSERT_OK(GetStringFromDBOptions(cfg_opts, db_opts, &opt_str));
  ASSERT_OK(GetDBOptionsFromString(cfg_opts, mdb_opts, opt_str, &mdb_opts));
  std::string mismatch;
  // Comparing only the mutable options, the two are equivalent
  auto mdb_config = DBOptionsAsConfigurable(mdb_opts);
  ASSERT_TRUE(mdb_config->AreEquivalent(cfg_opts, db_config.get(), &mismatch));
  ASSERT_TRUE(db_config->AreEquivalent(cfg_opts, mdb_config.get(), &mismatch));

  ASSERT_GT(a_names.size(), m_names.size());
  for (const auto& n : m_names) {
    std::string m, d;
    ASSERT_OK(mdb_config->GetOption(cfg_opts, n, &m));
    ASSERT_OK(db_config->GetOption(cfg_opts, n, &d));
    ASSERT_EQ(m, d);
  }

  cfg_opts.mutable_options_only = false;
  // Comparing all of the options, the two are not equivalent
  ASSERT_FALSE(mdb_config->AreEquivalent(cfg_opts, db_config.get(), &mismatch));
  ASSERT_FALSE(db_config->AreEquivalent(cfg_opts, mdb_config.get(), &mismatch));
}

TEST_F(OptionsTest, OnlyMutableCFOptions) {
  std::string opt_str;
  Random rnd(302);
  ConfigOptions cfg_opts;
  DBOptions db_opts;
  ColumnFamilyOptions mcf_opts;
  ColumnFamilyOptions cf_opts;
  std::unordered_set<std::string> m_names;
  std::unordered_set<std::string> a_names;

  test::RandomInitCFOptions(&cf_opts, db_opts, &rnd);
  auto cf_config = CFOptionsAsConfigurable(cf_opts);

  // Get all of the CF Option names (mutable or not)
  ASSERT_OK(cf_config->GetOptionNames(cfg_opts, &a_names));

  // Get only the mutable options from cf_opts and set those in mcf_opts
  cfg_opts.mutable_options_only = true;
  // Get only the Mutable CF Option names
  ASSERT_OK(cf_config->GetOptionNames(cfg_opts, &m_names));
  ASSERT_OK(GetStringFromColumnFamilyOptions(cfg_opts, cf_opts, &opt_str));
  ASSERT_OK(
      GetColumnFamilyOptionsFromString(cfg_opts, mcf_opts, opt_str, &mcf_opts));
  std::string mismatch;

  auto mcf_config = CFOptionsAsConfigurable(mcf_opts);
  // Comparing only the mutable options, the two are equivalent
  ASSERT_TRUE(mcf_config->AreEquivalent(cfg_opts, cf_config.get(), &mismatch));
  ASSERT_TRUE(cf_config->AreEquivalent(cfg_opts, mcf_config.get(), &mismatch));

  ASSERT_GT(a_names.size(), m_names.size());
  for (const auto& n : m_names) {
    std::string m, d;
    ASSERT_OK(mcf_config->GetOption(cfg_opts, n, &m));
    ASSERT_OK(cf_config->GetOption(cfg_opts, n, &d));
    ASSERT_EQ(m, d);
  }

  cfg_opts.mutable_options_only = false;
  // Comparing all of the options, the two are not equivalent
  ASSERT_FALSE(mcf_config->AreEquivalent(cfg_opts, cf_config.get(), &mismatch));
  ASSERT_FALSE(cf_config->AreEquivalent(cfg_opts, mcf_config.get(), &mismatch));

  delete cf_opts.compaction_filter;
}
#endif  // !ROCKSDB_LITE

TEST_F(OptionsTest, ConvertOptionsTest) {
  LevelDBOptions leveldb_opt;
  Options converted_opt = ConvertOptions(leveldb_opt);

  ASSERT_EQ(converted_opt.create_if_missing, leveldb_opt.create_if_missing);
  ASSERT_EQ(converted_opt.error_if_exists, leveldb_opt.error_if_exists);
  ASSERT_EQ(converted_opt.paranoid_checks, leveldb_opt.paranoid_checks);
  ASSERT_EQ(converted_opt.env, leveldb_opt.env);
  ASSERT_EQ(converted_opt.info_log.get(), leveldb_opt.info_log);
  ASSERT_EQ(converted_opt.write_buffer_size, leveldb_opt.write_buffer_size);
  ASSERT_EQ(converted_opt.max_open_files, leveldb_opt.max_open_files);
  ASSERT_EQ(converted_opt.compression, leveldb_opt.compression);

  std::shared_ptr<TableFactory> table_factory = converted_opt.table_factory;
  const auto table_opt = table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(table_opt, nullptr);

  ASSERT_EQ(table_opt->block_cache->GetCapacity(), 8UL << 20);
  ASSERT_EQ(table_opt->block_size, leveldb_opt.block_size);
  ASSERT_EQ(table_opt->block_restart_interval,
            leveldb_opt.block_restart_interval);
  ASSERT_EQ(table_opt->filter_policy.get(), leveldb_opt.filter_policy);
}

#ifndef ROCKSDB_LITE
// This test suite tests the old APIs into the Configure options methods.
// Once those APIs are officially deprecated, this test suite can be deleted.
class OptionsOldApiTest : public testing::Test {};

TEST_F(OptionsOldApiTest, GetOptionsFromMapTest) {
  std::unordered_map<std::string, std::string> cf_options_map = {
      {"write_buffer_size", "1"},
      {"max_write_buffer_number", "2"},
      {"min_write_buffer_number_to_merge", "3"},
      {"max_write_buffer_number_to_maintain", "99"},
      {"max_write_buffer_size_to_maintain", "-99999"},
      {"compression", "kSnappyCompression"},
      {"compression_per_level",
       "kNoCompression:"
       "kSnappyCompression:"
       "kZlibCompression:"
       "kBZip2Compression:"
       "kLZ4Compression:"
       "kLZ4HCCompression:"
       "kXpressCompression:"
       "kZSTD:"
       "kZSTDNotFinalCompression"},
      {"bottommost_compression", "kLZ4Compression"},
      {"bottommost_compression_opts", "5:6:7:8:9:true"},
      {"compression_opts", "4:5:6:7:8:true"},
      {"num_levels", "8"},
      {"level0_file_num_compaction_trigger", "8"},
      {"level0_slowdown_writes_trigger", "9"},
      {"level0_stop_writes_trigger", "10"},
      {"target_file_size_base", "12"},
      {"target_file_size_multiplier", "13"},
      {"max_bytes_for_level_base", "14"},
      {"level_compaction_dynamic_level_bytes", "true"},
      {"max_bytes_for_level_multiplier", "15.0"},
      {"max_bytes_for_level_multiplier_additional", "16:17:18"},
      {"max_compaction_bytes", "21"},
      {"soft_rate_limit", "1.1"},
      {"hard_rate_limit", "2.1"},
      {"hard_pending_compaction_bytes_limit", "211"},
      {"arena_block_size", "22"},
      {"disable_auto_compactions", "true"},
      {"compaction_style", "kCompactionStyleLevel"},
      {"compaction_pri", "kOldestSmallestSeqFirst"},
      {"verify_checksums_in_compaction", "false"},
      {"compaction_options_fifo", "23"},
      {"max_sequential_skip_in_iterations", "24"},
      {"inplace_update_support", "true"},
      {"report_bg_io_stats", "true"},
      {"compaction_measure_io_stats", "false"},
      {"inplace_update_num_locks", "25"},
      {"memtable_prefix_bloom_size_ratio", "0.26"},
      {"memtable_whole_key_filtering", "true"},
      {"memtable_huge_page_size", "28"},
      {"bloom_locality", "29"},
      {"max_successive_merges", "30"},
      {"min_partial_merge_operands", "31"},
      {"prefix_extractor", "fixed:31"},
      {"optimize_filters_for_hits", "true"},
      {"enable_blob_files", "true"},
      {"min_blob_size", "1K"},
      {"blob_file_size", "1G"},
      {"blob_compression_type", "kZSTD"},
      {"enable_blob_garbage_collection", "true"},
      {"blob_garbage_collection_age_cutoff", "0.5"},
  };

  std::unordered_map<std::string, std::string> db_options_map = {
      {"create_if_missing", "false"},
      {"create_missing_column_families", "true"},
      {"error_if_exists", "false"},
      {"paranoid_checks", "true"},
      {"track_and_verify_wals_in_manifest", "true"},
      {"max_open_files", "32"},
      {"max_total_wal_size", "33"},
      {"use_fsync", "true"},
      {"db_log_dir", "/db_log_dir"},
      {"wal_dir", "/wal_dir"},
      {"delete_obsolete_files_period_micros", "34"},
      {"max_background_compactions", "35"},
      {"max_background_flushes", "36"},
      {"max_log_file_size", "37"},
      {"log_file_time_to_roll", "38"},
      {"keep_log_file_num", "39"},
      {"recycle_log_file_num", "5"},
      {"max_manifest_file_size", "40"},
      {"table_cache_numshardbits", "41"},
      {"WAL_ttl_seconds", "43"},
      {"WAL_size_limit_MB", "44"},
      {"manifest_preallocation_size", "45"},
      {"allow_mmap_reads", "true"},
      {"allow_mmap_writes", "false"},
      {"use_direct_reads", "false"},
      {"use_direct_io_for_flush_and_compaction", "false"},
      {"is_fd_close_on_exec", "true"},
      {"skip_log_error_on_recovery", "false"},
      {"stats_dump_period_sec", "46"},
      {"stats_persist_period_sec", "57"},
      {"persist_stats_to_disk", "false"},
      {"stats_history_buffer_size", "69"},
      {"advise_random_on_open", "true"},
      {"use_adaptive_mutex", "false"},
      {"new_table_reader_for_compaction_inputs", "true"},
      {"compaction_readahead_size", "100"},
      {"random_access_max_buffer_size", "3145728"},
      {"writable_file_max_buffer_size", "314159"},
      {"bytes_per_sync", "47"},
      {"wal_bytes_per_sync", "48"},
      {"strict_bytes_per_sync", "true"},
  };

  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
            base_cf_opt, cf_options_map, &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 1U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 2);
  ASSERT_EQ(new_cf_opt.min_write_buffer_number_to_merge, 3);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number_to_maintain, 99);
  ASSERT_EQ(new_cf_opt.max_write_buffer_size_to_maintain, -99999);
  ASSERT_EQ(new_cf_opt.compression, kSnappyCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level.size(), 9U);
  ASSERT_EQ(new_cf_opt.compression_per_level[0], kNoCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[1], kSnappyCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[2], kZlibCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[3], kBZip2Compression);
  ASSERT_EQ(new_cf_opt.compression_per_level[4], kLZ4Compression);
  ASSERT_EQ(new_cf_opt.compression_per_level[5], kLZ4HCCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[6], kXpressCompression);
  ASSERT_EQ(new_cf_opt.compression_per_level[7], kZSTD);
  ASSERT_EQ(new_cf_opt.compression_per_level[8], kZSTDNotFinalCompression);
  ASSERT_EQ(new_cf_opt.compression_opts.window_bits, 4);
  ASSERT_EQ(new_cf_opt.compression_opts.level, 5);
  ASSERT_EQ(new_cf_opt.compression_opts.strategy, 6);
  ASSERT_EQ(new_cf_opt.compression_opts.max_dict_bytes, 7u);
  ASSERT_EQ(new_cf_opt.compression_opts.zstd_max_train_bytes, 8u);
  ASSERT_EQ(new_cf_opt.compression_opts.parallel_threads,
            CompressionOptions().parallel_threads);
  ASSERT_EQ(new_cf_opt.compression_opts.enabled, true);
  ASSERT_EQ(new_cf_opt.bottommost_compression, kLZ4Compression);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.window_bits, 5);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.level, 6);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.strategy, 7);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.max_dict_bytes, 8u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.zstd_max_train_bytes, 9u);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.parallel_threads,
            CompressionOptions().parallel_threads);
  ASSERT_EQ(new_cf_opt.bottommost_compression_opts.enabled, true);
  ASSERT_EQ(new_cf_opt.num_levels, 8);
  ASSERT_EQ(new_cf_opt.level0_file_num_compaction_trigger, 8);
  ASSERT_EQ(new_cf_opt.level0_slowdown_writes_trigger, 9);
  ASSERT_EQ(new_cf_opt.level0_stop_writes_trigger, 10);
  ASSERT_EQ(new_cf_opt.target_file_size_base, static_cast<uint64_t>(12));
  ASSERT_EQ(new_cf_opt.target_file_size_multiplier, 13);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_base, 14U);
  ASSERT_EQ(new_cf_opt.level_compaction_dynamic_level_bytes, true);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier, 15.0);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional.size(), 3U);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[0], 16);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[1], 17);
  ASSERT_EQ(new_cf_opt.max_bytes_for_level_multiplier_additional[2], 18);
  ASSERT_EQ(new_cf_opt.max_compaction_bytes, 21);
  ASSERT_EQ(new_cf_opt.hard_pending_compaction_bytes_limit, 211);
  ASSERT_EQ(new_cf_opt.arena_block_size, 22U);
  ASSERT_EQ(new_cf_opt.disable_auto_compactions, true);
  ASSERT_EQ(new_cf_opt.compaction_style, kCompactionStyleLevel);
  ASSERT_EQ(new_cf_opt.compaction_pri, kOldestSmallestSeqFirst);
  ASSERT_EQ(new_cf_opt.compaction_options_fifo.max_table_files_size,
            static_cast<uint64_t>(23));
  ASSERT_EQ(new_cf_opt.max_sequential_skip_in_iterations,
            static_cast<uint64_t>(24));
  ASSERT_EQ(new_cf_opt.inplace_update_support, true);
  ASSERT_EQ(new_cf_opt.inplace_update_num_locks, 25U);
  ASSERT_EQ(new_cf_opt.memtable_prefix_bloom_size_ratio, 0.26);
  ASSERT_EQ(new_cf_opt.memtable_whole_key_filtering, true);
  ASSERT_EQ(new_cf_opt.memtable_huge_page_size, 28U);
  ASSERT_EQ(new_cf_opt.bloom_locality, 29U);
  ASSERT_EQ(new_cf_opt.max_successive_merges, 30U);
  ASSERT_TRUE(new_cf_opt.prefix_extractor != nullptr);
  ASSERT_EQ(new_cf_opt.optimize_filters_for_hits, true);
  ASSERT_EQ(std::string(new_cf_opt.prefix_extractor->Name()),
            "rocksdb.FixedPrefix.31");
  ASSERT_EQ(new_cf_opt.enable_blob_files, true);
  ASSERT_EQ(new_cf_opt.min_blob_size, 1ULL << 10);
  ASSERT_EQ(new_cf_opt.blob_file_size, 1ULL << 30);
  ASSERT_EQ(new_cf_opt.blob_compression_type, kZSTD);
  ASSERT_EQ(new_cf_opt.enable_blob_garbage_collection, true);
  ASSERT_EQ(new_cf_opt.blob_garbage_collection_age_cutoff, 0.5);

  cf_options_map["write_buffer_size"] = "hello";
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
             base_cf_opt, cf_options_map, &new_cf_opt));
  ConfigOptions exact, loose;
  exact.sanity_level = ConfigOptions::kSanityLevelExactMatch;
  loose.sanity_level = ConfigOptions::kSanityLevelLooselyCompatible;

  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  cf_options_map["write_buffer_size"] = "1";
  ASSERT_OK(GetColumnFamilyOptionsFromMap(
            base_cf_opt, cf_options_map, &new_cf_opt));

  cf_options_map["unknown_option"] = "1";
  ASSERT_NOK(GetColumnFamilyOptionsFromMap(
             base_cf_opt, cf_options_map, &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  ASSERT_OK(GetColumnFamilyOptionsFromMap(base_cf_opt, cf_options_map,
                                          &new_cf_opt,
                                          false, /* input_strings_escaped  */
                                          true /* ignore_unknown_options */));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
      loose, base_cf_opt, new_cf_opt, nullptr /* new_opt_map */));
  ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
      exact /* default for VerifyCFOptions */, base_cf_opt, new_cf_opt, nullptr));

  DBOptions base_db_opt;
  DBOptions new_db_opt;
  ASSERT_OK(GetDBOptionsFromMap(base_db_opt, db_options_map, &new_db_opt));
  ASSERT_EQ(new_db_opt.create_if_missing, false);
  ASSERT_EQ(new_db_opt.create_missing_column_families, true);
  ASSERT_EQ(new_db_opt.error_if_exists, false);
  ASSERT_EQ(new_db_opt.paranoid_checks, true);
  ASSERT_EQ(new_db_opt.track_and_verify_wals_in_manifest, true);
  ASSERT_EQ(new_db_opt.max_open_files, 32);
  ASSERT_EQ(new_db_opt.max_total_wal_size, static_cast<uint64_t>(33));
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
  ASSERT_EQ(new_db_opt.recycle_log_file_num, 5U);
  ASSERT_EQ(new_db_opt.max_manifest_file_size, static_cast<uint64_t>(40));
  ASSERT_EQ(new_db_opt.table_cache_numshardbits, 41);
  ASSERT_EQ(new_db_opt.WAL_ttl_seconds, static_cast<uint64_t>(43));
  ASSERT_EQ(new_db_opt.WAL_size_limit_MB, static_cast<uint64_t>(44));
  ASSERT_EQ(new_db_opt.manifest_preallocation_size, 45U);
  ASSERT_EQ(new_db_opt.allow_mmap_reads, true);
  ASSERT_EQ(new_db_opt.allow_mmap_writes, false);
  ASSERT_EQ(new_db_opt.use_direct_reads, false);
  ASSERT_EQ(new_db_opt.use_direct_io_for_flush_and_compaction, false);
  ASSERT_EQ(new_db_opt.is_fd_close_on_exec, true);
  ASSERT_EQ(new_db_opt.skip_log_error_on_recovery, false);
  ASSERT_EQ(new_db_opt.stats_dump_period_sec, 46U);
  ASSERT_EQ(new_db_opt.stats_persist_period_sec, 57U);
  ASSERT_EQ(new_db_opt.persist_stats_to_disk, false);
  ASSERT_EQ(new_db_opt.stats_history_buffer_size, 69U);
  ASSERT_EQ(new_db_opt.advise_random_on_open, true);
  ASSERT_EQ(new_db_opt.use_adaptive_mutex, false);
  ASSERT_EQ(new_db_opt.new_table_reader_for_compaction_inputs, true);
  ASSERT_EQ(new_db_opt.compaction_readahead_size, 100);
  ASSERT_EQ(new_db_opt.random_access_max_buffer_size, 3145728);
  ASSERT_EQ(new_db_opt.writable_file_max_buffer_size, 314159);
  ASSERT_EQ(new_db_opt.bytes_per_sync, static_cast<uint64_t>(47));
  ASSERT_EQ(new_db_opt.wal_bytes_per_sync, static_cast<uint64_t>(48));
  ASSERT_EQ(new_db_opt.strict_bytes_per_sync, true);

  db_options_map["max_open_files"] = "hello";
  ASSERT_NOK(GetDBOptionsFromMap(base_db_opt, db_options_map, &new_db_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(loose, base_db_opt, new_db_opt));

  // unknow options should fail parsing without ignore_unknown_options = true
  db_options_map["unknown_db_option"] = "1";
  ASSERT_NOK(GetDBOptionsFromMap(base_db_opt, db_options_map, &new_db_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));

  ASSERT_OK(GetDBOptionsFromMap(base_db_opt, db_options_map, &new_db_opt,
                                false, /* input_strings_escaped  */
                                true /* ignore_unknown_options */));
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(loose, base_db_opt, new_db_opt));
  ASSERT_NOK(RocksDBOptionsParser::VerifyDBOptions(exact, base_db_opt, new_db_opt));
}

TEST_F(OptionsOldApiTest, GetColumnFamilyOptionsFromStringTest) {
  ColumnFamilyOptions base_cf_opt;
  ColumnFamilyOptions new_cf_opt;
  base_cf_opt.table_factory.reset();
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt, "", &new_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "write_buffer_size=5", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 5U);
  ASSERT_TRUE(new_cf_opt.table_factory == nullptr);
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "write_buffer_size=6;", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 6U);
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "  write_buffer_size =  7  ", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 7U);
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "  write_buffer_size =  8 ; ", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 8U);
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "write_buffer_size=9;max_write_buffer_number=10", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 9U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 10);
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "write_buffer_size=11; max_write_buffer_number  =  12 ;",
            &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 11U);
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 12);
  // Wrong name "max_write_buffer_number_"
  ASSERT_NOK(GetColumnFamilyOptionsFromString(base_cf_opt,
             "write_buffer_size=13;max_write_buffer_number_=14;",
              &new_cf_opt));
  ConfigOptions exact;
  exact.sanity_level = ConfigOptions::kSanityLevelExactMatch;
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // Comparator from object registry
  std::string kCompName = "reverse_comp";
  ObjectLibrary::Default()->Register<const Comparator>(
      kCompName,
      [](const std::string& /*name*/,
         std::unique_ptr<const Comparator>* /*guard*/,
         std::string* /* errmsg */) { return ReverseBytewiseComparator(); });

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      base_cf_opt, "comparator=" + kCompName + ";", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.comparator, ReverseBytewiseComparator());

  // MergeOperator from object registry
  std::unique_ptr<BytesXOROperator> bxo(new BytesXOROperator());
  std::string kMoName = bxo->Name();
  ObjectLibrary::Default()->Register<MergeOperator>(
      kMoName,
      [](const std::string& /*name*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /* errmsg */) {
        guard->reset(new BytesXOROperator());
        return guard->get();
      });

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      base_cf_opt, "merge_operator=" + kMoName + ";", &new_cf_opt));
  ASSERT_EQ(kMoName, std::string(new_cf_opt.merge_operator->Name()));

  // Wrong key/value pair
  ASSERT_NOK(GetColumnFamilyOptionsFromString(base_cf_opt,
             "write_buffer_size=13;max_write_buffer_number;", &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // Error Paring value
  ASSERT_NOK(GetColumnFamilyOptionsFromString(base_cf_opt,
             "write_buffer_size=13;max_write_buffer_number=;", &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // Missing option name
  ASSERT_NOK(GetColumnFamilyOptionsFromString(base_cf_opt,
             "write_buffer_size=13; =100;", &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  const uint64_t kilo = 1024UL;
  const uint64_t mega = 1024 * kilo;
  const uint64_t giga = 1024 * mega;
  const uint64_t tera = 1024 * giga;

  // Units (k)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      base_cf_opt, "max_write_buffer_number=15K", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 15 * kilo);
  // Units (m)
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "max_write_buffer_number=16m;inplace_update_num_locks=17M",
            &new_cf_opt));
  ASSERT_EQ(new_cf_opt.max_write_buffer_number, 16 * mega);
  ASSERT_EQ(new_cf_opt.inplace_update_num_locks, 17u * mega);
  // Units (g)
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      base_cf_opt,
      "write_buffer_size=18g;prefix_extractor=capped:8;"
      "arena_block_size=19G",
      &new_cf_opt));

  ASSERT_EQ(new_cf_opt.write_buffer_size, 18 * giga);
  ASSERT_EQ(new_cf_opt.arena_block_size, 19 * giga);
  ASSERT_TRUE(new_cf_opt.prefix_extractor.get() != nullptr);
  std::string prefix_name(new_cf_opt.prefix_extractor->Name());
  ASSERT_EQ(prefix_name, "rocksdb.CappedPrefix.8");

  // Units (t)
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "write_buffer_size=20t;arena_block_size=21T", &new_cf_opt));
  ASSERT_EQ(new_cf_opt.write_buffer_size, 20 * tera);
  ASSERT_EQ(new_cf_opt.arena_block_size, 21 * tera);

  // Nested block based table options
  // Empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "write_buffer_size=10;max_write_buffer_number=16;"
            "block_based_table_factory={};arena_block_size=1024",
            &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  // Non-empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "write_buffer_size=10;max_write_buffer_number=16;"
            "block_based_table_factory={block_cache=1M;block_size=4;};"
            "arena_block_size=1024",
            &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  // Last one
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "write_buffer_size=10;max_write_buffer_number=16;"
            "block_based_table_factory={block_cache=1M;block_size=4;}",
            &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  // Mismatch curly braces
  ASSERT_NOK(GetColumnFamilyOptionsFromString(base_cf_opt,
             "write_buffer_size=10;max_write_buffer_number=16;"
             "block_based_table_factory={{{block_size=4;};"
             "arena_block_size=1024",
             &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // Unexpected chars after closing curly brace
  ASSERT_NOK(GetColumnFamilyOptionsFromString(base_cf_opt,
             "write_buffer_size=10;max_write_buffer_number=16;"
             "block_based_table_factory={block_size=4;}};"
             "arena_block_size=1024",
             &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  ASSERT_NOK(GetColumnFamilyOptionsFromString(base_cf_opt,
             "write_buffer_size=10;max_write_buffer_number=16;"
             "block_based_table_factory={block_size=4;}xdfa;"
             "arena_block_size=1024",
             &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  ASSERT_NOK(GetColumnFamilyOptionsFromString(base_cf_opt,
             "write_buffer_size=10;max_write_buffer_number=16;"
             "block_based_table_factory={block_size=4;}xdfa",
             &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // Invalid block based table option
  ASSERT_NOK(GetColumnFamilyOptionsFromString(base_cf_opt,
             "write_buffer_size=10;max_write_buffer_number=16;"
             "block_based_table_factory={xx_block_size=4;}",
             &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
           "optimize_filters_for_hits=true",
           &new_cf_opt));
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "optimize_filters_for_hits=false",
            &new_cf_opt));

  ASSERT_NOK(GetColumnFamilyOptionsFromString(base_cf_opt,
              "optimize_filters_for_hits=junk",
              &new_cf_opt));
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(exact, base_cf_opt, new_cf_opt));

  // Nested plain table options
  // Empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "write_buffer_size=10;max_write_buffer_number=16;"
            "plain_table_factory={};arena_block_size=1024",
            &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  ASSERT_EQ(std::string(new_cf_opt.table_factory->Name()), "PlainTable");
  // Non-empty
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "write_buffer_size=10;max_write_buffer_number=16;"
            "plain_table_factory={user_key_len=66;bloom_bits_per_key=20;};"
            "arena_block_size=1024",
            &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.table_factory != nullptr);
  ASSERT_EQ(std::string(new_cf_opt.table_factory->Name()), "PlainTable");

  // memtable factory
  ASSERT_OK(GetColumnFamilyOptionsFromString(base_cf_opt,
            "write_buffer_size=10;max_write_buffer_number=16;"
            "memtable=skip_list:10;arena_block_size=1024",
            &new_cf_opt));
  ASSERT_TRUE(new_cf_opt.memtable_factory != nullptr);
  ASSERT_EQ(std::string(new_cf_opt.memtable_factory->Name()), "SkipListFactory");
}

TEST_F(OptionsOldApiTest, GetBlockBasedTableOptionsFromString) {
  BlockBasedTableOptions table_opt;
  BlockBasedTableOptions new_opt;
  // make sure default values are overwritten by something else
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      table_opt,
      "cache_index_and_filter_blocks=1;index_type=kHashSearch;"
      "checksum=kxxHash;hash_index_allow_collision=1;no_block_cache=1;"
      "block_cache=1M;block_cache_compressed=1k;block_size=1024;"
      "block_size_deviation=8;block_restart_interval=4;"
      "format_version=5;whole_key_filtering=1;"
      "filter_policy=bloomfilter:4.567:false;",
      &new_opt));
  ASSERT_TRUE(new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(new_opt.index_type, BlockBasedTableOptions::kHashSearch);
  ASSERT_EQ(new_opt.checksum, ChecksumType::kxxHash);
  ASSERT_TRUE(new_opt.hash_index_allow_collision);
  ASSERT_TRUE(new_opt.no_block_cache);
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 1024UL*1024UL);
  ASSERT_TRUE(new_opt.block_cache_compressed != nullptr);
  ASSERT_EQ(new_opt.block_cache_compressed->GetCapacity(), 1024UL);
  ASSERT_EQ(new_opt.block_size, 1024UL);
  ASSERT_EQ(new_opt.block_size_deviation, 8);
  ASSERT_EQ(new_opt.block_restart_interval, 4);
  ASSERT_EQ(new_opt.format_version, 5U);
  ASSERT_EQ(new_opt.whole_key_filtering, true);
  ASSERT_TRUE(new_opt.filter_policy != nullptr);
  const BloomFilterPolicy& bfp =
      dynamic_cast<const BloomFilterPolicy&>(*new_opt.filter_policy);
  EXPECT_EQ(bfp.GetMillibitsPerKey(), 4567);
  EXPECT_EQ(bfp.GetWholeBitsPerKey(), 5);

  // unknown option
  ASSERT_NOK(GetBlockBasedTableOptionsFromString(table_opt,
             "cache_index_and_filter_blocks=1;index_type=kBinarySearch;"
             "bad_option=1",
             &new_opt));
  ASSERT_EQ(static_cast<bool>(table_opt.cache_index_and_filter_blocks),
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.index_type, new_opt.index_type);

  // unrecognized index type
  ASSERT_NOK(GetBlockBasedTableOptionsFromString(table_opt,
             "cache_index_and_filter_blocks=1;index_type=kBinarySearchXX",
             &new_opt));
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.index_type, new_opt.index_type);

  // unrecognized checksum type
  ASSERT_NOK(GetBlockBasedTableOptionsFromString(table_opt,
             "cache_index_and_filter_blocks=1;checksum=kxxHashXX",
             &new_opt));
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.index_type, new_opt.index_type);

  // unrecognized filter policy name
  ASSERT_NOK(GetBlockBasedTableOptionsFromString(table_opt,
             "cache_index_and_filter_blocks=1;"
             "filter_policy=bloomfilterxx:4:true",
             &new_opt));
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.filter_policy, new_opt.filter_policy);

  // unrecognized filter policy config
  ASSERT_NOK(GetBlockBasedTableOptionsFromString(table_opt,
             "cache_index_and_filter_blocks=1;"
             "filter_policy=bloomfilter:4",
             &new_opt));
  ASSERT_EQ(table_opt.cache_index_and_filter_blocks,
            new_opt.cache_index_and_filter_blocks);
  ASSERT_EQ(table_opt.filter_policy, new_opt.filter_policy);

  // Check block cache options are overwritten when specified
  // in new format as a struct.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(table_opt,
             "block_cache={capacity=1M;num_shard_bits=4;"
             "strict_capacity_limit=true;high_pri_pool_ratio=0.5;};"
             "block_cache_compressed={capacity=1M;num_shard_bits=4;"
             "strict_capacity_limit=true;high_pri_pool_ratio=0.5;}",
             &new_opt));
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 1024UL*1024UL);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache)->GetNumShardBits(), 4);
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), true);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(
                new_opt.block_cache)->GetHighPriPoolRatio(), 0.5);
  ASSERT_TRUE(new_opt.block_cache_compressed != nullptr);
  ASSERT_EQ(new_opt.block_cache_compressed->GetCapacity(), 1024UL*1024UL);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache_compressed)->GetNumShardBits(), 4);
  ASSERT_EQ(new_opt.block_cache_compressed->HasStrictCapacityLimit(), true);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(
                new_opt.block_cache_compressed)->GetHighPriPoolRatio(),
                0.5);

  // Set only block cache capacity. Check other values are
  // reset to default values.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(table_opt,
             "block_cache={capacity=2M};"
             "block_cache_compressed={capacity=2M}",
             &new_opt));
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 2*1024UL*1024UL);
  // Default values
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache)->GetNumShardBits(),
                GetDefaultCacheShardBits(new_opt.block_cache->GetCapacity()));
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), false);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache)
                ->GetHighPriPoolRatio(),
            0.5);
  ASSERT_TRUE(new_opt.block_cache_compressed != nullptr);
  ASSERT_EQ(new_opt.block_cache_compressed->GetCapacity(), 2*1024UL*1024UL);
  // Default values
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache_compressed)->GetNumShardBits(),
                GetDefaultCacheShardBits(
                    new_opt.block_cache_compressed->GetCapacity()));
  ASSERT_EQ(new_opt.block_cache_compressed->HasStrictCapacityLimit(), false);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache_compressed)
                ->GetHighPriPoolRatio(),
            0.5);

  // Set couple of block cache options.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(
      table_opt,
      "block_cache={num_shard_bits=5;high_pri_pool_ratio=0.5;};"
      "block_cache_compressed={num_shard_bits=5;"
      "high_pri_pool_ratio=0.0;}",
      &new_opt));
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 0);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache)->GetNumShardBits(), 5);
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), false);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(
                new_opt.block_cache)->GetHighPriPoolRatio(), 0.5);
  ASSERT_TRUE(new_opt.block_cache_compressed != nullptr);
  ASSERT_EQ(new_opt.block_cache_compressed->GetCapacity(), 0);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache_compressed)->GetNumShardBits(), 5);
  ASSERT_EQ(new_opt.block_cache_compressed->HasStrictCapacityLimit(), false);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache_compressed)
                ->GetHighPriPoolRatio(),
            0.0);

  // Set couple of block cache options.
  ASSERT_OK(GetBlockBasedTableOptionsFromString(table_opt,
             "block_cache={capacity=1M;num_shard_bits=4;"
             "strict_capacity_limit=true;};"
             "block_cache_compressed={capacity=1M;num_shard_bits=4;"
             "strict_capacity_limit=true;}",
             &new_opt));
  ASSERT_TRUE(new_opt.block_cache != nullptr);
  ASSERT_EQ(new_opt.block_cache->GetCapacity(), 1024UL*1024UL);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache)->GetNumShardBits(), 4);
  ASSERT_EQ(new_opt.block_cache->HasStrictCapacityLimit(), true);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache)
                ->GetHighPriPoolRatio(),
            0.5);
  ASSERT_TRUE(new_opt.block_cache_compressed != nullptr);
  ASSERT_EQ(new_opt.block_cache_compressed->GetCapacity(), 1024UL*1024UL);
  ASSERT_EQ(std::dynamic_pointer_cast<ShardedCache>(
                new_opt.block_cache_compressed)->GetNumShardBits(), 4);
  ASSERT_EQ(new_opt.block_cache_compressed->HasStrictCapacityLimit(), true);
  ASSERT_EQ(std::dynamic_pointer_cast<LRUCache>(new_opt.block_cache_compressed)
                ->GetHighPriPoolRatio(),
            0.5);
}

TEST_F(OptionsOldApiTest, GetPlainTableOptionsFromString) {
  PlainTableOptions table_opt;
  PlainTableOptions new_opt;
  // make sure default values are overwritten by something else
  ASSERT_OK(GetPlainTableOptionsFromString(table_opt,
            "user_key_len=66;bloom_bits_per_key=20;hash_table_ratio=0.5;"
            "index_sparseness=8;huge_page_tlb_size=4;encoding_type=kPrefix;"
            "full_scan_mode=true;store_index_in_file=true",
            &new_opt));
  ASSERT_EQ(new_opt.user_key_len, 66u);
  ASSERT_EQ(new_opt.bloom_bits_per_key, 20);
  ASSERT_EQ(new_opt.hash_table_ratio, 0.5);
  ASSERT_EQ(new_opt.index_sparseness, 8);
  ASSERT_EQ(new_opt.huge_page_tlb_size, 4);
  ASSERT_EQ(new_opt.encoding_type, EncodingType::kPrefix);
  ASSERT_TRUE(new_opt.full_scan_mode);
  ASSERT_TRUE(new_opt.store_index_in_file);

  // unknown option
  ASSERT_NOK(GetPlainTableOptionsFromString(table_opt,
             "user_key_len=66;bloom_bits_per_key=20;hash_table_ratio=0.5;"
             "bad_option=1",
             &new_opt));

  // unrecognized EncodingType
  ASSERT_NOK(GetPlainTableOptionsFromString(table_opt,
             "user_key_len=66;bloom_bits_per_key=20;hash_table_ratio=0.5;"
             "encoding_type=kPrefixXX",
             &new_opt));
}

TEST_F(OptionsOldApiTest, GetOptionsFromStringTest) {
  Options base_options, new_options;
  base_options.write_buffer_size = 20;
  base_options.min_write_buffer_number_to_merge = 15;
  BlockBasedTableOptions block_based_table_options;
  block_based_table_options.cache_index_and_filter_blocks = true;
  base_options.table_factory.reset(
      NewBlockBasedTableFactory(block_based_table_options));

  // Register an Env with object registry.
  const static char* kCustomEnvName = "CustomEnv";
  class CustomEnv : public EnvWrapper {
   public:
    explicit CustomEnv(Env* _target) : EnvWrapper(_target) {}
  };

  ObjectLibrary::Default()->Register<Env>(
      kCustomEnvName,
      [](const std::string& /*name*/, std::unique_ptr<Env>* /*env_guard*/,
         std::string* /* errmsg */) {
        static CustomEnv env(Env::Default());
        return &env;
      });

  ASSERT_OK(GetOptionsFromString(
      base_options,
      "write_buffer_size=10;max_write_buffer_number=16;"
      "block_based_table_factory={block_cache=1M;block_size=4;};"
      "compression_opts=4:5:6;create_if_missing=true;max_open_files=1;"
      "bottommost_compression_opts=5:6:7;create_if_missing=true;max_open_files="
      "1;"
      "rate_limiter_bytes_per_sec=1024;env=CustomEnv",
      &new_options));

  ASSERT_EQ(new_options.compression_opts.window_bits, 4);
  ASSERT_EQ(new_options.compression_opts.level, 5);
  ASSERT_EQ(new_options.compression_opts.strategy, 6);
  ASSERT_EQ(new_options.compression_opts.max_dict_bytes, 0u);
  ASSERT_EQ(new_options.compression_opts.zstd_max_train_bytes, 0u);
  ASSERT_EQ(new_options.compression_opts.parallel_threads, 1u);
  ASSERT_EQ(new_options.compression_opts.enabled, false);
  ASSERT_EQ(new_options.bottommost_compression, kDisableCompressionOption);
  ASSERT_EQ(new_options.bottommost_compression_opts.window_bits, 5);
  ASSERT_EQ(new_options.bottommost_compression_opts.level, 6);
  ASSERT_EQ(new_options.bottommost_compression_opts.strategy, 7);
  ASSERT_EQ(new_options.bottommost_compression_opts.max_dict_bytes, 0u);
  ASSERT_EQ(new_options.bottommost_compression_opts.zstd_max_train_bytes, 0u);
  ASSERT_EQ(new_options.bottommost_compression_opts.parallel_threads, 1u);
  ASSERT_EQ(new_options.bottommost_compression_opts.enabled, false);
  ASSERT_EQ(new_options.write_buffer_size, 10U);
  ASSERT_EQ(new_options.max_write_buffer_number, 16);

  auto new_block_based_table_options =
      new_options.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(new_block_based_table_options, nullptr);
  ASSERT_EQ(new_block_based_table_options->block_cache->GetCapacity(),
            1U << 20);
  ASSERT_EQ(new_block_based_table_options->block_size, 4U);
  // don't overwrite block based table options
  ASSERT_TRUE(new_block_based_table_options->cache_index_and_filter_blocks);

  ASSERT_EQ(new_options.create_if_missing, true);
  ASSERT_EQ(new_options.max_open_files, 1);
  ASSERT_TRUE(new_options.rate_limiter.get() != nullptr);
  Env* newEnv = new_options.env;
  ASSERT_OK(Env::LoadEnv(kCustomEnvName, &newEnv));
  ASSERT_EQ(newEnv, new_options.env);
}

TEST_F(OptionsOldApiTest, DBOptionsSerialization) {
  Options base_options, new_options;
  Random rnd(301);

  // Phase 1: Make big change in base_options
  test::RandomInitDBOptions(&base_options, &rnd);

  // Phase 2: obtain a string from base_option
  std::string base_options_file_content;
  ASSERT_OK(GetStringFromDBOptions(&base_options_file_content, base_options));

  // Phase 3: Set new_options from the derived string and expect
  //          new_options == base_options
  ASSERT_OK(GetDBOptionsFromString(DBOptions(), base_options_file_content,
                                   &new_options));
  ConfigOptions config_options;
  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(config_options, base_options, new_options));
}

TEST_F(OptionsOldApiTest, ColumnFamilyOptionsSerialization) {
  Options options;
  ColumnFamilyOptions base_opt, new_opt;
  Random rnd(302);
  // Phase 1: randomly assign base_opt
  // custom type options
  test::RandomInitCFOptions(&base_opt, options, &rnd);

  // Phase 2: obtain a string from base_opt
  std::string base_options_file_content;
  ASSERT_OK(
      GetStringFromColumnFamilyOptions(&base_options_file_content, base_opt));

  // Phase 3: Set new_opt from the derived string and expect
  //          new_opt == base_opt
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      ColumnFamilyOptions(), base_options_file_content, &new_opt));
  ConfigOptions config_options;
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, base_opt, new_opt));
  if (base_opt.compaction_filter) {
    delete base_opt.compaction_filter;
  }
}
#endif  // !ROCKSDB_LITE

#ifndef ROCKSDB_LITE
class OptionsParserTest : public testing::Test {
 public:
  OptionsParserTest() { fs_.reset(new test::StringFS(FileSystem::Default())); }

 protected:
  std::shared_ptr<test::StringFS> fs_;
};

TEST_F(OptionsParserTest, Comment) {
  DBOptions db_opt;
  db_opt.max_open_files = 12345;
  db_opt.max_background_flushes = 301;
  db_opt.max_total_wal_size = 1024;
  ColumnFamilyOptions cf_opt;

  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.14.0\n"
      "  options_file_version=1\n"
      "[ DBOptions ]\n"
      "  # note that we don't support space around \"=\"\n"
      "  max_open_files=12345;\n"
      "  max_background_flushes=301  # comment after a statement is fine\n"
      "  # max_background_flushes=1000  # this line would be ignored\n"
      "  # max_background_compactions=2000 # so does this one\n"
      "  max_total_wal_size=1024  # keep_log_file_num=1000\n"
      "[CFOptions   \"default\"]  # column family must be specified\n"
      "                     # in the correct order\n"
      "  # if a section is blank, we will use the default\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_OK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));

  ConfigOptions exact;
  exact.input_strings_escaped = false;
  exact.sanity_level = ConfigOptions::kSanityLevelExactMatch;
  ASSERT_OK(
      RocksDBOptionsParser::VerifyDBOptions(exact, *parser.db_opt(), db_opt));
  ASSERT_EQ(parser.NumColumnFamilies(), 1U);
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
      exact, *parser.GetCFOptions("default"), cf_opt));
}

TEST_F(OptionsParserTest, ExtraSpace) {
  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[      Version   ]\n"
      "  rocksdb_version     = 3.14.0      \n"
      "  options_file_version=1   # some comment\n"
      "[DBOptions  ]  # some comment\n"
      "max_open_files=12345   \n"
      "    max_background_flushes   =    301   \n"
      " max_total_wal_size     =   1024  # keep_log_file_num=1000\n"
      "        [CFOptions      \"default\"     ]\n"
      "  # if a section is blank, we will use the default\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_OK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));
}

TEST_F(OptionsParserTest, MissingDBOptions) {
  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.14.0\n"
      "  options_file_version=1\n"
      "[CFOptions \"default\"]\n"
      "  # if a section is blank, we will use the default\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_NOK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));
  ;
}

TEST_F(OptionsParserTest, DoubleDBOptions) {
  DBOptions db_opt;
  db_opt.max_open_files = 12345;
  db_opt.max_background_flushes = 301;
  db_opt.max_total_wal_size = 1024;
  ColumnFamilyOptions cf_opt;

  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.14.0\n"
      "  options_file_version=1\n"
      "[DBOptions]\n"
      "  max_open_files=12345\n"
      "  max_background_flushes=301\n"
      "  max_total_wal_size=1024  # keep_log_file_num=1000\n"
      "[DBOptions]\n"
      "[CFOptions \"default\"]\n"
      "  # if a section is blank, we will use the default\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_NOK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));
}

TEST_F(OptionsParserTest, NoDefaultCFOptions) {
  DBOptions db_opt;
  db_opt.max_open_files = 12345;
  db_opt.max_background_flushes = 301;
  db_opt.max_total_wal_size = 1024;
  ColumnFamilyOptions cf_opt;

  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.14.0\n"
      "  options_file_version=1\n"
      "[DBOptions]\n"
      "  max_open_files=12345\n"
      "  max_background_flushes=301\n"
      "  max_total_wal_size=1024  # keep_log_file_num=1000\n"
      "[CFOptions \"something_else\"]\n"
      "  # if a section is blank, we will use the default\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_NOK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));
}

TEST_F(OptionsParserTest, DefaultCFOptionsMustBeTheFirst) {
  DBOptions db_opt;
  db_opt.max_open_files = 12345;
  db_opt.max_background_flushes = 301;
  db_opt.max_total_wal_size = 1024;
  ColumnFamilyOptions cf_opt;

  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.14.0\n"
      "  options_file_version=1\n"
      "[DBOptions]\n"
      "  max_open_files=12345\n"
      "  max_background_flushes=301\n"
      "  max_total_wal_size=1024  # keep_log_file_num=1000\n"
      "[CFOptions \"something_else\"]\n"
      "  # if a section is blank, we will use the default\n"
      "[CFOptions \"default\"]\n"
      "  # if a section is blank, we will use the default\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_NOK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));
}

TEST_F(OptionsParserTest, DuplicateCFOptions) {
  DBOptions db_opt;
  db_opt.max_open_files = 12345;
  db_opt.max_background_flushes = 301;
  db_opt.max_total_wal_size = 1024;
  ColumnFamilyOptions cf_opt;

  std::string options_file_content =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.14.0\n"
      "  options_file_version=1\n"
      "[DBOptions]\n"
      "  max_open_files=12345\n"
      "  max_background_flushes=301\n"
      "  max_total_wal_size=1024  # keep_log_file_num=1000\n"
      "[CFOptions \"default\"]\n"
      "[CFOptions \"something_else\"]\n"
      "[CFOptions \"something_else\"]\n";

  const std::string kTestFileName = "test-rocksdb-options.ini";
  ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
  RocksDBOptionsParser parser;
  ASSERT_NOK(
      parser.Parse(kTestFileName, fs_.get(), false, 4096 /* readahead_size */));
}

TEST_F(OptionsParserTest, IgnoreUnknownOptions) {
  for (int case_id = 0; case_id < 5; case_id++) {
    DBOptions db_opt;
    db_opt.max_open_files = 12345;
    db_opt.max_background_flushes = 301;
    db_opt.max_total_wal_size = 1024;
    ColumnFamilyOptions cf_opt;

    std::string version_string;
    bool should_ignore = true;
    if (case_id == 0) {
      // same version
      should_ignore = false;
      version_string =
          ToString(ROCKSDB_MAJOR) + "." + ToString(ROCKSDB_MINOR) + ".0";
    } else if (case_id == 1) {
      // higher minor version
      should_ignore = true;
      version_string =
          ToString(ROCKSDB_MAJOR) + "." + ToString(ROCKSDB_MINOR + 1) + ".0";
    } else if (case_id == 2) {
      // higher major version.
      should_ignore = true;
      version_string = ToString(ROCKSDB_MAJOR + 1) + ".0.0";
    } else if (case_id == 3) {
      // lower minor version
#if ROCKSDB_MINOR == 0
      continue;
#else
      version_string =
          ToString(ROCKSDB_MAJOR) + "." + ToString(ROCKSDB_MINOR - 1) + ".0";
      should_ignore = false;
#endif
    } else {
      // lower major version
      should_ignore = false;
      version_string =
          ToString(ROCKSDB_MAJOR - 1) + "." + ToString(ROCKSDB_MINOR) + ".0";
    }

    std::string options_file_content =
        "# This is a testing option string.\n"
        "# Currently we only support \"#\" styled comment.\n"
        "\n"
        "[Version]\n"
        "  rocksdb_version=" +
        version_string +
        "\n"
        "  options_file_version=1\n"
        "[DBOptions]\n"
        "  max_open_files=12345\n"
        "  max_background_flushes=301\n"
        "  max_total_wal_size=1024  # keep_log_file_num=1000\n"
        "  unknown_db_option1=321\n"
        "  unknown_db_option2=false\n"
        "[CFOptions \"default\"]\n"
        "  unknown_cf_option1=hello\n"
        "[CFOptions \"something_else\"]\n"
        "  unknown_cf_option2=world\n"
        "  # if a section is blank, we will use the default\n";

    const std::string kTestFileName = "test-rocksdb-options.ini";
    auto s = fs_->FileExists(kTestFileName, IOOptions(), nullptr);
    ASSERT_TRUE(s.ok() || s.IsNotFound());
    if (s.ok()) {
      ASSERT_OK(fs_->DeleteFile(kTestFileName, IOOptions(), nullptr));
    }
    ASSERT_OK(fs_->WriteToNewFile(kTestFileName, options_file_content));
    RocksDBOptionsParser parser;
    ASSERT_NOK(parser.Parse(kTestFileName, fs_.get(), false,
                            4096 /* readahead_size */));
    if (should_ignore) {
      ASSERT_OK(parser.Parse(kTestFileName, fs_.get(),
                             true /* ignore_unknown_options */,
                             4096 /* readahead_size */));
    } else {
      ASSERT_NOK(parser.Parse(kTestFileName, fs_.get(),
                              true /* ignore_unknown_options */,
                              4096 /* readahead_size */));
    }
  }
}

TEST_F(OptionsParserTest, ParseVersion) {
  DBOptions db_opt;
  db_opt.max_open_files = 12345;
  db_opt.max_background_flushes = 301;
  db_opt.max_total_wal_size = 1024;
  ColumnFamilyOptions cf_opt;

  std::string file_template =
      "# This is a testing option string.\n"
      "# Currently we only support \"#\" styled comment.\n"
      "\n"
      "[Version]\n"
      "  rocksdb_version=3.13.1\n"
      "  options_file_version=%s\n"
      "[DBOptions]\n"
      "[CFOptions \"default\"]\n";
  const int kLength = 1000;
  char buffer[kLength];
  RocksDBOptionsParser parser;

  const std::vector<std::string> invalid_versions = {
      "a.b.c", "3.2.2b", "3.-12", "3. 1",  // only digits and dots are allowed
      "1.2.3.4",
      "1.2.3"  // can only contains at most one dot.
      "0",     // options_file_version must be at least one
      "3..2",
      ".", ".1.2",             // must have at least one digit before each dot
      "1.2.", "1.", "2.34."};  // must have at least one digit after each dot
  for (auto iv : invalid_versions) {
    snprintf(buffer, kLength - 1, file_template.c_str(), iv.c_str());

    parser.Reset();
    ASSERT_OK(fs_->WriteToNewFile(iv, buffer));
    ASSERT_NOK(parser.Parse(iv, fs_.get(), false, 0 /* readahead_size */));
  }

  const std::vector<std::string> valid_versions = {
      "1.232", "100", "3.12", "1", "12.3  ", "  1.25  "};
  for (auto vv : valid_versions) {
    snprintf(buffer, kLength - 1, file_template.c_str(), vv.c_str());
    parser.Reset();
    ASSERT_OK(fs_->WriteToNewFile(vv, buffer));
    ASSERT_OK(parser.Parse(vv, fs_.get(), false, 0 /* readahead_size */));
  }
}

void VerifyCFPointerTypedOptions(
    ColumnFamilyOptions* base_cf_opt, const ColumnFamilyOptions* new_cf_opt,
    const std::unordered_map<std::string, std::string>* new_cf_opt_map) {
  std::string name_buffer;
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(config_options, *base_cf_opt,
                                                  *new_cf_opt, new_cf_opt_map));

  // change the name of merge operator back-and-forth
  {
    auto* merge_operator = dynamic_cast<test::ChanglingMergeOperator*>(
        base_cf_opt->merge_operator.get());
    if (merge_operator != nullptr) {
      name_buffer = merge_operator->Name();
      // change the name  and expect non-ok status
      merge_operator->SetName("some-other-name");
      ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
      // change the name back and expect ok status
      merge_operator->SetName(name_buffer);
      ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
    }
  }

  // change the name of the compaction filter factory back-and-forth
  {
    auto* compaction_filter_factory =
        dynamic_cast<test::ChanglingCompactionFilterFactory*>(
            base_cf_opt->compaction_filter_factory.get());
    if (compaction_filter_factory != nullptr) {
      name_buffer = compaction_filter_factory->Name();
      // change the name and expect non-ok status
      compaction_filter_factory->SetName("some-other-name");
      ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
      // change the name back and expect ok status
      compaction_filter_factory->SetName(name_buffer);
      ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
    }
  }

  // test by setting compaction_filter to nullptr
  {
    auto* tmp_compaction_filter = base_cf_opt->compaction_filter;
    if (tmp_compaction_filter != nullptr) {
      base_cf_opt->compaction_filter = nullptr;
      // set compaction_filter to nullptr and expect non-ok status
      ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
      // set the value back and expect ok status
      base_cf_opt->compaction_filter = tmp_compaction_filter;
      ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
    }
  }

  // test by setting table_factory to nullptr
  {
    auto tmp_table_factory = base_cf_opt->table_factory;
    if (tmp_table_factory != nullptr) {
      base_cf_opt->table_factory.reset();
      // set table_factory to nullptr and expect non-ok status
      ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
      // set the value back and expect ok status
      base_cf_opt->table_factory = tmp_table_factory;
      ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
    }
  }

  // test by setting memtable_factory to nullptr
  {
    auto tmp_memtable_factory = base_cf_opt->memtable_factory;
    if (tmp_memtable_factory != nullptr) {
      base_cf_opt->memtable_factory.reset();
      // set memtable_factory to nullptr and expect non-ok status
      ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
      // set the value back and expect ok status
      base_cf_opt->memtable_factory = tmp_memtable_factory;
      ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
          config_options, *base_cf_opt, *new_cf_opt, new_cf_opt_map));
    }
  }
}

TEST_F(OptionsParserTest, Readahead) {
  DBOptions base_db_opt;
  std::vector<ColumnFamilyOptions> base_cf_opts;
  base_cf_opts.emplace_back();
  base_cf_opts.emplace_back();

  std::string one_mb_string = std::string(1024 * 1024, 'x');
  std::vector<std::string> cf_names = {"default", one_mb_string};
  const std::string kOptionsFileName = "test-persisted-options.ini";

  ASSERT_OK(PersistRocksDBOptions(base_db_opt, cf_names, base_cf_opts,
                                  kOptionsFileName, fs_.get()));

  uint64_t file_size = 0;
  ASSERT_OK(
      fs_->GetFileSize(kOptionsFileName, IOOptions(), &file_size, nullptr));
  assert(file_size > 0);

  RocksDBOptionsParser parser;

  fs_->num_seq_file_read_ = 0;
  size_t readahead_size = 128 * 1024;

  ASSERT_OK(parser.Parse(kOptionsFileName, fs_.get(), false, readahead_size));
  ASSERT_EQ(fs_->num_seq_file_read_.load(),
            (file_size - 1) / readahead_size + 1);

  fs_->num_seq_file_read_.store(0);
  readahead_size = 1024 * 1024;
  ASSERT_OK(parser.Parse(kOptionsFileName, fs_.get(), false, readahead_size));
  ASSERT_EQ(fs_->num_seq_file_read_.load(),
            (file_size - 1) / readahead_size + 1);

  // Tiny readahead. 8 KB is read each time.
  fs_->num_seq_file_read_.store(0);
  ASSERT_OK(
      parser.Parse(kOptionsFileName, fs_.get(), false, 1 /* readahead_size */));
  ASSERT_GE(fs_->num_seq_file_read_.load(), file_size / (8 * 1024));
  ASSERT_LT(fs_->num_seq_file_read_.load(), file_size / (8 * 1024) * 2);

  // Disable readahead means 512KB readahead.
  fs_->num_seq_file_read_.store(0);
  ASSERT_OK(
      parser.Parse(kOptionsFileName, fs_.get(), false, 0 /* readahead_size */));
  ASSERT_GE(fs_->num_seq_file_read_.load(), (file_size - 1) / (512 * 1024) + 1);
}

TEST_F(OptionsParserTest, DumpAndParse) {
  DBOptions base_db_opt;
  std::vector<ColumnFamilyOptions> base_cf_opts;
  std::vector<std::string> cf_names = {"default", "cf1", "cf2", "cf3",
                                       "c:f:4:4:4"
                                       "p\\i\\k\\a\\chu\\\\\\",
                                       "###rocksdb#1-testcf#2###"};
  const int num_cf = static_cast<int>(cf_names.size());
  Random rnd(302);
  test::RandomInitDBOptions(&base_db_opt, &rnd);
  base_db_opt.db_log_dir += "/#odd #but #could #happen #path #/\\\\#OMG";

  BlockBasedTableOptions special_bbto;
  special_bbto.cache_index_and_filter_blocks = true;
  special_bbto.block_size = 999999;

  for (int c = 0; c < num_cf; ++c) {
    ColumnFamilyOptions cf_opt;
    Random cf_rnd(0xFB + c);
    test::RandomInitCFOptions(&cf_opt, base_db_opt, &cf_rnd);
    if (c < 4) {
      cf_opt.prefix_extractor.reset(test::RandomSliceTransform(&rnd, c));
    }
    if (c < 3) {
      cf_opt.table_factory.reset(test::RandomTableFactory(&rnd, c));
    } else if (c == 4) {
      cf_opt.table_factory.reset(NewBlockBasedTableFactory(special_bbto));
    }
    base_cf_opts.emplace_back(cf_opt);
  }

  const std::string kOptionsFileName = "test-persisted-options.ini";
  // Use default for escaped(true), unknown(false) and check (exact)
  ConfigOptions config_options;
  ASSERT_OK(PersistRocksDBOptions(base_db_opt, cf_names, base_cf_opts,
                                  kOptionsFileName, fs_.get()));

  RocksDBOptionsParser parser;
  ASSERT_OK(parser.Parse(config_options, kOptionsFileName, fs_.get()));

  // Make sure block-based table factory options was deserialized correctly
  std::shared_ptr<TableFactory> ttf = (*parser.cf_opts())[4].table_factory;
  ASSERT_EQ(TableFactory::kBlockBasedTableName(), std::string(ttf->Name()));
  const auto parsed_bbto = ttf->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(parsed_bbto, nullptr);
  ASSERT_EQ(special_bbto.block_size, parsed_bbto->block_size);
  ASSERT_EQ(special_bbto.cache_index_and_filter_blocks,
            parsed_bbto->cache_index_and_filter_blocks);

  ASSERT_OK(RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
      config_options, base_db_opt, cf_names, base_cf_opts, kOptionsFileName,
      fs_.get()));

  ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(
      config_options, *parser.db_opt(), base_db_opt));
  for (int c = 0; c < num_cf; ++c) {
    const auto* cf_opt = parser.GetCFOptions(cf_names[c]);
    ASSERT_NE(cf_opt, nullptr);
    ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
        config_options, base_cf_opts[c], *cf_opt,
        &(parser.cf_opt_maps()->at(c))));
  }

  // Further verify pointer-typed options
  for (int c = 0; c < num_cf; ++c) {
    const auto* cf_opt = parser.GetCFOptions(cf_names[c]);
    ASSERT_NE(cf_opt, nullptr);
    VerifyCFPointerTypedOptions(&base_cf_opts[c], cf_opt,
                                &(parser.cf_opt_maps()->at(c)));
  }

  ASSERT_EQ(parser.GetCFOptions("does not exist"), nullptr);

  base_db_opt.max_open_files++;
  ASSERT_NOK(RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
      config_options, base_db_opt, cf_names, base_cf_opts, kOptionsFileName,
      fs_.get()));

  for (int c = 0; c < num_cf; ++c) {
    if (base_cf_opts[c].compaction_filter) {
      delete base_cf_opts[c].compaction_filter;
    }
  }
}

TEST_F(OptionsParserTest, DifferentDefault) {
  const std::string kOptionsFileName = "test-persisted-options.ini";

  ColumnFamilyOptions cf_level_opts;
  ASSERT_EQ(CompactionPri::kMinOverlappingRatio, cf_level_opts.compaction_pri);
  cf_level_opts.OptimizeLevelStyleCompaction();

  ColumnFamilyOptions cf_univ_opts;
  cf_univ_opts.OptimizeUniversalStyleCompaction();

  ASSERT_OK(PersistRocksDBOptions(DBOptions(), {"default", "universal"},
                                  {cf_level_opts, cf_univ_opts},
                                  kOptionsFileName, fs_.get()));

  RocksDBOptionsParser parser;
  ASSERT_OK(parser.Parse(kOptionsFileName, fs_.get(), false,
                         4096 /* readahead_size */));

  {
    Options old_default_opts;
    old_default_opts.OldDefaults();
    ASSERT_EQ(10 * 1048576, old_default_opts.max_bytes_for_level_base);
    ASSERT_EQ(5000, old_default_opts.max_open_files);
    ASSERT_EQ(2 * 1024U * 1024U, old_default_opts.delayed_write_rate);
    ASSERT_EQ(WALRecoveryMode::kTolerateCorruptedTailRecords,
              old_default_opts.wal_recovery_mode);
  }
  {
    Options old_default_opts;
    old_default_opts.OldDefaults(4, 6);
    ASSERT_EQ(10 * 1048576, old_default_opts.max_bytes_for_level_base);
    ASSERT_EQ(5000, old_default_opts.max_open_files);
  }
  {
    Options old_default_opts;
    old_default_opts.OldDefaults(4, 7);
    ASSERT_NE(10 * 1048576, old_default_opts.max_bytes_for_level_base);
    ASSERT_NE(4, old_default_opts.table_cache_numshardbits);
    ASSERT_EQ(5000, old_default_opts.max_open_files);
    ASSERT_EQ(2 * 1024U * 1024U, old_default_opts.delayed_write_rate);
  }
  {
    ColumnFamilyOptions old_default_cf_opts;
    old_default_cf_opts.OldDefaults();
    ASSERT_EQ(2 * 1048576, old_default_cf_opts.target_file_size_base);
    ASSERT_EQ(4 << 20, old_default_cf_opts.write_buffer_size);
    ASSERT_EQ(2 * 1048576, old_default_cf_opts.target_file_size_base);
    ASSERT_EQ(0, old_default_cf_opts.soft_pending_compaction_bytes_limit);
    ASSERT_EQ(0, old_default_cf_opts.hard_pending_compaction_bytes_limit);
    ASSERT_EQ(CompactionPri::kByCompensatedSize,
              old_default_cf_opts.compaction_pri);
  }
  {
    ColumnFamilyOptions old_default_cf_opts;
    old_default_cf_opts.OldDefaults(4, 6);
    ASSERT_EQ(2 * 1048576, old_default_cf_opts.target_file_size_base);
    ASSERT_EQ(CompactionPri::kByCompensatedSize,
              old_default_cf_opts.compaction_pri);
  }
  {
    ColumnFamilyOptions old_default_cf_opts;
    old_default_cf_opts.OldDefaults(4, 7);
    ASSERT_NE(2 * 1048576, old_default_cf_opts.target_file_size_base);
    ASSERT_EQ(CompactionPri::kByCompensatedSize,
              old_default_cf_opts.compaction_pri);
  }
  {
    Options old_default_opts;
    old_default_opts.OldDefaults(5, 1);
    ASSERT_EQ(2 * 1024U * 1024U, old_default_opts.delayed_write_rate);
  }
  {
    Options old_default_opts;
    old_default_opts.OldDefaults(5, 2);
    ASSERT_EQ(16 * 1024U * 1024U, old_default_opts.delayed_write_rate);
    ASSERT_TRUE(old_default_opts.compaction_pri ==
                CompactionPri::kByCompensatedSize);
  }
  {
    Options old_default_opts;
    old_default_opts.OldDefaults(5, 18);
    ASSERT_TRUE(old_default_opts.compaction_pri ==
                CompactionPri::kByCompensatedSize);
  }

  Options small_opts;
  small_opts.OptimizeForSmallDb();
  ASSERT_EQ(2 << 20, small_opts.write_buffer_size);
  ASSERT_EQ(5000, small_opts.max_open_files);
}

class OptionsSanityCheckTest : public OptionsParserTest {
 public:
  OptionsSanityCheckTest() {}

 protected:
  Status SanityCheckCFOptions(const ColumnFamilyOptions& cf_opts,
                              ConfigOptions::SanityLevel level,
                              bool input_strings_escaped = true) {
    ConfigOptions config_options;
    config_options.sanity_level = level;
    config_options.ignore_unknown_options = false;
    config_options.input_strings_escaped = input_strings_escaped;

    return RocksDBOptionsParser::VerifyRocksDBOptionsFromFile(
        config_options, DBOptions(), {"default"}, {cf_opts}, kOptionsFileName,
        fs_.get());
  }

  Status PersistCFOptions(const ColumnFamilyOptions& cf_opts) {
    Status s = fs_->DeleteFile(kOptionsFileName, IOOptions(), nullptr);
    if (!s.ok()) {
      return s;
    }
    return PersistRocksDBOptions(DBOptions(), {"default"}, {cf_opts},
                                 kOptionsFileName, fs_.get());
  }

  const std::string kOptionsFileName = "OPTIONS";
};

TEST_F(OptionsSanityCheckTest, SanityCheck) {
  ColumnFamilyOptions opts;
  Random rnd(301);

  // default ColumnFamilyOptions
  {
    ASSERT_OK(PersistCFOptions(opts));
    ASSERT_OK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
  }

  // prefix_extractor
  {
    // Okay to change prefix_extractor form nullptr to non-nullptr
    ASSERT_EQ(opts.prefix_extractor.get(), nullptr);
    opts.prefix_extractor.reset(NewCappedPrefixTransform(10));
    ASSERT_OK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

    // persist the change
    ASSERT_OK(PersistCFOptions(opts));
    ASSERT_OK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));

    // use same prefix extractor but with different parameter
    opts.prefix_extractor.reset(NewCappedPrefixTransform(15));
    // expect pass only in
    // ConfigOptions::kSanityLevelLooselyCompatible
    ASSERT_NOK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    ASSERT_OK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

    // repeat the test with FixedPrefixTransform
    opts.prefix_extractor.reset(NewFixedPrefixTransform(10));
    ASSERT_NOK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    ASSERT_OK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

    // persist the change of prefix_extractor
    ASSERT_OK(PersistCFOptions(opts));
    ASSERT_OK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));

    // use same prefix extractor but with different parameter
    opts.prefix_extractor.reset(NewFixedPrefixTransform(15));
    // expect pass only in
    // ConfigOptions::kSanityLevelLooselyCompatible
    ASSERT_NOK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    ASSERT_OK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

    // Change prefix extractor from non-nullptr to nullptr
    opts.prefix_extractor.reset();
    // expect pass as it's safe to change prefix_extractor
    // from non-null to null
    ASSERT_OK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));
  }
  // persist the change
  ASSERT_OK(PersistCFOptions(opts));
  ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));

  // table_factory
  {
    for (int tb = 0; tb <= 2; ++tb) {
      // change the table factory
      opts.table_factory.reset(test::RandomTableFactory(&rnd, tb));
      ASSERT_NOK(SanityCheckCFOptions(
          opts, ConfigOptions::kSanityLevelLooselyCompatible));
      ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

      // persist the change
      ASSERT_OK(PersistCFOptions(opts));
      ASSERT_OK(
          SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    }
  }

  // merge_operator
  {
    // Test when going from nullptr -> merge operator
    opts.merge_operator.reset(test::RandomMergeOperator(&rnd));
    ASSERT_OK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

    // persist the change
    ASSERT_OK(PersistCFOptions(opts));
    ASSERT_OK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));

    for (int test = 0; test < 5; ++test) {
      // change the merge operator
      opts.merge_operator.reset(test::RandomMergeOperator(&rnd));
      ASSERT_NOK(SanityCheckCFOptions(
          opts, ConfigOptions::kSanityLevelLooselyCompatible));
      ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

      // persist the change
      ASSERT_OK(PersistCFOptions(opts));
      ASSERT_OK(
          SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    }

    // Test when going from merge operator -> nullptr
    opts.merge_operator = nullptr;
    ASSERT_NOK(SanityCheckCFOptions(
        opts, ConfigOptions::kSanityLevelLooselyCompatible));
    ASSERT_OK(SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelNone));

    // persist the change
    ASSERT_OK(PersistCFOptions(opts));
    ASSERT_OK(
        SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
  }

  // compaction_filter
  {
    for (int test = 0; test < 5; ++test) {
      // change the compaction filter
      opts.compaction_filter = test::RandomCompactionFilter(&rnd);
      ASSERT_NOK(
          SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
      ASSERT_OK(SanityCheckCFOptions(
          opts, ConfigOptions::kSanityLevelLooselyCompatible));

      // persist the change
      ASSERT_OK(PersistCFOptions(opts));
      ASSERT_OK(
          SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
      delete opts.compaction_filter;
      opts.compaction_filter = nullptr;
    }
  }

  // compaction_filter_factory
  {
    for (int test = 0; test < 5; ++test) {
      // change the compaction filter factory
      opts.compaction_filter_factory.reset(
          test::RandomCompactionFilterFactory(&rnd));
      ASSERT_NOK(
          SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
      ASSERT_OK(SanityCheckCFOptions(
          opts, ConfigOptions::kSanityLevelLooselyCompatible));

      // persist the change
      ASSERT_OK(PersistCFOptions(opts));
      ASSERT_OK(
          SanityCheckCFOptions(opts, ConfigOptions::kSanityLevelExactMatch));
    }
  }
}

namespace {
bool IsEscapedString(const std::string& str) {
  for (size_t i = 0; i < str.size(); ++i) {
    if (str[i] == '\\') {
      // since we already handle those two consecutive '\'s in
      // the next if-then branch, any '\' appear at the end
      // of an escaped string in such case is not valid.
      if (i == str.size() - 1) {
        return false;
      }
      if (str[i + 1] == '\\') {
        // if there're two consecutive '\'s, skip the second one.
        i++;
        continue;
      }
      switch (str[i + 1]) {
        case ':':
        case '\\':
        case '#':
          continue;
        default:
          // if true, '\' together with str[i + 1] is not a valid escape.
          if (UnescapeChar(str[i + 1]) == str[i + 1]) {
            return false;
          }
      }
    } else if (isSpecialChar(str[i]) && (i == 0 || str[i - 1] != '\\')) {
      return false;
    }
  }
  return true;
}
}  // namespace

TEST_F(OptionsParserTest, IntegerParsing) {
  ASSERT_EQ(ParseUint64("18446744073709551615"), 18446744073709551615U);
  ASSERT_EQ(ParseUint32("4294967295"), 4294967295U);
  ASSERT_EQ(ParseSizeT("18446744073709551615"), 18446744073709551615U);
  ASSERT_EQ(ParseInt64("9223372036854775807"), 9223372036854775807);
  ASSERT_EQ(ParseInt64("-9223372036854775808"), port::kMinInt64);
  ASSERT_EQ(ParseInt32("2147483647"), 2147483647);
  ASSERT_EQ(ParseInt32("-2147483648"), port::kMinInt32);
  ASSERT_EQ(ParseInt("-32767"), -32767);
  ASSERT_EQ(ParseDouble("-1.234567"), -1.234567);
}

TEST_F(OptionsParserTest, EscapeOptionString) {
  ASSERT_EQ(UnescapeOptionString(
                "This is a test string with \\# \\: and \\\\ escape chars."),
            "This is a test string with # : and \\ escape chars.");

  ASSERT_EQ(
      EscapeOptionString("This is a test string with # : and \\ escape chars."),
      "This is a test string with \\# \\: and \\\\ escape chars.");

  std::string readible_chars =
      "A String like this \"1234567890-=_)(*&^%$#@!ertyuiop[]{POIU"
      "YTREWQasdfghjkl;':LKJHGFDSAzxcvbnm,.?>"
      "<MNBVCXZ\\\" should be okay to \\#\\\\\\:\\#\\#\\#\\ "
      "be serialized and deserialized";

  std::string escaped_string = EscapeOptionString(readible_chars);
  ASSERT_TRUE(IsEscapedString(escaped_string));
  // This two transformations should be canceled and should output
  // the original input.
  ASSERT_EQ(UnescapeOptionString(escaped_string), readible_chars);

  std::string all_chars;
  for (unsigned char c = 0;; ++c) {
    all_chars += c;
    if (c == 255) {
      break;
    }
  }
  escaped_string = EscapeOptionString(all_chars);
  ASSERT_TRUE(IsEscapedString(escaped_string));
  ASSERT_EQ(UnescapeOptionString(escaped_string), all_chars);

  ASSERT_EQ(RocksDBOptionsParser::TrimAndRemoveComment(
                "     A simple statement with a comment.  # like this :)"),
            "A simple statement with a comment.");

  ASSERT_EQ(RocksDBOptionsParser::TrimAndRemoveComment(
                "Escape \\# and # comment together   ."),
            "Escape \\# and");
}

static void TestAndCompareOption(const ConfigOptions& config_options,
                                 const OptionTypeInfo& opt_info,
                                 const std::string& opt_name, void* base_ptr,
                                 void* comp_ptr) {
  std::string result, mismatch;
  ASSERT_OK(opt_info.Serialize(config_options, opt_name, base_ptr, &result));
  ASSERT_OK(opt_info.Parse(config_options, opt_name, result, comp_ptr));
  ASSERT_TRUE(opt_info.AreEqual(config_options, opt_name, base_ptr, comp_ptr,
                                &mismatch));
}

static void TestAndCompareOption(const ConfigOptions& config_options,
                                 const OptionTypeInfo& opt_info,
                                 const std::string& opt_name,
                                 const std::string& opt_value, void* base_ptr,
                                 void* comp_ptr) {
  ASSERT_OK(opt_info.Parse(config_options, opt_name, opt_value, base_ptr));
  TestAndCompareOption(config_options, opt_info, opt_name, base_ptr, comp_ptr);
}

template <typename T>
void TestOptInfo(const ConfigOptions& config_options, OptionType opt_type,
                 T* base, T* comp) {
  std::string result;
  OptionTypeInfo opt_info(0, opt_type);
  ASSERT_FALSE(opt_info.AreEqual(config_options, "base", base, comp, &result));
  ASSERT_EQ(result, "base");
  ASSERT_NE(*base, *comp);
  TestAndCompareOption(config_options, opt_info, "base", base, comp);
  ASSERT_EQ(*base, *comp);
}

class OptionTypeInfoTest : public testing::Test {};

TEST_F(OptionTypeInfoTest, BasicTypes) {
  ConfigOptions config_options;
  {
    bool a = true, b = false;
    TestOptInfo(config_options, OptionType::kBoolean, &a, &b);
  }
  {
    int a = 100, b = 200;
    TestOptInfo(config_options, OptionType::kInt, &a, &b);
  }
  {
    int32_t a = 100, b = 200;
    TestOptInfo(config_options, OptionType::kInt32T, &a, &b);
  }
  {
    int64_t a = 100, b = 200;
    TestOptInfo(config_options, OptionType::kInt64T, &a, &b);
  }
  {
    unsigned int a = 100, b = 200;
    TestOptInfo(config_options, OptionType::kUInt, &a, &b);
  }
  {
    uint32_t a = 100, b = 200;
    TestOptInfo(config_options, OptionType::kUInt32T, &a, &b);
  }
  {
    uint64_t a = 100, b = 200;
    TestOptInfo(config_options, OptionType::kUInt64T, &a, &b);
  }
  {
    size_t a = 100, b = 200;
    TestOptInfo(config_options, OptionType::kSizeT, &a, &b);
  }
  {
    std::string a = "100", b = "200";
    TestOptInfo(config_options, OptionType::kString, &a, &b);
  }
  {
    double a = 1.0, b = 2.0;
    TestOptInfo(config_options, OptionType::kDouble, &a, &b);
  }
}

TEST_F(OptionTypeInfoTest, TestInvalidArgs) {
  ConfigOptions config_options;
  bool b;
  int i;
  int32_t i32;
  int64_t i64;
  unsigned int u;
  int32_t u32;
  int64_t u64;
  size_t sz;
  double d;

  ASSERT_NOK(OptionTypeInfo(0, OptionType::kBoolean)
                 .Parse(config_options, "b", "x", &b));
  ASSERT_NOK(
      OptionTypeInfo(0, OptionType::kInt).Parse(config_options, "b", "x", &i));
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kInt32T)
                 .Parse(config_options, "b", "x", &i32));
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kInt64T)
                 .Parse(config_options, "b", "x", &i64));
  ASSERT_NOK(
      OptionTypeInfo(0, OptionType::kUInt).Parse(config_options, "b", "x", &u));
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kUInt32T)
                 .Parse(config_options, "b", "x", &u32));
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kUInt64T)
                 .Parse(config_options, "b", "x", &u64));
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kSizeT)
                 .Parse(config_options, "b", "x", &sz));
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kDouble)
                 .Parse(config_options, "b", "x", &d));

  // Don't know how to convert Unknowns to anything else
  ASSERT_NOK(OptionTypeInfo(0, OptionType::kUnknown)
                 .Parse(config_options, "b", "x", &d));

  // Verify that if the parse function throws an exception, it is also trapped
  OptionTypeInfo func_info(0, OptionType::kUnknown,
                           OptionVerificationType::kNormal,
                           OptionTypeFlags::kNone,
                           [](const ConfigOptions&, const std::string&,
                              const std::string& value, char* addr) {
                             auto ptr = reinterpret_cast<int*>(addr);
                             *ptr = ParseInt(value);
                             return Status::OK();
                           });
  ASSERT_OK(func_info.Parse(config_options, "b", "1", &i));
  ASSERT_NOK(func_info.Parse(config_options, "b", "x", &i));
}

TEST_F(OptionTypeInfoTest, TestParseFunc) {
  OptionTypeInfo opt_info(
      0, OptionType::kUnknown, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone,
      [](const ConfigOptions& /*opts*/, const std::string& name,
         const std::string& value, char* addr) {
        auto ptr = reinterpret_cast<std::string*>(addr);
        if (name == "Oops") {
          return Status::InvalidArgument(value);
        } else {
          *ptr = value + " " + name;
          return Status::OK();
        }
      });
  ConfigOptions config_options;
  std::string base;
  ASSERT_OK(opt_info.Parse(config_options, "World", "Hello", &base));
  ASSERT_EQ(base, "Hello World");
  ASSERT_NOK(opt_info.Parse(config_options, "Oops", "Hello", &base));
}

TEST_F(OptionTypeInfoTest, TestSerializeFunc) {
  OptionTypeInfo opt_info(
      0, OptionType::kString, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, nullptr,
      [](const ConfigOptions& /*opts*/, const std::string& name,
         const char* /*addr*/, std::string* value) {
        if (name == "Oops") {
          return Status::InvalidArgument(name);
        } else {
          *value = name;
          return Status::OK();
        }
      },
      nullptr);
  ConfigOptions config_options;
  std::string base;
  std::string value;
  ASSERT_OK(opt_info.Serialize(config_options, "Hello", &base, &value));
  ASSERT_EQ(value, "Hello");
  ASSERT_NOK(opt_info.Serialize(config_options, "Oops", &base, &value));
}

TEST_F(OptionTypeInfoTest, TestEqualsFunc) {
  OptionTypeInfo opt_info(
      0, OptionType::kInt, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, nullptr, nullptr,
      [](const ConfigOptions& /*opts*/, const std::string& name,
         const char* addr1, const char* addr2, std::string* mismatch) {
        auto i1 = *(reinterpret_cast<const int*>(addr1));
        auto i2 = *(reinterpret_cast<const int*>(addr2));
        if (name == "LT") {
          return i1 < i2;
        } else if (name == "GT") {
          return i1 > i2;
        } else if (name == "EQ") {
          return i1 == i2;
        } else {
          *mismatch = name + "???";
          return false;
        }
      });

  ConfigOptions config_options;
  int int1 = 100;
  int int2 = 200;
  std::string mismatch;
  ASSERT_TRUE(opt_info.AreEqual(config_options, "LT", &int1, &int2, &mismatch));
  ASSERT_EQ(mismatch, "");
  ASSERT_FALSE(
      opt_info.AreEqual(config_options, "GT", &int1, &int2, &mismatch));
  ASSERT_EQ(mismatch, "GT");
  ASSERT_FALSE(
      opt_info.AreEqual(config_options, "NO", &int1, &int2, &mismatch));
  ASSERT_EQ(mismatch, "NO???");
}

TEST_F(OptionTypeInfoTest, TestOptionFlags) {
  OptionTypeInfo opt_none(0, OptionType::kString,
                          OptionVerificationType::kNormal,
                          OptionTypeFlags::kDontSerialize);
  OptionTypeInfo opt_never(0, OptionType::kString,
                           OptionVerificationType::kNormal,
                           OptionTypeFlags::kCompareNever);
  OptionTypeInfo opt_alias(0, OptionType::kString,
                           OptionVerificationType::kAlias,
                           OptionTypeFlags::kNone);
  OptionTypeInfo opt_deprecated(0, OptionType::kString,
                                OptionVerificationType::kDeprecated,
                                OptionTypeFlags::kNone);
  ConfigOptions config_options;
  std::string opts_str;
  std::string base = "base";
  std::string comp = "comp";

  // If marked string none, the serialization returns not supported
  ASSERT_NOK(opt_none.Serialize(config_options, "None", &base, &opts_str));
  // If marked never compare, they match even when they do not
  ASSERT_TRUE(opt_never.AreEqual(config_options, "Never", &base, &comp, &base));
  ASSERT_FALSE(opt_none.AreEqual(config_options, "Never", &base, &comp, &base));

  // An alias can change the value via parse, but does nothing on serialize on
  // match
  std::string result;
  ASSERT_OK(opt_alias.Parse(config_options, "Alias", "Alias",
                            reinterpret_cast<char*>(&base)));
  ASSERT_OK(opt_alias.Serialize(config_options, "Alias", &base, &result));
  ASSERT_TRUE(
      opt_alias.AreEqual(config_options, "Alias", &base, &comp, &result));
  ASSERT_EQ(base, "Alias");
  ASSERT_NE(base, comp);

  // Deprecated options do nothing on any of the commands
  ASSERT_OK(opt_deprecated.Parse(config_options, "Alias", "Deprecated", &base));
  ASSERT_OK(opt_deprecated.Serialize(config_options, "Alias", &base, &result));
  ASSERT_TRUE(
      opt_deprecated.AreEqual(config_options, "Alias", &base, &comp, &result));
  ASSERT_EQ(base, "Alias");
  ASSERT_NE(base, comp);
}

TEST_F(OptionTypeInfoTest, TestCustomEnum) {
  enum TestEnum { kA, kB, kC };
  std::unordered_map<std::string, TestEnum> enum_map = {
      {"A", TestEnum::kA},
      {"B", TestEnum::kB},
      {"C", TestEnum::kC},
  };
  OptionTypeInfo opt_info = OptionTypeInfo::Enum<TestEnum>(0, &enum_map);
  TestEnum e1, e2;
  ConfigOptions config_options;
  std::string result, mismatch;

  e2 = TestEnum::kA;

  ASSERT_OK(opt_info.Parse(config_options, "", "B", &e1));
  ASSERT_OK(opt_info.Serialize(config_options, "", &e1, &result));
  ASSERT_EQ(e1, TestEnum::kB);
  ASSERT_EQ(result, "B");

  ASSERT_FALSE(opt_info.AreEqual(config_options, "Enum", &e1, &e2, &mismatch));
  ASSERT_EQ(mismatch, "Enum");

  TestAndCompareOption(config_options, opt_info, "", "C", &e1, &e2);
  ASSERT_EQ(e2, TestEnum::kC);

  ASSERT_NOK(opt_info.Parse(config_options, "", "D", &e1));
  ASSERT_EQ(e1, TestEnum::kC);
}

TEST_F(OptionTypeInfoTest, TestBuiltinEnum) {
  ConfigOptions config_options;
  for (auto iter : OptionsHelper::compaction_style_string_map) {
    CompactionStyle e1, e2;
    TestAndCompareOption(config_options,
                         OptionTypeInfo(0, OptionType::kCompactionStyle),
                         "CompactionStyle", iter.first, &e1, &e2);
    ASSERT_EQ(e1, iter.second);
  }
  for (auto iter : OptionsHelper::compaction_pri_string_map) {
    CompactionPri e1, e2;
    TestAndCompareOption(config_options,
                         OptionTypeInfo(0, OptionType::kCompactionPri),
                         "CompactionPri", iter.first, &e1, &e2);
    ASSERT_EQ(e1, iter.second);
  }
  for (auto iter : OptionsHelper::compression_type_string_map) {
    CompressionType e1, e2;
    TestAndCompareOption(config_options,
                         OptionTypeInfo(0, OptionType::kCompressionType),
                         "CompressionType", iter.first, &e1, &e2);
    ASSERT_EQ(e1, iter.second);
  }
  for (auto iter : OptionsHelper::compaction_stop_style_string_map) {
    CompactionStopStyle e1, e2;
    TestAndCompareOption(config_options,
                         OptionTypeInfo(0, OptionType::kCompactionStopStyle),
                         "CompactionStopStyle", iter.first, &e1, &e2);
    ASSERT_EQ(e1, iter.second);
  }
  for (auto iter : OptionsHelper::checksum_type_string_map) {
    ChecksumType e1, e2;
    TestAndCompareOption(config_options,
                         OptionTypeInfo(0, OptionType::kChecksumType),
                         "CheckSumType", iter.first, &e1, &e2);
    ASSERT_EQ(e1, iter.second);
  }
  for (auto iter : OptionsHelper::encoding_type_string_map) {
    EncodingType e1, e2;
    TestAndCompareOption(config_options,
                         OptionTypeInfo(0, OptionType::kEncodingType),
                         "EncodingType", iter.first, &e1, &e2);
    ASSERT_EQ(e1, iter.second);
  }
}

TEST_F(OptionTypeInfoTest, TestStruct) {
  struct Basic {
    int i = 42;
    std::string s = "Hello";
  };

  struct Extended {
    int j = 11;
    Basic b;
  };

  std::unordered_map<std::string, OptionTypeInfo> basic_type_map = {
      {"i", {offsetof(struct Basic, i), OptionType::kInt}},
      {"s", {offsetof(struct Basic, s), OptionType::kString}},
  };
  OptionTypeInfo basic_info = OptionTypeInfo::Struct(
      "b", &basic_type_map, 0, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable);

  std::unordered_map<std::string, OptionTypeInfo> extended_type_map = {
      {"j", {offsetof(struct Extended, j), OptionType::kInt}},
      {"b", OptionTypeInfo::Struct(
                "b", &basic_type_map, offsetof(struct Extended, b),
                OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
      {"m", OptionTypeInfo::Struct(
                "m", &basic_type_map, offsetof(struct Extended, b),
                OptionVerificationType::kNormal, OptionTypeFlags::kMutable)},
  };
  OptionTypeInfo extended_info = OptionTypeInfo::Struct(
      "e", &extended_type_map, 0, OptionVerificationType::kNormal,
      OptionTypeFlags::kMutable);
  Extended e1, e2;
  ConfigOptions config_options;
  std::string mismatch;
  TestAndCompareOption(config_options, basic_info, "b", "{i=33;s=33}", &e1.b,
                       &e2.b);
  ASSERT_EQ(e1.b.i, 33);
  ASSERT_EQ(e1.b.s, "33");

  TestAndCompareOption(config_options, basic_info, "b.i", "44", &e1.b, &e2.b);
  ASSERT_EQ(e1.b.i, 44);

  TestAndCompareOption(config_options, basic_info, "i", "55", &e1.b, &e2.b);
  ASSERT_EQ(e1.b.i, 55);

  e1.b.i = 0;

  ASSERT_FALSE(
      basic_info.AreEqual(config_options, "b", &e1.b, &e2.b, &mismatch));
  ASSERT_EQ(mismatch, "b.i");
  mismatch.clear();
  ASSERT_FALSE(
      basic_info.AreEqual(config_options, "b.i", &e1.b, &e2.b, &mismatch));
  ASSERT_EQ(mismatch, "b.i");
  mismatch.clear();
  ASSERT_FALSE(
      basic_info.AreEqual(config_options, "i", &e1.b, &e2.b, &mismatch));
  ASSERT_EQ(mismatch, "b.i");
  mismatch.clear();

  e1 = e2;
  ASSERT_NOK(basic_info.Parse(config_options, "b", "{i=33;s=33;j=44}", &e1.b));
  ASSERT_NOK(basic_info.Parse(config_options, "b.j", "44", &e1.b));
  ASSERT_NOK(basic_info.Parse(config_options, "j", "44", &e1.b));

  TestAndCompareOption(config_options, extended_info, "e",
                       "b={i=55;s=55}; j=22;", &e1, &e2);
  ASSERT_EQ(e1.b.i, 55);
  ASSERT_EQ(e1.j, 22);
  ASSERT_EQ(e1.b.s, "55");
  TestAndCompareOption(config_options, extended_info, "e.b", "{i=66;s=66;}",
                       &e1, &e2);
  ASSERT_EQ(e1.b.i, 66);
  ASSERT_EQ(e1.j, 22);
  ASSERT_EQ(e1.b.s, "66");
  TestAndCompareOption(config_options, extended_info, "e.b.i", "77", &e1, &e2);
  ASSERT_EQ(e1.b.i, 77);
  ASSERT_EQ(e1.j, 22);
  ASSERT_EQ(e1.b.s, "66");
}

TEST_F(OptionTypeInfoTest, TestVectorType) {
  OptionTypeInfo vec_info = OptionTypeInfo::Vector<std::string>(
      0, OptionVerificationType::kNormal, OptionTypeFlags::kNone,
      {0, OptionType::kString});
  std::vector<std::string> vec1, vec2;
  std::string mismatch;

  ConfigOptions config_options;
  TestAndCompareOption(config_options, vec_info, "v", "a:b:c:d", &vec1, &vec2);
  ASSERT_EQ(vec1.size(), 4);
  ASSERT_EQ(vec1[0], "a");
  ASSERT_EQ(vec1[1], "b");
  ASSERT_EQ(vec1[2], "c");
  ASSERT_EQ(vec1[3], "d");
  vec1[3] = "e";
  ASSERT_FALSE(vec_info.AreEqual(config_options, "v", &vec1, &vec2, &mismatch));
  ASSERT_EQ(mismatch, "v");

  // Test vectors with inner brackets
  TestAndCompareOption(config_options, vec_info, "v", "a:{b}:c:d", &vec1,
                       &vec2);
  ASSERT_EQ(vec1.size(), 4);
  ASSERT_EQ(vec1[0], "a");
  ASSERT_EQ(vec1[1], "b");
  ASSERT_EQ(vec1[2], "c");
  ASSERT_EQ(vec1[3], "d");

  OptionTypeInfo bar_info = OptionTypeInfo::Vector<std::string>(
      0, OptionVerificationType::kNormal, OptionTypeFlags::kNone,
      {0, OptionType::kString}, '|');
  TestAndCompareOption(config_options, vec_info, "v", "x|y|z", &vec1, &vec2);
  // Test vectors with inner vector
  TestAndCompareOption(config_options, bar_info, "v",
                       "a|{b1|b2}|{c1|c2|{d1|d2}}", &vec1, &vec2);
  ASSERT_EQ(vec1.size(), 3);
  ASSERT_EQ(vec1[0], "a");
  ASSERT_EQ(vec1[1], "b1|b2");
  ASSERT_EQ(vec1[2], "c1|c2|{d1|d2}");
}
#endif  // !ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
#ifdef GFLAGS
  ParseCommandLineFlags(&argc, &argv, true);
#endif  // GFLAGS
  return RUN_ALL_TESTS();
}
