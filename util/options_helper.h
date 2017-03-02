// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/cf_options.h"
#include "util/db_options.h"

namespace rocksdb {

DBOptions BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                         const MutableDBOptions& mutable_db_options);

ColumnFamilyOptions BuildColumnFamilyOptions(
    const ColumnFamilyOptions& ioptions,
    const MutableCFOptions& mutable_cf_options);

static std::map<CompactionStyle, std::string> compaction_style_to_string = {
    {kCompactionStyleLevel, "kCompactionStyleLevel"},
    {kCompactionStyleUniversal, "kCompactionStyleUniversal"},
    {kCompactionStyleFIFO, "kCompactionStyleFIFO"},
    {kCompactionStyleNone, "kCompactionStyleNone"}};

static std::map<CompactionPri, std::string> compaction_pri_to_string = {
    {kByCompensatedSize, "kByCompensatedSize"},
    {kOldestLargestSeqFirst, "kOldestLargestSeqFirst"},
    {kOldestSmallestSeqFirst, "kOldestSmallestSeqFirst"},
    {kMinOverlappingRatio, "kMinOverlappingRatio"}};

#ifndef ROCKSDB_LITE

// Returns true if the input char "c" is considered as a special character
// that will be escaped when EscapeOptionString() is called.
//
// @param c the input char
// @return true if the input char "c" is considered as a special character.
// @see EscapeOptionString
bool isSpecialChar(const char c);

// If the input char is an escaped char, it will return the its
// associated raw-char.  Otherwise, the function will simply return
// the original input char.
char UnescapeChar(const char c);

// If the input char is a control char, it will return the its
// associated escaped char.  Otherwise, the function will simply return
// the original input char.
char EscapeChar(const char c);

// Converts a raw string to an escaped string.  Escaped-characters are
// defined via the isSpecialChar() function.  When a char in the input
// string "raw_string" is classified as a special characters, then it
// will be prefixed by '\' in the output.
//
// It's inverse function is UnescapeOptionString().
// @param raw_string the input string
// @return the '\' escaped string of the input "raw_string"
// @see isSpecialChar, UnescapeOptionString
std::string EscapeOptionString(const std::string& raw_string);

// The inverse function of EscapeOptionString.  It converts
// an '\' escaped string back to a raw string.
//
// @param escaped_string the input '\' escaped string
// @return the raw string of the input "escaped_string"
std::string UnescapeOptionString(const std::string& escaped_string);

uint64_t ParseUint64(const std::string& value);

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableCFOptions* new_options);

Status GetMutableDBOptionsFromStrings(
    const MutableDBOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableDBOptions* new_options);

Status GetTableFactoryFromMap(
    const std::string& factory_name,
    const std::unordered_map<std::string, std::string>& opt_map,
    std::shared_ptr<TableFactory>* table_factory);

Status GetStringFromTableFactory(std::string* opts_str, const TableFactory* tf,
                                 const std::string& delimiter = ";  ");

enum class OptionType {
  kBoolean,
  kInt,
  kVectorInt,
  kUInt,
  kUInt32T,
  kUInt64T,
  kSizeT,
  kString,
  kDouble,
  kCompactionStyle,
  kCompactionPri,
  kSliceTransform,
  kCompressionType,
  kVectorCompressionType,
  kTableFactory,
  kComparator,
  kCompactionFilter,
  kCompactionFilterFactory,
  kMergeOperator,
  kMemTableRepFactory,
  kBlockBasedTableIndexType,
  kFilterPolicy,
  kFlushBlockPolicyFactory,
  kChecksumType,
  kEncodingType,
  kWALRecoveryMode,
  kAccessHint,
  kInfoLogLevel,
  kUnknown
};

enum class OptionVerificationType {
  kNormal,
  kByName,           // The option is pointer typed so we can only verify
                     // based on it's name.
  kByNameAllowNull,  // Same as kByName, but it also allows the case
                     // where one of them is a nullptr.
  kDeprecated        // The option is no longer used in rocksdb. The RocksDB
                     // OptionsParser will still accept this option if it
                     // happen to exists in some Options file.  However, the
                     // parser will not include it in serialization and
                     // verification processes.
};

// A struct for storing constant option information such as option name,
// option type, and offset.
struct OptionTypeInfo {
  int offset;
  OptionType type;
  OptionVerificationType verification;
  bool is_mutable;
  int mutable_offset;
};

// A helper function that converts "opt_address" to a std::string
// based on the specified OptionType.
bool SerializeSingleOptionHelper(const char* opt_address,
                                 const OptionType opt_type, std::string* value);

// In addition to its public version defined in rocksdb/convenience.h,
// this further takes an optional output vector "unsupported_options_names",
// which stores the name of all the unsupported options specified in "opts_map".
Status GetDBOptionsFromMapInternal(
    const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    DBOptions* new_options, bool input_strings_escaped,
    std::vector<std::string>* unsupported_options_names = nullptr);

// In addition to its public version defined in rocksdb/convenience.h,
// this further takes an optional output vector "unsupported_options_names",
// which stores the name of all the unsupported options specified in "opts_map".
Status GetColumnFamilyOptionsFromMapInternal(
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options, bool input_strings_escaped,
    std::vector<std::string>* unsupported_options_names = nullptr);

static std::unordered_map<std::string, OptionTypeInfo> db_options_type_info = {
    /*
     // not yet supported
      Env* env;
      std::shared_ptr<Cache> row_cache;
      std::shared_ptr<DeleteScheduler> delete_scheduler;
      std::shared_ptr<Logger> info_log;
      std::shared_ptr<RateLimiter> rate_limiter;
      std::shared_ptr<Statistics> statistics;
      std::vector<DbPath> db_paths;
      std::vector<std::shared_ptr<EventListener>> listeners;
     */
    {"advise_random_on_open",
     {offsetof(struct DBOptions, advise_random_on_open), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"allow_mmap_reads",
     {offsetof(struct DBOptions, allow_mmap_reads), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"allow_fallocate",
     {offsetof(struct DBOptions, allow_fallocate), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"allow_mmap_writes",
     {offsetof(struct DBOptions, allow_mmap_writes), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"use_direct_reads",
     {offsetof(struct DBOptions, use_direct_reads), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"use_direct_writes",
     {offsetof(struct DBOptions, use_direct_writes), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"allow_2pc",
     {offsetof(struct DBOptions, allow_2pc), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"allow_os_buffer",
     {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, true, 0}},
    {"create_if_missing",
     {offsetof(struct DBOptions, create_if_missing), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"create_missing_column_families",
     {offsetof(struct DBOptions, create_missing_column_families),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"disableDataSync",
     {offsetof(struct DBOptions, disableDataSync), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"disable_data_sync",  // for compatibility
     {offsetof(struct DBOptions, disableDataSync), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"enable_thread_tracking",
     {offsetof(struct DBOptions, enable_thread_tracking), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"error_if_exists",
     {offsetof(struct DBOptions, error_if_exists), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"is_fd_close_on_exec",
     {offsetof(struct DBOptions, is_fd_close_on_exec), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"paranoid_checks",
     {offsetof(struct DBOptions, paranoid_checks), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"skip_log_error_on_recovery",
     {offsetof(struct DBOptions, skip_log_error_on_recovery),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"skip_stats_update_on_db_open",
     {offsetof(struct DBOptions, skip_stats_update_on_db_open),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"new_table_reader_for_compaction_inputs",
     {offsetof(struct DBOptions, new_table_reader_for_compaction_inputs),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"compaction_readahead_size",
     {offsetof(struct DBOptions, compaction_readahead_size), OptionType::kSizeT,
      OptionVerificationType::kNormal, false, 0}},
    {"random_access_max_buffer_size",
     {offsetof(struct DBOptions, random_access_max_buffer_size),
      OptionType::kSizeT, OptionVerificationType::kNormal, false, 0}},
    {"writable_file_max_buffer_size",
     {offsetof(struct DBOptions, writable_file_max_buffer_size),
      OptionType::kSizeT, OptionVerificationType::kNormal, false, 0}},
    {"use_adaptive_mutex",
     {offsetof(struct DBOptions, use_adaptive_mutex), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"use_fsync",
     {offsetof(struct DBOptions, use_fsync), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"max_background_compactions",
     {offsetof(struct DBOptions, max_background_compactions), OptionType::kInt,
      OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, max_background_compactions)}},
    {"base_background_compactions",
     {offsetof(struct DBOptions, base_background_compactions), OptionType::kInt,
      OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, base_background_compactions)}},
    {"max_background_flushes",
     {offsetof(struct DBOptions, max_background_flushes), OptionType::kInt,
      OptionVerificationType::kNormal, false, 0}},
    {"max_file_opening_threads",
     {offsetof(struct DBOptions, max_file_opening_threads), OptionType::kInt,
      OptionVerificationType::kNormal, false, 0}},
    {"max_open_files",
     {offsetof(struct DBOptions, max_open_files), OptionType::kInt,
      OptionVerificationType::kNormal, false, 0}},
    {"table_cache_numshardbits",
     {offsetof(struct DBOptions, table_cache_numshardbits), OptionType::kInt,
      OptionVerificationType::kNormal, false, 0}},
    {"db_write_buffer_size",
     {offsetof(struct DBOptions, db_write_buffer_size), OptionType::kSizeT,
      OptionVerificationType::kNormal, false, 0}},
    {"keep_log_file_num",
     {offsetof(struct DBOptions, keep_log_file_num), OptionType::kSizeT,
      OptionVerificationType::kNormal, false, 0}},
    {"recycle_log_file_num",
     {offsetof(struct DBOptions, recycle_log_file_num), OptionType::kSizeT,
      OptionVerificationType::kNormal, false, 0}},
    {"log_file_time_to_roll",
     {offsetof(struct DBOptions, log_file_time_to_roll), OptionType::kSizeT,
      OptionVerificationType::kNormal, false, 0}},
    {"manifest_preallocation_size",
     {offsetof(struct DBOptions, manifest_preallocation_size),
      OptionType::kSizeT, OptionVerificationType::kNormal, false, 0}},
    {"max_log_file_size",
     {offsetof(struct DBOptions, max_log_file_size), OptionType::kSizeT,
      OptionVerificationType::kNormal, false, 0}},
    {"db_log_dir",
     {offsetof(struct DBOptions, db_log_dir), OptionType::kString,
      OptionVerificationType::kNormal, false, 0}},
    {"wal_dir",
     {offsetof(struct DBOptions, wal_dir), OptionType::kString,
      OptionVerificationType::kNormal, false, 0}},
    {"max_subcompactions",
     {offsetof(struct DBOptions, max_subcompactions), OptionType::kUInt32T,
      OptionVerificationType::kNormal, false, 0}},
    {"WAL_size_limit_MB",
     {offsetof(struct DBOptions, WAL_size_limit_MB), OptionType::kUInt64T,
      OptionVerificationType::kNormal, false, 0}},
    {"WAL_ttl_seconds",
     {offsetof(struct DBOptions, WAL_ttl_seconds), OptionType::kUInt64T,
      OptionVerificationType::kNormal, false, 0}},
    {"bytes_per_sync",
     {offsetof(struct DBOptions, bytes_per_sync), OptionType::kUInt64T,
      OptionVerificationType::kNormal, false, 0}},
    {"delayed_write_rate",
     {offsetof(struct DBOptions, delayed_write_rate), OptionType::kUInt64T,
      OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, delayed_write_rate)}},
    {"delete_obsolete_files_period_micros",
     {offsetof(struct DBOptions, delete_obsolete_files_period_micros),
      OptionType::kUInt64T, OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, delete_obsolete_files_period_micros)}},
    {"max_manifest_file_size",
     {offsetof(struct DBOptions, max_manifest_file_size), OptionType::kUInt64T,
      OptionVerificationType::kNormal, false, 0}},
    {"max_total_wal_size",
     {offsetof(struct DBOptions, max_total_wal_size), OptionType::kUInt64T,
      OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, max_total_wal_size)}},
    {"wal_bytes_per_sync",
     {offsetof(struct DBOptions, wal_bytes_per_sync), OptionType::kUInt64T,
      OptionVerificationType::kNormal, false, 0}},
    {"stats_dump_period_sec",
     {offsetof(struct DBOptions, stats_dump_period_sec), OptionType::kUInt,
      OptionVerificationType::kNormal, false, 0}},
    {"fail_if_options_file_error",
     {offsetof(struct DBOptions, fail_if_options_file_error),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"allow_concurrent_memtable_write",
     {offsetof(struct DBOptions, allow_concurrent_memtable_write),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"wal_recovery_mode",
     {offsetof(struct DBOptions, wal_recovery_mode),
      OptionType::kWALRecoveryMode, OptionVerificationType::kNormal, false, 0}},
    {"enable_write_thread_adaptive_yield",
     {offsetof(struct DBOptions, enable_write_thread_adaptive_yield),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"write_thread_slow_yield_usec",
     {offsetof(struct DBOptions, write_thread_slow_yield_usec),
      OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
    {"write_thread_max_yield_usec",
     {offsetof(struct DBOptions, write_thread_max_yield_usec),
      OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
    {"access_hint_on_compaction_start",
     {offsetof(struct DBOptions, access_hint_on_compaction_start),
      OptionType::kAccessHint, OptionVerificationType::kNormal, false, 0}},
    {"info_log_level",
     {offsetof(struct DBOptions, info_log_level), OptionType::kInfoLogLevel,
      OptionVerificationType::kNormal, false, 0}},
    {"dump_malloc_stats",
     {offsetof(struct DBOptions, dump_malloc_stats), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"avoid_flush_during_recovery",
     {offsetof(struct DBOptions, avoid_flush_during_recovery),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"avoid_flush_during_shutdown",
     {offsetof(struct DBOptions, avoid_flush_during_shutdown),
      OptionType::kBoolean, OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, avoid_flush_during_shutdown)}}};

static std::unordered_map<std::string, OptionTypeInfo> cf_options_type_info = {
    /* not yet supported
    CompactionOptionsFIFO compaction_options_fifo;
    CompactionOptionsUniversal compaction_options_universal;
    CompressionOptions compression_opts;
    TablePropertiesCollectorFactories table_properties_collector_factories;
    typedef std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
        TablePropertiesCollectorFactories;
    UpdateStatus (*inplace_callback)(char* existing_value,
                                     uint34_t* existing_value_size,
                                     Slice delta_value,
                                     std::string* merged_value);
     */
    {"report_bg_io_stats",
     {offsetof(struct ColumnFamilyOptions, report_bg_io_stats),
      OptionType::kBoolean, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, report_bg_io_stats)}},
    {"compaction_measure_io_stats",
     {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false, 0}},
    {"disable_auto_compactions",
     {offsetof(struct ColumnFamilyOptions, disable_auto_compactions),
      OptionType::kBoolean, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, disable_auto_compactions)}},
    {"filter_deletes",
     {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, true, 0}},
    {"inplace_update_support",
     {offsetof(struct ColumnFamilyOptions, inplace_update_support),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"level_compaction_dynamic_level_bytes",
     {offsetof(struct ColumnFamilyOptions,
               level_compaction_dynamic_level_bytes),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"optimize_filters_for_hits",
     {offsetof(struct ColumnFamilyOptions, optimize_filters_for_hits),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"paranoid_file_checks",
     {offsetof(struct ColumnFamilyOptions, paranoid_file_checks),
      OptionType::kBoolean, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, paranoid_file_checks)}},
    {"force_consistency_checks",
     {offsetof(struct ColumnFamilyOptions, force_consistency_checks),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"purge_redundant_kvs_while_flush",
     {offsetof(struct ColumnFamilyOptions, purge_redundant_kvs_while_flush),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"verify_checksums_in_compaction",
     {offsetof(struct ColumnFamilyOptions, verify_checksums_in_compaction),
      OptionType::kBoolean, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, verify_checksums_in_compaction)}},
    {"soft_pending_compaction_bytes_limit",
     {offsetof(struct ColumnFamilyOptions, soft_pending_compaction_bytes_limit),
      OptionType::kUInt64T, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, soft_pending_compaction_bytes_limit)}},
    {"hard_pending_compaction_bytes_limit",
     {offsetof(struct ColumnFamilyOptions, hard_pending_compaction_bytes_limit),
      OptionType::kUInt64T, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, hard_pending_compaction_bytes_limit)}},
    {"hard_rate_limit",
     {0, OptionType::kDouble, OptionVerificationType::kDeprecated, true, 0}},
    {"soft_rate_limit",
     {0, OptionType::kDouble, OptionVerificationType::kDeprecated, true, 0}},
    {"max_compaction_bytes",
     {offsetof(struct ColumnFamilyOptions, max_compaction_bytes),
      OptionType::kUInt64T, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, max_compaction_bytes)}},
    {"expanded_compaction_factor",
     {0, OptionType::kInt, OptionVerificationType::kDeprecated, true, 0}},
    {"level0_file_num_compaction_trigger",
     {offsetof(struct ColumnFamilyOptions, level0_file_num_compaction_trigger),
      OptionType::kInt, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, level0_file_num_compaction_trigger)}},
    {"level0_slowdown_writes_trigger",
     {offsetof(struct ColumnFamilyOptions, level0_slowdown_writes_trigger),
      OptionType::kInt, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, level0_slowdown_writes_trigger)}},
    {"level0_stop_writes_trigger",
     {offsetof(struct ColumnFamilyOptions, level0_stop_writes_trigger),
      OptionType::kInt, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, level0_stop_writes_trigger)}},
    {"max_grandparent_overlap_factor",
     {0, OptionType::kInt, OptionVerificationType::kDeprecated, true, 0}},
    {"max_mem_compaction_level",
     {0, OptionType::kInt, OptionVerificationType::kDeprecated, false, 0}},
    {"max_write_buffer_number",
     {offsetof(struct ColumnFamilyOptions, max_write_buffer_number),
      OptionType::kInt, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, max_write_buffer_number)}},
    {"max_write_buffer_number_to_maintain",
     {offsetof(struct ColumnFamilyOptions, max_write_buffer_number_to_maintain),
      OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
    {"min_write_buffer_number_to_merge",
     {offsetof(struct ColumnFamilyOptions, min_write_buffer_number_to_merge),
      OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
    {"num_levels",
     {offsetof(struct ColumnFamilyOptions, num_levels), OptionType::kInt,
      OptionVerificationType::kNormal, false, 0}},
    {"source_compaction_factor",
     {0, OptionType::kInt, OptionVerificationType::kDeprecated, true, 0}},
    {"target_file_size_multiplier",
     {offsetof(struct ColumnFamilyOptions, target_file_size_multiplier),
      OptionType::kInt, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, target_file_size_multiplier)}},
    {"arena_block_size",
     {offsetof(struct ColumnFamilyOptions, arena_block_size),
      OptionType::kSizeT, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, arena_block_size)}},
    {"inplace_update_num_locks",
     {offsetof(struct ColumnFamilyOptions, inplace_update_num_locks),
      OptionType::kSizeT, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, inplace_update_num_locks)}},
    {"max_successive_merges",
     {offsetof(struct ColumnFamilyOptions, max_successive_merges),
      OptionType::kSizeT, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, max_successive_merges)}},
    {"memtable_huge_page_size",
     {offsetof(struct ColumnFamilyOptions, memtable_huge_page_size),
      OptionType::kSizeT, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, memtable_huge_page_size)}},
    {"memtable_prefix_bloom_huge_page_tlb_size",
     {0, OptionType::kSizeT, OptionVerificationType::kDeprecated, true, 0}},
    {"write_buffer_size",
     {offsetof(struct ColumnFamilyOptions, write_buffer_size),
      OptionType::kSizeT, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, write_buffer_size)}},
    {"bloom_locality",
     {offsetof(struct ColumnFamilyOptions, bloom_locality),
      OptionType::kUInt32T, OptionVerificationType::kNormal, false, 0}},
    {"memtable_prefix_bloom_bits",
     {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated, true, 0}},
    {"memtable_prefix_bloom_size_ratio",
     {offsetof(struct ColumnFamilyOptions, memtable_prefix_bloom_size_ratio),
      OptionType::kDouble, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, memtable_prefix_bloom_size_ratio)}},
    {"memtable_prefix_bloom_probes",
     {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated, true, 0}},
    {"min_partial_merge_operands",
     {offsetof(struct ColumnFamilyOptions, min_partial_merge_operands),
      OptionType::kUInt32T, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, min_partial_merge_operands)}},
    {"max_bytes_for_level_base",
     {offsetof(struct ColumnFamilyOptions, max_bytes_for_level_base),
      OptionType::kUInt64T, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, max_bytes_for_level_base)}},
    {"max_bytes_for_level_multiplier",
     {offsetof(struct ColumnFamilyOptions, max_bytes_for_level_multiplier),
      OptionType::kDouble, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, max_bytes_for_level_multiplier)}},
    {"max_bytes_for_level_multiplier_additional",
     {offsetof(struct ColumnFamilyOptions,
               max_bytes_for_level_multiplier_additional),
      OptionType::kVectorInt, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions,
               max_bytes_for_level_multiplier_additional)}},
    {"max_sequential_skip_in_iterations",
     {offsetof(struct ColumnFamilyOptions, max_sequential_skip_in_iterations),
      OptionType::kUInt64T, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, max_sequential_skip_in_iterations)}},
    {"target_file_size_base",
     {offsetof(struct ColumnFamilyOptions, target_file_size_base),
      OptionType::kUInt64T, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, target_file_size_base)}},
    {"rate_limit_delay_max_milliseconds",
     {0, OptionType::kUInt, OptionVerificationType::kDeprecated, false, 0}},
    {"compression",
     {offsetof(struct ColumnFamilyOptions, compression),
      OptionType::kCompressionType, OptionVerificationType::kNormal, true,
      offsetof(struct MutableCFOptions, compression)}},
    {"compression_per_level",
     {offsetof(struct ColumnFamilyOptions, compression_per_level),
      OptionType::kVectorCompressionType, OptionVerificationType::kNormal,
      false, 0}},
    {"bottommost_compression",
     {offsetof(struct ColumnFamilyOptions, bottommost_compression),
      OptionType::kCompressionType, OptionVerificationType::kNormal, false, 0}},
    {"comparator",
     {offsetof(struct ColumnFamilyOptions, comparator), OptionType::kComparator,
      OptionVerificationType::kByName, false, 0}},
    {"prefix_extractor",
     {offsetof(struct ColumnFamilyOptions, prefix_extractor),
      OptionType::kSliceTransform, OptionVerificationType::kByNameAllowNull,
      false, 0}},
    {"memtable_insert_with_hint_prefix_extractor",
     {offsetof(struct ColumnFamilyOptions,
               memtable_insert_with_hint_prefix_extractor),
      OptionType::kSliceTransform, OptionVerificationType::kByNameAllowNull,
      false, 0}},
    {"memtable_factory",
     {offsetof(struct ColumnFamilyOptions, memtable_factory),
      OptionType::kMemTableRepFactory, OptionVerificationType::kByName, false,
      0}},
    {"table_factory",
     {offsetof(struct ColumnFamilyOptions, table_factory),
      OptionType::kTableFactory, OptionVerificationType::kByName, false, 0}},
    {"compaction_filter",
     {offsetof(struct ColumnFamilyOptions, compaction_filter),
      OptionType::kCompactionFilter, OptionVerificationType::kByName, false,
      0}},
    {"compaction_filter_factory",
     {offsetof(struct ColumnFamilyOptions, compaction_filter_factory),
      OptionType::kCompactionFilterFactory, OptionVerificationType::kByName,
      false, 0}},
    {"merge_operator",
     {offsetof(struct ColumnFamilyOptions, merge_operator),
      OptionType::kMergeOperator, OptionVerificationType::kByName, false, 0}},
    {"compaction_style",
     {offsetof(struct ColumnFamilyOptions, compaction_style),
      OptionType::kCompactionStyle, OptionVerificationType::kNormal, false, 0}},
    {"compaction_pri",
     {offsetof(struct ColumnFamilyOptions, compaction_pri),
      OptionType::kCompactionPri, OptionVerificationType::kNormal, false, 0}}};

static std::unordered_map<std::string, OptionTypeInfo>
    block_based_table_type_info = {
        /* currently not supported
          std::shared_ptr<Cache> block_cache = nullptr;
          std::shared_ptr<Cache> block_cache_compressed = nullptr;
         */
        {"flush_block_policy_factory",
         {offsetof(struct BlockBasedTableOptions, flush_block_policy_factory),
          OptionType::kFlushBlockPolicyFactory, OptionVerificationType::kByName,
          false, 0}},
        {"cache_index_and_filter_blocks",
         {offsetof(struct BlockBasedTableOptions,
                   cache_index_and_filter_blocks),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"cache_index_and_filter_blocks_with_high_priority",
         {offsetof(struct BlockBasedTableOptions,
                   cache_index_and_filter_blocks_with_high_priority),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"pin_l0_filter_and_index_blocks_in_cache",
         {offsetof(struct BlockBasedTableOptions,
                   pin_l0_filter_and_index_blocks_in_cache),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"index_type",
         {offsetof(struct BlockBasedTableOptions, index_type),
          OptionType::kBlockBasedTableIndexType,
          OptionVerificationType::kNormal, false, 0}},
        {"hash_index_allow_collision",
         {offsetof(struct BlockBasedTableOptions, hash_index_allow_collision),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"checksum",
         {offsetof(struct BlockBasedTableOptions, checksum),
          OptionType::kChecksumType, OptionVerificationType::kNormal, false,
          0}},
        {"no_block_cache",
         {offsetof(struct BlockBasedTableOptions, no_block_cache),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"block_size",
         {offsetof(struct BlockBasedTableOptions, block_size),
          OptionType::kSizeT, OptionVerificationType::kNormal, false, 0}},
        {"block_size_deviation",
         {offsetof(struct BlockBasedTableOptions, block_size_deviation),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"block_restart_interval",
         {offsetof(struct BlockBasedTableOptions, block_restart_interval),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"index_block_restart_interval",
         {offsetof(struct BlockBasedTableOptions, index_block_restart_interval),
          OptionType::kInt, OptionVerificationType::kNormal, false, 0}},
        {"index_per_partition",
         {offsetof(struct BlockBasedTableOptions, index_per_partition),
          OptionType::kUInt64T, OptionVerificationType::kNormal, false, 0}},
        {"filter_policy",
         {offsetof(struct BlockBasedTableOptions, filter_policy),
          OptionType::kFilterPolicy, OptionVerificationType::kByName, false,
          0}},
        {"whole_key_filtering",
         {offsetof(struct BlockBasedTableOptions, whole_key_filtering),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"skip_table_builder_flush",
         {offsetof(struct BlockBasedTableOptions, skip_table_builder_flush),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"format_version",
         {offsetof(struct BlockBasedTableOptions, format_version),
          OptionType::kUInt32T, OptionVerificationType::kNormal, false, 0}},
        {"verify_compression",
         {offsetof(struct BlockBasedTableOptions, verify_compression),
          OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
        {"read_amp_bytes_per_bit",
         {offsetof(struct BlockBasedTableOptions, read_amp_bytes_per_bit),
          OptionType::kSizeT, OptionVerificationType::kNormal, false, 0}}};

static std::unordered_map<std::string, OptionTypeInfo> plain_table_type_info = {
    {"user_key_len",
     {offsetof(struct PlainTableOptions, user_key_len), OptionType::kUInt32T,
      OptionVerificationType::kNormal, false, 0}},
    {"bloom_bits_per_key",
     {offsetof(struct PlainTableOptions, bloom_bits_per_key), OptionType::kInt,
      OptionVerificationType::kNormal, false, 0}},
    {"hash_table_ratio",
     {offsetof(struct PlainTableOptions, hash_table_ratio), OptionType::kDouble,
      OptionVerificationType::kNormal, false, 0}},
    {"index_sparseness",
     {offsetof(struct PlainTableOptions, index_sparseness), OptionType::kSizeT,
      OptionVerificationType::kNormal, false, 0}},
    {"huge_page_tlb_size",
     {offsetof(struct PlainTableOptions, huge_page_tlb_size),
      OptionType::kSizeT, OptionVerificationType::kNormal, false, 0}},
    {"encoding_type",
     {offsetof(struct PlainTableOptions, encoding_type),
      OptionType::kEncodingType, OptionVerificationType::kByName, false, 0}},
    {"full_scan_mode",
     {offsetof(struct PlainTableOptions, full_scan_mode), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"store_index_in_file",
     {offsetof(struct PlainTableOptions, store_index_in_file),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}}};

static std::unordered_map<std::string, CompressionType>
    compression_type_string_map = {
        {"kNoCompression", kNoCompression},
        {"kSnappyCompression", kSnappyCompression},
        {"kZlibCompression", kZlibCompression},
        {"kBZip2Compression", kBZip2Compression},
        {"kLZ4Compression", kLZ4Compression},
        {"kLZ4HCCompression", kLZ4HCCompression},
        {"kXpressCompression", kXpressCompression},
        {"kZSTD", kZSTD},
        {"kZSTDNotFinalCompression", kZSTDNotFinalCompression},
        {"kDisableCompressionOption", kDisableCompressionOption}};

static std::unordered_map<std::string, BlockBasedTableOptions::IndexType>
    block_base_table_index_type_string_map = {
        {"kBinarySearch", BlockBasedTableOptions::IndexType::kBinarySearch},
        {"kHashSearch", BlockBasedTableOptions::IndexType::kHashSearch},
        {"kTwoLevelIndexSearch",
         BlockBasedTableOptions::IndexType::kHashSearch}};

static std::unordered_map<std::string, EncodingType> encoding_type_string_map =
    {{"kPlain", kPlain}, {"kPrefix", kPrefix}};

static std::unordered_map<std::string, ChecksumType> checksum_type_string_map =
    {{"kNoChecksum", kNoChecksum}, {"kCRC32c", kCRC32c}, {"kxxHash", kxxHash}};

static std::unordered_map<std::string, CompactionStyle>
    compaction_style_string_map = {
        {"kCompactionStyleLevel", kCompactionStyleLevel},
        {"kCompactionStyleUniversal", kCompactionStyleUniversal},
        {"kCompactionStyleFIFO", kCompactionStyleFIFO},
        {"kCompactionStyleNone", kCompactionStyleNone}};

static std::unordered_map<std::string, CompactionPri>
    compaction_pri_string_map = {
        {"kByCompensatedSize", kByCompensatedSize},
        {"kOldestLargestSeqFirst", kOldestLargestSeqFirst},
        {"kOldestSmallestSeqFirst", kOldestSmallestSeqFirst},
        {"kMinOverlappingRatio", kMinOverlappingRatio}};

static std::unordered_map<std::string,
                          WALRecoveryMode> wal_recovery_mode_string_map = {
    {"kTolerateCorruptedTailRecords",
     WALRecoveryMode::kTolerateCorruptedTailRecords},
    {"kAbsoluteConsistency", WALRecoveryMode::kAbsoluteConsistency},
    {"kPointInTimeRecovery", WALRecoveryMode::kPointInTimeRecovery},
    {"kSkipAnyCorruptedRecords", WALRecoveryMode::kSkipAnyCorruptedRecords}};

static std::unordered_map<std::string, DBOptions::AccessHint>
    access_hint_string_map = {{"NONE", DBOptions::AccessHint::NONE},
                              {"NORMAL", DBOptions::AccessHint::NORMAL},
                              {"SEQUENTIAL", DBOptions::AccessHint::SEQUENTIAL},
                              {"WILLNEED", DBOptions::AccessHint::WILLNEED}};

static std::unordered_map<std::string, InfoLogLevel> info_log_level_string_map =
    {{"DEBUG_LEVEL", InfoLogLevel::DEBUG_LEVEL},
     {"INFO_LEVEL", InfoLogLevel::INFO_LEVEL},
     {"WARN_LEVEL", InfoLogLevel::WARN_LEVEL},
     {"ERROR_LEVEL", InfoLogLevel::ERROR_LEVEL},
     {"FATAL_LEVEL", InfoLogLevel::FATAL_LEVEL},
     {"HEADER_LEVEL", InfoLogLevel::HEADER_LEVEL}};

extern const std::string kNullptrString;
#endif  // !ROCKSDB_LITE

}  // namespace rocksdb
