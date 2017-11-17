// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "options/cf_options.h"
#include "options/db_options.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

namespace rocksdb {

DBOptions BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                         const MutableDBOptions& mutable_db_options);

ColumnFamilyOptions BuildColumnFamilyOptions(
    const ColumnFamilyOptions& ioptions,
    const MutableCFOptions& mutable_cf_options);

#ifndef ROCKSDB_LITE

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
    std::shared_ptr<TableFactory>* table_factory,
    bool ignore_unknown_options = false);

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
  kCompactionOptionsFIFO,
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
  kByName,               // The option is pointer typed so we can only verify
                         // based on it's name.
  kByNameAllowNull,      // Same as kByName, but it also allows the case
                         // where one of them is a nullptr.
  kByNameAllowFromNull,  // Same as kByName, but it also allows the case
                         // where the old option is nullptr.
  kDeprecated            // The option is no longer used in rocksdb. The RocksDB
                         // OptionsParser will still accept this option if it
                         // happen to exists in some Options file.  However,
                         // the parser will not include it in serialization
                         // and verification processes.
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
    std::vector<std::string>* unsupported_options_names = nullptr,
    bool ignore_unknown_options = false);

// In addition to its public version defined in rocksdb/convenience.h,
// this further takes an optional output vector "unsupported_options_names",
// which stores the name of all the unsupported options specified in "opts_map".
Status GetColumnFamilyOptionsFromMapInternal(
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options, bool input_strings_escaped,
    std::vector<std::string>* unsupported_options_names = nullptr,
    bool ignore_unknown_options = false);

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
     {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false, 0}},
    {"use_direct_io_for_flush_and_compaction",
     {offsetof(struct DBOptions, use_direct_io_for_flush_and_compaction),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
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
     {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false, 0}},
    {"disable_data_sync",  // for compatibility
     {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false, 0}},
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
      OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, compaction_readahead_size)}},
    {"random_access_max_buffer_size",
     {offsetof(struct DBOptions, random_access_max_buffer_size),
      OptionType::kSizeT, OptionVerificationType::kNormal, false, 0}},
    {"use_adaptive_mutex",
     {offsetof(struct DBOptions, use_adaptive_mutex), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"use_fsync",
     {offsetof(struct DBOptions, use_fsync), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
    {"max_background_jobs",
     {offsetof(struct DBOptions, max_background_jobs), OptionType::kInt,
      OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, max_background_jobs)}},
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
      OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, max_open_files)}},
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
      OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, bytes_per_sync)}},
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
      OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, wal_bytes_per_sync)}},
    {"stats_dump_period_sec",
     {offsetof(struct DBOptions, stats_dump_period_sec), OptionType::kUInt,
      OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, stats_dump_period_sec)}},
    {"fail_if_options_file_error",
     {offsetof(struct DBOptions, fail_if_options_file_error),
      OptionType::kBoolean, OptionVerificationType::kNormal, false, 0}},
    {"enable_pipelined_write",
     {offsetof(struct DBOptions, enable_pipelined_write), OptionType::kBoolean,
      OptionVerificationType::kNormal, false, 0}},
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
      offsetof(struct MutableDBOptions, avoid_flush_during_shutdown)}},
    {"writable_file_max_buffer_size",
     {offsetof(struct DBOptions, writable_file_max_buffer_size),
      OptionType::kSizeT, OptionVerificationType::kNormal, true,
      offsetof(struct MutableDBOptions, writable_file_max_buffer_size)}},
    {"allow_ingest_behind",
     {offsetof(struct DBOptions, allow_ingest_behind), OptionType::kBoolean,
      OptionVerificationType::kNormal, false,
      offsetof(struct ImmutableDBOptions, allow_ingest_behind)}},
    {"preserve_deletes",
     {offsetof(struct DBOptions, preserve_deletes), OptionType::kBoolean,
      OptionVerificationType::kNormal, false,
      offsetof(struct ImmutableDBOptions, preserve_deletes)}},
    {"concurrent_prepare",  // Deprecated by two_write_queues
     {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false, 0}},
    {"two_write_queues",
     {offsetof(struct DBOptions, two_write_queues), OptionType::kBoolean,
      OptionVerificationType::kNormal, false,
      offsetof(struct ImmutableDBOptions, two_write_queues)}},
    {"manual_wal_flush",
     {offsetof(struct DBOptions, manual_wal_flush), OptionType::kBoolean,
      OptionVerificationType::kNormal, false,
      offsetof(struct ImmutableDBOptions, manual_wal_flush)}},
    {"seq_per_batch",
     {0, OptionType::kBoolean, OptionVerificationType::kDeprecated, false, 0}}};
extern Status StringToMap(
    const std::string& opts_str,
    std::unordered_map<std::string, std::string>* opts_map);

extern bool ParseOptionHelper(char* opt_address, const OptionType& opt_type,
                              const std::string& value);
#endif  // !ROCKSDB_LITE

struct OptionsHelper {
  static std::map<CompactionStyle, std::string> compaction_style_to_string;
  static std::map<CompactionPri, std::string> compaction_pri_to_string;
  static std::map<CompactionStopStyle, std::string>
      compaction_stop_style_to_string;
  static std::unordered_map<std::string, ChecksumType> checksum_type_string_map;
#ifndef ROCKSDB_LITE
  static std::unordered_map<std::string, OptionTypeInfo> cf_options_type_info;
  static std::unordered_map<std::string, OptionTypeInfo>
      fifo_compaction_options_type_info;
  static std::unordered_map<std::string, OptionTypeInfo> db_options_type_info;
  static std::unordered_map<std::string, CompressionType>
      compression_type_string_map;
  static std::unordered_map<std::string, BlockBasedTableOptions::IndexType>
      block_base_table_index_type_string_map;
  static std::unordered_map<std::string, EncodingType> encoding_type_string_map;
  static std::unordered_map<std::string, CompactionStyle>
      compaction_style_string_map;
  static std::unordered_map<std::string, CompactionPri>
      compaction_pri_string_map;
  static std::unordered_map<std::string, WALRecoveryMode>
      wal_recovery_mode_string_map;
  static std::unordered_map<std::string, DBOptions::AccessHint>
      access_hint_string_map;
  static std::unordered_map<std::string, InfoLogLevel>
      info_log_level_string_map;
  static ColumnFamilyOptions dummy_cf_options;
  static CompactionOptionsFIFO dummy_comp_options;
#endif  // !ROCKSDB_LITE
};

// Some aliasing
static auto& compaction_style_to_string =
    OptionsHelper::compaction_style_to_string;
static auto& compaction_pri_to_string = OptionsHelper::compaction_pri_to_string;
static auto& compaction_stop_style_to_string =
    OptionsHelper::compaction_stop_style_to_string;
static auto& checksum_type_string_map = OptionsHelper::checksum_type_string_map;
#ifndef ROCKSDB_LITE
static auto& cf_options_type_info = OptionsHelper::cf_options_type_info;
static auto& fifo_compaction_options_type_info =
    OptionsHelper::fifo_compaction_options_type_info;
static auto& db_options_type_info = OptionsHelper::db_options_type_info;
static auto& compression_type_string_map =
    OptionsHelper::compression_type_string_map;
static auto& block_base_table_index_type_string_map =
    OptionsHelper::block_base_table_index_type_string_map;
static auto& encoding_type_string_map = OptionsHelper::encoding_type_string_map;
static auto& compaction_style_string_map =
    OptionsHelper::compaction_style_string_map;
static auto& compaction_pri_string_map =
    OptionsHelper::compaction_pri_string_map;
static auto& wal_recovery_mode_string_map =
    OptionsHelper::wal_recovery_mode_string_map;
static auto& access_hint_string_map = OptionsHelper::access_hint_string_map;
static auto& info_log_level_string_map =
    OptionsHelper::info_log_level_string_map;
#endif  // !ROCKSDB_LITE

}  // namespace rocksdb
