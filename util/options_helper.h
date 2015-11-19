// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <string>
#include <stdexcept>
#include <vector>

#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/mutable_cf_options.h"

#ifndef ROCKSDB_LITE
namespace rocksdb {

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

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableCFOptions* new_options);

Status GetTableFactoryFromMap(
    const std::string& factory_name,
    const std::unordered_map<std::string, std::string>& opt_map,
    std::shared_ptr<TableFactory>* table_factory);

Status GetStringFromTableFactory(std::string* opts_str, const TableFactory* tf,
                                 const std::string& delimiter = ";  ");

ColumnFamilyOptions BuildColumnFamilyOptions(
    const Options& options, const MutableCFOptions& mutable_cf_options);

enum class OptionType {
  kBoolean,
  kInt,
  kUInt,
  kUInt32T,
  kUInt64T,
  kSizeT,
  kString,
  kDouble,
  kCompactionStyle,
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
  kUnknown
};

enum class OptionVerificationType {
  kNormal,
  kByName,     // The option is pointer typed so we can only verify
               // based on it's name.
  kDeprecated  // The option is no longer used in rocksdb. The RocksDB
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
      AccessHint access_hint_on_compaction_start;
      Env* env;
      InfoLogLevel info_log_level;
      WALRecoveryMode wal_recovery_mode;
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
      OptionVerificationType::kNormal}},
    {"allow_mmap_reads",
     {offsetof(struct DBOptions, allow_mmap_reads), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"allow_fallocate",
     {offsetof(struct DBOptions, allow_fallocate), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"allow_mmap_writes",
     {offsetof(struct DBOptions, allow_mmap_writes), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"allow_os_buffer",
     {offsetof(struct DBOptions, allow_os_buffer), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"create_if_missing",
     {offsetof(struct DBOptions, create_if_missing), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"create_missing_column_families",
     {offsetof(struct DBOptions, create_missing_column_families),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"disableDataSync",
     {offsetof(struct DBOptions, disableDataSync), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"disable_data_sync",  // for compatibility
     {offsetof(struct DBOptions, disableDataSync), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"enable_thread_tracking",
     {offsetof(struct DBOptions, enable_thread_tracking), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"error_if_exists",
     {offsetof(struct DBOptions, error_if_exists), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"is_fd_close_on_exec",
     {offsetof(struct DBOptions, is_fd_close_on_exec), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"paranoid_checks",
     {offsetof(struct DBOptions, paranoid_checks), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"skip_log_error_on_recovery",
     {offsetof(struct DBOptions, skip_log_error_on_recovery),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"skip_stats_update_on_db_open",
     {offsetof(struct DBOptions, skip_stats_update_on_db_open),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"new_table_reader_for_compaction_inputs",
     {offsetof(struct DBOptions, new_table_reader_for_compaction_inputs),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"compaction_readahead_size",
     {offsetof(struct DBOptions, compaction_readahead_size), OptionType::kSizeT,
      OptionVerificationType::kNormal}},
    {"random_access_max_buffer_size",
     {offsetof(struct DBOptions, random_access_max_buffer_size),
      OptionType::kSizeT, OptionVerificationType::kNormal}},
    {"writable_file_max_buffer_size",
     {offsetof(struct DBOptions, writable_file_max_buffer_size),  
      OptionType::kSizeT, OptionVerificationType::kNormal}},
    {"use_adaptive_mutex",
     {offsetof(struct DBOptions, use_adaptive_mutex), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"use_fsync",
     {offsetof(struct DBOptions, use_fsync), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"max_background_compactions",
     {offsetof(struct DBOptions, max_background_compactions), OptionType::kInt,
      OptionVerificationType::kNormal}},
    {"max_background_flushes",
     {offsetof(struct DBOptions, max_background_flushes), OptionType::kInt,
      OptionVerificationType::kNormal}},
    {"max_file_opening_threads",
     {offsetof(struct DBOptions, max_file_opening_threads), OptionType::kInt,
      OptionVerificationType::kNormal}},
    {"max_open_files",
     {offsetof(struct DBOptions, max_open_files), OptionType::kInt,
      OptionVerificationType::kNormal}},
    {"table_cache_numshardbits",
     {offsetof(struct DBOptions, table_cache_numshardbits), OptionType::kInt,
      OptionVerificationType::kNormal}},
    {"db_write_buffer_size",
     {offsetof(struct DBOptions, db_write_buffer_size), OptionType::kSizeT,
      OptionVerificationType::kNormal}},
    {"keep_log_file_num",
     {offsetof(struct DBOptions, keep_log_file_num), OptionType::kSizeT,
      OptionVerificationType::kNormal}},
    {"recycle_log_file_num",
     {offsetof(struct DBOptions, recycle_log_file_num), OptionType::kSizeT,
      OptionVerificationType::kNormal}},
    {"log_file_time_to_roll",
     {offsetof(struct DBOptions, log_file_time_to_roll), OptionType::kSizeT,
      OptionVerificationType::kNormal}},
    {"manifest_preallocation_size",
     {offsetof(struct DBOptions, manifest_preallocation_size),
      OptionType::kSizeT, OptionVerificationType::kNormal}},
    {"max_log_file_size",
     {offsetof(struct DBOptions, max_log_file_size), OptionType::kSizeT,
      OptionVerificationType::kNormal}},
    {"db_log_dir",
     {offsetof(struct DBOptions, db_log_dir), OptionType::kString,
      OptionVerificationType::kNormal}},
    {"wal_dir",
     {offsetof(struct DBOptions, wal_dir), OptionType::kString,
      OptionVerificationType::kNormal}},
    {"max_subcompactions",
     {offsetof(struct DBOptions, max_subcompactions), OptionType::kUInt32T,
      OptionVerificationType::kNormal}},
    {"WAL_size_limit_MB",
     {offsetof(struct DBOptions, WAL_size_limit_MB), OptionType::kUInt64T,
      OptionVerificationType::kNormal}},
    {"WAL_ttl_seconds",
     {offsetof(struct DBOptions, WAL_ttl_seconds), OptionType::kUInt64T,
      OptionVerificationType::kNormal}},
    {"bytes_per_sync",
     {offsetof(struct DBOptions, bytes_per_sync), OptionType::kUInt64T,
      OptionVerificationType::kNormal}},
    {"delayed_write_rate",
     {offsetof(struct DBOptions, delayed_write_rate), OptionType::kUInt64T,
      OptionVerificationType::kNormal}},
    {"delete_obsolete_files_period_micros",
     {offsetof(struct DBOptions, delete_obsolete_files_period_micros),
      OptionType::kUInt64T, OptionVerificationType::kNormal}},
    {"max_manifest_file_size",
     {offsetof(struct DBOptions, max_manifest_file_size), OptionType::kUInt64T,
      OptionVerificationType::kNormal}},
    {"max_total_wal_size",
     {offsetof(struct DBOptions, max_total_wal_size), OptionType::kUInt64T,
      OptionVerificationType::kNormal}},
    {"wal_bytes_per_sync",
     {offsetof(struct DBOptions, wal_bytes_per_sync), OptionType::kUInt64T,
      OptionVerificationType::kNormal}},
    {"stats_dump_period_sec",
     {offsetof(struct DBOptions, stats_dump_period_sec), OptionType::kUInt,
      OptionVerificationType::kNormal}}};

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
    std::vector<int> max_bytes_for_level_multiplier_additional;
     */
    {"compaction_measure_io_stats",
     {offsetof(struct ColumnFamilyOptions, compaction_measure_io_stats),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"disable_auto_compactions",
     {offsetof(struct ColumnFamilyOptions, disable_auto_compactions),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"filter_deletes",
     {offsetof(struct ColumnFamilyOptions, filter_deletes),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"inplace_update_support",
     {offsetof(struct ColumnFamilyOptions, inplace_update_support),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"level_compaction_dynamic_level_bytes",
     {offsetof(struct ColumnFamilyOptions,
               level_compaction_dynamic_level_bytes),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"optimize_filters_for_hits",
     {offsetof(struct ColumnFamilyOptions, optimize_filters_for_hits),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"paranoid_file_checks",
     {offsetof(struct ColumnFamilyOptions, paranoid_file_checks),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"purge_redundant_kvs_while_flush",
     {offsetof(struct ColumnFamilyOptions, purge_redundant_kvs_while_flush),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"verify_checksums_in_compaction",
     {offsetof(struct ColumnFamilyOptions, verify_checksums_in_compaction),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"soft_pending_compaction_bytes_limit",
     {offsetof(struct ColumnFamilyOptions, soft_pending_compaction_bytes_limit),
      OptionType::kUInt64T, OptionVerificationType::kNormal}},
    {"hard_pending_compaction_bytes_limit",
     {offsetof(struct ColumnFamilyOptions, hard_pending_compaction_bytes_limit),
      OptionType::kUInt64T, OptionVerificationType::kNormal}},
    {"hard_rate_limit",
     {offsetof(struct ColumnFamilyOptions, hard_rate_limit),
      OptionType::kDouble, OptionVerificationType::kDeprecated}},
    {"soft_rate_limit",
     {offsetof(struct ColumnFamilyOptions, soft_rate_limit),
      OptionType::kDouble, OptionVerificationType::kNormal}},
    {"expanded_compaction_factor",
     {offsetof(struct ColumnFamilyOptions, expanded_compaction_factor),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"level0_file_num_compaction_trigger",
     {offsetof(struct ColumnFamilyOptions, level0_file_num_compaction_trigger),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"level0_slowdown_writes_trigger",
     {offsetof(struct ColumnFamilyOptions, level0_slowdown_writes_trigger),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"level0_stop_writes_trigger",
     {offsetof(struct ColumnFamilyOptions, level0_stop_writes_trigger),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"max_bytes_for_level_multiplier",
     {offsetof(struct ColumnFamilyOptions, max_bytes_for_level_multiplier),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"max_grandparent_overlap_factor",
     {offsetof(struct ColumnFamilyOptions, max_grandparent_overlap_factor),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"max_mem_compaction_level",
     {offsetof(struct ColumnFamilyOptions, max_mem_compaction_level),
      OptionType::kInt, OptionVerificationType::kDeprecated}},
    {"max_write_buffer_number",
     {offsetof(struct ColumnFamilyOptions, max_write_buffer_number),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"max_write_buffer_number_to_maintain",
     {offsetof(struct ColumnFamilyOptions, max_write_buffer_number_to_maintain),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"min_write_buffer_number_to_merge",
     {offsetof(struct ColumnFamilyOptions, min_write_buffer_number_to_merge),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"num_levels",
     {offsetof(struct ColumnFamilyOptions, num_levels), OptionType::kInt,
      OptionVerificationType::kNormal}},
    {"source_compaction_factor",
     {offsetof(struct ColumnFamilyOptions, source_compaction_factor),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"target_file_size_multiplier",
     {offsetof(struct ColumnFamilyOptions, target_file_size_multiplier),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"arena_block_size",
     {offsetof(struct ColumnFamilyOptions, arena_block_size),
      OptionType::kSizeT, OptionVerificationType::kNormal}},
    {"inplace_update_num_locks",
     {offsetof(struct ColumnFamilyOptions, inplace_update_num_locks),
      OptionType::kSizeT, OptionVerificationType::kNormal}},
    {"max_successive_merges",
     {offsetof(struct ColumnFamilyOptions, max_successive_merges),
      OptionType::kSizeT, OptionVerificationType::kNormal}},
    {"memtable_prefix_bloom_huge_page_tlb_size",
     {offsetof(struct ColumnFamilyOptions,
               memtable_prefix_bloom_huge_page_tlb_size),
      OptionType::kSizeT, OptionVerificationType::kNormal}},
    {"write_buffer_size",
     {offsetof(struct ColumnFamilyOptions, write_buffer_size),
      OptionType::kSizeT, OptionVerificationType::kNormal}},
    {"bloom_locality",
     {offsetof(struct ColumnFamilyOptions, bloom_locality),
      OptionType::kUInt32T, OptionVerificationType::kNormal}},
    {"memtable_prefix_bloom_bits",
     {offsetof(struct ColumnFamilyOptions, memtable_prefix_bloom_bits),
      OptionType::kUInt32T, OptionVerificationType::kNormal}},
    {"memtable_prefix_bloom_probes",
     {offsetof(struct ColumnFamilyOptions, memtable_prefix_bloom_probes),
      OptionType::kUInt32T, OptionVerificationType::kNormal}},
    {"min_partial_merge_operands",
     {offsetof(struct ColumnFamilyOptions, min_partial_merge_operands),
      OptionType::kUInt32T, OptionVerificationType::kNormal}},
    {"max_bytes_for_level_base",
     {offsetof(struct ColumnFamilyOptions, max_bytes_for_level_base),
      OptionType::kUInt64T, OptionVerificationType::kNormal}},
    {"max_sequential_skip_in_iterations",
     {offsetof(struct ColumnFamilyOptions, max_sequential_skip_in_iterations),
      OptionType::kUInt64T, OptionVerificationType::kNormal}},
    {"target_file_size_base",
     {offsetof(struct ColumnFamilyOptions, target_file_size_base),
      OptionType::kUInt64T, OptionVerificationType::kNormal}},
    {"rate_limit_delay_max_milliseconds",
     {offsetof(struct ColumnFamilyOptions, rate_limit_delay_max_milliseconds),
      OptionType::kUInt, OptionVerificationType::kDeprecated}},
    {"compression",
     {offsetof(struct ColumnFamilyOptions, compression),
      OptionType::kCompressionType, OptionVerificationType::kNormal}},
    {"compression_per_level",
     {offsetof(struct ColumnFamilyOptions, compression_per_level),
      OptionType::kVectorCompressionType, OptionVerificationType::kNormal}},
    {"comparator",
     {offsetof(struct ColumnFamilyOptions, comparator), OptionType::kComparator,
      OptionVerificationType::kByName}},
    {"prefix_extractor",
     {offsetof(struct ColumnFamilyOptions, prefix_extractor),
      OptionType::kSliceTransform, OptionVerificationType::kByName}},
    {"memtable_factory",
     {offsetof(struct ColumnFamilyOptions, memtable_factory),
      OptionType::kMemTableRepFactory, OptionVerificationType::kByName}},
    {"table_factory",
     {offsetof(struct ColumnFamilyOptions, table_factory),
      OptionType::kTableFactory, OptionVerificationType::kByName}},
    {"compaction_filter",
     {offsetof(struct ColumnFamilyOptions, compaction_filter),
      OptionType::kCompactionFilter, OptionVerificationType::kByName}},
    {"compaction_filter_factory",
     {offsetof(struct ColumnFamilyOptions, compaction_filter_factory),
      OptionType::kCompactionFilterFactory, OptionVerificationType::kByName}},
    {"merge_operator",
     {offsetof(struct ColumnFamilyOptions, merge_operator),
      OptionType::kMergeOperator, OptionVerificationType::kByName}},
    {"compaction_style",
     {offsetof(struct ColumnFamilyOptions, compaction_style),
      OptionType::kCompactionStyle, OptionVerificationType::kNormal}}};

static std::unordered_map<std::string,
                          OptionTypeInfo> block_based_table_type_info = {
    /* currently not supported
      std::shared_ptr<Cache> block_cache = nullptr;
      std::shared_ptr<Cache> block_cache_compressed = nullptr;
     */
    {"flush_block_policy_factory",
     {offsetof(struct BlockBasedTableOptions, flush_block_policy_factory),
      OptionType::kFlushBlockPolicyFactory, OptionVerificationType::kByName}},
    {"cache_index_and_filter_blocks",
     {offsetof(struct BlockBasedTableOptions, cache_index_and_filter_blocks),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"index_type",
     {offsetof(struct BlockBasedTableOptions, index_type),
      OptionType::kBlockBasedTableIndexType, OptionVerificationType::kNormal}},
    {"hash_index_allow_collision",
     {offsetof(struct BlockBasedTableOptions, hash_index_allow_collision),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"checksum",
     {offsetof(struct BlockBasedTableOptions, checksum),
      OptionType::kChecksumType, OptionVerificationType::kNormal}},
    {"no_block_cache",
     {offsetof(struct BlockBasedTableOptions, no_block_cache),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"block_size",
     {offsetof(struct BlockBasedTableOptions, block_size), OptionType::kSizeT,
      OptionVerificationType::kNormal}},
    {"block_size_deviation",
     {offsetof(struct BlockBasedTableOptions, block_size_deviation),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"block_restart_interval",
     {offsetof(struct BlockBasedTableOptions, block_restart_interval),
      OptionType::kInt, OptionVerificationType::kNormal}},
    {"filter_policy",
     {offsetof(struct BlockBasedTableOptions, filter_policy),
      OptionType::kFilterPolicy, OptionVerificationType::kByName}},
    {"whole_key_filtering",
     {offsetof(struct BlockBasedTableOptions, whole_key_filtering),
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"skip_table_builder_flush",
     {offsetof(struct BlockBasedTableOptions, skip_table_builder_flush),  
      OptionType::kBoolean, OptionVerificationType::kNormal}},
    {"format_version",
     {offsetof(struct BlockBasedTableOptions, format_version),
      OptionType::kUInt32T, OptionVerificationType::kNormal}}};

static std::unordered_map<std::string, OptionTypeInfo> plain_table_type_info = {
    {"user_key_len",
     {offsetof(struct PlainTableOptions, user_key_len), OptionType::kUInt32T,
      OptionVerificationType::kNormal}},
    {"bloom_bits_per_key",
     {offsetof(struct PlainTableOptions, bloom_bits_per_key), OptionType::kInt,
      OptionVerificationType::kNormal}},
    {"hash_table_ratio",
     {offsetof(struct PlainTableOptions, hash_table_ratio), OptionType::kDouble,
      OptionVerificationType::kNormal}},
    {"index_sparseness",
     {offsetof(struct PlainTableOptions, index_sparseness), OptionType::kSizeT,
      OptionVerificationType::kNormal}},
    {"huge_page_tlb_size",
     {offsetof(struct PlainTableOptions, huge_page_tlb_size),
      OptionType::kSizeT, OptionVerificationType::kNormal}},
    {"encoding_type",
     {offsetof(struct PlainTableOptions, encoding_type),
      OptionType::kEncodingType, OptionVerificationType::kByName}},
    {"full_scan_mode",
     {offsetof(struct PlainTableOptions, full_scan_mode), OptionType::kBoolean,
      OptionVerificationType::kNormal}},
    {"store_index_in_file",
     {offsetof(struct PlainTableOptions, store_index_in_file),
      OptionType::kBoolean, OptionVerificationType::kNormal}}};

static std::unordered_map<std::string, CompressionType>
    compression_type_string_map = {
        {"kNoCompression", kNoCompression},
        {"kSnappyCompression", kSnappyCompression},
        {"kZlibCompression", kZlibCompression},
        {"kBZip2Compression", kBZip2Compression},
        {"kLZ4Compression", kLZ4Compression},
        {"kLZ4HCCompression", kLZ4HCCompression},
        {"kZSTDNotFinalCompression", kZSTDNotFinalCompression}};

static std::unordered_map<std::string, BlockBasedTableOptions::IndexType>
    block_base_table_index_type_string_map = {
        {"kBinarySearch", BlockBasedTableOptions::IndexType::kBinarySearch},
        {"kHashSearch", BlockBasedTableOptions::IndexType::kHashSearch}};

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

}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
