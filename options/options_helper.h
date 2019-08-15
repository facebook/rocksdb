// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/universal_compaction.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"

namespace rocksdb {
struct ImmutableDBOptions;
struct MutableCFOptions;
struct MutableDBOptions;
class SliceTransform;
class TableFactory;
DBOptions BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                         const MutableDBOptions& mutable_db_options);

ColumnFamilyOptions BuildColumnFamilyOptions(
    const ColumnFamilyOptions& ioptions,
    const MutableCFOptions& mutable_cf_options);

#ifndef ROCKSDB_LITE
Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    Logger* info_log, MutableCFOptions* new_options);

Status GetMutableDBOptionsFromStrings(
    const MutableDBOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableDBOptions* new_options);

// A helper function that converts "opt_address" to a std::string
// based on the specified OptionType.
bool SerializeSingleOptionHelper(const char* opt_address,
                                 const OptionTypeInfo& opt_info,
                                 std::string* value);

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
    const DBOptions& db_options, const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options, bool input_strings_escaped,
    std::unordered_set<std::string>* unsupported_options_names = nullptr,
    bool ignore_unknown_options = false);

Status GetColumnFamilyOptionNames(std::unordered_set<std::string>* option_names,
                                  bool use_mutable = false);

bool ParseSliceTransform(
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform);

extern Status StringToMap(
    const std::string& opts_str,
    std::unordered_map<std::string, std::string>* opts_map);

extern bool ParseOptionHelper(char* opt_address, const OptionTypeInfo& opt_info,
                              const std::string& value);

template <typename T>
bool ParseEnum(const std::unordered_map<std::string, T>& type_map,
               const std::string& type, T* value) {
  auto iter = type_map.find(type);
  if (iter != type_map.end()) {
    *value = iter->second;
    return true;
  }
  return false;
}

template <typename T>
bool SerializeEnum(const std::unordered_map<std::string, T>& type_map,
                   const T& type, std::string* value) {
  for (const auto& pair : type_map) {
    if (pair.second == type) {
      *value = pair.first;
      return true;
    }
  }
  return false;
}

#endif  // !ROCKSDB_LITE

struct OptionsHelper {
  static std::map<CompactionStyle, std::string> compaction_style_to_string;
  static std::map<CompactionPri, std::string> compaction_pri_to_string;
  static std::map<CompactionStopStyle, std::string>
      compaction_stop_style_to_string;
  static std::unordered_map<std::string, ChecksumType> checksum_type_string_map;
  static std::unordered_map<std::string, CompressionType>
      compression_type_string_map;
#ifndef ROCKSDB_LITE
  static std::unordered_map<std::string, OptionTypeInfo>
      fifo_compaction_options_type_info;
  static std::unordered_map<std::string, OptionTypeInfo>
      universal_compaction_options_type_info;
  static std::unordered_map<std::string, CompactionStopStyle>
      compaction_stop_style_string_map;
  static std::unordered_map<std::string, OptionTypeInfo> db_options_type_info;
  static std::unordered_map<std::string, OptionTypeInfo>
      lru_cache_options_type_info;
  static std::unordered_map<std::string, BlockBasedTableOptions::IndexType>
      block_base_table_index_type_string_map;
  static std::unordered_map<std::string,
                            BlockBasedTableOptions::DataBlockIndexType>
      block_base_table_data_block_index_type_string_map;
  static std::unordered_map<std::string,
                            BlockBasedTableOptions::IndexShorteningMode>
      block_base_table_index_shortening_mode_string_map;
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
  static CompactionOptionsFIFO dummy_comp_options;
  static LRUCacheOptions dummy_lru_cache_options;
  static CompactionOptionsUniversal dummy_comp_options_universal;
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
static auto& fifo_compaction_options_type_info =
    OptionsHelper::fifo_compaction_options_type_info;
static auto& universal_compaction_options_type_info =
    OptionsHelper::universal_compaction_options_type_info;
static auto& compaction_stop_style_string_map =
    OptionsHelper::compaction_stop_style_string_map;
static auto& db_options_type_info = OptionsHelper::db_options_type_info;
static auto& lru_cache_options_type_info =
    OptionsHelper::lru_cache_options_type_info;
static auto& compression_type_string_map =
    OptionsHelper::compression_type_string_map;
static auto& block_base_table_index_type_string_map =
    OptionsHelper::block_base_table_index_type_string_map;
static auto& block_base_table_data_block_index_type_string_map =
    OptionsHelper::block_base_table_data_block_index_type_string_map;
static auto& block_base_table_index_shortening_mode_string_map =
    OptionsHelper::block_base_table_index_shortening_mode_string_map;
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
