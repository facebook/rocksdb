// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "options/options_type.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {
struct ColumnFamilyOptions;
struct ConfigOptions;
struct DBOptions;
struct ImmutableDBOptions;
struct MutableDBOptions;
struct MutableCFOptions;
struct Options;

DBOptions BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                         const MutableDBOptions& mutable_db_options);

ColumnFamilyOptions BuildColumnFamilyOptions(
    const ColumnFamilyOptions& ioptions,
    const MutableCFOptions& mutable_cf_options);

#ifndef ROCKSDB_LITE
Status ParseOptionsTypeFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, OptionTypeInfo>& opt_map,
    void* opt_ptr, const std::unordered_map<std::string, std::string>& options);
Status ParseOptionsTypeFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, OptionTypeInfo>& opt_map,
    void* opt_ptr, const std::unordered_map<std::string, std::string>& options,
    std::unordered_map<std::string, std::string>* unused);

Status GetStringFromMutableCFOptions(const ConfigOptions& cfg_opts,
                                     const MutableCFOptions& mutable_opts,
                                     std::string* opt_string);

Status GetStringFromMutableDBOptions(const ConfigOptions& cfg_opts,
                                     const MutableDBOptions& mutable_opts,
                                     std::string* opt_string);

Status GetStringFromStruct(
    const ConfigOptions& config_options, const void* const opt_ptr,
    const std::unordered_map<std::string, OptionTypeInfo>& type_info,
    std::string* opt_string);

// Compare all of the options in the map and returns true if the values for
// addr1 match those for addr2.  If they do not match, false is returned and
// mismatch is set to the option that does not match.
bool MatchesOptionsTypeFromMap(
    const ConfigOptions& config_options,
    const std::unordered_map<std::string, OptionTypeInfo>& opt_map,
    const void* const addr1, const void* const addr2, std::string* mismatch);

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    Logger* info_log, MutableCFOptions* new_options);

Status GetMutableDBOptionsFromStrings(
    const MutableDBOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableDBOptions* new_options);

Status GetTableFactoryFromMap(
    const std::string& factory_name,
    const std::unordered_map<std::string, std::string>& opt_map,
    std::shared_ptr<TableFactory>* table_factory,
    bool ignore_unknown_options = false);

Status GetTableFactoryFromMap(
    const ConfigOptions& config_options, const std::string& factory_name,
    const std::unordered_map<std::string, std::string>& opt_map,
    std::shared_ptr<TableFactory>* table_factory);

// A helper function that converts "opt_address" to a std::string
// based on the specified OptionType.
bool SerializeSingleOptionHelper(const char* opt_address,
                                 const OptionType opt_type, std::string* value);

// In addition to its public version defined in rocksdb/convenience.h,
// this further takes an optional output vector "unsupported_options_names",
// which stores the name of all the unsupported options specified in "opts_map".
Status GetDBOptionsFromMapInternal(
    const ConfigOptions& config_options, const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    DBOptions* new_options,
    std::unordered_map<std::string, std::string>* unused_opts);

bool ParseSliceTransform(
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform);

extern Status StringToMap(
    const std::string& opts_str,
    std::unordered_map<std::string, std::string>* opts_map);

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
      cf_immutable_options_type_info;
  static std::unordered_map<std::string, OptionTypeInfo>
      cf_mutable_options_type_info;
  static std::unordered_map<std::string, CompactionStopStyle>
      compaction_stop_style_string_map;
  static std::unordered_map<std::string, OptionTypeInfo>
      db_immutable_options_type_info;
  static std::unordered_map<std::string, OptionTypeInfo>
      db_mutable_options_type_info;
  static std::unordered_map<std::string, EncodingType> encoding_type_string_map;
  static std::unordered_map<std::string, CompactionStyle>
      compaction_style_string_map;
  static std::unordered_map<std::string, CompactionPri>
      compaction_pri_string_map;
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
static auto& cf_immutable_options_type_info =
    OptionsHelper::cf_immutable_options_type_info;
static auto& cf_mutable_options_type_info =
    OptionsHelper::cf_mutable_options_type_info;
static auto& compaction_stop_style_string_map =
    OptionsHelper::compaction_stop_style_string_map;
static auto& db_immutable_options_type_info =
    OptionsHelper::db_immutable_options_type_info;
static auto& db_mutable_options_type_info =
    OptionsHelper::db_mutable_options_type_info;
static auto& compression_type_string_map =
    OptionsHelper::compression_type_string_map;
static auto& encoding_type_string_map = OptionsHelper::encoding_type_string_map;
static auto& compaction_style_string_map =
    OptionsHelper::compaction_style_string_map;
static auto& compaction_pri_string_map =
    OptionsHelper::compaction_pri_string_map;
#endif  // !ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE
