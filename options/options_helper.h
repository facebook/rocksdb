// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "rocksdb/options.h"
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

std::vector<CompressionType> GetSupportedCompressions();

std::vector<CompressionType> GetSupportedDictCompressions();

// Checks that the combination of DBOptions and ColumnFamilyOptions are valid
Status ValidateOptions(const DBOptions& db_opts,
                       const ColumnFamilyOptions& cf_opts);

DBOptions BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                         const MutableDBOptions& mutable_db_options);

ColumnFamilyOptions BuildColumnFamilyOptions(
    const ColumnFamilyOptions& ioptions,
    const MutableCFOptions& mutable_cf_options);

#ifndef ROCKSDB_LITE
std::unique_ptr<Configurable> DBOptionsAsConfigurable(
    const MutableDBOptions& opts);
std::unique_ptr<Configurable> DBOptionsAsConfigurable(const DBOptions& opts);
std::unique_ptr<Configurable> CFOptionsAsConfigurable(
    const MutableCFOptions& opts);
std::unique_ptr<Configurable> CFOptionsAsConfigurable(
    const ColumnFamilyOptions& opts,
    const std::unordered_map<std::string, std::string>* opt_map = nullptr);

Status GetStringFromMutableCFOptions(const ConfigOptions& config_options,
                                     const MutableCFOptions& mutable_opts,
                                     std::string* opt_string);

Status GetStringFromMutableDBOptions(const ConfigOptions& config_options,
                                     const MutableDBOptions& mutable_opts,
                                     std::string* opt_string);

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    Logger* info_log, MutableCFOptions* new_options);

Status GetMutableDBOptionsFromStrings(
    const MutableDBOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableDBOptions* new_options);

bool ParseSliceTransform(
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform);

extern Status StringToMap(
    const std::string& opts_str,
    std::unordered_map<std::string, std::string>* opts_map);
#endif  // !ROCKSDB_LITE

struct OptionsHelper {
  static const std::string kCFOptionsName /*= "ColumnFamilyOptions"*/;
  static const std::string kDBOptionsName /*= "DBOptions" */;
  static std::map<CompactionStyle, std::string> compaction_style_to_string;
  static std::map<CompactionPri, std::string> compaction_pri_to_string;
  static std::map<CompactionStopStyle, std::string>
      compaction_stop_style_to_string;
  static std::unordered_map<std::string, ChecksumType> checksum_type_string_map;
  static std::unordered_map<std::string, CompressionType>
      compression_type_string_map;
#ifndef ROCKSDB_LITE
  static std::unordered_map<std::string, CompactionStopStyle>
      compaction_stop_style_string_map;
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
static auto& compaction_stop_style_string_map =
    OptionsHelper::compaction_stop_style_string_map;
static auto& compression_type_string_map =
    OptionsHelper::compression_type_string_map;
static auto& encoding_type_string_map = OptionsHelper::encoding_type_string_map;
static auto& compaction_style_string_map =
    OptionsHelper::compaction_style_string_map;
static auto& compaction_pri_string_map =
    OptionsHelper::compaction_pri_string_map;
#endif  // !ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE
