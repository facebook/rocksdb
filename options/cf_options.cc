//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/cf_options.h"

#include <cassert>
#include <cinttypes>
#include <limits>
#include <string>

#include "options/configurable_helper.h"
#include "options/db_options.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/configurable.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {
// offset_of is used to get the offset of a class data member
// ex: offset_of(&ColumnFamilyOptions::num_levels)
// This call will return the offset of num_levels in ColumnFamilyOptions class
//
// This is the same as offsetof() but allow us to work with non standard-layout
// classes and structures
// refs:
// http://en.cppreference.com/w/cpp/concept/StandardLayoutType
// https://gist.github.com/graphitemaster/494f21190bb2c63c5516
#ifndef ROCKSDB_LITE
static ImmutableCFOptions dummy_cf_options;
template <typename T1>
int offset_of(T1 ImmutableCFOptions::*member) {
  return int(size_t(&(dummy_cf_options.*member)) - size_t(&dummy_cf_options));
}

static Status ParseCompressionOptions(const std::string& value,
                                      const std::string& name,
                                      CompressionOptions& compression_opts) {
  const char kDelimiter = ':';
  std::istringstream field_stream(value);
  std::string field;

  if (!std::getline(field_stream, field, kDelimiter)) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  compression_opts.window_bits = ParseInt(field);

  if (!std::getline(field_stream, field, kDelimiter)) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  compression_opts.level = ParseInt(field);

  if (!std::getline(field_stream, field, kDelimiter)) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  compression_opts.strategy = ParseInt(field);

  // max_dict_bytes is optional for backwards compatibility
  if (!field_stream.eof()) {
    if (!std::getline(field_stream, field, kDelimiter)) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.max_dict_bytes = ParseInt(field);
  }

  // zstd_max_train_bytes is optional for backwards compatibility
  if (!field_stream.eof()) {
    if (!std::getline(field_stream, field, kDelimiter)) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.zstd_max_train_bytes = ParseInt(field);
  }

  // parallel_threads is optional for backwards compatibility
  if (!field_stream.eof()) {
    if (!std::getline(field_stream, field, kDelimiter)) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    // Since parallel_threads comes before enabled but was added optionally
    // later, we need to check if this is the final token (meaning it is the
    // enabled bit), or if there are more tokens (meaning this one is
    // parallel_threads).
    if (!field_stream.eof()) {
      compression_opts.parallel_threads = ParseInt(field);
    } else {
      // parallel_threads is not serialized with this format, but enabled is
      compression_opts.enabled = ParseBoolean("", field);
    }
  }

  // enabled is optional for backwards compatibility
  if (!field_stream.eof()) {
    if (!std::getline(field_stream, field, kDelimiter)) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.enabled = ParseBoolean("", field);
  }

  // max_dict_buffer_bytes is optional for backwards compatibility
  if (!field_stream.eof()) {
    if (!std::getline(field_stream, field, kDelimiter)) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.max_dict_buffer_bytes = ParseUint64(field);
  }

  if (!field_stream.eof()) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  return Status::OK();
}

const std::string kOptNameBMCompOpts = "bottommost_compression_opts";
const std::string kOptNameCompOpts = "compression_opts";

// OptionTypeInfo map for CompressionOptions
static std::unordered_map<std::string, OptionTypeInfo>
    compression_options_type_info = {
        {"window_bits",
         {offsetof(struct CompressionOptions, window_bits), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"level",
         {offsetof(struct CompressionOptions, level), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"strategy",
         {offsetof(struct CompressionOptions, strategy), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"max_dict_bytes",
         {offsetof(struct CompressionOptions, max_dict_bytes), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"zstd_max_train_bytes",
         {offsetof(struct CompressionOptions, zstd_max_train_bytes),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"parallel_threads",
         {offsetof(struct CompressionOptions, parallel_threads),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"enabled",
         {offsetof(struct CompressionOptions, enabled), OptionType::kBoolean,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"max_dict_buffer_bytes",
         {offsetof(struct CompressionOptions, max_dict_buffer_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    fifo_compaction_options_type_info = {
        {"max_table_files_size",
         {offsetof(struct CompactionOptionsFIFO, max_table_files_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"ttl",
         {0, OptionType::kUInt64T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"allow_compaction",
         {offsetof(struct CompactionOptionsFIFO, allow_compaction),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
};

static std::unordered_map<std::string, OptionTypeInfo>
    universal_compaction_options_type_info = {
        {"size_ratio",
         {offsetof(class CompactionOptionsUniversal, size_ratio),
          OptionType::kUInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"min_merge_width",
         {offsetof(class CompactionOptionsUniversal, min_merge_width),
          OptionType::kUInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_merge_width",
         {offsetof(class CompactionOptionsUniversal, max_merge_width),
          OptionType::kUInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_size_amplification_percent",
         {offsetof(class CompactionOptionsUniversal,
                   max_size_amplification_percent),
          OptionType::kUInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"compression_size_percent",
         {offsetof(class CompactionOptionsUniversal, compression_size_percent),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"stop_style",
         {offsetof(class CompactionOptionsUniversal, stop_style),
          OptionType::kCompactionStopStyle, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"allow_trivial_move",
         {offsetof(class CompactionOptionsUniversal, allow_trivial_move),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}}};

static std::unordered_map<std::string, OptionTypeInfo>
    cf_mutable_options_type_info = {
        {"report_bg_io_stats",
         {offsetof(struct MutableCFOptions, report_bg_io_stats),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"disable_auto_compactions",
         {offsetof(struct MutableCFOptions, disable_auto_compactions),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"filter_deletes",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"check_flush_compaction_key_order",
         {offsetof(struct MutableCFOptions, check_flush_compaction_key_order),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"paranoid_file_checks",
         {offsetof(struct MutableCFOptions, paranoid_file_checks),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"verify_checksums_in_compaction",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"soft_pending_compaction_bytes_limit",
         {offsetof(struct MutableCFOptions,
                   soft_pending_compaction_bytes_limit),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"hard_pending_compaction_bytes_limit",
         {offsetof(struct MutableCFOptions,
                   hard_pending_compaction_bytes_limit),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"hard_rate_limit",
         {0, OptionType::kDouble, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"soft_rate_limit",
         {0, OptionType::kDouble, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"max_compaction_bytes",
         {offsetof(struct MutableCFOptions, max_compaction_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"expanded_compaction_factor",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"level0_file_num_compaction_trigger",
         {offsetof(struct MutableCFOptions, level0_file_num_compaction_trigger),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"level0_slowdown_writes_trigger",
         {offsetof(struct MutableCFOptions, level0_slowdown_writes_trigger),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"level0_stop_writes_trigger",
         {offsetof(struct MutableCFOptions, level0_stop_writes_trigger),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_grandparent_overlap_factor",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"max_write_buffer_number",
         {offsetof(struct MutableCFOptions, max_write_buffer_number),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"source_compaction_factor",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"target_file_size_multiplier",
         {offsetof(struct MutableCFOptions, target_file_size_multiplier),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"arena_block_size",
         {offsetof(struct MutableCFOptions, arena_block_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"inplace_update_num_locks",
         {offsetof(struct MutableCFOptions, inplace_update_num_locks),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_successive_merges",
         {offsetof(struct MutableCFOptions, max_successive_merges),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"memtable_huge_page_size",
         {offsetof(struct MutableCFOptions, memtable_huge_page_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"memtable_prefix_bloom_huge_page_tlb_size",
         {0, OptionType::kSizeT, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"write_buffer_size",
         {offsetof(struct MutableCFOptions, write_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"memtable_prefix_bloom_bits",
         {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"memtable_prefix_bloom_size_ratio",
         {offsetof(struct MutableCFOptions, memtable_prefix_bloom_size_ratio),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"memtable_prefix_bloom_probes",
         {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"memtable_whole_key_filtering",
         {offsetof(struct MutableCFOptions, memtable_whole_key_filtering),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"min_partial_merge_operands",
         {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"max_bytes_for_level_base",
         {offsetof(struct MutableCFOptions, max_bytes_for_level_base),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"snap_refresh_nanos",
         {0, OptionType::kUInt64T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable}},
        {"max_bytes_for_level_multiplier",
         {offsetof(struct MutableCFOptions, max_bytes_for_level_multiplier),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"max_bytes_for_level_multiplier_additional",
         OptionTypeInfo::Vector<int>(
             offsetof(struct MutableCFOptions,
                      max_bytes_for_level_multiplier_additional),
             OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
             {0, OptionType::kInt})},
        {"max_sequential_skip_in_iterations",
         {offsetof(struct MutableCFOptions, max_sequential_skip_in_iterations),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"target_file_size_base",
         {offsetof(struct MutableCFOptions, target_file_size_base),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"compression",
         {offsetof(struct MutableCFOptions, compression),
          OptionType::kCompressionType, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"prefix_extractor",
         {offsetof(struct MutableCFOptions, prefix_extractor),
          OptionType::kSliceTransform, OptionVerificationType::kByNameAllowNull,
          OptionTypeFlags::kMutable}},
        {"compaction_options_fifo",
         OptionTypeInfo::Struct(
             "compaction_options_fifo", &fifo_compaction_options_type_info,
             offsetof(struct MutableCFOptions, compaction_options_fifo),
             OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
             [](const ConfigOptions& opts, const std::string& name,
                const std::string& value, void* addr) {
               // This is to handle backward compatibility, where
               // compaction_options_fifo could be assigned a single scalar
               // value, say, like "23", which would be assigned to
               // max_table_files_size.
               if (name == "compaction_options_fifo" &&
                   value.find("=") == std::string::npos) {
                 // Old format. Parse just a single uint64_t value.
                 auto options = static_cast<CompactionOptionsFIFO*>(addr);
                 options->max_table_files_size = ParseUint64(value);
                 return Status::OK();
               } else {
                 return OptionTypeInfo::ParseStruct(
                     opts, "compaction_options_fifo",
                     &fifo_compaction_options_type_info, name, value, addr);
               }
             })},
        {"compaction_options_universal",
         OptionTypeInfo::Struct(
             "compaction_options_universal",
             &universal_compaction_options_type_info,
             offsetof(struct MutableCFOptions, compaction_options_universal),
             OptionVerificationType::kNormal, OptionTypeFlags::kMutable)},
        {"ttl",
         {offsetof(struct MutableCFOptions, ttl), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable}},
        {"periodic_compaction_seconds",
         {offsetof(struct MutableCFOptions, periodic_compaction_seconds),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"enable_blob_files",
         {offsetof(struct MutableCFOptions, enable_blob_files),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"min_blob_size",
         {offsetof(struct MutableCFOptions, min_blob_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"blob_file_size",
         {offsetof(struct MutableCFOptions, blob_file_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"blob_compression_type",
         {offsetof(struct MutableCFOptions, blob_compression_type),
          OptionType::kCompressionType, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"enable_blob_garbage_collection",
         {offsetof(struct MutableCFOptions, enable_blob_garbage_collection),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"blob_garbage_collection_age_cutoff",
         {offsetof(struct MutableCFOptions, blob_garbage_collection_age_cutoff),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"sample_for_compression",
         {offsetof(struct MutableCFOptions, sample_for_compression),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"bottommost_compression",
         {offsetof(struct MutableCFOptions, bottommost_compression),
          OptionType::kCompressionType, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {kOptNameCompOpts,
         OptionTypeInfo::Struct(
             kOptNameCompOpts, &compression_options_type_info,
             offsetof(struct MutableCFOptions, compression_opts),
             OptionVerificationType::kNormal,
             (OptionTypeFlags::kMutable | OptionTypeFlags::kCompareNever),
             [](const ConfigOptions& opts, const std::string& name,
                const std::string& value, void* addr) {
               // This is to handle backward compatibility, where
               // compression_options was a ":" separated list.
               if (name == kOptNameCompOpts &&
                   value.find("=") == std::string::npos) {
                 auto* compression = static_cast<CompressionOptions*>(addr);
                 return ParseCompressionOptions(value, name, *compression);
               } else {
                 return OptionTypeInfo::ParseStruct(
                     opts, kOptNameCompOpts, &compression_options_type_info,
                     name, value, addr);
               }
             })},
        {kOptNameBMCompOpts,
         OptionTypeInfo::Struct(
             kOptNameBMCompOpts, &compression_options_type_info,
             offsetof(struct MutableCFOptions, bottommost_compression_opts),
             OptionVerificationType::kNormal,
             (OptionTypeFlags::kMutable | OptionTypeFlags::kCompareNever),
             [](const ConfigOptions& opts, const std::string& name,
                const std::string& value, void* addr) {
               // This is to handle backward compatibility, where
               // compression_options was a ":" separated list.
               if (name == kOptNameBMCompOpts &&
                   value.find("=") == std::string::npos) {
                 auto* compression = static_cast<CompressionOptions*>(addr);
                 return ParseCompressionOptions(value, name, *compression);
               } else {
                 return OptionTypeInfo::ParseStruct(
                     opts, kOptNameBMCompOpts, &compression_options_type_info,
                     name, value, addr);
               }
             })},
        // End special case properties
};

static std::unordered_map<std::string, OptionTypeInfo>
    cf_immutable_options_type_info = {
        /* not yet supported
        CompressionOptions compression_opts;
        TablePropertiesCollectorFactories table_properties_collector_factories;
        typedef std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
            TablePropertiesCollectorFactories;
        UpdateStatus (*inplace_callback)(char* existing_value,
                                         uint34_t* existing_value_size,
                                         Slice delta_value,
                                         std::string* merged_value);
        std::vector<DbPath> cf_paths;
         */
        {"compaction_measure_io_stats",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"inplace_update_support",
         {offset_of(&ImmutableCFOptions::inplace_update_support),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"level_compaction_dynamic_level_bytes",
         {offset_of(&ImmutableCFOptions::level_compaction_dynamic_level_bytes),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"optimize_filters_for_hits",
         {offset_of(&ImmutableCFOptions::optimize_filters_for_hits),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"force_consistency_checks",
         {offset_of(&ImmutableCFOptions::force_consistency_checks),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"purge_redundant_kvs_while_flush",
         {offset_of(&ImmutableCFOptions::purge_redundant_kvs_while_flush),
          OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"max_mem_compaction_level",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"max_write_buffer_number_to_maintain",
         {offset_of(&ImmutableCFOptions::max_write_buffer_number_to_maintain),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"max_write_buffer_size_to_maintain",
         {offset_of(&ImmutableCFOptions::max_write_buffer_size_to_maintain),
          OptionType::kInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"min_write_buffer_number_to_merge",
         {offset_of(&ImmutableCFOptions::min_write_buffer_number_to_merge),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"num_levels",
         {offset_of(&ImmutableCFOptions::num_levels), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"bloom_locality",
         {offset_of(&ImmutableCFOptions::bloom_locality), OptionType::kUInt32T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"rate_limit_delay_max_milliseconds",
         {0, OptionType::kUInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"compression_per_level",
         OptionTypeInfo::Vector<CompressionType>(
             offset_of(&ImmutableCFOptions::compression_per_level),
             OptionVerificationType::kNormal, OptionTypeFlags::kNone,
             {0, OptionType::kCompressionType})},
        {"comparator",
         OptionTypeInfo::AsCustomRawPtr<const Comparator>(
             offset_of(&ImmutableCFOptions::user_comparator),
             OptionVerificationType::kByName, OptionTypeFlags::kCompareLoose,
             // Serializes a Comparator
             [](const ConfigOptions& /*opts*/, const std::string&,
                const void* addr, std::string* value) {
               // it's a const pointer of const Comparator*
               const auto* ptr = static_cast<const Comparator* const*>(addr);

               // Since the user-specified comparator will be wrapped by
               // InternalKeyComparator, we should persist the user-specified
               // one instead of InternalKeyComparator.
               if (*ptr == nullptr) {
                 *value = kNullptrString;
               } else {
                 const Comparator* root_comp = (*ptr)->GetRootComparator();
                 if (root_comp == nullptr) {
                   root_comp = (*ptr);
                 }
                 *value = root_comp->Name();
               }
               return Status::OK();
             },
             /* Use the default match function*/ nullptr)},
        {"memtable_insert_with_hint_prefix_extractor",
         {offset_of(
              &ImmutableCFOptions::memtable_insert_with_hint_prefix_extractor),
          OptionType::kSliceTransform, OptionVerificationType::kByNameAllowNull,
          OptionTypeFlags::kNone}},
        {"memtable_factory",
         {offset_of(&ImmutableCFOptions::memtable_factory),
          OptionType::kMemTableRepFactory, OptionVerificationType::kByName,
          OptionTypeFlags::kNone}},
        {"memtable",
         {offset_of(&ImmutableCFOptions::memtable_factory),
          OptionType::kMemTableRepFactory, OptionVerificationType::kAlias,
          OptionTypeFlags::kNone,
          // Parses the value string and updates the memtable_factory
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, void* addr) {
            std::unique_ptr<MemTableRepFactory> new_mem_factory;
            Status s = GetMemTableRepFactoryFromString(value, &new_mem_factory);
            if (s.ok()) {
              auto memtable_factory =
                  static_cast<std::shared_ptr<MemTableRepFactory>*>(addr);
              memtable_factory->reset(new_mem_factory.release());
            }
            return s;
          }}},
        {"table_factory", OptionTypeInfo::AsCustomSharedPtr<TableFactory>(
                              offset_of(&ImmutableCFOptions::table_factory),
                              OptionVerificationType::kByName,
                              (OptionTypeFlags::kCompareLoose |
                               OptionTypeFlags::kStringNameOnly |
                               OptionTypeFlags::kDontPrepare))},
        {"block_based_table_factory",
         {offset_of(&ImmutableCFOptions::table_factory),
          OptionType::kCustomizable, OptionVerificationType::kAlias,
          OptionTypeFlags::kShared | OptionTypeFlags::kCompareLoose,
          // Parses the input value and creates a BlockBasedTableFactory
          [](const ConfigOptions& opts, const std::string& name,
             const std::string& value, void* addr) {
            BlockBasedTableOptions* old_opts = nullptr;
            auto table_factory =
                static_cast<std::shared_ptr<TableFactory>*>(addr);
            if (table_factory->get() != nullptr) {
              old_opts =
                  table_factory->get()->GetOptions<BlockBasedTableOptions>();
            }
            if (name == "block_based_table_factory") {
              std::unique_ptr<TableFactory> new_factory;
              if (old_opts != nullptr) {
                new_factory.reset(NewBlockBasedTableFactory(*old_opts));
              } else {
                new_factory.reset(NewBlockBasedTableFactory());
              }
              Status s = new_factory->ConfigureFromString(opts, value);
              if (s.ok()) {
                table_factory->reset(new_factory.release());
              }
              return s;
            } else if (old_opts != nullptr) {
              return table_factory->get()->ConfigureOption(opts, name, value);
            } else {
              return Status::NotFound("Mismatched table option: ", name);
            }
          }}},
        {"plain_table_factory",
         {offset_of(&ImmutableCFOptions::table_factory),
          OptionType::kCustomizable, OptionVerificationType::kAlias,
          OptionTypeFlags::kShared | OptionTypeFlags::kCompareLoose,
          // Parses the input value and creates a PlainTableFactory
          [](const ConfigOptions& opts, const std::string& name,
             const std::string& value, void* addr) {
            PlainTableOptions* old_opts = nullptr;
            auto table_factory =
                static_cast<std::shared_ptr<TableFactory>*>(addr);
            if (table_factory->get() != nullptr) {
              old_opts = table_factory->get()->GetOptions<PlainTableOptions>();
            }
            if (name == "plain_table_factory") {
              std::unique_ptr<TableFactory> new_factory;
              if (old_opts != nullptr) {
                new_factory.reset(NewPlainTableFactory(*old_opts));
              } else {
                new_factory.reset(NewPlainTableFactory());
              }
              Status s = new_factory->ConfigureFromString(opts, value);
              if (s.ok()) {
                table_factory->reset(new_factory.release());
              }
              return s;
            } else if (old_opts != nullptr) {
              return table_factory->get()->ConfigureOption(opts, name, value);
            } else {
              return Status::NotFound("Mismatched table option: ", name);
            }
          }}},
        {"compaction_filter",
         {offset_of(&ImmutableCFOptions::compaction_filter),
          OptionType::kCompactionFilter, OptionVerificationType::kByName,
          OptionTypeFlags::kNone}},
        {"compaction_filter_factory",
         {offset_of(&ImmutableCFOptions::compaction_filter_factory),
          OptionType::kCompactionFilterFactory, OptionVerificationType::kByName,
          OptionTypeFlags::kNone}},
        {"merge_operator",
         {offset_of(&ImmutableCFOptions::merge_operator),
          OptionType::kMergeOperator,
          OptionVerificationType::kByNameAllowFromNull,
          OptionTypeFlags::kCompareLoose,
          // Parses the input value as a MergeOperator, updating the value
          [](const ConfigOptions& opts, const std::string& /*name*/,
             const std::string& value, void* addr) {
            auto mop = static_cast<std::shared_ptr<MergeOperator>*>(addr);
            Status status =
                opts.registry->NewSharedObject<MergeOperator>(value, mop);
            // Only support static comparator for now.
            if (status.ok()) {
              return status;
            }
            return Status::OK();
          }}},
        {"compaction_style",
         {offset_of(&ImmutableCFOptions::compaction_style),
          OptionType::kCompactionStyle, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"compaction_pri",
         {offset_of(&ImmutableCFOptions::compaction_pri),
          OptionType::kCompactionPri, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

const std::string OptionsHelper::kCFOptionsName = "ColumnFamilyOptions";

class ConfigurableMutableCFOptions : public Configurable {
 public:
  explicit ConfigurableMutableCFOptions(const MutableCFOptions& mcf) {
    mutable_ = mcf;
    RegisterOptions(&mutable_, &cf_mutable_options_type_info);
  }

 protected:
  MutableCFOptions mutable_;
};

class ConfigurableCFOptions : public ConfigurableMutableCFOptions {
 public:
  ConfigurableCFOptions(const ColumnFamilyOptions& opts,
                        const std::unordered_map<std::string, std::string>* map)
      : ConfigurableMutableCFOptions(MutableCFOptions(opts)),
        immutable_(opts),
        cf_options_(opts),
        opt_map_(map) {
    RegisterOptions(&immutable_, &cf_immutable_options_type_info);
  }

 protected:
  Status ConfigureOptions(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opts_map,
      std::unordered_map<std::string, std::string>* unused) override {
    Status s = Configurable::ConfigureOptions(config_options, opts_map, unused);
    if (s.ok()) {
      UpdateColumnFamilyOptions(mutable_, &cf_options_);
      UpdateColumnFamilyOptions(immutable_, &cf_options_);
      s = PrepareOptions(config_options);
    }
    return s;
  }

  virtual const void* GetOptionsPtr(const std::string& name) const override {
    if (name == OptionsHelper::kCFOptionsName) {
      return &cf_options_;
    } else {
      return ConfigurableMutableCFOptions::GetOptionsPtr(name);
    }
  }

  bool OptionsAreEqual(const ConfigOptions& config_options,
                       const OptionTypeInfo& opt_info,
                       const std::string& opt_name, const void* const this_ptr,
                       const void* const that_ptr,
                       std::string* mismatch) const override {
    bool equals = opt_info.AreEqual(config_options, opt_name, this_ptr,
                                    that_ptr, mismatch);
    if (!equals && opt_info.IsByName()) {
      if (opt_map_ == nullptr) {
        equals = true;
      } else {
        const auto& iter = opt_map_->find(opt_name);
        if (iter == opt_map_->end()) {
          equals = true;
        } else {
          equals = opt_info.AreEqualByName(config_options, opt_name, this_ptr,
                                           iter->second);
        }
      }
      if (equals) {  // False alarm, clear mismatch
        *mismatch = "";
      }
    }
    if (equals && opt_info.IsConfigurable() && opt_map_ != nullptr) {
      const auto* this_config = opt_info.AsRawPointer<Configurable>(this_ptr);
      if (this_config == nullptr) {
        const auto& iter = opt_map_->find(opt_name);
        // If the name exists in the map and is not empty/null,
        // then the this_config should be set.
        if (iter != opt_map_->end() && !iter->second.empty() &&
            iter->second != kNullptrString) {
          *mismatch = opt_name;
          equals = false;
        }
      }
    }
    return equals;
  }

 private:
  ImmutableCFOptions immutable_;
  ColumnFamilyOptions cf_options_;
  const std::unordered_map<std::string, std::string>* opt_map_;
};

std::unique_ptr<Configurable> CFOptionsAsConfigurable(
    const MutableCFOptions& opts) {
  std::unique_ptr<Configurable> ptr(new ConfigurableMutableCFOptions(opts));
  return ptr;
}
std::unique_ptr<Configurable> CFOptionsAsConfigurable(
    const ColumnFamilyOptions& opts,
    const std::unordered_map<std::string, std::string>* opt_map) {
  std::unique_ptr<Configurable> ptr(new ConfigurableCFOptions(opts, opt_map));
  return ptr;
}
#endif  // ROCKSDB_LITE

ImmutableCFOptions::ImmutableCFOptions() : ImmutableCFOptions(Options()) {}

ImmutableCFOptions::ImmutableCFOptions(const ColumnFamilyOptions& cf_options)
    : compaction_style(cf_options.compaction_style),
      compaction_pri(cf_options.compaction_pri),
      user_comparator(cf_options.comparator),
      internal_comparator(InternalKeyComparator(cf_options.comparator)),
      merge_operator(cf_options.merge_operator),
      compaction_filter(cf_options.compaction_filter),
      compaction_filter_factory(cf_options.compaction_filter_factory),
      min_write_buffer_number_to_merge(
          cf_options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          cf_options.max_write_buffer_number_to_maintain),
      max_write_buffer_size_to_maintain(
          cf_options.max_write_buffer_size_to_maintain),
      inplace_update_support(cf_options.inplace_update_support),
      inplace_callback(cf_options.inplace_callback),
      memtable_factory(cf_options.memtable_factory),
      table_factory(cf_options.table_factory),
      table_properties_collector_factories(
          cf_options.table_properties_collector_factories),
      bloom_locality(cf_options.bloom_locality),
      purge_redundant_kvs_while_flush(
          cf_options.purge_redundant_kvs_while_flush),
      compression_per_level(cf_options.compression_per_level),
      level_compaction_dynamic_level_bytes(
          cf_options.level_compaction_dynamic_level_bytes),
      num_levels(cf_options.num_levels),
      optimize_filters_for_hits(cf_options.optimize_filters_for_hits),
      force_consistency_checks(cf_options.force_consistency_checks),
      memtable_insert_with_hint_prefix_extractor(
          cf_options.memtable_insert_with_hint_prefix_extractor),
      cf_paths(cf_options.cf_paths),
      compaction_thread_limiter(cf_options.compaction_thread_limiter),
      sst_partitioner_factory(cf_options.sst_partitioner_factory) {}

ImmutableOptions::ImmutableOptions() : ImmutableOptions(Options()) {}

ImmutableOptions::ImmutableOptions(const Options& options)
    : ImmutableOptions(options, options) {}

ImmutableOptions::ImmutableOptions(const DBOptions& db_options,
                                   const ColumnFamilyOptions& cf_options)
    : ImmutableDBOptions(db_options), ImmutableCFOptions(cf_options) {}

ImmutableOptions::ImmutableOptions(const DBOptions& db_options,
                                   const ImmutableCFOptions& cf_options)
    : ImmutableDBOptions(db_options), ImmutableCFOptions(cf_options) {}

ImmutableOptions::ImmutableOptions(const ImmutableDBOptions& db_options,
                                   const ColumnFamilyOptions& cf_options)
    : ImmutableDBOptions(db_options), ImmutableCFOptions(cf_options) {}

ImmutableOptions::ImmutableOptions(const ImmutableDBOptions& db_options,
                                   const ImmutableCFOptions& cf_options)
    : ImmutableDBOptions(db_options), ImmutableCFOptions(cf_options) {}

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

// when level_compaction_dynamic_level_bytes is true and leveled compaction
// is used, the base level is not always L1, so precomupted max_file_size can
// no longer be used. Recompute file_size_for_level from base level.
uint64_t MaxFileSizeForLevel(const MutableCFOptions& cf_options,
    int level, CompactionStyle compaction_style, int base_level,
    bool level_compaction_dynamic_level_bytes) {
  if (!level_compaction_dynamic_level_bytes || level < base_level ||
      compaction_style != kCompactionStyleLevel) {
    assert(level >= 0);
    assert(level < (int)cf_options.max_file_size.size());
    return cf_options.max_file_size[level];
  } else {
    assert(level >= 0 && base_level >= 0);
    assert(level - base_level < (int)cf_options.max_file_size.size());
    return cf_options.max_file_size[level - base_level];
  }
}

size_t MaxFileSizeForL0MetaPin(const MutableCFOptions& cf_options) {
  // We do not want to pin meta-blocks that almost certainly came from intra-L0
  // or a former larger `write_buffer_size` value to avoid surprising users with
  // pinned memory usage. We use a factor of 1.5 to account for overhead
  // introduced during flush in most cases.
  if (port::kMaxSizet / 3 < cf_options.write_buffer_size / 2) {
    return port::kMaxSizet;
  }
  return cf_options.write_buffer_size / 2 * 3;
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

void MutableCFOptions::Dump(Logger* log) const {
  // Memtable related options
  ROCKS_LOG_INFO(log,
                 "                        write_buffer_size: %" ROCKSDB_PRIszt,
                 write_buffer_size);
  ROCKS_LOG_INFO(log, "                  max_write_buffer_number: %d",
                 max_write_buffer_number);
  ROCKS_LOG_INFO(log,
                 "                         arena_block_size: %" ROCKSDB_PRIszt,
                 arena_block_size);
  ROCKS_LOG_INFO(log, "              memtable_prefix_bloom_ratio: %f",
                 memtable_prefix_bloom_size_ratio);
  ROCKS_LOG_INFO(log, "              memtable_whole_key_filtering: %d",
                 memtable_whole_key_filtering);
  ROCKS_LOG_INFO(log,
                 "                  memtable_huge_page_size: %" ROCKSDB_PRIszt,
                 memtable_huge_page_size);
  ROCKS_LOG_INFO(log,
                 "                    max_successive_merges: %" ROCKSDB_PRIszt,
                 max_successive_merges);
  ROCKS_LOG_INFO(log,
                 "                 inplace_update_num_locks: %" ROCKSDB_PRIszt,
                 inplace_update_num_locks);
  ROCKS_LOG_INFO(
      log, "                         prefix_extractor: %s",
      prefix_extractor == nullptr ? "nullptr" : prefix_extractor->Name());
  ROCKS_LOG_INFO(log, "                 disable_auto_compactions: %d",
                 disable_auto_compactions);
  ROCKS_LOG_INFO(log, "      soft_pending_compaction_bytes_limit: %" PRIu64,
                 soft_pending_compaction_bytes_limit);
  ROCKS_LOG_INFO(log, "      hard_pending_compaction_bytes_limit: %" PRIu64,
                 hard_pending_compaction_bytes_limit);
  ROCKS_LOG_INFO(log, "       level0_file_num_compaction_trigger: %d",
                 level0_file_num_compaction_trigger);
  ROCKS_LOG_INFO(log, "           level0_slowdown_writes_trigger: %d",
                 level0_slowdown_writes_trigger);
  ROCKS_LOG_INFO(log, "               level0_stop_writes_trigger: %d",
                 level0_stop_writes_trigger);
  ROCKS_LOG_INFO(log, "                     max_compaction_bytes: %" PRIu64,
                 max_compaction_bytes);
  ROCKS_LOG_INFO(log, "                    target_file_size_base: %" PRIu64,
                 target_file_size_base);
  ROCKS_LOG_INFO(log, "              target_file_size_multiplier: %d",
                 target_file_size_multiplier);
  ROCKS_LOG_INFO(log, "                 max_bytes_for_level_base: %" PRIu64,
                 max_bytes_for_level_base);
  ROCKS_LOG_INFO(log, "           max_bytes_for_level_multiplier: %f",
                 max_bytes_for_level_multiplier);
  ROCKS_LOG_INFO(log, "                                      ttl: %" PRIu64,
                 ttl);
  ROCKS_LOG_INFO(log, "              periodic_compaction_seconds: %" PRIu64,
                 periodic_compaction_seconds);
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

  ROCKS_LOG_INFO(log, "max_bytes_for_level_multiplier_additional: %s",
                 result.c_str());
  ROCKS_LOG_INFO(log, "        max_sequential_skip_in_iterations: %" PRIu64,
                 max_sequential_skip_in_iterations);
  ROCKS_LOG_INFO(log, "         check_flush_compaction_key_order: %d",
                 check_flush_compaction_key_order);
  ROCKS_LOG_INFO(log, "                     paranoid_file_checks: %d",
                 paranoid_file_checks);
  ROCKS_LOG_INFO(log, "                       report_bg_io_stats: %d",
                 report_bg_io_stats);
  ROCKS_LOG_INFO(log, "                              compression: %d",
                 static_cast<int>(compression));

  // Universal Compaction Options
  ROCKS_LOG_INFO(log, "compaction_options_universal.size_ratio : %d",
                 compaction_options_universal.size_ratio);
  ROCKS_LOG_INFO(log, "compaction_options_universal.min_merge_width : %d",
                 compaction_options_universal.min_merge_width);
  ROCKS_LOG_INFO(log, "compaction_options_universal.max_merge_width : %d",
                 compaction_options_universal.max_merge_width);
  ROCKS_LOG_INFO(
      log, "compaction_options_universal.max_size_amplification_percent : %d",
      compaction_options_universal.max_size_amplification_percent);
  ROCKS_LOG_INFO(log,
                 "compaction_options_universal.compression_size_percent : %d",
                 compaction_options_universal.compression_size_percent);
  ROCKS_LOG_INFO(log, "compaction_options_universal.stop_style : %d",
                 compaction_options_universal.stop_style);
  ROCKS_LOG_INFO(
      log, "compaction_options_universal.allow_trivial_move : %d",
      static_cast<int>(compaction_options_universal.allow_trivial_move));

  // FIFO Compaction Options
  ROCKS_LOG_INFO(log, "compaction_options_fifo.max_table_files_size : %" PRIu64,
                 compaction_options_fifo.max_table_files_size);
  ROCKS_LOG_INFO(log, "compaction_options_fifo.allow_compaction : %d",
                 compaction_options_fifo.allow_compaction);

  // Blob file related options
  ROCKS_LOG_INFO(log, "                        enable_blob_files: %s",
                 enable_blob_files ? "true" : "false");
  ROCKS_LOG_INFO(log, "                            min_blob_size: %" PRIu64,
                 min_blob_size);
  ROCKS_LOG_INFO(log, "                           blob_file_size: %" PRIu64,
                 blob_file_size);
  ROCKS_LOG_INFO(log, "                    blob_compression_type: %s",
                 CompressionTypeToString(blob_compression_type).c_str());
  ROCKS_LOG_INFO(log, "           enable_blob_garbage_collection: %s",
                 enable_blob_garbage_collection ? "true" : "false");
  ROCKS_LOG_INFO(log, "       blob_garbage_collection_age_cutoff: %f",
                 blob_garbage_collection_age_cutoff);
}

MutableCFOptions::MutableCFOptions(const Options& options)
    : MutableCFOptions(ColumnFamilyOptions(options)) {}

#ifndef ROCKSDB_LITE
Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    Logger* /*info_log*/, MutableCFOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  ConfigOptions config_options;
  Status s = OptionTypeInfo::ParseType(
      config_options, options_map, cf_mutable_options_type_info, new_options);
  if (!s.ok()) {
    *new_options = base_options;
  }
  return s;
}

Status GetStringFromMutableCFOptions(const ConfigOptions& config_options,
                                     const MutableCFOptions& mutable_opts,
                                     std::string* opt_string) {
  assert(opt_string);
  opt_string->clear();
  return OptionTypeInfo::SerializeType(
      config_options, cf_mutable_options_type_info, &mutable_opts, opt_string);
}
#endif  // ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE
