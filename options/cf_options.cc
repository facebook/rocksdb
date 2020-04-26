//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/cf_options.h"

#include <cassert>
#include <cinttypes>
#include <limits>
#include <string>

#include "options/db_options.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "rocksdb/compaction_filter.h"
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
static ColumnFamilyOptions dummy_cf_options;
template <typename T1>
int offset_of(T1 ColumnFamilyOptions::*member) {
  return int(size_t(&(dummy_cf_options.*member)) - size_t(&dummy_cf_options));
}
template <typename T1>
int offset_of(T1 AdvancedColumnFamilyOptions::*member) {
  return int(size_t(&(dummy_cf_options.*member)) - size_t(&dummy_cf_options));
}

static Status ParseCompressionOptions(const std::string& value,
                                      const std::string& name,
                                      CompressionOptions& compression_opts) {
  size_t start = 0;
  size_t end = value.find(':');
  if (end == std::string::npos) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  compression_opts.window_bits = ParseInt(value.substr(start, end - start));
  start = end + 1;
  end = value.find(':', start);
  if (end == std::string::npos) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  compression_opts.level = ParseInt(value.substr(start, end - start));
  start = end + 1;
  if (start >= value.size()) {
    return Status::InvalidArgument("unable to parse the specified CF option " +
                                   name);
  }
  end = value.find(':', start);
  compression_opts.strategy =
      ParseInt(value.substr(start, value.size() - start));
  // max_dict_bytes is optional for backwards compatibility
  if (end != std::string::npos) {
    start = end + 1;
    if (start >= value.size()) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.max_dict_bytes =
        ParseInt(value.substr(start, value.size() - start));
    end = value.find(':', start);
  }
  // zstd_max_train_bytes is optional for backwards compatibility
  if (end != std::string::npos) {
    start = end + 1;
    if (start >= value.size()) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.zstd_max_train_bytes =
        ParseInt(value.substr(start, value.size() - start));
    end = value.find(':', start);
  }
  // parallel_threads is optional for backwards compatibility
  if (end != std::string::npos) {
    start = end + 1;
    if (start >= value.size()) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.parallel_threads =
        ParseInt(value.substr(start, value.size() - start));
    end = value.find(':', start);
  }
  // enabled is optional for backwards compatibility
  if (end != std::string::npos) {
    start = end + 1;
    if (start >= value.size()) {
      return Status::InvalidArgument(
          "unable to parse the specified CF option " + name);
    }
    compression_opts.enabled =
        ParseBoolean("", value.substr(start, value.size() - start));
  }
  return Status::OK();
}

const std::string kOptNameBMCompOpts = "bottommost_compression_opts";
const std::string kOptNameCompOpts = "compression_opts";

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
          OptionTypeFlags::kMutable}},
};

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
                const std::string& value, char* addr) {
               // This is to handle backward compatibility, where
               // compaction_options_fifo could be assigned a single scalar
               // value, say, like "23", which would be assigned to
               // max_table_files_size.
               if (name == "compaction_options_fifo" &&
                   value.find("=") == std::string::npos) {
                 // Old format. Parse just a single uint64_t value.
                 auto options = reinterpret_cast<CompactionOptionsFIFO*>(addr);
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
        {"sample_for_compression",
         {offsetof(struct MutableCFOptions, sample_for_compression),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"bottommost_compression",
         {offsetof(struct MutableCFOptions, bottommost_compression),
          OptionType::kCompressionType, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {kOptNameCompOpts,
         {offsetof(struct MutableCFOptions, compression_opts),
          OptionType::kUnknown, OptionVerificationType::kNormal,
          (OptionTypeFlags::kDontSerialize | OptionTypeFlags::kCompareNever |
           OptionTypeFlags::kMutable),
          // Parses the value as a CompressionOptions
          [](const ConfigOptions& /*opts*/, const std::string& name,
             const std::string& value, char* addr) {
            auto* compression = reinterpret_cast<CompressionOptions*>(addr);
            return ParseCompressionOptions(value, name, *compression);
          }}},
        {kOptNameBMCompOpts,
         {offsetof(struct MutableCFOptions, bottommost_compression_opts),
          OptionType::kUnknown, OptionVerificationType::kNormal,
          (OptionTypeFlags::kDontSerialize | OptionTypeFlags::kCompareNever |
           OptionTypeFlags::kMutable),
          // Parses the value as a CompressionOptions
          [](const ConfigOptions& /*opts*/, const std::string& name,
             const std::string& value, char* addr) {
            auto* compression = reinterpret_cast<CompressionOptions*>(addr);
            return ParseCompressionOptions(value, name, *compression);
          }}},
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
         {offset_of(&ColumnFamilyOptions::inplace_update_support),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"level_compaction_dynamic_level_bytes",
         {offset_of(&ColumnFamilyOptions::level_compaction_dynamic_level_bytes),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"optimize_filters_for_hits",
         {offset_of(&ColumnFamilyOptions::optimize_filters_for_hits),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"force_consistency_checks",
         {offset_of(&ColumnFamilyOptions::force_consistency_checks),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"purge_redundant_kvs_while_flush",
         {offset_of(&ColumnFamilyOptions::purge_redundant_kvs_while_flush),
          OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"max_mem_compaction_level",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"max_write_buffer_number_to_maintain",
         {offset_of(&ColumnFamilyOptions::max_write_buffer_number_to_maintain),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"max_write_buffer_size_to_maintain",
         {offset_of(&ColumnFamilyOptions::max_write_buffer_size_to_maintain),
          OptionType::kInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"min_write_buffer_number_to_merge",
         {offset_of(&ColumnFamilyOptions::min_write_buffer_number_to_merge),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"num_levels",
         {offset_of(&ColumnFamilyOptions::num_levels), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"bloom_locality",
         {offset_of(&ColumnFamilyOptions::bloom_locality), OptionType::kUInt32T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
        {"rate_limit_delay_max_milliseconds",
         {0, OptionType::kUInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"compression_per_level",
         OptionTypeInfo::Vector<CompressionType>(
             offset_of(&ColumnFamilyOptions::compression_per_level),
             OptionVerificationType::kNormal, OptionTypeFlags::kNone,
             {0, OptionType::kCompressionType})},
        {"comparator",
         OptionTypeInfo::AsCustomP<const Comparator>(
             offset_of(&ColumnFamilyOptions::comparator),
             OptionVerificationType::kByName, OptionTypeFlags::kCompareLoose,
             // Serializes a Comparator
             [](const ConfigOptions& /*opts*/, const std::string&,
                const char* addr, std::string* value) {
               // it's a const pointer of const Comparator*
               const auto* ptr =
                   reinterpret_cast<const Comparator* const*>(addr);
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
              &ColumnFamilyOptions::memtable_insert_with_hint_prefix_extractor),
          OptionType::kSliceTransform, OptionVerificationType::kByNameAllowNull,
          OptionTypeFlags::kNone}},
        {"memtable_factory",
         OptionTypeInfo::AsCustomS<MemTableRepFactory>(
             offset_of(&ColumnFamilyOptions::memtable_factory),
             OptionVerificationType::kByName, OptionTypeFlags::kNone)},
        {"memtable",
         OptionTypeInfo::AsCustomS<MemTableRepFactory>(
             offset_of(&ColumnFamilyOptions::memtable_factory),
             OptionVerificationType::kAlias, OptionTypeFlags::kNone)},
        {"table_factory", OptionTypeInfo::AsCustomS<TableFactory>(
                              offset_of(&ColumnFamilyOptions::table_factory),
                              OptionVerificationType::kByName,
                              (OptionTypeFlags::kCompareLoose |
                               OptionTypeFlags::kStringNameOnly |
                               OptionTypeFlags::kDontPrepare))},
        {"block_based_table_factory",
         {offset_of(&ColumnFamilyOptions::table_factory),
          OptionType::kCustomizable, OptionVerificationType::kAlias,
          OptionTypeFlags::kShared | OptionTypeFlags::kCompareLoose,
          // Parses the input value and creates a BlockBasedTableFactory
          [](const ConfigOptions& opts, const std::string& /*name*/,
             const std::string& value, char* addr) {
            // Nested options
            BlockBasedTableOptions table_opts;
            auto old_factory =
                reinterpret_cast<std::shared_ptr<TableFactory>*>(addr);
            if (old_factory->get() != nullptr) {
              auto* old_opts =
                  old_factory->get()->GetOptions<BlockBasedTableOptions>(
                      TableFactory::kBlockBasedTableOpts);
              if (old_opts != nullptr) {
                table_opts = *old_opts;
              }
            }
            std::unique_ptr<TableFactory> new_factory(
                NewBlockBasedTableFactory(table_opts));
            ConfigOptions copy = opts;
            copy.invoke_prepare_options = true;
            Status s = new_factory->ConfigureFromString(copy, value);
            if (s.ok()) {
              old_factory->reset(new_factory.release());
            }
            return s;
          }}},
        {"plain_table_factory",
         {offset_of(&ColumnFamilyOptions::table_factory),
          OptionType::kCustomizable, OptionVerificationType::kAlias,
          OptionTypeFlags::kShared | OptionTypeFlags::kCompareLoose,
          // Parses the input value and creates a BlockBasedTableFactory
          [](const ConfigOptions& opts, const std::string& /*name*/,
             const std::string& value, char* addr) {
            // Nested options
            PlainTableOptions table_opts;
            auto old_factory =
                reinterpret_cast<std::shared_ptr<TableFactory>*>(addr);
            if (old_factory->get() != nullptr) {
              auto* old_opts =
                  old_factory->get()->GetOptions<PlainTableOptions>(
                      TableFactory::kPlainTableOpts);
              if (old_opts != nullptr) {
                table_opts = *old_opts;
              }
            }
            std::unique_ptr<TableFactory> new_factory(
                NewPlainTableFactory(table_opts));
            ConfigOptions copy = opts;
            copy.invoke_prepare_options = true;
            Status s = new_factory->ConfigureFromString(copy, value);
            if (s.ok()) {
              old_factory->reset(new_factory.release());
            }
            return s;
          }}},
        {"compaction_filter",
         OptionTypeInfo::AsCustomP<const CompactionFilter>(
             offset_of(&ColumnFamilyOptions::compaction_filter),
             OptionVerificationType::kByName, OptionTypeFlags::kAllowNull)},
        {"compaction_filter_factory",
         OptionTypeInfo::AsCustomS<CompactionFilterFactory>(
             offset_of(&ColumnFamilyOptions::compaction_filter_factory),
             OptionVerificationType::kByName, OptionTypeFlags::kAllowNull)},
        {"merge_operator",
         OptionTypeInfo::AsCustomS<MergeOperator>(
             offset_of(&ColumnFamilyOptions::merge_operator),
             OptionVerificationType::kByNameAllowFromNull,
             OptionTypeFlags::kCompareLoose | OptionTypeFlags::kAllowNull)},
        {"compaction_style",
         {offset_of(&ColumnFamilyOptions::compaction_style),
          OptionType::kCompactionStyle, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"compaction_pri",
         {offset_of(&ColumnFamilyOptions::compaction_pri),
          OptionType::kCompactionPri, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

const std::string OptionsHelper::kCFOptionsName = "ColumnFamilyOptions";
const std::string OptionsHelper::kMutableCFOptionsName = "MutableCFOptions";

class ConfigurableMutableCFOptions : public Configurable {
 public:
  ConfigurableMutableCFOptions(const MutableCFOptions& mcf) {
    mutable_ = mcf;
    RegisterOptions(OptionsHelper::kMutableCFOptionsName, &mutable_,
                    &cf_mutable_options_type_info);
  }

 protected:
  MutableCFOptions mutable_;
};

class ConfigurableCFOptions : public ConfigurableMutableCFOptions {
 public:
  ConfigurableCFOptions(const ColumnFamilyOptions& opts,
                        const std::unordered_map<std::string, std::string>* map)
      : ConfigurableMutableCFOptions(MutableCFOptions(opts)),
        cf_options_(opts),
        opt_map(map) {
    RegisterOptions(OptionsHelper::kCFOptionsName, &cf_options_,
                    &cf_immutable_options_type_info);
  }

 protected:
  Status DoConfigureOptions(
      const ConfigOptions& config_options, const std::string& type_name,
      const std::unordered_map<std::string, OptionTypeInfo>& type_map,
      std::unordered_map<std::string, std::string>* options,
      void* opt_ptr) override {
    Status s = ConfigurableMutableCFOptions::DoConfigureOptions(
        config_options, type_name, type_map, options, opt_ptr);
    if (type_name == OptionsHelper::kMutableCFOptionsName) {
      cf_options_ = BuildColumnFamilyOptions(cf_options_, mutable_);
    }
    return s;
  }

  bool MatchesOption(const ConfigOptions& config_options,
                     const OptionTypeInfo& opt_info,
                     const std::string& opt_name, const void* const this_ptr,
                     const void* const that_ptr,
                     std::string* mismatch) const override {
    bool matches = Configurable::MatchesOption(
        config_options, opt_info, opt_name, this_ptr, that_ptr, mismatch);
    if (matches && opt_info.IsConfigurable() && opt_map != nullptr) {
      const auto* this_config = opt_info.AsRawPointer<Configurable>(this_ptr);
      if (this_config == nullptr) {
        const auto iter = opt_map->find(opt_name);
        // If the name exists in the map and is not empty,
        // then the this_config should be set.
        if (iter != opt_map->end() && !iter->second.empty()) {
          matches = false;
        }
      }
    }
    return matches;
  }

  bool CheckByName(const ConfigOptions& config_options,
                   const OptionTypeInfo& opt_info, const std::string& opt_name,
                   const void* const this_ptr,
                   const void* const /*that_ptr*/) const override {
    std::string this_value;
    if (!opt_info.IsByName()) {
      return false;
    } else if (opt_map == nullptr) {
      return true;
    } else {
      auto iter = opt_map->find(opt_name);
      if (iter == opt_map->end()) {
        return true;
      } else {
        return opt_info.CheckByName(config_options, opt_name, this_ptr,
                                    iter->second);
      }
    }
  }

 private:
  ColumnFamilyOptions cf_options_;
  const std::unordered_map<std::string, std::string>* opt_map;
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

ImmutableCFOptions::ImmutableCFOptions(const Options& options)
    : ImmutableCFOptions(ImmutableDBOptions(options), options) {}

ImmutableCFOptions::ImmutableCFOptions(const ImmutableDBOptions& db_options,
                                       const ColumnFamilyOptions& cf_options)
    : compaction_style(cf_options.compaction_style),
      compaction_pri(cf_options.compaction_pri),
      user_comparator(cf_options.comparator),
      internal_comparator(InternalKeyComparator(cf_options.comparator)),
      merge_operator(cf_options.merge_operator.get()),
      compaction_filter(cf_options.compaction_filter),
      compaction_filter_factory(cf_options.compaction_filter_factory.get()),
      min_write_buffer_number_to_merge(
          cf_options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          cf_options.max_write_buffer_number_to_maintain),
      max_write_buffer_size_to_maintain(
          cf_options.max_write_buffer_size_to_maintain),
      inplace_update_support(cf_options.inplace_update_support),
      inplace_callback(cf_options.inplace_callback),
      info_log(db_options.info_log.get()),
      statistics(db_options.statistics.get()),
      rate_limiter(db_options.rate_limiter.get()),
      info_log_level(db_options.info_log_level),
      env(db_options.env),
      fs(db_options.fs.get()),
      allow_mmap_reads(db_options.allow_mmap_reads),
      allow_mmap_writes(db_options.allow_mmap_writes),
      db_paths(db_options.db_paths),
      memtable_factory(cf_options.memtable_factory.get()),
      table_factory(cf_options.table_factory.get()),
      table_properties_collector_factories(
          cf_options.table_properties_collector_factories),
      advise_random_on_open(db_options.advise_random_on_open),
      bloom_locality(cf_options.bloom_locality),
      purge_redundant_kvs_while_flush(
          cf_options.purge_redundant_kvs_while_flush),
      use_fsync(db_options.use_fsync),
      compression_per_level(cf_options.compression_per_level),
      level_compaction_dynamic_level_bytes(
          cf_options.level_compaction_dynamic_level_bytes),
      access_hint_on_compaction_start(
          db_options.access_hint_on_compaction_start),
      new_table_reader_for_compaction_inputs(
          db_options.new_table_reader_for_compaction_inputs),
      num_levels(cf_options.num_levels),
      optimize_filters_for_hits(cf_options.optimize_filters_for_hits),
      force_consistency_checks(cf_options.force_consistency_checks),
      allow_ingest_behind(db_options.allow_ingest_behind),
      preserve_deletes(db_options.preserve_deletes),
      listeners(db_options.listeners),
      row_cache(db_options.row_cache),
      max_subcompactions(db_options.max_subcompactions),
      memtable_insert_with_hint_prefix_extractor(
          cf_options.memtable_insert_with_hint_prefix_extractor.get()),
      cf_paths(cf_options.cf_paths),
      compaction_thread_limiter(cf_options.compaction_thread_limiter),
      file_checksum_gen_factory(db_options.file_checksum_gen_factory.get()) {}

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
}

MutableCFOptions::MutableCFOptions(const Options& options)
    : MutableCFOptions(ColumnFamilyOptions(options)) {}

}  // namespace ROCKSDB_NAMESPACE
