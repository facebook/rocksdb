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
#include "port/port.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/object_registry.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/plain/plain_table_factory.h"
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
ColumnFamilyOptions OptionsHelper::dummy_cf_options;
template <typename T1>
int offset_of(T1 ColumnFamilyOptions::*member) {
  return int(size_t(&(OptionsHelper::dummy_cf_options.*member)) -
             size_t(&OptionsHelper::dummy_cf_options));
}
template <typename T1>
int offset_of(T1 AdvancedColumnFamilyOptions::*member) {
  return int(size_t(&(OptionsHelper::dummy_cf_options.*member)) -
             size_t(&OptionsHelper::dummy_cf_options));
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
  // parallel_threads is not serialized with this format.
  // We plan to upgrade the format to a JSON-like format.
  compression_opts.parallel_threads = CompressionOptions().parallel_threads;

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

std::unordered_map<std::string, OptionTypeInfo>
    OptionsHelper::cf_options_type_info = {
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
        {"report_bg_io_stats",
         {offset_of(&ColumnFamilyOptions::report_bg_io_stats),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, report_bg_io_stats)}},
        {"compaction_measure_io_stats",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone, 0}},
        {"disable_auto_compactions",
         {offset_of(&ColumnFamilyOptions::disable_auto_compactions),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, disable_auto_compactions)}},
        {"filter_deletes",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"inplace_update_support",
         {offset_of(&ColumnFamilyOptions::inplace_update_support),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"level_compaction_dynamic_level_bytes",
         {offset_of(&ColumnFamilyOptions::level_compaction_dynamic_level_bytes),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"optimize_filters_for_hits",
         {offset_of(&ColumnFamilyOptions::optimize_filters_for_hits),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"paranoid_file_checks",
         {offset_of(&ColumnFamilyOptions::paranoid_file_checks),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, paranoid_file_checks)}},
        {"force_consistency_checks",
         {offset_of(&ColumnFamilyOptions::force_consistency_checks),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"purge_redundant_kvs_while_flush",
         {offset_of(&ColumnFamilyOptions::purge_redundant_kvs_while_flush),
          OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone, 0}},
        {"verify_checksums_in_compaction",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"soft_pending_compaction_bytes_limit",
         {offset_of(&ColumnFamilyOptions::soft_pending_compaction_bytes_limit),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions,
                   soft_pending_compaction_bytes_limit)}},
        {"hard_pending_compaction_bytes_limit",
         {offset_of(&ColumnFamilyOptions::hard_pending_compaction_bytes_limit),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions,
                   hard_pending_compaction_bytes_limit)}},
        {"hard_rate_limit",
         {0, OptionType::kDouble, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"soft_rate_limit",
         {0, OptionType::kDouble, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"max_compaction_bytes",
         {offset_of(&ColumnFamilyOptions::max_compaction_bytes),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, max_compaction_bytes)}},
        {"expanded_compaction_factor",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"level0_file_num_compaction_trigger",
         {offset_of(&ColumnFamilyOptions::level0_file_num_compaction_trigger),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions,
                   level0_file_num_compaction_trigger)}},
        {"level0_slowdown_writes_trigger",
         {offset_of(&ColumnFamilyOptions::level0_slowdown_writes_trigger),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, level0_slowdown_writes_trigger)}},
        {"level0_stop_writes_trigger",
         {offset_of(&ColumnFamilyOptions::level0_stop_writes_trigger),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, level0_stop_writes_trigger)}},
        {"max_grandparent_overlap_factor",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"max_mem_compaction_level",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone, 0}},
        {"max_write_buffer_number",
         {offset_of(&ColumnFamilyOptions::max_write_buffer_number),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, max_write_buffer_number)}},
        {"max_write_buffer_number_to_maintain",
         {offset_of(&ColumnFamilyOptions::max_write_buffer_number_to_maintain),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"max_write_buffer_size_to_maintain",
         {offset_of(&ColumnFamilyOptions::max_write_buffer_size_to_maintain),
          OptionType::kInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"min_write_buffer_number_to_merge",
         {offset_of(&ColumnFamilyOptions::min_write_buffer_number_to_merge),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"num_levels",
         {offset_of(&ColumnFamilyOptions::num_levels), OptionType::kInt,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"source_compaction_factor",
         {0, OptionType::kInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"target_file_size_multiplier",
         {offset_of(&ColumnFamilyOptions::target_file_size_multiplier),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, target_file_size_multiplier)}},
        {"arena_block_size",
         {offset_of(&ColumnFamilyOptions::arena_block_size), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, arena_block_size)}},
        {"inplace_update_num_locks",
         {offset_of(&ColumnFamilyOptions::inplace_update_num_locks),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, inplace_update_num_locks)}},
        {"max_successive_merges",
         {offset_of(&ColumnFamilyOptions::max_successive_merges),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, max_successive_merges)}},
        {"memtable_huge_page_size",
         {offset_of(&ColumnFamilyOptions::memtable_huge_page_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, memtable_huge_page_size)}},
        {"memtable_prefix_bloom_huge_page_tlb_size",
         {0, OptionType::kSizeT, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"write_buffer_size",
         {offset_of(&ColumnFamilyOptions::write_buffer_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, write_buffer_size)}},
        {"bloom_locality",
         {offset_of(&ColumnFamilyOptions::bloom_locality), OptionType::kUInt32T,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
        {"memtable_prefix_bloom_bits",
         {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"memtable_prefix_bloom_size_ratio",
         {offset_of(&ColumnFamilyOptions::memtable_prefix_bloom_size_ratio),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, memtable_prefix_bloom_size_ratio)}},
        {"memtable_prefix_bloom_probes",
         {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"memtable_whole_key_filtering",
         {offset_of(&ColumnFamilyOptions::memtable_whole_key_filtering),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, memtable_whole_key_filtering)}},
        {"min_partial_merge_operands",
         {0, OptionType::kUInt32T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"max_bytes_for_level_base",
         {offset_of(&ColumnFamilyOptions::max_bytes_for_level_base),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, max_bytes_for_level_base)}},
        {"snap_refresh_nanos",
         {0, OptionType::kUInt64T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kMutable, 0}},
        {"max_bytes_for_level_multiplier",
         {offset_of(&ColumnFamilyOptions::max_bytes_for_level_multiplier),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, max_bytes_for_level_multiplier)}},
        {"max_bytes_for_level_multiplier_additional",
         {offset_of(
              &ColumnFamilyOptions::max_bytes_for_level_multiplier_additional),
          OptionType::kVectorInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions,
                   max_bytes_for_level_multiplier_additional)}},
        {"max_sequential_skip_in_iterations",
         {offset_of(&ColumnFamilyOptions::max_sequential_skip_in_iterations),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions,
                   max_sequential_skip_in_iterations)}},
        {"target_file_size_base",
         {offset_of(&ColumnFamilyOptions::target_file_size_base),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, target_file_size_base)}},
        {"rate_limit_delay_max_milliseconds",
         {0, OptionType::kUInt, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone, 0}},
        {"compression",
         {offset_of(&ColumnFamilyOptions::compression),
          OptionType::kCompressionType, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, compression)}},
        {"compression_per_level",
         {offset_of(&ColumnFamilyOptions::compression_per_level),
          OptionType::kVectorCompressionType, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"bottommost_compression",
         {offset_of(&ColumnFamilyOptions::bottommost_compression),
          OptionType::kCompressionType, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, bottommost_compression)}},
        {"comparator",
         {offset_of(&ColumnFamilyOptions::comparator), OptionType::kComparator,
          OptionVerificationType::kByName, OptionTypeFlags::kCompareLoose, 0,
          // Parses the string and sets the corresponding comparator
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, char* addr) {
            auto old_comparator = reinterpret_cast<const Comparator**>(addr);
            const Comparator* new_comparator = *old_comparator;
            Status status = ObjectRegistry::NewInstance()->NewStaticObject(
                value, &new_comparator);
            if (status.ok()) {
              *old_comparator = new_comparator;
              return status;
            }
            return Status::OK();
          }}},
        {"prefix_extractor",
         {offset_of(&ColumnFamilyOptions::prefix_extractor),
          OptionType::kSliceTransform, OptionVerificationType::kByNameAllowNull,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, prefix_extractor)}},
        {"memtable_insert_with_hint_prefix_extractor",
         {offset_of(
              &ColumnFamilyOptions::memtable_insert_with_hint_prefix_extractor),
          OptionType::kSliceTransform, OptionVerificationType::kByNameAllowNull,
          OptionTypeFlags::kNone, 0}},
        {"memtable_factory",
         {offset_of(&ColumnFamilyOptions::memtable_factory),
          OptionType::kMemTableRepFactory, OptionVerificationType::kByName,
          OptionTypeFlags::kNone, 0}},
        {"memtable",
         {offset_of(&ColumnFamilyOptions::memtable_factory),
          OptionType::kMemTableRepFactory, OptionVerificationType::kAlias,
          OptionTypeFlags::kNone, 0,
          // Parses the value string and updates the memtable_factory
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, char* addr) {
            std::unique_ptr<MemTableRepFactory> new_mem_factory;
            Status s = GetMemTableRepFactoryFromString(value, &new_mem_factory);
            if (s.ok()) {
              auto memtable_factory =
                  reinterpret_cast<std::shared_ptr<MemTableRepFactory>*>(addr);
              memtable_factory->reset(new_mem_factory.release());
            }
            return s;
          }}},
        {"table_factory",
         {offset_of(&ColumnFamilyOptions::table_factory),
          OptionType::kTableFactory, OptionVerificationType::kByName,
          OptionTypeFlags::kCompareLoose, 0}},
        {"block_based_table_factory",
         {offset_of(&ColumnFamilyOptions::table_factory),
          OptionType::kTableFactory, OptionVerificationType::kAlias,
          OptionTypeFlags::kCompareLoose, 0,
          // Parses the input value and creates a BlockBasedTableFactory
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, char* addr) {
            // Nested options
            auto old_table_factory =
                reinterpret_cast<std::shared_ptr<TableFactory>*>(addr);
            BlockBasedTableOptions table_opts, base_opts;
            BlockBasedTableFactory* block_based_table_factory =
                static_cast_with_check<BlockBasedTableFactory>(
                    old_table_factory->get());
            if (block_based_table_factory != nullptr) {
              base_opts = block_based_table_factory->table_options();
            }
            Status s = GetBlockBasedTableOptionsFromString(base_opts, value,
                                                           &table_opts);
            if (s.ok()) {
              old_table_factory->reset(NewBlockBasedTableFactory(table_opts));
            }
            return s;
          }}},
        {"plain_table_factory",
         {offset_of(&ColumnFamilyOptions::table_factory),
          OptionType::kTableFactory, OptionVerificationType::kAlias,
          OptionTypeFlags::kCompareLoose, 0,
          // Parses the input value and creates a PlainTableFactory
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, char* addr) {
            // Nested options
            auto old_table_factory =
                reinterpret_cast<std::shared_ptr<TableFactory>*>(addr);
            PlainTableOptions table_opts, base_opts;
            PlainTableFactory* plain_table_factory =
                static_cast_with_check<PlainTableFactory>(
                    old_table_factory->get());
            if (plain_table_factory != nullptr) {
              base_opts = plain_table_factory->table_options();
            }
            Status s =
                GetPlainTableOptionsFromString(base_opts, value, &table_opts);
            if (s.ok()) {
              old_table_factory->reset(NewPlainTableFactory(table_opts));
            }
            return s;
          }}},
        {"compaction_filter",
         {offset_of(&ColumnFamilyOptions::compaction_filter),
          OptionType::kCompactionFilter, OptionVerificationType::kByName,
          OptionTypeFlags::kNone, 0}},
        {"compaction_filter_factory",
         {offset_of(&ColumnFamilyOptions::compaction_filter_factory),
          OptionType::kCompactionFilterFactory, OptionVerificationType::kByName,
          OptionTypeFlags::kNone, 0}},
        {"merge_operator",
         {offset_of(&ColumnFamilyOptions::merge_operator),
          OptionType::kMergeOperator,
          OptionVerificationType::kByNameAllowFromNull,
          OptionTypeFlags::kCompareLoose, 0,
          // Parses the input value as a MergeOperator, updating the value
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, char* addr) {
            auto mop = reinterpret_cast<std::shared_ptr<MergeOperator>*>(addr);
            ObjectRegistry::NewInstance()->NewSharedObject<MergeOperator>(value,
                                                                          mop);
            return Status::OK();
          }}},
        {"compaction_style",
         {offset_of(&ColumnFamilyOptions::compaction_style),
          OptionType::kCompactionStyle, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"compaction_pri",
         {offset_of(&ColumnFamilyOptions::compaction_pri),
          OptionType::kCompactionPri, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"compaction_options_fifo",
         {offset_of(&ColumnFamilyOptions::compaction_options_fifo),
          OptionType::kCompactionOptionsFIFO, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, compaction_options_fifo)}},
        {"compaction_options_universal",
         {offset_of(&ColumnFamilyOptions::compaction_options_universal),
          OptionType::kCompactionOptionsUniversal,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, compaction_options_universal)}},
        {"ttl",
         {offset_of(&ColumnFamilyOptions::ttl), OptionType::kUInt64T,
          OptionVerificationType::kNormal, OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, ttl)}},
        {"periodic_compaction_seconds",
         {offset_of(&ColumnFamilyOptions::periodic_compaction_seconds),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, periodic_compaction_seconds)}},
        {"sample_for_compression",
         {offset_of(&ColumnFamilyOptions::sample_for_compression),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable,
          offsetof(struct MutableCFOptions, sample_for_compression)}},
        // The following properties were handled as special cases in ParseOption
        // This means that the properties could be read from the options file
        // but never written to the file or compared to each other.
        {kOptNameCompOpts,
         {offset_of(&ColumnFamilyOptions::compression_opts),
          OptionType::kUnknown, OptionVerificationType::kNormal,
          (OptionTypeFlags::kDontSerialize | OptionTypeFlags::kCompareNever |
           OptionTypeFlags::kMutable),
          offsetof(struct MutableCFOptions, compression_opts),
          // Parses the value as a CompressionOptions
          [](const ConfigOptions& /*opts*/, const std::string& name,
             const std::string& value, char* addr) {
            auto* compression = reinterpret_cast<CompressionOptions*>(addr);
            return ParseCompressionOptions(value, name, *compression);
          }}},
        {kOptNameBMCompOpts,
         {offset_of(&ColumnFamilyOptions::bottommost_compression_opts),
          OptionType::kUnknown, OptionVerificationType::kNormal,
          (OptionTypeFlags::kDontSerialize | OptionTypeFlags::kCompareNever |
           OptionTypeFlags::kMutable),
          offsetof(struct MutableCFOptions, bottommost_compression_opts),
          // Parses the value as a CompressionOptions
          [](const ConfigOptions& /*opts*/, const std::string& name,
             const std::string& value, char* addr) {
            auto* compression = reinterpret_cast<CompressionOptions*>(addr);
            return ParseCompressionOptions(value, name, *compression);
          }}},
        // End special case properties
};

Status ParseColumnFamilyOption(const ConfigOptions& config_options,
                               const std::string& name,
                               const std::string& org_value,
                               ColumnFamilyOptions* new_options) {
  const std::string& value = config_options.input_strings_escaped
                                 ? UnescapeOptionString(org_value)
                                 : org_value;
  try {
    auto iter = cf_options_type_info.find(name);
    if (iter == cf_options_type_info.end()) {
      return Status::InvalidArgument(
          "Unable to parse the specified CF option " + name);
    } else {
      return iter->second.ParseOption(
          config_options, name, value,
          reinterpret_cast<char*>(new_options) + iter->second.offset);
    }
  } catch (const std::exception&) {
    return Status::InvalidArgument("unable to parse the specified option " +
                                   name);
  }
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
