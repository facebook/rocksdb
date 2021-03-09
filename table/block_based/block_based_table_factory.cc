//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_based/block_based_table_factory.h"

#include <stdint.h>

#include <cinttypes>
#include <memory>
#include <string>

#include "options/configurable_helper.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/utilities/options_type.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/format.h"
#include "util/mutexlock.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

void TailPrefetchStats::RecordEffectiveSize(size_t len) {
  MutexLock l(&mutex_);
  if (num_records_ < kNumTracked) {
    num_records_++;
  }
  records_[next_++] = len;
  if (next_ == kNumTracked) {
    next_ = 0;
  }
}

size_t TailPrefetchStats::GetSuggestedPrefetchSize() {
  std::vector<size_t> sorted;
  {
    MutexLock l(&mutex_);

    if (num_records_ == 0) {
      return 0;
    }
    sorted.assign(records_, records_ + num_records_);
  }

  // Of the historic size, we find the maximum one that satisifis the condtiion
  // that if prefetching all, less than 1/8 will be wasted.
  std::sort(sorted.begin(), sorted.end());

  // Assuming we have 5 data points, and after sorting it looks like this:
  //
  //                                     +---+
  //                             +---+   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                    +---+    |   |   |   |
  //                    |   |    |   |   |   |
  //           +---+    |   |    |   |   |   |
  //           |   |    |   |    |   |   |   |
  //  +---+    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  +---+    +---+    +---+    +---+   +---+
  //
  // and we use every of the value as a candidate, and estimate how much we
  // wasted, compared to read. For example, when we use the 3rd record
  // as candiate. This area is what we read:
  //                                     +---+
  //                             +---+   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //  ***  ***  ***  ***+ ***  ***  *** *** **
  //  *                 |   |    |   |   |   |
  //           +---+    |   |    |   |   |   *
  //  *        |   |    |   |    |   |   |   |
  //  +---+    |   |    |   |    |   |   |   *
  //  *   |    |   |    | X |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   *
  //  *   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   *
  //  *   |    |   |    |   |    |   |   |   |
  //  *** *** ***-***  ***--*** ***--*** +****
  // which is (size of the record) X (number of records).
  //
  // While wasted is this area:
  //                                     +---+
  //                             +---+   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //                             |   |   |   |
  //  ***  ***  ***  ****---+    |   |   |   |
  //  *                 *   |    |   |   |   |
  //  *        *-***  ***   |    |   |   |   |
  //  *        *   |    |   |    |   |   |   |
  //  *--**  ***   |    |   |    |   |   |   |
  //  |   |    |   |    | X |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  +---+    +---+    +---+    +---+   +---+
  //
  // Which can be calculated iteratively.
  // The difference between wasted using 4st and 3rd record, will
  // be following area:
  //                                     +---+
  //  +--+  +-+   ++  +-+  +-+   +---+   |   |
  //  + xxxxxxxxxxxxxxxxxxxxxxxx |   |   |   |
  //    xxxxxxxxxxxxxxxxxxxxxxxx |   |   |   |
  //  + xxxxxxxxxxxxxxxxxxxxxxxx |   |   |   |
  //  | xxxxxxxxxxxxxxxxxxxxxxxx |   |   |   |
  //  +-+ +-+  +-+  ++  +---+ +--+   |   |   |
  //  |                 |   |    |   |   |   |
  //           +---+ ++ |   |    |   |   |   |
  //  |        |   |    |   |    | X |   |   |
  //  +---+ ++ |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  |   |    |   |    |   |    |   |   |   |
  //  +---+    +---+    +---+    +---+   +---+
  //
  // which will be the size difference between 4st and 3rd record,
  // times 3, which is number of records before the 4st.
  // Here we assume that all data within the prefetch range will be useful. In
  // reality, it may not be the case when a partial block is inside the range,
  // or there are data in the middle that is not read. We ignore those cases
  // for simplicity.
  assert(!sorted.empty());
  size_t prev_size = sorted[0];
  size_t max_qualified_size = sorted[0];
  size_t wasted = 0;
  for (size_t i = 1; i < sorted.size(); i++) {
    size_t read = sorted[i] * sorted.size();
    wasted += (sorted[i] - prev_size) * i;
    if (wasted <= read / 8) {
      max_qualified_size = sorted[i];
    }
    prev_size = sorted[i];
  }
  const size_t kMaxPrefetchSize = 512 * 1024;  // Never exceed 512KB
  return std::min(kMaxPrefetchSize, max_qualified_size);
}

#ifndef ROCKSDB_LITE

const std::string kOptNameMetadataCacheOpts = "metadata_cache_options";

static std::unordered_map<std::string, PinningTier>
    pinning_tier_type_string_map = {
        {"kFallback", PinningTier::kFallback},
        {"kNone", PinningTier::kNone},
        {"kFlushedAndSimilar", PinningTier::kFlushedAndSimilar},
        {"kAll", PinningTier::kAll}};

static std::unordered_map<std::string, BlockBasedTableOptions::IndexType>
    block_base_table_index_type_string_map = {
        {"kBinarySearch", BlockBasedTableOptions::IndexType::kBinarySearch},
        {"kHashSearch", BlockBasedTableOptions::IndexType::kHashSearch},
        {"kTwoLevelIndexSearch",
         BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch},
        {"kBinarySearchWithFirstKey",
         BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey}};

static std::unordered_map<std::string,
                          BlockBasedTableOptions::DataBlockIndexType>
    block_base_table_data_block_index_type_string_map = {
        {"kDataBlockBinarySearch",
         BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinarySearch},
        {"kDataBlockBinaryAndHash",
         BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash}};

static std::unordered_map<std::string,
                          BlockBasedTableOptions::IndexShorteningMode>
    block_base_table_index_shortening_mode_string_map = {
        {"kNoShortening",
         BlockBasedTableOptions::IndexShorteningMode::kNoShortening},
        {"kShortenSeparators",
         BlockBasedTableOptions::IndexShorteningMode::kShortenSeparators},
        {"kShortenSeparatorsAndSuccessor",
         BlockBasedTableOptions::IndexShorteningMode::
             kShortenSeparatorsAndSuccessor}};

static std::unordered_map<std::string, OptionTypeInfo>
    metadata_cache_options_type_info = {
        {"top_level_index_pinning",
         OptionTypeInfo::Enum<PinningTier>(
             offsetof(struct MetadataCacheOptions, top_level_index_pinning),
             &pinning_tier_type_string_map)},
        {"partition_pinning",
         OptionTypeInfo::Enum<PinningTier>(
             offsetof(struct MetadataCacheOptions, partition_pinning),
             &pinning_tier_type_string_map)},
        {"unpartitioned_pinning",
         OptionTypeInfo::Enum<PinningTier>(
             offsetof(struct MetadataCacheOptions, unpartitioned_pinning),
             &pinning_tier_type_string_map)}};

#endif  // ROCKSDB_LITE

static std::unordered_map<std::string, OptionTypeInfo>
    block_based_table_type_info = {
#ifndef ROCKSDB_LITE
        /* currently not supported
          std::shared_ptr<Cache> block_cache = nullptr;
          std::shared_ptr<Cache> block_cache_compressed = nullptr;
         */
        {"flush_block_policy_factory",
         {offsetof(struct BlockBasedTableOptions, flush_block_policy_factory),
          OptionType::kFlushBlockPolicyFactory, OptionVerificationType::kByName,
          OptionTypeFlags::kCompareNever}},
        {"cache_index_and_filter_blocks",
         {offsetof(struct BlockBasedTableOptions,
                   cache_index_and_filter_blocks),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"cache_index_and_filter_blocks_with_high_priority",
         {offsetof(struct BlockBasedTableOptions,
                   cache_index_and_filter_blocks_with_high_priority),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"pin_l0_filter_and_index_blocks_in_cache",
         {offsetof(struct BlockBasedTableOptions,
                   pin_l0_filter_and_index_blocks_in_cache),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"index_type", OptionTypeInfo::Enum<BlockBasedTableOptions::IndexType>(
                           offsetof(struct BlockBasedTableOptions, index_type),
                           &block_base_table_index_type_string_map)},
        {"hash_index_allow_collision",
         {offsetof(struct BlockBasedTableOptions, hash_index_allow_collision),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"data_block_index_type",
         OptionTypeInfo::Enum<BlockBasedTableOptions::DataBlockIndexType>(
             offsetof(struct BlockBasedTableOptions, data_block_index_type),
             &block_base_table_data_block_index_type_string_map)},
        {"index_shortening",
         OptionTypeInfo::Enum<BlockBasedTableOptions::IndexShorteningMode>(
             offsetof(struct BlockBasedTableOptions, index_shortening),
             &block_base_table_index_shortening_mode_string_map)},
        {"data_block_hash_table_util_ratio",
         {offsetof(struct BlockBasedTableOptions,
                   data_block_hash_table_util_ratio),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"checksum",
         {offsetof(struct BlockBasedTableOptions, checksum),
          OptionType::kChecksumType, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"no_block_cache",
         {offsetof(struct BlockBasedTableOptions, no_block_cache),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"block_size",
         {offsetof(struct BlockBasedTableOptions, block_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"block_size_deviation",
         {offsetof(struct BlockBasedTableOptions, block_size_deviation),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"block_restart_interval",
         {offsetof(struct BlockBasedTableOptions, block_restart_interval),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
        {"index_block_restart_interval",
         {offsetof(struct BlockBasedTableOptions, index_block_restart_interval),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"index_per_partition",
         {0, OptionType::kUInt64T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"metadata_block_size",
         {offsetof(struct BlockBasedTableOptions, metadata_block_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"partition_filters",
         {offsetof(struct BlockBasedTableOptions, partition_filters),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"optimize_filters_for_memory",
         {offsetof(struct BlockBasedTableOptions, optimize_filters_for_memory),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"filter_policy",
         {offsetof(struct BlockBasedTableOptions, filter_policy),
          OptionType::kUnknown, OptionVerificationType::kByNameAllowFromNull,
          OptionTypeFlags::kNone,
          // Parses the Filter policy
          [](const ConfigOptions& opts, const std::string&,
             const std::string& value, char* addr) {
            auto* policy =
                reinterpret_cast<std::shared_ptr<const FilterPolicy>*>(addr);
            return FilterPolicy::CreateFromString(opts, value, policy);
          },
          // Converts the FilterPolicy to its string representation
          [](const ConfigOptions&, const std::string&, const char* addr,
             std::string* value) {
            const auto* policy =
                reinterpret_cast<const std::shared_ptr<const FilterPolicy>*>(
                    addr);
            if (policy->get()) {
              *value = (*policy)->Name();
            } else {
              *value = kNullptrString;
            }
            return Status::OK();
          },
          // Compares two FilterPolicy objects for equality
          [](const ConfigOptions&, const std::string&, const char* addr1,
             const char* addr2, std::string*) {
            const auto* policy1 =
                reinterpret_cast<const std::shared_ptr<const FilterPolicy>*>(
                    addr1)
                    ->get();
            const auto* policy2 =
                reinterpret_cast<const std::shared_ptr<FilterPolicy>*>(addr2)
                    ->get();
            if (policy1 == policy2) {
              return true;
            } else if (policy1 != nullptr && policy2 != nullptr) {
              return (strcmp(policy1->Name(), policy2->Name()) == 0);
            } else {
              return false;
            }
          }}},
        {"whole_key_filtering",
         {offsetof(struct BlockBasedTableOptions, whole_key_filtering),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"skip_table_builder_flush",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone}},
        {"format_version",
         {offsetof(struct BlockBasedTableOptions, format_version),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"verify_compression",
         {offsetof(struct BlockBasedTableOptions, verify_compression),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"read_amp_bytes_per_bit",
         {offsetof(struct BlockBasedTableOptions, read_amp_bytes_per_bit),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone,
          [](const ConfigOptions& /*opts*/, const std::string& /*name*/,
             const std::string& value, char* addr) {
            // A workaround to fix a bug in 6.10, 6.11, 6.12, 6.13
            // and 6.14. The bug will write out 8 bytes to OPTIONS file from the
            // starting address of BlockBasedTableOptions.read_amp_bytes_per_bit
            // which is actually a uint32. Consequently, the value of
            // read_amp_bytes_per_bit written in the OPTIONS file is wrong.
            // From 6.15, RocksDB will try to parse the read_amp_bytes_per_bit
            // from OPTIONS file as a uint32. To be able to load OPTIONS file
            // generated by affected releases before the fix, we need to
            // manually parse read_amp_bytes_per_bit with this special hack.
            uint64_t read_amp_bytes_per_bit = ParseUint64(value);
            *(reinterpret_cast<uint32_t*>(addr)) =
                static_cast<uint32_t>(read_amp_bytes_per_bit);
            return Status::OK();
          }}},
        {"enable_index_compression",
         {offsetof(struct BlockBasedTableOptions, enable_index_compression),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"block_align",
         {offsetof(struct BlockBasedTableOptions, block_align),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"pin_top_level_index_and_filter",
         {offsetof(struct BlockBasedTableOptions,
                   pin_top_level_index_and_filter),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {kOptNameMetadataCacheOpts,
         OptionTypeInfo::Struct(
             kOptNameMetadataCacheOpts, &metadata_cache_options_type_info,
             offsetof(struct BlockBasedTableOptions, metadata_cache_options),
             OptionVerificationType::kNormal, OptionTypeFlags::kNone)},
        {"block_cache",
         {offsetof(struct BlockBasedTableOptions, block_cache),
          OptionType::kUnknown, OptionVerificationType::kNormal,
          (OptionTypeFlags::kCompareNever | OptionTypeFlags::kDontSerialize),
          // Parses the input vsalue as a Cache
          [](const ConfigOptions& opts, const std::string&,
             const std::string& value, char* addr) {
            auto* cache = reinterpret_cast<std::shared_ptr<Cache>*>(addr);
            return Cache::CreateFromString(opts, value, cache);
          }}},
        {"block_cache_compressed",
         {offsetof(struct BlockBasedTableOptions, block_cache_compressed),
          OptionType::kUnknown, OptionVerificationType::kNormal,
          (OptionTypeFlags::kCompareNever | OptionTypeFlags::kDontSerialize),
          // Parses the input vsalue as a Cache
          [](const ConfigOptions& opts, const std::string&,
             const std::string& value, char* addr) {
            auto* cache = reinterpret_cast<std::shared_ptr<Cache>*>(addr);
            return Cache::CreateFromString(opts, value, cache);
          }}},
        {"max_auto_readahead_size",
         {offsetof(struct BlockBasedTableOptions, max_auto_readahead_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kMutable}},
#endif  // ROCKSDB_LITE
};

// TODO(myabandeh): We should return an error instead of silently changing the
// options
BlockBasedTableFactory::BlockBasedTableFactory(
    const BlockBasedTableOptions& _table_options)
    : table_options_(_table_options) {
  InitializeOptions();
  ConfigurableHelper::RegisterOptions(*this, &table_options_,
                                      &block_based_table_type_info);
}

void BlockBasedTableFactory::InitializeOptions() {
  if (table_options_.flush_block_policy_factory == nullptr) {
    table_options_.flush_block_policy_factory.reset(
        new FlushBlockBySizePolicyFactory());
  }
  if (table_options_.no_block_cache) {
    table_options_.block_cache.reset();
  } else if (table_options_.block_cache == nullptr) {
    LRUCacheOptions co;
    co.capacity = 8 << 20;
    // It makes little sense to pay overhead for mid-point insertion while the
    // block size is only 8MB.
    co.high_pri_pool_ratio = 0.0;
    table_options_.block_cache = NewLRUCache(co);
  }
  if (table_options_.block_size_deviation < 0 ||
      table_options_.block_size_deviation > 100) {
    table_options_.block_size_deviation = 0;
  }
  if (table_options_.block_restart_interval < 1) {
    table_options_.block_restart_interval = 1;
  }
  if (table_options_.index_block_restart_interval < 1) {
    table_options_.index_block_restart_interval = 1;
  }
  if (table_options_.index_type == BlockBasedTableOptions::kHashSearch &&
      table_options_.index_block_restart_interval != 1) {
    // Currently kHashSearch is incompatible with index_block_restart_interval > 1
    table_options_.index_block_restart_interval = 1;
  }
  if (table_options_.partition_filters &&
      table_options_.index_type !=
          BlockBasedTableOptions::kTwoLevelIndexSearch) {
    // We do not support partitioned filters without partitioning indexes
    table_options_.partition_filters = false;
  }
}

Status BlockBasedTableFactory::PrepareOptions(const ConfigOptions& opts) {
  InitializeOptions();
  return TableFactory::PrepareOptions(opts);
}

Status BlockBasedTableFactory::NewTableReader(
    const ReadOptions& ro, const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table_reader,
    bool prefetch_index_and_filter_in_cache) const {
  return BlockBasedTable::Open(
      ro, table_reader_options.ioptions, table_reader_options.env_options,
      table_options_, table_reader_options.internal_comparator, std::move(file),
      file_size, table_reader, table_reader_options.prefix_extractor,
      prefetch_index_and_filter_in_cache, table_reader_options.skip_filters,
      table_reader_options.level, table_reader_options.immortal,
      table_reader_options.largest_seqno,
      table_reader_options.force_direct_prefetch, &tail_prefetch_stats_,
      table_reader_options.block_cache_tracer,
      table_reader_options.max_file_size_for_l0_meta_pin);
}

TableBuilder* BlockBasedTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    WritableFileWriter* file) const {
  auto table_builder = new BlockBasedTableBuilder(
      table_builder_options.ioptions, table_builder_options.moptions,
      table_options_, table_builder_options.internal_comparator,
      table_builder_options.int_tbl_prop_collector_factories, column_family_id,
      file, table_builder_options.compression_type,
      table_builder_options.sample_for_compression,
      table_builder_options.compression_opts,
      table_builder_options.skip_filters,
      table_builder_options.column_family_name, table_builder_options.level,
      table_builder_options.creation_time,
      table_builder_options.oldest_key_time,
      table_builder_options.target_file_size,
      table_builder_options.file_creation_time, table_builder_options.db_id,
      table_builder_options.db_session_id);

  return table_builder;
}

Status BlockBasedTableFactory::ValidateOptions(
    const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts) const {
  if (table_options_.index_type == BlockBasedTableOptions::kHashSearch &&
      cf_opts.prefix_extractor == nullptr) {
    return Status::InvalidArgument(
        "Hash index is specified for block-based "
        "table, but prefix_extractor is not given");
  }
  if (table_options_.cache_index_and_filter_blocks &&
      table_options_.no_block_cache) {
    return Status::InvalidArgument(
        "Enable cache_index_and_filter_blocks, "
        ", but block cache is disabled");
  }
  if (table_options_.pin_l0_filter_and_index_blocks_in_cache &&
      table_options_.no_block_cache) {
    return Status::InvalidArgument(
        "Enable pin_l0_filter_and_index_blocks_in_cache, "
        ", but block cache is disabled");
  }
  if (!BlockBasedTableSupportedVersion(table_options_.format_version)) {
    return Status::InvalidArgument(
        "Unsupported BlockBasedTable format_version. Please check "
        "include/rocksdb/table.h for more info");
  }
  if (table_options_.block_align && (cf_opts.compression != kNoCompression)) {
    return Status::InvalidArgument(
        "Enable block_align, but compression "
        "enabled");
  }
  if (table_options_.block_align &&
      (table_options_.block_size & (table_options_.block_size - 1))) {
    return Status::InvalidArgument(
        "Block alignment requested but block size is not a power of 2");
  }
  if (table_options_.block_size > port::kMaxUint32) {
    return Status::InvalidArgument(
        "block size exceeds maximum number (4GiB) allowed");
  }
  if (table_options_.data_block_index_type ==
          BlockBasedTableOptions::kDataBlockBinaryAndHash &&
      table_options_.data_block_hash_table_util_ratio <= 0) {
    return Status::InvalidArgument(
        "data_block_hash_table_util_ratio should be greater than 0 when "
        "data_block_index_type is set to kDataBlockBinaryAndHash");
  }
  if (db_opts.unordered_write && cf_opts.max_successive_merges > 0) {
    // TODO(myabandeh): support it
    return Status::InvalidArgument(
        "max_successive_merges larger than 0 is currently inconsistent with "
        "unordered_write");
  }
  return TableFactory::ValidateOptions(db_opts, cf_opts);
}

std::string BlockBasedTableFactory::GetPrintableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  flush_block_policy_factory: %s (%p)\n",
           table_options_.flush_block_policy_factory->Name(),
           static_cast<void*>(table_options_.flush_block_policy_factory.get()));
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  cache_index_and_filter_blocks: %d\n",
           table_options_.cache_index_and_filter_blocks);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  cache_index_and_filter_blocks_with_high_priority: %d\n",
           table_options_.cache_index_and_filter_blocks_with_high_priority);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  pin_l0_filter_and_index_blocks_in_cache: %d\n",
           table_options_.pin_l0_filter_and_index_blocks_in_cache);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  pin_top_level_index_and_filter: %d\n",
           table_options_.pin_top_level_index_and_filter);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_type: %d\n",
           table_options_.index_type);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  data_block_index_type: %d\n",
           table_options_.data_block_index_type);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_shortening: %d\n",
           static_cast<int>(table_options_.index_shortening));
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  data_block_hash_table_util_ratio: %lf\n",
           table_options_.data_block_hash_table_util_ratio);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  hash_index_allow_collision: %d\n",
           table_options_.hash_index_allow_collision);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  checksum: %d\n", table_options_.checksum);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  no_block_cache: %d\n",
           table_options_.no_block_cache);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_cache: %p\n",
           static_cast<void*>(table_options_.block_cache.get()));
  ret.append(buffer);
  if (table_options_.block_cache) {
    const char* block_cache_name = table_options_.block_cache->Name();
    if (block_cache_name != nullptr) {
      snprintf(buffer, kBufferSize, "  block_cache_name: %s\n",
               block_cache_name);
      ret.append(buffer);
    }
    ret.append("  block_cache_options:\n");
    ret.append(table_options_.block_cache->GetPrintableOptions());
  }
  snprintf(buffer, kBufferSize, "  block_cache_compressed: %p\n",
           static_cast<void*>(table_options_.block_cache_compressed.get()));
  ret.append(buffer);
  if (table_options_.block_cache_compressed) {
    const char* block_cache_compressed_name =
        table_options_.block_cache_compressed->Name();
    if (block_cache_compressed_name != nullptr) {
      snprintf(buffer, kBufferSize, "  block_cache_name: %s\n",
               block_cache_compressed_name);
      ret.append(buffer);
    }
    ret.append("  block_cache_compressed_options:\n");
    ret.append(table_options_.block_cache_compressed->GetPrintableOptions());
  }
  snprintf(buffer, kBufferSize, "  persistent_cache: %p\n",
           static_cast<void*>(table_options_.persistent_cache.get()));
  ret.append(buffer);
  if (table_options_.persistent_cache) {
    snprintf(buffer, kBufferSize, "  persistent_cache_options:\n");
    ret.append(buffer);
    ret.append(table_options_.persistent_cache->GetPrintableOptions());
  }
  snprintf(buffer, kBufferSize, "  block_size: %" ROCKSDB_PRIszt "\n",
           table_options_.block_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_size_deviation: %d\n",
           table_options_.block_size_deviation);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_restart_interval: %d\n",
           table_options_.block_restart_interval);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_block_restart_interval: %d\n",
           table_options_.index_block_restart_interval);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  metadata_block_size: %" PRIu64 "\n",
           table_options_.metadata_block_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  partition_filters: %d\n",
           table_options_.partition_filters);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  use_delta_encoding: %d\n",
           table_options_.use_delta_encoding);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  filter_policy: %s\n",
           table_options_.filter_policy == nullptr
               ? "nullptr"
               : table_options_.filter_policy->Name());
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  whole_key_filtering: %d\n",
           table_options_.whole_key_filtering);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  verify_compression: %d\n",
           table_options_.verify_compression);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  read_amp_bytes_per_bit: %d\n",
           table_options_.read_amp_bytes_per_bit);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  format_version: %d\n",
           table_options_.format_version);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  enable_index_compression: %d\n",
           table_options_.enable_index_compression);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_align: %d\n",
           table_options_.block_align);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  max_auto_readahead_size: %" ROCKSDB_PRIszt "\n",
           table_options_.max_auto_readahead_size);
  return ret;
}

const void* BlockBasedTableFactory::GetOptionsPtr(
    const std::string& name) const {
  if (name == kBlockCacheOpts()) {
    if (table_options_.no_block_cache) {
      return nullptr;
    } else {
      return table_options_.block_cache.get();
    }
  } else {
    return TableFactory::GetOptionsPtr(name);
  }
}

#ifndef ROCKSDB_LITE
// Take a default BlockBasedTableOptions "table_options" in addition to a
// map "opts_map" of option name to option value to construct the new
// BlockBasedTableOptions "new_table_options".
//
// Below are the instructions of how to config some non-primitive-typed
// options in BlockBasedTableOptions:
//
// * filter_policy:
//   We currently only support the following FilterPolicy in the convenience
//   functions:
//   - BloomFilter: use "bloomfilter:[bits_per_key]:[use_block_based_builder]"
//     to specify BloomFilter.  The above string is equivalent to calling
//     NewBloomFilterPolicy(bits_per_key, use_block_based_builder).
//     [Example]:
//     - Pass {"filter_policy", "bloomfilter:4:true"} in
//       GetBlockBasedTableOptionsFromMap to use a BloomFilter with 4-bits
//       per key and use_block_based_builder enabled.
//
// * block_cache / block_cache_compressed:
//   We currently only support LRU cache in the GetOptions API.  The LRU
//   cache can be set by directly specifying its size.
//   [Example]:
//   - Passing {"block_cache", "1M"} in GetBlockBasedTableOptionsFromMap is
//     equivalent to setting block_cache using NewLRUCache(1024 * 1024).
//
// @param table_options the default options of the output "new_table_options".
// @param opts_map an option name to value map for specifying how
//     "new_table_options" should be set.
// @param new_table_options the resulting options based on "table_options"
//     with the change specified in "opts_map".
// @param input_strings_escaped when set to true, each escaped characters
//     prefixed by '\' in the values of the opts_map will be further converted
//     back to the raw string before assigning to the associated options.
// @param ignore_unknown_options when set to true, unknown options are ignored
//     instead of resulting in an unknown-option error.
// @return Status::OK() on success.  Otherwise, a non-ok status indicating
//     error will be returned, and "new_table_options" will be set to
//     "table_options".
Status BlockBasedTableFactory::ParseOption(const ConfigOptions& config_options,
                                           const OptionTypeInfo& opt_info,
                                           const std::string& opt_name,
                                           const std::string& opt_value,
                                           void* opt_ptr) {
  Status status = TableFactory::ParseOption(config_options, opt_info, opt_name,
                                            opt_value, opt_ptr);
  if (config_options.input_strings_escaped && !status.ok()) {  // Got an error
    // !input_strings_escaped indicates the old API, where everything is
    // parsable.
    if (opt_info.IsByName()) {
      status = Status::OK();
    }
  }
  return status;
}

Status GetBlockBasedTableOptionsFromString(
    const BlockBasedTableOptions& table_options, const std::string& opts_str,
    BlockBasedTableOptions* new_table_options) {
  ConfigOptions config_options;
  config_options.input_strings_escaped = false;
  config_options.ignore_unknown_options = false;
  config_options.invoke_prepare_options = false;
  return GetBlockBasedTableOptionsFromString(config_options, table_options,
                                             opts_str, new_table_options);
}
Status GetBlockBasedTableOptionsFromString(
    const ConfigOptions& config_options,
    const BlockBasedTableOptions& table_options, const std::string& opts_str,
    BlockBasedTableOptions* new_table_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }
  s = GetBlockBasedTableOptionsFromMap(config_options, table_options, opts_map,
                                       new_table_options);
  // Translate any errors (NotFound, NotSupported, to InvalidArgument
  if (s.ok() || s.IsInvalidArgument()) {
    return s;
  } else {
    return Status::InvalidArgument(s.getState());
  }
}

Status GetBlockBasedTableOptionsFromMap(
    const BlockBasedTableOptions& table_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    BlockBasedTableOptions* new_table_options, bool input_strings_escaped,
    bool ignore_unknown_options) {
  ConfigOptions config_options;
  config_options.input_strings_escaped = input_strings_escaped;
  config_options.ignore_unknown_options = ignore_unknown_options;
  config_options.invoke_prepare_options = false;

  return GetBlockBasedTableOptionsFromMap(config_options, table_options,
                                          opts_map, new_table_options);
}

Status GetBlockBasedTableOptionsFromMap(
    const ConfigOptions& config_options,
    const BlockBasedTableOptions& table_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    BlockBasedTableOptions* new_table_options) {
  assert(new_table_options);
  BlockBasedTableFactory bbtf(table_options);
  Status s = bbtf.ConfigureFromMap(config_options, opts_map);
  if (s.ok()) {
    *new_table_options = *(bbtf.GetOptions<BlockBasedTableOptions>());
  } else {
    *new_table_options = table_options;
  }
  return s;
}
#endif  // !ROCKSDB_LITE

TableFactory* NewBlockBasedTableFactory(
    const BlockBasedTableOptions& _table_options) {
  return new BlockBasedTableFactory(_table_options);
}

const std::string BlockBasedTablePropertyNames::kIndexType =
    "rocksdb.block.based.table.index.type";
const std::string BlockBasedTablePropertyNames::kWholeKeyFiltering =
    "rocksdb.block.based.table.whole.key.filtering";
const std::string BlockBasedTablePropertyNames::kPrefixFiltering =
    "rocksdb.block.based.table.prefix.filtering";
const std::string kHashIndexPrefixesBlock = "rocksdb.hashindex.prefixes";
const std::string kHashIndexPrefixesMetadataBlock =
    "rocksdb.hashindex.metadata";
const std::string kPropTrue = "1";
const std::string kPropFalse = "0";

}  // namespace ROCKSDB_NAMESPACE
