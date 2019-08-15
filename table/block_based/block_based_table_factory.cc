//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cinttypes>
#include <stdint.h>

#include <memory>
#include <string>

#include "options/options_helper.h"
#include "options/options_parser.h"
#include "options/options_sanity_check.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/flush_block_policy.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/format.h"
#include "util/mutexlock.h"
#include "util/string_util.h"

namespace rocksdb {
const std::string BlockBasedTableFactory::kBlockTablePrefix =
    "rocksdb.table.block_based.";
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

static std::unordered_map<std::string, OptionTypeInfo>
    block_based_table_type_info = {
#ifndef ROCKSDB_LITE
        /* currently not supported
          std::shared_ptr<Cache> block_cache = nullptr;
          std::shared_ptr<Cache> block_cache_compressed = nullptr;
         */
        {"flush_block_policy_factory",
         {offsetof(struct BlockBasedTableOptions, flush_block_policy_factory),
          OptionType::kUnknown, OptionVerificationType::kByName,
          OptionTypeFlags::kNone, 0,
          [](const DBOptions&, const std::string&, char*, const std::string&) {
            // Currently, we do not do anything to create a flush block policy
            // factory
            return Status::OK();
          },
          [](uint32_t, const std::string&, const char* addr,
             std::string* value) {
            const auto factory =
                *(reinterpret_cast<
                    const std::shared_ptr<FlushBlockPolicyFactory>*>(addr));
            if (factory) {
              *value = factory->Name();
            } else {
              *value = kNullptrString;
            }
            return Status::OK();
          },
          [](OptionsSanityCheckLevel, const std::string&, const char*,
             const char*) {
            return true;  //**TODO: In the original code, we never bother to
                          // check that
            // the factories are equal
          }}},
        {"cache_index_and_filter_blocks",
         {offsetof(struct BlockBasedTableOptions,
                   cache_index_and_filter_blocks),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"cache_index_and_filter_blocks_with_high_priority",
         {offsetof(struct BlockBasedTableOptions,
                   cache_index_and_filter_blocks_with_high_priority),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"pin_l0_filter_and_index_blocks_in_cache",
         {offsetof(struct BlockBasedTableOptions,
                   pin_l0_filter_and_index_blocks_in_cache),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"index_type",
         {offsetof(struct BlockBasedTableOptions, index_type),
          OptionType::kBlockBasedTableIndexType,
          OptionVerificationType::kNormal, OptionTypeFlags::kEnum, 0}},
        {"hash_index_allow_collision",
         {offsetof(struct BlockBasedTableOptions, hash_index_allow_collision),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"data_block_index_type",
         {offsetof(struct BlockBasedTableOptions, data_block_index_type),
          OptionType::kBlockBasedTableDataBlockIndexType,
          OptionVerificationType::kNormal, OptionTypeFlags::kEnum, 0}},
        {"index_shortening",
         {offsetof(struct BlockBasedTableOptions, index_shortening),
          OptionType::kBlockBasedTableIndexShorteningMode,
          OptionVerificationType::kNormal, OptionTypeFlags::kEnum, 0}},
        {"data_block_hash_table_util_ratio",
         {offsetof(struct BlockBasedTableOptions,
                   data_block_hash_table_util_ratio),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"checksum",
         {offsetof(struct BlockBasedTableOptions, checksum),
          OptionType::kChecksumType, OptionVerificationType::kNormal,
          OptionTypeFlags::kEnum, 0}},
        {"no_block_cache",
         {offsetof(struct BlockBasedTableOptions, no_block_cache),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"block_size",
         {offsetof(struct BlockBasedTableOptions, block_size),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"block_size_deviation",
         {offsetof(struct BlockBasedTableOptions, block_size_deviation),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"block_restart_interval",
         {offsetof(struct BlockBasedTableOptions, block_restart_interval),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"index_block_restart_interval",
         {offsetof(struct BlockBasedTableOptions, index_block_restart_interval),
          OptionType::kInt, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"index_per_partition",
         {0, OptionType::kUInt64T, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone, 0}},
        {"metadata_block_size",
         {offsetof(struct BlockBasedTableOptions, metadata_block_size),
          OptionType::kUInt64T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"partition_filters",
         {offsetof(struct BlockBasedTableOptions, partition_filters),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"filter_policy",
         {offsetof(struct BlockBasedTableOptions, filter_policy),
          OptionType::kUnknown, OptionVerificationType::kByName,
          OptionTypeFlags::kNone, 0}},
        {"whole_key_filtering",
         {offsetof(struct BlockBasedTableOptions, whole_key_filtering),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"skip_table_builder_flush",
         {0, OptionType::kBoolean, OptionVerificationType::kDeprecated,
          OptionTypeFlags::kNone, 0}},
        {"format_version",
         {offsetof(struct BlockBasedTableOptions, format_version),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"verify_compression",
         {offsetof(struct BlockBasedTableOptions, verify_compression),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"read_amp_bytes_per_bit",
         {offsetof(struct BlockBasedTableOptions, read_amp_bytes_per_bit),
          OptionType::kSizeT, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"enable_index_compression",
         {offsetof(struct BlockBasedTableOptions, enable_index_compression),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"block_align",
         {offsetof(struct BlockBasedTableOptions, block_align),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"pin_top_level_index_and_filter",
         {offsetof(struct BlockBasedTableOptions,
                   pin_top_level_index_and_filter),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
#endif  // !ROCKSDB_LITE
};

BlockBasedTableFactory::BlockBasedTableFactory(
    const BlockBasedTableOptions& _table_options)
    : table_options_(_table_options) {
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
  if (table_options_.partition_filters &&
      table_options_.index_type !=
          BlockBasedTableOptions::kTwoLevelIndexSearch) {
    // We do not support partitioned filters without partitioning indexes
    table_options_.partition_filters = false;
  }
  RegisterOptionsMap(kBlockBasedTableOpts, &table_options_,
                     block_based_table_type_info);
}
#ifndef ROCKSDB_LITE
const std::unordered_map<std::string, OptionsSanityCheckLevel>*
BlockBasedTableFactory::GetOptionsSanityCheckLevel(
    const std::string& name) const {
  static std::unordered_map<std::string, OptionsSanityCheckLevel> skipped = {
      {"filter_policy", OptionsSanityCheckLevel::kSanityLevelNone},
  };
  if (name == kBlockBasedTableOpts) {
    return &skipped;
  } else {
    return TableFactory::GetOptionsSanityCheckLevel(name);
  }
}
#endif  // ROCKSDB_LITE

Status BlockBasedTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table_reader,
    bool prefetch_index_and_filter_in_cache) const {
  return BlockBasedTable::Open(
      table_reader_options.ioptions, table_reader_options.env_options,
      table_options_, table_reader_options.internal_comparator, std::move(file),
      file_size, table_reader, table_reader_options.prefix_extractor,
      prefetch_index_and_filter_in_cache, table_reader_options.skip_filters,
      table_reader_options.level, table_reader_options.immortal,
      table_reader_options.largest_seqno, &tail_prefetch_stats_,
      table_reader_options.block_cache_tracer);
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
      table_builder_options.column_family_name,
      table_builder_options.creation_time,
      table_builder_options.oldest_key_time,
      table_builder_options.target_file_size,
      table_builder_options.file_creation_time);

  return table_builder;
}

Status BlockBasedTableFactory::Validate(
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
  return TableFactory::Validate(db_opts, cf_opts);
}

const void* BlockBasedTableFactory::GetOptionsPtr(
    const std::string& name) const {
  if (name == "BlockCache") {
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
Status BlockBasedTableFactory::UnknownToString(uint32_t mode,
                                               const std::string& name,
                                               std::string* value) const {
  Status status;
  if (name == "filter_policy") {
    if (table_options_.filter_policy) {
      *value = table_options_.filter_policy->Name();
    } else {
      *value = kNullptrString;
    }
  } else {
    status = TableFactory::UnknownToString(mode, name, value);
  }
  return status;
}

bool BlockBasedTableFactory::IsUnknownEqual(
    const std::string& name, const OptionTypeInfo& /* type_info */,
    OptionsSanityCheckLevel /* sanity_check_level */, const char* thisOffset,
    const char* thatOffset) const {
  if (name == "filter_policy") {
    const auto* thisOne =
        reinterpret_cast<const std::shared_ptr<FilterPolicy>*>(thisOffset)
            ->get();
    const auto* thatOne =
        reinterpret_cast<const std::shared_ptr<FilterPolicy>*>(thatOffset)
            ->get();
    if (thisOne == thatOne) {
      return true;
    } else if (thisOne != nullptr) {
      return thatOne == nullptr ||
             strcmp(thisOne->Name(), thatOne->Name()) == 0;
    } else {
      return false;
    }
  } else {
    return false;
  }
}

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
Status BlockBasedTableFactory::SetUnknown(const DBOptions& db_opts,
                                          const std::string& name,
                                          const std::string& value) {
  Status status = Status::OK();
  if (name == "block_cache" || name == "block_cache_compressed") {
    // cache options can be specified in the following format
    //   "block_cache={capacity=1M;num_shard_bits=4;
    //    strict_capacity_limit=true;high_pri_pool_ratio=0.5;}"
    // To support backward compatibility, the following format
    // is also supported.
    //   "block_cache=1M"
    std::shared_ptr<Cache> cache;
    // block_cache is specified in format block_cache=<cache_size>.
    if (value.find('=') == std::string::npos) {
      cache = NewLRUCache(ParseSizeT(value));
    } else {
      LRUCacheOptions cache_opts;
      OptionTypeInfo cache_info{0, OptionType::kLRUCacheOptions,
                                OptionVerificationType::kNormal,
                                OptionTypeFlags::kNone, 0};
      if (!ParseOptionHelper(reinterpret_cast<char*>(&cache_opts), cache_info,
                             value)) {
        return Status::InvalidArgument("Invalid cache options");
      }
      cache = NewLRUCache(cache_opts);
    }
    if (name == "block_cache") {
      table_options_.block_cache = cache;
    } else {
      table_options_.block_cache_compressed = cache;
    }
  } else if (name == "filter_policy") {
    // Expect the following format
    // bloomfilter:int:bool
    const std::string kBloomName = "bloomfilter:";
    if (value == kNullptrString || value == "rocksdb.BuiltinBloomFilter") {
      table_options_.filter_policy.reset();
    } else if (value.compare(0, kBloomName.size(), kBloomName) == 0) {
      size_t pos = value.find(':', kBloomName.size());
      if (pos == std::string::npos) {
        status = Status::InvalidArgument(
            "Invalid filter policy config, missing bits_per_key");
      } else {
        int bits_per_key = ParseInt(
            trim(value.substr(kBloomName.size(), pos - kBloomName.size())));
        bool use_block_based_builder = ParseBoolean(
            "use_block_based_builder", trim(value.substr(pos + 1)));
        table_options_.filter_policy.reset(
            NewBloomFilterPolicy(bits_per_key, use_block_based_builder));
      }
    } else {
      status = Status::InvalidArgument("Invalid filter policy name ", value);
    }
  } else {
    status = TableFactory::SetUnknown(db_opts, name, value);
  }
  return status;
}

Status BlockBasedTableFactory::ParseOption(const OptionTypeInfo& opt_info,
                                           const DBOptions& db_opts,
                                           void* opt_ptr,
                                           const std::string& opt_name,
                                           const std::string& opt_value,
                                           bool input_strings_escaped) {
  Status status = TableFactory::ParseOption(
      opt_info, db_opts, opt_ptr, opt_name, opt_value, input_strings_escaped);
  if (input_strings_escaped && !status.ok()) {  // Got an error
    // !input_strings_escaped indicates the old API, where everything is
    // parsable.
    if (opt_info.verification == OptionVerificationType::kByName ||
        opt_info.verification == OptionVerificationType::kByNameAllowNull ||
        opt_info.verification == OptionVerificationType::kByNameAllowFromNull) {
      status = Status::OK();
    }
  }
  return status;
}
#endif  // ROCKSDB_LITE

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

}  // namespace rocksdb
