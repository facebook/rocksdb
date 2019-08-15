// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE
#include "table/plain/plain_table_factory.h"

#include <stdint.h>
#include <memory>
#include "db/dbformat.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "table/plain/plain_table_builder.h"
#include "table/plain/plain_table_reader.h"
#include "util/string_util.h"

namespace rocksdb {
const std::string PlainTableFactory::kPlainTablePrefix = "rocksdb.table.plain.";
static std::unordered_map<std::string, OptionTypeInfo> plain_table_type_info = {
#ifndef ROCKSDB_LITE
    {"user_key_len",
     {offsetof(struct PlainTableOptions, user_key_len), OptionType::kUInt32T,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"bloom_bits_per_key",
     {offsetof(struct PlainTableOptions, bloom_bits_per_key), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"hash_table_ratio",
     {offsetof(struct PlainTableOptions, hash_table_ratio), OptionType::kDouble,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"index_sparseness",
     {offsetof(struct PlainTableOptions, index_sparseness), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"huge_page_tlb_size",
     {offsetof(struct PlainTableOptions, huge_page_tlb_size),
      OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0}},
    {"encoding_type",
     {offsetof(struct PlainTableOptions, encoding_type),
      OptionType::kEncodingType, OptionVerificationType::kNormal,
      OptionTypeFlags::kEnum, 0}},
    {"full_scan_mode",
     {offsetof(struct PlainTableOptions, full_scan_mode), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone, 0}},
    {"store_index_in_file",
     {offsetof(struct PlainTableOptions, store_index_in_file),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone, 0}},
#endif  // ROCKSDB_LITE
};

PlainTableFactory::PlainTableFactory(const PlainTableOptions& options)
    : table_options_(options) {
  RegisterOptionsMap(kPlainTableOpts, &table_options_, plain_table_type_info);
}

Status PlainTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table,
    bool /*prefetch_index_and_filter_in_cache*/) const {
  return PlainTableReader::Open(
      table_reader_options.ioptions, table_reader_options.env_options,
      table_reader_options.internal_comparator, std::move(file), file_size,
      table, table_options_.bloom_bits_per_key, table_options_.hash_table_ratio,
      table_options_.index_sparseness, table_options_.huge_page_tlb_size,
      table_options_.full_scan_mode, table_reader_options.immortal,
      table_reader_options.prefix_extractor);
}

TableBuilder* PlainTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    WritableFileWriter* file) const {
  // Ignore the skip_filters flag. PlainTable format is optimized for small
  // in-memory dbs. The skip_filters optimization is not useful for plain
  // tables
  //
  return new PlainTableBuilder(
      table_builder_options.ioptions, table_builder_options.moptions,
      table_builder_options.int_tbl_prop_collector_factories, column_family_id,
      file, table_options_.user_key_len, table_options_.encoding_type,
      table_options_.index_sparseness, table_options_.bloom_bits_per_key,
      table_builder_options.column_family_name, 6,
      table_options_.huge_page_tlb_size, table_options_.hash_table_ratio,
      table_options_.store_index_in_file);
}

Status GetMemTableRepFactoryFromString(
    const std::string& opts_str,
    std::unique_ptr<MemTableRepFactory>* new_mem_factory) {
  std::vector<std::string> opts_list = StringSplit(opts_str, ':');
  size_t len = opts_list.size();

  if (opts_list.empty() || opts_list.size() > 2) {
    return Status::InvalidArgument("Can't parse memtable_factory option ",
                                   opts_str);
  }

  MemTableRepFactory* mem_factory = nullptr;

  if (opts_list[0] == "skip_list" || opts_list[0] == "SkipListFactory") {
    // Expecting format
    // skip_list:<lookahead>
    if (2 == len) {
      size_t lookahead = ParseSizeT(opts_list[1]);
      mem_factory = new SkipListFactory(lookahead);
    } else if (1 == len) {
      mem_factory = new SkipListFactory();
    }
  } else if (opts_list[0] == "prefix_hash" ||
             opts_list[0] == "HashSkipListRepFactory") {
    // Expecting format
    // prfix_hash:<hash_bucket_count>
    if (2 == len) {
      size_t hash_bucket_count = ParseSizeT(opts_list[1]);
      mem_factory = NewHashSkipListRepFactory(hash_bucket_count);
    } else if (1 == len) {
      mem_factory = NewHashSkipListRepFactory();
    }
  } else if (opts_list[0] == "hash_linkedlist" ||
             opts_list[0] == "HashLinkListRepFactory") {
    // Expecting format
    // hash_linkedlist:<hash_bucket_count>
    if (2 == len) {
      size_t hash_bucket_count = ParseSizeT(opts_list[1]);
      mem_factory = NewHashLinkListRepFactory(hash_bucket_count);
    } else if (1 == len) {
      mem_factory = NewHashLinkListRepFactory();
    }
  } else if (opts_list[0] == "vector" || opts_list[0] == "VectorRepFactory") {
    // Expecting format
    // vector:<count>
    if (2 == len) {
      size_t count = ParseSizeT(opts_list[1]);
      mem_factory = new VectorRepFactory(count);
    } else if (1 == len) {
      mem_factory = new VectorRepFactory();
    }
  } else if (opts_list[0] == "cuckoo") {
    return Status::NotSupported(
        "cuckoo hash memtable is not supported anymore.");
  } else {
    return Status::InvalidArgument("Unrecognized memtable_factory option ",
                                   opts_str);
  }

  if (mem_factory != nullptr) {
    new_mem_factory->reset(mem_factory);
  }

  return Status::OK();
}

extern TableFactory* NewPlainTableFactory(const PlainTableOptions& options) {
  return new PlainTableFactory(options);
}

const std::string PlainTablePropertyNames::kEncodingType =
    "rocksdb.plain.table.encoding.type";

const std::string PlainTablePropertyNames::kBloomVersion =
    "rocksdb.plain.table.bloom.version";

const std::string PlainTablePropertyNames::kNumBloomBlocks =
    "rocksdb.plain.table.bloom.numblocks";

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
