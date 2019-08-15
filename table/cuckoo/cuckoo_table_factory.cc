// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "table/cuckoo/cuckoo_table_factory.h"

#include "db/dbformat.h"
#include "table/cuckoo/cuckoo_table_builder.h"
#include "table/cuckoo/cuckoo_table_reader.h"

namespace rocksdb {
const std::string CuckooTableFactory::kCuckooTablePrefix =
    "rocksdb.table.cuckoo.";

Status CuckooTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table,
    bool /*prefetch_index_and_filter_in_cache*/) const {
  std::unique_ptr<CuckooTableReader> new_reader(new CuckooTableReader(
      table_reader_options.ioptions, std::move(file), file_size,
      table_reader_options.internal_comparator.user_comparator(), nullptr));
  Status s = new_reader->status();
  if (s.ok()) {
    *table = std::move(new_reader);
  }
  return s;
}

TableBuilder* CuckooTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    WritableFileWriter* file) const {
  // Ignore the skipFIlters flag. Does not apply to this file format
  //

  // TODO: change builder to take the option struct
  return new CuckooTableBuilder(
      file, table_options_.hash_table_ratio, 64,
      table_options_.max_search_depth,
      table_builder_options.internal_comparator.user_comparator(),
      table_options_.cuckoo_block_size, table_options_.use_module_hash,
      table_options_.identity_as_first_hash, nullptr /* get_slice_hash */,
      column_family_id, table_builder_options.column_family_name);
}

static std::unordered_map<std::string, OptionTypeInfo> cuckoo_table_type_info =
    {
#ifndef ROCKSDB_LITE
        {"hash_table_ratio",
         {offsetof(struct CuckooTableOptions, hash_table_ratio),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"max_search_depth",
         {offsetof(struct CuckooTableOptions, max_search_depth),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"cuckoo_block_size",
         {offsetof(struct CuckooTableOptions, cuckoo_block_size),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"identity_as_first_hash",
         {offsetof(struct CuckooTableOptions, identity_as_first_hash),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
        {"use_module_hash",
         {offsetof(struct CuckooTableOptions, use_module_hash),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone, 0}},
#endif  // ROCKSDB_LITE
};

CuckooTableFactory::CuckooTableFactory(const CuckooTableOptions& table_options)
    : table_options_(table_options) {
  RegisterOptionsMap(kCuckooTableOpts, &table_options_, cuckoo_table_type_info);
}

TableFactory* NewCuckooTableFactory(const CuckooTableOptions& table_options) {
  return new CuckooTableFactory(table_options);
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
