// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE
#include "table/plain_table_factory.h"

#include <memory>
#include <stdint.h>
#include "db/dbformat.h"
#include "table/plain_table_builder.h"
#include "table/plain_table_reader.h"
#include "port/port.h"

namespace rocksdb {

Status PlainTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    unique_ptr<TableReader>* table,
    bool prefetch_index_and_filter_in_cache) const {
  return PlainTableReader::Open(
      table_reader_options.ioptions, table_reader_options.env_options,
      table_reader_options.internal_comparator, std::move(file), file_size,
      table, table_options_.bloom_bits_per_key, table_options_.hash_table_ratio,
      table_options_.index_sparseness, table_options_.huge_page_tlb_size,
      table_options_.full_scan_mode);
}

TableBuilder* PlainTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    WritableFileWriter* file) const {
  // Ignore the skip_filters flag. PlainTable format is optimized for small
  // in-memory dbs. The skip_filters optimization is not useful for plain
  // tables
  //
  return new PlainTableBuilder(
      table_builder_options.ioptions,
      table_builder_options.int_tbl_prop_collector_factories, column_family_id,
      file, table_options_.user_key_len, table_options_.encoding_type,
      table_options_.index_sparseness, table_options_.bloom_bits_per_key,
      table_builder_options.column_family_name, 6,
      table_options_.huge_page_tlb_size, table_options_.hash_table_ratio,
      table_options_.store_index_in_file);
}

std::string PlainTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  user_key_len: %u\n",
           table_options_.user_key_len);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  bloom_bits_per_key: %d\n",
           table_options_.bloom_bits_per_key);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  hash_table_ratio: %lf\n",
           table_options_.hash_table_ratio);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_sparseness: %" ROCKSDB_PRIszt "\n",
           table_options_.index_sparseness);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  huge_page_tlb_size: %" ROCKSDB_PRIszt "\n",
           table_options_.huge_page_tlb_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  encoding_type: %d\n",
           table_options_.encoding_type);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  full_scan_mode: %d\n",
           table_options_.full_scan_mode);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  store_index_in_file: %d\n",
           table_options_.store_index_in_file);
  ret.append(buffer);
  return ret;
}

const PlainTableOptions& PlainTableFactory::table_options() const {
  return table_options_;
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
