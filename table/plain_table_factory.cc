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

Status PlainTableFactory::NewTableReader(const Options& options,
                                         const EnvOptions& soptions,
                                         const InternalKeyComparator& icomp,
                                         unique_ptr<RandomAccessFile>&& file,
                                         uint64_t file_size,
                                         unique_ptr<TableReader>* table) const {
  return PlainTableReader::Open(options, soptions, icomp, std::move(file),
                                file_size, table, bloom_bits_per_key_,
                                hash_table_ratio_, index_sparseness_,
                                huge_page_tlb_size_, full_scan_mode_);
}

TableBuilder* PlainTableFactory::NewTableBuilder(
    const Options& options, const InternalKeyComparator& internal_comparator,
    WritableFile* file, CompressionType compression_type) const {
  return new PlainTableBuilder(options, file, user_key_len_, encoding_type_,
                               index_sparseness_);
}

extern TableFactory* NewPlainTableFactory(
    uint32_t user_key_len, int bloom_bits_per_key, double hash_table_ratio,
    size_t index_sparseness, size_t huge_page_tlb_size,
    EncodingType encoding_type, bool full_scan_mode) {
  return new PlainTableFactory(
      user_key_len, bloom_bits_per_key, hash_table_ratio, index_sparseness,
      huge_page_tlb_size, encoding_type, full_scan_mode);
}

const std::string PlainTablePropertyNames::kPrefixExtractorName =
    "rocksdb.prefix.extractor.name";

const std::string PlainTablePropertyNames::kEncodingType =
    "rocksdb.plain.table.encoding.type";

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
