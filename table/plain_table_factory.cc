// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/plain_table_factory.h"

#include <memory>
#include <stdint.h>
#include "table/plain_table_builder.h"
#include "table/plain_table_reader.h"
#include "port/port.h"

namespace rocksdb {

Status PlainTableFactory::GetTableReader(const Options& options,
                                         const EnvOptions& soptions,
                                         unique_ptr<RandomAccessFile> && file,
                                         uint64_t file_size,
                                         unique_ptr<TableReader>* table)
     const {
  return PlainTableReader::Open(options, soptions, std::move(file), file_size,
                                table, bloom_num_bits_, hash_table_ratio_);
}

TableBuilder* PlainTableFactory::GetTableBuilder(
    const Options& options, WritableFile* file,
    CompressionType compression_type) const {
  return new PlainTableBuilder(options, file, user_key_len_);
}
}  // namespace rocksdb
