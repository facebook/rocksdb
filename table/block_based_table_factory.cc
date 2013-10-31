//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#include "table/block_based_table_factory.h"

#include <memory>
#include <stdint.h>
#include "table/block_based_table_builder.h"
#include "table/block_based_table_reader.h"
#include "port/port.h"

namespace rocksdb {

Status BlockBasedTableFactory::GetTableReader(
    const Options& options, const EnvOptions& soptions,
    unique_ptr<RandomAccessFile> && file, uint64_t file_size,
    unique_ptr<TableReader>* table_reader) const {
  return BlockBasedTable::Open(options, soptions, std::move(file), file_size,
                               table_reader);
}

TableBuilder* BlockBasedTableFactory::GetTableBuilder(
    const Options& options, WritableFile* file,
    CompressionType compression_type) const {
  return new BlockBasedTableBuilder(options, file, compression_type);
}
}  // namespace rocksdb
