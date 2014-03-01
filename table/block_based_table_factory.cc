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

BlockBasedTableFactory::BlockBasedTableFactory(
    const BlockBasedTableOptions& table_options)
    : table_options_(table_options) {
  if (table_options_.flush_block_policy_factory == nullptr) {
    table_options_.flush_block_policy_factory.reset(
        new FlushBlockBySizePolicyFactory());
  }
}

Status BlockBasedTableFactory::NewTableReader(
    const Options& options, const EnvOptions& soptions,
    const InternalKeyComparator& internal_comparator,
    unique_ptr<RandomAccessFile>&& file, uint64_t file_size,
    unique_ptr<TableReader>* table_reader) const {
  return BlockBasedTable::Open(options, soptions, table_options_,
                               internal_comparator, std::move(file), file_size,
                               table_reader);
}

TableBuilder* BlockBasedTableFactory::NewTableBuilder(
    const Options& options, const InternalKeyComparator& internal_comparator,
    WritableFile* file, CompressionType compression_type) const {
  auto flush_block_policy_factory =
      table_options_.flush_block_policy_factory.get();

  auto table_builder =
      new BlockBasedTableBuilder(options, internal_comparator, file,
                                 flush_block_policy_factory, compression_type);

  return table_builder;
}

TableFactory* NewBlockBasedTableFactory(
    const BlockBasedTableOptions& table_options) {
  return new BlockBasedTableFactory(table_options);
}

}  // namespace rocksdb
