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
  auto flush_block_policy_factory = 
    table_options_.flush_block_policy_factory.get();

  // if flush block policy factory is not set, we'll create the default one
  // from the options.
  //
  // NOTE: we cannot pre-cache the "default block policy factory" because
  // `FlushBlockBySizePolicyFactory` takes `options.block_size` and
  // `options.block_size_deviation` as parameters, which may be different
  // every time.
  if (flush_block_policy_factory == nullptr) {
    flush_block_policy_factory =
        new FlushBlockBySizePolicyFactory(options.block_size,
                                          options.block_size_deviation);
  }

  auto table_builder =  new BlockBasedTableBuilder(
      options,
      file,
      flush_block_policy_factory,
      compression_type);

  // Delete flush_block_policy_factory only when it's just created from the
  // options.
  // We can safely delete flush_block_policy_factory since it will only be used
  // during the construction of `BlockBasedTableBuilder`.
  if (flush_block_policy_factory != 
      table_options_.flush_block_policy_factory.get()) {
    delete flush_block_policy_factory;
  }

  return table_builder;
}

}  // namespace rocksdb
