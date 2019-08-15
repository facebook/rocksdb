// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/table.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/cuckoo/cuckoo_table_factory.h"
#include "table/plain/plain_table_factory.h"

namespace rocksdb {
const std::string TableFactory::kBlockBasedTableName = "BlockBasedTable";
const std::string TableFactory::kBlockBasedTableOpts = "BlockTableOptions";
const std::string TableFactory::kPlainTableName = "PlainTable";
const std::string TableFactory::kPlainTableOpts = "PlainTableOptions";
const std::string TableFactory::kCuckooTableName = "CuckooTable";
const std::string TableFactory::kCuckooTableOpts = "CuckooTableOptions";

Status TableFactory::LoadTableFactory(const std::string &id,
                                      std::shared_ptr<TableFactory> *factory) {
  Status status;
  std::string name = id;
  if (factory->get() == nullptr || name != factory->get()->Name()) {
    if (name == kBlockBasedTableName) {
      factory->reset(new BlockBasedTableFactory());
#ifndef ROCKSDB_LITE
    } else if (name == kPlainTableName) {
      factory->reset(new PlainTableFactory());
    } else if (name == kCuckooTableName) {
      factory->reset(new CuckooTableFactory());
#endif  // ROCKSDB_LITE
    } else {
      status = Status::NotFound("Could not load table factory: ", name);
    }
  }
  return status;
}

}  // namespace rocksdb
