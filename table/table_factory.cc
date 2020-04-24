// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "options/customizable_helper.h"
#include "rocksdb/convenience.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/cuckoo/cuckoo_table_factory.h"
#include "table/plain/plain_table_factory.h"

namespace ROCKSDB_NAMESPACE {
const std::string TableFactory::kBlockBasedTableName = "BlockBasedTable";
const std::string TableFactory::kBlockBasedTableOpts = "BlockTableOptions";
const std::string TableFactory::kPlainTableName = "PlainTable";
const std::string TableFactory::kPlainTableOpts = "PlainTableOptions";
const std::string TableFactory::kCuckooTableName = "CuckooTable";
const std::string TableFactory::kCuckooTableOpts = "CuckooTableOptions";
const std::string TableFactory::kBlockCacheOpts = "BlockCacheOptions";

static bool LoadFactory(const std::string& id,
                        std::shared_ptr<TableFactory>* factory) {
  bool success = true;
  if (id == TableFactory::kBlockBasedTableName) {
    factory->reset(new BlockBasedTableFactory());
#ifndef ROCKSDB_LITE
  } else if (id == TableFactory::kPlainTableName) {
    factory->reset(new PlainTableFactory());
  } else if (id == TableFactory::kCuckooTableName) {
    factory->reset(new CuckooTableFactory());
#endif  // ROCKSDB_LITE
  } else {
    success = false;
  }
  return success;
}

Status TableFactory::CreateFromString(const ConfigOptions& config_options,
                                      const std::string& value,
                                      std::shared_ptr<TableFactory>* factory) {
  return LoadSharedObject<TableFactory>(config_options, value, LoadFactory,
                                        factory);
}

}  // namespace ROCKSDB_NAMESPACE
