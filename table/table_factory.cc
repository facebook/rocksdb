// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

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

Status TableFactory::CreateFromString(const ConfigOptions& config_options,
                                      const std::string& id,
                                      std::shared_ptr<TableFactory>* factory) {
  Status status;
  std::string name = id;

  std::string existing_opts;

  if (factory->get() != nullptr && name == factory->get()->Name()) {
    status = factory->get()->GetOptionString(config_options.Embedded(),
                                             &existing_opts);
    if (!status.ok()) {
      return status;
    }
  }
  if (name == kBlockBasedTableName) {
    factory->reset(new BlockBasedTableFactory());
#ifndef ROCKSDB_LITE
  } else if (name == kPlainTableName) {
    factory->reset(new PlainTableFactory());
  } else if (name == kCuckooTableName) {
    factory->reset(new CuckooTableFactory());
#endif  // ROCKSDB_LITE
  } else {
    return Status::NotSupported("Could not load table factory: ", name);
  }
  if (!existing_opts.empty()) {
    status = factory->get()->ConfigureFromString(config_options, existing_opts);
  }
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
