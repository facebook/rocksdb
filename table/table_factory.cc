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

Status TableFactory::CreateFromString(const ConfigOptions& config_options_in,
                                      const std::string& id,
                                      std::shared_ptr<TableFactory>* factory) {
  Status status;
  std::string name = id;

  std::string existing_opts;

  ConfigOptions config_options = config_options_in;
  if (factory->get() != nullptr && name == factory->get()->Name()) {
    config_options.delimiter = ";";

    status = factory->get()->GetOptionString(config_options, &existing_opts);
    if (!status.ok()) {
      return status;
    }
  }
  if (name == TableFactory::kBlockBasedTableName()) {
    factory->reset(new BlockBasedTableFactory());
#ifndef ROCKSDB_LITE
  } else if (name == TableFactory::kPlainTableName()) {
    factory->reset(new PlainTableFactory());
  } else if (name == TableFactory::kCuckooTableName()) {
    factory->reset(new CuckooTableFactory());
#endif  // ROCKSDB_LITE
  } else {
    status = Status::NotSupported("Could not load table factory: ", name);
    return status;
  }
  if (status.ok() && !existing_opts.empty()) {
    config_options.invoke_prepare_options = false;
    status = factory->get()->ConfigureFromString(config_options, existing_opts);
  }
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
