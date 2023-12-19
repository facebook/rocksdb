//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/table_properties_collectors.h"

#ifndef ROCKSDB_TABLE_PROPERTIES_COLLECTOR_FACTORY_H
#define ROCKSDB_TABLE_PROPERTIES_COLLECTOR_FACTORY_H

struct TablePropertiesCollectorFactoriesJniWrapper {
  std::shared_ptr<rocksdb::TablePropertiesCollectorFactory>
      table_properties_collector_factories;
};
#endif  // ROCKSDB_TABLE_PROPERTIES_COLLECTOR_FACTORY_H
