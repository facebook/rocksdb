//
// Created by rhubner on 24-Oct-23.
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
