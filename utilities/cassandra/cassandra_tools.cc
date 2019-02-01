#include "utilities/cassandra/merge_operator.h"
#include "utilities/cassandra/cassandra_compaction_filter.h"
#include "rocksdb/extension_loader.h"

extern "C" {
  void loadCassandraExtensions(rocksdb::ExtensionLoader & factory, const std::string &) {
#ifndef ROCKSDB_LITE
    rocksdb::cassandra::CassandraValueMergeOperator::RegisterFactory(factory);
    rocksdb::cassandra::CassandraCompactionFilter::RegisterFactory(factory);
    rocksdb::cassandra::CassandraCompactionFilterFactory::RegisterFactory(factory);
#endif   
  }
}
