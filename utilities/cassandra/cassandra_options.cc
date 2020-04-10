// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/cassandra/cassandra_options.h"

#include "rocksdb/utilities/object_registry.h"
#include "utilities/cassandra/cassandra_compaction_filter.h"
#include "utilities/cassandra/merge_operator.h"
namespace ROCKSDB_NAMESPACE {
namespace cassandra {

const std::string CassandraOptionsHelper::kValueMergeOperatorName =
    "CassandraValueMergeOperator";
const std::string CassandraOptionsHelper::kCompactionFilterName =
    "CassandraCompactionFilter";

#ifndef ROCKSDB_LITE
void RegisterCassandraObjects(ObjectLibrary& library,
                              const std::string& /*arg*/) {
  library.Register<MergeOperator>(
      CassandraOptionsHelper::kValueMergeOperatorName,
      [](const std::string& /*uri*/, std::unique_ptr<MergeOperator>* guard,
         std::string* /* errmsg */) {
        guard->reset(new CassandraValueMergeOperator(0));
        return guard->get();
      });
  library.Register<CompactionFilter>(
      CassandraOptionsHelper::kCompactionFilterName,
      [](const std::string& /*uri*/,
         std::unique_ptr<CompactionFilter>* /*guard */,
         std::string* /* errmsg */) {
        return new CassandraCompactionFilter(false, 0);
      });
  library.Register<CompactionFilterFactory>(
      CassandraOptionsHelper::kCompactionFilterName,
      [](const std::string& /*uri*/,
         std::unique_ptr<CompactionFilterFactory>* guard,
         std::string* /* errmsg */) {
        guard->reset(new CassandraCompactionFilterFactory(false, 0));
        return guard->get();
      });
}
#endif  // ROCKSDB_LITE
}  // namespace cassandra
}  // namespace ROCKSDB_NAMESPACE
