// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <cinttypes>
#include <string>
#include <unordered_map>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
class ObjectLibrary;

namespace cassandra {
struct CassandraOptions {
  size_t operands_limit;
  bool purge_ttl_on_expiration;
  int32_t gc_grace_period_in_seconds;
};
struct CassandraOptionsHelper {
  static const std::string kValueMergeOperatorName;
  static const std::string kCompactionFilterName;
};
#ifndef ROCKSDB_LITE
extern "C" {
void RegisterCassandraObjects(ObjectLibrary& library, const std::string& arg);
}
#endif  // ROCKSDB_LITE
}  // namespace cassandra
}  // namespace ROCKSDB_NAMESPACE
