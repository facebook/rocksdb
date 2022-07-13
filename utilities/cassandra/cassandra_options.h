// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
class ObjectLibrary;
namespace cassandra {
struct CassandraOptions {
  static const char* kName() { return "CassandraOptions"; }
  CassandraOptions(int32_t _gc_grace_period_in_seconds, size_t _operands_limit,
                   bool _purge_ttl_on_expiration = false)
      : operands_limit(_operands_limit),
        gc_grace_period_in_seconds(_gc_grace_period_in_seconds),
        purge_ttl_on_expiration(_purge_ttl_on_expiration) {}
  // Limit on the number of merge operands.
  size_t operands_limit;

  // How long (in seconds) tombstoned data remains before it is purged
  int32_t gc_grace_period_in_seconds;

  // If is set to true, expired data will be directly purged.
  // Otherwise expired data will be converted tombstones first,
  // then be eventually removed after gc grace period. This value should
  // only true if all writes have same ttl setting, otherwise it could bring old
  // data back.
  bool purge_ttl_on_expiration;
};
#ifndef ROCKSDB_LITE
extern "C" {
int RegisterCassandraObjects(ObjectLibrary& library, const std::string& arg);
}  // extern "C"
#endif  // ROCKSDB_LITE
}  // namespace cassandra
}  // namespace ROCKSDB_NAMESPACE
