// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#pragma once

#include <jni.h>

#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
// The portal class for org.rocksdb.MemoryUsageType
class MemoryUsageTypeJni {
 public:
  // Returns the equivalent org.rocksdb.MemoryUsageType for the provided
  // C++ ROCKSDB_NAMESPACE::MemoryUtil::UsageType enum
  static jbyte toJavaMemoryUsageType(
      const ROCKSDB_NAMESPACE::MemoryUtil::UsageType& usage_type) {
    switch (usage_type) {
      case ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kMemTableTotal:
        return 0x0;
      case ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kMemTableUnFlushed:
        return 0x1;
      case ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kTableReadersTotal:
        return 0x2;
      case ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kCacheTotal:
        return 0x3;
      default:
        // undefined: use kNumUsageTypes
        return 0x4;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::MemoryUtil::UsageType enum
  // for the provided Java org.rocksdb.MemoryUsageType
  static ROCKSDB_NAMESPACE::MemoryUtil::UsageType toCppMemoryUsageType(
      jbyte usage_type) {
    switch (usage_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kMemTableTotal;
      case 0x1:
        return ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kMemTableUnFlushed;
      case 0x2:
        return ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kTableReadersTotal;
      case 0x3:
        return ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kCacheTotal;
      default:
        // undefined/default: use kNumUsageTypes
        return ROCKSDB_NAMESPACE::MemoryUtil::UsageType::kNumUsageTypes;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
