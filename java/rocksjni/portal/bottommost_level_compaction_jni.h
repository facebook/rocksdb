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
#include "rocksjni/portal/common.h"

namespace ROCKSDB_NAMESPACE {
// The portal class for org.rocksdb.BottommostLevelCompaction
class BottommostLevelCompactionJni {
 public:
  // Returns the equivalent org.rocksdb.BottommostLevelCompaction for the
  // provided C++ ROCKSDB_NAMESPACE::BottommostLevelCompaction enum
  static jint toJavaBottommostLevelCompaction(
      const ROCKSDB_NAMESPACE::BottommostLevelCompaction&
          bottommost_level_compaction) {
    switch (bottommost_level_compaction) {
      case ROCKSDB_NAMESPACE::BottommostLevelCompaction::kSkip:
        return 0x0;
      case ROCKSDB_NAMESPACE::BottommostLevelCompaction::
          kIfHaveCompactionFilter:
        return 0x1;
      case ROCKSDB_NAMESPACE::BottommostLevelCompaction::kForce:
        return 0x2;
      case ROCKSDB_NAMESPACE::BottommostLevelCompaction::kForceOptimized:
        return 0x3;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::BottommostLevelCompaction
  // enum for the provided Java org.rocksdb.BottommostLevelCompaction
  static ROCKSDB_NAMESPACE::BottommostLevelCompaction
  toCppBottommostLevelCompaction(jint bottommost_level_compaction) {
    switch (bottommost_level_compaction) {
      case 0x0:
        return ROCKSDB_NAMESPACE::BottommostLevelCompaction::kSkip;
      case 0x1:
        return ROCKSDB_NAMESPACE::BottommostLevelCompaction::
            kIfHaveCompactionFilter;
      case 0x2:
        return ROCKSDB_NAMESPACE::BottommostLevelCompaction::kForce;
      case 0x3:
        return ROCKSDB_NAMESPACE::BottommostLevelCompaction::kForceOptimized;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::BottommostLevelCompaction::
            kIfHaveCompactionFilter;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
