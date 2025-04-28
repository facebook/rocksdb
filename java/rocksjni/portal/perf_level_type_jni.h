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
class PerfLevelTypeJni {
 public:
  static jbyte toJavaPerfLevelType(const ROCKSDB_NAMESPACE::PerfLevel level) {
    switch (level) {
      case ROCKSDB_NAMESPACE::PerfLevel::kUninitialized:
        return 0x0;
      case ROCKSDB_NAMESPACE::PerfLevel::kDisable:
        return 0x1;
      case ROCKSDB_NAMESPACE::PerfLevel::kEnableCount:
        return 0x2;
      case ROCKSDB_NAMESPACE::PerfLevel::kEnableTimeExceptForMutex:
        return 0x3;
      case ROCKSDB_NAMESPACE::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex:
        return 0x4;
      case ROCKSDB_NAMESPACE::PerfLevel::kEnableTime:
        return 0x5;
      case ROCKSDB_NAMESPACE::PerfLevel::kOutOfBounds:
        return 0x6;
      default:
        return 0x6;
    }
  }

  static ROCKSDB_NAMESPACE::PerfLevel toCppPerfLevelType(const jbyte level) {
    switch (level) {
      case 0x0:
        return ROCKSDB_NAMESPACE::PerfLevel::kUninitialized;
      case 0x1:
        return ROCKSDB_NAMESPACE::PerfLevel::kDisable;
      case 0x2:
        return ROCKSDB_NAMESPACE::PerfLevel::kEnableCount;
      case 0x3:
        return ROCKSDB_NAMESPACE::PerfLevel::kEnableTimeExceptForMutex;
      case 0x4:
        return ROCKSDB_NAMESPACE::PerfLevel::
            kEnableTimeAndCPUTimeExceptForMutex;
      case 0x5:
        return ROCKSDB_NAMESPACE::PerfLevel::kEnableTime;
      case 0x6:
        return ROCKSDB_NAMESPACE::PerfLevel::kOutOfBounds;
      default:
        return ROCKSDB_NAMESPACE::PerfLevel::kOutOfBounds;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
