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
// The portal class for org.rocksdb.StatsLevel
class StatsLevelJni {
 public:
  // Returns the equivalent org.rocksdb.StatsLevel for the provided
  // C++ ROCKSDB_NAMESPACE::StatsLevel enum
  static jbyte toJavaStatsLevel(
      const ROCKSDB_NAMESPACE::StatsLevel& stats_level) {
    switch (stats_level) {
      case ROCKSDB_NAMESPACE::StatsLevel::kExceptDetailedTimers:
        return 0x0;
      case ROCKSDB_NAMESPACE::StatsLevel::kExceptTimeForMutex:
        return 0x1;
      case ROCKSDB_NAMESPACE::StatsLevel::kAll:
        return 0x2;

      default:
        // undefined/default
        return 0x0;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::StatsLevel enum for the
  // provided Java org.rocksdb.StatsLevel
  static ROCKSDB_NAMESPACE::StatsLevel toCppStatsLevel(jbyte jstats_level) {
    switch (jstats_level) {
      case 0x0:
        return ROCKSDB_NAMESPACE::StatsLevel::kExceptDetailedTimers;
      case 0x1:
        return ROCKSDB_NAMESPACE::StatsLevel::kExceptTimeForMutex;
      case 0x2:
        return ROCKSDB_NAMESPACE::StatsLevel::kAll;

      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::StatsLevel::kExceptDetailedTimers;
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
