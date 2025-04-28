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
// The portal class for org.rocksdb.CompactionPriority
class CompactionPriorityJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionPriority for the provided
  // C++ ROCKSDB_NAMESPACE::CompactionPri enum
  static jbyte toJavaCompactionPriority(
      const ROCKSDB_NAMESPACE::CompactionPri& compaction_priority) {
    switch (compaction_priority) {
      case ROCKSDB_NAMESPACE::CompactionPri::kByCompensatedSize:
        return 0x0;
      case ROCKSDB_NAMESPACE::CompactionPri::kOldestLargestSeqFirst:
        return 0x1;
      case ROCKSDB_NAMESPACE::CompactionPri::kOldestSmallestSeqFirst:
        return 0x2;
      case ROCKSDB_NAMESPACE::CompactionPri::kMinOverlappingRatio:
        return 0x3;
      case ROCKSDB_NAMESPACE::CompactionPri::kRoundRobin:
        return 0x4;
      default:
        return 0x0;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::CompactionPri enum for the
  // provided Java org.rocksdb.CompactionPriority
  static ROCKSDB_NAMESPACE::CompactionPri toCppCompactionPriority(
      jbyte jcompaction_priority) {
    switch (jcompaction_priority) {
      case 0x0:
        return ROCKSDB_NAMESPACE::CompactionPri::kByCompensatedSize;
      case 0x1:
        return ROCKSDB_NAMESPACE::CompactionPri::kOldestLargestSeqFirst;
      case 0x2:
        return ROCKSDB_NAMESPACE::CompactionPri::kOldestSmallestSeqFirst;
      case 0x3:
        return ROCKSDB_NAMESPACE::CompactionPri::kMinOverlappingRatio;
      case 0x4:
        return ROCKSDB_NAMESPACE::CompactionPri::kRoundRobin;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::CompactionPri::kByCompensatedSize;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
