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
// The portal class for org.rocksdb.CompactionReason
class CompactionReasonJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionReason for the provided
  // C++ ROCKSDB_NAMESPACE::CompactionReason enum
  static jbyte toJavaCompactionReason(
      const ROCKSDB_NAMESPACE::CompactionReason& compaction_reason) {
    switch (compaction_reason) {
      case ROCKSDB_NAMESPACE::CompactionReason::kUnknown:
        return 0x0;
      case ROCKSDB_NAMESPACE::CompactionReason::kLevelL0FilesNum:
        return 0x1;
      case ROCKSDB_NAMESPACE::CompactionReason::kLevelMaxLevelSize:
        return 0x2;
      case ROCKSDB_NAMESPACE::CompactionReason::kUniversalSizeAmplification:
        return 0x3;
      case ROCKSDB_NAMESPACE::CompactionReason::kUniversalSizeRatio:
        return 0x4;
      case ROCKSDB_NAMESPACE::CompactionReason::kUniversalSortedRunNum:
        return 0x5;
      case ROCKSDB_NAMESPACE::CompactionReason::kFIFOMaxSize:
        return 0x6;
      case ROCKSDB_NAMESPACE::CompactionReason::kFIFOReduceNumFiles:
        return 0x7;
      case ROCKSDB_NAMESPACE::CompactionReason::kFIFOTtl:
        return 0x8;
      case ROCKSDB_NAMESPACE::CompactionReason::kManualCompaction:
        return 0x9;
      case ROCKSDB_NAMESPACE::CompactionReason::kFilesMarkedForCompaction:
        return 0x10;
      case ROCKSDB_NAMESPACE::CompactionReason::kBottommostFiles:
        return 0x0A;
      case ROCKSDB_NAMESPACE::CompactionReason::kTtl:
        return 0x0B;
      case ROCKSDB_NAMESPACE::CompactionReason::kFlush:
        return 0x0C;
      case ROCKSDB_NAMESPACE::CompactionReason::kExternalSstIngestion:
        return 0x0D;
      case ROCKSDB_NAMESPACE::CompactionReason::kPeriodicCompaction:
        return 0x0E;
      case ROCKSDB_NAMESPACE::CompactionReason::kChangeTemperature:
        return 0x0F;
      case ROCKSDB_NAMESPACE::CompactionReason::kForcedBlobGC:
        return 0x11;
      case ROCKSDB_NAMESPACE::CompactionReason::kRoundRobinTtl:
        return 0x12;
      case ROCKSDB_NAMESPACE::CompactionReason::kRefitLevel:
        return 0x13;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::CompactionReason enum for the
  // provided Java org.rocksdb.CompactionReason
  static ROCKSDB_NAMESPACE::CompactionReason toCppCompactionReason(
      jbyte jcompaction_reason) {
    switch (jcompaction_reason) {
      case 0x0:
        return ROCKSDB_NAMESPACE::CompactionReason::kUnknown;
      case 0x1:
        return ROCKSDB_NAMESPACE::CompactionReason::kLevelL0FilesNum;
      case 0x2:
        return ROCKSDB_NAMESPACE::CompactionReason::kLevelMaxLevelSize;
      case 0x3:
        return ROCKSDB_NAMESPACE::CompactionReason::kUniversalSizeAmplification;
      case 0x4:
        return ROCKSDB_NAMESPACE::CompactionReason::kUniversalSizeRatio;
      case 0x5:
        return ROCKSDB_NAMESPACE::CompactionReason::kUniversalSortedRunNum;
      case 0x6:
        return ROCKSDB_NAMESPACE::CompactionReason::kFIFOMaxSize;
      case 0x7:
        return ROCKSDB_NAMESPACE::CompactionReason::kFIFOReduceNumFiles;
      case 0x8:
        return ROCKSDB_NAMESPACE::CompactionReason::kFIFOTtl;
      case 0x9:
        return ROCKSDB_NAMESPACE::CompactionReason::kManualCompaction;
      case 0x10:
        return ROCKSDB_NAMESPACE::CompactionReason::kFilesMarkedForCompaction;
      case 0x0A:
        return ROCKSDB_NAMESPACE::CompactionReason::kBottommostFiles;
      case 0x0B:
        return ROCKSDB_NAMESPACE::CompactionReason::kTtl;
      case 0x0C:
        return ROCKSDB_NAMESPACE::CompactionReason::kFlush;
      case 0x0D:
        return ROCKSDB_NAMESPACE::CompactionReason::kExternalSstIngestion;
      case 0x0E:
        return ROCKSDB_NAMESPACE::CompactionReason::kPeriodicCompaction;
      case 0x0F:
        return ROCKSDB_NAMESPACE::CompactionReason::kChangeTemperature;
      case 0x11:
        return ROCKSDB_NAMESPACE::CompactionReason::kForcedBlobGC;
      case 0x12:
        return ROCKSDB_NAMESPACE::CompactionReason::kRoundRobinTtl;
      case 0x13:
        return ROCKSDB_NAMESPACE::CompactionReason::kRefitLevel;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::CompactionReason::kUnknown;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
