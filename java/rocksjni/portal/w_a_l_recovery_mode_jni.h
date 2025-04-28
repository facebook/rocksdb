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
// The portal class for org.rocksdb.WALRecoveryMode
class WALRecoveryModeJni {
 public:
  // Returns the equivalent org.rocksdb.WALRecoveryMode for the provided
  // C++ ROCKSDB_NAMESPACE::WALRecoveryMode enum
  static jbyte toJavaWALRecoveryMode(
      const ROCKSDB_NAMESPACE::WALRecoveryMode& wal_recovery_mode) {
    switch (wal_recovery_mode) {
      case ROCKSDB_NAMESPACE::WALRecoveryMode::kTolerateCorruptedTailRecords:
        return 0x0;
      case ROCKSDB_NAMESPACE::WALRecoveryMode::kAbsoluteConsistency:
        return 0x1;
      case ROCKSDB_NAMESPACE::WALRecoveryMode::kPointInTimeRecovery:
        return 0x2;
      case ROCKSDB_NAMESPACE::WALRecoveryMode::kSkipAnyCorruptedRecords:
        return 0x3;
      default:
        // undefined/default
        return 0x2;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::WALRecoveryMode enum for the
  // provided Java org.rocksdb.WALRecoveryMode
  static ROCKSDB_NAMESPACE::WALRecoveryMode toCppWALRecoveryMode(
      jbyte jwal_recovery_mode) {
    switch (jwal_recovery_mode) {
      case 0x0:
        return ROCKSDB_NAMESPACE::WALRecoveryMode::
            kTolerateCorruptedTailRecords;
      case 0x1:
        return ROCKSDB_NAMESPACE::WALRecoveryMode::kAbsoluteConsistency;
      case 0x2:
        return ROCKSDB_NAMESPACE::WALRecoveryMode::kPointInTimeRecovery;
      case 0x3:
        return ROCKSDB_NAMESPACE::WALRecoveryMode::kSkipAnyCorruptedRecords;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::WALRecoveryMode::kPointInTimeRecovery;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
