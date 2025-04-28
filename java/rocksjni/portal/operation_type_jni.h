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
// The portal class for org.rocksdb.OperationType
class OperationTypeJni {
 public:
  // Returns the equivalent org.rocksdb.OperationType for the provided
  // C++ ROCKSDB_NAMESPACE::ThreadStatus::OperationType enum
  static jbyte toJavaOperationType(
      const ROCKSDB_NAMESPACE::ThreadStatus::OperationType& operation_type) {
    switch (operation_type) {
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_UNKNOWN:
        return 0x0;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_COMPACTION:
        return 0x1;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_FLUSH:
        return 0x2;
      case ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_DBOPEN:
        return 0x3;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ThreadStatus::OperationType
  // enum for the provided Java org.rocksdb.OperationType
  static ROCKSDB_NAMESPACE::ThreadStatus::OperationType toCppOperationType(
      jbyte joperation_type) {
    switch (joperation_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_UNKNOWN;
      case 0x1:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_COMPACTION;
      case 0x2:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_FLUSH;
      case 0x3:
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_DBOPEN;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ThreadStatus::OperationType::OP_UNKNOWN;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
