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
// The portal class for org.rocksdb.ThreadType
class ThreadTypeJni {
 public:
  // Returns the equivalent org.rocksdb.ThreadType for the provided
  // C++ ROCKSDB_NAMESPACE::ThreadStatus::ThreadType enum
  static jbyte toJavaThreadType(
      const ROCKSDB_NAMESPACE::ThreadStatus::ThreadType& thread_type) {
    switch (thread_type) {
      case ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::HIGH_PRIORITY:
        return 0x0;
      case ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::LOW_PRIORITY:
        return 0x1;
      case ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::USER:
        return 0x2;
      case ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::BOTTOM_PRIORITY:
        return 0x3;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ThreadStatus::ThreadType enum
  // for the provided Java org.rocksdb.ThreadType
  static ROCKSDB_NAMESPACE::ThreadStatus::ThreadType toCppThreadType(
      jbyte jthread_type) {
    switch (jthread_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::HIGH_PRIORITY;
      case 0x1:
        return ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::LOW_PRIORITY;
      case 0x2:
        return ThreadStatus::ThreadType::USER;
      case 0x3:
        return ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::BOTTOM_PRIORITY;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ThreadStatus::ThreadType::LOW_PRIORITY;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
