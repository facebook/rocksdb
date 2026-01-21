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
// The portal class for org.rocksdb.StateType
class StateTypeJni {
 public:
  // Returns the equivalent org.rocksdb.StateType for the provided
  // C++ ROCKSDB_NAMESPACE::ThreadStatus::StateType enum
  static jbyte toJavaStateType(
      const ROCKSDB_NAMESPACE::ThreadStatus::StateType& state_type) {
    switch (state_type) {
      case ROCKSDB_NAMESPACE::ThreadStatus::StateType::STATE_UNKNOWN:
        return 0x0;
      case ROCKSDB_NAMESPACE::ThreadStatus::StateType::STATE_MUTEX_WAIT:
        return 0x1;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ThreadStatus::StateType enum
  // for the provided Java org.rocksdb.StateType
  static ROCKSDB_NAMESPACE::ThreadStatus::StateType toCppStateType(
      jbyte jstate_type) {
    switch (jstate_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ThreadStatus::StateType::STATE_UNKNOWN;
      case 0x1:
        return ROCKSDB_NAMESPACE::ThreadStatus::StateType::STATE_MUTEX_WAIT;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ThreadStatus::StateType::STATE_UNKNOWN;
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
