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
// The portal class for org.rocksdb.Priority
class PriorityJni {
 public:
  // Returns the equivalent org.rocksdb.Priority for the provided
  // C++ ROCKSDB_NAMESPACE::Env::Priority enum
  static jbyte toJavaPriority(
      const ROCKSDB_NAMESPACE::Env::Priority& priority) {
    switch (priority) {
      case ROCKSDB_NAMESPACE::Env::Priority::BOTTOM:
        return 0x0;
      case ROCKSDB_NAMESPACE::Env::Priority::LOW:
        return 0x1;
      case ROCKSDB_NAMESPACE::Env::Priority::HIGH:
        return 0x2;
      case ROCKSDB_NAMESPACE::Env::Priority::TOTAL:
        return 0x3;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::env::Priority enum for the
  // provided Java org.rocksdb.Priority
  static ROCKSDB_NAMESPACE::Env::Priority toCppPriority(jbyte jpriority) {
    switch (jpriority) {
      case 0x0:
        return ROCKSDB_NAMESPACE::Env::Priority::BOTTOM;
      case 0x1:
        return ROCKSDB_NAMESPACE::Env::Priority::LOW;
      case 0x2:
        return ROCKSDB_NAMESPACE::Env::Priority::HIGH;
      case 0x3:
        return ROCKSDB_NAMESPACE::Env::Priority::TOTAL;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::Env::Priority::LOW;
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
