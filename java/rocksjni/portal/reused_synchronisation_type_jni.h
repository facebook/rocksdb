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
// The portal class for org.rocksdb.ReusedSynchronisationType
class ReusedSynchronisationTypeJni {
 public:
  // Returns the equivalent org.rocksdb.ReusedSynchronisationType for the
  // provided C++ ROCKSDB_NAMESPACE::ReusedSynchronisationType enum
  static jbyte toJavaReusedSynchronisationType(
      const ROCKSDB_NAMESPACE::ReusedSynchronisationType&
          reused_synchronisation_type) {
    switch (reused_synchronisation_type) {
      case ROCKSDB_NAMESPACE::ReusedSynchronisationType::MUTEX:
        return 0x0;
      case ROCKSDB_NAMESPACE::ReusedSynchronisationType::ADAPTIVE_MUTEX:
        return 0x1;
      case ROCKSDB_NAMESPACE::ReusedSynchronisationType::THREAD_LOCAL:
        return 0x2;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ReusedSynchronisationType
  // enum for the provided Java org.rocksdb.ReusedSynchronisationType
  static ROCKSDB_NAMESPACE::ReusedSynchronisationType
  toCppReusedSynchronisationType(jbyte reused_synchronisation_type) {
    switch (reused_synchronisation_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ReusedSynchronisationType::MUTEX;
      case 0x1:
        return ROCKSDB_NAMESPACE::ReusedSynchronisationType::ADAPTIVE_MUTEX;
      case 0x2:
        return ROCKSDB_NAMESPACE::ReusedSynchronisationType::THREAD_LOCAL;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ReusedSynchronisationType::ADAPTIVE_MUTEX;
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
