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
// The portal class for org.rocksdb.IndexType
class IndexTypeJni {
 public:
  // Returns the equivalent org.rocksdb.IndexType for the provided
  // C++ ROCKSDB_NAMESPACE::IndexType enum
  static jbyte toJavaIndexType(
      const ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType& index_type) {
    switch (index_type) {
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::kBinarySearch:
        return 0x0;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::kHashSearch:
        return 0x1;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
          kTwoLevelIndexSearch:
        return 0x2;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
          kBinarySearchWithFirstKey:
        return 0x3;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::IndexType enum for the
  // provided Java org.rocksdb.IndexType
  static ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType toCppIndexType(
      jbyte jindex_type) {
    switch (jindex_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
            kBinarySearch;
      case 0x1:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
            kHashSearch;
      case 0x2:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
            kTwoLevelIndexSearch;
      case 0x3:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
            kBinarySearchWithFirstKey;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexType::
            kBinarySearch;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
