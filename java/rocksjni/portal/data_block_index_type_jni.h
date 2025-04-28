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
// The portal class for org.rocksdb.DataBlockIndexType
class DataBlockIndexTypeJni {
 public:
  // Returns the equivalent org.rocksdb.DataBlockIndexType for the provided
  // C++ ROCKSDB_NAMESPACE::DataBlockIndexType enum
  static jbyte toJavaDataBlockIndexType(
      const ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType&
          index_type) {
    switch (index_type) {
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType::
          kDataBlockBinarySearch:
        return 0x0;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType::
          kDataBlockBinaryAndHash:
        return 0x1;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::DataBlockIndexType enum for
  // the provided Java org.rocksdb.DataBlockIndexType
  static ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType
  toCppDataBlockIndexType(jbyte jindex_type) {
    switch (jindex_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType::
            kDataBlockBinarySearch;
      case 0x1:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType::
            kDataBlockBinaryAndHash;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::DataBlockIndexType::
            kDataBlockBinarySearch;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
