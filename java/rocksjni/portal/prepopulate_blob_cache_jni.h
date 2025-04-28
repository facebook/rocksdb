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
// The portal class for org.rocksdb.PrepopulateBlobCache
class PrepopulateBlobCacheJni {
 public:
  // Returns the equivalent org.rocksdb.PrepopulateBlobCache for the provided
  // C++ ROCKSDB_NAMESPACE::PrepopulateBlobCache enum
  static jbyte toJavaPrepopulateBlobCache(
      ROCKSDB_NAMESPACE::PrepopulateBlobCache prepopulate_blob_cache) {
    switch (prepopulate_blob_cache) {
      case ROCKSDB_NAMESPACE::PrepopulateBlobCache::kDisable:
        return 0x0;
      case ROCKSDB_NAMESPACE::PrepopulateBlobCache::kFlushOnly:
        return 0x1;
      default:
        return 0x7f;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::PrepopulateBlobCache enum for
  // the provided Java org.rocksdb.PrepopulateBlobCache
  static ROCKSDB_NAMESPACE::PrepopulateBlobCache toCppPrepopulateBlobCache(
      jbyte jprepopulate_blob_cache) {
    switch (jprepopulate_blob_cache) {
      case 0x0:
        return ROCKSDB_NAMESPACE::PrepopulateBlobCache::kDisable;
      case 0x1:
        return ROCKSDB_NAMESPACE::PrepopulateBlobCache::kFlushOnly;
      case 0x7F:
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::PrepopulateBlobCache::kDisable;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
