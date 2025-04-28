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
// The portal class for org.rocksdb.ChecksumType
class ChecksumTypeJni {
 public:
  // Returns the equivalent org.rocksdb.ChecksumType for the provided
  // C++ ROCKSDB_NAMESPACE::ChecksumType enum
  static jbyte toJavaChecksumType(
      const ROCKSDB_NAMESPACE::ChecksumType& checksum_type) {
    switch (checksum_type) {
      case ROCKSDB_NAMESPACE::ChecksumType::kNoChecksum:
        return 0x0;
      case ROCKSDB_NAMESPACE::ChecksumType::kCRC32c:
        return 0x1;
      case ROCKSDB_NAMESPACE::ChecksumType::kxxHash:
        return 0x2;
      case ROCKSDB_NAMESPACE::ChecksumType::kxxHash64:
        return 0x3;
      case ROCKSDB_NAMESPACE::ChecksumType::kXXH3:
        return 0x4;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ChecksumType enum for the
  // provided Java org.rocksdb.ChecksumType
  static ROCKSDB_NAMESPACE::ChecksumType toCppChecksumType(
      jbyte jchecksum_type) {
    switch (jchecksum_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ChecksumType::kNoChecksum;
      case 0x1:
        return ROCKSDB_NAMESPACE::ChecksumType::kCRC32c;
      case 0x2:
        return ROCKSDB_NAMESPACE::ChecksumType::kxxHash;
      case 0x3:
        return ROCKSDB_NAMESPACE::ChecksumType::kxxHash64;
      case 0x4:
        return ROCKSDB_NAMESPACE::ChecksumType::kXXH3;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ChecksumType::kXXH3;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
