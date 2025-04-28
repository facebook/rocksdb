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
// The portal class for org.rocksdb.CompressionType
class CompressionTypeJni {
 public:
  // Returns the equivalent org.rocksdb.CompressionType for the provided
  // C++ ROCKSDB_NAMESPACE::CompressionType enum
  static jbyte toJavaCompressionType(
      const ROCKSDB_NAMESPACE::CompressionType& compression_type) {
    switch (compression_type) {
      case ROCKSDB_NAMESPACE::CompressionType::kNoCompression:
        return 0x0;
      case ROCKSDB_NAMESPACE::CompressionType::kSnappyCompression:
        return 0x1;
      case ROCKSDB_NAMESPACE::CompressionType::kZlibCompression:
        return 0x2;
      case ROCKSDB_NAMESPACE::CompressionType::kBZip2Compression:
        return 0x3;
      case ROCKSDB_NAMESPACE::CompressionType::kLZ4Compression:
        return 0x4;
      case ROCKSDB_NAMESPACE::CompressionType::kLZ4HCCompression:
        return 0x5;
      case ROCKSDB_NAMESPACE::CompressionType::kXpressCompression:
        return 0x6;
      case ROCKSDB_NAMESPACE::CompressionType::kZSTD:
        return 0x7;
      case ROCKSDB_NAMESPACE::CompressionType::kDisableCompressionOption:
      default:
        return 0x7F;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::CompressionType enum for the
  // provided Java org.rocksdb.CompressionType
  static ROCKSDB_NAMESPACE::CompressionType toCppCompressionType(
      jbyte jcompression_type) {
    switch (jcompression_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::CompressionType::kNoCompression;
      case 0x1:
        return ROCKSDB_NAMESPACE::CompressionType::kSnappyCompression;
      case 0x2:
        return ROCKSDB_NAMESPACE::CompressionType::kZlibCompression;
      case 0x3:
        return ROCKSDB_NAMESPACE::CompressionType::kBZip2Compression;
      case 0x4:
        return ROCKSDB_NAMESPACE::CompressionType::kLZ4Compression;
      case 0x5:
        return ROCKSDB_NAMESPACE::CompressionType::kLZ4HCCompression;
      case 0x6:
        return ROCKSDB_NAMESPACE::CompressionType::kXpressCompression;
      case 0x7:
        return ROCKSDB_NAMESPACE::CompressionType::kZSTD;
      case 0x7F:
      default:
        return ROCKSDB_NAMESPACE::CompressionType::kDisableCompressionOption;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
