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
// The portal class for org.rocksdb.CompactionStyle
class CompactionStyleJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionStyle for the provided
  // C++ ROCKSDB_NAMESPACE::CompactionStyle enum
  static jbyte toJavaCompactionStyle(
      const ROCKSDB_NAMESPACE::CompactionStyle& compaction_style) {
    switch (compaction_style) {
      case ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleLevel:
        return 0x0;
      case ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleUniversal:
        return 0x1;
      case ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleFIFO:
        return 0x2;
      case ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleNone:
        return 0x3;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::CompactionStyle enum for the
  // provided Java org.rocksdb.CompactionStyle
  static ROCKSDB_NAMESPACE::CompactionStyle toCppCompactionStyle(
      jbyte jcompaction_style) {
    switch (jcompaction_style) {
      case 0x0:
        return ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleLevel;
      case 0x1:
        return ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleUniversal;
      case 0x2:
        return ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleFIFO;
      case 0x3:
        return ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleNone;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleLevel;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
