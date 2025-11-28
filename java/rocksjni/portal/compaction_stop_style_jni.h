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
// The portal class for org.rocksdb.CompactionStopStyle
class CompactionStopStyleJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionStopStyle for the provided
  // C++ ROCKSDB_NAMESPACE::CompactionStopStyle enum
  static jbyte toJavaCompactionStopStyle(
      const ROCKSDB_NAMESPACE::CompactionStopStyle& compaction_stop_style) {
    switch (compaction_stop_style) {
      case ROCKSDB_NAMESPACE::CompactionStopStyle::
          kCompactionStopStyleSimilarSize:
        return 0x0;
      case ROCKSDB_NAMESPACE::CompactionStopStyle::
          kCompactionStopStyleTotalSize:
        return 0x1;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::CompactionStopStyle enum for
  // the provided Java org.rocksdb.CompactionStopStyle
  static ROCKSDB_NAMESPACE::CompactionStopStyle toCppCompactionStopStyle(
      jbyte jcompaction_stop_style) {
    switch (jcompaction_stop_style) {
      case 0x0:
        return ROCKSDB_NAMESPACE::CompactionStopStyle::
            kCompactionStopStyleSimilarSize;
      case 0x1:
        return ROCKSDB_NAMESPACE::CompactionStopStyle::
            kCompactionStopStyleTotalSize;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::CompactionStopStyle::
            kCompactionStopStyleSimilarSize;
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
