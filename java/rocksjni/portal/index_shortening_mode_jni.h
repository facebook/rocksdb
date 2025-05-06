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
// The portal class for org.rocksdb.IndexShorteningMode
class IndexShorteningModeJni {
 public:
  // Returns the equivalent org.rocksdb.IndexShorteningMode for the provided
  // C++ ROCKSDB_NAMESPACE::IndexShorteningMode enum
  static jbyte toJavaIndexShorteningMode(
      const ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode&
          index_shortening_mode) {
    switch (index_shortening_mode) {
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
          kNoShortening:
        return 0x0;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
          kShortenSeparators:
        return 0x1;
      case ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
          kShortenSeparatorsAndSuccessor:
        return 0x2;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::IndexShorteningMode enum for
  // the provided Java org.rocksdb.IndexShorteningMode
  static ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode
  toCppIndexShorteningMode(jbyte jindex_shortening_mode) {
    switch (jindex_shortening_mode) {
      case 0x0:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
            kNoShortening;
      case 0x1:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
            kShortenSeparators;
      case 0x2:
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
            kShortenSeparatorsAndSuccessor;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::BlockBasedTableOptions::IndexShorteningMode::
            kShortenSeparators;
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
