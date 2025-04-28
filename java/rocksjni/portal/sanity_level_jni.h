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
// The portal class for org.rocksdb.SanityLevel
class SanityLevelJni {
 public:
  // Returns the equivalent org.rocksdb.SanityLevel for the provided
  // C++ ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel enum
  static jbyte toJavaSanityLevel(
      const ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel& sanity_level) {
    switch (sanity_level) {
      case ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel::kSanityLevelNone:
        return 0x0;
      case ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel::
          kSanityLevelLooselyCompatible:
        return 0x1;
      case ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel::
          kSanityLevelExactMatch:
        return -0x01;
      default:
        return -0x01;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel
  // enum for the provided Java org.rocksdb.SanityLevel
  static ROCKSDB_NAMESPACE::ConfigOptions::SanityLevel toCppSanityLevel(
      jbyte sanity_level) {
    switch (sanity_level) {
      case 0x0:
        return ROCKSDB_NAMESPACE::ConfigOptions::kSanityLevelNone;
      case 0x1:
        return ROCKSDB_NAMESPACE::ConfigOptions::kSanityLevelLooselyCompatible;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::ConfigOptions::kSanityLevelExactMatch;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
