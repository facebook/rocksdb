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
// The portal class for org.rocksdb.RateLimiterMode
class RateLimiterModeJni {
 public:
  // Returns the equivalent org.rocksdb.RateLimiterMode for the provided
  // C++ ROCKSDB_NAMESPACE::RateLimiter::Mode enum
  static jbyte toJavaRateLimiterMode(
      const ROCKSDB_NAMESPACE::RateLimiter::Mode& rate_limiter_mode) {
    switch (rate_limiter_mode) {
      case ROCKSDB_NAMESPACE::RateLimiter::Mode::kReadsOnly:
        return 0x0;
      case ROCKSDB_NAMESPACE::RateLimiter::Mode::kWritesOnly:
        return 0x1;
      case ROCKSDB_NAMESPACE::RateLimiter::Mode::kAllIo:
        return 0x2;

      default:
        // undefined/default
        return 0x1;
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::RateLimiter::Mode enum for
  // the provided Java org.rocksdb.RateLimiterMode
  static ROCKSDB_NAMESPACE::RateLimiter::Mode toCppRateLimiterMode(
      jbyte jrate_limiter_mode) {
    switch (jrate_limiter_mode) {
      case 0x0:
        return ROCKSDB_NAMESPACE::RateLimiter::Mode::kReadsOnly;
      case 0x1:
        return ROCKSDB_NAMESPACE::RateLimiter::Mode::kWritesOnly;
      case 0x2:
        return ROCKSDB_NAMESPACE::RateLimiter::Mode::kAllIo;

      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::RateLimiter::Mode::kWritesOnly;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
