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
// The portal class for org.rocksdb.WalProcessingOption
class WalProcessingOptionJni {
 public:
  // Returns the equivalent org.rocksdb.WalProcessingOption for the provided
  // C++ ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption enum
  static jbyte toJavaWalProcessingOption(
      const ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption&
          wal_processing_option) {
    switch (wal_processing_option) {
      case ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::
          kContinueProcessing:
        return 0x0;
      case ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::
          kIgnoreCurrentRecord:
        return 0x1;
      case ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::kStopReplay:
        return 0x2;
      case ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::kCorruptedRecord:
        return 0x3;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++
  // ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption enum for the provided
  // Java org.rocksdb.WalProcessingOption
  static ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption
  toCppWalProcessingOption(jbyte jwal_processing_option) {
    switch (jwal_processing_option) {
      case 0x0:
        return ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::
            kContinueProcessing;
      case 0x1:
        return ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::
            kIgnoreCurrentRecord;
      case 0x2:
        return ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::kStopReplay;
      case 0x3:
        return ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::
            kCorruptedRecord;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::WalFilter::WalProcessingOption::
            kCorruptedRecord;
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
