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
// The portal class for org.rocksdb.WalFileType
class WalFileTypeJni {
 public:
  // Returns the equivalent org.rocksdb.WalFileType for the provided
  // C++ ROCKSDB_NAMESPACE::WalFileType enum
  static jbyte toJavaWalFileType(
      const ROCKSDB_NAMESPACE::WalFileType& wal_file_type) {
    switch (wal_file_type) {
      case ROCKSDB_NAMESPACE::WalFileType::kArchivedLogFile:
        return 0x0;
      case ROCKSDB_NAMESPACE::WalFileType::kAliveLogFile:
        return 0x1;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::WalFileType enum for the
  // provided Java org.rocksdb.WalFileType
  static ROCKSDB_NAMESPACE::WalFileType toCppWalFileType(jbyte jwal_file_type) {
    switch (jwal_file_type) {
      case 0x0:
        return ROCKSDB_NAMESPACE::WalFileType::kArchivedLogFile;
      case 0x1:
        return ROCKSDB_NAMESPACE::WalFileType::kAliveLogFile;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::WalFileType::kAliveLogFile;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
