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
// The portal class for org.rocksdb.TxnDBWritePolicy
class TxnDBWritePolicyJni {
 public:
  // Returns the equivalent org.rocksdb.TxnDBWritePolicy for the provided
  // C++ ROCKSDB_NAMESPACE::TxnDBWritePolicy enum
  static jbyte toJavaTxnDBWritePolicy(
      const ROCKSDB_NAMESPACE::TxnDBWritePolicy& txndb_write_policy) {
    switch (txndb_write_policy) {
      case ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_COMMITTED:
        return 0x0;
      case ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_PREPARED:
        return 0x1;
      case ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_UNPREPARED:
        return 0x2;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ ROCKSDB_NAMESPACE::TxnDBWritePolicy enum for the
  // provided Java org.rocksdb.TxnDBWritePolicy
  static ROCKSDB_NAMESPACE::TxnDBWritePolicy toCppTxnDBWritePolicy(
      jbyte jtxndb_write_policy) {
    switch (jtxndb_write_policy) {
      case 0x0:
        return ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_COMMITTED;
      case 0x1:
        return ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_PREPARED;
      case 0x2:
        return ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_UNPREPARED;
      default:
        // undefined/default
        return ROCKSDB_NAMESPACE::TxnDBWritePolicy::WRITE_COMMITTED;
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
