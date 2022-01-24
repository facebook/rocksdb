// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines the "bridge" object between Java and C++ for
// ROCKSDB_NAMESPACE::ColumnFamilyHandle.

#pragma once

#include <jni.h>

#include <iostream>

#include "api_weakdb.h"
#include "portal.h"
#include "rocksdb/db.h"

class APIColumnFamilyHandle : public APIWeakDB {
 public:
  std::weak_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh;

  APIColumnFamilyHandle(
      std::shared_ptr<ROCKSDB_NAMESPACE::DB> db,
      std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh)
      : APIWeakDB(db), cfh(cfh){};

  void check(std::string message);

  /**
   * @brief lock the referenced pointer if the weak pointer is valid
   *
   * @param handle
   * @return std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>
   */
  static std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> lock(
      JNIEnv* env, jlong handle) {
    auto* cfhAPI = reinterpret_cast<APIColumnFamilyHandle*>(handle);
    std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh =
        cfhAPI->cfh.lock();
    if (!cfh) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, "Column family already closed");
    }
    return cfh;
  }
};
