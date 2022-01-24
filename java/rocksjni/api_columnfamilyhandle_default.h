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

class APIColumnFamilyHandleDefault : public APIWeakDB {
 public:
  ROCKSDB_NAMESPACE::ColumnFamilyHandle* default_unowned;

  APIColumnFamilyHandleDefault(
      std::shared_ptr<ROCKSDB_NAMESPACE::DB> db,
      ROCKSDB_NAMESPACE::ColumnFamilyHandle* default_unowned)
      : APIWeakDB(db), default_unowned(default_unowned){};

  void check(std::string message);

  /**
   * @brief get the default CFH if it exists, otherwise prepare a JNI exception
   * @pre weak db in the superclass should not be deleted - call this method
   * while holding a lock() on it.
   *
   * @param env the JNI environment
   * @param handle of the API object for the default column family
   * @return ROCKSDB_NAMESPACE::ColumnFamilyHandle* the default CFH if it is set
   */
  static ROCKSDB_NAMESPACE::ColumnFamilyHandle* getCFH(JNIEnv* env,
                                                       jlong handle) {
    auto cfhAPI = reinterpret_cast<APIColumnFamilyHandleDefault*>(handle);
    if (cfhAPI->default_unowned == nullptr) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, "Unknown default column family. Is the DB already closed ?");
      return nullptr;
    }
    return cfhAPI->default_unowned;
  }
};
