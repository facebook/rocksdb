// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines a "bridge" object between Java and C++ for
// weak references to a RocksDB database. That is to say, the database
// may be closed by other methods and we need to be cognisant of that.

#pragma once

#include <jni.h>

#include <iostream>

#include "api_base.h"
#include "portal.h"
#include "rocksdb/db.h"

template <class TDatabase>
class APIWeakDB : public APIBase {
 public:
  std::weak_ptr<TDatabase> db;

  APIWeakDB(std::shared_ptr<TDatabase> db) : db(db){};

  /**
   * @brief lock the referenced pointer if the weak pointer is valid
   *
   * @return std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>
   */
  const std::shared_ptr<TDatabase> dbLock(JNIEnv* env) const {
    auto lock = db.lock();
    if (!lock) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::DBIsClosed());
    }
    return lock;
  }

  /**
   * @brief lock the referenced pointer if the weak pointer is valid
   *
   * @param handle
   * @return std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>
   */
  static std::shared_ptr<TDatabase> lockDB(JNIEnv* env, jlong handle) {
    auto* weakDBAPI = reinterpret_cast<APIWeakDB*>(handle);
    auto lock = weakDBAPI->db.lock();
    if (!lock) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, "Internal DB is already closed");
    }
    return lock;
  }
};
