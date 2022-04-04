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

#include "api_rocksdb.h"
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

  std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfhLock(JNIEnv* env) {
    auto lock = cfh.lock();
    if (!lock) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::OrphanedColumnFamily());
    }
    return lock;
  }

  /**
   * @brief lock the CF (std::shared_ptr) if the weak pointer is valid, and
   * check we have the correct DB
   * @return locked CF if the weak ptr is still valid and the DB matches, empty
   * ptr otherwise
   */
  std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfhLockDBCheck(
      JNIEnv* env, APIRocksDB& dbAPI) {
    if (dbLock(env) != *dbAPI) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env,
          ROCKSDB_NAMESPACE::RocksDBExceptionJni::MismatchedColumnFamily());
      std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> lock;
      return lock;
    }
    auto lock = cfh.lock();
    if (!lock) {
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, ROCKSDB_NAMESPACE::RocksDBExceptionJni::OrphanedColumnFamily());
    }
    return lock;
  }

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

  /**
   * @brief create a CFH wrapped with a SharedPtrContent which will delete the
   * CFH at delete() time
   *
   * @param handle
   * @return std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>
   */
  static std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> createSharedPtr(
      ROCKSDB_NAMESPACE::ColumnFamilyHandle* handle) {
    std::shared_ptr<SharedPtrHolder> holder(new SharedPtrHolder(handle, false));
    return std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>(holder,
                                                                  handle);
  };
};
