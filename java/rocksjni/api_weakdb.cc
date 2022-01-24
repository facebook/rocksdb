// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::ColumnFamilyHandle.

#include "api_columnfamilyhandle_default.h"
#include "api_rocksdb.h"

/**
 * @brief lock the referenced pointer if the weak pointer is valid
 *
 * @param handle
 * @return std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>
 */
std::shared_ptr<ROCKSDB_NAMESPACE::DB> APIWeakDB::lockDB(JNIEnv* env,
                                                         jlong handle) {
  auto* weakDBAPI = reinterpret_cast<APIWeakDB*>(handle);
  std::shared_ptr<ROCKSDB_NAMESPACE::DB> db = weakDBAPI->db.lock();
  if (!db) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Column family (DB) already closed");
  }
  return db;
}
