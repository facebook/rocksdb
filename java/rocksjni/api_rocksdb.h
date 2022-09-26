// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::RocksDB.

#pragma once

#include <jni.h>

#include "rocksdb/db.h"

class APIRocksDB {
 public:
  std::shared_ptr<ROCKSDB_NAMESPACE::DB> db;
  std::vector<std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>>
      columnFamilyHandles;

  APIRocksDB(std::shared_ptr<ROCKSDB_NAMESPACE::DB> db) : db(db){};
};

/**
 * @brief construct reference counted DB API object from DB*
 *
 * @param jresult_db_handle
 * @return jlong
 */
jlong db_api(jlong jresult_db_handle);

/**
 * @brief construct reference counted DB API object and CFH API objects from
 * DB*, CF*[]
 *
 * @param jresult_handles
 * @return jlongArray
 */
jlongArray db_api(jlongArray jresult_handles);