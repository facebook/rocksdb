// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::RocksDB.

#pragma once

#include <jni.h>

#include "api_base.h"
#include "rocksdb/db.h"

class APIRocksDB : APIBase {
 public:
  std::shared_ptr<ROCKSDB_NAMESPACE::DB> db;
  std::vector<std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>>
      columnFamilyHandles;

  APIRocksDB(std::shared_ptr<ROCKSDB_NAMESPACE::DB> db) : db(db){};

  ROCKSDB_NAMESPACE::DB* operator->() const { return db.get(); }

  /**
   * @brief dump some status info to std::cout
   *
   */
  void check(std::string message);
};
