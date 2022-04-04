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

class APIIterator;

class APIRocksDB : APIBase {
 public:
  /**
   * @brief dump some status info to std::cout
   *
   */
  void check(std::string message);

  std::shared_ptr<ROCKSDB_NAMESPACE::DB> db;
  std::vector<std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>>
      columnFamilyHandles;
  // Every default APIColumnFamilyHandle must start from (i.e. share) this
  // shared_ptr
  std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>
      defaultColumnFamilyHandle;

  APIRocksDB(std::shared_ptr<ROCKSDB_NAMESPACE::DB> db)
      : db(db),
        defaultColumnFamilyHandle(APIBase::createSharedPtr(
            db->DefaultColumnFamily(), true /*isDefault*/)){};

  ROCKSDB_NAMESPACE::DB* operator->() const { return db.get(); }

  std::shared_ptr<ROCKSDB_NAMESPACE::DB>& operator*() { return db; }

  ROCKSDB_NAMESPACE::DB* get() const { return db.get(); }

  std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> DefaultColumnFamily()
      const {
    return defaultColumnFamilyHandle;
  }

  std::unique_ptr<APIIterator> newIterator(
      ROCKSDB_NAMESPACE::Iterator* iterator,
      std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh);
};
