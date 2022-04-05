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

template <class TDatabase>
class APIIterator;

/**
 * @brief Most commonly used to hold a ROCKSDB_NAMESPACE::DB
 * Can also handle ROCKSDB_NAMESPACE::OptimisticTransactionDB
 */
template <class TDatabase>
class APIRocksDB : APIBase {
 public:

  std::shared_ptr<TDatabase> db;
  std::vector<std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>>
      columnFamilyHandles;
  // Every default APIColumnFamilyHandle must start from (i.e. share) this
  // shared_ptr
  std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>
      defaultColumnFamilyHandle;

  APIRocksDB(std::shared_ptr<TDatabase> db)
      : db(db),
        defaultColumnFamilyHandle(APIBase::createSharedPtr(
            db->DefaultColumnFamily(), true /*isDefault*/)){};

  TDatabase* operator->() const { return db.get(); }

  std::shared_ptr<TDatabase>& operator*() { return db; }

  TDatabase* get() const { return db.get(); }

  std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> DefaultColumnFamily()
      const {
    return defaultColumnFamilyHandle;
  }

  /**
   * @brief dump some status info to std::cout
   *
   */
  void check(std::string message) {
    std::cout << " APIRocksDB::check(); " << message << " ";
    std::cout << " db.use_count() " << db.use_count() << "; ";
    for (auto& cfh : columnFamilyHandles) {
      std::cout << " cfh.use_count() " << cfh.use_count() << "; ";
    }
    std::cout << std::endl;
  }

  std::unique_ptr<APIIterator<TDatabase>> newIterator(
      ROCKSDB_NAMESPACE::Iterator* iterator,
      std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh) {
    std::shared_ptr<ROCKSDB_NAMESPACE::Iterator> iter(iterator);
    std::unique_ptr<APIIterator<TDatabase>> iterAPI(new APIIterator(db, iter, cfh));
    return iterAPI;
  }
};
