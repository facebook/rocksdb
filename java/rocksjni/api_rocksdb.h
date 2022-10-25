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

template <class TDatabase, class TIterator>
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

  APIRocksDB(std::shared_ptr<TDatabase>& db)
      : db(db),
        defaultColumnFamilyHandle(APIBase::createSharedPtr(
            db->DefaultColumnFamily(), true /*isDefault*/)){};

  /**
   * @brief Construct a new APIRocksDB object sharing an existing object
   * Used to create an APIRocksDB for the base DB of an optimistic DB,
   * it copies the column family handles from the source DB, but uses the
   * supplied "new" db
   *
   * @tparam TSourceDatabase
   * @param db
   * @param sourceDB
   */
  template <class TSourceDatabase>
  APIRocksDB(std::shared_ptr<TDatabase>& newDB,
             APIRocksDB<TSourceDatabase>& sourceDB)
      : db(newDB),
        columnFamilyHandles(sourceDB.columnFamilyHandles),
        defaultColumnFamilyHandle(sourceDB.defaultColumnFamilyHandle){};

  TDatabase* operator->() const { return db.get(); }

  std::shared_ptr<TDatabase>& operator*() { return db; }

  TDatabase* get() const { return db.get(); }

  std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> DefaultColumnFamily()
      const {
    return defaultColumnFamilyHandle;
  }

  std::vector<long> use_counts() {
    std::vector<long> vec;

    vec.push_back(db.use_count());
    for (auto const& cfh : columnFamilyHandles) {
      vec.push_back(cfh.use_count());
    }

    return vec;
  }

  template <class TIterator>
  std::unique_ptr<APIIterator<TDatabase, TIterator>> newIterator(
      TIterator* iterator,
      std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>& cfh) {
    return std::move(std::make_unique<APIIterator<TDatabase, TIterator>>(
        db, std::move(std::unique_ptr<TIterator>(iterator)), cfh));
  }

  template <class TIterator>
  std::unique_ptr<APIIterator<TDatabase, TIterator>> newIterator(
      TIterator* iterator) {
    return std::move(std::make_unique<APIIterator<TDatabase, TIterator>>(
        db, std::move(std::unique_ptr<TIterator>(iterator))));
  }
};
