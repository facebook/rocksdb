// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines the "bridge" object between Java and C++ for
// ROCKSDB_NAMESPACE::Iterator.

#pragma once

#include <iostream>

#include "api_base.h"
#include "rocksdb/db.h"

template <class TDatabase>
class APIIterator : APIBase {
 public:
  std::shared_ptr<TDatabase> db;
  std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh;
  std::shared_ptr<ROCKSDB_NAMESPACE::Iterator> iterator;

  APIIterator(std::shared_ptr<TDatabase> db,
              std::shared_ptr<ROCKSDB_NAMESPACE::Iterator> iterator,
              std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh)
      : db(db), cfh(cfh), iterator(iterator){};

  ROCKSDB_NAMESPACE::Iterator* operator->() const { return iterator.get(); }

  std::shared_ptr<ROCKSDB_NAMESPACE::Iterator>& operator*() { return iterator; }

  ROCKSDB_NAMESPACE::Iterator* get() const { return iterator.get(); }

  void check(std::string message) {
    std::cout << " APIIterator::check(); " << message << " ";
    std::cout << " iterator.use_count() " << iterator.use_count() << "; ";
    std::cout << " db.use_count() " << db.use_count() << "; ";
    std::cout << " cfh.use_count() " << cfh.use_count();
    std::cout << std::endl;
  }
};