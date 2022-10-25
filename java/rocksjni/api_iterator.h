// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines the "bridge" object between Java and C++ for
// ROCKSDB_NAMESPACE::Iterator.
//
// The shared objects are used to ensure the lifetime of their contents
// When the Iterator is not a DB iterator, db may be empty.
//

#pragma once

#include <iostream>

#include "api_base.h"
#include "rocksdb/db.h"

template <class TDatabase, class TIterator>
class APIIterator : APIBase {
 public:
  std::shared_ptr<TDatabase> db;
  std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh;
  std::unique_ptr<TIterator> iterator;

  APIIterator(const std::shared_ptr<TDatabase>& db,
              std::unique_ptr<TIterator> iterator,
              const std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>& cfh)
      : db(db), cfh(cfh), iterator(std::move(iterator)){};

  APIIterator(const std::shared_ptr<TDatabase>& db,
              std::unique_ptr<TIterator> iterator)
      : db(db), iterator(std::move(iterator)){};

  TIterator* operator->() const { return iterator.get(); }

  std::shared_ptr<TIterator>& operator*() { return iterator; }

  TIterator* get() const { return iterator.get(); }

  template <class TChildIterator>
  std::unique_ptr<APIIterator<TDatabase, TChildIterator>> childIteratorWithBase(
      TChildIterator* rocksdbIterator,
      std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>& rocksdbCFH) {
    auto childIterator =
        std::make_unique<APIIterator<TDatabase, TChildIterator>>(
            db, std::move(std::unique_ptr<TIterator>(rocksdbIterator)),
            rocksdbCFH);
    // Internally, ~BaseDeltaIterator() deletes its base iterator,
    // therefore it is effectively delete()d by childIterator,
    // and we here should NOT delete() it. Hence the call to release()
    // Subsequent uses of this will segfault
    iterator.release();
    return childIterator;
  }

  std::vector<long> use_counts() {
    std::vector<long> vec;

    vec.push_back(db.use_count());
    vec.push_back(cfh.use_count());

    return vec;
  }
};