// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::ColumnFamilyHandle.

#include "api_rocksdb.h"

#include <iostream>
#include <memory>

#include "api_columnfamilyhandle.h"
#include "api_iterator.h"

void APIRocksDB::check(std::string message) {
  std::cout << " APIRocksDB::check(); " << message << " ";
  std::cout << " db.use_count() " << db.use_count() << "; ";
  for (auto& cfh : columnFamilyHandles) {
    std::cout << " cfh.use_count() " << cfh.use_count() << "; ";
  }
  std::cout << std::endl;
}

std::unique_ptr<APIIterator> APIRocksDB::newIterator(
    ROCKSDB_NAMESPACE::Iterator* iterator,
    std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh) {
  std::shared_ptr<ROCKSDB_NAMESPACE::Iterator> iter(iterator);
  std::unique_ptr<APIIterator> iterAPI(new APIIterator(db, iter, cfh));
  return iterAPI;
}
