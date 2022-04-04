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

void APIRocksDB::check() {
  std::cout << " APIRocksDB::check() " << std::endl;
  std::cout << " db.use_count() " << db.use_count() << "; ";
  for (auto& cfh : columnFamilyHandles) {
    std::cout << " cfh.use_count() " << cfh.use_count() << "; ";
  }
  std::cout << std::endl;
}