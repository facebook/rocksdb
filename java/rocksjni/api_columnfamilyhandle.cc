// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines the "bridge" methods between Java and C++ for
// ROCKSDB_NAMESPACE::ColumnFamilyHandle.

#include "api_columnfamilyhandle.h"

#include <iostream>
#include <memory>

void APIColumnFamilyHandle::check() {
  std::cout << " APIColumnFamilyHandle::check() " << std::endl;
  std::shared_ptr<ROCKSDB_NAMESPACE::DB> dbLocked = db.lock();
  std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfhLocked = cfh.lock();
  if (dbLocked) {
    std::cout << " db.use_count() " << dbLocked.use_count() << std::endl;
  } else {
    std::cout << " db 0 uses " << std::endl;
  }
  if (cfhLocked) {
    std::cout << " cfh.use_count() " << cfhLocked.use_count() << std::endl;
  } else {
    std::cout << " cfh 0 uses " << std::endl;
  }
}