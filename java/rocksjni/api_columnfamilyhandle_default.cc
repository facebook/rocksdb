// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines the "bridge" methods between Java and C++ for
// ROCKSDB_NAMESPACE::ColumnFamilyHandle.

#include "api_columnfamilyhandle_default.h"

#include <iostream>
#include <memory>

void APIColumnFamilyHandleDefault::check(std::string message) {
  std::cout << " APIColumnFamilyHandleDefault::check(); " << message << " ";
  std::shared_ptr<ROCKSDB_NAMESPACE::DB> dbLocked = db.lock();
  if (dbLocked) {
    std::cout << " db.use_count() " << dbLocked.use_count() << "; ";
  } else {
    std::cout << " db 0 uses; ";
  }
  std::cout << std::endl;
}