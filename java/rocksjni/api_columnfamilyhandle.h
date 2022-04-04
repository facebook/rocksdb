// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines the "bridge" object between Java and C++ for
// ROCKSDB_NAMESPACE::ColumnFamilyHandle.

#pragma once

#include "api_base.h"
#include "rocksdb/db.h"

class APIColumnFamilyHandle : APIBase {
 public:
  std::weak_ptr<ROCKSDB_NAMESPACE::DB> db;
  std::weak_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh;

  APIColumnFamilyHandle(
      std::shared_ptr<ROCKSDB_NAMESPACE::DB> db,
      std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle> cfh)
      : db(db), cfh(cfh){};

  void check();
};
