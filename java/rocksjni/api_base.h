// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines the "bridge" object between Java and C++ for
// ROCKSDB_NAMESPACE::ColumnFamilyHandle.

#pragma once

#include <iostream>

#include "rocksdb/db.h"

class APIBase {
  /**
   * @brief control deletion of the RocksDB CFH, which we must avoid if this is
   * the DB's default CFH. We could template this..
   *
   */

 public:
  class SharedPtrHolder {
    const ROCKSDB_NAMESPACE::ColumnFamilyHandle* handle;
    const bool isDefault;

   public:
    virtual ~SharedPtrHolder() {
      if (!isDefault) {
        delete handle;
      }
    }

    SharedPtrHolder(ROCKSDB_NAMESPACE::ColumnFamilyHandle* handle,
                    bool isDefault)
        : handle(handle), isDefault(isDefault){};
  };

 public:
  void check(std::string message);
};