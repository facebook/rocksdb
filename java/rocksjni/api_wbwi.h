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
#include "rocksdb/utilities/write_batch_with_index.h"

/**
 * @brief wrapper for WBWI - does not need/hold a database reference for its
 * lifetime
 *
 */
class APIWriteBatchWithIndex {  // no DB, so no APIBase inheritance

 public:
  std::shared_ptr<ROCKSDB_NAMESPACE::WriteBatchWithIndex> wbwi;

  APIWriteBatchWithIndex(
      std::shared_ptr<ROCKSDB_NAMESPACE::WriteBatchWithIndex>& wbwi)
      : wbwi(wbwi){};

  ROCKSDB_NAMESPACE::WriteBatchWithIndex* operator->() const {
    return wbwi.get();
  }

  std::shared_ptr<ROCKSDB_NAMESPACE::WriteBatchWithIndex>& operator*() {
    return wbwi;
  }

  ROCKSDB_NAMESPACE::WriteBatchWithIndex* get() const { return wbwi.get(); }

  /**
   * @brief dump some status info to std::cout
   *
   */
  void check(std::string message) {
    std::cout << " APIWBWI::check(); " << message << " ";
    std::cout << " wbwi.use_count() " << wbwi.use_count() << "; ";
    std::cout << std::endl;
  }
};