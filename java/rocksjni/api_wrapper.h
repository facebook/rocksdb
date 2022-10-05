// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file defines a template for the "bridge" object between Java and C++ for
// any simple object; defined as one which doesn't hold any other references,
// especially references to a containing database and/or a column family handle.

#pragma once

#include <iostream>

#include "rocksdb/db.h"
#include "rocksdb/utilities/write_batch_with_index.h"

/**
 * @brief wrapper for simple native RocksDB class that does not need/hold a
 * database reference for its lifetime
 *
 */
template <class TWrapped>
class APIWrapper {  // no DB, so no APIBase inheritance

 public:
  std::shared_ptr<TWrapped> wrapped;

  APIWrapper(std::shared_ptr<TWrapped>& wrapped) : wrapped(wrapped){};

  TWrapped* operator->() const { return wrapped.get(); }

  std::shared_ptr<TWrapped>& operator*() { return wrapped; }

  TWrapped* get() const { return wrapped.get(); }

  /**
   * @brief dump some status info to std::cout
   *
   */
  void check(std::string message) {
    std::cout << " APIWrapper::check(); " << message << " ";
    std::cout << " wrapped.use_count() " << wrapped.use_count() << "; ";
    std::cout << std::endl;
  }
};
