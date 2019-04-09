//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/cache.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

template <typename T>
class SimpleDeleter : public Cache::Deleter {
 public:
  static SimpleDeleter* GetInstance() {
    static auto deleter = new SimpleDeleter;
    return deleter;
  }

  void operator()(const Slice& /* key */, void* value) override {
    T* const t = static_cast<T*>(value);
    delete t;
  }

 private:
  SimpleDeleter() = default;
};

template <typename T>
class SimpleDeleter<T[]> : public Cache::Deleter {
 public:
  static SimpleDeleter* GetInstance() {
    static auto deleter = new SimpleDeleter;
    return deleter;
  }

  void operator()(const Slice& /* key */, void* value) override {
    T* const t = static_cast<T*>(value);
    delete[] t;
  }

 private:
  SimpleDeleter() = default;
};

}  // namespace ROCKSDB_NAMESPACE
