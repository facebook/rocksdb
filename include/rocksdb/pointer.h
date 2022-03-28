//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// UniquePtrOut<T> can be initialized from either std::unique_ptr<T>* or T**.
// This provides for API evolution with backward compatibility for out
// parameters that should have type std::unique_ptr<T>* but were previously
// T**. This is a special-purpose class that should only replace
// existing uses of T** for output parameters taking ownership of an object.
// We expect to replace uses of this class with std::unique_ptr<T>* in a
// future release.
template <typename T>
class UniquePtrOut {
 public:
  /* implicit */ UniquePtrOut(std::unique_ptr<T> *p) : p_(p) {
    // Reset early to close any old resource, for example if we
    //   std::unique_ptr<DB> db;
    //   DB::Open(..., &db);
    //   ...
    //   DB::Open(..., &db);
    // then we want to be sure the previous DB is closed before we
    // start opening the next.
    p->reset();
  }
  /* implicit */ UniquePtrOut(T **p)
      : p_(reinterpret_cast<std::unique_ptr<T> *>(p)) {
    static_assert(sizeof(T *) == sizeof(std::unique_ptr<T>),
                  "Raw pointer and unique_ptr must be reinterpret convertible");
    // Match reset behavior on unique_ptr, except because *p will be accessed
    // as if it's a unique_ptr, we need to ensure that the current content of
    // *p is not erroneously deleted.
    *p = nullptr;
  }

  inline std::unique_ptr<T> &operator*() const { return *p_; }
  inline std::unique_ptr<T> *operator->() const noexcept { return p_; }

 private:
  std::unique_ptr<T> *const p_;
};

}  // namespace ROCKSDB_NAMESPACE
