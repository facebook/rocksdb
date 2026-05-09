//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <utility>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Wraps a value of type T and tracks whether it has been mutated since the
// last Reset(). Reset() short-circuits when the value is not dirty — useful
// when T::Reset() is non-trivial (allocations, container clears) and the
// value is rarely populated on a hot path.
//
// Requirements on T:
//   - exposes a `void Reset()` method that returns it to a clean state.
//
// Read access via operator-> / operator* always returns a const view, so
// reads are free of dirty bookkeeping regardless of whether the wrapper
// itself is const. Mutation must go through mut(), which marks dirty and
// returns a mutable pointer.
//
// Thread safety: not thread safe. The owning object is responsible for
// synchronization.
template <typename T>
class DirtyTracked {
 public:
  // Forwards constructor arguments to T.
  template <typename... Args>
  explicit DirtyTracked(Args&&... args) : value_{std::forward<Args>(args)...} {}

  // Read-only access.
  const T* operator->() const { return &value_; }
  const T& operator*() const { return value_; }

  // Explicit mutating access — marks dirty, returns a mutable pointer.
  T* mut() {
    dirty_ = true;
    return &value_;
  }

  void Reset() {
    if (dirty_) {
      value_.Reset();
      dirty_ = false;
    }
  }

 private:
  T value_;
  bool dirty_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
