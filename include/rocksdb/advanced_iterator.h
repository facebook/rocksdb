//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

enum class IterBoundCheck : char {
  kUnknown = 0,
  kOutOfBound,
  kInbound,
};

// This structure encapsulates the result of NextAndGetResult()
struct IterateResult {
  // The lifetime of key is guaranteed until Next()/NextAndGetResult() is
  // called.
  Slice key;
  // If the iterator becomes invalid after a NextAndGetResult(), the table
  // iterator should set this to indicate whether it became invalid due
  // to the next key being out of bound (kOutOfBound) or it reached end
  // of file (kUnknown). If the iiterator is still valid, this should
  // be set to kInbound.
  IterBoundCheck bound_check_result = IterBoundCheck::kUnknown;
  // If false, PrepareValue() needs to be called before value()
  // This is useful if the table reader wants to materialize the value in a
  // lazy manner. In that case, it can set this to false and RocksDB
  // guarantees that it'll call PrepareValue() before calling value().
  bool value_prepared = true;
};

}  // namespace ROCKSDB_NAMESPACE
