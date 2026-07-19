//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

class Logger;
class Slice;

// A reference merge operator that exercises the FullMergeV3 deletion path
// (`std::monostate` result). It treats both the base value and merge
// operands as 64-bit little-endian signed integers, sums them, and:
//   - If the total is > 0: emits the total as the new value.
//   - If the total is <= 0: signals deletion via `std::monostate`.
//
// This implements the canonical "counter that auto-deletes when it reaches
// zero" use case for FullMergeV3 deletion. Useful for db_bench and as a
// minimal example for users.
//
// Wire format: 8-byte little-endian (DecodeFixed64) for both the base
// value and operands. Malformed (wrong-size) inputs are treated as zero,
// matching the convention used by `UInt64AddOperator`.
class CounterDeleteOperator : public MergeOperator {
 public:
  static const char* kClassName() { return "CounterDeleteOperator"; }
  static const char* kNickName() { return "counter_delete"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

  bool FullMergeV3(const MergeOperationInputV3& merge_in,
                   MergeOperationOutputV3* merge_out) const override;
};

}  // namespace ROCKSDB_NAMESPACE
