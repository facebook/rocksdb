//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/merge_operators/counter_delete.h"

#include <cstdint>
#include <string>
#include <variant>

#include "rocksdb/slice.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

namespace {

inline int64_t DecodeSignedInt64OrZero(const Slice& s) {
  if (s.size() != sizeof(int64_t)) {
    return 0;
  }
  return static_cast<int64_t>(DecodeFixed64(s.data()));
}

}  // namespace

bool CounterDeleteOperator::FullMergeV3(
    const MergeOperationInputV3& merge_in,
    MergeOperationOutputV3* merge_out) const {
  assert(merge_out);

  int64_t counter = 0;
  if (const auto* base = std::get_if<Slice>(&merge_in.existing_value)) {
    counter = DecodeSignedInt64OrZero(*base);
  }
  // Note: monostate base (no existing value) starts counter at 0.
  // Wide-column base values are not supported by this reference operator
  // and are treated as zero.

  for (const Slice& operand : merge_in.operand_list) {
    counter += DecodeSignedInt64OrZero(operand);
  }

  if (counter <= 0) {
    // Signal deletion. The library will materialize this as a tombstone
    // (or drop the key entirely at the bottommost level during compaction).
    merge_out->new_value = std::monostate{};
    return true;
  }

  std::string out;
  PutFixed64(&out, static_cast<uint64_t>(counter));
  merge_out->new_value = std::move(out);
  return true;
}

std::shared_ptr<MergeOperator> MergeOperators::CreateCounterDeleteOperator() {
  return std::make_shared<CounterDeleteOperator>();
}

}  // namespace ROCKSDB_NAMESPACE
