//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/merge_operator.h"

namespace ROCKSDB_NAMESPACE {

// A test merge operator that implements the wide-column aware FullMergeV3
// interface. Similarly to the simple "put" type merge operators, the merge
// result is based on the last merge operand; however, the merge result can
// potentially be a wide-column entity, depending on the value base encoded into
// the merge operand and the value of the "use_put_entity_one_in" stress test
// option. Following the same rule as for writes ensures that the queries
// issued by the validation logic receive the expected results.
class DBStressWideMergeOperator : public MergeOperator {
 public:
  bool FullMergeV3(const MergeOperationInputV3& merge_in,
                   MergeOperationOutputV3* merge_out) const override;

  const char* Name() const override { return "DBStressWideMergeOperator"; }
};

}  // namespace ROCKSDB_NAMESPACE
