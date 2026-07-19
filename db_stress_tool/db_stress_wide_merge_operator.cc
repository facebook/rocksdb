//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS

#include "db_stress_tool/db_stress_wide_merge_operator.h"

#include "db_stress_tool/db_stress_common.h"

namespace ROCKSDB_NAMESPACE {

bool DBStressWideMergeOperator::FullMergeV3(
    const MergeOperationInputV3& merge_in,
    MergeOperationOutputV3* merge_out) const {
  assert(!merge_in.operand_list.empty());
  assert(merge_out);

  const Slice& latest = merge_in.operand_list.back();

  if (latest.size() < sizeof(uint32_t)) {
    return false;
  }

  const uint32_t value_base = GetValueBase(latest);

  // Exercise the FullMergeV3 deletion path (`std::monostate`) once per
  // every `use_merge_deletion_one_in` calls on average. The value_base is
  // already deterministically derived from the operand, so the expected
  // state in the stress test can predict whether the key was deleted by
  // applying the same modular test in `ExpectedState`-aware code paths.
  // The deletion check is done BEFORE the put-entity check so that a
  // chosen deletion takes precedence even when it would otherwise be
  // converted into a wide-column write.
  if (FLAGS_use_merge_deletion_one_in > 0 &&
      (value_base % FLAGS_use_merge_deletion_one_in) == 0) {
    merge_out->new_value = std::monostate{};
    return true;
  }

  if (FLAGS_use_put_entity_one_in == 0 ||
      (value_base % FLAGS_use_put_entity_one_in) != 0) {
    merge_out->new_value = latest;
    return true;
  }

  const auto columns = GenerateWideColumns(value_base, latest);

  merge_out->new_value = MergeOperationOutputV3::NewColumns();
  auto& new_columns =
      std::get<MergeOperationOutputV3::NewColumns>(merge_out->new_value);
  new_columns.reserve(columns.size());

  for (const auto& column : columns) {
    new_columns.emplace_back(column.name().ToString(),
                             column.value().ToString());
  }

  return true;
}

// Shared predicate used by the stress validation logic to determine whether
// a particular merge operand would cause the wide-column merge operator to
// resolve to a deletion. Defined here (next to the operator) so both stay
// in sync.
bool DBStressMergeOperandWouldDelete(uint32_t value_base) {
  return FLAGS_use_merge_deletion_one_in > 0 &&
         (value_base % FLAGS_use_merge_deletion_one_in) == 0;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
