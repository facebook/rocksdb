/**
 * Back-end implementation details specific to the Merge Operator.
 *
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#include "rocksdb/merge_operator.h"

namespace leveldb {

// Given a "real" merge from the library, call the user's
// associative merge function one-by-one on each of the operands.
// NOTE: It is assumed that the client's merge-operator will handle any errors.
bool AssociativeMergeOperator::FullMerge(
    const Slice& key,
    const Slice* existing_value,
    const std::deque<std::string>& operand_list,
    std::string* new_value,
    Logger* logger) const {

  // Simply loop through the operands
  Slice temp_existing;
  std::string temp_value;
  for (const auto& operand : operand_list) {
    Slice value(operand);
    if (!Merge(key, existing_value, value, &temp_value, logger)) {
      return false;
    }
    swap(temp_value, *new_value);
    temp_existing = Slice(*new_value);
    existing_value = &temp_existing;
  }

  // The result will be in *new_value. All merges succeeded.
  return true;
}

// Call the user defined simple merge on the operands;
// NOTE: It is assumed that the client's merge-operator will handle any errors.
bool AssociativeMergeOperator::PartialMerge(
    const Slice& key,
    const Slice& left_operand,
    const Slice& right_operand,
    std::string* new_value,
    Logger* logger) const {

  return Merge(key, &left_operand, right_operand, new_value, logger);
}

} // namespace leveldb
