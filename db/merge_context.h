//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include <algorithm>
#include <memory>
#include <string>
#include <vector>
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

// The merge context for merging a user key.
// When doing a Get(), DB will create such a class and pass it when
// issuing Get() operation to memtables and version_set. The operands
// will be fetched from the context when issuing partial of full merge.
class MergeContext {
 public:
  // Clear all the operands
  void Clear() {
    operand_list_.clear();
    copied_operands_.clear();
  }

  // Push a merge operand
  void PushOperand(const Slice& operand_slice, bool operand_pinned = false) {
    SetDirectionBackward();

    if (operand_pinned) {
      operand_list_.push_back(operand_slice);
    } else {
      // We need to have our own copy of the operand since it's not pinned
      char* copy = MakeCopy(operand_slice);
      copied_operands_.emplace_back(copy);
      operand_list_.emplace_back(copy, operand_slice.size());
    }
  }

  // Push back a merge operand
  void PushOperandBack(const Slice& operand_slice,
                       bool operand_pinned = false) {
    SetDirectionForward();

    if (operand_pinned) {
      operand_list_.push_back(operand_slice);
    } else {
      // We need to have our own copy of the operand since it's not pinned
      char* copy = MakeCopy(operand_slice);
      copied_operands_.emplace_back(copy);
      operand_list_.emplace_back(copy, operand_slice.size());
    }
  }

  // return total number of operands in the list
  size_t GetNumOperands() const { return operand_list_.size(); }

  // Get the operand at the index.
  Slice GetOperand(size_t index) const {
    assert(index < operand_list_.size());
    SetDirectionForward();
    return operand_list_[index];
  }

  // Same as GetOperandsDirectionForward
  //
  // Note that the returned reference is only good until another call
  // to this MergeContext.  If the returned value is needed for longer,
  // a copy must be made.
  const std::vector<Slice>& GetOperands() const {
    return GetOperandsDirectionForward();
  }

  // Return all the operands in the order as they were merged (passed to
  // FullMerge or FullMergeV2)
  //
  // Note that the returned reference is only good until another call
  // to this MergeContext.  If the returned value is needed for longer,
  // a copy must be made.
  const std::vector<Slice>& GetOperandsDirectionForward() const {
    SetDirectionForward();
    return operand_list_;
  }

  // Return all the operands in the reversed order relative to how they were
  // merged (passed to FullMerge or FullMergeV2)
  //
  // Note that the returned reference is only good until another call
  // to this MergeContext.  If the returned value is needed for longer,
  // a copy must be made.
  const std::vector<Slice>& GetOperandsDirectionBackward() const {
    SetDirectionBackward();
    return operand_list_;
  }

 private:
  static char* MakeCopy(Slice src) {
    char* copy = new char[src.size()];
    memcpy(copy, src.data(), src.size());
    return copy;
  }

  void SetDirectionForward() const {
    if (operands_reversed_ == true) {
      std::reverse(operand_list_.begin(), operand_list_.end());
      operands_reversed_ = false;
    }
  }

  void SetDirectionBackward() const {
    if (operands_reversed_ == false) {
      std::reverse(operand_list_.begin(), operand_list_.end());
      operands_reversed_ = true;
    }
  }

  // List of operands
  mutable std::vector<Slice> operand_list_;
  // Copy of operands that are not pinned.
  std::vector<std::unique_ptr<char[]> > copied_operands_;
  mutable bool operands_reversed_ = true;
};

}  // namespace ROCKSDB_NAMESPACE
