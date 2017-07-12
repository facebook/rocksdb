//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.
//
#pragma once
#include <string>
#include <vector>
#include "db/dbformat.h"
#include "rocksdb/slice.h"

namespace rocksdb {

const std::vector<Slice> empty_operand_list;

// The merge context for merging a user key.
// When doing a Get(), DB will create such a class and pass it when
// issuing Get() operation to memtables and version_set. The operands
// will be fetched from the context when issuing partial of full merge.
class MergeContext {
 public:
  // Clear all the operands
  void Clear() {
    if (operand_list_) {
      operand_list_->clear();
      copied_operands_->clear();
    }
  }

  // Push a merge operand
  void PushOperand(const Slice& operand_slice, bool operand_pinned = false) {
    Initialize();
    SetDirectionBackward();

    if (operand_pinned) {
      operand_list_->push_back(operand_slice);
    } else {
      // We need to have our own copy of the operand since it's not pinned
      copied_operands_->emplace_back(
          new std::string(operand_slice.data(), operand_slice.size()));
      operand_list_->push_back(*copied_operands_->back());
    }
  }

  // Push back a merge operand
  void PushOperandBack(const Slice& operand_slice,
                       bool operand_pinned = false) {
    Initialize();
    SetDirectionForward();

    if (operand_pinned) {
      operand_list_->push_back(operand_slice);
    } else {
      // We need to have our own copy of the operand since it's not pinned
      copied_operands_->emplace_back(
          new std::string(operand_slice.data(), operand_slice.size()));
      operand_list_->push_back(*copied_operands_->back());
    }
  }

  // return total number of operands in the list
  size_t GetNumOperands() const {
    if (!operand_list_) {
      return 0;
    }
    return operand_list_->size();
  }

  // Get the operand at the index.
  Slice GetOperand(int index) {
    assert(operand_list_);

    SetDirectionForward();
    return (*operand_list_)[index];
  }

  // Return all the operands.
  const std::vector<Slice>& GetOperands() {
    if (!operand_list_) {
      return empty_operand_list;
    }

    SetDirectionForward();
    return *operand_list_;
  }

 private:
  void Initialize() {
    if (!operand_list_) {
      operand_list_.reset(new std::vector<Slice>());
      copied_operands_.reset(new std::vector<std::unique_ptr<std::string>>());
    }
  }

  void SetDirectionForward() {
    if (operands_reversed_ == true) {
      std::reverse(operand_list_->begin(), operand_list_->end());
      operands_reversed_ = false;
    }
  }

  void SetDirectionBackward() {
    if (operands_reversed_ == false) {
      std::reverse(operand_list_->begin(), operand_list_->end());
      operands_reversed_ = true;
    }
  }

  // List of operands
  std::unique_ptr<std::vector<Slice>> operand_list_;
  // Copy of operands that are not pinned.
  std::unique_ptr<std::vector<std::unique_ptr<std::string>>> copied_operands_;
  bool operands_reversed_ = true;
};

} // namespace rocksdb
