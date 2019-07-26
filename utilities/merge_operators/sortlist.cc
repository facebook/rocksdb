//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <memory>
#include <iostream>
#include <exception>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/sortlist.h"

using rocksdb::Slice;
using rocksdb::Logger;
using rocksdb::MergeOperator;

namespace rocksdb {

// Merge operator that picks the maximum operand, Comparison is based on
// Slice::compare
  bool SortList::FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const {

	  (void) merge_out;
	  std::vector<int> left;
	  for (Slice slice : merge_in.operand_list) {
    	std::vector<int> right;
    	make_vector(right, slice);
    	left = merge(left, right);
	  }
	  std::string result;
	  for (int i = 0; i < (int)left.size()-1; i++){
		  result.append(std::to_string(left[i])).append(",");
	  }
	  result.append(std::to_string(left.back()));
	  Slice result_slice(result);
	  merge_out->existing_operand = result_slice;
	  return true;
  }

  bool SortList::PartialMerge(const Slice& /*key*/, const Slice& left_operand,
                    const Slice& right_operand, std::string* new_value,
                    Logger* /*logger*/) const {
	  std::vector<int> left;
	  std::vector<int> right;
	  make_vector(left, left_operand);
	  make_vector(right, right_operand);
	  left = merge(left, right);
	  for (int i = 0; i < (int)left.size()-1; i++){
		  new_value->append(std::to_string(left[i])).append(",");
	  }
	  new_value->append(std::to_string(left.back()));
	  return true;
  }

  bool SortList::PartialMergeMulti(const Slice& /*key*/,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value,
                         Logger* /*logger*/) const {
	(void)operand_list;
	(void)new_value;
    return true;
  }

  const char* SortList::Name() const { return "MergeSortOperator"; }

std::shared_ptr<MergeOperator> MergeOperators::CreateSortAndSearchOperator() {
  return std::make_shared<SortList>();
}
}
