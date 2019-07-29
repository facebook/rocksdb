/**
 * A test MergeOperator for rocksdb that implements Merge Sort.
 * It is built using the MergeOperator interface. This is useful for
 * testing/benchmarking
 */
#pragma once
#include <deque>
#include <string>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace rocksdb {

class SortList : public MergeOperator {
 public:
  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override;

  virtual bool PartialMerge(const Slice& /*key*/, const Slice& left_operand,
                            const Slice& right_operand, std::string* new_value,
                            Logger* /*logger*/) const override;

  virtual bool PartialMergeMulti(const Slice& key,
                                 const std::deque<Slice>& operand_list,
                                 std::string* new_value,
                                 Logger* logger) const override;

  virtual const char* Name() const override;

  void make_vector(std::vector<int>& operand, Slice slice) const;

  std::vector<int> merge(std::vector<int>& left, std::vector<int>& right) const;
};

}  // namespace rocksdb
