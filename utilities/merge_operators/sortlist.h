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
                                 std::string* new_value, Logger* logger) const
      override;

  virtual const char* Name() const override;

  void make_vector(std::vector<int>& operand, Slice slice) const {
  	  int errors = 0;
  	  do {
  		const char *begin = slice.data_;
  		while (*slice.data_ != ',' && *slice.data_)
  			slice.data_++;
  		try {
  			operand.push_back(std::stoi(std::string(begin, slice.data_)));
  		} catch(...) {
  //			std::cout << "Malformed string: " << std::string(begin, slice.data_) << "\n";
  			errors++;
  		}
  	} while (0 != *slice.data_++);
    }

    std::vector<int> merge(std::vector<int>& left, std::vector<int>& right) const
    {
        // Fill the resultant vector with sorted results from both vectors
        std::vector<int> result;
        unsigned left_it = 0, right_it = 0;

        while(left_it < left.size() && right_it < right.size())
        {
            // If the left value is smaller than the right it goes next
            // into the resultant vector
            if(left[left_it] < right[right_it])
            {
                result.push_back(left[left_it]);
                left_it++;
            }
            else
            {
                result.push_back(right[right_it]);
                right_it++;
            }
        }

        // Push the remaining data from both vectors onto the resultant
        while(left_it < left.size())
        {
            result.push_back(left[left_it]);
            left_it++;
        }

        while(right_it < right.size())
        {
            result.push_back(right[right_it]);
            right_it++;
        }

        return result;
    }
};

} // namespace rocksdb
