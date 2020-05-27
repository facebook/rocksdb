/**
 * A MergeOperator for rocksdb that implements string append.
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#include "stringappend.h"

#include <memory>
#include <assert.h>

#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

// Constructor: also specify the delimiter character.
StringAppendOperator::StringAppendOperator(char delim_char)
    : delim_(delim_char) {
}

// Implementation for the merge operation (concatenates two strings)
bool StringAppendOperator::BinaryMerge(const Slice& /*key*/, const Slice& left,
                                       const Slice& right, std::string* result,
                                       Logger* /*logger*/) const {
  assert(result);
  assert(result->empty());

  // Generic append.
  // Reserve *result to correct size, and apply concatenation.
  result->reserve(left.size() + 1 + right.size());
  result->assign(left.data(), left.size());
  result->append(1, delim_);
  result->append(right.data(), right.size());

  return true;
}

const char* StringAppendOperator::Name() const  {
  return "StringAppendOperator";
}

std::shared_ptr<MergeOperator> MergeOperators::CreateStringAppendOperator() {
  return std::make_shared<StringAppendOperator>(',');
}

std::shared_ptr<MergeOperator> MergeOperators::CreateStringAppendOperator(char delim_char) {
  return std::make_shared<StringAppendOperator>(delim_char);
}

}  // namespace ROCKSDB_NAMESPACE
