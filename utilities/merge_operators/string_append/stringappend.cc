/**
 * A MergeOperator for rocksdb that implements string append.
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#include "stringappend.h"

#include <assert.h>

#include <memory>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/options_type.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {
namespace {
static std::unordered_map<std::string, OptionTypeInfo>
    stringappend_merge_type_info = {
#ifndef ROCKSDB_LITE
        {"delimiter",
         {0, OptionType::kString, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
#endif  // ROCKSDB_LITE
};
}  // namespace
// Constructor: also specify the delimiter character.
StringAppendOperator::StringAppendOperator(char delim_char)
    : delim_(1, delim_char) {
  RegisterOptions("Delimiter", &delim_, &stringappend_merge_type_info);
}

StringAppendOperator::StringAppendOperator(const std::string& delim)
    : delim_(delim) {
  RegisterOptions("Delimiter", &delim_, &stringappend_merge_type_info);
}

// Implementation for the merge operation (concatenates two strings)
bool StringAppendOperator::Merge(const Slice& /*key*/,
                                 const Slice* existing_value,
                                 const Slice& value, std::string* new_value,
                                 Logger* /*logger*/) const {
  // Clear the *new_value for writing.
  assert(new_value);
  new_value->clear();

  if (!existing_value) {
    // No existing_value. Set *new_value = value
    new_value->assign(value.data(),value.size());
  } else {
    // Generic append (existing_value != null).
    // Reserve *new_value to correct size, and apply concatenation.
    new_value->reserve(existing_value->size() + delim_.size() + value.size());
    new_value->assign(existing_value->data(), existing_value->size());
    new_value->append(delim_);
    new_value->append(value.data(), value.size());
  }

  return true;
}


std::shared_ptr<MergeOperator> MergeOperators::CreateStringAppendOperator() {
  return std::make_shared<StringAppendOperator>(',');
}

std::shared_ptr<MergeOperator> MergeOperators::CreateStringAppendOperator(char delim_char) {
  return std::make_shared<StringAppendOperator>(delim_char);
}

std::shared_ptr<MergeOperator> MergeOperators::CreateStringAppendOperator(
    const std::string& delim) {
  return std::make_shared<StringAppendOperator>(delim);
}

}  // namespace ROCKSDB_NAMESPACE
