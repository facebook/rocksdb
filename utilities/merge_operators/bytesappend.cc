/**
*  Created by Jesse Ma on 2019-05-22.
*  for bytes append operator
*/

#include "bytesappend.h"

#include <memory>
#include <assert.h>
#include "utilities/merge_operators.h"

namespace rocksdb {
// Implementation for the merge operation (concatenates two strings)
bool BytesAppendOperator::Merge(const Slice & /*key*/,
                                const Slice *existing_value,
                                const Slice &value,
                                std::string *new_value,
                                Logger * /*logger*/) const {
  // Clear the *new_value for writing.
  assert(new_value);
  new_value->clear();

  if (!existing_value) {
    // No existing_value. Set *new_value = value
    new_value->assign(value.data(), value.size());
  } else {
    // Generic append (existing_value != null).
    // Reserve *new_value to correct size, and apply concatenation.
    new_value->reserve(existing_value->size() + value.size());
    new_value->assign(existing_value->data(), existing_value->size());
    new_value->append(value.data(), value.size());
  }
  return true;
}

const char *BytesAppendOperator::Name() const {
  return "BytesAppendOperator";
}

std::shared_ptr<MergeOperator> MergeOperators::CreateBytesAppendOperator() {
  return std::make_shared<BytesAppendOperator>();
}

}