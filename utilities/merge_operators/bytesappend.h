/**
*  Created by Jesse Ma on 2019-05-22.
*  for bytes append operator
*/

#pragma once

#include <string>
#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"

namespace rocksdb {

// BytesAppend merge operator appends the bytes to the end of the original value.
class BytesAppendOperator : public AssociativeMergeOperator {
  public:
    virtual bool Merge(const Slice &key,
                       const Slice *existing_value,
                       const Slice &value,
                       std::string *new_value,
                       Logger *logger) const override;

    virtual const char *Name() const override;
};

}  // namespace rocksdb

