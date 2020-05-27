/**
 * A MergeOperator for rocksdb that implements string append.
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#pragma once
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class StringAppendOperator : public SimpleAssociativeMergeOperator {
 public:
  // Constructor: specify delimiter
  explicit StringAppendOperator(char delim_char);

  virtual bool BinaryMerge(const Slice& key, const Slice& left,
                           const Slice& right, std::string* result,
                           Logger* logger) const override;

  virtual const char* Name() const override;

 private:
  char delim_;         // The delimiter is inserted between elements
};

}  // namespace ROCKSDB_NAMESPACE
