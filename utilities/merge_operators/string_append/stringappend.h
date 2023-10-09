/**
 * A MergeOperator for rocksdb that implements string append.
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#pragma once
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class StringAppendOperator : public AssociativeMergeOperator {
 public:
  // Constructor: specify delimiter
  explicit StringAppendOperator(char delim_char);
  explicit StringAppendOperator(const std::string& delim);

  virtual bool Merge(const Slice& key, const Slice* existing_value,
                     const Slice& value, std::string* new_value,
                     Logger* logger) const override;

  static const char* kClassName() { return "StringAppendOperator"; }
  static const char* kNickName() { return "stringappend"; }
  virtual const char* Name() const override { return kClassName(); }
  virtual const char* NickName() const override { return kNickName(); }

 private:
  std::string delim_;  // The delimiter is inserted between elements
};

}  // namespace ROCKSDB_NAMESPACE
