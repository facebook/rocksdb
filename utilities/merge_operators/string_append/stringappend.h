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

  virtual bool Merge(const Slice& key,
                     const Slice* existing_value,
                     const Slice& value,
                     std::string* new_value,
                     Logger* logger) const override;

  virtual const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "StringAppendOperator"; }
  bool IsInstanceOf(const std::string& id) const override {
    if (id == "stringappend") {
      return true;
    } else {
      return AssociativeMergeOperator::IsInstanceOf(id);
    }
  }

 private:
  std::string delim_;  // The delimiter is inserted between elements
};

}  // namespace ROCKSDB_NAMESPACE
