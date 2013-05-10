/**
 * A MergeOperator for rocksdb/leveldb that implements string append.
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook
 */

#include "leveldb/merge_operator.h"
#include "leveldb/slice.h"

namespace leveldb {

class StringAppendOperator : public MergeOperator {
 public:

  StringAppendOperator(char delim_char);    /// Constructor: specify delimiter

  virtual void Merge(const Slice& key,
                     const Slice* existing_value,
                     const Slice& value,
                     std::string* new_value,
                     Logger* logger) const override;

  virtual const char* Name() const override;

 private:
 char delim_;         // The delimiter is inserted between elements

};

} // namespace leveldb

