// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef UTILITIES_MERGE_OPERATORS_BYTESXOR_H_
#define UTILITIES_MERGE_OPERATORS_BYTESXOR_H_

#include <algorithm>
#include <memory>
#include <string>
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

// A 'model' merge operator that XORs two (same sized) array of bytes.
// Implemented as an AssociativeMergeOperator for simplicity and example.
class BytesXOROperator : public AssociativeMergeOperator {
 public:
  // XORs the two array of bytes one byte at a time and stores the result
  // in new_value. len is the number of xored bytes, and the length of new_value
  virtual bool Merge(const Slice& key,
                     const Slice* existing_value,
                     const Slice& value,
                     std::string* new_value,
                     Logger* logger) const override;

  virtual const char* Name() const override {
    return "BytesXOR";
  }

  void XOR(const Slice* existing_value, const Slice& value,
          std::string* new_value) const;
};

}  // namespace rocksdb

#endif  // UTILITIES_MERGE_OPERATORS_BYTESXOR_H_
