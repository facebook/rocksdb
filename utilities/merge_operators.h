//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include "rocksdb/merge_operator.h"

#include <stdio.h>

#include <memory>
#include <string>

namespace rocksdb {

class MergeOperators {
 public:
  static std::shared_ptr<MergeOperator> CreatePutOperator();
  static std::shared_ptr<MergeOperator> CreateUInt64AddOperator();
  static std::shared_ptr<MergeOperator> CreateStringAppendOperator();
  static std::shared_ptr<MergeOperator> CreateStringAppendTESTOperator();
  static std::shared_ptr<MergeOperator> CreateBytesXOROperator();

  // Will return a different merge operator depending on the string.
  // TODO(pshareghi): Hook the "name" up to the actual Name() of the operator?
  static std::shared_ptr<MergeOperator> CreateFromStringId(
      const std::string& name) {
    if (name == "put") {
      return CreatePutOperator();
    } else if (name == "uint64add") {
      return CreateUInt64AddOperator();
    } else if (name == "stringappend") {
      return CreateStringAppendOperator();
    } else if (name == "stringappendtest") {
      return CreateStringAppendTESTOperator();
    } else if (name == "bytesxor") {
      return CreateBytesXOROperator();
    } else {
      // Empty or unknown, just return nullptr
      return nullptr;
    }
  }
};

}  // namespace rocksdb
