//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#include <string>
#include "db/merge_context.h"
#include "rocksdb/env.h"

namespace rocksdb {
class MergeContext;

class GetContext {
 public:
  enum GetState {
    kNotFound,
    kFound,
    kDeleted,
    kCorrupt,
    kMerge  // saver contains the current merge result (the operands)
  };

  GetContext(const Comparator* ucmp, const MergeOperator* merge_operator,
             Logger* logger, Statistics* statistics, GetState init_state,
             const Slice& user_key, std::string* ret_value, bool* value_found,
             MergeContext* merge_context, Env* env_);

  void MarkKeyMayExist();
  void SaveValue(const Slice& value);
  bool SaveValue(const ParsedInternalKey& parsed_key, const Slice& value);
  GetState State() const { return state_; }

 private:
  const Comparator* ucmp_;
  const MergeOperator* merge_operator_;
  // the merge operations encountered;
  Logger* logger_;
  Statistics* statistics_;

  GetState state_;
  Slice user_key_;
  std::string* value_;
  bool* value_found_;  // Is value set correctly? Used by KeyMayExist
  MergeContext* merge_context_;
  Env* env_;
};

}  // namespace rocksdb
