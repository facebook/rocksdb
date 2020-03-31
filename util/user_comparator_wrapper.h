// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "monitoring/perf_context_imp.h"
#include "rocksdb/comparator.h"

namespace rocksdb {

// Wrapper of user comparator, with auto increment to
// perf_context.user_key_comparison_count.
class UserComparatorWrapper final : public Comparator {
 public:
  explicit UserComparatorWrapper(const Comparator* const user_cmp)
      : user_comparator_(user_cmp) {}
  
  ~UserComparatorWrapper() = default;

  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const Slice& a, const Slice& b) const override {
    PERF_COUNTER_ADD(user_key_comparison_count, 1);
    return user_comparator_->Compare(a, b);
  }

  bool Equal(const Slice& a, const Slice& b) const override {
    PERF_COUNTER_ADD(user_key_comparison_count, 1);
    return user_comparator_->Equal(a, b);
  }

  const char* Name() const override { return user_comparator_->Name(); }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    return user_comparator_->FindShortestSeparator(start, limit);
  }

  void FindShortSuccessor(std::string* key) const override {
    return user_comparator_->FindShortSuccessor(key);
  }

  const Comparator* GetRootComparator() const override {
    return user_comparator_->GetRootComparator();
  }

  bool IsSameLengthImmediateSuccessor(const Slice& s,
                                      const Slice& t) const override {
    return user_comparator_->IsSameLengthImmediateSuccessor(s, t);
  }

  bool CanKeysWithDifferentByteContentsBeEqual() const override {
    return user_comparator_->CanKeysWithDifferentByteContentsBeEqual();
  }

 private:
  const Comparator* user_comparator_;
};

}  // namespace rocksdb
