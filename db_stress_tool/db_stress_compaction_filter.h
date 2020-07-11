//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/compaction_filter.h"

namespace ROCKSDB_NAMESPACE {

// DbStressCompactionFilter is safe to use with db_stress as it does not perform
// any mutation. It only makes `kRemove` decisions for keys that are already
// non-existent according to the `SharedState`.
class DbStressCompactionFilter : public CompactionFilter {
 public:
  DbStressCompactionFilter(SharedState* state, int cf_id)
      : state_(state), cf_id_(cf_id) {}

  Decision FilterV2(int /*level*/, const Slice& key, ValueType /*value_type*/,
                    const Slice& /*existing_value*/, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    if (state_ == nullptr) {
      return Decision::kKeep;
    }
    if (key.empty() || ('0' <= key[0] && key[0] <= '9')) {
      // It is likely leftover from a test_batches_snapshots run. Below this
      // conditional, the test_batches_snapshots key format is not handled
      // properly. Just keep it to be safe.
      return Decision::kKeep;
    }
    uint64_t key_num = 0;
    bool ok = GetIntVal(key.ToString(), &key_num);
    assert(ok);
    (void)ok;
    MutexLock key_lock(state_->GetMutexForKey(cf_id_, key_num));
    if (!state_->Exists(cf_id_, key_num)) {
      return Decision::kRemove;
    }
    return Decision::kKeep;
  }

  const char* Name() const override { return "DbStressCompactionFilter"; }

 private:
  SharedState* const state_;
  const int cf_id_;
};

class DbStressCompactionFilterFactory : public CompactionFilterFactory {
 public:
  DbStressCompactionFilterFactory() : state_(nullptr) {}

  void SetSharedState(SharedState* state) {
    MutexLock state_mutex_guard(&state_mutex_);
    state_ = state;
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    MutexLock state_mutex_guard(&state_mutex_);
    return std::unique_ptr<CompactionFilter>(
        new DbStressCompactionFilter(state_, context.column_family_id));
  }

  const char* Name() const override {
    return "DbStressCompactionFilterFactory";
  }

 private:
  port::Mutex state_mutex_;
  SharedState* state_;
};

}  // namespace ROCKSDB_NAMESPACE
