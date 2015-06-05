//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "table/get_context.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/statistics.h"
#include "util/perf_context_imp.h"
#include "util/statistics.h"

namespace rocksdb {

GetContext::GetContext(const Comparator* ucmp,
                       const MergeOperator* merge_operator, Logger* logger,
                       Statistics* statistics, GetState init_state,
                       const Slice& user_key, std::string* ret_value,
                       bool* value_found, MergeContext* merge_context, Env* env)
    : ucmp_(ucmp),
      merge_operator_(merge_operator),
      logger_(logger),
      statistics_(statistics),
      state_(init_state),
      user_key_(user_key),
      value_(ret_value),
      value_found_(value_found),
      merge_context_(merge_context),
      env_(env) {}

// Called from TableCache::Get and Table::Get when file/block in which
// key may exist are not there in TableCache/BlockCache respectively. In this
// case we can't guarantee that key does not exist and are not permitted to do
// IO to be certain.Set the status=kFound and value_found=false to let the
// caller know that key may exist but is not there in memory
void GetContext::MarkKeyMayExist() {
  state_ = kFound;
  if (value_found_ != nullptr) {
    *value_found_ = false;
  }
}

void GetContext::SaveValue(const Slice& value) {
  state_ = kFound;
  value_->assign(value.data(), value.size());
}

bool GetContext::SaveValue(const ParsedInternalKey& parsed_key,
                           const Slice& value) {
  assert((state_ != kMerge && parsed_key.type != kTypeMerge) ||
         merge_context_ != nullptr);
  if (ucmp_->Compare(parsed_key.user_key, user_key_) == 0) {
    // Key matches. Process it
    switch (parsed_key.type) {
      case kTypeValue:
        assert(state_ == kNotFound || state_ == kMerge);
        if (kNotFound == state_) {
          state_ = kFound;
          value_->assign(value.data(), value.size());
        } else if (kMerge == state_) {
          assert(merge_operator_ != nullptr);
          state_ = kFound;
          bool merge_success = false;
          {
            StopWatchNano timer(env_, statistics_ != nullptr);
            PERF_TIMER_GUARD(merge_operator_time_nanos);
            merge_success = merge_operator_->FullMerge(
                user_key_, &value, merge_context_->GetOperands(), value_,
                logger_);
            RecordTick(statistics_, MERGE_OPERATION_TOTAL_TIME,
                       env_ != nullptr ? timer.ElapsedNanos() : 0);
          }
          if (!merge_success) {
            RecordTick(statistics_, NUMBER_MERGE_FAILURES);
            state_ = kCorrupt;
          }
        }
        return false;

      case kTypeDeletion:
        assert(state_ == kNotFound || state_ == kMerge);
        if (kNotFound == state_) {
          state_ = kDeleted;
        } else if (kMerge == state_) {
          state_ = kFound;
          bool merge_success = false;
          {
            StopWatchNano timer(env_, statistics_ != nullptr);
            PERF_TIMER_GUARD(merge_operator_time_nanos);
            merge_success = merge_operator_->FullMerge(
                user_key_, nullptr, merge_context_->GetOperands(), value_,
                logger_);
            RecordTick(statistics_, MERGE_OPERATION_TOTAL_TIME,
                       env_ != nullptr ? timer.ElapsedNanos() : 0);
          }
          if (!merge_success) {
            RecordTick(statistics_, NUMBER_MERGE_FAILURES);
            state_ = kCorrupt;
          }
        }
        return false;

      case kTypeMerge:
        assert(state_ == kNotFound || state_ == kMerge);
        state_ = kMerge;
        merge_context_->PushOperand(value);
        return true;

      default:
        assert(false);
        break;
    }
  }

  // state_ could be Corrupt, merge or notfound
  return false;
}

}  // namespace rocksdb
