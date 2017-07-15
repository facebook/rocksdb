//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/get_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/statistics.h"

namespace rocksdb {

namespace {

void appendToReplayLog(std::string* replay_log, ValueType type, Slice value) {
#ifndef ROCKSDB_LITE
  if (replay_log) {
    if (replay_log->empty()) {
      // Optimization: in the common case of only one operation in the
      // log, we allocate the exact amount of space needed.
      replay_log->reserve(1 + VarintLength(value.size()) + value.size());
    }
    replay_log->push_back(type);
    PutLengthPrefixedSlice(replay_log, value);
  }
#endif  // ROCKSDB_LITE
}

}  // namespace

GetContext::GetContext(const Comparator* ucmp,
                       const MergeOperator* merge_operator, Logger* logger,
                       Statistics* statistics, GetState init_state,
                       const Slice& user_key, PinnableSlice* pinnable_val,
                       bool* value_found, MergeContext* merge_context,
                       RangeDelAggregator* _range_del_agg, Env* env,
                       SequenceNumber* seq,
                       PinnedIteratorsManager* _pinned_iters_mgr)
    : ucmp_(ucmp),
      merge_operator_(merge_operator),
      logger_(logger),
      statistics_(statistics),
      state_(init_state),
      user_key_(user_key),
      pinnable_val_(pinnable_val),
      value_found_(value_found),
      merge_context_(merge_context),
      range_del_agg_(_range_del_agg),
      env_(env),
      seq_(seq),
      replay_log_(nullptr),
      pinned_iters_mgr_(_pinned_iters_mgr) {
  if (seq_) {
    *seq_ = kMaxSequenceNumber;
  }
}

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

void GetContext::SaveValue(const Slice& value, SequenceNumber seq) {
  assert(state_ == kNotFound);
  appendToReplayLog(replay_log_, kTypeValue, value);

  state_ = kFound;
  if (LIKELY(pinnable_val_ != nullptr)) {
    pinnable_val_->PinSelf(value);
  }
}

bool GetContext::SaveValue(const ParsedInternalKey& parsed_key,
                           const Slice& value, Cleanable* value_pinner) {
  assert((state_ != kMerge && parsed_key.type != kTypeMerge) ||
         merge_context_ != nullptr);
  if (ucmp_->Equal(parsed_key.user_key, user_key_)) {
    appendToReplayLog(replay_log_, parsed_key.type, value);

    if (seq_ != nullptr) {
      // Set the sequence number if it is uninitialized
      if (*seq_ == kMaxSequenceNumber) {
        *seq_ = parsed_key.sequence;
      }
    }

    auto type = parsed_key.type;
    // Key matches. Process it
    if ((type == kTypeValue || type == kTypeMerge) &&
        range_del_agg_ != nullptr && range_del_agg_->ShouldDelete(parsed_key)) {
      type = kTypeRangeDeletion;
    }
    switch (type) {
      case kTypeValue:
        assert(state_ == kNotFound || state_ == kMerge);
        if (kNotFound == state_) {
          state_ = kFound;
          if (LIKELY(pinnable_val_ != nullptr)) {
            if (LIKELY(value_pinner != nullptr)) {
              // If the backing resources for the value are provided, pin them
              pinnable_val_->PinSlice(value, value_pinner);
            } else {
              // Otherwise copy the value
              pinnable_val_->PinSelf(value);
            }
          }
        } else if (kMerge == state_) {
          assert(merge_operator_ != nullptr);
          state_ = kFound;
          if (LIKELY(pinnable_val_ != nullptr)) {
            Status merge_status = MergeHelper::TimedFullMerge(
                merge_operator_, user_key_, &value,
                merge_context_->GetOperands(), pinnable_val_->GetSelf(),
                logger_, statistics_, env_);
            pinnable_val_->PinSelf();
            if (!merge_status.ok()) {
              state_ = kCorrupt;
            }
          }
        }
        return false;

      case kTypeDeletion:
      case kTypeSingleDeletion:
      case kTypeRangeDeletion:
        // TODO(noetzli): Verify correctness once merge of single-deletes
        // is supported
        assert(state_ == kNotFound || state_ == kMerge);
        if (kNotFound == state_) {
          state_ = kDeleted;
        } else if (kMerge == state_) {
          state_ = kFound;
          if (LIKELY(pinnable_val_ != nullptr)) {
            Status merge_status = MergeHelper::TimedFullMerge(
                merge_operator_, user_key_, nullptr,
                merge_context_->GetOperands(), pinnable_val_->GetSelf(),
                logger_, statistics_, env_);
            pinnable_val_->PinSelf();
            if (!merge_status.ok()) {
              state_ = kCorrupt;
            }
          }
        }
        return false;

      case kTypeMerge:
        assert(state_ == kNotFound || state_ == kMerge);
        state_ = kMerge;
        // value_pinner is not set from plain_table_reader.cc for example.
        if (pinned_iters_mgr() && pinned_iters_mgr()->PinningEnabled() &&
            value_pinner != nullptr) {
          value_pinner->DelegateCleanupsTo(pinned_iters_mgr());
          merge_context_->PushOperand(value, true /*value_pinned*/);
        } else {
          merge_context_->PushOperand(value, false);
        }
        return true;

      default:
        assert(false);
        break;
    }
  }

  // state_ could be Corrupt, merge or notfound
  return false;
}

void replayGetContextLog(const Slice& replay_log, const Slice& user_key,
                         GetContext* get_context) {
#ifndef ROCKSDB_LITE
  Slice s = replay_log;
  while (s.size()) {
    auto type = static_cast<ValueType>(*s.data());
    s.remove_prefix(1);
    Slice value;
    bool ret = GetLengthPrefixedSlice(&s, &value);
    assert(ret);
    (void)ret;

    // Since SequenceNumber is not stored and unknown, we will use
    // kMaxSequenceNumber.
    get_context->SaveValue(
        ParsedInternalKey(user_key, kMaxSequenceNumber, type), value, nullptr);
  }
#else   // ROCKSDB_LITE
  assert(false);
#endif  // ROCKSDB_LITE
}

}  // namespace rocksdb
