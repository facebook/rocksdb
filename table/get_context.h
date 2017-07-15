//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>
#include "db/merge_context.h"
#include "db/range_del_aggregator.h"
#include "rocksdb/env.h"
#include "rocksdb/types.h"
#include "table/block.h"

namespace rocksdb {
class MergeContext;
class PinnedIteratorsManager;

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
             const Slice& user_key, PinnableSlice* value, bool* value_found,
             MergeContext* merge_context, RangeDelAggregator* range_del_agg,
             Env* env, SequenceNumber* seq = nullptr,
             PinnedIteratorsManager* _pinned_iters_mgr = nullptr);

  void MarkKeyMayExist();

  // Records this key, value, and any meta-data (such as sequence number and
  // state) into this GetContext.
  //
  // Returns True if more keys need to be read (due to merges) or
  //         False if the complete value has been found.
  bool SaveValue(const ParsedInternalKey& parsed_key, const Slice& value,
                 Cleanable* value_pinner = nullptr);

  // Simplified version of the previous function. Should only be used when we
  // know that the operation is a Put.
  void SaveValue(const Slice& value, SequenceNumber seq);

  GetState State() const { return state_; }

  RangeDelAggregator* range_del_agg() { return range_del_agg_; }

  PinnedIteratorsManager* pinned_iters_mgr() { return pinned_iters_mgr_; }

  // If a non-null string is passed, all the SaveValue calls will be
  // logged into the string. The operations can then be replayed on
  // another GetContext with replayGetContextLog.
  void SetReplayLog(std::string* replay_log) { replay_log_ = replay_log; }

  // Do we need to fetch the SequenceNumber for this key?
  bool NeedToReadSequence() const { return (seq_ != nullptr); }

 private:
  const Comparator* ucmp_;
  const MergeOperator* merge_operator_;
  // the merge operations encountered;
  Logger* logger_;
  Statistics* statistics_;

  GetState state_;
  Slice user_key_;
  PinnableSlice* pinnable_val_;
  bool* value_found_;  // Is value set correctly? Used by KeyMayExist
  MergeContext* merge_context_;
  RangeDelAggregator* range_del_agg_;
  Env* env_;
  // If a key is found, seq_ will be set to the SequenceNumber of most recent
  // write to the key or kMaxSequenceNumber if unknown
  SequenceNumber* seq_;
  std::string* replay_log_;
  // Used to temporarily pin blocks when state_ == GetContext::kMerge
  PinnedIteratorsManager* pinned_iters_mgr_;
};

void replayGetContextLog(const Slice& replay_log, const Slice& user_key,
                         GetContext* get_context);

}  // namespace rocksdb
