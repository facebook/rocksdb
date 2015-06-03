//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <stdio.h>
#include <string>

#include "merge_helper.h"
#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "util/statistics.h"
#include "util/perf_context_imp.h"
#include "util/stop_watch.h"

namespace rocksdb {

// TODO(agiardullo): Clean up merge callsites to use this func
Status MergeHelper::TimedFullMerge(const Slice& key, const Slice* value,
                                   const std::deque<std::string>& operands,
                                   const MergeOperator* merge_operator,
                                   Statistics* statistics, Env* env,
                                   Logger* logger, std::string* result) {
  if (operands.size() == 0) {
    result->assign(value->data(), value->size());
    return Status::OK();
  }

  if (merge_operator == nullptr) {
    return Status::NotSupported("Provide a merge_operator when opening DB");
  }

  // Setup to time the merge
  StopWatchNano timer(env, statistics != nullptr);
  PERF_TIMER_GUARD(merge_operator_time_nanos);

  // Do the merge
  bool success =
      merge_operator->FullMerge(key, value, operands, result, logger);

  RecordTick(statistics, MERGE_OPERATION_TOTAL_TIME,
             env != nullptr ? timer.ElapsedNanos() : 0);

  if (!success) {
    RecordTick(statistics, NUMBER_MERGE_FAILURES);
    return Status::Corruption("Error: Could not perform merge.");
  }

  return Status::OK();
}

// PRE:  iter points to the first merge type entry
// POST: iter points to the first entry beyond the merge process (or the end)
//       keys_, operands_ are updated to reflect the merge result.
//       keys_ stores the list of keys encountered while merging.
//       operands_ stores the list of merge operands encountered while merging.
//       keys_[i] corresponds to operands_[i] for each i.
void MergeHelper::MergeUntil(Iterator* iter, SequenceNumber stop_before,
                             bool at_bottom, Statistics* stats, int* steps,
                             Env* env_) {
  // Get a copy of the internal key, before it's invalidated by iter->Next()
  // Also maintain the list of merge operands seen.
  assert(HasOperator());
  keys_.clear();
  operands_.clear();
  keys_.push_front(iter->key().ToString());
  operands_.push_front(iter->value().ToString());
  assert(user_merge_operator_);

  success_ = false;   // Will become true if we hit Put/Delete or bottom

  // We need to parse the internal key again as the parsed key is
  // backed by the internal key!
  // Assume no internal key corruption as it has been successfully parsed
  // by the caller.
  // Invariant: keys_.back() will not change. Hence, orig_ikey is always valid.
  ParsedInternalKey orig_ikey;
  ParseInternalKey(keys_.back(), &orig_ikey);

  bool hit_the_next_user_key = false;
  std::string merge_result;  // Temporary value for merge results
  if (steps) {
    ++(*steps);
  }
  for (iter->Next(); iter->Valid(); iter->Next()) {
    ParsedInternalKey ikey;
    assert(operands_.size() >= 1);        // Should be invariants!
    assert(keys_.size() == operands_.size());

    if (!ParseInternalKey(iter->key(), &ikey)) {
      // stop at corrupted key
      if (assert_valid_internal_key_) {
        assert(!"corrupted internal key is not expected");
      }
      break;
    }

    if (user_comparator_->Compare(ikey.user_key, orig_ikey.user_key) != 0) {
      // hit a different user key, stop right here
      hit_the_next_user_key = true;
      break;
    }

    if (stop_before && ikey.sequence <= stop_before) {
      // hit an entry that's visible by the previous snapshot, can't touch that
      break;
    }

    // At this point we are guaranteed that we need to process this key.

    if (kTypeDeletion == ikey.type) {
      // hit a delete
      //   => merge nullptr with operands_
      //   => store result in operands_.back() (and update keys_.back())
      //   => change the entry type to kTypeValue for keys_.back()
      // We are done! Return a success if the merge passes.

      Status s = TimedFullMerge(ikey.user_key, nullptr, operands_,
                                user_merge_operator_, stats, env_, logger_,
                                &merge_result);

      // We store the result in keys_.back() and operands_.back()
      // if nothing went wrong (i.e.: no operand corruption on disk)
      if (s.ok()) {
        std::string& original_key =
            keys_.back();  // The original key encountered
        orig_ikey.type = kTypeValue;
        UpdateInternalKey(&original_key[0], original_key.size(),
                          orig_ikey.sequence, orig_ikey.type);
        swap(operands_.back(), merge_result);
      }

      // move iter to the next entry (before doing anything else)
      iter->Next();
      if (steps) {
        ++(*steps);
      }
      return;
    }

    if (kTypeValue == ikey.type) {
      // hit a put
      //   => merge the put value with operands_
      //   => store result in operands_.back() (and update keys_.back())
      //   => change the entry type to kTypeValue for keys_.back()
      // We are done! Success!
      const Slice val = iter->value();
      Status s =
          TimedFullMerge(ikey.user_key, &val, operands_, user_merge_operator_,
                         stats, env_, logger_, &merge_result);

      // We store the result in keys_.back() and operands_.back()
      // if nothing went wrong (i.e.: no operand corruption on disk)
      if (s.ok()) {
        std::string& original_key =
            keys_.back();  // The original key encountered
        orig_ikey.type = kTypeValue;
        UpdateInternalKey(&original_key[0], original_key.size(),
                          orig_ikey.sequence, orig_ikey.type);
        swap(operands_.back(), merge_result);
      }

      // move iter to the next entry
      iter->Next();
      if (steps) {
        ++(*steps);
      }
      return;
    }

    if (kTypeMerge == ikey.type) {
      // hit a merge
      //   => merge the operand into the front of the operands_ list
      //   => use the user's associative merge function to determine how.
      //   => then continue because we haven't yet seen a Put/Delete.
      assert(!operands_.empty()); // Should have at least one element in it

      // keep queuing keys and operands until we either meet a put / delete
      // request or later did a partial merge.
      keys_.push_front(iter->key().ToString());
      operands_.push_front(iter->value().ToString());
      if (steps) {
        ++(*steps);
      }
    }
  }

  // We are sure we have seen this key's entire history if we are at the
  // last level and exhausted all internal keys of this user key.
  // NOTE: !iter->Valid() does not necessarily mean we hit the
  // beginning of a user key, as versions of a user key might be
  // split into multiple files (even files on the same level)
  // and some files might not be included in the compaction/merge.
  //
  // There are also cases where we have seen the root of history of this
  // key without being sure of it. Then, we simply miss the opportunity
  // to combine the keys. Since VersionSet::SetupOtherInputs() always makes
  // sure that all merge-operands on the same level get compacted together,
  // this will simply lead to these merge operands moving to the next level.
  //
  // So, we only perform the following logic (to merge all operands together
  // without a Put/Delete) if we are certain that we have seen the end of key.
  bool surely_seen_the_beginning = hit_the_next_user_key && at_bottom;
  if (surely_seen_the_beginning) {
    // do a final merge with nullptr as the existing value and say
    // bye to the merge type (it's now converted to a Put)
    assert(kTypeMerge == orig_ikey.type);
    assert(operands_.size() >= 1);
    assert(operands_.size() == keys_.size());
    {
      StopWatchNano timer(env_, stats != nullptr);
      PERF_TIMER_GUARD(merge_operator_time_nanos);
      success_ = user_merge_operator_->FullMerge(
          orig_ikey.user_key, nullptr, operands_, &merge_result, logger_);
      RecordTick(stats, MERGE_OPERATION_TOTAL_TIME,
                 env_ != nullptr ? timer.ElapsedNanos() : 0);
    }
    if (success_) {
      std::string& original_key = keys_.back();  // The original key encountered
      orig_ikey.type = kTypeValue;
      UpdateInternalKey(&original_key[0], original_key.size(),
                        orig_ikey.sequence, orig_ikey.type);

      swap(operands_.back(),merge_result);
    } else {
      RecordTick(stats, NUMBER_MERGE_FAILURES);
      // Do nothing if not success_. Leave keys() and operands() as they are.
    }
  } else {
    // We haven't seen the beginning of the key nor a Put/Delete.
    // Attempt to use the user's associative merge function to
    // merge the stacked merge operands into a single operand.

    if (operands_.size() >= 2 &&
        operands_.size() >= min_partial_merge_operands_) {
      bool merge_success = false;
      {
        StopWatchNano timer(env_, stats != nullptr);
        PERF_TIMER_GUARD(merge_operator_time_nanos);
        merge_success = user_merge_operator_->PartialMergeMulti(
            orig_ikey.user_key,
            std::deque<Slice>(operands_.begin(), operands_.end()),
            &merge_result, logger_);
        RecordTick(stats, MERGE_OPERATION_TOTAL_TIME,
                   env_ != nullptr ? timer.ElapsedNanos() : 0);
      }
      if (merge_success) {
        // Merging of operands (associative merge) was successful.
        // Replace operands with the merge result
        operands_.clear();
        operands_.push_front(std::move(merge_result));
        keys_.erase(keys_.begin(), keys_.end() - 1);
      }
    }
  }
}

} // namespace rocksdb
