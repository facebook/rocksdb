//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/merge_helper.h"

#include <stdio.h>
#include <string>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "table/internal_iterator.h"
#include "util/perf_context_imp.h"
#include "util/statistics.h"

namespace rocksdb {

Status MergeHelper::TimedFullMerge(const MergeOperator* merge_operator,
                                   const Slice& key, const Slice* value,
                                   const std::vector<Slice>& operands,
                                   std::string* result, Logger* logger,
                                   Statistics* statistics, Env* env,
                                   Slice* result_operand) {
  assert(merge_operator != nullptr);

  if (operands.size() == 0) {
    assert(value != nullptr && result != nullptr);
    result->assign(value->data(), value->size());
    return Status::OK();
  }

  bool success;
  Slice tmp_result_operand(nullptr, 0);
  const MergeOperator::MergeOperationInput merge_in(key, value, operands,
                                                    logger);
  MergeOperator::MergeOperationOutput merge_out(*result, tmp_result_operand);
  {
    // Setup to time the merge
    StopWatchNano timer(env, statistics != nullptr);
    PERF_TIMER_GUARD(merge_operator_time_nanos);

    // Do the merge
    success = merge_operator->FullMergeV2(merge_in, &merge_out);

    if (tmp_result_operand.data()) {
      // FullMergeV2 result is an existing operand
      if (result_operand != nullptr) {
        *result_operand = tmp_result_operand;
      } else {
        result->assign(tmp_result_operand.data(), tmp_result_operand.size());
      }
    } else if (result_operand) {
      *result_operand = Slice(nullptr, 0);
    }

    RecordTick(statistics, MERGE_OPERATION_TOTAL_TIME,
               statistics ? timer.ElapsedNanos() : 0);
  }

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
Status MergeHelper::MergeUntil(InternalIterator* iter,
                               const SequenceNumber stop_before,
                               const bool at_bottom) {
  // Get a copy of the internal key, before it's invalidated by iter->Next()
  // Also maintain the list of merge operands seen.
  assert(HasOperator());
  keys_.clear();
  merge_context_.Clear();
  assert(user_merge_operator_);
  bool first_key = true;

  // We need to parse the internal key again as the parsed key is
  // backed by the internal key!
  // Assume no internal key corruption as it has been successfully parsed
  // by the caller.
  // original_key_is_iter variable is just caching the information:
  // original_key_is_iter == (iter->key().ToString() == original_key)
  bool original_key_is_iter = true;
  std::string original_key = iter->key().ToString();
  // Important:
  // orig_ikey is backed by original_key if keys_.empty()
  // orig_ikey is backed by keys_.back() if !keys_.empty()
  ParsedInternalKey orig_ikey;
  ParseInternalKey(original_key, &orig_ikey);

  Status s;
  bool hit_the_next_user_key = false;
  for (; iter->Valid(); iter->Next(), original_key_is_iter = false) {
    ParsedInternalKey ikey;
    assert(keys_.size() == merge_context_.GetNumOperands());

    if (!ParseInternalKey(iter->key(), &ikey)) {
      // stop at corrupted key
      if (assert_valid_internal_key_) {
        assert(!"Corrupted internal key not expected.");
        return Status::Corruption("Corrupted internal key not expected.");
      }
      break;
    } else if (first_key) {
      assert(user_comparator_->Equal(ikey.user_key, orig_ikey.user_key));
      first_key = false;
    } else if (!user_comparator_->Equal(ikey.user_key, orig_ikey.user_key)) {
      // hit a different user key, stop right here
      hit_the_next_user_key = true;
      break;
    } else if (stop_before && ikey.sequence <= stop_before) {
      // hit an entry that's visible by the previous snapshot, can't touch that
      break;
    }

    // At this point we are guaranteed that we need to process this key.

    assert(IsValueType(ikey.type));
    if (ikey.type != kTypeMerge) {

      // hit a put/delete/single delete
      //   => merge the put value or a nullptr with operands_
      //   => store result in operands_.back() (and update keys_.back())
      //   => change the entry type to kTypeValue for keys_.back()
      // We are done! Success!

      // If there are no operands, just return the Status::OK(). That will cause
      // the compaction iterator to write out the key we're currently at, which
      // is the put/delete we just encountered.
      if (keys_.empty()) {
        return Status::OK();
      }

      // TODO(noetzli) If the merge operator returns false, we are currently
      // (almost) silently dropping the put/delete. That's probably not what we
      // want.
      const Slice val = iter->value();
      const Slice* val_ptr = (kTypeValue == ikey.type) ? &val : nullptr;
      std::string merge_result;
      s = TimedFullMerge(user_merge_operator_, ikey.user_key, val_ptr,
                         merge_context_.GetOperands(), &merge_result, logger_,
                         stats_, env_);

      // We store the result in keys_.back() and operands_.back()
      // if nothing went wrong (i.e.: no operand corruption on disk)
      if (s.ok()) {
        // The original key encountered
        original_key = std::move(keys_.back());
        orig_ikey.type = kTypeValue;
        UpdateInternalKey(&original_key, orig_ikey.sequence, orig_ikey.type);
        keys_.clear();
        merge_context_.Clear();
        keys_.emplace_front(std::move(original_key));
        merge_context_.PushOperand(merge_result);
      }

      // move iter to the next entry
      iter->Next();
      return s;
    } else {
      // hit a merge
      //   => if there is a compaction filter, apply it.
      //   => merge the operand into the front of the operands_ list
      //      if not filtered
      //   => then continue because we haven't yet seen a Put/Delete.
      //
      // Keep queuing keys and operands until we either meet a put / delete
      // request or later did a partial merge.

      Slice value_slice = iter->value();
      // add an operand to the list if:
      // 1) it's included in one of the snapshots. in that case we *must* write
      // it out, no matter what compaction filter says
      // 2) it's not filtered by a compaction filter
      if (ikey.sequence <= latest_snapshot_ ||
          !FilterMerge(orig_ikey.user_key, value_slice)) {
        if (original_key_is_iter) {
          // this is just an optimization that saves us one memcpy
          keys_.push_front(std::move(original_key));
        } else {
          keys_.push_front(iter->key().ToString());
        }
        if (keys_.size() == 1) {
          // we need to re-anchor the orig_ikey because it was anchored by
          // original_key before
          ParseInternalKey(keys_.back(), &orig_ikey);
        }
        merge_context_.PushOperand(value_slice,
                                   iter->IsValuePinned() /* operand_pinned */);
      }
    }
  }

  if (merge_context_.GetNumOperands() == 0) {
    // we filtered out all the merge operands
    return Status::OK();
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
    assert(merge_context_.GetNumOperands() >= 1);
    assert(merge_context_.GetNumOperands() == keys_.size());
    std::string merge_result;
    s = TimedFullMerge(user_merge_operator_, orig_ikey.user_key, nullptr,
                       merge_context_.GetOperands(), &merge_result, logger_,
                       stats_, env_);
    if (s.ok()) {
      // The original key encountered
      // We are certain that keys_ is not empty here (see assertions couple of
      // lines before).
      original_key = std::move(keys_.back());
      orig_ikey.type = kTypeValue;
      UpdateInternalKey(&original_key, orig_ikey.sequence, orig_ikey.type);
      keys_.clear();
      merge_context_.Clear();
      keys_.emplace_front(std::move(original_key));
      merge_context_.PushOperand(merge_result);
    }
  } else {
    // We haven't seen the beginning of the key nor a Put/Delete.
    // Attempt to use the user's associative merge function to
    // merge the stacked merge operands into a single operand.
    //
    // TODO(noetzli) The docblock of MergeUntil suggests that a successful
    // partial merge returns Status::OK(). Should we change the status code
    // after a successful partial merge?
    s = Status::MergeInProgress();
    if (merge_context_.GetNumOperands() >= 2 &&
        merge_context_.GetNumOperands() >= min_partial_merge_operands_) {
      bool merge_success = false;
      std::string merge_result;
      {
        StopWatchNano timer(env_, stats_ != nullptr);
        PERF_TIMER_GUARD(merge_operator_time_nanos);
        merge_success = user_merge_operator_->PartialMergeMulti(
            orig_ikey.user_key,
            std::deque<Slice>(merge_context_.GetOperands().begin(),
                              merge_context_.GetOperands().end()),
            &merge_result, logger_);
        RecordTick(stats_, MERGE_OPERATION_TOTAL_TIME,
                   stats_ ? timer.ElapsedNanosSafe() : 0);
      }
      if (merge_success) {
        // Merging of operands (associative merge) was successful.
        // Replace operands with the merge result
        merge_context_.Clear();
        merge_context_.PushOperand(merge_result);
        keys_.erase(keys_.begin(), keys_.end() - 1);
      }
    }
  }

  return s;
}

MergeOutputIterator::MergeOutputIterator(const MergeHelper* merge_helper)
    : merge_helper_(merge_helper) {
  it_keys_ = merge_helper_->keys().rend();
  it_values_ = merge_helper_->values().rend();
}

void MergeOutputIterator::SeekToFirst() {
  const auto& keys = merge_helper_->keys();
  const auto& values = merge_helper_->values();
  assert(keys.size() == values.size());
  it_keys_ = keys.rbegin();
  it_values_ = values.rbegin();
}

void MergeOutputIterator::Next() {
  ++it_keys_;
  ++it_values_;
}

bool MergeHelper::FilterMerge(const Slice& user_key, const Slice& value_slice) {
  if (compaction_filter_ == nullptr) {
    return false;
  }
  if (stats_ != nullptr) {
    filter_timer_.Start();
  }
  bool to_delete =
      compaction_filter_->FilterMergeOperand(level_, user_key, value_slice);
  total_filter_time_ += filter_timer_.ElapsedNanosSafe();
  return to_delete;
}

} // namespace rocksdb
