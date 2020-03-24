//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/merge_helper.h"

#include <string>

#include "db/dbformat.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "port/likely.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "table/format.h"
#include "table/internal_iterator.h"

namespace rocksdb {

MergeHelper::MergeHelper(Env* env, const Comparator* user_comparator,
                         const MergeOperator* user_merge_operator,
                         const CompactionFilter* compaction_filter,
                         Logger* logger, bool assert_valid_internal_key,
                         SequenceNumber latest_snapshot,
                         const SnapshotChecker* snapshot_checker, int level,
                         Statistics* stats,
                         const std::atomic<bool>* shutting_down)
    : env_(env),
      user_comparator_(user_comparator),
      user_merge_operator_(user_merge_operator),
      compaction_filter_(compaction_filter),
      shutting_down_(shutting_down),
      logger_(logger),
      assert_valid_internal_key_(assert_valid_internal_key),
      allow_single_operand_(false),
      latest_snapshot_(latest_snapshot),
      snapshot_checker_(snapshot_checker),
      level_(level),
      keys_(),
      filter_timer_(env_),
      total_filter_time_(0U),
      stats_(stats) {
  assert(user_comparator_ != nullptr);
  if (user_merge_operator_) {
    allow_single_operand_ = user_merge_operator_->AllowSingleOperand();
  }
}

Status MergeHelper::TimedFullMerge(const MergeOperator* merge_operator,
                                   const Slice& key, const Slice* value,
                                   const std::vector<Slice>& operands,
                                   std::string* result, Logger* logger,
                                   Statistics* statistics, Env* env,
                                   Slice* result_operand,
                                   bool update_num_ops_stats) {
  assert(merge_operator != nullptr);

  if (operands.size() == 0) {
    assert(value != nullptr && result != nullptr);
    result->assign(value->data(), value->size());
    return Status::OK();
  }

  if (update_num_ops_stats) {
    RecordInHistogram(statistics, READ_NUM_MERGE_OPERANDS,
                      static_cast<uint64_t>(operands.size()));
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
//
// TODO: Avoid the snapshot stripe map lookup in CompactionRangeDelAggregator
// and just pass the StripeRep corresponding to the stripe being merged.
Status MergeHelper::MergeUntil(InternalIterator* iter,
                               CompactionRangeDelAggregator* range_del_agg,
                               const SequenceNumber stop_before,
                               const bool at_bottom) {
  // Get a copy of the internal key, before it's invalidated by iter->Next()
  // Also maintain the list of merge operands seen.
  assert(HasOperator());
  // define workareas for resolving merge operands
  // the VLog for this CF
  std::shared_ptr<VLog> vlog = iter->GetVlogForIteratorCF();
  // We resolve the values for the merge in different strings so that they can
  // be pinned for the merge mill. The storage area is created at the
  // compaction-iterator level, because a merge routine is allowed
  // to return one of its operands; thus we might end up returning the buffer
  // read here to the caller of this routine, and that would fail if the buffers
  // were in the scope of this routine. We do assume that the caller has copied
  // any value returned here to a safe area before they call here again, so we
  // release any buffers read for the previous operation when we start
  // processing a merge.
  iter->resolved_indirect_values.clear();  // where the resolved values go
  keys_.clear();
  merge_context_.Clear();
  has_compaction_filter_skip_until_ = false;
  assert(user_merge_operator_);
  bool first_key = true;

  // We need to parse the internal key again as the parsed key is
  // backed by the internal key!
  // Assume no internal key corruption as it has been successfully parsed
  // by the caller.
  // original_key_is_iter variable is just caching the information:
  // original_key_is_iter == (iter->key().ToString() == original_key)
  bool original_key_is_iter = true;
  Slice origkeyslice = iter->key();  // set the slice form of the original key
  // Important:
  // orig_ikey is backed by original_key if keys_.empty()
  // orig_ikey is backed by keys_.back() if !keys_.empty()
  ParsedInternalKey orig_ikey;
#ifdef ROCKSDB_UBSAN_RUN
  orig_ikey.type=kMaxValue; //Required to suppress maybe-uninitialized warning
#endif
  ParseInternalKey(origkeyslice, &orig_ikey);
  // If the type was changed when the value was resolved, replicate that change
  // here.  This is a pitiable kludge.  It would be better for the user's merge
  // to be required to set the type properly, but the tests don't do that, never
  // mind actual users.  We require that the merge fiter return a direct type
  // (which may be converted to indirect by the compaction job)
  if(IsTypeIndirect(orig_ikey.type)){
    // the types we show to the merge filter are always direct
    orig_ikey.type = orig_ikey.type==kTypeIndirectMerge ? kTypeMerge : kTypeValue;
  }
  // create string rep of (possibly updated) key
  std::string original_key = *InternalKey(orig_ikey).rep();
  bool succ = ParseInternalKey(original_key, &orig_ikey);
  assert(succ);
  if (!succ) {
    return Status::Corruption("Cannot parse key in MergeUntil");
  }

  Status s;
  bool hit_the_next_user_key = false;
  for (; iter->Valid(); iter->Next(), original_key_is_iter = false) {
    if (IsShuttingDown()) {
      return Status::ShutdownInProgress();
    }

    Slice ikeyslice = iter->key();  // set the slice form of the next key
    std::string ikeystring;  // place to save the modified key, if we modify it

    ParsedInternalKey ikey;
    assert(keys_.size() == merge_context_.GetNumOperands());

    if (!ParseInternalKey(ikeyslice, &ikey)) {
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
    } else if (stop_before > 0 && ikey.sequence <= stop_before &&
               LIKELY(snapshot_checker_ == nullptr ||
                      snapshot_checker_->CheckInSnapshot(ikey.sequence,
                                                         stop_before) !=
                          SnapshotCheckerResult::kNotInSnapshot)) {
      // hit an entry that's possibly visible by the previous snapshot, can't
      // touch that
      break;
    }

    // At this point we are guaranteed that we need to process this key.

    assert(IsTypeMemorSST(ikey.type));
    Slice val = (Slice) iter->value();
    if(IsTypeIndirect(ikey.type)){
      // The key specifies an indirect type.  Resolve it
      // create a new string to hold the resolved value.  We keep all the values
      // for a single result in separate strings, so that they are all available
      // for merge.  The string is copied to the persistent name
      // resolved_indirect_values before it is filled.  resolved_indirect_values
      // is defined at the compaction-iterator level. The vector holds pointers
      // to the strings so that resizing the vector doesn't invalidate them
      // add a new empty string for upcoming read
      iter->resolved_indirect_values.push_back(std::make_shared<std::string>());
      // resolve the value in the new string.
      // turn the reference into a value, in the string
      vlog->VLogGet(val,*iter->resolved_indirect_values.back());
      // create a slice for the resolved value
      val = Slice(*iter->resolved_indirect_values.back());
      // the types we give to the merge filter are always direct, for backward compatibility
      ikey.type = ikey.type==kTypeIndirectMerge ? kTypeMerge : kTypeValue;
      // save string form where it won't be freed till we've used it
      ikeystring = InternalKey(ikey).Encode().ToString();
      // keep the Slice form, which is what we refer to below, current
      ikeyslice = ikeystring;
    }
    if (!IsTypeMerge(ikey.type)) {

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
      // want. Also if we're in compaction and it's a put, it would be nice to
      // run compaction filter on it.
      const Slice* val_ptr = (IsTypeValueNonBlob(ikey.type)) ? &val : nullptr;
      /*
      const Slice val = iter->value();
      const Slice* val_ptr;
      if (kTypeValue == ikey.type &&
          (range_del_agg == nullptr ||
           !range_del_agg->ShouldDelete(
               ikey, RangeDelPositioningMode::kForwardTraversal))) {
        val_ptr = &val;
      } else {
        val_ptr = nullptr;
      }
      */
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
      //   => check for range tombstones covering the operand
      //   => merge the operand into the front of the operands_ list
      //      if not filtered
      //   => then continue because we haven't yet seen a Put/Delete.
      //
      // Keep queuing keys and operands until we either meet a put / delete
      // request or later did a partial merge.

      // add an operand to the list if:
      // 1) it's included in one of the snapshots. in that case we *must* write
      // it out, no matter what compaction filter says
      // 2) it's not filtered by a compaction filter
      CompactionFilter::Decision filter =
          ikey.sequence <= latest_snapshot_
              ? CompactionFilter::Decision::kKeep
              : FilterMerge(orig_ikey.user_key, val);
      if (filter != CompactionFilter::Decision::kRemoveAndSkipUntil &&
          range_del_agg != nullptr &&
          range_del_agg->ShouldDelete(
              ikeyslice, RangeDelPositioningMode::kForwardTraversal)) {
        filter = CompactionFilter::Decision::kRemove;
      }
      if (filter == CompactionFilter::Decision::kKeep ||
          filter == CompactionFilter::Decision::kChangeValue) {
        if (original_key_is_iter) {
          // this is just an optimization that saves us one memcpy
          keys_.push_front(std::move(original_key));
        } else {
          keys_.push_front(ikeyslice.ToString());
        }
        if (keys_.size() == 1) {
          // we need to re-anchor the orig_ikey because it was anchored by
          // original_key before
          ParseInternalKey(keys_.back(), &orig_ikey);
        }
        if (filter == CompactionFilter::Decision::kKeep) {
          merge_context_.PushOperand(
              val, iter->IsValuePinned() /* operand_pinned */);
        } else {  // kChangeValue
          // Compaction filter asked us to change the operand from val
          // to compaction_filter_value_.
          merge_context_.PushOperand(compaction_filter_value_, false);
        }
      } else if (filter == CompactionFilter::Decision::kRemoveAndSkipUntil) {
        // Compaction filter asked us to remove this key altogether
        // (not just this operand), along with some keys following it.
        keys_.clear();
        merge_context_.Clear();
        has_compaction_filter_skip_until_ = true;
        return Status::OK();
      }
    }
  }

  if (merge_context_.GetNumOperands() == 0) {
    // we filtered out all the merge operands
    return Status::OK();
  }

  // We are sure we have seen this key's entire history if:
  // at_bottom == true (this does not necessarily mean it is the bottommost
  // layer, but rather that we are confident the key does not appear on any of
  // the lower layers, at_bottom == false doesn't mean it does appear, just
  // that we can't be sure, see Compaction::IsBottommostLevel for details)
  // AND
  // we have either encountered another key or end of key history on this
  // layer.
  //
  // When these conditions are true we are able to merge all the keys
  // using full merge.
  //
  // For these cases we are not sure about, we simply miss the opportunity
  // to combine the keys. Since VersionSet::SetupOtherInputs() always makes
  // sure that all merge-operands on the same level get compacted together,
  // this will simply lead to these merge operands moving to the next level.
  bool surely_seen_the_beginning =
      (hit_the_next_user_key || !iter->Valid()) && at_bottom;
  if (surely_seen_the_beginning) {
    // do a final merge with nullptr as the existing value and say
    // bye to the merge type (it's now converted to a Put)
    assert(IsTypeMerge(orig_ikey.type));
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
    s = Status::MergeInProgress();
    if (merge_context_.GetNumOperands() >= 2 ||
        (allow_single_operand_ && merge_context_.GetNumOperands() == 1)) {
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
    // ToDo: return unexecuted merge in its original form
    // If there is only one merge, return it in its original form (indirect/direct)
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

CompactionFilter::Decision MergeHelper::FilterMerge(const Slice& user_key,
                                                    const Slice& value_slice) {
  if (compaction_filter_ == nullptr) {
    return CompactionFilter::Decision::kKeep;
  }
  if (stats_ != nullptr && ShouldReportDetailedTime(env_, stats_)) {
    filter_timer_.Start();
  }
  compaction_filter_value_.clear();
  compaction_filter_skip_until_.Clear();
  auto ret = compaction_filter_->FilterV2(
      level_, user_key, CompactionFilter::ValueType::kMergeOperand, value_slice,
      &compaction_filter_value_, compaction_filter_skip_until_.rep());
  if (ret == CompactionFilter::Decision::kRemoveAndSkipUntil) {
    if (user_comparator_->Compare(*compaction_filter_skip_until_.rep(),
                                  user_key) <= 0) {
      // Invalid skip_until returned from compaction filter.
      // Keep the key as per FilterV2 documentation.
      ret = CompactionFilter::Decision::kKeep;
    } else {
      compaction_filter_skip_until_.ConvertFromUserKey(kMaxSequenceNumber,
                                                       kValueTypeForSeek);
    }
  }
  total_filter_time_ += filter_timer_.ElapsedNanosSafe();
  return ret;
}

} // namespace rocksdb
