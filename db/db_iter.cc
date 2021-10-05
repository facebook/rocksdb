//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include <iostream>
#include <limits>
#include <string>

#include "db/dbformat.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "memory/arena.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/system_clock.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "trace_replay/trace_replay.h"
#include "util/mutexlock.h"
#include "util/string_util.h"
#include "util/user_comparator_wrapper.h"

namespace ROCKSDB_NAMESPACE {

DBIter::DBIter(Env* _env, const ReadOptions& read_options,
               const ImmutableOptions& ioptions,
               const MutableCFOptions& mutable_cf_options,
               const Comparator* cmp, InternalIterator* iter,
               const Version* version, SequenceNumber s, bool arena_mode,
               uint64_t max_sequential_skip_in_iterations,
               ReadCallback* read_callback, DBImpl* db_impl,
               ColumnFamilyData* cfd, bool expose_blob_index)
    : prefix_extractor_(mutable_cf_options.prefix_extractor.get()),
      env_(_env),
      clock_(ioptions.clock),
      logger_(ioptions.logger),
      user_comparator_(cmp),
      merge_operator_(ioptions.merge_operator.get()),
      iter_(iter),
      version_(version),
      read_callback_(read_callback),
      sequence_(s),
      statistics_(ioptions.stats),
      max_skip_(max_sequential_skip_in_iterations),
      max_skippable_internal_keys_(read_options.max_skippable_internal_keys),
      num_internal_keys_skipped_(0),
      iterate_lower_bound_(read_options.iterate_lower_bound),
      iterate_upper_bound_(read_options.iterate_upper_bound),
      direction_(kForward),
      valid_(false),
      current_entry_is_merged_(false),
      is_key_seqnum_zero_(false),
      prefix_same_as_start_(mutable_cf_options.prefix_extractor
                                ? read_options.prefix_same_as_start
                                : false),
      pin_thru_lifetime_(read_options.pin_data),
      expect_total_order_inner_iter_(prefix_extractor_ == nullptr ||
                                     read_options.total_order_seek ||
                                     read_options.auto_prefix_mode),
      read_tier_(read_options.read_tier),
      verify_checksums_(read_options.verify_checksums),
      expose_blob_index_(expose_blob_index),
      is_blob_(false),
      arena_mode_(arena_mode),
      range_del_agg_(&ioptions.internal_comparator, s),
      db_impl_(db_impl),
      cfd_(cfd),
      start_seqnum_(read_options.iter_start_seqnum),
      timestamp_ub_(read_options.timestamp),
      timestamp_lb_(read_options.iter_start_ts),
      timestamp_size_(timestamp_ub_ ? timestamp_ub_->size() : 0) {
  RecordTick(statistics_, NO_ITERATOR_CREATED);
  if (pin_thru_lifetime_) {
    pinned_iters_mgr_.StartPinning();
  }
  if (iter_.iter()) {
    iter_.iter()->SetPinnedItersMgr(&pinned_iters_mgr_);
  }
  assert(timestamp_size_ == user_comparator_.timestamp_size());
}

Status DBIter::GetProperty(std::string prop_name, std::string* prop) {
  if (prop == nullptr) {
    return Status::InvalidArgument("prop is nullptr");
  }
  if (prop_name == "rocksdb.iterator.super-version-number") {
    // First try to pass the value returned from inner iterator.
    return iter_.iter()->GetProperty(prop_name, prop);
  } else if (prop_name == "rocksdb.iterator.is-key-pinned") {
    if (valid_) {
      *prop = (pin_thru_lifetime_ && saved_key_.IsKeyPinned()) ? "1" : "0";
    } else {
      *prop = "Iterator is not valid.";
    }
    return Status::OK();
  } else if (prop_name == "rocksdb.iterator.internal-key") {
    *prop = saved_key_.GetUserKey().ToString();
    return Status::OK();
  }
  return Status::InvalidArgument("Unidentified property.");
}

bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Status s = ParseInternalKey(iter_.key(), ikey, false /* log_err_key */);
  if (!s.ok()) {
    status_ = Status::Corruption("In DBIter: ", s.getState());
    valid_ = false;
    ROCKS_LOG_ERROR(logger_, "In DBIter: %s", status_.getState());
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);
  assert(status_.ok());

  PERF_CPU_TIMER_GUARD(iter_next_cpu_nanos, clock_);
  // Release temporarily pinned blocks from last operation
  ReleaseTempPinnedData();
  local_stats_.skip_count_ += num_internal_keys_skipped_;
  local_stats_.skip_count_--;
  num_internal_keys_skipped_ = 0;
  bool ok = true;
  if (direction_ == kReverse) {
    is_key_seqnum_zero_ = false;
    if (!ReverseToForward()) {
      ok = false;
    }
  } else if (!current_entry_is_merged_) {
    // If the current value is not a merge, the iter position is the
    // current key, which is already returned. We can safely issue a
    // Next() without checking the current key.
    // If the current key is a merge, very likely iter already points
    // to the next internal position.
    assert(iter_.Valid());
    iter_.Next();
    PERF_COUNTER_ADD(internal_key_skipped_count, 1);
  }

  local_stats_.next_count_++;
  if (ok && iter_.Valid()) {
    if (prefix_same_as_start_) {
      assert(prefix_extractor_ != nullptr);
      const Slice prefix = prefix_.GetUserKey();
      FindNextUserEntry(true /* skipping the current user key */, &prefix);
    } else {
      FindNextUserEntry(true /* skipping the current user key */, nullptr);
    }
  } else {
    is_key_seqnum_zero_ = false;
    valid_ = false;
  }
  if (statistics_ != nullptr && valid_) {
    local_stats_.next_found_count_++;
    local_stats_.bytes_read_ += (key().size() + value().size());
  }
}

bool DBIter::SetBlobValueIfNeeded(const Slice& user_key,
                                  const Slice& blob_index) {
  assert(!is_blob_);

  if (expose_blob_index_) {  // Stacked BlobDB implementation
    is_blob_ = true;
    return true;
  }

  if (!version_) {
    status_ = Status::Corruption("Encountered unexpected blob index.");
    valid_ = false;
    return false;
  }

  // TODO: consider moving ReadOptions from ArenaWrappedDBIter to DBIter to
  // avoid having to copy options back and forth.
  ReadOptions read_options;
  read_options.read_tier = read_tier_;
  read_options.verify_checksums = verify_checksums_;

  constexpr uint64_t* bytes_read = nullptr;

  const Status s = version_->GetBlob(read_options, user_key, blob_index,
                                     &blob_value_, bytes_read);

  if (!s.ok()) {
    status_ = s;
    valid_ = false;
    return false;
  }

  is_blob_ = true;
  return true;
}

// PRE: saved_key_ has the current user key if skipping_saved_key
// POST: saved_key_ should have the next user key if valid_,
//       if the current entry is a result of merge
//           current_entry_is_merged_ => true
//           saved_value_             => the merged value
//
// NOTE: In between, saved_key_ can point to a user key that has
//       a delete marker or a sequence number higher than sequence_
//       saved_key_ MUST have a proper user_key before calling this function
//
// The prefix parameter, if not null, indicates that we need to iterate
// within the prefix, and the iterator needs to be made invalid, if no
// more entry for the prefix can be found.
bool DBIter::FindNextUserEntry(bool skipping_saved_key, const Slice* prefix) {
  PERF_TIMER_GUARD(find_next_user_entry_time);
  return FindNextUserEntryInternal(skipping_saved_key, prefix);
}

// Actual implementation of DBIter::FindNextUserEntry()
bool DBIter::FindNextUserEntryInternal(bool skipping_saved_key,
                                       const Slice* prefix) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_.Valid());
  assert(status_.ok());
  assert(direction_ == kForward);
  current_entry_is_merged_ = false;

  // How many times in a row we have skipped an entry with user key less than
  // or equal to saved_key_. We could skip these entries either because
  // sequence numbers were too high or because skipping_saved_key = true.
  // What saved_key_ contains throughout this method:
  //  - if skipping_saved_key : saved_key_ contains the key that we need
  //                            to skip, and we haven't seen any keys greater
  //                            than that,
  //  - if num_skipped > 0    : saved_key_ contains the key that we have skipped
  //                            num_skipped times, and we haven't seen any keys
  //                            greater than that,
  //  - none of the above     : saved_key_ can contain anything, it doesn't
  //                            matter.
  uint64_t num_skipped = 0;
  // For write unprepared, the target sequence number in reseek could be larger
  // than the snapshot, and thus needs to be skipped again. This could result in
  // an infinite loop of reseeks. To avoid that, we limit the number of reseeks
  // to one.
  bool reseek_done = false;

  is_blob_ = false;

  do {
    // Will update is_key_seqnum_zero_ as soon as we parsed the current key
    // but we need to save the previous value to be used in the loop.
    bool is_prev_key_seqnum_zero = is_key_seqnum_zero_;
    if (!ParseKey(&ikey_)) {
      is_key_seqnum_zero_ = false;
      return false;
    }
    Slice user_key_without_ts =
        StripTimestampFromUserKey(ikey_.user_key, timestamp_size_);

    is_key_seqnum_zero_ = (ikey_.sequence == 0);

    assert(iterate_upper_bound_ == nullptr ||
           iter_.UpperBoundCheckResult() != IterBoundCheck::kInbound ||
           user_comparator_.CompareWithoutTimestamp(
               user_key_without_ts, /*a_has_ts=*/false, *iterate_upper_bound_,
               /*b_has_ts=*/false) < 0);
    if (iterate_upper_bound_ != nullptr &&
        iter_.UpperBoundCheckResult() != IterBoundCheck::kInbound &&
        user_comparator_.CompareWithoutTimestamp(
            user_key_without_ts, /*a_has_ts=*/false, *iterate_upper_bound_,
            /*b_has_ts=*/false) >= 0) {
      break;
    }

    assert(prefix == nullptr || prefix_extractor_ != nullptr);
    if (prefix != nullptr &&
        prefix_extractor_->Transform(user_key_without_ts).compare(*prefix) !=
            0) {
      assert(prefix_same_as_start_);
      break;
    }

    if (TooManyInternalKeysSkipped()) {
      return false;
    }

    assert(ikey_.user_key.size() >= timestamp_size_);
    Slice ts = timestamp_size_ > 0 ? ExtractTimestampFromUserKey(
                                         ikey_.user_key, timestamp_size_)
                                   : Slice();
    bool more_recent = false;
    if (IsVisible(ikey_.sequence, ts, &more_recent)) {
      // If the previous entry is of seqnum 0, the current entry will not
      // possibly be skipped. This condition can potentially be relaxed to
      // prev_key.seq <= ikey_.sequence. We are cautious because it will be more
      // prone to bugs causing the same user key with the same sequence number.
      // Note that with current timestamp implementation, the same user key can
      // have different timestamps and zero sequence number on the bottommost
      // level. This may change in the future.
      if ((!is_prev_key_seqnum_zero || timestamp_size_ > 0) &&
          skipping_saved_key &&
          CompareKeyForSkip(ikey_.user_key, saved_key_.GetUserKey()) <= 0) {
        num_skipped++;  // skip this entry
        PERF_COUNTER_ADD(internal_key_skipped_count, 1);
      } else {
        assert(!skipping_saved_key ||
               CompareKeyForSkip(ikey_.user_key, saved_key_.GetUserKey()) > 0);
        if (!iter_.PrepareValue()) {
          assert(!iter_.status().ok());
          valid_ = false;
          return false;
        }
        num_skipped = 0;
        reseek_done = false;
        switch (ikey_.type) {
          case kTypeDeletion:
          case kTypeDeletionWithTimestamp:
          case kTypeSingleDeletion:
            // Arrange to skip all upcoming entries for this key since
            // they are hidden by this deletion.
            // if iterartor specified start_seqnum we
            // 1) return internal key, including the type
            // 2) return ikey only if ikey.seqnum >= start_seqnum_
            // note that if deletion seqnum is < start_seqnum_ we
            // just skip it like in normal iterator.
            if (start_seqnum_ > 0) {
              if (ikey_.sequence >= start_seqnum_) {
                saved_key_.SetInternalKey(ikey_);
                valid_ = true;
                return true;
              } else {
                saved_key_.SetUserKey(
                    ikey_.user_key,
                    !pin_thru_lifetime_ ||
                        !iter_.iter()->IsKeyPinned() /* copy */);
                skipping_saved_key = true;
                PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
              }
            } else if (timestamp_lb_) {
              saved_key_.SetInternalKey(ikey_);
              valid_ = true;
              return true;
            } else {
              saved_key_.SetUserKey(
                  ikey_.user_key, !pin_thru_lifetime_ ||
                                      !iter_.iter()->IsKeyPinned() /* copy */);
              skipping_saved_key = true;
              PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
            }
            break;
          case kTypeValue:
          case kTypeBlobIndex:
            if (start_seqnum_ > 0) {
              if (ikey_.sequence >= start_seqnum_) {
                saved_key_.SetInternalKey(ikey_);

                if (ikey_.type == kTypeBlobIndex) {
                  if (!SetBlobValueIfNeeded(ikey_.user_key, iter_.value())) {
                    return false;
                  }
                }

                valid_ = true;
                return true;
              } else {
                // this key and all previous versions shouldn't be included,
                // skipping_saved_key
                saved_key_.SetUserKey(
                    ikey_.user_key,
                    !pin_thru_lifetime_ ||
                        !iter_.iter()->IsKeyPinned() /* copy */);
                skipping_saved_key = true;
              }
            } else if (timestamp_lb_) {
              saved_key_.SetInternalKey(ikey_);

              if (ikey_.type == kTypeBlobIndex) {
                if (!SetBlobValueIfNeeded(ikey_.user_key, iter_.value())) {
                  return false;
                }
              }

              valid_ = true;
              return true;
            } else {
              saved_key_.SetUserKey(
                  ikey_.user_key, !pin_thru_lifetime_ ||
                                      !iter_.iter()->IsKeyPinned() /* copy */);
              if (range_del_agg_.ShouldDelete(
                      ikey_, RangeDelPositioningMode::kForwardTraversal)) {
                // Arrange to skip all upcoming entries for this key since
                // they are hidden by this deletion.
                skipping_saved_key = true;
                num_skipped = 0;
                reseek_done = false;
                PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
              } else {
                if (ikey_.type == kTypeBlobIndex) {
                  if (!SetBlobValueIfNeeded(ikey_.user_key, iter_.value())) {
                    return false;
                  }
                }

                valid_ = true;
                return true;
              }
            }
            break;
          case kTypeMerge:
            saved_key_.SetUserKey(
                ikey_.user_key,
                !pin_thru_lifetime_ || !iter_.iter()->IsKeyPinned() /* copy */);
            if (range_del_agg_.ShouldDelete(
                    ikey_, RangeDelPositioningMode::kForwardTraversal)) {
              // Arrange to skip all upcoming entries for this key since
              // they are hidden by this deletion.
              skipping_saved_key = true;
              num_skipped = 0;
              reseek_done = false;
              PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
            } else {
              // By now, we are sure the current ikey is going to yield a
              // value
              current_entry_is_merged_ = true;
              valid_ = true;
              return MergeValuesNewToOld();  // Go to a different state machine
            }
            break;
          default:
            valid_ = false;
            status_ = Status::Corruption(
                "Unknown value type: " +
                std::to_string(static_cast<unsigned int>(ikey_.type)));
            return false;
        }
      }
    } else {
      if (more_recent) {
        PERF_COUNTER_ADD(internal_recent_skipped_count, 1);
      }

      // This key was inserted after our snapshot was taken or skipped by
      // timestamp range. If this happens too many times in a row for the same
      // user key, we want to seek to the target sequence number.
      int cmp = user_comparator_.CompareWithoutTimestamp(
          ikey_.user_key, saved_key_.GetUserKey());
      if (cmp == 0 || (skipping_saved_key && cmp < 0)) {
        num_skipped++;
      } else {
        saved_key_.SetUserKey(
            ikey_.user_key,
            !iter_.iter()->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);
        skipping_saved_key = false;
        num_skipped = 0;
        reseek_done = false;
      }
    }

    // If we have sequentially iterated via numerous equal keys, then it's
    // better to seek so that we can avoid too many key comparisons.
    //
    // To avoid infinite loops, do not reseek if we have already attempted to
    // reseek previously.
    //
    // TODO(lth): If we reseek to sequence number greater than ikey_.sequence,
    // then it does not make sense to reseek as we would actually land further
    // away from the desired key. There is opportunity for optimization here.
    if (num_skipped > max_skip_ && !reseek_done) {
      is_key_seqnum_zero_ = false;
      num_skipped = 0;
      reseek_done = true;
      std::string last_key;
      if (skipping_saved_key) {
        // We're looking for the next user-key but all we see are the same
        // user-key with decreasing sequence numbers. Fast forward to
        // sequence number 0 and type deletion (the smallest type).
        if (timestamp_size_ == 0) {
          AppendInternalKey(
              &last_key,
              ParsedInternalKey(saved_key_.GetUserKey(), 0, kTypeDeletion));
        } else {
          const std::string kTsMin(timestamp_size_, '\0');
          AppendInternalKeyWithDifferentTimestamp(
              &last_key,
              ParsedInternalKey(saved_key_.GetUserKey(), 0, kTypeDeletion),
              kTsMin);
        }
        // Don't set skipping_saved_key = false because we may still see more
        // user-keys equal to saved_key_.
      } else {
        // We saw multiple entries with this user key and sequence numbers
        // higher than sequence_. Fast forward to sequence_.
        // Note that this only covers a case when a higher key was overwritten
        // many times since our snapshot was taken, not the case when a lot of
        // different keys were inserted after our snapshot was taken.
        if (timestamp_size_ == 0) {
          AppendInternalKey(
              &last_key, ParsedInternalKey(saved_key_.GetUserKey(), sequence_,
                                           kValueTypeForSeek));
        } else {
          AppendInternalKeyWithDifferentTimestamp(
              &last_key,
              ParsedInternalKey(saved_key_.GetUserKey(), sequence_,
                                kValueTypeForSeek),
              *timestamp_ub_);
        }
      }
      iter_.Seek(last_key);
      RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
    } else {
      iter_.Next();
    }
  } while (iter_.Valid());

  valid_ = false;
  return iter_.status().ok();
}

// Merge values of the same user key starting from the current iter_ position
// Scan from the newer entries to older entries.
// PRE: iter_.key() points to the first merge type entry
//      saved_key_ stores the user key
//      iter_.PrepareValue() has been called
// POST: saved_value_ has the merged value for the user key
//       iter_ points to the next entry (or invalid)
bool DBIter::MergeValuesNewToOld() {
  if (!merge_operator_) {
    ROCKS_LOG_ERROR(logger_, "Options::merge_operator is null.");
    status_ = Status::InvalidArgument("merge_operator_ must be set.");
    valid_ = false;
    return false;
  }

  // Temporarily pin the blocks that hold merge operands
  TempPinData();
  merge_context_.Clear();
  // Start the merge process by pushing the first operand
  merge_context_.PushOperand(
      iter_.value(), iter_.iter()->IsValuePinned() /* operand_pinned */);
  TEST_SYNC_POINT("DBIter::MergeValuesNewToOld:PushedFirstOperand");

  ParsedInternalKey ikey;
  for (iter_.Next(); iter_.Valid(); iter_.Next()) {
    TEST_SYNC_POINT("DBIter::MergeValuesNewToOld:SteppedToNextOperand");
    if (!ParseKey(&ikey)) {
      return false;
    }

    if (!user_comparator_.Equal(ikey.user_key, saved_key_.GetUserKey())) {
      // hit the next user key, stop right here
      break;
    }
    if (kTypeDeletion == ikey.type || kTypeSingleDeletion == ikey.type ||
               range_del_agg_.ShouldDelete(
                   ikey, RangeDelPositioningMode::kForwardTraversal)) {
      // hit a delete with the same user key, stop right here
      // iter_ is positioned after delete
      iter_.Next();
      break;
    }
    if (!iter_.PrepareValue()) {
      valid_ = false;
      return false;
    }

    if (kTypeValue == ikey.type) {
      // hit a put, merge the put value with operands and store the
      // final result in saved_value_. We are done!
      const Slice val = iter_.value();
      Status s = Merge(&val, ikey.user_key);
      if (!s.ok()) {
        return false;
      }
      // iter_ is positioned after put
      iter_.Next();
      if (!iter_.status().ok()) {
        valid_ = false;
        return false;
      }
      return true;
    } else if (kTypeMerge == ikey.type) {
      // hit a merge, add the value as an operand and run associative merge.
      // when complete, add result to operands and continue.
      merge_context_.PushOperand(
          iter_.value(), iter_.iter()->IsValuePinned() /* operand_pinned */);
      PERF_COUNTER_ADD(internal_merge_count, 1);
    } else if (kTypeBlobIndex == ikey.type) {
      if (expose_blob_index_) {
        status_ =
            Status::NotSupported("BlobDB does not support merge operator.");
        valid_ = false;
        return false;
      }
      // hit a put, merge the put value with operands and store the
      // final result in saved_value_. We are done!
      if (!SetBlobValueIfNeeded(ikey.user_key, iter_.value())) {
        return false;
      }
      valid_ = true;
      const Slice blob_value = value();
      Status s = Merge(&blob_value, ikey.user_key);
      if (!s.ok()) {
        return false;
      }
      is_blob_ = false;
      // iter_ is positioned after put
      iter_.Next();
      if (!iter_.status().ok()) {
        valid_ = false;
        return false;
      }
      return true;
    } else {
      valid_ = false;
      status_ = Status::Corruption(
          "Unrecognized value type: " +
          std::to_string(static_cast<unsigned int>(ikey.type)));
      return false;
    }
  }

  if (!iter_.status().ok()) {
    valid_ = false;
    return false;
  }

  // we either exhausted all internal keys under this user key, or hit
  // a deletion marker.
  // feed null as the existing value to the merge operator, such that
  // client can differentiate this scenario and do things accordingly.
  Status s = Merge(nullptr, saved_key_.GetUserKey());
  if (!s.ok()) {
    return false;
  }
  assert(status_.ok());
  return true;
}

void DBIter::Prev() {
  assert(valid_);
  assert(status_.ok());

  PERF_CPU_TIMER_GUARD(iter_prev_cpu_nanos, clock_);
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  bool ok = true;
  if (direction_ == kForward) {
    if (!ReverseToBackward()) {
      ok = false;
    }
  }
  if (ok) {
    Slice prefix;
    if (prefix_same_as_start_) {
      assert(prefix_extractor_ != nullptr);
      prefix = prefix_.GetUserKey();
    }
    PrevInternal(prefix_same_as_start_ ? &prefix : nullptr);
  }

  if (statistics_ != nullptr) {
    local_stats_.prev_count_++;
    if (valid_) {
      local_stats_.prev_found_count_++;
      local_stats_.bytes_read_ += (key().size() + value().size());
    }
  }
}

bool DBIter::ReverseToForward() {
  assert(iter_.status().ok());

  // When moving backwards, iter_ is positioned on _previous_ key, which may
  // not exist or may have different prefix than the current key().
  // If that's the case, seek iter_ to current key.
  if (!expect_total_order_inner_iter() || !iter_.Valid()) {
    IterKey last_key;
    ParsedInternalKey pikey(saved_key_.GetUserKey(), kMaxSequenceNumber,
                            kValueTypeForSeek);
    if (timestamp_size_ > 0) {
      // TODO: pre-create kTsMax.
      const std::string kTsMax(timestamp_size_, '\xff');
      pikey.SetTimestamp(kTsMax);
    }
    last_key.SetInternalKey(pikey);
    iter_.Seek(last_key.GetInternalKey());
    RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
  }

  direction_ = kForward;
  // Skip keys less than the current key() (a.k.a. saved_key_).
  while (iter_.Valid()) {
    ParsedInternalKey ikey;
    if (!ParseKey(&ikey)) {
      return false;
    }
    if (user_comparator_.Compare(ikey.user_key, saved_key_.GetUserKey()) >= 0) {
      return true;
    }
    iter_.Next();
  }

  if (!iter_.status().ok()) {
    valid_ = false;
    return false;
  }

  return true;
}

// Move iter_ to the key before saved_key_.
bool DBIter::ReverseToBackward() {
  assert(iter_.status().ok());

  // When current_entry_is_merged_ is true, iter_ may be positioned on the next
  // key, which may not exist or may have prefix different from current.
  // If that's the case, seek to saved_key_.
  if (current_entry_is_merged_ &&
      (!expect_total_order_inner_iter() || !iter_.Valid())) {
    IterKey last_key;
    // Using kMaxSequenceNumber and kValueTypeForSeek
    // (not kValueTypeForSeekForPrev) to seek to a key strictly smaller
    // than saved_key_.
    last_key.SetInternalKey(ParsedInternalKey(
        saved_key_.GetUserKey(), kMaxSequenceNumber, kValueTypeForSeek));
    if (!expect_total_order_inner_iter()) {
      iter_.SeekForPrev(last_key.GetInternalKey());
    } else {
      // Some iterators may not support SeekForPrev(), so we avoid using it
      // when prefix seek mode is disabled. This is somewhat expensive
      // (an extra Prev(), as well as an extra change of direction of iter_),
      // so we may need to reconsider it later.
      iter_.Seek(last_key.GetInternalKey());
      if (!iter_.Valid() && iter_.status().ok()) {
        iter_.SeekToLast();
      }
    }
    RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
  }

  direction_ = kReverse;
  return FindUserKeyBeforeSavedKey();
}

void DBIter::PrevInternal(const Slice* prefix) {
  while (iter_.Valid()) {
    saved_key_.SetUserKey(
        ExtractUserKey(iter_.key()),
        !iter_.iter()->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);

    assert(prefix == nullptr || prefix_extractor_ != nullptr);
    if (prefix != nullptr &&
        prefix_extractor_
                ->Transform(StripTimestampFromUserKey(saved_key_.GetUserKey(),
                                                      timestamp_size_))
                .compare(*prefix) != 0) {
      assert(prefix_same_as_start_);
      // Current key does not have the same prefix as start
      valid_ = false;
      return;
    }

    assert(iterate_lower_bound_ == nullptr || iter_.MayBeOutOfLowerBound() ||
           user_comparator_.CompareWithoutTimestamp(
               saved_key_.GetUserKey(), /*a_has_ts=*/true,
               *iterate_lower_bound_, /*b_has_ts=*/false) >= 0);
    if (iterate_lower_bound_ != nullptr && iter_.MayBeOutOfLowerBound() &&
        user_comparator_.CompareWithoutTimestamp(
            saved_key_.GetUserKey(), /*a_has_ts=*/true, *iterate_lower_bound_,
            /*b_has_ts=*/false) < 0) {
      // We've iterated earlier than the user-specified lower bound.
      valid_ = false;
      return;
    }

    if (!FindValueForCurrentKey()) {  // assigns valid_
      return;
    }

    // Whether or not we found a value for current key, we need iter_ to end up
    // on a smaller key.
    if (!FindUserKeyBeforeSavedKey()) {
      return;
    }

    if (valid_) {
      // Found the value.
      return;
    }

    if (TooManyInternalKeysSkipped(false)) {
      return;
    }
  }

  // We haven't found any key - iterator is not valid
  valid_ = false;
}

// Used for backwards iteration.
// Looks at the entries with user key saved_key_ and finds the most up-to-date
// value for it, or executes a merge, or determines that the value was deleted.
// Sets valid_ to true if the value is found and is ready to be presented to
// the user through value().
// Sets valid_ to false if the value was deleted, and we should try another key.
// Returns false if an error occurred, and !status().ok() and !valid_.
//
// PRE: iter_ is positioned on the last entry with user key equal to saved_key_.
// POST: iter_ is positioned on one of the entries equal to saved_key_, or on
//       the entry just before them, or on the entry just after them.
bool DBIter::FindValueForCurrentKey() {
  assert(iter_.Valid());
  merge_context_.Clear();
  current_entry_is_merged_ = false;
  // last entry before merge (could be kTypeDeletion,
  // kTypeDeletionWithTimestamp, kTypeSingleDeletion or kTypeValue)
  ValueType last_not_merge_type = kTypeDeletion;
  ValueType last_key_entry_type = kTypeDeletion;

  // Temporarily pin blocks that hold (merge operands / the value)
  ReleaseTempPinnedData();
  TempPinData();
  size_t num_skipped = 0;
  while (iter_.Valid()) {
    ParsedInternalKey ikey;
    if (!ParseKey(&ikey)) {
      return false;
    }

    assert(ikey.user_key.size() >= timestamp_size_);
    Slice ts;
    if (timestamp_size_ > 0) {
      ts = Slice(ikey.user_key.data() + ikey.user_key.size() - timestamp_size_,
                 timestamp_size_);
    }
    if (!IsVisible(ikey.sequence, ts) ||
        !user_comparator_.EqualWithoutTimestamp(ikey.user_key,
                                                saved_key_.GetUserKey())) {
      break;
    }
    if (!ts.empty()) {
      saved_timestamp_.assign(ts.data(), ts.size());
    }
    if (TooManyInternalKeysSkipped()) {
      return false;
    }

    // This user key has lots of entries.
    // We're going from old to new, and it's taking too long. Let's do a Seek()
    // and go from new to old. This helps when a key was overwritten many times.
    if (num_skipped >= max_skip_) {
      return FindValueForCurrentKeyUsingSeek();
    }

    if (!iter_.PrepareValue()) {
      valid_ = false;
      return false;
    }

    last_key_entry_type = ikey.type;
    switch (last_key_entry_type) {
      case kTypeValue:
      case kTypeBlobIndex:
        if (range_del_agg_.ShouldDelete(
                ikey, RangeDelPositioningMode::kBackwardTraversal)) {
          last_key_entry_type = kTypeRangeDeletion;
          PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
        } else if (iter_.iter()->IsValuePinned()) {
          pinned_value_ = iter_.value();
        } else {
          valid_ = false;
          status_ = Status::NotSupported(
              "Backward iteration not supported if underlying iterator's value "
              "cannot be pinned.");
        }
        merge_context_.Clear();
        last_not_merge_type = last_key_entry_type;
        if (!status_.ok()) {
          return false;
        }
        break;
      case kTypeDeletion:
      case kTypeDeletionWithTimestamp:
      case kTypeSingleDeletion:
        merge_context_.Clear();
        last_not_merge_type = last_key_entry_type;
        PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
        break;
      case kTypeMerge:
        if (range_del_agg_.ShouldDelete(
                ikey, RangeDelPositioningMode::kBackwardTraversal)) {
          merge_context_.Clear();
          last_key_entry_type = kTypeRangeDeletion;
          last_not_merge_type = last_key_entry_type;
          PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
        } else {
          assert(merge_operator_ != nullptr);
          merge_context_.PushOperandBack(
              iter_.value(),
              iter_.iter()->IsValuePinned() /* operand_pinned */);
          PERF_COUNTER_ADD(internal_merge_count, 1);
        }
        break;
      default:
        valid_ = false;
        status_ = Status::Corruption(
            "Unknown value type: " +
            std::to_string(static_cast<unsigned int>(last_key_entry_type)));
        return false;
    }

    PERF_COUNTER_ADD(internal_key_skipped_count, 1);
    iter_.Prev();
    ++num_skipped;
  }

  if (!iter_.status().ok()) {
    valid_ = false;
    return false;
  }

  Status s;
  s.PermitUncheckedError();
  is_blob_ = false;
  switch (last_key_entry_type) {
    case kTypeDeletion:
    case kTypeDeletionWithTimestamp:
    case kTypeSingleDeletion:
    case kTypeRangeDeletion:
      valid_ = false;
      return true;
    case kTypeMerge:
      current_entry_is_merged_ = true;
      if (last_not_merge_type == kTypeDeletion ||
          last_not_merge_type == kTypeSingleDeletion ||
          last_not_merge_type == kTypeRangeDeletion) {
        s = Merge(nullptr, saved_key_.GetUserKey());
        if (!s.ok()) {
          return false;
        }
        return true;
      } else if (last_not_merge_type == kTypeBlobIndex) {
        if (expose_blob_index_) {
          status_ =
              Status::NotSupported("BlobDB does not support merge operator.");
          valid_ = false;
          return false;
        }
        if (!SetBlobValueIfNeeded(saved_key_.GetUserKey(), pinned_value_)) {
          return false;
        }
        valid_ = true;
        const Slice blob_value = value();
        s = Merge(&blob_value, saved_key_.GetUserKey());
        if (!s.ok()) {
          return false;
        }
        is_blob_ = false;
        return true;
      } else {
        assert(last_not_merge_type == kTypeValue);
        s = Merge(&pinned_value_, saved_key_.GetUserKey());
        if (!s.ok()) {
          return false;
        }
        return true;
      }
      break;
    case kTypeValue:
      // do nothing - we've already has value in pinned_value_
      break;
    case kTypeBlobIndex:
      if (!SetBlobValueIfNeeded(saved_key_.GetUserKey(), pinned_value_)) {
        return false;
      }
      break;
    default:
      valid_ = false;
      status_ = Status::Corruption(
          "Unknown value type: " +
          std::to_string(static_cast<unsigned int>(last_key_entry_type)));
      return false;
  }
  if (!s.ok()) {
    valid_ = false;
    status_ = s;
    return false;
  }
  valid_ = true;
  return true;
}

// This function is used in FindValueForCurrentKey.
// We use Seek() function instead of Prev() to find necessary value
// TODO: This is very similar to FindNextUserEntry() and MergeValuesNewToOld().
//       Would be nice to reuse some code.
bool DBIter::FindValueForCurrentKeyUsingSeek() {
  // FindValueForCurrentKey will enable pinning before calling
  // FindValueForCurrentKeyUsingSeek()
  assert(pinned_iters_mgr_.PinningEnabled());
  std::string last_key;
  if (0 == timestamp_size_) {
    AppendInternalKey(&last_key,
                      ParsedInternalKey(saved_key_.GetUserKey(), sequence_,
                                        kValueTypeForSeek));
  } else {
    AppendInternalKeyWithDifferentTimestamp(
        &last_key,
        ParsedInternalKey(saved_key_.GetUserKey(), sequence_,
                          kValueTypeForSeek),
        *timestamp_ub_);
  }
  iter_.Seek(last_key);
  RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);

  // In case read_callback presents, the value we seek to may not be visible.
  // Find the next value that's visible.
  ParsedInternalKey ikey;
  is_blob_ = false;
  while (true) {
    if (!iter_.Valid()) {
      valid_ = false;
      return iter_.status().ok();
    }

    if (!ParseKey(&ikey)) {
      return false;
    }
    assert(ikey.user_key.size() >= timestamp_size_);
    Slice ts;
    if (timestamp_size_ > 0) {
      ts = Slice(ikey.user_key.data() + ikey.user_key.size() - timestamp_size_,
                 timestamp_size_);
    }

    if (!user_comparator_.EqualWithoutTimestamp(ikey.user_key,
                                                saved_key_.GetUserKey())) {
      // No visible values for this key, even though FindValueForCurrentKey()
      // has seen some. This is possible if we're using a tailing iterator, and
      // the entries were discarded in a compaction.
      valid_ = false;
      return true;
    }

    if (IsVisible(ikey.sequence, ts)) {
      break;
    }

    iter_.Next();
  }

  if (ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion ||
      range_del_agg_.ShouldDelete(
          ikey, RangeDelPositioningMode::kBackwardTraversal) ||
      kTypeDeletionWithTimestamp == ikey.type) {
    valid_ = false;
    return true;
  }
  if (!iter_.PrepareValue()) {
    valid_ = false;
    return false;
  }
  if (timestamp_size_ > 0) {
    Slice ts = ExtractTimestampFromUserKey(ikey.user_key, timestamp_size_);
    saved_timestamp_.assign(ts.data(), ts.size());
  }
  if (ikey.type == kTypeValue || ikey.type == kTypeBlobIndex) {
    assert(iter_.iter()->IsValuePinned());
    pinned_value_ = iter_.value();
    if (ikey.type == kTypeBlobIndex) {
      if (!SetBlobValueIfNeeded(ikey.user_key, pinned_value_)) {
        return false;
      }
    }

    valid_ = true;
    return true;
  }

  // kTypeMerge. We need to collect all kTypeMerge values and save them
  // in operands
  assert(ikey.type == kTypeMerge);
  current_entry_is_merged_ = true;
  merge_context_.Clear();
  merge_context_.PushOperand(
      iter_.value(), iter_.iter()->IsValuePinned() /* operand_pinned */);
  while (true) {
    iter_.Next();

    if (!iter_.Valid()) {
      if (!iter_.status().ok()) {
        valid_ = false;
        return false;
      }
      break;
    }
    if (!ParseKey(&ikey)) {
      return false;
    }
    if (!user_comparator_.Equal(ikey.user_key, saved_key_.GetUserKey())) {
      break;
    }
    if (ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion ||
        range_del_agg_.ShouldDelete(
            ikey, RangeDelPositioningMode::kForwardTraversal)) {
      break;
    }
    if (!iter_.PrepareValue()) {
      valid_ = false;
      return false;
    }

    if (ikey.type == kTypeValue) {
      const Slice val = iter_.value();
      Status s = Merge(&val, saved_key_.GetUserKey());
      if (!s.ok()) {
        return false;
      }
      return true;
    } else if (ikey.type == kTypeMerge) {
      merge_context_.PushOperand(
          iter_.value(), iter_.iter()->IsValuePinned() /* operand_pinned */);
      PERF_COUNTER_ADD(internal_merge_count, 1);
    } else if (ikey.type == kTypeBlobIndex) {
      if (expose_blob_index_) {
        status_ =
            Status::NotSupported("BlobDB does not support merge operator.");
        valid_ = false;
        return false;
      }
      if (!SetBlobValueIfNeeded(ikey.user_key, iter_.value())) {
        return false;
      }
      valid_ = true;
      const Slice blob_value = value();
      Status s = Merge(&blob_value, saved_key_.GetUserKey());
      if (!s.ok()) {
        return false;
      }
      is_blob_ = false;
      return true;
    } else {
      valid_ = false;
      status_ = Status::Corruption(
          "Unknown value type: " +
          std::to_string(static_cast<unsigned int>(ikey.type)));
      return false;
    }
  }

  Status s = Merge(nullptr, saved_key_.GetUserKey());
  if (!s.ok()) {
    return false;
  }

  // Make sure we leave iter_ in a good state. If it's valid and we don't care
  // about prefixes, that's already good enough. Otherwise it needs to be
  // seeked to the current key.
  if (!expect_total_order_inner_iter() || !iter_.Valid()) {
    if (!expect_total_order_inner_iter()) {
      iter_.SeekForPrev(last_key);
    } else {
      iter_.Seek(last_key);
      if (!iter_.Valid() && iter_.status().ok()) {
        iter_.SeekToLast();
      }
    }
    RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
  }

  valid_ = true;
  return true;
}

Status DBIter::Merge(const Slice* val, const Slice& user_key) {
  Status s = MergeHelper::TimedFullMerge(
      merge_operator_, user_key, val, merge_context_.GetOperands(),
      &saved_value_, logger_, statistics_, clock_, &pinned_value_, true);
  if (!s.ok()) {
    valid_ = false;
    status_ = s;
    return s;
  }
  valid_ = true;
  return s;
}

// Move backwards until the key smaller than saved_key_.
// Changes valid_ only if return value is false.
bool DBIter::FindUserKeyBeforeSavedKey() {
  assert(status_.ok());
  size_t num_skipped = 0;
  while (iter_.Valid()) {
    ParsedInternalKey ikey;
    if (!ParseKey(&ikey)) {
      return false;
    }

    if (user_comparator_.CompareWithoutTimestamp(ikey.user_key,
                                                 saved_key_.GetUserKey()) < 0) {
      return true;
    }

    if (TooManyInternalKeysSkipped()) {
      return false;
    }

    assert(ikey.sequence != kMaxSequenceNumber);
    assert(ikey.user_key.size() >= timestamp_size_);
    Slice ts;
    if (timestamp_size_ > 0) {
      ts = Slice(ikey.user_key.data() + ikey.user_key.size() - timestamp_size_,
                 timestamp_size_);
    }
    if (!IsVisible(ikey.sequence, ts)) {
      PERF_COUNTER_ADD(internal_recent_skipped_count, 1);
    } else {
      PERF_COUNTER_ADD(internal_key_skipped_count, 1);
    }

    if (num_skipped >= max_skip_) {
      num_skipped = 0;
      IterKey last_key;
      ParsedInternalKey pikey(saved_key_.GetUserKey(), kMaxSequenceNumber,
                              kValueTypeForSeek);
      if (timestamp_size_ > 0) {
        // TODO: pre-create kTsMax.
        const std::string kTsMax(timestamp_size_, '\xff');
        pikey.SetTimestamp(kTsMax);
      }
      last_key.SetInternalKey(pikey);
      // It would be more efficient to use SeekForPrev() here, but some
      // iterators may not support it.
      iter_.Seek(last_key.GetInternalKey());
      RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
      if (!iter_.Valid()) {
        break;
      }
    } else {
      ++num_skipped;
    }

    iter_.Prev();
  }

  if (!iter_.status().ok()) {
    valid_ = false;
    return false;
  }

  return true;
}

bool DBIter::TooManyInternalKeysSkipped(bool increment) {
  if ((max_skippable_internal_keys_ > 0) &&
      (num_internal_keys_skipped_ > max_skippable_internal_keys_)) {
    valid_ = false;
    status_ = Status::Incomplete("Too many internal keys skipped.");
    return true;
  } else if (increment) {
    num_internal_keys_skipped_++;
  }
  return false;
}

bool DBIter::IsVisible(SequenceNumber sequence, const Slice& ts,
                       bool* more_recent) {
  // Remember that comparator orders preceding timestamp as larger.
  // TODO(yanqin): support timestamp in read_callback_.
  bool visible_by_seq = (read_callback_ == nullptr)
                            ? sequence <= sequence_
                            : read_callback_->IsVisible(sequence);

  bool visible_by_ts =
      (timestamp_ub_ == nullptr ||
       user_comparator_.CompareTimestamp(ts, *timestamp_ub_) <= 0) &&
      (timestamp_lb_ == nullptr ||
       user_comparator_.CompareTimestamp(ts, *timestamp_lb_) >= 0);

  if (more_recent) {
    *more_recent = !visible_by_seq;
  }
  return visible_by_seq && visible_by_ts;
}

void DBIter::SetSavedKeyToSeekTarget(const Slice& target) {
  is_key_seqnum_zero_ = false;
  SequenceNumber seq = sequence_;
  saved_key_.Clear();
  saved_key_.SetInternalKey(target, seq, kValueTypeForSeek, timestamp_ub_);

  if (iterate_lower_bound_ != nullptr &&
      user_comparator_.CompareWithoutTimestamp(
          saved_key_.GetUserKey(), /*a_has_ts=*/true, *iterate_lower_bound_,
          /*b_has_ts=*/false) < 0) {
    // Seek key is smaller than the lower bound.
    saved_key_.Clear();
    saved_key_.SetInternalKey(*iterate_lower_bound_, seq, kValueTypeForSeek,
                              timestamp_ub_);
  }
}

void DBIter::SetSavedKeyToSeekForPrevTarget(const Slice& target) {
  is_key_seqnum_zero_ = false;
  saved_key_.Clear();
  // now saved_key is used to store internal key.
  saved_key_.SetInternalKey(target, 0 /* sequence_number */,
                            kValueTypeForSeekForPrev, timestamp_ub_);

  if (timestamp_size_ > 0) {
    const std::string kTsMin(timestamp_size_, '\0');
    Slice ts = kTsMin;
    saved_key_.UpdateInternalKey(/*seq=*/0, kValueTypeForSeekForPrev, &ts);
  }

  if (iterate_upper_bound_ != nullptr &&
      user_comparator_.CompareWithoutTimestamp(
          saved_key_.GetUserKey(), /*a_has_ts=*/true, *iterate_upper_bound_,
          /*b_has_ts=*/false) >= 0) {
    saved_key_.Clear();
    saved_key_.SetInternalKey(*iterate_upper_bound_, kMaxSequenceNumber,
                              kValueTypeForSeekForPrev, timestamp_ub_);
    if (timestamp_size_ > 0) {
      const std::string kTsMax(timestamp_size_, '\xff');
      Slice ts = kTsMax;
      saved_key_.UpdateInternalKey(kMaxSequenceNumber, kValueTypeForSeekForPrev,
                                   &ts);
    }
  }
}

void DBIter::Seek(const Slice& target) {
  PERF_CPU_TIMER_GUARD(iter_seek_cpu_nanos, clock_);
  StopWatch sw(clock_, statistics_, DB_SEEK);

#ifndef ROCKSDB_LITE
  if (db_impl_ != nullptr && cfd_ != nullptr) {
    // TODO: What do we do if this returns an error?
    Slice lower_bound, upper_bound;
    if (iterate_lower_bound_ != nullptr) {
      lower_bound = *iterate_lower_bound_;
    } else {
      lower_bound = Slice("");
    }
    if (iterate_upper_bound_ != nullptr) {
      upper_bound = *iterate_upper_bound_;
    } else {
      upper_bound = Slice("");
    }
    db_impl_->TraceIteratorSeek(cfd_->GetID(), target, lower_bound, upper_bound)
        .PermitUncheckedError();
  }
#endif  // ROCKSDB_LITE

  status_ = Status::OK();
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();

  // Seek the inner iterator based on the target key.
  {
    PERF_TIMER_GUARD(seek_internal_seek_time);

    SetSavedKeyToSeekTarget(target);
    iter_.Seek(saved_key_.GetInternalKey());

    range_del_agg_.InvalidateRangeDelMapPositions();
    RecordTick(statistics_, NUMBER_DB_SEEK);
  }
  if (!iter_.Valid()) {
    valid_ = false;
    return;
  }
  direction_ = kForward;

  // Now the inner iterator is placed to the target position. From there,
  // we need to find out the next key that is visible to the user.
  ClearSavedValue();
  if (prefix_same_as_start_) {
    // The case where the iterator needs to be invalidated if it has exhausted
    // keys within the same prefix of the seek key.
    assert(prefix_extractor_ != nullptr);
    Slice target_prefix = prefix_extractor_->Transform(target);
    FindNextUserEntry(false /* not skipping saved_key */,
                      &target_prefix /* prefix */);
    if (valid_) {
      // Remember the prefix of the seek key for the future Next() call to
      // check.
      prefix_.SetUserKey(target_prefix);
    }
  } else {
    FindNextUserEntry(false /* not skipping saved_key */, nullptr);
  }
  if (!valid_) {
    return;
  }

  // Updating stats and perf context counters.
  if (statistics_ != nullptr) {
    // Decrement since we don't want to count this key as skipped
    RecordTick(statistics_, NUMBER_DB_SEEK_FOUND);
    RecordTick(statistics_, ITER_BYTES_READ, key().size() + value().size());
  }
  PERF_COUNTER_ADD(iter_read_bytes, key().size() + value().size());
}

void DBIter::SeekForPrev(const Slice& target) {
  PERF_CPU_TIMER_GUARD(iter_seek_cpu_nanos, clock_);
  StopWatch sw(clock_, statistics_, DB_SEEK);

#ifndef ROCKSDB_LITE
  if (db_impl_ != nullptr && cfd_ != nullptr) {
    // TODO: What do we do if this returns an error?
    Slice lower_bound, upper_bound;
    if (iterate_lower_bound_ != nullptr) {
      lower_bound = *iterate_lower_bound_;
    } else {
      lower_bound = Slice("");
    }
    if (iterate_upper_bound_ != nullptr) {
      upper_bound = *iterate_upper_bound_;
    } else {
      upper_bound = Slice("");
    }
    db_impl_
        ->TraceIteratorSeekForPrev(cfd_->GetID(), target, lower_bound,
                                   upper_bound)
        .PermitUncheckedError();
  }
#endif  // ROCKSDB_LITE

  status_ = Status::OK();
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();

  // Seek the inner iterator based on the target key.
  {
    PERF_TIMER_GUARD(seek_internal_seek_time);
    SetSavedKeyToSeekForPrevTarget(target);
    iter_.SeekForPrev(saved_key_.GetInternalKey());
    range_del_agg_.InvalidateRangeDelMapPositions();
    RecordTick(statistics_, NUMBER_DB_SEEK);
  }
  if (!iter_.Valid()) {
    valid_ = false;
    return;
  }
  direction_ = kReverse;

  // Now the inner iterator is placed to the target position. From there,
  // we need to find out the first key that is visible to the user in the
  // backward direction.
  ClearSavedValue();
  if (prefix_same_as_start_) {
    // The case where the iterator needs to be invalidated if it has exhausted
    // keys within the same prefix of the seek key.
    assert(prefix_extractor_ != nullptr);
    Slice target_prefix = prefix_extractor_->Transform(target);
    PrevInternal(&target_prefix);
    if (valid_) {
      // Remember the prefix of the seek key for the future Prev() call to
      // check.
      prefix_.SetUserKey(target_prefix);
    }
  } else {
    PrevInternal(nullptr);
  }

  // Report stats and perf context.
  if (statistics_ != nullptr && valid_) {
    RecordTick(statistics_, NUMBER_DB_SEEK_FOUND);
    RecordTick(statistics_, ITER_BYTES_READ, key().size() + value().size());
    PERF_COUNTER_ADD(iter_read_bytes, key().size() + value().size());
  }
}

void DBIter::SeekToFirst() {
  if (iterate_lower_bound_ != nullptr) {
    Seek(*iterate_lower_bound_);
    return;
  }
  PERF_CPU_TIMER_GUARD(iter_seek_cpu_nanos, clock_);
  // Don't use iter_::Seek() if we set a prefix extractor
  // because prefix seek will be used.
  if (!expect_total_order_inner_iter()) {
    max_skip_ = std::numeric_limits<uint64_t>::max();
  }
  status_ = Status::OK();
  direction_ = kForward;
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  ClearSavedValue();
  is_key_seqnum_zero_ = false;

  {
    PERF_TIMER_GUARD(seek_internal_seek_time);
    iter_.SeekToFirst();
    range_del_agg_.InvalidateRangeDelMapPositions();
  }

  RecordTick(statistics_, NUMBER_DB_SEEK);
  if (iter_.Valid()) {
    saved_key_.SetUserKey(
        ExtractUserKey(iter_.key()),
        !iter_.iter()->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);
    FindNextUserEntry(false /* not skipping saved_key */,
                      nullptr /* no prefix check */);
    if (statistics_ != nullptr) {
      if (valid_) {
        RecordTick(statistics_, NUMBER_DB_SEEK_FOUND);
        RecordTick(statistics_, ITER_BYTES_READ, key().size() + value().size());
        PERF_COUNTER_ADD(iter_read_bytes, key().size() + value().size());
      }
    }
  } else {
    valid_ = false;
  }
  if (valid_ && prefix_same_as_start_) {
    assert(prefix_extractor_ != nullptr);
    prefix_.SetUserKey(prefix_extractor_->Transform(
        StripTimestampFromUserKey(saved_key_.GetUserKey(), timestamp_size_)));
  }
}

void DBIter::SeekToLast() {
  if (iterate_upper_bound_ != nullptr) {
    // Seek to last key strictly less than ReadOptions.iterate_upper_bound.
    SeekForPrev(*iterate_upper_bound_);
    if (Valid() && 0 == user_comparator_.CompareWithoutTimestamp(
                            *iterate_upper_bound_, /*a_has_ts=*/false, key(),
                            /*b_has_ts=*/false)) {
      ReleaseTempPinnedData();
      PrevInternal(nullptr);
    }
    return;
  }

  PERF_CPU_TIMER_GUARD(iter_seek_cpu_nanos, clock_);
  // Don't use iter_::Seek() if we set a prefix extractor
  // because prefix seek will be used.
  if (!expect_total_order_inner_iter()) {
    max_skip_ = std::numeric_limits<uint64_t>::max();
  }
  status_ = Status::OK();
  direction_ = kReverse;
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  ClearSavedValue();
  is_key_seqnum_zero_ = false;

  {
    PERF_TIMER_GUARD(seek_internal_seek_time);
    iter_.SeekToLast();
    range_del_agg_.InvalidateRangeDelMapPositions();
  }
  PrevInternal(nullptr);
  if (statistics_ != nullptr) {
    RecordTick(statistics_, NUMBER_DB_SEEK);
    if (valid_) {
      RecordTick(statistics_, NUMBER_DB_SEEK_FOUND);
      RecordTick(statistics_, ITER_BYTES_READ, key().size() + value().size());
      PERF_COUNTER_ADD(iter_read_bytes, key().size() + value().size());
    }
  }
  if (valid_ && prefix_same_as_start_) {
    assert(prefix_extractor_ != nullptr);
    prefix_.SetUserKey(prefix_extractor_->Transform(
        StripTimestampFromUserKey(saved_key_.GetUserKey(), timestamp_size_)));
  }
}

Iterator* NewDBIterator(Env* env, const ReadOptions& read_options,
                        const ImmutableOptions& ioptions,
                        const MutableCFOptions& mutable_cf_options,
                        const Comparator* user_key_comparator,
                        InternalIterator* internal_iter, const Version* version,
                        const SequenceNumber& sequence,
                        uint64_t max_sequential_skip_in_iterations,
                        ReadCallback* read_callback, DBImpl* db_impl,
                        ColumnFamilyData* cfd, bool expose_blob_index) {
  DBIter* db_iter =
      new DBIter(env, read_options, ioptions, mutable_cf_options,
                 user_key_comparator, internal_iter, version, sequence, false,
                 max_sequential_skip_in_iterations, read_callback, db_impl, cfd,
                 expose_blob_index);
  return db_iter;
}

}  // namespace ROCKSDB_NAMESPACE
