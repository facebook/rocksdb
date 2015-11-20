// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/compaction_iterator.h"
#include "table/internal_iterator.h"

namespace rocksdb {

CompactionIterator::CompactionIterator(
    InternalIterator* input, const Comparator* cmp, MergeHelper* merge_helper,
    SequenceNumber last_sequence, std::vector<SequenceNumber>* snapshots,
    Env* env, bool expect_valid_internal_key, Compaction* compaction,
    const CompactionFilter* compaction_filter, LogBuffer* log_buffer)
    : input_(input),
      cmp_(cmp),
      merge_helper_(merge_helper),
      snapshots_(snapshots),
      env_(env),
      expect_valid_internal_key_(expect_valid_internal_key),
      compaction_(compaction),
      compaction_filter_(compaction_filter),
      log_buffer_(log_buffer),
      merge_out_iter_(merge_helper_) {
  assert(compaction_filter_ == nullptr || compaction_ != nullptr);
  bottommost_level_ =
      compaction_ == nullptr ? false : compaction_->bottommost_level();
  if (compaction_ != nullptr) {
    level_ptrs_ = std::vector<size_t>(compaction_->number_levels(), 0);
  }

  if (snapshots_->size() == 0) {
    // optimize for fast path if there are no snapshots
    visible_at_tip_ = last_sequence;
    earliest_snapshot_ = visible_at_tip_;
    latest_snapshot_ = 0;
  } else {
    visible_at_tip_ = 0;
    earliest_snapshot_ = snapshots_->at(0);
    latest_snapshot_ = snapshots_->back();
  }
  if (compaction_filter_ != nullptr && compaction_filter_->IgnoreSnapshots()) {
    ignore_snapshots_ = true;
  } else {
    ignore_snapshots_ = false;
  }
}

void CompactionIterator::ResetRecordCounts() {
  iter_stats_.num_record_drop_user = 0;
  iter_stats_.num_record_drop_hidden = 0;
  iter_stats_.num_record_drop_obsolete = 0;
}

void CompactionIterator::SeekToFirst() {
  NextFromInput();
  PrepareOutput();
}

void CompactionIterator::Next() {
  // If there is a merge output, return it before continuing to process the
  // input.
  if (merge_out_iter_.Valid()) {
    merge_out_iter_.Next();

    // Check if we returned all records of the merge output.
    if (merge_out_iter_.Valid()) {
      key_ = merge_out_iter_.key();
      value_ = merge_out_iter_.value();
      bool valid_key __attribute__((__unused__)) =
          ParseInternalKey(key_, &ikey_);
      // MergeUntil stops when it encounters a corrupt key and does not
      // include them in the result, so we expect the keys here to be valid.
      assert(valid_key);
      // Keep current_key_ in sync.
      current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
      key_ = current_key_.GetKey();
      ikey_.user_key = current_key_.GetUserKey();
      valid_ = true;
    } else {
      // MergeHelper moves the iterator to the first record after the merged
      // records, so even though we reached the end of the merge output, we do
      // not want to advance the iterator.
      NextFromInput();
    }
  } else {
    // Only advance the input iterator if there is no merge output and the
    // iterator is not already at the next record.
    if (!at_next_) {
      input_->Next();
    }
    NextFromInput();
  }

  PrepareOutput();
}

void CompactionIterator::NextFromInput() {
  at_next_ = false;
  valid_ = false;

  while (!valid_ && input_->Valid()) {
    key_ = input_->key();
    value_ = input_->value();
    iter_stats_.num_input_records++;

    if (!ParseInternalKey(key_, &ikey_)) {
      // If `expect_valid_internal_key_` is false, return the corrupted key
      // and let the caller decide what to do with it.
      // TODO(noetzli): We should have a more elegant solution for this.
      if (expect_valid_internal_key_) {
        assert(!"Corrupted internal key not expected.");
        status_ = Status::Corruption("Corrupted internal key not expected.");
        break;
      }
      key_ = current_key_.SetKey(key_);
      has_current_user_key_ = false;
      current_user_key_sequence_ = kMaxSequenceNumber;
      current_user_key_snapshot_ = 0;
      iter_stats_.num_input_corrupt_records++;
      valid_ = true;
      break;
    }

    // Update input statistics
    if (ikey_.type == kTypeDeletion || ikey_.type == kTypeSingleDeletion) {
      iter_stats_.num_input_deletion_records++;
    }
    iter_stats_.total_input_raw_key_bytes += key_.size();
    iter_stats_.total_input_raw_value_bytes += value_.size();

    // Check whether the user key changed. After this if statement current_key_
    // is a copy of the current input key (maybe converted to a delete by the
    // compaction filter). ikey_.user_key is pointing to the copy.
    if (!has_current_user_key_ ||
        !cmp_->Equal(ikey_.user_key, current_user_key_)) {
      // First occurrence of this user key
      key_ = current_key_.SetKey(key_, &ikey_);
      current_user_key_ = ikey_.user_key;
      has_current_user_key_ = true;
      current_user_key_sequence_ = kMaxSequenceNumber;
      current_user_key_snapshot_ = 0;
      // apply the compaction filter to the first occurrence of the user key
      if (compaction_filter_ != nullptr && ikey_.type == kTypeValue &&
          (visible_at_tip_ || ikey_.sequence > latest_snapshot_ ||
           ignore_snapshots_)) {
        // If the user has specified a compaction filter and the sequence
        // number is greater than any external snapshot, then invoke the
        // filter. If the return value of the compaction filter is true,
        // replace the entry with a deletion marker.
        bool value_changed = false;
        bool to_delete = false;
        compaction_filter_value_.clear();
        {
          StopWatchNano timer(env_, true);
          to_delete = compaction_filter_->Filter(
              compaction_->level(), ikey_.user_key, value_,
              &compaction_filter_value_, &value_changed);
          iter_stats_.total_filter_time +=
              env_ != nullptr ? timer.ElapsedNanos() : 0;
        }
        if (to_delete) {
          // convert the current key to a delete
          ikey_.type = kTypeDeletion;
          current_key_.UpdateInternalKey(ikey_.sequence, kTypeDeletion);
          // no value associated with delete
          value_.clear();
          iter_stats_.num_record_drop_user++;
        } else if (value_changed) {
          value_ = compaction_filter_value_;
        }
      }
    } else {
      // Update the current key to reflect the new sequence number/type without
      // copying the user key.
      // TODO(rven): Compaction filter does not process keys in this path
      // Need to have the compaction filter process multiple versions
      // if we have versions on both sides of a snapshot
      current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
      key_ = current_key_.GetKey();
      ikey_.user_key = current_key_.GetUserKey();
    }

    // If there are no snapshots, then this kv affect visibility at tip.
    // Otherwise, search though all existing snapshots to find the earliest
    // snapshot that is affected by this kv.
    SequenceNumber last_sequence __attribute__((__unused__)) =
        current_user_key_sequence_;
    current_user_key_sequence_ = ikey_.sequence;
    SequenceNumber last_snapshot = current_user_key_snapshot_;
    SequenceNumber prev_snapshot = 0;  // 0 means no previous snapshot
    current_user_key_snapshot_ =
        visible_at_tip_ ? visible_at_tip_ : findEarliestVisibleSnapshot(
                                                ikey_.sequence, &prev_snapshot);

    if (ikey_.type == kTypeSingleDeletion) {
      ParsedInternalKey next_ikey;
      input_->Next();

      // Check whether the current key is valid, not corrupt and the same
      // as the single delete.
      if (input_->Valid() && ParseInternalKey(input_->key(), &next_ikey) &&
          cmp_->Equal(ikey_.user_key, next_ikey.user_key)) {
        // Mixing single deletes and merges is not supported. Consecutive
        // single deletes are not valid.
        if (next_ikey.type != kTypeValue) {
          assert(false);
          status_ =
              Status::InvalidArgument("Put expected after single delete.");
          break;
        }

        // Check whether the current key belongs to the same snapshot as the
        // single delete.
        if (prev_snapshot == 0 || next_ikey.sequence > prev_snapshot) {
          // Found the matching value, we can drop the single delete and the
          // value.
          ++iter_stats_.num_record_drop_hidden;
          ++iter_stats_.num_record_drop_obsolete;
          input_->Next();
        } else {
          // We hit the next snapshot without hitting a put, so the iterator
          // returns the single delete.
          valid_ = true;
        }
      } else {
        // We are at the end of the input, could not parse the next key, or hit
        // the next key. The iterator returns the single delete if the key
        // possibly exists beyond the current output level.  We set
        // has_current_user_key to false so that if the iterator is at the next
        // key, we do not compare it again against the previous key at the next
        // iteration. If the next key is corrupt, we return before the
        // comparison, so the value of has_current_user_key does not matter.
        has_current_user_key_ = false;
        if (compaction_ != nullptr &&
            compaction_->KeyNotExistsBeyondOutputLevel(ikey_.user_key,
                                                       &level_ptrs_)) {
          ++iter_stats_.num_record_drop_obsolete;
        } else {
          valid_ = true;
        }
      }

      if (valid_) {
        at_next_ = true;
      }
    } else if (last_snapshot == current_user_key_snapshot_) {
      // If the earliest snapshot is which this key is visible in
      // is the same as the visibility of a previous instance of the
      // same key, then this kv is not visible in any snapshot.
      // Hidden by an newer entry for same user key
      // TODO: why not > ?
      assert(last_sequence >= current_user_key_sequence_);
      ++iter_stats_.num_record_drop_hidden;  // (A)
      input_->Next();
    } else if (compaction_ != nullptr && ikey_.type == kTypeDeletion &&
               ikey_.sequence <= earliest_snapshot_ &&
               compaction_->KeyNotExistsBeyondOutputLevel(ikey_.user_key,
                                                          &level_ptrs_)) {
      // TODO(noetzli): This is the only place where we use compaction_
      // (besides the constructor). We should probably get rid of this
      // dependency and find a way to do similar filtering during flushes.
      //
      // For this user key:
      // (1) there is no data in higher levels
      // (2) data in lower levels will have larger sequence numbers
      // (3) data in layers that are being compacted here and have
      //     smaller sequence numbers will be dropped in the next
      //     few iterations of this loop (by rule (A) above).
      // Therefore this deletion marker is obsolete and can be dropped.
      ++iter_stats_.num_record_drop_obsolete;
      input_->Next();
    } else if (ikey_.type == kTypeMerge) {
      if (!merge_helper_->HasOperator()) {
        LogToBuffer(log_buffer_, "Options::merge_operator is null.");
        status_ = Status::InvalidArgument(
            "merge_operator is not properly initialized.");
        return;
      }

      // We know the merge type entry is not hidden, otherwise we would
      // have hit (A)
      // We encapsulate the merge related state machine in a different
      // object to minimize change to the existing flow.
      merge_helper_->MergeUntil(input_, prev_snapshot, bottommost_level_);
      merge_out_iter_.SeekToFirst();

      if (merge_out_iter_.Valid()) {
        // NOTE: key, value, and ikey_ refer to old entries.
        //       These will be correctly set below.
        key_ = merge_out_iter_.key();
        value_ = merge_out_iter_.value();
        bool valid_key __attribute__((__unused__)) =
            ParseInternalKey(key_, &ikey_);
        // MergeUntil stops when it encounters a corrupt key and does not
        // include them in the result, so we expect the keys here to valid.
        assert(valid_key);
        // Keep current_key_ in sync.
        current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
        key_ = current_key_.GetKey();
        ikey_.user_key = current_key_.GetUserKey();
        valid_ = true;
      } else {
        // all merge operands were filtered out. reset the user key, since the
        // batch consumed by the merge operator should not shadow any keys
        // coming after the merges
        has_current_user_key_ = false;
      }
    } else {
      valid_ = true;
    }
  }
}

void CompactionIterator::PrepareOutput() {
  // Zeroing out the sequence number leads to better compression.
  // If this is the bottommost level (no files in lower levels)
  // and the earliest snapshot is larger than this seqno
  // then we can squash the seqno to zero.
  if (bottommost_level_ && valid_ && ikey_.sequence < earliest_snapshot_ &&
      ikey_.type != kTypeMerge) {
    assert(ikey_.type != kTypeDeletion && ikey_.type != kTypeSingleDeletion);
    ikey_.sequence = 0;
    current_key_.UpdateInternalKey(0, ikey_.type);
  }
}

inline SequenceNumber CompactionIterator::findEarliestVisibleSnapshot(
    SequenceNumber in, SequenceNumber* prev_snapshot) {
  assert(snapshots_->size());
  SequenceNumber prev __attribute__((unused)) = 0;
  for (const auto cur : *snapshots_) {
    assert(prev <= cur);
    if (cur >= in) {
      *prev_snapshot = prev;
      return cur;
    }
    prev = cur;
    assert(prev);
  }
  *prev_snapshot = prev;
  return kMaxSequenceNumber;
}

}  // namespace rocksdb
