//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/compaction_iterator.h"

#include "db/snapshot_checker.h"
#include "port/likely.h"
#include "rocksdb/listener.h"
#include "table/internal_iterator.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
CompactionEventListener::CompactionListenerValueType fromInternalValueType(
    ValueType vt) {
  switch (vt) {
    case kTypeDeletion:
      return CompactionEventListener::CompactionListenerValueType::kDelete;
    case kTypeValue:
      return CompactionEventListener::CompactionListenerValueType::kValue;
    case kTypeMerge:
      return CompactionEventListener::CompactionListenerValueType::
          kMergeOperand;
    case kTypeSingleDeletion:
      return CompactionEventListener::CompactionListenerValueType::
          kSingleDelete;
    case kTypeRangeDeletion:
      return CompactionEventListener::CompactionListenerValueType::kRangeDelete;
    case kTypeBlobIndex:
      return CompactionEventListener::CompactionListenerValueType::kBlobIndex;
    default:
      assert(false);
      return CompactionEventListener::CompactionListenerValueType::kInvalid;
  }
}
#endif  // ROCKSDB_LITE

CompactionIterator::CompactionIterator(
    InternalIterator* input, const Comparator* cmp, MergeHelper* merge_helper,
    SequenceNumber last_sequence, std::vector<SequenceNumber>* snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    const SnapshotChecker* snapshot_checker, Env* env,
    bool expect_valid_internal_key, RangeDelAggregator* range_del_agg,
    const Compaction* compaction, const CompactionFilter* compaction_filter,
    CompactionEventListener* compaction_listener,
    const std::atomic<bool>* shutting_down,
    const SequenceNumber preserve_deletes_seqnum)
    : CompactionIterator(
          input, cmp, merge_helper, last_sequence, snapshots,
          earliest_write_conflict_snapshot, snapshot_checker, env,
          expect_valid_internal_key, range_del_agg,
          std::unique_ptr<CompactionProxy>(
              compaction ? new CompactionProxy(compaction) : nullptr),
          compaction_filter, compaction_listener, shutting_down,
          preserve_deletes_seqnum) {}

CompactionIterator::CompactionIterator(
    InternalIterator* input, const Comparator* cmp, MergeHelper* merge_helper,
    SequenceNumber last_sequence, std::vector<SequenceNumber>* snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    const SnapshotChecker* snapshot_checker, Env* env,
    bool expect_valid_internal_key, RangeDelAggregator* range_del_agg,
    std::unique_ptr<CompactionProxy> compaction,
    const CompactionFilter* compaction_filter,
    CompactionEventListener* compaction_listener,
    const std::atomic<bool>* shutting_down,
    const SequenceNumber preserve_deletes_seqnum
  )
    : input_(input),
      cmp_(cmp),
      merge_helper_(merge_helper),
      snapshots_(snapshots),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      snapshot_checker_(snapshot_checker),
      env_(env),
      expect_valid_internal_key_(expect_valid_internal_key),
      range_del_agg_(range_del_agg),
      compaction_(std::move(compaction)),
      compaction_filter_(compaction_filter),
#ifndef ROCKSDB_LITE
      compaction_listener_(compaction_listener),
#endif  // ROCKSDB_LITE
      shutting_down_(shutting_down),
      preserve_deletes_seqnum_(preserve_deletes_seqnum),
      ignore_snapshots_(false),
      current_user_key_sequence_(0),
      current_user_key_snapshot_(0),
      merge_out_iter_(merge_helper_),
      current_key_committed_(false) {
  assert(compaction_filter_ == nullptr || compaction_ != nullptr);
  bottommost_level_ =
      compaction_ == nullptr ? false : compaction_->bottommost_level();
  if (compaction_ != nullptr) {
    level_ptrs_ = std::vector<size_t>(compaction_->number_levels(), 0);
  }

  if (snapshots_->size() == 0) {
    // optimize for fast path if there are no snapshots
    visible_at_tip_ = true;
    earliest_snapshot_ = kMaxSequenceNumber;
    latest_snapshot_ = 0;
  } else {
    visible_at_tip_ = false;
    earliest_snapshot_ = snapshots_->at(0);
    latest_snapshot_ = snapshots_->back();
  }
  if (compaction_filter_ != nullptr) {
    if (compaction_filter_->IgnoreSnapshots()) {
      ignore_snapshots_ = true;
    }
  } else {
    ignore_snapshots_ = false;
  }
  input_->SetPinnedItersMgr(&pinned_iters_mgr_);
}

CompactionIterator::~CompactionIterator() {
  // input_ Iteartor lifetime is longer than pinned_iters_mgr_ lifetime
  input_->SetPinnedItersMgr(nullptr);
}

void CompactionIterator::ResetRecordCounts() {
  iter_stats_.num_record_drop_user = 0;
  iter_stats_.num_record_drop_hidden = 0;
  iter_stats_.num_record_drop_obsolete = 0;
  iter_stats_.num_record_drop_range_del = 0;
  iter_stats_.num_range_del_drop_obsolete = 0;
  iter_stats_.num_optimized_del_drop_obsolete = 0;
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
      bool valid_key __attribute__((__unused__));
      valid_key =  ParseInternalKey(key_, &ikey_);
      // MergeUntil stops when it encounters a corrupt key and does not
      // include them in the result, so we expect the keys here to be valid.
      assert(valid_key);
      // Keep current_key_ in sync.
      current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
      key_ = current_key_.GetInternalKey();
      ikey_.user_key = current_key_.GetUserKey();
      valid_ = true;
    } else {
      // We consumed all pinned merge operands, release pinned iterators
      pinned_iters_mgr_.ReleasePinnedData();
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

  if (valid_) {
    // Record that we've outputted a record for the current key.
    has_outputted_key_ = true;
  }

  PrepareOutput();
}

void CompactionIterator::InvokeFilterIfNeeded(bool* need_skip,
                                              Slice* skip_until) {
  if (compaction_filter_ != nullptr &&
      (ikey_.type == kTypeValue || ikey_.type == kTypeBlobIndex) &&
      (visible_at_tip_ || ikey_.sequence > latest_snapshot_ ||
       ignore_snapshots_)) {
    // If the user has specified a compaction filter and the sequence
    // number is greater than any external snapshot, then invoke the
    // filter. If the return value of the compaction filter is true,
    // replace the entry with a deletion marker.
    CompactionFilter::Decision filter;
    compaction_filter_value_.clear();
    compaction_filter_skip_until_.Clear();
    CompactionFilter::ValueType value_type =
        ikey_.type == kTypeValue ? CompactionFilter::ValueType::kValue
                                 : CompactionFilter::ValueType::kBlobIndex;
    {
      StopWatchNano timer(env_, true);
      filter = compaction_filter_->FilterV2(
          compaction_->level(), ikey_.user_key, value_type, value_,
          &compaction_filter_value_, compaction_filter_skip_until_.rep());
      iter_stats_.total_filter_time +=
          env_ != nullptr ? timer.ElapsedNanos() : 0;
    }

    if (filter == CompactionFilter::Decision::kRemoveAndSkipUntil &&
        cmp_->Compare(*compaction_filter_skip_until_.rep(), ikey_.user_key) <=
            0) {
      // Can't skip to a key smaller than the current one.
      // Keep the key as per FilterV2 documentation.
      filter = CompactionFilter::Decision::kKeep;
    }

    if (filter == CompactionFilter::Decision::kRemove) {
      // convert the current key to a delete; key_ is pointing into
      // current_key_ at this point, so updating current_key_ updates key()
      ikey_.type = kTypeDeletion;
      current_key_.UpdateInternalKey(ikey_.sequence, kTypeDeletion);
      // no value associated with delete
      value_.clear();
      iter_stats_.num_record_drop_user++;
    } else if (filter == CompactionFilter::Decision::kChangeValue) {
      value_ = compaction_filter_value_;
    } else if (filter == CompactionFilter::Decision::kRemoveAndSkipUntil) {
      *need_skip = true;
      compaction_filter_skip_until_.ConvertFromUserKey(kMaxSequenceNumber,
                                                       kValueTypeForSeek);
      *skip_until = compaction_filter_skip_until_.Encode();
    }
  }
}

void CompactionIterator::NextFromInput() {
  at_next_ = false;
  valid_ = false;

  while (!valid_ && input_->Valid() && !IsShuttingDown()) {
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
      key_ = current_key_.SetInternalKey(key_);
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

    // If need_skip is true, we should seek the input iterator
    // to internal key skip_until and continue from there.
    bool need_skip = false;
    // Points either into compaction_filter_skip_until_ or into
    // merge_helper_->compaction_filter_skip_until_.
    Slice skip_until;

    // Check whether the user key changed. After this if statement current_key_
    // is a copy of the current input key (maybe converted to a delete by the
    // compaction filter). ikey_.user_key is pointing to the copy.
    if (!has_current_user_key_ ||
        !cmp_->Equal(ikey_.user_key, current_user_key_)) {
      // First occurrence of this user key
      // Copy key for output
      key_ = current_key_.SetInternalKey(key_, &ikey_);
      current_user_key_ = ikey_.user_key;
      has_current_user_key_ = true;
      has_outputted_key_ = false;
      current_user_key_sequence_ = kMaxSequenceNumber;
      current_user_key_snapshot_ = 0;
      current_key_committed_ =
          (snapshot_checker_ == nullptr ||
           snapshot_checker_->IsInSnapshot(ikey_.sequence, kMaxSequenceNumber));

#ifndef ROCKSDB_LITE
      if (compaction_listener_) {
        compaction_listener_->OnCompaction(compaction_->level(), ikey_.user_key,
                                           fromInternalValueType(ikey_.type),
                                           value_, ikey_.sequence, true);
      }
#endif  // !ROCKSDB_LITE

      // Apply the compaction filter to the first committed version of the user
      // key.
      if (current_key_committed_) {
        InvokeFilterIfNeeded(&need_skip, &skip_until);
      }
    } else {
#ifndef ROCKSDB_LITE
      if (compaction_listener_) {
        compaction_listener_->OnCompaction(compaction_->level(), ikey_.user_key,
                                           fromInternalValueType(ikey_.type),
                                           value_, ikey_.sequence, false);
      }
#endif  // ROCKSDB_LITE

      // Update the current key to reflect the new sequence number/type without
      // copying the user key.
      // TODO(rven): Compaction filter does not process keys in this path
      // Need to have the compaction filter process multiple versions
      // if we have versions on both sides of a snapshot
      current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
      key_ = current_key_.GetInternalKey();
      ikey_.user_key = current_key_.GetUserKey();

      // Note that newer version of a key is ordered before older versions. If a
      // newer version of a key is committed, so as the older version. No need
      // to query snapshot_checker_ in that case.
      if (UNLIKELY(!current_key_committed_)) {
        assert(snapshot_checker_ != nullptr);
        current_key_committed_ =
            snapshot_checker_->IsInSnapshot(ikey_.sequence, kMaxSequenceNumber);
        // Apply the compaction filter to the first committed version of the
        // user key.
        if (current_key_committed_) {
          InvokeFilterIfNeeded(&need_skip, &skip_until);
        }
      }
    }

    if (UNLIKELY(!current_key_committed_)) {
      assert(snapshot_checker_ != nullptr);
      valid_ = true;
      break;
    }

    // If there are no snapshots, then this kv affect visibility at tip.
    // Otherwise, search though all existing snapshots to find the earliest
    // snapshot that is affected by this kv.
    SequenceNumber last_sequence __attribute__((__unused__));
    last_sequence = current_user_key_sequence_;
    current_user_key_sequence_ = ikey_.sequence;
    SequenceNumber last_snapshot = current_user_key_snapshot_;
    SequenceNumber prev_snapshot = 0;  // 0 means no previous snapshot
    current_user_key_snapshot_ =
        visible_at_tip_
            ? earliest_snapshot_
            : findEarliestVisibleSnapshot(ikey_.sequence, &prev_snapshot);

    if (need_skip) {
      // This case is handled below.
    } else if (clear_and_output_next_key_) {
      // In the previous iteration we encountered a single delete that we could
      // not compact out.  We will keep this Put, but can drop it's data.
      // (See Optimization 3, below.)
      assert(ikey_.type == kTypeValue);
      assert(current_user_key_snapshot_ == last_snapshot);

      value_.clear();
      valid_ = true;
      clear_and_output_next_key_ = false;
    } else if (ikey_.type == kTypeSingleDeletion) {
      // We can compact out a SingleDelete if:
      // 1) We encounter the corresponding PUT -OR- we know that this key
      //    doesn't appear past this output level
      // =AND=
      // 2) We've already returned a record in this snapshot -OR-
      //    there are no earlier earliest_write_conflict_snapshot.
      //
      // Rule 1 is needed for SingleDelete correctness.  Rule 2 is needed to
      // allow Transactions to do write-conflict checking (if we compacted away
      // all keys, then we wouldn't know that a write happened in this
      // snapshot).  If there is no earlier snapshot, then we know that there
      // are no active transactions that need to know about any writes.
      //
      // Optimization 3:
      // If we encounter a SingleDelete followed by a PUT and Rule 2 is NOT
      // true, then we must output a SingleDelete.  In this case, we will decide
      // to also output the PUT.  While we are compacting less by outputting the
      // PUT now, hopefully this will lead to better compaction in the future
      // when Rule 2 is later true (Ie, We are hoping we can later compact out
      // both the SingleDelete and the Put, while we couldn't if we only
      // outputted the SingleDelete now).
      // In this case, we can save space by removing the PUT's value as it will
      // never be read.
      //
      // Deletes and Merges are not supported on the same key that has a
      // SingleDelete as it is not possible to correctly do any partial
      // compaction of such a combination of operations.  The result of mixing
      // those operations for a given key is documented as being undefined.  So
      // we can choose how to handle such a combinations of operations.  We will
      // try to compact out as much as we can in these cases.
      // We will report counts on these anomalous cases.

      // The easiest way to process a SingleDelete during iteration is to peek
      // ahead at the next key.
      ParsedInternalKey next_ikey;
      input_->Next();

      // Check whether the next key exists, is not corrupt, and is the same key
      // as the single delete.
      if (input_->Valid() && ParseInternalKey(input_->key(), &next_ikey) &&
          cmp_->Equal(ikey_.user_key, next_ikey.user_key)) {
        // Check whether the next key belongs to the same snapshot as the
        // SingleDelete.
        if (prev_snapshot == 0 || next_ikey.sequence > prev_snapshot) {
          if (next_ikey.type == kTypeSingleDeletion) {
            // We encountered two SingleDeletes in a row.  This could be due to
            // unexpected user input.
            // Skip the first SingleDelete and let the next iteration decide how
            // to handle the second SingleDelete

            // First SingleDelete has been skipped since we already called
            // input_->Next().
            ++iter_stats_.num_record_drop_obsolete;
            ++iter_stats_.num_single_del_mismatch;
          } else if ((ikey_.sequence <= earliest_write_conflict_snapshot_) ||
                     has_outputted_key_) {
            // Found a matching value, we can drop the single delete and the
            // value.  It is safe to drop both records since we've already
            // outputted a key in this snapshot, or there is no earlier
            // snapshot (Rule 2 above).

            // Note: it doesn't matter whether the second key is a Put or if it
            // is an unexpected Merge or Delete.  We will compact it out
            // either way. We will maintain counts of how many mismatches
            // happened
            if (next_ikey.type != kTypeValue) {
              ++iter_stats_.num_single_del_mismatch;
            }

            ++iter_stats_.num_record_drop_hidden;
            ++iter_stats_.num_record_drop_obsolete;
            // Already called input_->Next() once.  Call it a second time to
            // skip past the second key.
            input_->Next();
          } else {
            // Found a matching value, but we cannot drop both keys since
            // there is an earlier snapshot and we need to leave behind a record
            // to know that a write happened in this snapshot (Rule 2 above).
            // Clear the value and output the SingleDelete. (The value will be
            // outputted on the next iteration.)

            // Setting valid_ to true will output the current SingleDelete
            valid_ = true;

            // Set up the Put to be outputted in the next iteration.
            // (Optimization 3).
            clear_and_output_next_key_ = true;
          }
        } else {
          // We hit the next snapshot without hitting a put, so the iterator
          // returns the single delete.
          valid_ = true;
        }
      } else {
        // We are at the end of the input, could not parse the next key, or hit
        // a different key. The iterator returns the single delete if the key
        // possibly exists beyond the current output level.  We set
        // has_current_user_key to false so that if the iterator is at the next
        // key, we do not compare it again against the previous key at the next
        // iteration. If the next key is corrupt, we return before the
        // comparison, so the value of has_current_user_key does not matter.
        has_current_user_key_ = false;
        if (compaction_ != nullptr && ikey_.sequence <= earliest_snapshot_ &&
            compaction_->KeyNotExistsBeyondOutputLevel(ikey_.user_key,
                                                       &level_ptrs_)) {
          // Key doesn't exist outside of this range.
          // Can compact out this SingleDelete.
          ++iter_stats_.num_record_drop_obsolete;
          ++iter_stats_.num_single_del_fallthru;
          if (!bottommost_level_) {
            ++iter_stats_.num_optimized_del_drop_obsolete;
          }
        } else {
          // Output SingleDelete
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
      // TODO(noetzli): why not > ?
      //
      // Note: Dropping this key will not affect TransactionDB write-conflict
      // checking since there has already been a record returned for this key
      // in this snapshot.
      assert(last_sequence >= current_user_key_sequence_);
      ++iter_stats_.num_record_drop_hidden;  // (A)
      input_->Next();
    } else if (compaction_ != nullptr && ikey_.type == kTypeDeletion &&
               ikey_.sequence <= earliest_snapshot_ &&
               ikeyNotNeededForIncrementalSnapshot() &&
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
      //
      // Note:  Dropping this Delete will not affect TransactionDB
      // write-conflict checking since it is earlier than any snapshot.
      ++iter_stats_.num_record_drop_obsolete;
      if (!bottommost_level_) {
        ++iter_stats_.num_optimized_del_drop_obsolete;
      }
      input_->Next();
    } else if (ikey_.type == kTypeMerge) {
      if (!merge_helper_->HasOperator()) {
        status_ = Status::InvalidArgument(
            "merge_operator is not properly initialized.");
        return;
      }

      pinned_iters_mgr_.StartPinning();
      // We know the merge type entry is not hidden, otherwise we would
      // have hit (A)
      // We encapsulate the merge related state machine in a different
      // object to minimize change to the existing flow.
      Status s = merge_helper_->MergeUntil(input_, range_del_agg_,
                                           prev_snapshot, bottommost_level_);
      merge_out_iter_.SeekToFirst();

      if (!s.ok() && !s.IsMergeInProgress()) {
        status_ = s;
        return;
      } else if (merge_out_iter_.Valid()) {
        // NOTE: key, value, and ikey_ refer to old entries.
        //       These will be correctly set below.
        key_ = merge_out_iter_.key();
        value_ = merge_out_iter_.value();
        bool valid_key __attribute__((__unused__));
        valid_key = ParseInternalKey(key_, &ikey_);
        // MergeUntil stops when it encounters a corrupt key and does not
        // include them in the result, so we expect the keys here to valid.
        assert(valid_key);
        // Keep current_key_ in sync.
        current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
        key_ = current_key_.GetInternalKey();
        ikey_.user_key = current_key_.GetUserKey();
        valid_ = true;
      } else {
        // all merge operands were filtered out. reset the user key, since the
        // batch consumed by the merge operator should not shadow any keys
        // coming after the merges
        has_current_user_key_ = false;
        pinned_iters_mgr_.ReleasePinnedData();

        if (merge_helper_->FilteredUntil(&skip_until)) {
          need_skip = true;
        }
      }
    } else {
      // 1. new user key -OR-
      // 2. different snapshot stripe
      bool should_delete = range_del_agg_->ShouldDelete(
          key_, RangeDelAggregator::RangePositioningMode::kForwardTraversal);
      if (should_delete) {
        ++iter_stats_.num_record_drop_hidden;
        ++iter_stats_.num_record_drop_range_del;
        input_->Next();
      } else {
        valid_ = true;
      }
    }

    if (need_skip) {
      input_->Seek(skip_until);
    }
  }

  if (!valid_ && IsShuttingDown()) {
    status_ = Status::ShutdownInProgress();
  }
}

void CompactionIterator::PrepareOutput() {
  // Zeroing out the sequence number leads to better compression.
  // If this is the bottommost level (no files in lower levels)
  // and the earliest snapshot is larger than this seqno
  // and the userkey differs from the last userkey in compaction
  // then we can squash the seqno to zero.

  // This is safe for TransactionDB write-conflict checking since transactions
  // only care about sequence number larger than any active snapshots.
  if ((compaction_ != nullptr &&
      !compaction_->allow_ingest_behind()) &&
      ikeyNotNeededForIncrementalSnapshot() &&
      bottommost_level_ && valid_ && ikey_.sequence <= earliest_snapshot_ &&
      (snapshot_checker_ == nullptr || LIKELY(snapshot_checker_->IsInSnapshot(
        ikey_.sequence, earliest_snapshot_))) &&
      ikey_.type != kTypeMerge &&
      !cmp_->Equal(compaction_->GetLargestUserKey(), ikey_.user_key)) {
    assert(ikey_.type != kTypeDeletion && ikey_.type != kTypeSingleDeletion);
    ikey_.sequence = 0;
    current_key_.UpdateInternalKey(0, ikey_.type);
  }
}

inline SequenceNumber CompactionIterator::findEarliestVisibleSnapshot(
    SequenceNumber in, SequenceNumber* prev_snapshot) {
  assert(snapshots_->size());
  SequenceNumber prev = kMaxSequenceNumber;
  for (const auto cur : *snapshots_) {
    assert(prev == kMaxSequenceNumber || prev <= cur);
    if (cur >= in && (snapshot_checker_ == nullptr ||
                      snapshot_checker_->IsInSnapshot(in, cur))) {
      *prev_snapshot = prev == kMaxSequenceNumber ? 0 : prev;
      return cur;
    }
    prev = cur;
    assert(prev < kMaxSequenceNumber);
  }
  *prev_snapshot = prev;
  return kMaxSequenceNumber;
}

// used in 2 places - prevents deletion markers to be dropped if they may be
// needed and disables seqnum zero-out in PrepareOutput for recent keys.
inline bool CompactionIterator::ikeyNotNeededForIncrementalSnapshot() {
  return (!compaction_->preserve_deletes()) ||
         (ikey_.sequence < preserve_deletes_seqnum_);
}

}  // namespace rocksdb
