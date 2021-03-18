//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/compaction/compaction_iterator.h"

#include <iterator>
#include <limits>

#include "db/blob/blob_file_builder.h"
#include "db/blob/blob_index.h"
#include "db/snapshot_checker.h"
#include "port/likely.h"
#include "rocksdb/listener.h"
#include "table/internal_iterator.h"
#include "test_util/sync_point.h"

#define DEFINITELY_IN_SNAPSHOT(seq, snapshot)                       \
  ((seq) <= (snapshot) &&                                           \
   (snapshot_checker_ == nullptr ||                                 \
    LIKELY(snapshot_checker_->CheckInSnapshot((seq), (snapshot)) == \
           SnapshotCheckerResult::kInSnapshot)))

#define DEFINITELY_NOT_IN_SNAPSHOT(seq, snapshot)                     \
  ((seq) > (snapshot) ||                                              \
   (snapshot_checker_ != nullptr &&                                   \
    UNLIKELY(snapshot_checker_->CheckInSnapshot((seq), (snapshot)) == \
             SnapshotCheckerResult::kNotInSnapshot)))

#define IN_EARLIEST_SNAPSHOT(seq) \
  ((seq) <= earliest_snapshot_ && \
   (snapshot_checker_ == nullptr || LIKELY(IsInEarliestSnapshot(seq))))

namespace ROCKSDB_NAMESPACE {

CompactionIterator::CompactionIterator(
    InternalIterator* input, const Comparator* cmp, MergeHelper* merge_helper,
    SequenceNumber last_sequence, std::vector<SequenceNumber>* snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    const SnapshotChecker* snapshot_checker, Env* env,
    bool report_detailed_time, bool expect_valid_internal_key,
    CompactionRangeDelAggregator* range_del_agg,
    BlobFileBuilder* blob_file_builder, bool allow_data_in_errors,
    const Compaction* compaction, const CompactionFilter* compaction_filter,
    const std::atomic<bool>* shutting_down,
    const SequenceNumber preserve_deletes_seqnum,
    const std::atomic<int>* manual_compaction_paused,
    const std::shared_ptr<Logger> info_log,
    const std::string* full_history_ts_low)
    : CompactionIterator(
          input, cmp, merge_helper, last_sequence, snapshots,
          earliest_write_conflict_snapshot, snapshot_checker, env,
          report_detailed_time, expect_valid_internal_key, range_del_agg,
          blob_file_builder, allow_data_in_errors,
          std::unique_ptr<CompactionProxy>(
              compaction ? new RealCompaction(compaction) : nullptr),
          compaction_filter, shutting_down, preserve_deletes_seqnum,
          manual_compaction_paused, info_log, full_history_ts_low) {}

CompactionIterator::CompactionIterator(
    InternalIterator* input, const Comparator* cmp, MergeHelper* merge_helper,
    SequenceNumber /*last_sequence*/, std::vector<SequenceNumber>* snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    const SnapshotChecker* snapshot_checker, Env* env,
    bool report_detailed_time, bool expect_valid_internal_key,
    CompactionRangeDelAggregator* range_del_agg,
    BlobFileBuilder* blob_file_builder, bool allow_data_in_errors,
    std::unique_ptr<CompactionProxy> compaction,
    const CompactionFilter* compaction_filter,
    const std::atomic<bool>* shutting_down,
    const SequenceNumber preserve_deletes_seqnum,
    const std::atomic<int>* manual_compaction_paused,
    const std::shared_ptr<Logger> info_log,
    const std::string* full_history_ts_low)
    : input_(input),
      cmp_(cmp),
      merge_helper_(merge_helper),
      snapshots_(snapshots),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      snapshot_checker_(snapshot_checker),
      env_(env),
      clock_(env_->GetSystemClock().get()),
      report_detailed_time_(report_detailed_time),
      expect_valid_internal_key_(expect_valid_internal_key),
      range_del_agg_(range_del_agg),
      blob_file_builder_(blob_file_builder),
      compaction_(std::move(compaction)),
      compaction_filter_(compaction_filter),
      shutting_down_(shutting_down),
      manual_compaction_paused_(manual_compaction_paused),
      preserve_deletes_seqnum_(preserve_deletes_seqnum),
      info_log_(info_log),
      allow_data_in_errors_(allow_data_in_errors),
      timestamp_size_(cmp_ ? cmp_->timestamp_size() : 0),
      full_history_ts_low_(full_history_ts_low),
      current_user_key_sequence_(0),
      current_user_key_snapshot_(0),
      merge_out_iter_(merge_helper_),
      blob_garbage_collection_cutoff_file_number_(
          ComputeBlobGarbageCollectionCutoffFileNumber(compaction_.get())),
      current_key_committed_(false),
      cmp_with_history_ts_low_(0) {
  assert(compaction_filter_ == nullptr || compaction_ != nullptr);
  assert(snapshots_ != nullptr);
  bottommost_level_ = compaction_ == nullptr
                          ? false
                          : compaction_->bottommost_level() &&
                                !compaction_->allow_ingest_behind();
  if (compaction_ != nullptr) {
    level_ptrs_ = std::vector<size_t>(compaction_->number_levels(), 0);
  }
  if (snapshots_->size() == 0) {
    // optimize for fast path if there are no snapshots
    visible_at_tip_ = true;
    earliest_snapshot_iter_ = snapshots_->end();
    earliest_snapshot_ = kMaxSequenceNumber;
    latest_snapshot_ = 0;
  } else {
    visible_at_tip_ = false;
    earliest_snapshot_iter_ = snapshots_->begin();
    earliest_snapshot_ = snapshots_->at(0);
    latest_snapshot_ = snapshots_->back();
  }
#ifndef NDEBUG
  // findEarliestVisibleSnapshot assumes this ordering.
  for (size_t i = 1; i < snapshots_->size(); ++i) {
    assert(snapshots_->at(i - 1) < snapshots_->at(i));
  }
  assert(timestamp_size_ == 0 || !full_history_ts_low_ ||
         timestamp_size_ == full_history_ts_low_->size());
#endif
  input_->SetPinnedItersMgr(&pinned_iters_mgr_);
  TEST_SYNC_POINT_CALLBACK("CompactionIterator:AfterInit", compaction_.get());
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
      Status s = ParseInternalKey(key_, &ikey_, allow_data_in_errors_);
      // MergeUntil stops when it encounters a corrupt key and does not
      // include them in the result, so we expect the keys here to be valid.
      assert(s.ok());
      if (!s.ok()) {
        ROCKS_LOG_FATAL(info_log_, "Invalid key in compaction. %s",
                        s.getState());
      }

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

bool CompactionIterator::InvokeFilterIfNeeded(bool* need_skip,
                                              Slice* skip_until) {
  if (!compaction_filter_ ||
      (ikey_.type != kTypeValue && ikey_.type != kTypeBlobIndex)) {
    return true;
  }
  bool error = false;
  // If the user has specified a compaction filter and the sequence
  // number is greater than any external snapshot, then invoke the
  // filter. If the return value of the compaction filter is true,
  // replace the entry with a deletion marker.
  CompactionFilter::Decision filter = CompactionFilter::Decision::kUndetermined;
  compaction_filter_value_.clear();
  compaction_filter_skip_until_.Clear();
  CompactionFilter::ValueType value_type =
      ikey_.type == kTypeValue ? CompactionFilter::ValueType::kValue
                               : CompactionFilter::ValueType::kBlobIndex;
  // Hack: pass internal key to BlobIndexCompactionFilter since it needs
  // to get sequence number.
  assert(compaction_filter_);
  Slice& filter_key =
      (ikey_.type == kTypeValue ||
       !compaction_filter_->IsStackedBlobDbInternalCompactionFilter())
          ? ikey_.user_key
          : key_;
  {
    StopWatchNano timer(clock_, report_detailed_time_);
    if (kTypeBlobIndex == ikey_.type) {
      blob_value_.Reset();
      filter = compaction_filter_->FilterBlobByKey(
          compaction_->level(), filter_key, &compaction_filter_value_,
          compaction_filter_skip_until_.rep());
      if (CompactionFilter::Decision::kUndetermined == filter &&
          !compaction_filter_->IsStackedBlobDbInternalCompactionFilter()) {
        // For integrated BlobDB impl, CompactionIterator reads blob value.
        // For Stacked BlobDB impl, the corresponding CompactionFilter's
        // FilterV2 method should read the blob value.
        BlobIndex blob_index;
        Status s = blob_index.DecodeFrom(value_);
        if (!s.ok()) {
          status_ = s;
          valid_ = false;
          return false;
        }
        if (blob_index.HasTTL() || blob_index.IsInlined()) {
          status_ = Status::Corruption("Unexpected TTL/inlined blob index");
          valid_ = false;
          return false;
        }
        const Version* const version = compaction_->input_version();
        assert(version);

        uint64_t bytes_read = 0;
        s = version->GetBlob(ReadOptions(), ikey_.user_key, blob_index,
                             &blob_value_, &bytes_read);
        if (!s.ok()) {
          status_ = s;
          valid_ = false;
          return false;
        }

        ++iter_stats_.num_blobs_read;
        iter_stats_.total_blob_bytes_read += bytes_read;

        value_type = CompactionFilter::ValueType::kValue;
      }
    }
    if (CompactionFilter::Decision::kUndetermined == filter) {
      filter = compaction_filter_->FilterV2(
          compaction_->level(), filter_key, value_type,
          blob_value_.empty() ? value_ : blob_value_, &compaction_filter_value_,
          compaction_filter_skip_until_.rep());
    }
    iter_stats_.total_filter_time +=
        env_ != nullptr && report_detailed_time_ ? timer.ElapsedNanos() : 0;
  }

  if (CompactionFilter::Decision::kUndetermined == filter) {
    // Should not reach here, since FilterV2 should never return kUndetermined.
    status_ =
        Status::NotSupported("FilterV2() should never return kUndetermined");
    valid_ = false;
    return false;
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
    if (ikey_.type == kTypeBlobIndex) {
      // value transfer from blob file to inlined data
      ikey_.type = kTypeValue;
      current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
    }
    value_ = compaction_filter_value_;
  } else if (filter == CompactionFilter::Decision::kRemoveAndSkipUntil) {
    *need_skip = true;
    compaction_filter_skip_until_.ConvertFromUserKey(kMaxSequenceNumber,
                                                     kValueTypeForSeek);
    *skip_until = compaction_filter_skip_until_.Encode();
  } else if (filter == CompactionFilter::Decision::kChangeBlobIndex) {
    // Only the StackableDB-based BlobDB impl's compaction filter should return
    // kChangeBlobIndex. Decision about rewriting blob and changing blob index
    // in the integrated BlobDB impl is made in subsequent call to
    // PrepareOutput() and its callees.
    if (!compaction_filter_->IsStackedBlobDbInternalCompactionFilter()) {
      status_ = Status::NotSupported(
          "Only stacked BlobDB's internal compaction filter can return "
          "kChangeBlobIndex.");
      valid_ = false;
      return false;
    }
    if (ikey_.type == kTypeValue) {
      // value transfer from inlined data to blob file
      ikey_.type = kTypeBlobIndex;
      current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
    }
    value_ = compaction_filter_value_;
  } else if (filter == CompactionFilter::Decision::kIOError) {
    if (!compaction_filter_->IsStackedBlobDbInternalCompactionFilter()) {
      status_ = Status::NotSupported(
          "CompactionFilter for integrated BlobDB should not return kIOError");
      valid_ = false;
      return false;
    }
    status_ = Status::IOError("Failed to access blob during compaction filter");
    error = true;
  }
  return !error;
}

void CompactionIterator::NextFromInput() {
  at_next_ = false;
  valid_ = false;

  while (!valid_ && input_->Valid() && !IsPausingManualCompaction() &&
         !IsShuttingDown()) {
    key_ = input_->key();
    value_ = input_->value();
    iter_stats_.num_input_records++;

    Status pik_status = ParseInternalKey(key_, &ikey_, allow_data_in_errors_);
    if (!pik_status.ok()) {
      iter_stats_.num_input_corrupt_records++;

      // If `expect_valid_internal_key_` is false, return the corrupted key
      // and let the caller decide what to do with it.
      if (expect_valid_internal_key_) {
        status_ = pik_status;
        return;
      }
      key_ = current_key_.SetInternalKey(key_);
      has_current_user_key_ = false;
      current_user_key_sequence_ = kMaxSequenceNumber;
      current_user_key_snapshot_ = 0;
      valid_ = true;
      break;
    }
    TEST_SYNC_POINT_CALLBACK("CompactionIterator:ProcessKV", &ikey_);

    // Update input statistics
    if (ikey_.type == kTypeDeletion || ikey_.type == kTypeSingleDeletion ||
        ikey_.type == kTypeDeletionWithTimestamp) {
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

    bool user_key_equal_without_ts = false;
    int cmp_ts = 0;
    if (has_current_user_key_) {
      user_key_equal_without_ts =
          cmp_->EqualWithoutTimestamp(ikey_.user_key, current_user_key_);
      // if timestamp_size_ > 0, then curr_ts_ has been initialized by a
      // previous key.
      cmp_ts = timestamp_size_ ? cmp_->CompareTimestamp(
                                     ExtractTimestampFromUserKey(
                                         ikey_.user_key, timestamp_size_),
                                     curr_ts_)
                               : 0;
    }

    // Check whether the user key changed. After this if statement current_key_
    // is a copy of the current input key (maybe converted to a delete by the
    // compaction filter). ikey_.user_key is pointing to the copy.
    if (!has_current_user_key_ || !user_key_equal_without_ts || cmp_ts != 0) {
      // First occurrence of this user key
      // Copy key for output
      key_ = current_key_.SetInternalKey(key_, &ikey_);

      // If timestamp_size_ > 0, then copy from ikey_ to curr_ts_ for the use
      // in next iteration to compare with the timestamp of next key.
      UpdateTimestampAndCompareWithFullHistoryLow();

      // If
      // (1) !has_current_user_key_, OR
      // (2) timestamp is disabled, OR
      // (3) all history will be preserved, OR
      // (4) user key (excluding timestamp) is different from previous key, OR
      // (5) timestamp is NO older than *full_history_ts_low_
      // then current_user_key_ must be treated as a different user key.
      // This means, if a user key (excluding ts) is the same as the previous
      // user key, and its ts is older than *full_history_ts_low_, then we
      // consider this key for GC, e.g. it may be dropped if certain conditions
      // match.
      if (!has_current_user_key_ || !timestamp_size_ || !full_history_ts_low_ ||
          !user_key_equal_without_ts || cmp_with_history_ts_low_ >= 0) {
        // Initialize for future comparison for rule (A) and etc.
        current_user_key_sequence_ = kMaxSequenceNumber;
        current_user_key_snapshot_ = 0;
        has_current_user_key_ = true;
      }
      current_user_key_ = ikey_.user_key;

      has_outputted_key_ = false;

      current_key_committed_ = KeyCommitted(ikey_.sequence);

      // Apply the compaction filter to the first committed version of the user
      // key.
      if (current_key_committed_ &&
          !InvokeFilterIfNeeded(&need_skip, &skip_until)) {
        break;
      }
    } else {
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
        current_key_committed_ = KeyCommitted(ikey_.sequence);
        // Apply the compaction filter to the first committed version of the
        // user key.
        if (current_key_committed_ &&
            !InvokeFilterIfNeeded(&need_skip, &skip_until)) {
          break;
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
    SequenceNumber last_sequence = current_user_key_sequence_;
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
      assert(ikey_.type == kTypeValue || ikey_.type == kTypeBlobIndex);
      if (ikey_.type != kTypeValue && ikey_.type != kTypeBlobIndex) {
        ROCKS_LOG_FATAL(info_log_,
                        "Unexpected key type %d for compaction output",
                        ikey_.type);
      }
      assert(current_user_key_snapshot_ == last_snapshot);
      if (current_user_key_snapshot_ != last_snapshot) {
        ROCKS_LOG_FATAL(info_log_,
                        "current_user_key_snapshot_ (%" PRIu64
                        ") != last_snapshot (%" PRIu64 ")",
                        current_user_key_snapshot_, last_snapshot);
      }

      if (ikey_.type == kTypeBlobIndex) {
        ikey_.type = kTypeValue;
        current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
      }

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
      if (input_->Valid() &&
          ParseInternalKey(input_->key(), &next_ikey, allow_data_in_errors_)
              .ok() &&
          cmp_->Equal(ikey_.user_key, next_ikey.user_key)) {
        // Check whether the next key belongs to the same snapshot as the
        // SingleDelete.
        if (prev_snapshot == 0 ||
            DEFINITELY_NOT_IN_SNAPSHOT(next_ikey.sequence, prev_snapshot)) {
          if (next_ikey.type == kTypeSingleDeletion) {
            // We encountered two SingleDeletes in a row.  This could be due to
            // unexpected user input.
            // Skip the first SingleDelete and let the next iteration decide how
            // to handle the second SingleDelete

            // First SingleDelete has been skipped since we already called
            // input_->Next().
            ++iter_stats_.num_record_drop_obsolete;
            ++iter_stats_.num_single_del_mismatch;
          } else if (has_outputted_key_ ||
                     DEFINITELY_IN_SNAPSHOT(
                         ikey_.sequence, earliest_write_conflict_snapshot_)) {
            // Found a matching value, we can drop the single delete and the
            // value.  It is safe to drop both records since we've already
            // outputted a key in this snapshot, or there is no earlier
            // snapshot (Rule 2 above).

            // Note: it doesn't matter whether the second key is a Put or if it
            // is an unexpected Merge or Delete.  We will compact it out
            // either way. We will maintain counts of how many mismatches
            // happened
            if (next_ikey.type != kTypeValue &&
                next_ikey.type != kTypeBlobIndex) {
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
        if (compaction_ != nullptr && IN_EARLIEST_SNAPSHOT(ikey_.sequence) &&
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
    } else if (last_snapshot == current_user_key_snapshot_ ||
               (last_snapshot > 0 &&
                last_snapshot < current_user_key_snapshot_)) {
      // If the earliest snapshot is which this key is visible in
      // is the same as the visibility of a previous instance of the
      // same key, then this kv is not visible in any snapshot.
      // Hidden by an newer entry for same user key
      //
      // Note: Dropping this key will not affect TransactionDB write-conflict
      // checking since there has already been a record returned for this key
      // in this snapshot.
      assert(last_sequence >= current_user_key_sequence_);
      if (last_sequence < current_user_key_sequence_) {
        ROCKS_LOG_FATAL(info_log_,
                        "last_sequence (%" PRIu64
                        ") < current_user_key_sequence_ (%" PRIu64 ")",
                        last_sequence, current_user_key_sequence_);
      }

      ++iter_stats_.num_record_drop_hidden;  // rule (A)
      input_->Next();
    } else if (compaction_ != nullptr &&
               (ikey_.type == kTypeDeletion ||
                (ikey_.type == kTypeDeletionWithTimestamp &&
                 cmp_with_history_ts_low_ < 0)) &&
               IN_EARLIEST_SNAPSHOT(ikey_.sequence) &&
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
      //
      // It seems that we can also drop deletion later than earliest snapshot
      // given that:
      // (1) The deletion is earlier than earliest_write_conflict_snapshot, and
      // (2) No value exist earlier than the deletion.
      //
      // Note also that a deletion marker of type kTypeDeletionWithTimestamp
      // will be treated as a different user key unless the timestamp is older
      // than *full_history_ts_low_.
      ++iter_stats_.num_record_drop_obsolete;
      if (!bottommost_level_) {
        ++iter_stats_.num_optimized_del_drop_obsolete;
      }
      input_->Next();
    } else if ((ikey_.type == kTypeDeletion ||
                (ikey_.type == kTypeDeletionWithTimestamp &&
                 cmp_with_history_ts_low_ < 0)) &&
               bottommost_level_ && ikeyNotNeededForIncrementalSnapshot()) {
      // Handle the case where we have a delete key at the bottom most level
      // We can skip outputting the key iff there are no subsequent puts for this
      // key
      assert(!compaction_ || compaction_->KeyNotExistsBeyondOutputLevel(
                                 ikey_.user_key, &level_ptrs_));
      ParsedInternalKey next_ikey;
      input_->Next();
      // Skip over all versions of this key that happen to occur in the same
      // snapshot range as the delete.
      //
      // Note that a deletion marker of type kTypeDeletionWithTimestamp will be
      // considered to have a different user key unless the timestamp is older
      // than *full_history_ts_low_.
      while (!IsPausingManualCompaction() && !IsShuttingDown() &&
             input_->Valid() &&
             (ParseInternalKey(input_->key(), &next_ikey, allow_data_in_errors_)
                  .ok()) &&
             cmp_->EqualWithoutTimestamp(ikey_.user_key, next_ikey.user_key) &&
             (prev_snapshot == 0 ||
              DEFINITELY_NOT_IN_SNAPSHOT(next_ikey.sequence, prev_snapshot))) {
        input_->Next();
      }
      // If you find you still need to output a row with this key, we need to output the
      // delete too
      if (input_->Valid() &&
          (ParseInternalKey(input_->key(), &next_ikey, allow_data_in_errors_)
               .ok()) &&
          cmp_->EqualWithoutTimestamp(ikey_.user_key, next_ikey.user_key)) {
        valid_ = true;
        at_next_ = true;
      }
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
      Status s =
          merge_helper_->MergeUntil(input_, range_del_agg_, prev_snapshot,
                                    bottommost_level_, allow_data_in_errors_);
      merge_out_iter_.SeekToFirst();

      if (!s.ok() && !s.IsMergeInProgress()) {
        status_ = s;
        return;
      } else if (merge_out_iter_.Valid()) {
        // NOTE: key, value, and ikey_ refer to old entries.
        //       These will be correctly set below.
        key_ = merge_out_iter_.key();
        value_ = merge_out_iter_.value();
        pik_status = ParseInternalKey(key_, &ikey_, allow_data_in_errors_);
        // MergeUntil stops when it encounters a corrupt key and does not
        // include them in the result, so we expect the keys here to valid.
        assert(pik_status.ok());
        if (!pik_status.ok()) {
          ROCKS_LOG_FATAL(info_log_, "Invalid key in compaction. %s",
                          pik_status.getState());
        }
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
          key_, RangeDelPositioningMode::kForwardTraversal);
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

  if (IsPausingManualCompaction()) {
    status_ = Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }
}

bool CompactionIterator::ExtractLargeValueIfNeededImpl() {
  if (!blob_file_builder_) {
    return false;
  }

  blob_index_.clear();
  const Status s = blob_file_builder_->Add(user_key(), value_, &blob_index_);

  if (!s.ok()) {
    status_ = s;
    valid_ = false;

    return false;
  }

  if (blob_index_.empty()) {
    return false;
  }

  value_ = blob_index_;

  return true;
}

void CompactionIterator::ExtractLargeValueIfNeeded() {
  assert(ikey_.type == kTypeValue);

  if (!ExtractLargeValueIfNeededImpl()) {
    return;
  }

  ikey_.type = kTypeBlobIndex;
  current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
}

void CompactionIterator::GarbageCollectBlobIfNeeded() {
  assert(ikey_.type == kTypeBlobIndex);

  if (!compaction_) {
    return;
  }

  // GC for integrated BlobDB
  if (compaction_->enable_blob_garbage_collection()) {
    BlobIndex blob_index;

    {
      const Status s = blob_index.DecodeFrom(value_);

      if (!s.ok()) {
        status_ = s;
        valid_ = false;

        return;
      }
    }

    if (blob_index.IsInlined() || blob_index.HasTTL()) {
      status_ = Status::Corruption("Unexpected TTL/inlined blob index");
      valid_ = false;

      return;
    }

    if (blob_index.file_number() >=
        blob_garbage_collection_cutoff_file_number_) {
      return;
    }

    const Version* const version = compaction_->input_version();
    assert(version);

    uint64_t bytes_read = 0;

    {
      const Status s = version->GetBlob(ReadOptions(), user_key(), blob_index,
                                        &blob_value_, &bytes_read);

      if (!s.ok()) {
        status_ = s;
        valid_ = false;

        return;
      }
    }

    ++iter_stats_.num_blobs_read;
    iter_stats_.total_blob_bytes_read += bytes_read;

    value_ = blob_value_;

    if (ExtractLargeValueIfNeededImpl()) {
      return;
    }

    ikey_.type = kTypeValue;
    current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);

    return;
  }

  // GC for stacked BlobDB
  if (compaction_filter_ &&
      compaction_filter_->IsStackedBlobDbInternalCompactionFilter()) {
    const auto blob_decision = compaction_filter_->PrepareBlobOutput(
        user_key(), value_, &compaction_filter_value_);

    if (blob_decision == CompactionFilter::BlobDecision::kCorruption) {
      status_ =
          Status::Corruption("Corrupted blob reference encountered during GC");
      valid_ = false;

      return;
    }

    if (blob_decision == CompactionFilter::BlobDecision::kIOError) {
      status_ = Status::IOError("Could not relocate blob during GC");
      valid_ = false;

      return;
    }

    if (blob_decision == CompactionFilter::BlobDecision::kChangeValue) {
      value_ = compaction_filter_value_;

      return;
    }
  }
}

void CompactionIterator::PrepareOutput() {
  if (valid_) {
    if (ikey_.type == kTypeValue) {
      ExtractLargeValueIfNeeded();
    } else if (ikey_.type == kTypeBlobIndex) {
      GarbageCollectBlobIfNeeded();
    }

    // Zeroing out the sequence number leads to better compression.
    // If this is the bottommost level (no files in lower levels)
    // and the earliest snapshot is larger than this seqno
    // and the userkey differs from the last userkey in compaction
    // then we can squash the seqno to zero.
    //
    // This is safe for TransactionDB write-conflict checking since transactions
    // only care about sequence number larger than any active snapshots.
    //
    // Can we do the same for levels above bottom level as long as
    // KeyNotExistsBeyondOutputLevel() return true?
    if (valid_ && compaction_ != nullptr &&
        !compaction_->allow_ingest_behind() &&
        ikeyNotNeededForIncrementalSnapshot() && bottommost_level_ &&
        IN_EARLIEST_SNAPSHOT(ikey_.sequence) && ikey_.type != kTypeMerge) {
      assert(ikey_.type != kTypeDeletion && ikey_.type != kTypeSingleDeletion);
      if (ikey_.type == kTypeDeletion || ikey_.type == kTypeSingleDeletion) {
        ROCKS_LOG_FATAL(info_log_,
                        "Unexpected key type %d for seq-zero optimization",
                        ikey_.type);
      }
      ikey_.sequence = 0;
      if (!timestamp_size_) {
        current_key_.UpdateInternalKey(0, ikey_.type);
      } else if (full_history_ts_low_ && cmp_with_history_ts_low_ < 0) {
        // We can also zero out timestamp for better compression.
        // For the same user key (excluding timestamp), the timestamp-based
        // history can be collapsed to save some space if the timestamp is
        // older than *full_history_ts_low_.
        const std::string kTsMin(timestamp_size_, static_cast<char>(0));
        const Slice ts_slice = kTsMin;
        ikey_.SetTimestamp(ts_slice);
        current_key_.UpdateInternalKey(0, ikey_.type, &ts_slice);
      }
    }
  }
}

inline SequenceNumber CompactionIterator::findEarliestVisibleSnapshot(
    SequenceNumber in, SequenceNumber* prev_snapshot) {
  assert(snapshots_->size());
  if (snapshots_->size() == 0) {
    ROCKS_LOG_FATAL(info_log_,
                    "No snapshot left in findEarliestVisibleSnapshot");
  }
  auto snapshots_iter = std::lower_bound(
      snapshots_->begin(), snapshots_->end(), in);
  if (snapshots_iter == snapshots_->begin()) {
    *prev_snapshot = 0;
  } else {
    *prev_snapshot = *std::prev(snapshots_iter);
    assert(*prev_snapshot < in);
    if (*prev_snapshot >= in) {
      ROCKS_LOG_FATAL(info_log_,
                      "*prev_snapshot >= in in findEarliestVisibleSnapshot");
    }
  }
  if (snapshot_checker_ == nullptr) {
    return snapshots_iter != snapshots_->end()
      ? *snapshots_iter : kMaxSequenceNumber;
  }
  bool has_released_snapshot = !released_snapshots_.empty();
  for (; snapshots_iter != snapshots_->end(); ++snapshots_iter) {
    auto cur = *snapshots_iter;
    assert(in <= cur);
    if (in > cur) {
      ROCKS_LOG_FATAL(info_log_, "in > cur in findEarliestVisibleSnapshot");
    }
    // Skip if cur is in released_snapshots.
    if (has_released_snapshot && released_snapshots_.count(cur) > 0) {
      continue;
    }
    auto res = snapshot_checker_->CheckInSnapshot(in, cur);
    if (res == SnapshotCheckerResult::kInSnapshot) {
      return cur;
    } else if (res == SnapshotCheckerResult::kSnapshotReleased) {
      released_snapshots_.insert(cur);
    }
    *prev_snapshot = cur;
  }
  return kMaxSequenceNumber;
}

// used in 2 places - prevents deletion markers to be dropped if they may be
// needed and disables seqnum zero-out in PrepareOutput for recent keys.
inline bool CompactionIterator::ikeyNotNeededForIncrementalSnapshot() {
  return (!compaction_->preserve_deletes()) ||
         (ikey_.sequence < preserve_deletes_seqnum_);
}

bool CompactionIterator::IsInEarliestSnapshot(SequenceNumber sequence) {
  assert(snapshot_checker_ != nullptr);
  bool pre_condition = (earliest_snapshot_ == kMaxSequenceNumber ||
                        (earliest_snapshot_iter_ != snapshots_->end() &&
                         *earliest_snapshot_iter_ == earliest_snapshot_));
  assert(pre_condition);
  if (!pre_condition) {
    ROCKS_LOG_FATAL(info_log_,
                    "Pre-Condition is not hold in IsInEarliestSnapshot");
  }
  auto in_snapshot =
      snapshot_checker_->CheckInSnapshot(sequence, earliest_snapshot_);
  while (UNLIKELY(in_snapshot == SnapshotCheckerResult::kSnapshotReleased)) {
    // Avoid the the current earliest_snapshot_ being return as
    // earliest visible snapshot for the next value. So if a value's sequence
    // is zero-ed out by PrepareOutput(), the next value will be compact out.
    released_snapshots_.insert(earliest_snapshot_);
    earliest_snapshot_iter_++;

    if (earliest_snapshot_iter_ == snapshots_->end()) {
      earliest_snapshot_ = kMaxSequenceNumber;
    } else {
      earliest_snapshot_ = *earliest_snapshot_iter_;
    }
    in_snapshot =
        snapshot_checker_->CheckInSnapshot(sequence, earliest_snapshot_);
  }
  assert(in_snapshot != SnapshotCheckerResult::kSnapshotReleased);
  if (in_snapshot == SnapshotCheckerResult::kSnapshotReleased) {
    ROCKS_LOG_FATAL(info_log_,
                    "Unexpected released snapshot in IsInEarliestSnapshot");
  }
  return in_snapshot == SnapshotCheckerResult::kInSnapshot;
}

uint64_t CompactionIterator::ComputeBlobGarbageCollectionCutoffFileNumber(
    const CompactionProxy* compaction) {
  if (!compaction) {
    return 0;
  }

  if (!compaction->enable_blob_garbage_collection()) {
    return 0;
  }

  Version* const version = compaction->input_version();
  assert(version);

  const VersionStorageInfo* const storage_info = version->storage_info();
  assert(storage_info);

  const auto& blob_files = storage_info->GetBlobFiles();

  auto it = blob_files.begin();
  std::advance(
      it, compaction->blob_garbage_collection_age_cutoff() * blob_files.size());

  return it != blob_files.end() ? it->first
                                : std::numeric_limits<uint64_t>::max();
}

}  // namespace ROCKSDB_NAMESPACE
