//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <algorithm>
#include <deque>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/compaction/compaction.h"
#include "db/compaction/compaction_iteration_stats.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/range_del_aggregator.h"
#include "db/snapshot_checker.h"
#include "options/cf_options.h"
#include "rocksdb/compaction_filter.h"

namespace rocksdb {

// This callback can be used to refresh the snapshot list from the db. It
// includes logics to exponentially decrease the refresh rate to limit the
// overhead of refresh.
class SnapshotListFetchCallback {
 public:
  SnapshotListFetchCallback(Env* env, uint64_t snap_refresh_nanos,
                            size_t every_nth_key = 1024)
      : timer_(env, /*auto restart*/ true),
        snap_refresh_nanos_(snap_refresh_nanos),
        every_nth_key_minus_one_(every_nth_key - 1) {
    assert(every_nth_key > 0);
    assert((ceil(log2(every_nth_key)) == floor(log2(every_nth_key))));
  }
  // Refresh the snapshot list. snapshots will bre replacted with the new list.
  // max is the upper bound. Note: this function will acquire the db_mutex_.
  virtual void Refresh(std::vector<SequenceNumber>* snapshots,
                       SequenceNumber max) = 0;
  inline bool TimeToRefresh(const size_t key_index) {
    // skip the key if key_index % every_nth_key (which is of power 2) is not 0.
    if ((key_index & every_nth_key_minus_one_) != 0) {
      return false;
    }
    const uint64_t elapsed = timer_.ElapsedNanos();
    auto ret = elapsed > snap_refresh_nanos_;
    // pre-compute the next time threshold
    if (ret) {
      // inc next refresh period exponentially (by x4)
      auto next_refresh_threshold = snap_refresh_nanos_ << 2;
      // make sure the shift has not overflown the highest 1 bit
      snap_refresh_nanos_ =
          std::max(snap_refresh_nanos_, next_refresh_threshold);
    }
    return ret;
  }
  static constexpr SnapshotListFetchCallback* kDisabled = nullptr;

  virtual ~SnapshotListFetchCallback() {}

 private:
  // Time since the callback was created
  StopWatchNano timer_;
  // The delay before calling ::Refresh. To be increased exponentially.
  uint64_t snap_refresh_nanos_;
  // Skip evey nth key. Number n if of power 2. The math will require n-1.
  const uint64_t every_nth_key_minus_one_;
};

class CompactionIterator {
 public:
  // A wrapper around Compaction. Has a much smaller interface, only what
  // CompactionIterator uses. Tests can override it.
  class CompactionProxy {
   public:
    explicit CompactionProxy(const Compaction* compaction)
        : compaction_(compaction) {}

    virtual ~CompactionProxy() = default;
    virtual int level(size_t /*compaction_input_level*/ = 0) const {
      return compaction_->level();
    }
    virtual bool KeyNotExistsBeyondOutputLevel(
        const Slice& user_key, std::vector<size_t>* level_ptrs) const {
      return compaction_->KeyNotExistsBeyondOutputLevel(user_key, level_ptrs);
    }
    virtual bool bottommost_level() const {
      return compaction_->bottommost_level();
    }
    virtual int number_levels() const { return compaction_->number_levels(); }
    virtual Slice GetLargestUserKey() const {
      return compaction_->GetLargestUserKey();
    }
    virtual bool allow_ingest_behind() const {
      return compaction_->immutable_cf_options()->allow_ingest_behind;
    }
    virtual bool preserve_deletes() const {
      return compaction_->immutable_cf_options()->preserve_deletes;
    }

   protected:
    CompactionProxy() = default;

   private:
    const Compaction* compaction_;
  };

  CompactionIterator(InternalIterator* input, const Comparator* cmp,
                     MergeHelper* merge_helper, SequenceNumber last_sequence,
                     std::vector<SequenceNumber>* snapshots,
                     SequenceNumber earliest_write_conflict_snapshot,
                     const SnapshotChecker* snapshot_checker, Env* env,
                     bool report_detailed_time, bool expect_valid_internal_key,
                     CompactionRangeDelAggregator* range_del_agg,
                     const Compaction* compaction = nullptr,
                     const CompactionFilter* compaction_filter = nullptr,
                     const std::atomic<bool>* shutting_down = nullptr,
                     const SequenceNumber preserve_deletes_seqnum = 0,
                     SnapshotListFetchCallback* snap_list_callback = nullptr);

  // Constructor with custom CompactionProxy, used for tests.
  CompactionIterator(InternalIterator* input, const Comparator* cmp,
                     MergeHelper* merge_helper, SequenceNumber last_sequence,
                     std::vector<SequenceNumber>* snapshots,
                     SequenceNumber earliest_write_conflict_snapshot,
                     const SnapshotChecker* snapshot_checker, Env* env,
                     bool report_detailed_time, bool expect_valid_internal_key,
                     CompactionRangeDelAggregator* range_del_agg,
                     std::unique_ptr<CompactionProxy> compaction,
                     const CompactionFilter* compaction_filter = nullptr,
                     const std::atomic<bool>* shutting_down = nullptr,
                     const SequenceNumber preserve_deletes_seqnum = 0,
                     SnapshotListFetchCallback* snap_list_callback = nullptr);

  ~CompactionIterator();

  void ResetRecordCounts();

  // Seek to the beginning of the compaction iterator output.
  //
  // REQUIRED: Call only once.
  void SeekToFirst();

  // Produces the next record in the compaction.
  //
  // REQUIRED: SeekToFirst() has been called.
  void Next();

  // Getters
  const Slice& key() const { return key_; }
  const Slice& value() const { return value_; }
  const Status& status() const { return status_; }
  const ParsedInternalKey& ikey() const { return ikey_; }
  bool Valid() const { return valid_; }
  const Slice& user_key() const { return current_user_key_; }
  const CompactionIterationStats& iter_stats() const { return iter_stats_; }

 private:
  // Processes the input stream to find the next output
  void NextFromInput();
  // Process snapshots_ and assign related variables
  void ProcessSnapshotList();

  // Do last preparations before presenting the output to the callee. At this
  // point this only zeroes out the sequence number if possible for better
  // compression.
  void PrepareOutput();

  // Invoke compaction filter if needed.
  void InvokeFilterIfNeeded(bool* need_skip, Slice* skip_until);

  // Given a sequence number, return the sequence number of the
  // earliest snapshot that this sequence number is visible in.
  // The snapshots themselves are arranged in ascending order of
  // sequence numbers.
  // Employ a sequential search because the total number of
  // snapshots are typically small.
  inline SequenceNumber findEarliestVisibleSnapshot(
      SequenceNumber in, SequenceNumber* prev_snapshot);

  // Checks whether the currently seen ikey_ is needed for
  // incremental (differential) snapshot and hence can't be dropped
  // or seqnum be zero-ed out even if all other conditions for it are met.
  inline bool ikeyNotNeededForIncrementalSnapshot();

  inline bool KeyCommitted(SequenceNumber sequence) {
    return snapshot_checker_ == nullptr ||
           snapshot_checker_->CheckInSnapshot(sequence, kMaxSequenceNumber) ==
               SnapshotCheckerResult::kInSnapshot;
  }

  bool IsInEarliestSnapshot(SequenceNumber sequence);

  InternalIterator* input_;
  const Comparator* cmp_;
  MergeHelper* merge_helper_;
  std::vector<SequenceNumber>* snapshots_;
  // List of snapshots released during compaction.
  // findEarliestVisibleSnapshot() find them out from return of
  // snapshot_checker, and make sure they will not be returned as
  // earliest visible snapshot of an older value.
  // See WritePreparedTransactionTest::ReleaseSnapshotDuringCompaction3.
  std::unordered_set<SequenceNumber> released_snapshots_;
  std::vector<SequenceNumber>::const_iterator earliest_snapshot_iter_;
  const SequenceNumber earliest_write_conflict_snapshot_;
  const SnapshotChecker* const snapshot_checker_;
  Env* env_;
  bool report_detailed_time_;
  bool expect_valid_internal_key_;
  CompactionRangeDelAggregator* range_del_agg_;
  std::unique_ptr<CompactionProxy> compaction_;
  const CompactionFilter* compaction_filter_;
  const std::atomic<bool>* shutting_down_;
  const SequenceNumber preserve_deletes_seqnum_;
  bool bottommost_level_;
  bool valid_ = false;
  bool visible_at_tip_;
  SequenceNumber earliest_snapshot_;
  SequenceNumber latest_snapshot_;

  // State
  //
  // Points to a copy of the current compaction iterator output (current_key_)
  // if valid_.
  Slice key_;
  // Points to the value in the underlying iterator that corresponds to the
  // current output.
  Slice value_;
  // The status is OK unless compaction iterator encounters a merge operand
  // while not having a merge operator defined.
  Status status_;
  // Stores the user key, sequence number and type of the current compaction
  // iterator output (or current key in the underlying iterator during
  // NextFromInput()).
  ParsedInternalKey ikey_;
  // Stores whether ikey_.user_key is valid. If set to false, the user key is
  // not compared against the current key in the underlying iterator.
  bool has_current_user_key_ = false;
  bool at_next_ = false;  // If false, the iterator
  // Holds a copy of the current compaction iterator output (or current key in
  // the underlying iterator during NextFromInput()).
  IterKey current_key_;
  Slice current_user_key_;
  SequenceNumber current_user_key_sequence_;
  SequenceNumber current_user_key_snapshot_;

  // True if the iterator has already returned a record for the current key.
  bool has_outputted_key_ = false;

  // truncated the value of the next key and output it without applying any
  // compaction rules.  This is used for outputting a put after a single delete.
  bool clear_and_output_next_key_ = false;

  MergeOutputIterator merge_out_iter_;
  // PinnedIteratorsManager used to pin input_ Iterator blocks while reading
  // merge operands and then releasing them after consuming them.
  PinnedIteratorsManager pinned_iters_mgr_;
  std::string compaction_filter_value_;
  InternalKey compaction_filter_skip_until_;
  // "level_ptrs" holds indices that remember which file of an associated
  // level we were last checking during the last call to compaction->
  // KeyNotExistsBeyondOutputLevel(). This allows future calls to the function
  // to pick off where it left off since each subcompaction's key range is
  // increasing so a later call to the function must be looking for a key that
  // is in or beyond the last file checked during the previous call
  std::vector<size_t> level_ptrs_;
  CompactionIterationStats iter_stats_;

  // Used to avoid purging uncommitted values. The application can specify
  // uncommitted values by providing a SnapshotChecker object.
  bool current_key_committed_;
  SnapshotListFetchCallback* snap_list_callback_;
  // number of distinct keys processed
  size_t num_keys_ = 0;

  bool IsShuttingDown() {
    // This is a best-effort facility, so memory_order_relaxed is sufficient.
    return shutting_down_ && shutting_down_->load(std::memory_order_relaxed);
  }
};
}  // namespace rocksdb
