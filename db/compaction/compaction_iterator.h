//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <algorithm>
#include <cinttypes>
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

namespace ROCKSDB_NAMESPACE {

class BlobFileBuilder;

// A wrapper of internal iterator whose purpose is to count how
// many entries there are in the iterator.
class SequenceIterWrapper : public InternalIterator {
 public:
  SequenceIterWrapper(InternalIterator* iter, const Comparator* cmp,
                      bool need_count_entries)
      : icmp_(cmp, /*named=*/false),
        inner_iter_(iter),
        need_count_entries_(need_count_entries) {}
  bool Valid() const override { return inner_iter_->Valid(); }
  Status status() const override { return inner_iter_->status(); }
  void Next() override {
    num_itered_++;
    inner_iter_->Next();
  }
  void Seek(const Slice& target) override {
    if (!need_count_entries_) {
      inner_iter_->Seek(target);
    } else {
      // For flush cases, we need to count total number of entries, so we
      // do Next() rather than Seek().
      while (inner_iter_->Valid() &&
             icmp_.Compare(inner_iter_->key(), target) < 0) {
        Next();
      }
    }
  }
  Slice key() const override { return inner_iter_->key(); }
  Slice value() const override { return inner_iter_->value(); }

  // Unused InternalIterator methods
  void SeekToFirst() override { assert(false); }
  void Prev() override { assert(false); }
  void SeekForPrev(const Slice& /* target */) override { assert(false); }
  void SeekToLast() override { assert(false); }

  uint64_t num_itered() const { return num_itered_; }

 private:
  InternalKeyComparator icmp_;
  InternalIterator* inner_iter_;  // not owned
  uint64_t num_itered_ = 0;
  bool need_count_entries_;
};

class CompactionIterator {
 public:
  // A wrapper around Compaction. Has a much smaller interface, only what
  // CompactionIterator uses. Tests can override it.
  class CompactionProxy {
   public:
    virtual ~CompactionProxy() = default;

    virtual int level() const = 0;

    virtual bool KeyNotExistsBeyondOutputLevel(
        const Slice& user_key, std::vector<size_t>* level_ptrs) const = 0;

    virtual bool bottommost_level() const = 0;

    virtual int number_levels() const = 0;

    virtual Slice GetLargestUserKey() const = 0;

    virtual bool allow_ingest_behind() const = 0;

    virtual bool preserve_deletes() const = 0;

    virtual bool enable_blob_garbage_collection() const = 0;

    virtual double blob_garbage_collection_age_cutoff() const = 0;

    virtual Version* input_version() const = 0;

    virtual bool DoesInputReferenceBlobFiles() const = 0;
  };

  class RealCompaction : public CompactionProxy {
   public:
    explicit RealCompaction(const Compaction* compaction)
        : compaction_(compaction) {
      assert(compaction_);
      assert(compaction_->immutable_options());
      assert(compaction_->mutable_cf_options());
    }

    int level() const override { return compaction_->level(); }

    bool KeyNotExistsBeyondOutputLevel(
        const Slice& user_key, std::vector<size_t>* level_ptrs) const override {
      return compaction_->KeyNotExistsBeyondOutputLevel(user_key, level_ptrs);
    }

    bool bottommost_level() const override {
      return compaction_->bottommost_level();
    }

    int number_levels() const override { return compaction_->number_levels(); }

    Slice GetLargestUserKey() const override {
      return compaction_->GetLargestUserKey();
    }

    bool allow_ingest_behind() const override {
      return compaction_->immutable_options()->allow_ingest_behind;
    }

    bool preserve_deletes() const override {
      return compaction_->immutable_options()->preserve_deletes;
    }

    bool enable_blob_garbage_collection() const override {
      return compaction_->mutable_cf_options()->enable_blob_garbage_collection;
    }

    double blob_garbage_collection_age_cutoff() const override {
      return compaction_->mutable_cf_options()
          ->blob_garbage_collection_age_cutoff;
    }

    Version* input_version() const override {
      return compaction_->input_version();
    }

    bool DoesInputReferenceBlobFiles() const override {
      return compaction_->DoesInputReferenceBlobFiles();
    }

   private:
    const Compaction* compaction_;
  };

  CompactionIterator(
      InternalIterator* input, const Comparator* cmp, MergeHelper* merge_helper,
      SequenceNumber last_sequence, std::vector<SequenceNumber>* snapshots,
      SequenceNumber earliest_write_conflict_snapshot,
      const SnapshotChecker* snapshot_checker, Env* env,
      bool report_detailed_time, bool expect_valid_internal_key,
      CompactionRangeDelAggregator* range_del_agg,
      BlobFileBuilder* blob_file_builder, bool allow_data_in_errors,
      const Compaction* compaction = nullptr,
      const CompactionFilter* compaction_filter = nullptr,
      const std::atomic<bool>* shutting_down = nullptr,
      const SequenceNumber preserve_deletes_seqnum = 0,
      const std::atomic<int>* manual_compaction_paused = nullptr,
      const std::atomic<bool>* manual_compaction_canceled = nullptr,
      const std::shared_ptr<Logger> info_log = nullptr,
      const std::string* full_history_ts_low = nullptr);

  // Constructor with custom CompactionProxy, used for tests.
  CompactionIterator(
      InternalIterator* input, const Comparator* cmp, MergeHelper* merge_helper,
      SequenceNumber last_sequence, std::vector<SequenceNumber>* snapshots,
      SequenceNumber earliest_write_conflict_snapshot,
      const SnapshotChecker* snapshot_checker, Env* env,
      bool report_detailed_time, bool expect_valid_internal_key,
      CompactionRangeDelAggregator* range_del_agg,
      BlobFileBuilder* blob_file_builder, bool allow_data_in_errors,
      std::unique_ptr<CompactionProxy> compaction,
      const CompactionFilter* compaction_filter = nullptr,
      const std::atomic<bool>* shutting_down = nullptr,
      const SequenceNumber preserve_deletes_seqnum = 0,
      const std::atomic<int>* manual_compaction_paused = nullptr,
      const std::atomic<bool>* manual_compaction_canceled = nullptr,
      const std::shared_ptr<Logger> info_log = nullptr,
      const std::string* full_history_ts_low = nullptr);

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
  uint64_t num_input_entry_scanned() const { return input_.num_itered(); }

 private:
  // Processes the input stream to find the next output
  void NextFromInput();

  // Do final preparations before presenting the output to the callee.
  void PrepareOutput();

  // Passes the output value to the blob file builder (if any), and replaces it
  // with the corresponding blob reference if it has been actually written to a
  // blob file (i.e. if it passed the value size check). Returns true if the
  // value got extracted to a blob file, false otherwise.
  bool ExtractLargeValueIfNeededImpl();

  // Extracts large values as described above, and updates the internal key's
  // type to kTypeBlobIndex if the value got extracted. Should only be called
  // for regular values (kTypeValue).
  void ExtractLargeValueIfNeeded();

  // Relocates valid blobs residing in the oldest blob files if garbage
  // collection is enabled. Relocated blobs are written to new blob files or
  // inlined in the LSM tree depending on the current settings (i.e.
  // enable_blob_files and min_blob_size). Should only be called for blob
  // references (kTypeBlobIndex).
  //
  // Note: the stacked BlobDB implementation's compaction filter based GC
  // algorithm is also called from here.
  void GarbageCollectBlobIfNeeded();

  // Invoke compaction filter if needed.
  // Return true on success, false on failures (e.g.: kIOError).
  bool InvokeFilterIfNeeded(bool* need_skip, Slice* skip_until);

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

  // Extract user-defined timestamp from user key if possible and compare it
  // with *full_history_ts_low_ if applicable.
  inline void UpdateTimestampAndCompareWithFullHistoryLow() {
    if (!timestamp_size_) {
      return;
    }
    Slice ts = ExtractTimestampFromUserKey(ikey_.user_key, timestamp_size_);
    curr_ts_.assign(ts.data(), ts.size());
    if (full_history_ts_low_) {
      cmp_with_history_ts_low_ =
          cmp_->CompareTimestamp(ts, *full_history_ts_low_);
    }
  }

  static uint64_t ComputeBlobGarbageCollectionCutoffFileNumber(
      const CompactionProxy* compaction);

  SequenceIterWrapper input_;
  const Comparator* cmp_;
  MergeHelper* merge_helper_;
  const std::vector<SequenceNumber>* snapshots_;
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
  SystemClock* clock_;
  bool report_detailed_time_;
  bool expect_valid_internal_key_;
  CompactionRangeDelAggregator* range_del_agg_;
  BlobFileBuilder* blob_file_builder_;
  std::unique_ptr<CompactionProxy> compaction_;
  const CompactionFilter* compaction_filter_;
  const std::atomic<bool>* shutting_down_;
  const std::atomic<int>* manual_compaction_paused_;
  const std::atomic<bool>* manual_compaction_canceled_;
  const SequenceNumber preserve_deletes_seqnum_;
  bool bottommost_level_;
  bool valid_ = false;
  bool visible_at_tip_;
  SequenceNumber earliest_snapshot_;
  SequenceNumber latest_snapshot_;

  std::shared_ptr<Logger> info_log_;

  bool allow_data_in_errors_;

  // Comes from comparator.
  const size_t timestamp_size_;

  // Lower bound timestamp to retain full history in terms of user-defined
  // timestamp. If a key's timestamp is older than full_history_ts_low_, then
  // the key *may* be eligible for garbage collection (GC). The skipping logic
  // is in `NextFromInput()` and `PrepareOutput()`.
  // If nullptr, NO GC will be performed and all history will be preserved.
  const std::string* const full_history_ts_low_;

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
  // If false, the iterator holds a copy of the current compaction iterator
  // output (or current key in the underlying iterator during NextFromInput()).
  bool at_next_ = false;

  IterKey current_key_;
  Slice current_user_key_;
  std::string curr_ts_;
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

  uint64_t blob_garbage_collection_cutoff_file_number_;

  std::string blob_index_;
  PinnableSlice blob_value_;
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

  // Saved result of ucmp->CompareTimestamp(current_ts_, *full_history_ts_low_)
  int cmp_with_history_ts_low_;

  const int level_;

  void AdvanceInputIter() { input_.Next(); }

  void SkipUntil(const Slice& skip_until) { input_.Seek(skip_until); }

  bool IsShuttingDown() {
    // This is a best-effort facility, so memory_order_relaxed is sufficient.
    return shutting_down_ && shutting_down_->load(std::memory_order_relaxed);
  }

  bool IsPausingManualCompaction() {
    // This is a best-effort facility, so memory_order_relaxed is sufficient.
    return (manual_compaction_paused_ &&
            manual_compaction_paused_->load(std::memory_order_relaxed) > 0) ||
           (manual_compaction_canceled_ &&
            manual_compaction_canceled_->load(std::memory_order_relaxed));
  }
};
}  // namespace ROCKSDB_NAMESPACE
