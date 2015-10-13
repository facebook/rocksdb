//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef MERGE_HELPER_H
#define MERGE_HELPER_H

#include <deque>
#include <string>

#include "db/dbformat.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "util/stop_watch.h"

namespace rocksdb {

class Comparator;
class Iterator;
class Logger;
class MergeOperator;
class Statistics;

class MergeHelper {
 public:
  MergeHelper(Env* env, const Comparator* user_comparator,
              const MergeOperator* user_merge_operator,
              const CompactionFilter* compaction_filter, Logger* logger,
              unsigned min_partial_merge_operands,
              bool assert_valid_internal_key, SequenceNumber latest_snapshot,
              int level = 0, Statistics* stats = nullptr)
      : env_(env),
        user_comparator_(user_comparator),
        user_merge_operator_(user_merge_operator),
        compaction_filter_(compaction_filter),
        logger_(logger),
        min_partial_merge_operands_(min_partial_merge_operands),
        assert_valid_internal_key_(assert_valid_internal_key),
        latest_snapshot_(latest_snapshot),
        level_(level),
        keys_(),
        operands_(),
        filter_timer_(env_),
        total_filter_time_(0U),
        stats_(stats) {
    assert(user_comparator_ != nullptr);
  }

  // Wrapper around MergeOperator::FullMerge() that records perf statistics.
  // Result of merge will be written to result if status returned is OK.
  // If operands is empty, the value will simply be copied to result.
  // Returns one of the following statuses:
  // - OK: Entries were successfully merged.
  // - Corruption: Merge operator reported unsuccessful merge.
  // - NotSupported: Merge operator is missing.
  static Status TimedFullMerge(const Slice& key, const Slice* value,
                               const std::deque<std::string>& operands,
                               const MergeOperator* merge_operator,
                               Statistics* statistics, Env* env, Logger* logger,
                               std::string* result);

  // Merge entries until we hit
  //     - a corrupted key
  //     - a Put/Delete,
  //     - a different user key,
  //     - a specific sequence number (snapshot boundary),
  //  or - the end of iteration
  // iter: (IN)  points to the first merge type entry
  //       (OUT) points to the first entry not included in the merge process
  // stop_before: (IN) a sequence number that merge should not cross.
  //                   0 means no restriction
  // at_bottom:   (IN) true if the iterator covers the bottem level, which means
  //                   we could reach the start of the history of this user key.
  //
  // Returns one of the following statuses:
  // - OK: Entries were successfully merged.
  // - MergeInProgress: Put/Delete not encountered and unable to merge operands.
  // - Corruption: Merge operator reported unsuccessful merge or a corrupted
  //   key has been encountered and not expected (applies only when compiling
  //   with asserts removed).
  //
  // REQUIRED: The first key in the input is not corrupted.
  Status MergeUntil(Iterator* iter, const SequenceNumber stop_before = 0,
                    const bool at_bottom = false);

  // Filters a merge operand using the compaction filter specified
  // in the constructor. Returns true if the operand should be filtered out.
  bool FilterMerge(const Slice& user_key, const Slice& value_slice);

  // Query the merge result
  // These are valid until the next MergeUntil call
  // If the merging was successful:
  //   - keys() contains a single element with the latest sequence number of
  //     the merges. The type will be Put or Merge. See IMPORTANT 1 note, below.
  //   - values() contains a single element with the result of merging all the
  //     operands together
  //
  //   IMPORTANT 1: the key type could change after the MergeUntil call.
  //        Put/Delete + Merge + ... + Merge => Put
  //        Merge + ... + Merge => Merge
  //
  // If the merge operator is not associative, and if a Put/Delete is not found
  // then the merging will be unsuccessful. In this case:
  //   - keys() contains the list of internal keys seen in order of iteration.
  //   - values() contains the list of values (merges) seen in the same order.
  //              values() is parallel to keys() so that the first entry in
  //              keys() is the key associated with the first entry in values()
  //              and so on. These lists will be the same length.
  //              All of these pairs will be merges over the same user key.
  //              See IMPORTANT 2 note below.
  //
  //   IMPORTANT 2: The entries were traversed in order from BACK to FRONT.
  //                So keys().back() was the first key seen by iterator.
  // TODO: Re-style this comment to be like the first one
  const std::deque<std::string>& keys() const { return keys_; }
  const std::deque<std::string>& values() const { return operands_; }
  uint64_t TotalFilterTime() const { return total_filter_time_; }
  bool HasOperator() const { return user_merge_operator_ != nullptr; }

 private:
  Env* env_;
  const Comparator* user_comparator_;
  const MergeOperator* user_merge_operator_;
  const CompactionFilter* compaction_filter_;
  Logger* logger_;
  unsigned min_partial_merge_operands_;
  bool assert_valid_internal_key_; // enforce no internal key corruption?
  SequenceNumber latest_snapshot_;
  int level_;

  // the scratch area that holds the result of MergeUntil
  // valid up to the next MergeUntil call
  std::deque<std::string> keys_;    // Keeps track of the sequence of keys seen
  std::deque<std::string> operands_;  // Parallel with keys_; stores the values

  StopWatchNano filter_timer_;
  uint64_t total_filter_time_;
  Statistics* stats_;
};

// MergeOutputIterator can be used to iterate over the result of a merge.
class MergeOutputIterator {
 public:
  // The MergeOutputIterator is bound to a MergeHelper instance.
  explicit MergeOutputIterator(const MergeHelper* merge_helper);

  // Seeks to the first record in the output.
  void SeekToFirst();
  // Advances to the next record in the output.
  void Next();

  Slice key() { return Slice(*it_keys_); }
  Slice value() { return Slice(*it_values_); }
  bool Valid() { return it_keys_ != merge_helper_->keys().rend(); }

 private:
  const MergeHelper* merge_helper_;
  std::deque<std::string>::const_reverse_iterator it_keys_;
  std::deque<std::string>::const_reverse_iterator it_values_;
};

} // namespace rocksdb

#endif
