//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef MERGE_HELPER_H
#define MERGE_HELPER_H

#include "db/dbformat.h"
#include "rocksdb/slice.h"
#include <string>
#include <deque>
#include "rocksdb/env.h"

namespace rocksdb {

class Comparator;
class Iterator;
class Logger;
class MergeOperator;
class Statistics;

class MergeHelper {
 public:
  MergeHelper(const Comparator* user_comparator,
              const MergeOperator* user_merge_operator, Logger* logger,
              unsigned min_partial_merge_operands,
              bool assert_valid_internal_key)
      : user_comparator_(user_comparator),
        user_merge_operator_(user_merge_operator),
        logger_(logger),
        min_partial_merge_operands_(min_partial_merge_operands),
        assert_valid_internal_key_(assert_valid_internal_key),
        keys_(),
        operands_(),
        success_(false) {}

  // Wrapper around MergeOperator::FullMerge() that records perf statistics.
  // Result of merge will be written to result if status returned is OK.
  // If operands is empty, the value will simply be copied to result.
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
  void MergeUntil(Iterator* iter, SequenceNumber stop_before = 0,
                  bool at_bottom = false, Statistics* stats = nullptr,
                  int* steps = nullptr, Env* env_ = nullptr);

  // Query the merge result
  // These are valid until the next MergeUntil call
  // If the merging was successful:
  //   - IsSuccess() will be true
  //   - key() will have the latest sequence number of the merges.
  //           The type will be Put or Merge. See IMPORTANT 1 note, below.
  //   - value() will be the result of merging all the operands together
  //   - The user should ignore keys() and values().
  //
  //   IMPORTANT 1: the key type could change after the MergeUntil call.
  //        Put/Delete + Merge + ... + Merge => Put
  //        Merge + ... + Merge => Merge
  //
  // If the merge operator is not associative, and if a Put/Delete is not found
  // then the merging will be unsuccessful. In this case:
  //   - IsSuccess() will be false
  //   - keys() contains the list of internal keys seen in order of iteration.
  //   - values() contains the list of values (merges) seen in the same order.
  //              values() is parallel to keys() so that the first entry in
  //              keys() is the key associated with the first entry in values()
  //              and so on. These lists will be the same length.
  //              All of these pairs will be merges over the same user key.
  //              See IMPORTANT 2 note below.
  //   - The user should ignore key() and value().
  //
  //   IMPORTANT 2: The entries were traversed in order from BACK to FRONT.
  //                So keys().back() was the first key seen by iterator.
  // TODO: Re-style this comment to be like the first one
  bool IsSuccess() const { return success_; }
  Slice key() const { assert(success_); return Slice(keys_.back()); }
  Slice value() const { assert(success_); return Slice(operands_.back()); }
  const std::deque<std::string>& keys() const {
    assert(!success_); return keys_;
  }
  const std::deque<std::string>& values() const {
    assert(!success_); return operands_;
  }
  bool HasOperator() const { return user_merge_operator_ != nullptr; }

 private:
  const Comparator* user_comparator_;
  const MergeOperator* user_merge_operator_;
  Logger* logger_;
  unsigned min_partial_merge_operands_;
  bool assert_valid_internal_key_; // enforce no internal key corruption?

  // the scratch area that holds the result of MergeUntil
  // valid up to the next MergeUntil call
  std::deque<std::string> keys_;    // Keeps track of the sequence of keys seen
  std::deque<std::string> operands_;  // Parallel with keys_; stores the values
  bool success_;
};

} // namespace rocksdb

#endif
