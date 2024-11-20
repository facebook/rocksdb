//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>

#include "db/read_callback.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {
class BlobFetcher;
class Comparator;
class Logger;
class MergeContext;
class MergeOperator;
class PinnableWideColumns;
class PinnedIteratorsManager;
class Statistics;
class SystemClock;
struct ParsedInternalKey;

// Data structure for accumulating statistics during a point lookup. At the
// end of the point lookup, the corresponding ticker stats are updated. This
// avoids the overhead of frequent ticker stats updates
struct GetContextStats {
  uint64_t num_cache_hit = 0;
  uint64_t num_cache_index_hit = 0;
  uint64_t num_cache_data_hit = 0;
  uint64_t num_cache_filter_hit = 0;
  uint64_t num_cache_compression_dict_hit = 0;
  uint64_t num_cache_index_miss = 0;
  uint64_t num_cache_filter_miss = 0;
  uint64_t num_cache_data_miss = 0;
  uint64_t num_cache_compression_dict_miss = 0;
  uint64_t num_cache_bytes_read = 0;
  uint64_t num_cache_miss = 0;
  uint64_t num_cache_add = 0;
  uint64_t num_cache_add_redundant = 0;
  uint64_t num_cache_bytes_write = 0;
  uint64_t num_cache_index_add = 0;
  uint64_t num_cache_index_add_redundant = 0;
  uint64_t num_cache_index_bytes_insert = 0;
  uint64_t num_cache_data_add = 0;
  uint64_t num_cache_data_add_redundant = 0;
  uint64_t num_cache_data_bytes_insert = 0;
  uint64_t num_cache_filter_add = 0;
  uint64_t num_cache_filter_add_redundant = 0;
  uint64_t num_cache_filter_bytes_insert = 0;
  uint64_t num_cache_compression_dict_add = 0;
  uint64_t num_cache_compression_dict_add_redundant = 0;
  uint64_t num_cache_compression_dict_bytes_insert = 0;
  // MultiGet stats.
  uint64_t num_filter_read = 0;
  uint64_t num_index_read = 0;
  uint64_t num_sst_read = 0;
};

// A class to hold context about a point lookup, such as pointer to value
// slice, key, merge context etc, as well as the current state of the
// lookup. Any user using GetContext to track the lookup result must call
// SaveValue() whenever the internal key is found. This can happen
// repeatedly in case of merge operands. In case the key may exist with
// high probability, but IO is required to confirm and the user doesn't allow
// it, MarkKeyMayExist() must be called instead of SaveValue().
class GetContext {
 public:
  // Current state of the point lookup. All except kNotFound and kMerge are
  // terminal states
  enum GetState {
    kNotFound,
    kFound,
    kDeleted,
    kCorrupt,
    kMerge,  // saver contains the current merge result (the operands)
    kUnexpectedBlobIndex,
    kMergeOperatorFailed,
  };
  GetContextStats get_context_stats_;

  // Constructor
  // @param value Holds the value corresponding to user_key. If its nullptr
  //              then return all merge operands corresponding to user_key
  //              via merge_context
  // @param value_found If non-nullptr, set to false if key may be present
  //                    but we can't be certain because we cannot do IO
  // @param max_covering_tombstone_seq Pointer to highest sequence number of
  //                    range deletion covering the key. When an internal key
  //                    is found with smaller sequence number, the lookup
  //                    terminates
  // @param seq If non-nullptr, the sequence number of the found key will be
  //            saved here
  // @param callback Pointer to ReadCallback to perform additional checks
  //                 for visibility of a key
  // @param is_blob_index If non-nullptr, will be used to indicate if a found
  //                      key is of type blob index
  // @param do_merge True if value associated with user_key has to be returned
  // and false if all the merge operands associated with user_key has to be
  // returned. Id do_merge=false then all the merge operands are stored in
  // merge_context and they are never merged. The value pointer is untouched.
  GetContext(const Comparator* ucmp, const MergeOperator* merge_operator,
             Logger* logger, Statistics* statistics, GetState init_state,
             const Slice& user_key, PinnableSlice* value,
             PinnableWideColumns* columns, bool* value_found,
             MergeContext* merge_context, bool do_merge,
             SequenceNumber* max_covering_tombstone_seq, SystemClock* clock,
             SequenceNumber* seq = nullptr,
             PinnedIteratorsManager* _pinned_iters_mgr = nullptr,
             ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
             uint64_t tracing_get_id = 0, BlobFetcher* blob_fetcher = nullptr);
  GetContext(const Comparator* ucmp, const MergeOperator* merge_operator,
             Logger* logger, Statistics* statistics, GetState init_state,
             const Slice& user_key, PinnableSlice* value,
             PinnableWideColumns* columns, std::string* timestamp,
             bool* value_found, MergeContext* merge_context, bool do_merge,
             SequenceNumber* max_covering_tombstone_seq, SystemClock* clock,
             SequenceNumber* seq = nullptr,
             PinnedIteratorsManager* _pinned_iters_mgr = nullptr,
             ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
             uint64_t tracing_get_id = 0, BlobFetcher* blob_fetcher = nullptr);

  GetContext() = delete;

  // This can be called to indicate that a key may be present, but cannot be
  // confirmed due to IO not allowed
  void MarkKeyMayExist();

  // Records this key, value, and any meta-data (such as sequence number and
  // state) into this GetContext.
  //
  // If the parsed_key matches the user key that we are looking for, sets
  // matched to true.
  //
  // Returns True if more keys need to be read (due to merges) or
  //         False if the complete value has been found.
  bool SaveValue(const ParsedInternalKey& parsed_key, const Slice& value,
                 bool* matched, Status* read_status,
                 Cleanable* value_pinner = nullptr);

  // Simplified version of the previous function. Should only be used when we
  // know that the operation is a Put.
  void SaveValue(const Slice& value, SequenceNumber seq);

  GetState State() const { return state_; }

  SequenceNumber* max_covering_tombstone_seq() {
    return max_covering_tombstone_seq_;
  }

  bool NeedTimestamp() { return timestamp_ != nullptr; }

  inline size_t TimestampSize() { return ucmp_->timestamp_size(); }

  void SetTimestampFromRangeTombstone(const Slice& timestamp) {
    assert(timestamp_);
    timestamp_->assign(timestamp.data(), timestamp.size());
    ts_from_rangetombstone_ = true;
  }

  PinnedIteratorsManager* pinned_iters_mgr() { return pinned_iters_mgr_; }

  // If a non-null string is passed, all the SaveValue calls will be
  // logged into the string. The operations can then be replayed on
  // another GetContext with replayGetContextLog.
  void SetReplayLog(std::string* replay_log) { replay_log_ = replay_log; }

  // Do we need to fetch the SequenceNumber for this key?
  bool NeedToReadSequence() const { return (seq_ != nullptr); }

  bool sample() const { return sample_; }

  bool CheckCallback(SequenceNumber seq) {
    if (callback_) {
      return callback_->IsVisible(seq);
    }
    return true;
  }

  void ReportCounters();

  bool has_callback() const { return callback_ != nullptr; }

  const Slice& ukey_to_get_blob_value() const {
    if (!ukey_with_ts_found_.empty()) {
      return ukey_with_ts_found_;
    } else {
      return user_key_;
    }
  }

  uint64_t get_tracing_get_id() const { return tracing_get_id_; }

  void push_operand(const Slice& value, Cleanable* value_pinner);

 private:
  // Helper method that postprocesses the results of merge operations, e.g. it
  // sets the state correctly upon merge errors.
  void PostprocessMerge(const Status& merge_status);

  // The following methods perform the actual merge operation for the
  // no base value/plain base value/wide-column base value cases.
  void MergeWithNoBaseValue();
  void MergeWithPlainBaseValue(const Slice& value);
  void MergeWithWideColumnBaseValue(const Slice& entity);

  bool GetBlobValue(const Slice& user_key, const Slice& blob_index,
                    PinnableSlice* blob_value, Status* read_status);

  void appendToReplayLog(ValueType type, Slice value, Slice ts);

  const Comparator* ucmp_;
  const MergeOperator* merge_operator_;
  // the merge operations encountered;
  Logger* logger_;
  Statistics* statistics_;

  GetState state_;
  Slice user_key_;
  // When a blob index is found with the user key containing timestamp,
  // this copies the corresponding user key on record in the sst file
  // and is later used for blob verification.
  PinnableSlice ukey_with_ts_found_;
  PinnableSlice* pinnable_val_;
  PinnableWideColumns* columns_;
  std::string* timestamp_;
  bool ts_from_rangetombstone_{false};
  bool* value_found_;  // Is value set correctly? Used by KeyMayExist
  MergeContext* merge_context_;
  SequenceNumber* max_covering_tombstone_seq_;
  SystemClock* clock_;
  // If a key is found, seq_ will be set to the SequenceNumber of most recent
  // write to the key or kMaxSequenceNumber if unknown
  SequenceNumber* seq_;
  std::string* replay_log_;
  // Used to temporarily pin blocks when state_ == GetContext::kMerge
  PinnedIteratorsManager* pinned_iters_mgr_;
  ReadCallback* callback_;
  bool sample_;
  // Value is true if it's called as part of DB Get API and false if it's
  // called as part of DB GetMergeOperands API. When it's false merge operators
  // are never merged.
  bool do_merge_;
  bool* is_blob_index_;
  // Used for block cache tracing only. A tracing get id uniquely identifies a
  // Get or a MultiGet.
  const uint64_t tracing_get_id_;
  BlobFetcher* blob_fetcher_;
};

// Call this to replay a log and bring the get_context up to date. The replay
// log must have been created by another GetContext object, whose replay log
// must have been set by calling GetContext::SetReplayLog().
Status replayGetContextLog(const Slice& replay_log, const Slice& user_key,
                           GetContext* get_context,
                           Cleanable* value_pinner = nullptr,
                           SequenceNumber seq_no = kMaxSequenceNumber);

}  // namespace ROCKSDB_NAMESPACE
