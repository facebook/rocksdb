//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <cinttypes>
#include <deque>
#include <functional>
#include <iterator>
#include <string>

#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

constexpr uint64_t kUnknownSeqnoTime = 0;

// SeqnoToTimeMapping stores the sequence number to time mapping, so given a
// sequence number it can estimate the oldest possible time for that sequence
// number. For example:
//   10 -> 100
//   50 -> 300
// then if a key has seqno 19, the OldestApproximateTime would be 100, for 51 it
// would be 300.
// As it's a sorted list, the new entry is inserted from the back. The old data
// will be popped from the front if they're no longer used.
//
// Note: the data struct is not thread safe, both read and write need to be
//  synchronized by caller.
class SeqnoToTimeMapping {
 public:
  // Maximum number of entries can be encoded into SST. The data is delta encode
  // so the maximum data usage for each SST is < 0.3K
  static constexpr uint64_t kMaxSeqnoTimePairsPerSST = 100;

  // Maximum number of entries per CF. If there's only CF with this feature on,
  // the max duration divided by this number, so for example, if
  // preclude_last_level_data_seconds = 100000 (~1day), then it will sample the
  // seqno -> time every 1000 seconds (~17minutes). Then the maximum entry it
  // needs is 100.
  // When there are multiple CFs having this feature on, the sampling cadence is
  // determined by the smallest setting, the capacity is determined the largest
  // setting, also it's caped by kMaxSeqnoTimePairsPerCF * 10.
  static constexpr uint64_t kMaxSeqnoTimePairsPerCF = 100;

  // A simple struct for sequence number to time pair
  struct SeqnoTimePair {
    SequenceNumber seqno = 0;
    uint64_t time = 0;

    SeqnoTimePair() = default;
    SeqnoTimePair(SequenceNumber _seqno, uint64_t _time)
        : seqno(_seqno), time(_time) {}

    // Encode to dest string
    void Encode(std::string& dest) const;

    // Decode the value from input Slice and remove it from the input
    Status Decode(Slice& input);

    // subtraction of 2 SeqnoTimePair
    SeqnoTimePair operator-(const SeqnoTimePair& other) const;

    // Add 2 values together
    void Add(const SeqnoTimePair& obj) {
      seqno += obj.seqno;
      time += obj.time;
    }

    // Compare SeqnoTimePair with a sequence number, used for binary search a
    // sequence number in a list of SeqnoTimePair
    bool operator<(const SequenceNumber& other) const { return seqno < other; }

    // Compare 2 SeqnoTimePair
    bool operator<(const SeqnoTimePair& other) const {
      return std::tie(seqno, time) < std::tie(other.seqno, other.time);
    }

    // Check if 2 SeqnoTimePair is the same
    bool operator==(const SeqnoTimePair& other) const {
      return std::tie(seqno, time) == std::tie(other.seqno, other.time);
    }
  };

  // constractor of SeqnoToTimeMapping
  // max_time_duration is the maximum time it should track. For example, if
  // preclude_last_level_data_seconds is 1 day, then if an entry is older than 1
  // day, then it can be removed.
  // max_capacity is the maximum number of entry it can hold. For single CF,
  // it's caped at 100 (kMaxSeqnoTimePairsPerCF), otherwise
  // kMaxSeqnoTimePairsPerCF * 10.
  // If it's set to 0, means it won't truncate any old data.
  explicit SeqnoToTimeMapping(uint64_t max_time_duration = 0,
                              uint64_t max_capacity = 0)
      : max_time_duration_(max_time_duration), max_capacity_(max_capacity) {}

  // Append a new entry to the list. The new entry should be newer than the
  // existing ones. It maintains the internal sorted status.
  bool Append(SequenceNumber seqno, uint64_t time);

  // Given a sequence number, estimate it's oldest time
  uint64_t GetOldestApproximateTime(SequenceNumber seqno) const;

  // Truncate the old entries based on the current time and max_time_duration_
  void TruncateOldEntries(uint64_t now);

  // Given a time, return it's oldest possible sequence number
  SequenceNumber GetOldestSequenceNum(uint64_t time);

  // Encode to a binary string
  void Encode(std::string& des, SequenceNumber start, SequenceNumber end,
              uint64_t now,
              uint64_t output_size = kMaxSeqnoTimePairsPerSST) const;

  // Add a new random entry, unlike Append(), it can be any data, but also makes
  // the list un-sorted.
  void Add(SequenceNumber seqno, uint64_t time);

  // Decode and add the entries to the current obj. The list will be unsorted
  Status Add(const std::string& seqno_time_mapping_str);

  // Return the number of entries
  size_t Size() const { return seqno_time_mapping_.size(); }

  // Reduce the size of internal list
  bool Resize(uint64_t min_time_duration, uint64_t max_time_duration);

  // Override the max_time_duration_
  void SetMaxTimeDuration(uint64_t max_time_duration) {
    max_time_duration_ = max_time_duration;
  }

  uint64_t GetCapacity() const { return max_capacity_; }

  // Sort the list, which also remove the redundant entries, useless entries,
  // which makes sure the seqno is sorted, but also the time
  Status Sort();

  // copy the current obj from the given smallest_seqno.
  SeqnoToTimeMapping Copy(SequenceNumber smallest_seqno) const;

  // If the internal list is empty
  bool Empty() const { return seqno_time_mapping_.empty(); }

  // clear all entries
  void Clear() { seqno_time_mapping_.clear(); }

  // return the string for user message
  // Note: Not efficient, okay for print
  std::string ToHumanString() const;

#ifndef NDEBUG
  const std::deque<SeqnoTimePair>& TEST_GetInternalMapping() const {
    return seqno_time_mapping_;
  }
#endif

 private:
  static constexpr uint64_t kMaxSeqnoToTimeEntries =
      kMaxSeqnoTimePairsPerCF * 10;

  uint64_t max_time_duration_;
  uint64_t max_capacity_;

  std::deque<SeqnoTimePair> seqno_time_mapping_;

  bool is_sorted_ = true;

  static uint64_t CalculateMaxCapacity(uint64_t min_time_duration,
                                       uint64_t max_time_duration);

  SeqnoTimePair& Last() {
    assert(!Empty());
    return seqno_time_mapping_.back();
  }
};

// for searching the sequence number from SeqnoToTimeMapping
inline bool operator<(const SequenceNumber& seqno,
                      const SeqnoToTimeMapping::SeqnoTimePair& other) {
  return seqno < other.seqno;
}

}  // namespace ROCKSDB_NAMESPACE
