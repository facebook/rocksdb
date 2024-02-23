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

constexpr uint64_t kUnknownTimeBeforeAll = 0;
constexpr SequenceNumber kUnknownSeqnoBeforeAll = 0;

// SeqnoToTimeMapping stores a sampled mapping from sequence numbers to
// unix times (seconds since epoch). This information provides rough bounds
// between sequence numbers and their write times, but is primarily designed
// for getting a best lower bound on the sequence number of data written no
// later than a specified time.
//
// For ease of sampling, it is assumed that the recorded time in each pair
// comes at or after the sequence number and before the next sequence number,
// so this example:
//
// Seqno: 10,       11, ... 20,       21, ... 30,       31, ...
// Time:  ...  500      ...      600      ...      700      ...
//
// would be represented as
//   10 -> 500
//   20 -> 600
//   30 -> 700
//
// In typical operation, the list is sorted, both among seqnos and among times,
// with a bounded number of entries, but some public working states violate
// these constraints.
//
// NOT thread safe - requires external synchronization.
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

    // For delta encoding
    SeqnoTimePair ComputeDelta(const SeqnoTimePair& base) const {
      return {seqno - base.seqno, time - base.time};
    }

    // For delta decoding
    void ApplyDelta(const SeqnoTimePair& delta_or_base) {
      seqno += delta_or_base.seqno;
      time += delta_or_base.time;
    }

    // Ordering used for Sort()
    bool operator<(const SeqnoTimePair& other) const {
      return std::tie(seqno, time) < std::tie(other.seqno, other.time);
    }

    bool operator==(const SeqnoTimePair& other) const {
      return std::tie(seqno, time) == std::tie(other.seqno, other.time);
    }

    static bool SeqnoLess(const SeqnoTimePair& a, const SeqnoTimePair& b) {
      return a.seqno < b.seqno;
    }

    static bool TimeLess(const SeqnoTimePair& a, const SeqnoTimePair& b) {
      return a.time < b.time;
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

  // Both seqno range and time range are inclusive. ... TODO
  //
  bool PrePopulate(SequenceNumber from_seqno, SequenceNumber to_seqno,
                   uint64_t from_time, uint64_t to_time);

  // Append a new entry to the list. The new entry should be newer than the
  // existing ones. It maintains the internal sorted status.
  bool Append(SequenceNumber seqno, uint64_t time);

  // Given a sequence number, return the best (largest / newest) known time
  // that is no later than the write time of that given sequence number.
  // If no such specific time is known, returns kUnknownTimeBeforeAll.
  // Using the example in the class comment above,
  //  GetProximalTimeBeforeSeqno(10) -> kUnknownTimeBeforeAll
  //  GetProximalTimeBeforeSeqno(11) -> 500
  //  GetProximalTimeBeforeSeqno(20) -> 500
  //  GetProximalTimeBeforeSeqno(21) -> 600
  uint64_t GetProximalTimeBeforeSeqno(SequenceNumber seqno) const;

  // Remove any entries not needed for GetProximalSeqnoBeforeTime queries of
  // times older than `now - max_time_duration_`
  void TruncateOldEntries(uint64_t now);

  // Given a time, return the best (largest) sequence number whose write time
  // is no later than that given time. If no such specific sequence number is
  // known, returns kUnknownSeqnoBeforeAll. Using the example in the class
  // comment above,
  //  GetProximalSeqnoBeforeTime(499) -> kUnknownSeqnoBeforeAll
  //  GetProximalSeqnoBeforeTime(500) -> 10
  //  GetProximalSeqnoBeforeTime(599) -> 10
  //  GetProximalSeqnoBeforeTime(600) -> 20
  SequenceNumber GetProximalSeqnoBeforeTime(uint64_t time);

  // Encode to a binary string. start and end seqno are both inclusive.
  void Encode(std::string& des, SequenceNumber start, SequenceNumber end,
              uint64_t now,
              uint64_t output_size = kMaxSeqnoTimePairsPerSST) const;

  // Add a new random entry, unlike Append(), it can be any data, but also makes
  // the list un-sorted.
  void Add(SequenceNumber seqno, uint64_t time);

  // Decode and add the entries to the current obj. The list will be unsorted
  Status Add(const std::string& pairs_str);

  // Return the number of entries
  size_t Size() const { return pairs_.size(); }

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
  bool Empty() const { return pairs_.empty(); }

  // clear all entries
  void Clear() { pairs_.clear(); }

  // return the string for user message
  // Note: Not efficient, okay for print
  std::string ToHumanString() const;

#ifndef NDEBUG
  const std::deque<SeqnoTimePair>& TEST_GetInternalMapping() const {
    return pairs_;
  }
#endif

 private:
  static constexpr uint64_t kMaxSeqnoToTimeEntries =
      kMaxSeqnoTimePairsPerCF * 10;

  uint64_t max_time_duration_;
  uint64_t max_capacity_;

  std::deque<SeqnoTimePair> pairs_;

  bool is_sorted_ = true;

  static uint64_t CalculateMaxCapacity(uint64_t min_time_duration,
                                       uint64_t max_time_duration);

  SeqnoTimePair& Last() {
    assert(!Empty());
    return pairs_.back();
  }

  using pair_const_iterator =
      std::deque<SeqnoToTimeMapping::SeqnoTimePair>::const_iterator;
  pair_const_iterator FindGreaterTime(uint64_t time) const;
  pair_const_iterator FindGreaterSeqno(SequenceNumber seqno) const;
  pair_const_iterator FindGreaterEqSeqno(SequenceNumber seqno) const;
};

}  // namespace ROCKSDB_NAMESPACE
