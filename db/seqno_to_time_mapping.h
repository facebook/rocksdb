//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <cinttypes>
#include <cstdint>
#include <deque>
#include <functional>
#include <iterator>
#include <string>

#include "db/dbformat.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

constexpr uint64_t kUnknownTimeBeforeAll = 0;
constexpr SequenceNumber kUnknownSeqnoBeforeAll = 0;

// Maximum number of entries can be encoded into SST. The data is delta encode
// so the maximum data usage for each SST is < 0.3K
constexpr uint64_t kMaxSeqnoTimePairsPerSST = 100;

// Maximum number of entries per CF. If there's only CF with this feature on,
// the max span divided by this number, so for example, if
// preclude_last_level_data_seconds = 100000 (~1day), then it will sample the
// seqno -> time every 1000 seconds (~17minutes). Then the maximum entry it
// needs is 100.
// When there are multiple CFs having this feature on, the sampling cadence is
// determined by the smallest setting, the capacity is determined the largest
// setting, also it's caped by kMaxSeqnoTimePairsPerCF * 10.
constexpr uint64_t kMaxSeqnoTimePairsPerCF = 100;

constexpr uint64_t kMaxSeqnoToTimeEntries = kMaxSeqnoTimePairsPerCF * 10;

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
// In typical operation, the list is in "enforced" operation to maintain
// invariants on sortedness, capacity, and time span of entries. However, some
// operations will put the object into "unenforced" mode where those invariants
// are relaxed until explicitly or implicitly re-enforced (which will sort and
// filter the data).
//
// NOT thread safe - requires external synchronization, except a const
// object allows concurrent reads.
class SeqnoToTimeMapping {
 public:
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

    // If another pair can be combined into this one (for optimizing
    // normal SeqnoToTimeMapping behavior), then this mapping is modified
    // and true is returned, indicating the other mapping can be discarded.
    // Otherwise false is returned and nothing is changed.
    bool Merge(const SeqnoTimePair& other);

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

  // Construct an empty SeqnoToTimeMapping with no limits.
  SeqnoToTimeMapping() {}

  // ==== Configuration for enforced state ==== //
  // Set a time span beyond which old entries can be deleted. Specifically,
  // under enforcement mode, the structure will maintian only one entry older
  // than the newest entry time minus max_time_span, so that
  // GetProximalSeqnoBeforeTime queries back to that time return a good result.
  // UINT64_MAX == unlimited. 0 == retain just one latest entry. Returns *this.
  SeqnoToTimeMapping& SetMaxTimeSpan(uint64_t max_time_span);

  // Set the nominal capacity under enforcement mode. The structure is allowed
  // to grow some reasonable fraction larger but will automatically compact
  // down to this size. UINT64_MAX == unlimited. 0 == retain nothing.
  // Returns *this.
  SeqnoToTimeMapping& SetCapacity(uint64_t capacity);

  // ==== Modifiers, enforced ==== //
  // Adds a series of mappings interpolating from from_seqno->from_time to
  // to_seqno->to_time. This can only be called on an empty object and both
  // seqno range and time range are inclusive.
  bool PrePopulate(SequenceNumber from_seqno, SequenceNumber to_seqno,
                   uint64_t from_time, uint64_t to_time);

  // Append a new entry to the list. The `seqno` should be >= all previous
  // entries. This operation maintains enforced mode invariants, and will
  // automatically (re-)enter enforced mode if not already in that state.
  // Returns false if the entry was merged into the most recent entry
  // rather than creating a new entry.
  bool Append(SequenceNumber seqno, uint64_t time);

  // Clear all entries and (re-)enter enforced mode if not already in that
  // state. Enforced limits are unchanged.
  void Clear() {
    pairs_.clear();
    enforced_ = true;
  }

  // Enters the "enforced" state if not already in that state, which is
  // useful before copying or querying. This will
  //  * Sort the entries
  //  * Discard any obsolete entries, which is aided if the caller specifies
  // the `now` time so that entries older than now minus the max time span can
  // be discarded.
  //  * Compact the entries to the configured capacity.
  // Returns *this.
  SeqnoToTimeMapping& Enforce(uint64_t now = 0);

  // ==== Modifiers, unenforced ==== //
  // Add a new random entry and enter "unenforced" state. Unlike Append(), it
  // can be any historical data.
  void AddUnenforced(SequenceNumber seqno, uint64_t time);

  // Decode and add the entries to this mapping object. Unless starting from
  // an empty mapping with no configured enforcement limits, this operation
  // enters the unenforced state.
  Status DecodeFrom(const std::string& pairs_str);

  // Copies entries from the src mapping object to this one, limited to entries
  // needed to answer GetProximalTimeBeforeSeqno() queries for the given
  // *inclusive* seqno range. The source structure must be in enforced
  // state as a precondition. Unless starting with this object as empty mapping
  // with no configured enforcement limits, this object enters the unenforced
  // state.
  void CopyFromSeqnoRange(const SeqnoToTimeMapping& src,
                          SequenceNumber from_seqno,
                          SequenceNumber to_seqno = kMaxSequenceNumber);
  void CopyFrom(const SeqnoToTimeMapping& src) {
    CopyFromSeqnoRange(src, kUnknownSeqnoBeforeAll, kMaxSequenceNumber);
  }

  // ==== Accessors ==== //
  // Given a sequence number, return the best (largest / newest) known time
  // that is no later than the write time of that given sequence number.
  // If no such specific time is known, returns kUnknownTimeBeforeAll.
  // Using the example in the class comment above,
  //  GetProximalTimeBeforeSeqno(10) -> kUnknownTimeBeforeAll
  //  GetProximalTimeBeforeSeqno(11) -> 500
  //  GetProximalTimeBeforeSeqno(20) -> 500
  //  GetProximalTimeBeforeSeqno(21) -> 600
  // Because this is a const operation depending on sortedness, the structure
  // must be in enforced state as a precondition.
  uint64_t GetProximalTimeBeforeSeqno(SequenceNumber seqno) const;

  // Given a time, return the best (largest) sequence number whose write time
  // is no later than that given time. If no such specific sequence number is
  // known, returns kUnknownSeqnoBeforeAll. Using the example in the class
  // comment above,
  //  GetProximalSeqnoBeforeTime(499) -> kUnknownSeqnoBeforeAll
  //  GetProximalSeqnoBeforeTime(500) -> 10
  //  GetProximalSeqnoBeforeTime(599) -> 10
  //  GetProximalSeqnoBeforeTime(600) -> 20
  // Because this is a const operation depending on sortedness, the structure
  // must be in enforced state as a precondition.
  SequenceNumber GetProximalSeqnoBeforeTime(uint64_t time) const;

  // Given current time, the configured `preserve_internal_time_seconds`, and
  // `preclude_last_level_data_seconds`, find the relevant cutoff sequence
  // numbers for tiering.
  void GetCurrentTieringCutoffSeqnos(
      uint64_t current_time, uint64_t preserve_internal_time_seconds,
      uint64_t preclude_last_level_data_seconds,
      SequenceNumber* preserve_time_min_seqno,
      SequenceNumber* preclude_last_level_min_seqno) const;

  // Encode to a binary string by appending to `dest`.
  // Because this is a const operation depending on sortedness, the structure
  // must be in enforced state as a precondition.
  void EncodeTo(std::string& dest) const;

  // Return the number of entries
  size_t Size() const { return pairs_.size(); }

  uint64_t GetCapacity() const { return capacity_; }

  // If the internal list is empty
  bool Empty() const { return pairs_.empty(); }

  // return the string for user message
  // Note: Not efficient, okay for print
  std::string ToHumanString() const;

#ifndef NDEBUG
  const SeqnoTimePair& TEST_GetLastEntry() const { return pairs_.back(); }
  const std::deque<SeqnoTimePair>& TEST_GetInternalMapping() const {
    return pairs_;
  }
  bool TEST_IsEnforced() const { return enforced_; }
#endif

 private:
  uint64_t max_time_span_ = UINT64_MAX;
  uint64_t capacity_ = UINT64_MAX;

  std::deque<SeqnoTimePair> pairs_;

  // Whether this object is in the "enforced" state. Between calls to public
  // functions, enforced_==true means that
  // * `pairs_` is sorted
  // * The capacity limit (non-strict) is met
  // * The time span limit is met
  // However, some places within the implementation (Append()) will temporarily
  // violate those last two conditions while enforced_==true. See also the
  // Enforce*() and Sort*() private functions below.
  bool enforced_ = true;

  void EnforceMaxTimeSpan(uint64_t now = 0);
  void EnforceCapacity(bool strict);
  void SortAndMerge();

  using pair_const_iterator =
      std::deque<SeqnoToTimeMapping::SeqnoTimePair>::const_iterator;
  pair_const_iterator FindGreaterTime(uint64_t time) const;
  pair_const_iterator FindGreaterSeqno(SequenceNumber seqno) const;
  pair_const_iterator FindGreaterEqSeqno(SequenceNumber seqno) const;
};

// === Utility methods used for TimedPut === //

// Pack a value Slice and a unix write time into buffer `buf` and return a Slice
// for the packed value backed by `buf`.
Slice PackValueAndWriteTime(const Slice& value, uint64_t unix_write_time,
                            std::string* buf);

// Pack a value Slice and a sequence number into buffer `buf` and return a Slice
// for the packed value backed by `buf`.
Slice PackValueAndSeqno(const Slice& value, SequenceNumber seqno,
                        std::string* buf);

// Parse a packed value to get the write time.
uint64_t ParsePackedValueForWriteTime(const Slice& value);

// Parse a packed value to get the value and the write time. The unpacked value
// Slice is backed up by the same memory backing up `value`.
std::tuple<Slice, uint64_t> ParsePackedValueWithWriteTime(const Slice& value);

// Parse a packed value to get the sequence number.
SequenceNumber ParsePackedValueForSeqno(const Slice& value);

// Parse a packed value to get the value and the sequence number. The unpacked
// value Slice is backed up by the same memory backing up `value`.
std::tuple<Slice, SequenceNumber> ParsePackedValueWithSeqno(const Slice& value);

// Parse a packed value to get the value. The unpacked value Slice is backed up
// by the same memory backing up `value`.
Slice ParsePackedValueForValue(const Slice& value);

}  // namespace ROCKSDB_NAMESPACE
