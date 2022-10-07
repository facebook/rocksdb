//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/seqno_to_time_mapping.h"

#include "db/version_edit.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

uint64_t SeqnoToTimeMapping::GetOldestApproximateTime(
    const SequenceNumber seqno) const {
  assert(is_sorted_);
  auto it = std::upper_bound(seqno_time_mapping_.begin(),
                             seqno_time_mapping_.end(), seqno);
  if (it == seqno_time_mapping_.begin()) {
    return 0;
  }
  it--;
  return it->time;
}

void SeqnoToTimeMapping::Add(SequenceNumber seqno, uint64_t time) {
  if (seqno == 0) {
    return;
  }
  is_sorted_ = false;
  seqno_time_mapping_.emplace_back(seqno, time);
}

void SeqnoToTimeMapping::TruncateOldEntries(const uint64_t now) {
  assert(is_sorted_);

  if (max_time_duration_ == 0) {
    return;
  }

  const uint64_t cut_off_time =
      now > max_time_duration_ ? now - max_time_duration_ : 0;
  assert(cut_off_time <= now);  // no overflow

  auto it = std::upper_bound(
      seqno_time_mapping_.begin(), seqno_time_mapping_.end(), cut_off_time,
      [](uint64_t target, const SeqnoTimePair& other) -> bool {
        return target < other.time;
      });
  if (it == seqno_time_mapping_.begin()) {
    return;
  }
  it--;
  seqno_time_mapping_.erase(seqno_time_mapping_.begin(), it);
}

SequenceNumber SeqnoToTimeMapping::GetOldestSequenceNum(uint64_t time) {
  assert(is_sorted_);

  auto it = std::upper_bound(
      seqno_time_mapping_.begin(), seqno_time_mapping_.end(), time,
      [](uint64_t target, const SeqnoTimePair& other) -> bool {
        return target < other.time;
      });
  if (it == seqno_time_mapping_.begin()) {
    return 0;
  }
  it--;
  return it->seqno;
}

// The encoded format is:
//  [num_of_entries][[seqno][time],[seqno][time],...]
//      ^                                 ^
//    var_int                      delta_encoded (var_int)
void SeqnoToTimeMapping::Encode(std::string& dest, const SequenceNumber start,
                                const SequenceNumber end, const uint64_t now,
                                const uint64_t output_size) const {
  assert(is_sorted_);
  if (start > end) {
    // It could happen when the SST file is empty, the initial value of min
    // sequence number is kMaxSequenceNumber and max is 0.
    // The empty output file will be removed in the final step of compaction.
    return;
  }

  auto start_it = std::upper_bound(seqno_time_mapping_.begin(),
                                   seqno_time_mapping_.end(), start);
  if (start_it != seqno_time_mapping_.begin()) {
    start_it--;
  }

  auto end_it = std::upper_bound(seqno_time_mapping_.begin(),
                                 seqno_time_mapping_.end(), end);
  if (end_it == seqno_time_mapping_.begin()) {
    return;
  }
  if (start_it >= end_it) {
    return;
  }

  // truncate old entries that are not needed
  if (max_time_duration_ > 0) {
    const uint64_t cut_off_time =
        now > max_time_duration_ ? now - max_time_duration_ : 0;
    while (start_it < end_it && start_it->time < cut_off_time) {
      start_it++;
    }
  }
  // to include the first element
  if (start_it != seqno_time_mapping_.begin()) {
    start_it--;
  }

  // If there are more data than needed, pick the entries for encoding.
  // It's not the most optimized algorithm for selecting the best representative
  // entries over the time.
  // It starts from the beginning and makes sure the distance is larger than
  // `(end - start) / size` before selecting the number. For example, for the
  // following list, pick 3 entries (it will pick seqno #1, #6, #8):
  //    1 -> 10
  //    5 -> 17
  //    6 -> 25
  //    8 -> 30
  // first, it always picks the first one, then there are 2 num_entries_to_fill
  // and the time difference between current one vs. the last one is
  // (30 - 10) = 20. 20/2 = 10. So it will skip until 10+10 = 20. => it skips
  // #5 and pick #6.
  // But the most optimized solution is picking #1 #5 #8, as it will be more
  // evenly distributed for time. Anyway the following algorithm is simple and
  // may over-select new data, which is good. We do want more accurate time
  // information for recent data.
  std::deque<SeqnoTimePair> output_copy;
  if (std::distance(start_it, end_it) > static_cast<int64_t>(output_size)) {
    int64_t num_entries_to_fill = static_cast<int64_t>(output_size);
    auto last_it = end_it;
    last_it--;
    uint64_t end_time = last_it->time;
    uint64_t skip_until_time = 0;
    for (auto it = start_it; it < end_it; it++) {
      // skip if it's not reach the skip_until_time yet
      if (std::distance(it, end_it) > num_entries_to_fill &&
          it->time < skip_until_time) {
        continue;
      }
      output_copy.push_back(*it);
      num_entries_to_fill--;
      if (std::distance(it, end_it) > num_entries_to_fill &&
          num_entries_to_fill > 0) {
        // If there are more entries than we need, re-calculate the
        // skip_until_time, which means skip until that time
        skip_until_time =
            it->time + ((end_time - it->time) / num_entries_to_fill);
      }
    }

    // Make sure all entries are filled
    assert(num_entries_to_fill == 0);
    start_it = output_copy.begin();
    end_it = output_copy.end();
  }

  // Delta encode the data
  uint64_t size = std::distance(start_it, end_it);
  PutVarint64(&dest, size);
  SeqnoTimePair base;
  for (auto it = start_it; it < end_it; it++) {
    assert(base < *it);
    SeqnoTimePair val = *it - base;
    base = *it;
    val.Encode(dest);
  }
}

Status SeqnoToTimeMapping::Add(const std::string& seqno_time_mapping_str) {
  Slice input(seqno_time_mapping_str);
  if (input.empty()) {
    return Status::OK();
  }
  uint64_t size;
  if (!GetVarint64(&input, &size)) {
    return Status::Corruption("Invalid sequence number time size");
  }
  is_sorted_ = false;
  SeqnoTimePair base;
  for (uint64_t i = 0; i < size; i++) {
    SeqnoTimePair val;
    Status s = val.Decode(input);
    if (!s.ok()) {
      return s;
    }
    val.Add(base);
    seqno_time_mapping_.emplace_back(val);
    base = val;
  }
  return Status::OK();
}

void SeqnoToTimeMapping::SeqnoTimePair::Encode(std::string& dest) const {
  PutVarint64Varint64(&dest, seqno, time);
}

Status SeqnoToTimeMapping::SeqnoTimePair::Decode(Slice& input) {
  if (!GetVarint64(&input, &seqno)) {
    return Status::Corruption("Invalid sequence number");
  }
  if (!GetVarint64(&input, &time)) {
    return Status::Corruption("Invalid time");
  }
  return Status::OK();
}

bool SeqnoToTimeMapping::Append(SequenceNumber seqno, uint64_t time) {
  assert(is_sorted_);

  // skip seq number 0, which may have special meaning, like zeroed out data
  if (seqno == 0) {
    return false;
  }
  if (!Empty()) {
    if (seqno < Last().seqno || time < Last().time) {
      return false;
    }
    if (seqno == Last().seqno) {
      Last().time = time;
      return true;
    }
    if (time == Last().time) {
      // new sequence has the same time as old one, no need to add new mapping
      return false;
    }
  }

  seqno_time_mapping_.emplace_back(seqno, time);

  if (seqno_time_mapping_.size() > max_capacity_) {
    seqno_time_mapping_.pop_front();
  }
  return true;
}

bool SeqnoToTimeMapping::Resize(uint64_t min_time_duration,
                                uint64_t max_time_duration) {
  uint64_t new_max_capacity =
      CalculateMaxCapacity(min_time_duration, max_time_duration);
  if (new_max_capacity == max_capacity_) {
    return false;
  } else if (new_max_capacity < seqno_time_mapping_.size()) {
    uint64_t delta = seqno_time_mapping_.size() - new_max_capacity;
    seqno_time_mapping_.erase(seqno_time_mapping_.begin(),
                              seqno_time_mapping_.begin() + delta);
  }
  max_capacity_ = new_max_capacity;
  return true;
}

Status SeqnoToTimeMapping::Sort() {
  if (is_sorted_) {
    return Status::OK();
  }
  if (seqno_time_mapping_.empty()) {
    is_sorted_ = true;
    return Status::OK();
  }

  std::deque<SeqnoTimePair> copy = std::move(seqno_time_mapping_);

  std::sort(copy.begin(), copy.end());

  seqno_time_mapping_.clear();

  // remove seqno = 0, which may have special meaning, like zeroed out data
  while (copy.front().seqno == 0) {
    copy.pop_front();
  }

  SeqnoTimePair prev = copy.front();
  for (const auto& it : copy) {
    // If sequence number is the same, pick the one with larger time, which is
    // more accurate than the older time.
    if (it.seqno == prev.seqno) {
      assert(it.time >= prev.time);
      prev.time = it.time;
    } else {
      assert(it.seqno > prev.seqno);
      // If a larger sequence number has an older time which is not useful, skip
      if (it.time > prev.time) {
        seqno_time_mapping_.push_back(prev);
        prev = it;
      }
    }
  }
  seqno_time_mapping_.emplace_back(prev);

  is_sorted_ = true;
  return Status::OK();
}

std::string SeqnoToTimeMapping::ToHumanString() const {
  std::string ret;
  for (const auto& seq_time : seqno_time_mapping_) {
    AppendNumberTo(&ret, seq_time.seqno);
    ret.append("->");
    AppendNumberTo(&ret, seq_time.time);
    ret.append(",");
  }
  return ret;
}

SeqnoToTimeMapping SeqnoToTimeMapping::Copy(
    SequenceNumber smallest_seqno) const {
  SeqnoToTimeMapping ret;
  auto it = std::upper_bound(seqno_time_mapping_.begin(),
                             seqno_time_mapping_.end(), smallest_seqno);
  if (it != seqno_time_mapping_.begin()) {
    it--;
  }
  std::copy(it, seqno_time_mapping_.end(),
            std::back_inserter(ret.seqno_time_mapping_));
  return ret;
}

uint64_t SeqnoToTimeMapping::CalculateMaxCapacity(uint64_t min_time_duration,
                                                  uint64_t max_time_duration) {
  if (min_time_duration == 0) {
    return 0;
  }
  return std::min(
      kMaxSeqnoToTimeEntries,
      max_time_duration * kMaxSeqnoTimePairsPerCF / min_time_duration);
}

SeqnoToTimeMapping::SeqnoTimePair SeqnoToTimeMapping::SeqnoTimePair::operator-(
    const SeqnoTimePair& other) const {
  SeqnoTimePair res;
  res.seqno = seqno - other.seqno;
  res.time = time - other.time;
  return res;
}

}  // namespace ROCKSDB_NAMESPACE
