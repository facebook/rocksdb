//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/seqno_to_time_mapping.h"

#include "db/version_edit.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

SeqnoToTimeMapping::pair_const_iterator SeqnoToTimeMapping::FindGreaterTime(
    uint64_t time) const {
  return std::upper_bound(pairs_.cbegin(), pairs_.cend(),
                          SeqnoTimePair{0, time}, SeqnoTimePair::TimeLess);
}

SeqnoToTimeMapping::pair_const_iterator SeqnoToTimeMapping::FindGreaterEqSeqno(
    SequenceNumber seqno) const {
  return std::lower_bound(pairs_.cbegin(), pairs_.cend(),
                          SeqnoTimePair{seqno, 0}, SeqnoTimePair::SeqnoLess);
}

SeqnoToTimeMapping::pair_const_iterator SeqnoToTimeMapping::FindGreaterSeqno(
    SequenceNumber seqno) const {
  return std::upper_bound(pairs_.cbegin(), pairs_.cend(),
                          SeqnoTimePair{seqno, 0}, SeqnoTimePair::SeqnoLess);
}

uint64_t SeqnoToTimeMapping::GetProximalTimeBeforeSeqno(
    SequenceNumber seqno) const {
  assert(is_sorted_);
  // Find the last entry with a seqno strictly less than the given seqno.
  // First, find the first entry >= the given seqno (or end)
  auto it = FindGreaterEqSeqno(seqno);
  if (it == pairs_.cbegin()) {
    return kUnknownTimeBeforeAll;
  }
  // Then return data from previous.
  it--;
  return it->time;
}

void SeqnoToTimeMapping::Add(SequenceNumber seqno, uint64_t time) {
  if (seqno == 0) {
    return;
  }
  is_sorted_ = false;
  pairs_.emplace_back(seqno, time);
}

void SeqnoToTimeMapping::TruncateOldEntries(const uint64_t now) {
  assert(is_sorted_);

  if (max_time_duration_ == 0) {
    // No cutoff time
    return;
  }

  if (now < max_time_duration_) {
    // Would under-flow
    return;
  }

  const uint64_t cut_off_time = now - max_time_duration_;
  assert(cut_off_time <= now);  // no under/overflow

  auto it = FindGreaterTime(cut_off_time);
  if (it == pairs_.cbegin()) {
    return;
  }
  // Move back one, to the entry that would be used to return a good seqno from
  // GetProximalSeqnoBeforeTime(cut_off_time)
  --it;
  // Remove everything strictly before that entry
  pairs_.erase(pairs_.cbegin(), std::move(it));
}

SequenceNumber SeqnoToTimeMapping::GetProximalSeqnoBeforeTime(uint64_t time) {
  assert(is_sorted_);

  // Find the last entry with a time <= the given time.
  // First, find the first entry > the given time (or end).
  auto it = FindGreaterTime(time);
  if (it == pairs_.cbegin()) {
    return kUnknownSeqnoBeforeAll;
  }
  // Then return data from previous.
  --it;
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

  auto start_it = FindGreaterSeqno(start);
  if (start_it != pairs_.begin()) {
    start_it--;
  }

  auto end_it = FindGreaterSeqno(end);
  if (end_it == pairs_.begin()) {
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
  if (start_it != pairs_.begin()) {
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
    SeqnoTimePair val = it->ComputeDelta(base);
    base = *it;
    val.Encode(dest);
  }
}

Status SeqnoToTimeMapping::Add(const std::string& pairs_str) {
  Slice input(pairs_str);
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
    val.ApplyDelta(base);
    pairs_.emplace_back(val);
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
      // Updating Last() would hurt GetProximalSeqnoBeforeTime() queries, so
      // NOT doing it (for now)
      return false;
    }
    if (time == Last().time) {
      // Updating Last() here helps GetProximalSeqnoBeforeTime() queries, so
      // doing it (for now)
      Last().seqno = seqno;
      return true;
    }
  }

  pairs_.emplace_back(seqno, time);

  if (pairs_.size() > max_capacity_) {
    // FIXME: be smarter about how we erase to avoid data falling off the
    // front prematurely.
    pairs_.pop_front();
  }
  return true;
}

bool SeqnoToTimeMapping::PrePopulate(SequenceNumber from_seqno,
                                     SequenceNumber to_seqno,
                                     uint64_t from_time, uint64_t to_time) {
  assert(Empty());
  assert(from_seqno > 0);
  assert(to_seqno > from_seqno);
  assert(from_time > kUnknownTimeBeforeAll);
  assert(to_time >= from_time);

  // TODO: smartly limit this to max_capacity_ representative samples
  for (auto i = from_seqno; i <= to_seqno; i++) {
    uint64_t t = from_time + (to_time - from_time) * (i - from_seqno) /
                                 (to_seqno - from_seqno);
    pairs_.emplace_back(i, t);
  }

  return /*success*/ true;
}

bool SeqnoToTimeMapping::Resize(uint64_t min_time_duration,
                                uint64_t max_time_duration) {
  uint64_t new_max_capacity =
      CalculateMaxCapacity(min_time_duration, max_time_duration);
  if (new_max_capacity == max_capacity_) {
    return false;
  } else if (new_max_capacity < pairs_.size()) {
    uint64_t delta = pairs_.size() - new_max_capacity;
    // FIXME: be smarter about how we erase to avoid data falling off the
    // front prematurely.
    pairs_.erase(pairs_.begin(), pairs_.begin() + delta);
  }
  max_capacity_ = new_max_capacity;
  return true;
}

Status SeqnoToTimeMapping::Sort() {
  if (is_sorted_) {
    return Status::OK();
  }
  if (pairs_.empty()) {
    is_sorted_ = true;
    return Status::OK();
  }

  std::deque<SeqnoTimePair> copy = std::move(pairs_);

  std::sort(copy.begin(), copy.end());

  pairs_.clear();

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
        pairs_.push_back(prev);
        prev = it;
      }
    }
  }
  pairs_.emplace_back(prev);

  is_sorted_ = true;
  return Status::OK();
}

std::string SeqnoToTimeMapping::ToHumanString() const {
  std::string ret;
  for (const auto& seq_time : pairs_) {
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
  auto it = FindGreaterSeqno(smallest_seqno);
  if (it != pairs_.begin()) {
    it--;
  }
  std::copy(it, pairs_.end(), std::back_inserter(ret.pairs_));
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

}  // namespace ROCKSDB_NAMESPACE
