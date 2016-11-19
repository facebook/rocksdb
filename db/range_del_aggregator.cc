//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/range_del_aggregator.h"

#include <algorithm>

namespace rocksdb {

RangeDelAggregator::RangeDelAggregator(
    const InternalKeyComparator& icmp,
    const std::vector<SequenceNumber>& snapshots)
    : upper_bound_(kMaxSequenceNumber), icmp_(icmp) {
  InitRep(snapshots);
}

RangeDelAggregator::RangeDelAggregator(const InternalKeyComparator& icmp,
                                       SequenceNumber snapshot)
    : upper_bound_(snapshot), icmp_(icmp) {}

void RangeDelAggregator::InitRep(const std::vector<SequenceNumber>& snapshots) {
  assert(rep_ == nullptr);
  rep_.reset(new Rep());
  for (auto snapshot : snapshots) {
    rep_->stripe_map_.emplace(
        snapshot, TombstoneMap(stl_wrappers::LessOfComparator(&icmp_)));
  }
  // Data newer than any snapshot falls in this catch-all stripe
  rep_->stripe_map_.emplace(
      kMaxSequenceNumber, TombstoneMap(stl_wrappers::LessOfComparator(&icmp_)));
  rep_->pinned_iters_mgr_.StartPinning();
}

bool RangeDelAggregator::ShouldDelete(const Slice& internal_key) {
  if (rep_ == nullptr) {
    return false;
  }
  ParsedInternalKey parsed;
  if (!ParseInternalKey(internal_key, &parsed)) {
    assert(false);
  }
  return ShouldDelete(parsed);
}

bool RangeDelAggregator::ShouldDelete(const ParsedInternalKey& parsed) {
  assert(IsValueType(parsed.type));
  if (rep_ == nullptr) {
    return false;
  }
  const auto& tombstone_map = GetTombstoneMap(parsed.sequence);
  for (const auto& start_key_and_tombstone : tombstone_map) {
    const auto& tombstone = start_key_and_tombstone.second;
    if (icmp_.user_comparator()->Compare(parsed.user_key,
                                         tombstone.start_key_) < 0) {
      break;
    }
    if (parsed.sequence < tombstone.seq_ &&
        icmp_.user_comparator()->Compare(parsed.user_key, tombstone.end_key_) <
            0) {
      return true;
    }
  }
  return false;
}

bool RangeDelAggregator::ShouldAddTombstones(
    bool bottommost_level /* = false */) {
  if (rep_ == nullptr) {
    return false;
  }
  auto stripe_map_iter = rep_->stripe_map_.begin();
  assert(stripe_map_iter != rep_->stripe_map_.end());
  if (bottommost_level) {
    // For the bottommost level, keys covered by tombstones in the first
    // (oldest) stripe have been compacted away, so the tombstones are obsolete.
    ++stripe_map_iter;
  }
  while (stripe_map_iter != rep_->stripe_map_.end()) {
    if (!stripe_map_iter->second.empty()) {
      return true;
    }
    ++stripe_map_iter;
  }
  return false;
}

Status RangeDelAggregator::AddTombstones(
    std::unique_ptr<InternalIterator> input) {
  if (input == nullptr) {
    return Status::OK();
  }
  input->SeekToFirst();
  bool first_iter = true;
  while (input->Valid()) {
    if (first_iter) {
      if (rep_ == nullptr) {
        InitRep({upper_bound_});
      }
      first_iter = false;
    }
    ParsedInternalKey parsed_key;
    if (!ParseInternalKey(input->key(), &parsed_key)) {
      return Status::Corruption("Unable to parse range tombstone InternalKey");
    }
    RangeTombstone tombstone(parsed_key, input->value());
    auto& tombstone_map = GetTombstoneMap(tombstone.seq_);
    tombstone_map.emplace(input->key(), std::move(tombstone));
    input->Next();
  }
  if (!first_iter) {
    rep_->pinned_iters_mgr_.PinIterator(input.release(), false /* arena */);
  }
  return Status::OK();
}

RangeDelAggregator::TombstoneMap& RangeDelAggregator::GetTombstoneMap(
    SequenceNumber seq) {
  assert(rep_ != nullptr);
  // The stripe includes seqnum for the snapshot above and excludes seqnum for
  // the snapshot below.
  StripeMap::iterator iter;
  if (seq > 0) {
    // upper_bound() checks strict inequality so need to subtract one
    iter = rep_->stripe_map_.upper_bound(seq - 1);
  } else {
    iter = rep_->stripe_map_.begin();
  }
  // catch-all stripe justifies this assertion in either of above cases
  assert(iter != rep_->stripe_map_.end());
  return iter->second;
}

// TODO(andrewkr): We should implement an iterator over range tombstones in our
// map. It'd enable compaction to open tables on-demand, i.e., only once range
// tombstones are known to be available, without the code duplication we have
// in ShouldAddTombstones(). It'll also allow us to move the table-modifying
// code into more coherent places: CompactionJob and BuildTable().
void RangeDelAggregator::AddToBuilder(
    TableBuilder* builder, const Slice* lower_bound, const Slice* upper_bound,
    FileMetaData* meta,
    bool bottommost_level /* = false */) {
  if (rep_ == nullptr) {
    return;
  }
  auto stripe_map_iter = rep_->stripe_map_.begin();
  assert(stripe_map_iter != rep_->stripe_map_.end());
  if (bottommost_level) {
    // For the bottommost level, keys covered by tombstones in the first
    // (oldest) stripe have been compacted away, so the tombstones are obsolete.
    ++stripe_map_iter;
  }

  // Note the order in which tombstones are stored is insignificant since we
  // insert them into a std::map on the read path.
  bool first_added = false;
  while (stripe_map_iter != rep_->stripe_map_.end()) {
    for (const auto& start_key_and_tombstone : stripe_map_iter->second) {
      const auto& tombstone = start_key_and_tombstone.second;
      if (upper_bound != nullptr &&
          icmp_.user_comparator()->Compare(*upper_bound,
                                           tombstone.start_key_) <= 0) {
        // Tombstones starting at upper_bound or later only need to be included
        // in the next table. Break because subsequent tombstones will start
        // even later.
        break;
      }
      if (lower_bound != nullptr &&
          icmp_.user_comparator()->Compare(tombstone.end_key_,
                                           *lower_bound) <= 0) {
        // Tombstones ending before or at lower_bound only need to be included
        // in the prev table. Continue because subsequent tombstones may still
        // overlap [lower_bound, upper_bound).
        continue;
      }

      auto ikey_and_end_key = tombstone.Serialize();
      builder->Add(ikey_and_end_key.first.Encode(), ikey_and_end_key.second);
      if (!first_added) {
        first_added = true;
        InternalKey smallest_candidate = std::move(ikey_and_end_key.first);;
        if (lower_bound != nullptr &&
            icmp_.user_comparator()->Compare(smallest_candidate.user_key(),
                                             *lower_bound) <= 0) {
          // Pretend the smallest key has the same user key as lower_bound
          // (the max key in the previous table or subcompaction) in order for
          // files to appear key-space partitioned.
          //
          // Choose lowest seqnum so this file's smallest internal key comes
          // after the previous file's/subcompaction's largest. The fake seqnum
          // is OK because the read path's file-picking code only considers user
          // key.
          smallest_candidate = InternalKey(*lower_bound, 0, kTypeRangeDeletion);
        }
        if (meta->smallest.size() == 0 ||
            icmp_.Compare(smallest_candidate, meta->smallest) < 0) {
          meta->smallest = std::move(smallest_candidate);
        }
      }
      InternalKey largest_candidate = tombstone.SerializeEndKey();
      if (upper_bound != nullptr &&
          icmp_.user_comparator()->Compare(*upper_bound,
                                           largest_candidate.user_key()) <= 0) {
        // Pretend the largest key has the same user key as upper_bound (the
        // min key in the following table or subcompaction) in order for files
        // to appear key-space partitioned.
        //
        // Choose highest seqnum so this file's largest internal key comes
        // before the next file's/subcompaction's smallest. The fake seqnum is
        // OK because the read path's file-picking code only considers the user
        // key portion.
        //
        // Note Seek() also creates InternalKey with (user_key,
        // kMaxSequenceNumber), but with kTypeDeletion (0x7) instead of
        // kTypeRangeDeletion (0xF), so the range tombstone comes before the
        // Seek() key in InternalKey's ordering. So Seek() will look in the
        // next file for the user key.
        largest_candidate = InternalKey(*upper_bound, kMaxSequenceNumber,
                                        kTypeRangeDeletion);
      }
      if (meta->largest.size() == 0 ||
          icmp_.Compare(meta->largest, largest_candidate) < 0) {
        meta->largest = std::move(largest_candidate);
      }
      meta->smallest_seqno = std::min(meta->smallest_seqno, tombstone.seq_);
      meta->largest_seqno = std::max(meta->largest_seqno, tombstone.seq_);
    }
    ++stripe_map_iter;
  }
}

bool RangeDelAggregator::IsEmpty() {
  if (rep_ == nullptr) {
    return true;
  }
  for (auto stripe_map_iter = rep_->stripe_map_.begin();
       stripe_map_iter != rep_->stripe_map_.end(); ++stripe_map_iter) {
    if (!stripe_map_iter->second.empty()) {
      return false;
    }
  }
  return true;
}

}  // namespace rocksdb
