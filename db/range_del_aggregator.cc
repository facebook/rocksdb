//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_del_aggregator.h"

#include <algorithm>

namespace rocksdb {

RangeDelAggregator::RangeDelAggregator(
    const InternalKeyComparator& icmp,
    const std::vector<SequenceNumber>& snapshots,
    bool collapse_deletions /* = true */)
    : upper_bound_(kMaxSequenceNumber),
      icmp_(icmp),
      collapse_deletions_(collapse_deletions) {
  InitRep(snapshots);
}

RangeDelAggregator::RangeDelAggregator(const InternalKeyComparator& icmp,
                                       SequenceNumber snapshot,
                                       bool collapse_deletions /* = false */)
    : upper_bound_(snapshot),
      icmp_(icmp),
      collapse_deletions_(collapse_deletions) {}

void RangeDelAggregator::InitRep(const std::vector<SequenceNumber>& snapshots) {
  assert(rep_ == nullptr);
  rep_.reset(new Rep());
  for (auto snapshot : snapshots) {
    rep_->stripe_map_.emplace(
        snapshot,
        PositionalTombstoneMap(TombstoneMap(
            stl_wrappers::LessOfComparator(icmp_.user_comparator()))));
  }
  // Data newer than any snapshot falls in this catch-all stripe
  rep_->stripe_map_.emplace(
      kMaxSequenceNumber,
      PositionalTombstoneMap(TombstoneMap(
          stl_wrappers::LessOfComparator(icmp_.user_comparator()))));
  rep_->pinned_iters_mgr_.StartPinning();
}

bool RangeDelAggregator::ShouldDeleteImpl(
    const Slice& internal_key, RangeDelAggregator::RangePositioningMode mode) {
  assert(rep_ != nullptr);
  ParsedInternalKey parsed;
  if (!ParseInternalKey(internal_key, &parsed)) {
    assert(false);
  }
  return ShouldDelete(parsed, mode);
}

bool RangeDelAggregator::ShouldDeleteImpl(
    const ParsedInternalKey& parsed,
    RangeDelAggregator::RangePositioningMode mode) {
  assert(IsValueType(parsed.type));
  assert(rep_ != nullptr);
  auto& positional_tombstone_map = GetPositionalTombstoneMap(parsed.sequence);
  const auto& tombstone_map = positional_tombstone_map.raw_map;
  if (tombstone_map.empty()) {
    return false;
  }
  auto& tombstone_map_iter = positional_tombstone_map.iter;
  if (tombstone_map_iter == tombstone_map.end() &&
      (mode == kForwardTraversal || mode == kBackwardTraversal)) {
    // invalid (e.g., if AddTombstones() changed the deletions), so need to
    // reseek
    mode = kBinarySearch;
  }
  switch (mode) {
    case kFullScan:
      assert(!collapse_deletions_);
      // The maintained state (PositionalTombstoneMap::iter) isn't useful when
      // we linear scan from the beginning each time, but we maintain it anyways
      // for consistency.
      tombstone_map_iter = tombstone_map.begin();
      while (tombstone_map_iter != tombstone_map.end()) {
        const auto& tombstone = tombstone_map_iter->second;
        if (icmp_.user_comparator()->Compare(parsed.user_key,
                                             tombstone.start_key_) < 0) {
          break;
        }
        if (parsed.sequence < tombstone.seq_ &&
            icmp_.user_comparator()->Compare(parsed.user_key,
                                             tombstone.end_key_) < 0) {
          return true;
        }
        ++tombstone_map_iter;
      }
      return false;
    case kForwardTraversal:
      assert(collapse_deletions_ && tombstone_map_iter != tombstone_map.end());
      if (tombstone_map_iter == tombstone_map.begin() &&
          icmp_.user_comparator()->Compare(parsed.user_key,
                                           tombstone_map_iter->first) < 0) {
        // before start of deletion intervals
        return false;
      }
      while (std::next(tombstone_map_iter) != tombstone_map.end() &&
             icmp_.user_comparator()->Compare(
                 std::next(tombstone_map_iter)->first, parsed.user_key) <= 0) {
        ++tombstone_map_iter;
      }
      break;
    case kBackwardTraversal:
      assert(collapse_deletions_ && tombstone_map_iter != tombstone_map.end());
      while (tombstone_map_iter != tombstone_map.begin() &&
             icmp_.user_comparator()->Compare(parsed.user_key,
                                              tombstone_map_iter->first) < 0) {
        --tombstone_map_iter;
      }
      if (tombstone_map_iter == tombstone_map.begin() &&
          icmp_.user_comparator()->Compare(parsed.user_key,
                                           tombstone_map_iter->first) < 0) {
        // before start of deletion intervals
        return false;
      }
      break;
    case kBinarySearch:
      assert(collapse_deletions_);
      tombstone_map_iter =
          tombstone_map.upper_bound(parsed.user_key);
      if (tombstone_map_iter == tombstone_map.begin()) {
        // before start of deletion intervals
        return false;
      }
      --tombstone_map_iter;
      break;
  }
  assert(mode != kFullScan);
  assert(tombstone_map_iter != tombstone_map.end() &&
         icmp_.user_comparator()->Compare(tombstone_map_iter->first,
                                          parsed.user_key) <= 0);
  assert(std::next(tombstone_map_iter) == tombstone_map.end() ||
         icmp_.user_comparator()->Compare(
             parsed.user_key, std::next(tombstone_map_iter)->first) < 0);
  return parsed.sequence < tombstone_map_iter->second.seq_;
}

bool RangeDelAggregator::IsRangeOverlapped(const Slice& start,
                                           const Slice& end) {
  // so far only implemented for non-collapsed mode since file ingestion (only
  //  client) doesn't use collapsing
  assert(!collapse_deletions_);
  if (rep_ == nullptr) {
    return false;
  }
  for (const auto& seqnum_and_tombstone_map : rep_->stripe_map_) {
    for (const auto& start_key_and_tombstone :
         seqnum_and_tombstone_map.second.raw_map) {
      const auto& tombstone = start_key_and_tombstone.second;
      if (icmp_.user_comparator()->Compare(start, tombstone.end_key_) < 0 &&
          icmp_.user_comparator()->Compare(tombstone.start_key_, end) <= 0 &&
          icmp_.user_comparator()->Compare(tombstone.start_key_,
                                           tombstone.end_key_) < 0) {
        return true;
      }
    }
  }
  return false;
}

bool RangeDelAggregator::ShouldAddTombstones(
    bool bottommost_level /* = false */) {
  // TODO(andrewkr): can we just open a file and throw it away if it ends up
  // empty after AddToBuilder()? This function doesn't take into subcompaction
  // boundaries so isn't completely accurate.
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
    if (!stripe_map_iter->second.raw_map.empty()) {
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
      } else {
        InvalidateTombstoneMapPositions();
      }
      first_iter = false;
    }
    ParsedInternalKey parsed_key;
    if (!ParseInternalKey(input->key(), &parsed_key)) {
      return Status::Corruption("Unable to parse range tombstone InternalKey");
    }
    RangeTombstone tombstone(parsed_key, input->value());
    AddTombstone(std::move(tombstone));
    input->Next();
  }
  if (!first_iter) {
    rep_->pinned_iters_mgr_.PinIterator(input.release(), false /* arena */);
  }
  return Status::OK();
}

void RangeDelAggregator::InvalidateTombstoneMapPositions() {
  if (rep_ == nullptr) {
    return;
  }
  for (auto stripe_map_iter = rep_->stripe_map_.begin();
       stripe_map_iter != rep_->stripe_map_.end(); ++stripe_map_iter) {
    stripe_map_iter->second.iter = stripe_map_iter->second.raw_map.end();
  }
}

Status RangeDelAggregator::AddTombstone(RangeTombstone tombstone) {
  auto& positional_tombstone_map = GetPositionalTombstoneMap(tombstone.seq_);
  auto& tombstone_map = positional_tombstone_map.raw_map;
  if (collapse_deletions_) {
    // In collapsed mode, we only fill the seq_ field in the TombstoneMap's
    // values. The end_key is unneeded because we assume the tombstone extends
    // until the next tombstone starts. For gaps between real tombstones and
    // for the last real tombstone, we denote end keys by inserting fake
    // tombstones with sequence number zero.
    std::vector<RangeTombstone> new_range_dels{
        tombstone, RangeTombstone(tombstone.end_key_, Slice(), 0)};
    auto new_range_dels_iter = new_range_dels.begin();
    // Position at the first overlapping existing tombstone; if none exists,
    // insert until we find an existing one overlapping a new point
    const Slice* tombstone_map_begin = nullptr;
    if (!tombstone_map.empty()) {
      tombstone_map_begin = &tombstone_map.begin()->first;
    }
    auto last_range_dels_iter = new_range_dels_iter;
    while (new_range_dels_iter != new_range_dels.end() &&
           (tombstone_map_begin == nullptr ||
            icmp_.user_comparator()->Compare(new_range_dels_iter->start_key_,
                                             *tombstone_map_begin) < 0)) {
      tombstone_map.emplace(
          new_range_dels_iter->start_key_,
          RangeTombstone(Slice(), Slice(), new_range_dels_iter->seq_));
      last_range_dels_iter = new_range_dels_iter;
      ++new_range_dels_iter;
    }
    if (new_range_dels_iter == new_range_dels.end()) {
      return Status::OK();
    }
    // above loop advances one too far
    new_range_dels_iter = last_range_dels_iter;
    auto tombstone_map_iter =
        tombstone_map.upper_bound(new_range_dels_iter->start_key_);
    // if nothing overlapped we would've already inserted all the new points
    // and returned early
    assert(tombstone_map_iter != tombstone_map.begin());
    tombstone_map_iter--;

    // untermed_seq is non-kMaxSequenceNumber when we covered an existing point
    // but haven't seen its corresponding endpoint. It's used for (1) deciding
    // whether to forcibly insert the new interval's endpoint; and (2) possibly
    // raising the seqnum for the to-be-inserted element (we insert the max
    // seqnum between the next new interval and the unterminated interval).
    SequenceNumber untermed_seq = kMaxSequenceNumber;
    while (tombstone_map_iter != tombstone_map.end() &&
           new_range_dels_iter != new_range_dels.end()) {
      const Slice *tombstone_map_iter_end = nullptr,
                  *new_range_dels_iter_end = nullptr;
      if (tombstone_map_iter != tombstone_map.end()) {
        auto next_tombstone_map_iter = std::next(tombstone_map_iter);
        if (next_tombstone_map_iter != tombstone_map.end()) {
          tombstone_map_iter_end = &next_tombstone_map_iter->first;
        }
      }
      if (new_range_dels_iter != new_range_dels.end()) {
        auto next_new_range_dels_iter = std::next(new_range_dels_iter);
        if (next_new_range_dels_iter != new_range_dels.end()) {
          new_range_dels_iter_end = &next_new_range_dels_iter->start_key_;
        }
      }

      // our positions in existing/new tombstone collections should always
      // overlap. The non-overlapping cases are handled above and below this
      // loop.
      assert(new_range_dels_iter_end == nullptr ||
             icmp_.user_comparator()->Compare(tombstone_map_iter->first,
                                              *new_range_dels_iter_end) < 0);
      assert(tombstone_map_iter_end == nullptr ||
             icmp_.user_comparator()->Compare(new_range_dels_iter->start_key_,
                                              *tombstone_map_iter_end) < 0);

      int new_to_old_start_cmp = icmp_.user_comparator()->Compare(
          new_range_dels_iter->start_key_, tombstone_map_iter->first);
      // nullptr end means extends infinitely rightwards, set new_to_old_end_cmp
      // accordingly so we can use common code paths later.
      int new_to_old_end_cmp;
      if (new_range_dels_iter_end == nullptr &&
          tombstone_map_iter_end == nullptr) {
        new_to_old_end_cmp = 0;
      } else if (new_range_dels_iter_end == nullptr) {
        new_to_old_end_cmp = 1;
      } else if (tombstone_map_iter_end == nullptr) {
        new_to_old_end_cmp = -1;
      } else {
        new_to_old_end_cmp = icmp_.user_comparator()->Compare(
            *new_range_dels_iter_end, *tombstone_map_iter_end);
      }

      if (new_to_old_start_cmp < 0) {
        // the existing one's left endpoint comes after, so raise/delete it if
        // it's covered.
        if (tombstone_map_iter->second.seq_ < new_range_dels_iter->seq_) {
          untermed_seq = tombstone_map_iter->second.seq_;
          if (tombstone_map_iter != tombstone_map.begin() &&
              std::prev(tombstone_map_iter)->second.seq_ ==
                  new_range_dels_iter->seq_) {
            tombstone_map_iter = tombstone_map.erase(tombstone_map_iter);
            --tombstone_map_iter;
          } else {
            tombstone_map_iter->second.seq_ = new_range_dels_iter->seq_;
          }
        }
      } else if (new_to_old_start_cmp > 0) {
        if (untermed_seq != kMaxSequenceNumber ||
            tombstone_map_iter->second.seq_ < new_range_dels_iter->seq_) {
          auto seq = tombstone_map_iter->second.seq_;
          // need to adjust this element if not intended to span beyond the new
          // element (i.e., was_tombstone_map_iter_raised == true), or if it
          // can be raised
          tombstone_map_iter = tombstone_map.emplace(
              new_range_dels_iter->start_key_,
              RangeTombstone(
                  Slice(), Slice(),
                  std::max(
                      untermed_seq == kMaxSequenceNumber ? 0 : untermed_seq,
                      new_range_dels_iter->seq_)));
          untermed_seq = seq;
        }
      } else {
        // their left endpoints coincide, so raise the existing one if needed
        if (tombstone_map_iter->second.seq_ < new_range_dels_iter->seq_) {
          untermed_seq = tombstone_map_iter->second.seq_;
          tombstone_map_iter->second.seq_ = new_range_dels_iter->seq_;
        }
      }

      // advance whichever one ends earlier, or both if their right endpoints
      // coincide
      if (new_to_old_end_cmp < 0) {
        ++new_range_dels_iter;
      } else if (new_to_old_end_cmp > 0) {
        ++tombstone_map_iter;
        untermed_seq = kMaxSequenceNumber;
      } else {
        ++new_range_dels_iter;
        ++tombstone_map_iter;
        untermed_seq = kMaxSequenceNumber;
      }
    }
    while (new_range_dels_iter != new_range_dels.end()) {
      tombstone_map.emplace(
          new_range_dels_iter->start_key_,
          RangeTombstone(Slice(), Slice(), new_range_dels_iter->seq_));
      ++new_range_dels_iter;
    }
  } else {
    auto start_key = tombstone.start_key_;
    tombstone_map.emplace(start_key, std::move(tombstone));
  }
  return Status::OK();
}

RangeDelAggregator::PositionalTombstoneMap&
RangeDelAggregator::GetPositionalTombstoneMap(SequenceNumber seq) {
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
    CompactionIterationStats* range_del_out_stats /* = nullptr */,
    bool bottommost_level /* = false */) {
  if (rep_ == nullptr) {
    return;
  }
  auto stripe_map_iter = rep_->stripe_map_.begin();
  assert(stripe_map_iter != rep_->stripe_map_.end());
  if (bottommost_level) {
    // TODO(andrewkr): these are counted for each compaction output file, so
    // lots of double-counting.
    if (!stripe_map_iter->second.raw_map.empty()) {
      range_del_out_stats->num_range_del_drop_obsolete +=
          static_cast<int64_t>(stripe_map_iter->second.raw_map.size()) -
          (collapse_deletions_ ? 1 : 0);
      range_del_out_stats->num_record_drop_obsolete +=
          static_cast<int64_t>(stripe_map_iter->second.raw_map.size()) -
          (collapse_deletions_ ? 1 : 0);
    }
    // For the bottommost level, keys covered by tombstones in the first
    // (oldest) stripe have been compacted away, so the tombstones are obsolete.
    ++stripe_map_iter;
  }

  // Note the order in which tombstones are stored is insignificant since we
  // insert them into a std::map on the read path.
  while (stripe_map_iter != rep_->stripe_map_.end()) {
    bool first_added = false;
    for (auto tombstone_map_iter = stripe_map_iter->second.raw_map.begin();
         tombstone_map_iter != stripe_map_iter->second.raw_map.end();
         ++tombstone_map_iter) {
      RangeTombstone tombstone;
      if (collapse_deletions_) {
        auto next_tombstone_map_iter = std::next(tombstone_map_iter);
        if (next_tombstone_map_iter == stripe_map_iter->second.raw_map.end() ||
            tombstone_map_iter->second.seq_ == 0) {
          // it's a sentinel tombstone
          continue;
        }
        tombstone.start_key_ = tombstone_map_iter->first;
        tombstone.end_key_ = next_tombstone_map_iter->first;
        tombstone.seq_ = tombstone_map_iter->second.seq_;
      } else {
        tombstone = tombstone_map_iter->second;
      }
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
        InternalKey smallest_candidate = std::move(ikey_and_end_key.first);
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
    if (!stripe_map_iter->second.raw_map.empty()) {
      return false;
    }
  }
  return true;
}

bool RangeDelAggregator::AddFile(uint64_t file_number) {
  if (rep_ == nullptr) {
    return true;
  }
  return rep_->added_files_.emplace(file_number).second;
}

}  // namespace rocksdb
