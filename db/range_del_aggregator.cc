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
    : icmp_(icmp) {
  pinned_iters_mgr_.StartPinning();
  for (auto snapshot : snapshots) {
    stripe_map_.emplace(
        snapshot,
        TombstoneMap(stl_wrappers::LessOfComparator(icmp_.user_comparator())));
  }
  // Data newer than any snapshot falls in this catch-all stripe
  stripe_map_.emplace(kMaxSequenceNumber, TombstoneMap());
}

bool RangeDelAggregator::ShouldDelete(const Slice& internal_key,
                                      bool for_compaction /* = false */) {
  ParsedInternalKey parsed;
  if (!ParseInternalKey(internal_key, &parsed)) {
    assert(false);
  }
  assert(IsValueType(parsed.type));

  // Starting point is the snapshot stripe in which the key lives, then need to
  // search all earlier stripes too, unless it's for compaction.
  for (auto stripe_map_iter = GetStripeMapIter(parsed.sequence);
       stripe_map_iter != stripe_map_.end(); ++stripe_map_iter) {
    const auto& tombstone_map = stripe_map_iter->second;
    for (const auto& start_key_and_tombstone : tombstone_map) {
      const auto& tombstone = start_key_and_tombstone.second;
      if (icmp_.user_comparator()->Compare(parsed.user_key,
                                           tombstone.start_key_) < 0) {
        break;
      }
      if (parsed.sequence < tombstone.seq_ &&
          icmp_.user_comparator()->Compare(parsed.user_key,
                                           tombstone.end_key_) <= 0) {
        return true;
      }
    }
    if (for_compaction) {
      break;
    }
  }
  return false;
}

bool RangeDelAggregator::ShouldAddTombstones(
    bool bottommost_level /* = false */) {
  auto stripe_map_iter = stripe_map_.begin();
  assert(stripe_map_iter != stripe_map_.end());
  if (bottommost_level) {
    // For the bottommost level, keys covered by tombstones in the first
    // (oldest) stripe have been compacted away, so the tombstones are obsolete.
    ++stripe_map_iter;
  }
  while (stripe_map_iter != stripe_map_.end()) {
    if (!stripe_map_iter->second.empty()) {
      return true;
    }
    ++stripe_map_iter;
  }
  return false;
}

void RangeDelAggregator::AddTombstones(ScopedArenaIterator input) {
  AddTombstones(input.release(), true /* arena */);
}

void RangeDelAggregator::AddTombstones(
    std::unique_ptr<InternalIterator> input) {
  AddTombstones(input.release(), false /* arena */);
}

void RangeDelAggregator::AddTombstones(InternalIterator* input, bool arena) {
  pinned_iters_mgr_.PinIterator(input, arena);
  input->SeekToFirst();
  while (input->Valid()) {
    RangeTombstone tombstone(input->key(), input->value());
    auto& tombstone_map = GetStripeMapIter(tombstone.seq_)->second;
    tombstone_map.emplace(tombstone.start_key_.ToString(),
                          std::move(tombstone));
    input->Next();
  }
}

RangeDelAggregator::StripeMap::iterator RangeDelAggregator::GetStripeMapIter(
    SequenceNumber seq) {
  // The stripe includes seqnum for the snapshot above and excludes seqnum for
  // the snapshot below.
  StripeMap::iterator iter;
  if (seq > 0) {
    // upper_bound() checks strict inequality so need to subtract one
    iter = stripe_map_.upper_bound(seq - 1);
  } else {
    iter = stripe_map_.begin();
  }
  // catch-all stripe justifies this assertion in either of above cases
  assert(iter != stripe_map_.end());
  return iter;
}

// TODO(andrewkr): We should implement an iterator over range tombstones in our
// map. It'd enable compaction to open tables on-demand, i.e., only once range
// tombstones are known to be available, without the code duplication we have
// in ShouldAddTombstones(). It'll also allow us to move the table-modifying
// code into more coherent places: CompactionJob and BuildTable().
void RangeDelAggregator::AddToBuilder(TableBuilder* builder,
                                      bool extend_before_min_key,
                                      const Slice* next_table_min_key,
                                      FileMetaData* meta,
                                      bool bottommost_level /* = false */) {
  auto stripe_map_iter = stripe_map_.begin();
  assert(stripe_map_iter != stripe_map_.end());
  if (bottommost_level) {
    // For the bottommost level, keys covered by tombstones in the first
    // (oldest) stripe have been compacted away, so the tombstones are obsolete.
    ++stripe_map_iter;
  }

  // Note the order in which tombstones are stored is insignificant since we
  // insert them into a std::map on the read path.
  bool first_added = false;
  while (stripe_map_iter != stripe_map_.end()) {
    for (const auto& start_key_and_tombstone : stripe_map_iter->second) {
      const auto& tombstone = start_key_and_tombstone.second;
      if (next_table_min_key != nullptr &&
          icmp_.user_comparator()->Compare(*next_table_min_key,
                                           tombstone.start_key_) < 0) {
        // Tombstones starting after next_table_min_key only need to be included
        // in the next table.
        break;
      }
      if (!extend_before_min_key && meta->smallest.size() != 0 &&
          icmp_.user_comparator()->Compare(tombstone.end_key_,
                                           meta->smallest.user_key()) < 0) {
        // Tombstones ending before this table's smallest key can conditionally
        // be excluded, e.g., when this table is a non-first compaction output,
        // we know such tombstones are included in the previous table. In that
        // case extend_before_min_key would be false.
        continue;
      }

      auto ikey_and_end_key = tombstone.Serialize();
      builder->Add(ikey_and_end_key.first.Encode(), ikey_and_end_key.second);
      if (!first_added) {
        first_added = true;
        if (extend_before_min_key &&
            (meta->smallest.size() == 0 ||
             icmp_.Compare(ikey_and_end_key.first, meta->smallest) < 0)) {
          meta->smallest = ikey_and_end_key.first;
        }
      }
      auto end_ikey = tombstone.SerializeEndKey();
      if (meta->largest.size() == 0 ||
          icmp_.Compare(meta->largest, end_ikey) < 0) {
        if (next_table_min_key != nullptr &&
            icmp_.Compare(*next_table_min_key, end_ikey.Encode()) < 0) {
          // Pretend the largest key has the same user key as the min key in the
          // following table in order for files to appear key-space partitioned.
          // Choose highest seqnum so this file's largest comes before the next
          // file's smallest. The fake seqnum is OK because the read path's
          // file-picking code only considers the user key portion.
          //
          // Note Seek() also creates InternalKey with (user_key,
          // kMaxSequenceNumber), but with kTypeDeletion (0x7) instead of
          // kTypeRangeDeletion (0xF), so the range tombstone comes before the
          // Seek() key in InternalKey's ordering. So Seek() will look in the
          // next file for the user key.
          ParsedInternalKey parsed;
          ParseInternalKey(*next_table_min_key, &parsed);
          meta->largest = InternalKey(parsed.user_key, kMaxSequenceNumber,
                                      kTypeRangeDeletion);
        } else {
          meta->largest = std::move(end_ikey);
        }
      }
      meta->smallest_seqno = std::min(meta->smallest_seqno, tombstone.seq_);
      meta->largest_seqno = std::max(meta->largest_seqno, tombstone.seq_);
    }
    ++stripe_map_iter;
  }
}

}  // namespace rocksdb
