//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_del_aggregator_v2.h"

#include "db/compaction_iteration_stats.h"
#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "db/range_del_aggregator.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/version_edit.h"
#include "include/rocksdb/comparator.h"
#include "include/rocksdb/types.h"
#include "table/internal_iterator.h"
#include "table/scoped_arena_iterator.h"
#include "table/table_builder.h"
#include "util/heap.h"
#include "util/kv_map.h"
#include "util/vector_iterator.h"

namespace rocksdb {

TruncatedRangeDelIterator::TruncatedRangeDelIterator(
    std::unique_ptr<FragmentedRangeTombstoneIterator> iter,
    const InternalKeyComparator* icmp, const InternalKey* smallest,
    const InternalKey* largest)
    : iter_(std::move(iter)), icmp_(icmp) {
  if (smallest != nullptr) {
    pinned_bounds_.emplace_back();
    auto& parsed_smallest = pinned_bounds_.back();
    if (!ParseInternalKey(smallest->Encode(), &parsed_smallest)) {
      assert(false);
    }
    smallest_ = &parsed_smallest;
  }
  if (largest != nullptr) {
    pinned_bounds_.emplace_back();
    auto& parsed_largest = pinned_bounds_.back();
    if (!ParseInternalKey(largest->Encode(), &parsed_largest)) {
      assert(false);
    }
    if (parsed_largest.type == kTypeRangeDeletion &&
        parsed_largest.sequence == kMaxSequenceNumber) {
      // The file boundary has been artificially extended by a range tombstone.
      // We do not need to adjust largest to properly truncate range
      // tombstones that extend past the boundary.
    } else if (parsed_largest.sequence == 0) {
      // The largest key in the sstable has a sequence number of 0. Since we
      // guarantee that no internal keys with the same user key and sequence
      // number can exist in a DB, we know that the largest key in this sstable
      // cannot exist as the smallest key in the next sstable. This further
      // implies that no range tombstone in this sstable covers largest;
      // otherwise, the file boundary would have been artificially extended.
      //
      // Therefore, we will never truncate a range tombstone at largest, so we
      // can leave it unchanged.
    } else {
      // The same user key may straddle two sstable boundaries. To ensure that
      // the truncated end key can cover the largest key in this sstable, reduce
      // its sequence number by 1.
      parsed_largest.sequence -= 1;
    }
    largest_ = &parsed_largest;
  }
}

bool TruncatedRangeDelIterator::Valid() const {
  return iter_->Valid() &&
         (smallest_ == nullptr ||
          icmp_->Compare(*smallest_, iter_->parsed_end_key()) < 0) &&
         (largest_ == nullptr ||
          icmp_->Compare(iter_->parsed_start_key(), *largest_) < 0);
}

void TruncatedRangeDelIterator::Next() { iter_->TopNext(); }

void TruncatedRangeDelIterator::Prev() { iter_->TopPrev(); }

// NOTE: target is a user key
void TruncatedRangeDelIterator::Seek(const Slice& target) {
  if (largest_ != nullptr &&
      icmp_->Compare(*largest_, ParsedInternalKey(target, kMaxSequenceNumber,
                                                  kTypeRangeDeletion)) <= 0) {
    iter_->Invalidate();
    return;
  }
  iter_->Seek(target);
}

// NOTE: target is a user key
void TruncatedRangeDelIterator::SeekForPrev(const Slice& target) {
  if (smallest_ != nullptr &&
      icmp_->Compare(ParsedInternalKey(target, 0, kTypeRangeDeletion),
                     *smallest_) < 0) {
    iter_->Invalidate();
    return;
  }
  iter_->SeekForPrev(target);
}

void TruncatedRangeDelIterator::SeekToFirst() { iter_->SeekToTopFirst(); }

void TruncatedRangeDelIterator::SeekToLast() { iter_->SeekToTopLast(); }

RangeDelAggregatorV2::RangeDelAggregatorV2(const InternalKeyComparator* icmp,
                                           SequenceNumber upper_bound)
    : icmp_(icmp), upper_bound_(upper_bound) {}

void RangeDelAggregatorV2::AddTombstones(
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter,
    const InternalKey* smallest, const InternalKey* largest) {
  if (input_iter == nullptr || input_iter->empty()) {
    return;
  }
  if (wrapped_range_del_agg != nullptr) {
    wrapped_range_del_agg->AddTombstones(std::move(input_iter), smallest,
                                         largest);
    // TODO: this eats the status of the wrapped call; may want to propagate it
    return;
  }
  iters_.emplace_back(new TruncatedRangeDelIterator(std::move(input_iter),
                                                    icmp_, smallest, largest));
}

void RangeDelAggregatorV2::AddUnfragmentedTombstones(
    std::unique_ptr<InternalIterator> input_iter) {
  assert(wrapped_range_del_agg == nullptr);
  if (input_iter == nullptr) {
    return;
  }
  pinned_fragments_.emplace_back(new FragmentedRangeTombstoneList(
      std::move(input_iter), *icmp_, false /* one_time_use */));
  auto fragmented_iter = new FragmentedRangeTombstoneIterator(
      pinned_fragments_.back().get(), upper_bound_, *icmp_);
  AddTombstones(
      std::unique_ptr<FragmentedRangeTombstoneIterator>(fragmented_iter));
}

bool RangeDelAggregatorV2::ShouldDelete(const ParsedInternalKey& parsed,
                                        RangeDelPositioningMode mode) {
  if (wrapped_range_del_agg != nullptr) {
    return wrapped_range_del_agg->ShouldDelete(parsed, mode);
  }
  // TODO: avoid re-seeking every call
  for (auto& iter : iters_) {
    iter->Seek(parsed.user_key);
    if (iter->Valid() && icmp_->Compare(iter->start_key(), parsed) <= 0 &&
        iter->seq() > parsed.sequence) {
      return true;
    }
  }
  return false;
}

bool RangeDelAggregatorV2::IsRangeOverlapped(const Slice& start,
                                             const Slice& end) {
  assert(wrapped_range_del_agg == nullptr);

  // Set the internal start/end keys so that:
  // - if start_ikey has the same user key and sequence number as the current
  // end key, start_ikey will be considered greater; and
  // - if end_ikey has the same user key and sequence number as the current
  // start key, end_ikey will be considered greater.
  ParsedInternalKey start_ikey(start, kMaxSequenceNumber,
                               static_cast<ValueType>(0));
  ParsedInternalKey end_ikey(end, 0, static_cast<ValueType>(0));
  for (auto& iter : iters_) {
    bool checked_candidate_tombstones = false;
    for (iter->SeekForPrev(start);
         iter->Valid() && icmp_->Compare(iter->start_key(), end_ikey) <= 0;
         iter->Next()) {
      checked_candidate_tombstones = true;
      if (icmp_->Compare(start_ikey, iter->end_key()) < 0 &&
          icmp_->Compare(iter->start_key(), end_ikey) <= 0) {
        return true;
      }
    }

    if (!checked_candidate_tombstones) {
      // Do an additional check for when the end of the range is the begin key
      // of a tombstone, which we missed earlier since SeekForPrev'ing to the
      // start was invalid.
      iter->SeekForPrev(end);
      if (iter->Valid() && icmp_->Compare(start_ikey, iter->end_key()) < 0 &&
          icmp_->Compare(iter->start_key(), end_ikey) <= 0) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace rocksdb
