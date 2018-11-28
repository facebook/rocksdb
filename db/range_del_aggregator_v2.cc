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

ForwardRangeDelIterator::ForwardRangeDelIterator(
    const InternalKeyComparator* icmp,
    const std::vector<std::unique_ptr<TruncatedRangeDelIterator>>* iters)
    : icmp_(icmp),
      iters_(iters),
      unused_idx_(0),
      active_seqnums_(SeqMaxComparator()),
      active_iters_(EndKeyMinComparator(icmp)),
      inactive_iters_(StartKeyMinComparator(icmp)) {}

bool ForwardRangeDelIterator::ShouldDelete(const ParsedInternalKey& parsed) {
  assert(iters_ != nullptr);
  // Pick up previously unseen iterators.
  for (auto it = std::next(iters_->begin(), unused_idx_); it != iters_->end();
       ++it, ++unused_idx_) {
    auto& iter = *it;
    iter->Seek(parsed.user_key);
    PushIter(iter.get(), parsed);
    assert(active_iters_.size() == active_seqnums_.size());
  }

  // Move active iterators that end before parsed.
  while (!active_iters_.empty() &&
         icmp_->Compare((*active_iters_.top())->end_key(), parsed) <= 0) {
    TruncatedRangeDelIterator* iter = PopActiveIter();
    do {
      iter->Next();
    } while (iter->Valid() && icmp_->Compare(iter->end_key(), parsed) <= 0);
    PushIter(iter, parsed);
    assert(active_iters_.size() == active_seqnums_.size());
  }

  // Move inactive iterators that start before parsed.
  while (!inactive_iters_.empty() &&
         icmp_->Compare(inactive_iters_.top()->start_key(), parsed) <= 0) {
    TruncatedRangeDelIterator* iter = PopInactiveIter();
    while (iter->Valid() && icmp_->Compare(iter->end_key(), parsed) <= 0) {
      iter->Next();
    }
    PushIter(iter, parsed);
    assert(active_iters_.size() == active_seqnums_.size());
  }

  return active_seqnums_.empty()
             ? false
             : (*active_seqnums_.begin())->seq() > parsed.sequence;
}

void ForwardRangeDelIterator::Invalidate() {
  unused_idx_ = 0;
  active_iters_.clear();
  active_seqnums_.clear();
  inactive_iters_.clear();
}

ReverseRangeDelIterator::ReverseRangeDelIterator(
    const InternalKeyComparator* icmp,
    const std::vector<std::unique_ptr<TruncatedRangeDelIterator>>* iters)
    : icmp_(icmp),
      iters_(iters),
      unused_idx_(0),
      active_seqnums_(SeqMaxComparator()),
      active_iters_(StartKeyMaxComparator(icmp)),
      inactive_iters_(EndKeyMaxComparator(icmp)) {}

bool ReverseRangeDelIterator::ShouldDelete(const ParsedInternalKey& parsed) {
  assert(iters_ != nullptr);
  // Pick up previously unseen iterators.
  for (auto it = std::next(iters_->begin(), unused_idx_); it != iters_->end();
       ++it, ++unused_idx_) {
    auto& iter = *it;
    iter->SeekForPrev(parsed.user_key);
    PushIter(iter.get(), parsed);
    assert(active_iters_.size() == active_seqnums_.size());
  }

  // Move active iterators that start after parsed.
  while (!active_iters_.empty() &&
         icmp_->Compare(parsed, (*active_iters_.top())->start_key()) < 0) {
    TruncatedRangeDelIterator* iter = PopActiveIter();
    do {
      iter->Prev();
    } while (iter->Valid() && icmp_->Compare(parsed, iter->start_key()) < 0);
    PushIter(iter, parsed);
    assert(active_iters_.size() == active_seqnums_.size());
  }

  // Move inactive iterators that end after parsed.
  while (!inactive_iters_.empty() &&
         icmp_->Compare(parsed, inactive_iters_.top()->end_key()) < 0) {
    TruncatedRangeDelIterator* iter = PopInactiveIter();
    while (iter->Valid() && icmp_->Compare(parsed, iter->start_key()) < 0) {
      iter->Prev();
    }
    PushIter(iter, parsed);
    assert(active_iters_.size() == active_seqnums_.size());
  }

  return active_seqnums_.empty()
             ? false
             : (*active_seqnums_.begin())->seq() > parsed.sequence;
}

void ReverseRangeDelIterator::Invalidate() {
  unused_idx_ = 0;
  active_iters_.clear();
  active_seqnums_.clear();
  inactive_iters_.clear();
}

RangeDelAggregatorV2::RangeDelAggregatorV2(const InternalKeyComparator* icmp,
                                           SequenceNumber /* upper_bound */)
    : icmp_(icmp), forward_iter_(icmp, &iters_), reverse_iter_(icmp, &iters_) {}

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

bool RangeDelAggregatorV2::ShouldDelete(const ParsedInternalKey& parsed,
                                        RangeDelPositioningMode mode) {
  if (wrapped_range_del_agg != nullptr) {
    return wrapped_range_del_agg->ShouldDelete(parsed, mode);
  }

  switch (mode) {
    case RangeDelPositioningMode::kForwardTraversal:
      reverse_iter_.Invalidate();
      return forward_iter_.ShouldDelete(parsed);
    case RangeDelPositioningMode::kBackwardTraversal:
      forward_iter_.Invalidate();
      return reverse_iter_.ShouldDelete(parsed);
    default:
      assert(false);
      return false;
  }
}

bool RangeDelAggregatorV2::IsRangeOverlapped(const Slice& start,
                                             const Slice& end) {
  assert(wrapped_range_del_agg == nullptr);
  InvalidateRangeDelMapPositions();

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
