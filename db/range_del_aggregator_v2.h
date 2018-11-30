//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

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

namespace rocksdb {

class RangeDelAggregatorV2;

class TruncatedRangeDelIterator {
 public:
  TruncatedRangeDelIterator(
      std::unique_ptr<FragmentedRangeTombstoneIterator> iter,
      const InternalKeyComparator* icmp, const InternalKey* smallest,
      const InternalKey* largest);

  bool Valid() const;

  void Next();
  void Prev();

  // Seeks to the tombstone with the highest viisble sequence number that covers
  // target (a user key). If no such tombstone exists, the position will be at
  // the earliest tombstone that ends after target.
  void Seek(const Slice& target);

  // Seeks to the tombstone with the highest viisble sequence number that covers
  // target (a user key). If no such tombstone exists, the position will be at
  // the latest tombstone that starts before target.
  void SeekForPrev(const Slice& target);

  void SeekToFirst();
  void SeekToLast();

  ParsedInternalKey start_key() const {
    return (smallest_ == nullptr ||
            icmp_->Compare(*smallest_, iter_->parsed_start_key()) <= 0)
               ? iter_->parsed_start_key()
               : *smallest_;
  }

  ParsedInternalKey end_key() const {
    return (largest_ == nullptr ||
            icmp_->Compare(iter_->parsed_end_key(), *largest_) <= 0)
               ? iter_->parsed_end_key()
               : *largest_;
  }

  SequenceNumber seq() const { return iter_->seq(); }

 private:
  std::unique_ptr<FragmentedRangeTombstoneIterator> iter_;
  const InternalKeyComparator* icmp_;
  const ParsedInternalKey* smallest_ = nullptr;
  const ParsedInternalKey* largest_ = nullptr;
  std::list<ParsedInternalKey> pinned_bounds_;
};

struct SeqMaxComparator {
  bool operator()(const TruncatedRangeDelIterator* a,
                  const TruncatedRangeDelIterator* b) const {
    return a->seq() > b->seq();
  }
};

class ForwardRangeDelIterator {
 public:
  ForwardRangeDelIterator(
      const InternalKeyComparator* icmp,
      const std::vector<std::unique_ptr<TruncatedRangeDelIterator>>* iters);

  bool ShouldDelete(const ParsedInternalKey& parsed);
  void Invalidate();

 private:
  using ActiveSeqSet =
      std::multiset<TruncatedRangeDelIterator*, SeqMaxComparator>;

  struct StartKeyMinComparator {
    explicit StartKeyMinComparator(const InternalKeyComparator* c) : icmp(c) {}

    bool operator()(const TruncatedRangeDelIterator* a,
                    const TruncatedRangeDelIterator* b) const {
      return icmp->Compare(a->start_key(), b->start_key()) > 0;
    }

    const InternalKeyComparator* icmp;
  };
  struct EndKeyMinComparator {
    explicit EndKeyMinComparator(const InternalKeyComparator* c) : icmp(c) {}

    bool operator()(const ActiveSeqSet::const_iterator& a,
                    const ActiveSeqSet::const_iterator& b) const {
      return icmp->Compare((*a)->end_key(), (*b)->end_key()) > 0;
    }

    const InternalKeyComparator* icmp;
  };

  void PushIter(TruncatedRangeDelIterator* iter,
                const ParsedInternalKey& parsed) {
    if (!iter->Valid()) {
      // The iterator has been fully consumed, so we don't need to add it to
      // either of the heaps.
    } else if (icmp_->Compare(parsed, iter->start_key()) < 0) {
      PushInactiveIter(iter);
    } else {
      PushActiveIter(iter);
    }
  }

  void PushActiveIter(TruncatedRangeDelIterator* iter) {
    auto seq_pos = active_seqnums_.insert(iter);
    active_iters_.push(seq_pos);
  }

  TruncatedRangeDelIterator* PopActiveIter() {
    auto active_top = active_iters_.top();
    auto iter = *active_top;
    active_iters_.pop();
    active_seqnums_.erase(active_top);
    return iter;
  }

  void PushInactiveIter(TruncatedRangeDelIterator* iter) {
    inactive_iters_.push(iter);
  }

  TruncatedRangeDelIterator* PopInactiveIter() {
    auto* iter = inactive_iters_.top();
    inactive_iters_.pop();
    return iter;
  }

  const InternalKeyComparator* icmp_;
  const std::vector<std::unique_ptr<TruncatedRangeDelIterator>>* iters_;
  size_t unused_idx_;
  ActiveSeqSet active_seqnums_;
  BinaryHeap<ActiveSeqSet::const_iterator, EndKeyMinComparator> active_iters_;
  BinaryHeap<TruncatedRangeDelIterator*, StartKeyMinComparator> inactive_iters_;
};

class ReverseRangeDelIterator {
 public:
  ReverseRangeDelIterator(
      const InternalKeyComparator* icmp,
      const std::vector<std::unique_ptr<TruncatedRangeDelIterator>>* iters);

  bool ShouldDelete(const ParsedInternalKey& parsed);
  void Invalidate();

 private:
  using ActiveSeqSet =
      std::multiset<TruncatedRangeDelIterator*, SeqMaxComparator>;

  struct EndKeyMaxComparator {
    explicit EndKeyMaxComparator(const InternalKeyComparator* c) : icmp(c) {}

    bool operator()(const TruncatedRangeDelIterator* a,
                    const TruncatedRangeDelIterator* b) const {
      return icmp->Compare(a->end_key(), b->end_key()) < 0;
    }

    const InternalKeyComparator* icmp;
  };
  struct StartKeyMaxComparator {
    explicit StartKeyMaxComparator(const InternalKeyComparator* c) : icmp(c) {}

    bool operator()(const ActiveSeqSet::const_iterator& a,
                    const ActiveSeqSet::const_iterator& b) const {
      return icmp->Compare((*a)->start_key(), (*b)->start_key()) < 0;
    }

    const InternalKeyComparator* icmp;
  };

  void PushIter(TruncatedRangeDelIterator* iter,
                const ParsedInternalKey& parsed) {
    if (!iter->Valid()) {
      // The iterator has been fully consumed, so we don't need to add it to
      // either of the heaps.
    } else if (icmp_->Compare(iter->end_key(), parsed) <= 0) {
      PushInactiveIter(iter);
    } else {
      PushActiveIter(iter);
    }
  }

  void PushActiveIter(TruncatedRangeDelIterator* iter) {
    auto seq_pos = active_seqnums_.insert(iter);
    active_iters_.push(seq_pos);
  }

  TruncatedRangeDelIterator* PopActiveIter() {
    auto active_top = active_iters_.top();
    auto iter = *active_top;
    active_iters_.pop();
    active_seqnums_.erase(active_top);
    return iter;
  }

  void PushInactiveIter(TruncatedRangeDelIterator* iter) {
    inactive_iters_.push(iter);
  }

  TruncatedRangeDelIterator* PopInactiveIter() {
    auto* iter = inactive_iters_.top();
    inactive_iters_.pop();
    return iter;
  }

  const InternalKeyComparator* icmp_;
  const std::vector<std::unique_ptr<TruncatedRangeDelIterator>>* iters_;
  size_t unused_idx_;
  ActiveSeqSet active_seqnums_;
  BinaryHeap<ActiveSeqSet::const_iterator, StartKeyMaxComparator> active_iters_;
  BinaryHeap<TruncatedRangeDelIterator*, EndKeyMaxComparator> inactive_iters_;
};

class RangeDelAggregatorV2 {
 public:
  RangeDelAggregatorV2(const InternalKeyComparator* icmp,
                       SequenceNumber upper_bound);

  void AddTombstones(
      std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter,
      const InternalKey* smallest = nullptr,
      const InternalKey* largest = nullptr);

  bool ShouldDelete(const ParsedInternalKey& parsed,
                    RangeDelPositioningMode mode);

  bool IsRangeOverlapped(const Slice& start, const Slice& end);

  void InvalidateRangeDelMapPositions() {
    forward_iter_.Invalidate();
    reverse_iter_.Invalidate();
  }

  bool IsEmpty() const { return iters_.empty(); }
  bool AddFile(uint64_t file_number) {
    return files_seen_.insert(file_number).second;
  }

  // Adaptor method to pass calls through to an old-style RangeDelAggregator.
  // Will be removed once this new version supports an iterator that can be used
  // during flush/compaction.
  RangeDelAggregator* DelegateToRangeDelAggregator(
      const std::vector<SequenceNumber>& snapshots) {
    wrapped_range_del_agg.reset(new RangeDelAggregator(
        *icmp_, snapshots, true /* collapse_deletions */));
    return wrapped_range_del_agg.get();
  }

  std::unique_ptr<RangeDelIterator> NewIterator() {
    assert(wrapped_range_del_agg != nullptr);
    return wrapped_range_del_agg->NewIterator();
  }

 private:
  const InternalKeyComparator* icmp_;

  std::vector<std::unique_ptr<TruncatedRangeDelIterator>> iters_;
  std::set<uint64_t> files_seen_;

  ForwardRangeDelIterator forward_iter_;
  ReverseRangeDelIterator reverse_iter_;

  // TODO: remove once V2 supports exposing tombstone iterators
  std::unique_ptr<RangeDelAggregator> wrapped_range_del_agg;
};

}  // namespace rocksdb
