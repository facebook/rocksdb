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

class RangeDelAggregatorV2 {
 public:
  RangeDelAggregatorV2(const InternalKeyComparator* icmp,
                       SequenceNumber upper_bound);

  void AddTombstones(
      std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter,
      const InternalKey* smallest = nullptr,
      const InternalKey* largest = nullptr);

  void AddUnfragmentedTombstones(std::unique_ptr<InternalIterator> input_iter);

  bool ShouldDelete(const ParsedInternalKey& parsed,
                    RangeDelPositioningMode mode);

  bool IsRangeOverlapped(const Slice& start, const Slice& end);

  // TODO: no-op for now, but won't be once ShouldDelete leverages positioning
  // mode and doesn't re-seek every ShouldDelete
  void InvalidateRangeDelMapPositions() {}

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
  SequenceNumber upper_bound_;

  std::vector<std::unique_ptr<TruncatedRangeDelIterator>> iters_;
  std::list<std::unique_ptr<FragmentedRangeTombstoneList>> pinned_fragments_;
  std::set<uint64_t> files_seen_;

  // TODO: remove once V2 supports exposing tombstone iterators
  std::unique_ptr<RangeDelAggregator> wrapped_range_del_agg;
};

}  // namespace rocksdb
