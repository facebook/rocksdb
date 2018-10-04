//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <vector>
#include <list>
#include <string>
#include <memory>

#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "table/internal_iterator.h"

namespace rocksdb {

// FragmentedRangeTombstoneIterator converts an InternalIterator of a range-del
// meta block into an iterator over non-overlapping tombstone fragments. The
// tombstone fragmentation process should be more efficient than the range
// tombstone collapsing algorithm in RangeDelAggregator because this leverages
// the internal key ordering already provided by the input iterator. If there
// are few overlaps, creating a FragmentedRangeTombstoneIterator should be
// O(n), while the RangeDelAggregator tombstone collapsing is always O(n log n).
class FragmentedRangeTombstoneIterator {
 public:
  FragmentedRangeTombstoneIterator(
      std::unique_ptr<InternalIterator> unfragmented_tombstones,
      const InternalKeyComparator& icmp,
      SequenceNumber snapshot);
  void SeekToFirst();
  void SeekForPrev(const Slice& target);
  void Next();
  bool Valid() const;
  Slice start_key() const;
  Slice end_key() const;
  SequenceNumber seq() const;
  SequenceNumber MaxCoveringTombstoneSeqnum(const Slice& key);

 private:
  struct FragmentedRangeTombstoneComparator {
    FragmentedRangeTombstoneComparator(const Comparator* c) : cmp(c) {}

    bool operator()(const RangeTombstone& a, const RangeTombstone& b) const {
      return cmp->Compare(a.start_key_, b.start_key_) < 0;
    }

    const Comparator* cmp;
  };

  const FragmentedRangeTombstoneComparator tombstone_cmp_;
  const Comparator* ucmp_;
  std::vector<RangeTombstone> tombstones_;
  std::list<std::string> pinned_slices_;
  std::vector<RangeTombstone>::const_iterator pos_;
  PinnedIteratorsManager pinned_iters_mgr_;
};

}  // namespace rocksdb
