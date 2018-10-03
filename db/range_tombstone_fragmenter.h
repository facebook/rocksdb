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

// TODO: handle snapshots (i.e. keep track of snapshot as high water mark for
// seqnums during a read)
class FragmentedRangeTombstoneIterator {
 public:
  FragmentedRangeTombstoneIterator(
      std::unique_ptr<InternalIterator> unfragmented_tombstones,
      const InternalKeyComparator& icmp);
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
      int start_cmp = cmp->Compare(a.start_key_, b.start_key_);
      if (start_cmp < 0) {
        return true;
      } else if (start_cmp > 0) {
        return false;
      } else if (a.seq_ > b.seq_) {
        return true;
      }
      return false;
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
