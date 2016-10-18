//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <map>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "db/version_edit.h"
#include "include/rocksdb/comparator.h"
#include "include/rocksdb/types.h"
#include "table/internal_iterator.h"
#include "table/scoped_arena_iterator.h"
#include "table/table_builder.h"
#include "util/kv_map.h"

namespace rocksdb {
class RangeDelAggregator {
 public:
  RangeDelAggregator(const InternalKeyComparator& icmp,
                     const std::vector<SequenceNumber>& snapshots);

  bool ShouldDelete(const Slice& internal_key, bool for_compaction = false);
  bool ShouldAddTombstones(bool bottommost_level = false);
  void AddTombstones(ScopedArenaIterator input);
  void AddTombstones(std::unique_ptr<InternalIterator> input);
  // write tombstones covering a range to a table builder
  // usually don't add to a max-level table builder
  void AddToBuilder(TableBuilder* builder, bool extend_before_min_key,
                    const Slice* next_table_min_key, FileMetaData* meta,
                    bool bottommost_level = false);

 private:
  // Maps tombstone start key -> tombstone object
  typedef std::map<std::string, RangeTombstone, stl_wrappers::LessOfComparator>
      TombstoneMap;
  // Maps snapshot seqnum -> map of tombstones that fall in that stripe, i.e.,
  // their seqnums are greater than the next smaller snapshot's seqnum.
  typedef std::map<SequenceNumber, TombstoneMap> StripeMap;

  void AddTombstones(InternalIterator* input, bool arena);
  StripeMap::iterator GetStripeMapIter(SequenceNumber seq);

  PinnedIteratorsManager pinned_iters_mgr_;
  StripeMap stripe_map_;
  const InternalKeyComparator icmp_;
};
}  // namespace rocksdb
