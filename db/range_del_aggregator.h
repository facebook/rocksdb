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

// A RangeDelAggregator aggregates range deletion tombstones as they are
// encountered in memtables/SST files. It provides methods that check whether a
// key is covered by range tombstones or write the relevant tombstones to a new
// SST file.
class RangeDelAggregator {
 public:
  // @param snapshots These are used to organize the tombstones into snapshot
  //    stripes, which is the seqnum range between consecutive snapshots,
  //    including the higher snapshot and excluding the lower one. Currently,
  //    this is used by ShouldDelete() to prevent deletion of keys that are
  //    covered by range tombstones in other snapshot stripes. In case of writes
  //    (flush/compaction), all DB snapshots are provided such that no keys are
  //    removed that are uncovered according to any DB snapshot. In case of read
  //    (get/iterator), only the user snapshot is provided such that the seqnum
  //    space is divided into two stripes, where only tombstones in the older
  //    stripe are considered by ShouldDelete().
  RangeDelAggregator(const InternalKeyComparator& icmp,
                     const std::vector<SequenceNumber>& snapshots);

  // Returns whether the key should be deleted, which is the case when it is
  // covered by a range tombstone residing in the same snapshot stripe.
  bool ShouldDelete(const Slice& internal_key, bool for_compaction = false);
  bool ShouldAddTombstones(bool bottommost_level = false);

  // Adds tombstones to the tombstone aggregation structure maintained by this
  // object.
  // @return non-OK status if any of the tombstone keys are corrupted.
  Status AddTombstones(ScopedArenaIterator input);
  Status AddTombstones(std::unique_ptr<InternalIterator> input);

  // Writes tombstones covering a range to a table builder.
  // @param extend_before_min_key If true, the range of tombstones to be added
  //    to the TableBuilder starts from the beginning of the key-range;
  //    otherwise, it starts from meta->smallest.
  // @param next_table_min_key If nullptr, the range of tombstones to be added
  //    to the TableBuilder ends at the end of the key-range; otherwise, it
  //    ends at next_table_min_key.
  // @param meta The file's metadata. We modify the begin and end keys according
  //    to the range tombstones added to this file such that the read path does
  //    not miss range tombstones that cover gaps before/after/between files in
  //    a level.
  // @param bottommost_level If true, we will filter out any tombstones
  //    belonging to the oldest snapshot stripe, because all keys potentially
  //    covered by this tombstone are guaranteed to have been deleted by
  //    compaction.
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

  Status AddTombstones(InternalIterator* input, bool arena);
  StripeMap::iterator GetStripeMapIter(SequenceNumber seq);

  PinnedIteratorsManager pinned_iters_mgr_;
  StripeMap stripe_map_;
  const InternalKeyComparator icmp_;
};
}  // namespace rocksdb
