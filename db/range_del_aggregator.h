//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <string>
#include <vector>

#include "db/compaction_iteration_stats.h"
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
  //    covered by range tombstones in other snapshot stripes. This constructor
  //    is used for writes (flush/compaction). All DB snapshots are provided
  //    such that no keys are removed that are uncovered according to any DB
  //    snapshot.
  // Note this overload does not lazily initialize Rep.
  RangeDelAggregator(const InternalKeyComparator& icmp,
                     const std::vector<SequenceNumber>& snapshots,
                     bool collapse_deletions = true);

  // @param upper_bound Similar to snapshots above, except with a single
  //    snapshot, which allows us to store the snapshot on the stack and defer
  //    initialization of heap-allocating members (in Rep) until the first range
  //    deletion is encountered. This constructor is used in case of reads (get/
  //    iterator), for which only the user snapshot (upper_bound) is provided
  //    such that the seqnum space is divided into two stripes. Only the older
  //    stripe will be used by ShouldDelete().
  RangeDelAggregator(const InternalKeyComparator& icmp,
                     SequenceNumber upper_bound,
                     bool collapse_deletions = false);

  // We maintain position in the tombstone map across calls to ShouldDelete. The
  // caller may wish to specify a mode to optimize positioning the iterator
  // during the next call to ShouldDelete. The non-kFullScan modes are only
  // available when deletion collapsing is enabled.
  //
  // For example, if we invoke Next() on an iterator, kForwardTraversal should
  // be specified to advance one-by-one through deletions until one is found
  // with its interval containing the key. This will typically be faster than
  // doing a full binary search (kBinarySearch).
  enum RangePositioningMode {
    kFullScan,  // used iff collapse_deletions_ == false
    kForwardTraversal,
    kBackwardTraversal,
    kBinarySearch,
  };

  // Returns whether the key should be deleted, which is the case when it is
  // covered by a range tombstone residing in the same snapshot stripe.
  // @param mode If collapse_deletions_ is true, this dictates how we will find
  //             the deletion whose interval contains this key. Otherwise, its
  //             value must be kFullScan indicating linear scan from beginning..
  bool ShouldDelete(const ParsedInternalKey& parsed,
                    RangePositioningMode mode = kFullScan);
  bool ShouldDelete(const Slice& internal_key,
                    RangePositioningMode mode = kFullScan);
  bool ShouldAddTombstones(bool bottommost_level = false);

  // Adds tombstones to the tombstone aggregation structure maintained by this
  // object.
  // @return non-OK status if any of the tombstone keys are corrupted.
  Status AddTombstones(std::unique_ptr<InternalIterator> input);

  // Resets iterators maintained across calls to ShouldDelete(). This may be
  // called when the tombstones change, or the owner may call explicitly, e.g.,
  // if it's an iterator that just seeked to an arbitrary position. The effect
  // of invalidation is that the following call to ShouldDelete() will binary
  // search for its tombstone.
  void InvalidateTombstoneMapPositions();

  // Writes tombstones covering a range to a table builder.
  // @param extend_before_min_key If true, the range of tombstones to be added
  //    to the TableBuilder starts from the beginning of the key-range;
  //    otherwise, it starts from meta->smallest.
  // @param lower_bound/upper_bound Any range deletion with [start_key, end_key)
  //    that overlaps the target range [*lower_bound, *upper_bound) is added to
  //    the builder. If lower_bound is nullptr, the target range extends
  //    infinitely to the left. If upper_bound is nullptr, the target range
  //    extends infinitely to the right. If both are nullptr, the target range
  //    extends infinitely in both directions, i.e., all range deletions are
  //    added to the builder.
  // @param meta The file's metadata. We modify the begin and end keys according
  //    to the range tombstones added to this file such that the read path does
  //    not miss range tombstones that cover gaps before/after/between files in
  //    a level. lower_bound/upper_bound above constrain how far file boundaries
  //    can be extended.
  // @param bottommost_level If true, we will filter out any tombstones
  //    belonging to the oldest snapshot stripe, because all keys potentially
  //    covered by this tombstone are guaranteed to have been deleted by
  //    compaction.
  void AddToBuilder(TableBuilder* builder, const Slice* lower_bound,
                    const Slice* upper_bound, FileMetaData* meta,
                    CompactionIterationStats* range_del_out_stats = nullptr,
                    bool bottommost_level = false);
  bool IsEmpty();

 private:
  // Maps tombstone user start key -> tombstone object
  typedef std::multimap<Slice, RangeTombstone, stl_wrappers::LessOfComparator>
      TombstoneMap;
  // Also maintains position in TombstoneMap last seen by ShouldDelete(). The
  // end iterator indicates invalidation (e.g., if AddTombstones() changes the
  // underlying map). End iterator cannot be invalidated.
  struct PositionalTombstoneMap {
    explicit PositionalTombstoneMap(TombstoneMap _raw_map)
        : raw_map(std::move(_raw_map)), iter(raw_map.end()) {}
    PositionalTombstoneMap(const PositionalTombstoneMap&) = delete;
    PositionalTombstoneMap(PositionalTombstoneMap&& other)
        : raw_map(std::move(other.raw_map)), iter(raw_map.end()) {}

    TombstoneMap raw_map;
    TombstoneMap::const_iterator iter;
  };

  // Maps snapshot seqnum -> map of tombstones that fall in that stripe, i.e.,
  // their seqnums are greater than the next smaller snapshot's seqnum.
  typedef std::map<SequenceNumber, PositionalTombstoneMap> StripeMap;

  struct Rep {
    StripeMap stripe_map_;
    PinnedIteratorsManager pinned_iters_mgr_;
  };
  // Initializes rep_ lazily. This aggregator object is constructed for every
  // read, so expensive members should only be created when necessary, i.e.,
  // once the first range deletion is encountered.
  void InitRep(const std::vector<SequenceNumber>& snapshots);

  PositionalTombstoneMap& GetPositionalTombstoneMap(SequenceNumber seq);
  Status AddTombstone(RangeTombstone tombstone);

  SequenceNumber upper_bound_;
  std::unique_ptr<Rep> rep_;
  const InternalKeyComparator& icmp_;
  // collapse range deletions so they're binary searchable
  const bool collapse_deletions_;
};

}  // namespace rocksdb
