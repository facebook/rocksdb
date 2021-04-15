//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <memory>

#include "memory/arena.h"
#include "memtable/skiplist.h"

namespace ROCKSDB_NAMESPACE {

//
// Range of values set or not set. Query or add ranges, but not delete them.
//
// Concurrency is guaranteed by the underlying skiplist.
//
// TODO how do we order and arrange updates so that concurrent readers always
// get a consistent view ?
//
template <class PointKey, class Comparator>
class IntervalMap {
 private:
  enum Marker { Start, Stop };

  class PointData {
    Marker marker;
  };

  class PointEntry {
   public:
    PointKey pointKey;
    PointData pointData;

    PointEntry(PointKey& key) : pointKey(key) {}
  };

  class PointEntryComparator {
   public:
    PointEntryComparator(Comparator cmp) : comparator_(cmp){};
    int operator()(const PointEntry* entry1, const PointEntry* entry2) const;

   private:
    Comparator comparator_;
  };

  using PointEntrySkipList = SkipList<PointEntry*, PointEntryComparator>;

 public:
  // Create a new SkipList-based IntervalMap object that will allocate
  // memory using "*allocator".  Objects allocated in the allocator must
  // remain allocated for the lifetime of the range map object. The
  // IntervalMap is expected to share a lifetime with the write batch index,
  // and to share its allocator too.
  explicit IntervalMap(Comparator cmp, Allocator* allocator);

  // No copying allowed
  IntervalMap(const IntervalMap&) = delete;
  void operator=(const IntervalMap&) = delete;

  // Merge the semi-open interval [from_key, to_key) with the other intervals
  void AddInterval(const PointKey& from_key, const PointKey& to_key);

  // Check whether the key is in any of the intervals
  bool IsInInterval(const PointKey& key);

  void Clear();

 private:
  PointEntryComparator const comparator_;
  Allocator* const allocator_;  // Allocator used for allocations of nodes
  PointEntrySkipList skip_list;
};

template <class PointKey, class Comparator>
IntervalMap<PointKey, Comparator>::IntervalMap(Comparator cmp,
                                               Allocator* allocator)
    : comparator_(cmp),
      allocator_(allocator),
      skip_list(comparator_, allocator) {}

template <class PointKey, class Comparator>
void IntervalMap<PointKey, Comparator>::AddInterval(const PointKey& from_key,
                                                    const PointKey& to_key) {
  // TODO implement
  typename PointEntrySkipList::Iterator iter(&skip_list);
  PointEntry fromEntry(from_key);
  iter.Seek(&fromEntry);
  PointEntry toEntry(to_key);
  iter.Seek(&toEntry);
}

template <class PointKey, class Comparator>
bool IntervalMap<PointKey, Comparator>::IsInInterval(const PointKey& key) {
  // TODO implement
  typename PointEntrySkipList::Iterator iter(&skip_list);
  PointEntry keyEntry(key);
  iter.Seek(&keyEntry);

  // TODO replace this placeholder
  return false;
}

template <class PointKey, class Comparator>
int IntervalMap<PointKey, Comparator>::PointEntryComparator::operator()(
    const IntervalMap<PointKey, Comparator>::PointEntry* entry1,
    const IntervalMap<PointKey, Comparator>::PointEntry* entry2) const {
  return comparator_(&entry1->pointKey, &entry2->pointKey);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
