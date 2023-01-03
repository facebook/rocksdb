//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/range_del_aggregator.h"
#include "rocksdb/slice.h"
#include "rocksdb/types.h"
#include "table/iterator_wrapper.h"

namespace ROCKSDB_NAMESPACE {

class Arena;
class ArenaWrappedDBIter;
class InternalKeyComparator;

template <class TValue>
class InternalIteratorBase;
using InternalIterator = InternalIteratorBase<Slice>;

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0
extern InternalIterator* NewMergingIterator(
    const InternalKeyComparator* comparator, InternalIterator** children, int n,
    Arena* arena = nullptr, bool prefix_seek_mode = false);

class MergingIterator;

// A builder class to build a merging iterator by adding iterators one by one.
// User should call only one of AddIterator() or AddPointAndTombstoneIterator()
// exclusively for the same builder.
class MergeIteratorBuilder {
 public:
  // comparator: the comparator used in merging comparator
  // arena: where the merging iterator needs to be allocated from.
  explicit MergeIteratorBuilder(const InternalKeyComparator* comparator,
                                Arena* arena, bool prefix_seek_mode = false,
                                const Slice* iterate_upper_bound = nullptr);
  ~MergeIteratorBuilder();

  // Add iter to the merging iterator.
  void AddIterator(InternalIterator* iter);

  // Add a point key iterator and a range tombstone iterator.
  // `tombstone_iter_ptr` should and only be set by LevelIterator.
  // *tombstone_iter_ptr will be set to where the merging iterator stores
  // `tombstone_iter` when MergeIteratorBuilder::Finish() is called. This is
  // used by LevelIterator to update range tombstone iters when switching to a
  // different SST file. If a single point iterator with a nullptr range
  // tombstone iterator is provided, and the point iterator is not a level
  // iterator, then this builder will return the point iterator directly,
  // instead of creating a merging iterator on top of it. Internally, if all
  // point iterators are not LevelIterator, then range tombstone iterator is
  // only added to the merging iter if there is a non-null `tombstone_iter`.
  void AddPointAndTombstoneIterator(
      InternalIterator* point_iter, TruncatedRangeDelIterator* tombstone_iter,
      TruncatedRangeDelIterator*** tombstone_iter_ptr = nullptr);

  // Get arena used to build the merging iterator. It is called one a child
  // iterator needs to be allocated.
  Arena* GetArena() { return arena; }

  // Return the result merging iterator.
  // If db_iter is not nullptr, then db_iter->SetMemtableRangetombstoneIter()
  // will be called with pointer to where the merging iterator
  // stores the memtable range tombstone iterator.
  // This is used for DB iterator to refresh memtable range tombstones.
  InternalIterator* Finish(ArenaWrappedDBIter* db_iter = nullptr);

 private:
  MergingIterator* merge_iter;
  InternalIterator* first_iter;
  bool use_merging_iter;
  Arena* arena;
  // Used to set LevelIterator.range_tombstone_iter_.
  // See AddRangeTombstoneIterator() implementation for more detail.
  std::vector<std::pair<size_t, TruncatedRangeDelIterator***>>
      range_del_iter_ptrs_;
};

// For merging iterator to process range tombstones, we treat the start and end
// keys of a range tombstone as point keys and put them into the minHeap/maxHeap
// used in merging iterator. Take minHeap for example, we are able to keep track
// of currently "active" range tombstones (the ones whose start keys are popped
// but end keys are still in the heap) in `active_`. This `active_` set of range
// tombstones is then used to quickly determine whether the point key at heap
// top is deleted (by heap property, the point key at heap top must be within
// internal key range of active range tombstones).
//
// The HeapItem struct represents 3 types of elements in the minHeap/maxHeap:
// point key and the start and end keys of a range tombstone.
struct HeapItem {
  HeapItem() = default;

  enum Type { ITERATOR, DELETE_RANGE_START, DELETE_RANGE_END };
  IteratorWrapper iter;
  size_t level = 0;
  ParsedInternalKey parsed_ikey;
  std::string range_tombstone_key;
  // Will be overwritten before use, initialize here so compiler does not
  // complain.
  Type type = ITERATOR;

  explicit HeapItem(size_t _level, InternalIteratorBase<Slice>* _iter)
      : level(_level), type(Type::ITERATOR) {
    iter.Set(_iter);
  }

  void SetTombstoneKey(ParsedInternalKey&& pik) {
    // op_type is already initialized in MergingIterator::Finish().
    parsed_ikey.user_key = pik.user_key;
    parsed_ikey.sequence = pik.sequence;
  }

  void SetTombstoneForCompaction(const ParsedInternalKey&& pik) {
    range_tombstone_key.clear();
    AppendInternalKey(&range_tombstone_key, pik);
  }

  Slice key() const {
    if (LIKELY(type == ITERATOR)) {
      return iter.key();
    }
    return range_tombstone_key;
  }

  bool IsDeleteRangeSentinelKey() const {
    if (LIKELY(type == ITERATOR)) {
      return iter.IsDeleteRangeSentinelKey();
    }
    return false;
  }
};

class MinHeapItemComparator {
 public:
  explicit MinHeapItemComparator(const InternalKeyComparator* comparator)
      : comparator_(comparator) {}
  bool operator()(HeapItem* a, HeapItem* b) const {
    if (LIKELY(a->type == HeapItem::ITERATOR)) {
      if (LIKELY(b->type == HeapItem::ITERATOR)) {
        return comparator_->Compare(a->iter.key(), b->iter.key()) > 0;
      } else {
        return comparator_->Compare(a->iter.key(), b->parsed_ikey) > 0;
      }
    } else {
      if (LIKELY(b->type == HeapItem::ITERATOR)) {
        return comparator_->Compare(a->parsed_ikey, b->iter.key()) > 0;
      } else {
        return comparator_->Compare(a->parsed_ikey, b->parsed_ikey) > 0;
      }
    }
  }

 private:
  const InternalKeyComparator* comparator_;
};

using MergerMinIterHeap = BinaryHeap<HeapItem*, MinHeapItemComparator>;
}  // namespace ROCKSDB_NAMESPACE
