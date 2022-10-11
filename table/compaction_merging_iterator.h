//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "db/range_del_aggregator.h"
#include "rocksdb/slice.h"
#include "rocksdb/types.h"
#include "table/merging_iterator.h"

namespace ROCKSDB_NAMESPACE {

/*
 * This is a simplified version of MergingIterator that is specifically used for
 * compaction. It merges the input `children` iterators into a sorted stream of
 * keys. Range tombstones are also emitted to prevent oversize compaction caused
 * by them. For example, consider an L1 file with content [a, b), y, z, where
 * [a, b) is a range tombstone and y and z are point keys. This could cause an
 * oversize compaction as it can overlap with a wide range of key space in L2.
 * Another example is just a large range tombstone, say [a, z), that covers the
 * entire keyspace. We should be able to break it down to allow smaller
 * compactions.
 *
 * CompactionMergingIterator treats range tombstones [start, end)@seqno as a key
 * start@seqno, with value end@kMaxSequenceNumber unless truncated at file
 * boundary.
 *
 */
class CompactionMergingIterator : public InternalIterator {
 public:
  CompactionMergingIterator(
      const InternalKeyComparator* comparator, InternalIterator** children,
      int n, bool is_arena_mode,
      std::vector<
          std::pair<TruncatedRangeDelIterator*, TruncatedRangeDelIterator***>>
          range_tombstones)
      : is_arena_mode_(is_arena_mode),
        comparator_(comparator),
        current_(nullptr),
        minHeap_(comparator_),
        pinned_iters_mgr_(nullptr) {
    children_.resize(n);
    for (int i = 0; i < n; i++) {
      children_[i].level = i;
      children_[i].iter.Set(children[i]);
      assert(children_[i].type == HeapItem::ITERATOR);
    }
    assert(range_tombstones.size() == static_cast<size_t>(n));
    for (auto& p : range_tombstones) {
      range_tombstone_iters_.push_back(p.first);
    }

    pinned_heap_item_.resize(n);
    for (int i = 0; i < n; ++i) {
      if (range_tombstones[i].second) {
        // For LevelIterator's
        *range_tombstones[i].second = &range_tombstone_iters_[i];
      }
      pinned_heap_item_[i].level = i;
      pinned_heap_item_[i].type = HeapItem::DELETE_RANGE_START;
    }
  }

  void considerStatus(const Status& s) {
    if (!s.ok() && status_.ok()) {
      status_ = s;
    }
  }

  ~CompactionMergingIterator() override {
    // TODO: use unique_ptr for range_tombstone_iters_
    for (auto child : range_tombstone_iters_) {
      delete child;
    }

    for (auto& child : children_) {
      child.iter.DeleteIter(is_arena_mode_);
    }
    status_.PermitUncheckedError();
  }

  bool Valid() const override { return current_ != nullptr && status_.ok(); }

  Status status() const override { return status_; }

  // Add the current range tombstone from range_tombstone_iters_[level].
  void InsertRangeTombstoneToMinHeap(size_t level) {
    assert(range_tombstone_iters_[level]->Valid());
    pinned_heap_item_[level].SetTombstoneForCompaction(
        range_tombstone_iters_[level]);
    minHeap_.push(&pinned_heap_item_[level]);
  }

  void SeekToFirst() override;

  void Seek(const Slice& target) override;

  void Next() override;

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  // Here we simply relay MayBeOutOfLowerBound/MayBeOutOfUpperBound result
  // from current child iterator. Potentially as long as one of child iterator
  // report out of bound is not possible, we know current key is within bound.
  bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return current_->type == HeapItem::DELETE_RANGE_START ||
           current_->iter.MayBeOutOfLowerBound();
  }

  IterBoundCheck UpperBoundCheckResult() override {
    assert(Valid());
    return current_->type == HeapItem::DELETE_RANGE_START
               ? IterBoundCheck::kUnknown
               : current_->iter.UpperBoundCheckResult();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    for (auto& child : children_) {
      child.iter.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  // Compaction uses a subset of InternalIterator interface.
  void SeekToLast() override { assert(false); }

  void SeekForPrev(const Slice&) override { assert(false); }

  void Prev() override { assert(false); }

  bool NextAndGetResult(IterateResult*) override {
    assert(false);
    return false;
  }

  bool IsKeyPinned() const override {
    assert(false);
    return false;
  }

  bool IsValuePinned() const override {
    assert(false);
    return false;
  }

  bool PrepareValue() override {
    assert(false);
    return false;
  }

 private:
  bool is_arena_mode_;
  const InternalKeyComparator* comparator_;
  // HeapItem for all child point iterators.
  std::vector<HeapItem> children_;
  // HeapItem for range tombstones. pinned_heap_item_[i] corresponds to the
  // current range tombstone from range_tombstone_iters_[i].
  std::vector<HeapItem> pinned_heap_item_;
  // range_tombstone_iters_[i] contains range tombstones in the sorted run that
  // corresponds to children_[i]. range_tombstone_iters_[i] ==
  // nullptr means the sorted run of children_[i] does not have range
  // tombstones (or the current SSTable does not have range tombstones in the
  // case of LevelIterator).
  std::vector<TruncatedRangeDelIterator*> range_tombstone_iters_;

  // Skip file boundary sentinel keys.
  void FindNextVisibleKey();

  // top of minHeap_
  HeapItem* current_;
  // If any of the children have non-ok status, this is one of them.
  Status status_;
  MergerMinIterHeap minHeap_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  // Process a child that is not in the min heap.
  // If valid, add to the min heap. Otherwise, check status.
  void AddToMinHeapOrCheckStatus(HeapItem*);

  HeapItem* CurrentForward() const {
    return !minHeap_.empty() ? minHeap_.top() : nullptr;
  }
};

InternalIterator* NewCompactionMergingIterator(
    const InternalKeyComparator* comparator, InternalIterator** children, int n,
    std::vector<std::pair<TruncatedRangeDelIterator*,
                          TruncatedRangeDelIterator***>>& range_tombstone_iters,
    Arena* arena = nullptr);
}  // namespace ROCKSDB_NAMESPACE
