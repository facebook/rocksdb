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

class CompactionHeapItemComparator {
 public:
  explicit CompactionHeapItemComparator(const InternalKeyComparator* comparator)
      : comparator_(comparator) {}
  bool operator()(HeapItem* a, HeapItem* b) const {
    int r = comparator_->Compare(a->key(), b->key());
    if (r > 0) {
      return true;
    } else if (r < 0) {
      return false;
    } else {
      // When range tombstone and point key have the same internal key,
      // range tombstone comes first. So that when range tombstone and
      // file's largest key are the same, the file boundary sentinel key
      // comes after.
      return a->type == HeapItem::ITERATOR &&
             b->type == HeapItem::DELETE_RANGE_START;
    }
  }

 private:
  const InternalKeyComparator* comparator_;
};

using CompactionMinHeap = BinaryHeap<HeapItem*, CompactionHeapItemComparator>;
/*
 * This is a simplified version of MergingIterator and is specifically used for
 * compaction. It merges the input `children` iterators into a sorted stream of
 * keys. Range tombstone start keys are also emitted to prevent oversize
 * compactions. For example, consider an L1 file with content [a, b), y, z,
 * where [a, b) is a range tombstone and y and z are point keys. This could
 * cause an oversize compaction as it can overlap with a wide range of key space
 * in L2.
 *
 * CompactionMergingIterator emits range tombstone start keys from each LSM
 * level's range tombstone iterator, and for each range tombstone
 * [start,end)@seqno, the key will be start@kMaxSequenceNumber unless truncated
 * at file boundary (see detail TruncatedRangeDelIterator::start_key()).
 *
 * Caller should use CompactionMergingIterator::IsDeleteRangeSentinelKey() to
 * check if the current key is a range tombstone key.
 * TODO(cbi): IsDeleteRangeSentinelKey() is used for two kinds of keys at
 * different layers: file boundary and range tombstone keys. Separate them into
 * two APIs for clarity.
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
        minHeap_(CompactionHeapItemComparator(comparator_)),
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
        // for LevelIterator
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

  void SeekToFirst() override;

  void Seek(const Slice& target) override;

  void Next() override;

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    if (LIKELY(current_->type == HeapItem::ITERATOR)) {
      return current_->iter.value();
    } else {
      return dummy_tombstone_val;
    }
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

  bool IsDeleteRangeSentinelKey() const override {
    assert(Valid());
    return current_->type == HeapItem::DELETE_RANGE_START;
  }

  // Compaction uses the above subset of InternalIterator interface.
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
  // Used as value for range tombstone keys
  std::string dummy_tombstone_val{};

  // Skip file boundary sentinel keys.
  void FindNextVisibleKey();

  // top of minHeap_
  HeapItem* current_;
  // If any of the children have non-ok status, this is one of them.
  Status status_;
  CompactionMinHeap minHeap_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  // Process a child that is not in the min heap.
  // If valid, add to the min heap. Otherwise, check status.
  void AddToMinHeapOrCheckStatus(HeapItem*);

  HeapItem* CurrentForward() const {
    return !minHeap_.empty() ? minHeap_.top() : nullptr;
  }

  void InsertRangeTombstoneAtLevel(size_t level) {
    if (range_tombstone_iters_[level]->Valid()) {
      pinned_heap_item_[level].SetTombstoneForCompaction(
          range_tombstone_iters_[level]->start_key());
      minHeap_.push(&pinned_heap_item_[level]);
    }
  }
};

InternalIterator* NewCompactionMergingIterator(
    const InternalKeyComparator* comparator, InternalIterator** children, int n,
    std::vector<std::pair<TruncatedRangeDelIterator*,
                          TruncatedRangeDelIterator***>>& range_tombstone_iters,
    Arena* arena = nullptr);
}  // namespace ROCKSDB_NAMESPACE
