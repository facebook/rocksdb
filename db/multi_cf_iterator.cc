//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/multi_cf_iterator.h"

#include <cassert>

namespace ROCKSDB_NAMESPACE {

void MultiCfIterator::SeekCommon(
    const std::function<void(Iterator*)>& child_seek_func,
    Direction direction) {
  direction_ = direction;
  Reset();
  int i = 0;
  for (auto& cfh_iter_pair : cfh_iter_pairs_) {
    auto& cfh = cfh_iter_pair.first;
    auto& iter = cfh_iter_pair.second;
    child_seek_func(iter.get());
    if (iter->Valid()) {
      assert(iter->status().ok());
      if (direction_ == kReverse) {
        auto& max_heap = std::get<MultiCfMaxHeap>(heap_);
        max_heap.push(MultiCfIteratorInfo{iter.get(), cfh, i});
      } else {
        auto& min_heap = std::get<MultiCfMinHeap>(heap_);
        min_heap.push(MultiCfIteratorInfo{iter.get(), cfh, i});
      }
    } else {
      considerStatus(iter->status());
    }
    ++i;
  }
}

template <typename BinaryHeap>
void MultiCfIterator::AdvanceIterator(
    BinaryHeap& heap, const std::function<void(Iterator*)>& advance_func) {
  // 1. Keep the top iterator (by popping it from the heap)
  // 2. Make sure all others have iterated past the top iterator key slice
  // 3. Advance the top iterator, and add it back to the heap if valid
  auto top = heap.top();
  heap.pop();
  if (!heap.empty()) {
    auto* current = heap.top().iterator;
    while (current->Valid() &&
           comparator_->Compare(top.iterator->key(), current->key()) == 0) {
      assert(current->status().ok());
      advance_func(current);
      if (current->Valid()) {
        heap.replace_top(heap.top());
      } else {
        considerStatus(current->status());
        heap.pop();
      }
      if (!heap.empty()) {
        current = heap.top().iterator;
      }
    }
  }
  advance_func(top.iterator);
  if (top.iterator->Valid()) {
    assert(top.iterator->status().ok());
    heap.push(top);
  } else {
    considerStatus(top.iterator->status());
  }
}

void MultiCfIterator::SeekToFirst() {
  SeekCommon([](Iterator* iter) { iter->SeekToFirst(); }, kForward);
}
void MultiCfIterator::Seek(const Slice& target) {
  SeekCommon([&target](Iterator* iter) { iter->Seek(target); }, kForward);
}
void MultiCfIterator::SeekToLast() {
  SeekCommon([](Iterator* iter) { iter->SeekToLast(); }, kReverse);
}
void MultiCfIterator::SeekForPrev(const Slice& target) {
  SeekCommon([&target](Iterator* iter) { iter->SeekForPrev(target); },
             kReverse);
}

void MultiCfIterator::Next() {
  assert(Valid());
  if (direction_ != kForward) {
    SwitchToDirection(kForward);
  }
  auto& min_heap = std::get<MultiCfMinHeap>(heap_);
  AdvanceIterator(min_heap, [](Iterator* iter) { iter->Next(); });
}
void MultiCfIterator::Prev() {
  assert(Valid());
  if (direction_ != kReverse) {
    SwitchToDirection(kReverse);
  }
  auto& max_heap = std::get<MultiCfMaxHeap>(heap_);
  AdvanceIterator(max_heap, [](Iterator* iter) { iter->Prev(); });
}

}  // namespace ROCKSDB_NAMESPACE
