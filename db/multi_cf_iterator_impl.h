//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <variant>

#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "util/heap.h"
#include "util/overload.h"

namespace ROCKSDB_NAMESPACE {

// UNDER CONSTRUCTION - DO NOT USE
// A cross-column-family iterator from a consistent database state.
// When the same key exists in more than one column families, the iterator
// selects the value from the first column family containing the key, in the
// order provided in the `column_families` parameter.
class MultiCfIteratorImpl {
 public:
  MultiCfIteratorImpl(const Comparator* comparator,
                  const std::vector<ColumnFamilyHandle*>& column_families,
                  const std::vector<Iterator*>& child_iterators)
      : comparator_(comparator),
        heap_(MultiCfMinHeap(
            MultiCfHeapItemComparator<std::greater<int>>(comparator_))) {
    assert(column_families.size() > 0 &&
           column_families.size() == child_iterators.size());
    cfh_iter_pairs_.reserve(column_families.size());
    for (size_t i = 0; i < column_families.size(); ++i) {
      cfh_iter_pairs_.emplace_back(
          column_families[i], std::unique_ptr<Iterator>(child_iterators[i]));
    }
  }
  ~MultiCfIteratorImpl() { status_.PermitUncheckedError(); }

  // No copy allowed
  MultiCfIteratorImpl(const MultiCfIteratorImpl&) = delete;
  MultiCfIteratorImpl& operator=(const MultiCfIteratorImpl&) = delete;

  Slice key() const {
    assert(Valid());
    return current()->key();
  }

  bool Valid() const {
    if (std::holds_alternative<MultiCfMaxHeap>(heap_)) {
      auto& max_heap = std::get<MultiCfMaxHeap>(heap_);
      return !max_heap.empty() && status_.ok();
    }
    auto& min_heap = std::get<MultiCfMinHeap>(heap_);
    return !min_heap.empty() && status_.ok();
  }

  Status status() const { return status_; }

  void SeekToFirst() {
    auto& min_heap = GetHeap<MultiCfMinHeap>([this]() { InitMinHeap(); });
    SeekCommon(min_heap, [](Iterator* iter) { iter->SeekToFirst(); });
  }
  void Seek(const Slice& target) {
    auto& min_heap = GetHeap<MultiCfMinHeap>([this]() { InitMinHeap(); });
    SeekCommon(min_heap, [&target](Iterator* iter) { iter->Seek(target); });
  }
  void SeekToLast() {
    auto& max_heap = GetHeap<MultiCfMaxHeap>([this]() { InitMaxHeap(); });
    SeekCommon(max_heap, [](Iterator* iter) { iter->SeekToLast(); });
  }
  void SeekForPrev(const Slice& target) {
    auto& max_heap = GetHeap<MultiCfMaxHeap>([this]() { InitMaxHeap(); });
    SeekCommon(max_heap,
              [&target](Iterator* iter) { iter->SeekForPrev(target); });
  }

  void Next() {
    assert(Valid());
    auto& min_heap = GetHeap<MultiCfMinHeap>([this]() {
      Slice target = key();
      InitMinHeap();
      Seek(target);
    });
    AdvanceIterator(min_heap, [](Iterator* iter) { iter->Next(); });
  }
  void Prev() {
    assert(Valid());
    auto& max_heap = GetHeap<MultiCfMaxHeap>([this]() {
      Slice target = key();
      InitMaxHeap();
      SeekForPrev(target);
    });
    AdvanceIterator(max_heap, [](Iterator* iter) { iter->Prev(); });
  }

 private:
  std::vector<std::pair<ColumnFamilyHandle*, std::unique_ptr<Iterator>>>
      cfh_iter_pairs_;
  ReadOptions read_options_;
  Status status_;

  struct MultiCfIteratorInfo {
    Iterator* iterator;
    ColumnFamilyHandle* cfh;
    int order;
  };

  template <typename CompareOp>
  class MultiCfHeapItemComparator {
   public:
    explicit MultiCfHeapItemComparator(const Comparator* comparator)
        : comparator_(comparator) {}
    bool operator()(const MultiCfIteratorInfo& a,
                    const MultiCfIteratorInfo& b) const {
      assert(a.iterator);
      assert(b.iterator);
      assert(a.iterator->Valid());
      assert(b.iterator->Valid());
      int c = comparator_->Compare(a.iterator->key(), b.iterator->key());
      assert(c != 0 || a.order != b.order);
      return c == 0 ? a.order - b.order > 0 : CompareOp()(c, 0);
    }

   private:
    const Comparator* comparator_;
  };
  const Comparator* comparator_;
  using MultiCfMinHeap =
      BinaryHeap<MultiCfIteratorInfo,
                 MultiCfHeapItemComparator<std::greater<int>>>;
  using MultiCfMaxHeap = BinaryHeap<MultiCfIteratorInfo,
                                    MultiCfHeapItemComparator<std::less<int>>>;

  using MultiCfIterHeap = std::variant<MultiCfMinHeap, MultiCfMaxHeap>;

  MultiCfIterHeap heap_;

  // TODO: Lower and Upper bounds

  void considerStatus(Status s) {
    if (!s.ok() && status_.ok()) {
      status_ = std::move(s);
    }
  }

  Iterator* current() const {
    if (std::holds_alternative<MultiCfMaxHeap>(heap_)) {
      auto& max_heap = std::get<MultiCfMaxHeap>(heap_);
      return max_heap.top().iterator;
    }
    auto& min_heap = std::get<MultiCfMinHeap>(heap_);
    return min_heap.top().iterator;
  }



  template <typename HeapType, typename InitFunc>
  HeapType& GetHeap(InitFunc initFunc) {
    if (!std::holds_alternative<HeapType>(heap_)) {
      initFunc();
    }
    return std::get<HeapType>(heap_);
  }

  void InitMinHeap() {
    heap_.emplace<MultiCfMinHeap>(
        MultiCfHeapItemComparator<std::greater<int>>(comparator_));
  }
  void InitMaxHeap() {
    heap_.emplace<MultiCfMaxHeap>(
        MultiCfHeapItemComparator<std::less<int>>(comparator_));
  }

template <typename BinaryHeap, typename ChildSeekFuncType>
void SeekCommon(BinaryHeap& heap,
                                 ChildSeekFuncType child_seek_func) {
  heap.clear();
  int i = 0;
  for (auto& cfh_iter_pair : cfh_iter_pairs_) {
    auto& cfh = cfh_iter_pair.first;
    auto& iter = cfh_iter_pair.second;
    child_seek_func(iter.get());
    if (iter->Valid()) {
      assert(iter->status().ok());
      heap.push(MultiCfIteratorInfo{iter.get(), cfh, i});
    } else {
      considerStatus(iter->status());
    }
    ++i;
  }
}

template <typename BinaryHeap, typename AdvanceFuncType>
void AdvanceIterator(BinaryHeap& heap,
                                      AdvanceFuncType advance_func) {
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

};

}  // namespace ROCKSDB_NAMESPACE
