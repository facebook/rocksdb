//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <functional>
#include <variant>

#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "util/heap.h"

namespace ROCKSDB_NAMESPACE {

class MultiCfIteratorImpl {
 public:
  MultiCfIteratorImpl(
      const Comparator* comparator,
      const std::vector<ColumnFamilyHandle*>& column_families,
      const std::vector<Iterator*>& child_iterators,
      std::function<void()> reset_func,
      std::function<void(ColumnFamilyHandle*, Iterator*)> populate_func)
      : comparator_(comparator),
        heap_(MultiCfMinHeap(
            MultiCfHeapItemComparator<std::greater<int>>(comparator_))),
        reset_func_(std::move(reset_func)),
        populate_func_(std::move(populate_func)) {
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
    SeekCommon(
        min_heap, [](Iterator* iter) { iter->SeekToFirst(); },
        [](Iterator* iter) { iter->Next(); });
  }
  void Seek(const Slice& target) {
    auto& min_heap = GetHeap<MultiCfMinHeap>([this]() { InitMinHeap(); });
    SeekCommon(
        min_heap, [&target](Iterator* iter) { iter->Seek(target); },
        [](Iterator* iter) { iter->Next(); });
  }
  void SeekToLast() {
    auto& max_heap = GetHeap<MultiCfMaxHeap>([this]() { InitMaxHeap(); });
    SeekCommon(
        max_heap, [](Iterator* iter) { iter->SeekToLast(); },
        [](Iterator* iter) { iter->Prev(); });
  }
  void SeekForPrev(const Slice& target) {
    auto& max_heap = GetHeap<MultiCfMaxHeap>([this]() { InitMaxHeap(); });
    SeekCommon(
        max_heap, [&target](Iterator* iter) { iter->SeekForPrev(target); },
        [](Iterator* iter) { iter->Prev(); });
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
  Status status_;

  struct MultiCfIteratorInfo {
    Iterator* iterator;
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

  std::function<void()> reset_func_;
  std::function<void(ColumnFamilyHandle*, Iterator*)> populate_func_;

  // TODO: Lower and Upper bounds

  Iterator* current() const {
    if (std::holds_alternative<MultiCfMaxHeap>(heap_)) {
      auto& max_heap = std::get<MultiCfMaxHeap>(heap_);
      return max_heap.top().iterator;
    }
    auto& min_heap = std::get<MultiCfMinHeap>(heap_);
    return min_heap.top().iterator;
  }

  void considerStatus(Status s) {
    if (!s.ok() && status_.ok()) {
      status_ = std::move(s);
    }
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

  template <typename BinaryHeap, typename ChildSeekFuncType,
            typename AdvanceFuncType>
  void SeekCommon(BinaryHeap& heap, ChildSeekFuncType child_seek_func,
                  AdvanceFuncType advance_func) {
    reset_func_();
    heap.clear();
    int i = 0;
    for (auto& cfh_iter_pair : cfh_iter_pairs_) {
      auto& iter = cfh_iter_pair.second;
      child_seek_func(iter.get());
      if (iter->Valid()) {
        assert(iter->status().ok());
        heap.push(MultiCfIteratorInfo{iter.get(), i});
      } else {
        considerStatus(iter->status());
        if (!status_.ok()) {
          // Non-OK status from the iterator. Bail out early
          heap.clear();
          break;
        }
      }
      ++i;
    }
    if (!heap.empty()) {
      PopulateIterator(heap, advance_func);
    }
  }

  template <typename BinaryHeap, typename AdvanceFuncType>
  void AdvanceIterator(BinaryHeap& heap, AdvanceFuncType advance_func) {
    assert(!heap.empty());
    reset_func_();

    // Because PopulateIterator() advances the same key in all non-top
    // iterators, we do not have to do that here again. Just advance the top
    // iterator and re-heapify
    auto top = heap.top();
    advance_func(top.iterator);
    if (top.iterator->Valid()) {
      assert(top.iterator->status().ok());
      heap.replace_top(top);
    } else {
      considerStatus(top.iterator->status());
      if (!status_.ok()) {
        heap.clear();
        return;
      } else {
        heap.pop();
      }
    }
    if (!heap.empty()) {
      PopulateIterator(heap, advance_func);
    }
  }

  template <typename BinaryHeap, typename AdvanceFuncType>
  void PopulateIterator(BinaryHeap& heap, AdvanceFuncType advance_func) {
    // 1. Keep the top iterator (by popping it from the heap) and populate
    //    value, columns and attribute_groups
    // 2. Make sure all others have iterated past the top iterator key slice.
    //    While iterating, coalesce/populate value, columns and attribute_groups
    // 3. Add the top iterator back without advancing it
    assert(!heap.empty());
    auto top = heap.top();
    auto& [top_cfh, top_iter] = cfh_iter_pairs_[top.order];
    populate_func_(top_cfh, top_iter.get());
    heap.pop();
    if (!heap.empty()) {
      auto* current = heap.top().iterator;
      while (current->Valid() &&
             comparator_->Compare(top.iterator->key(), current->key()) == 0) {
        assert(current->status().ok());
        auto& [curr_cfh, curr_iter] = cfh_iter_pairs_[heap.top().order];
        populate_func_(curr_cfh, curr_iter.get());
        advance_func(current);
        if (current->Valid()) {
          heap.replace_top(heap.top());
        } else {
          considerStatus(current->status());
          if (!status_.ok()) {
            // Non-OK status from the iterator. Bail out early
            heap.clear();
            break;
          } else {
            heap.pop();
          }
        }
        if (!heap.empty()) {
          current = heap.top().iterator;
        }
      }
    }
    heap.push(top);
  }
};

}  // namespace ROCKSDB_NAMESPACE
