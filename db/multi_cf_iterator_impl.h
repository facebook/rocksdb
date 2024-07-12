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

struct MultiCfIteratorInfo {
  ColumnFamilyHandle* cfh;
  Iterator* iterator;
  int order;
};

class MultiCfIteratorImpl {
 public:
  MultiCfIteratorImpl(
      const Comparator* comparator,
      const std::vector<ColumnFamilyHandle*>& column_families,
      const std::vector<Iterator*>& child_iterators,
      std::function<void()> reset_func,
      std::function<void(const autovector<MultiCfIteratorInfo>&)> populate_func)
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
      std::string target(key().data(), key().size());
      InitMinHeap();
      Seek(target);
    });
    AdvanceIterator(min_heap, [](Iterator* iter) { iter->Next(); });
  }
  void Prev() {
    assert(Valid());
    auto& max_heap = GetHeap<MultiCfMaxHeap>([this]() {
      std::string target(key().data(), key().size());
      InitMaxHeap();
      SeekForPrev(target);
    });
    AdvanceIterator(max_heap, [](Iterator* iter) { iter->Prev(); });
  }

 private:
  std::vector<std::pair<ColumnFamilyHandle*, std::unique_ptr<Iterator>>>
      cfh_iter_pairs_;
  Status status_;

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
  std::function<void(autovector<MultiCfIteratorInfo>)> populate_func_;

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

  template <typename BinaryHeap, typename ChildSeekFuncType>
  void SeekCommon(BinaryHeap& heap, ChildSeekFuncType child_seek_func) {
    reset_func_();
    heap.clear();
    int i = 0;
    for (auto& [cfh, iter] : cfh_iter_pairs_) {
      child_seek_func(iter.get());
      if (iter->Valid()) {
        assert(iter->status().ok());
        heap.push(MultiCfIteratorInfo{cfh, iter.get(), i});
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
      PopulateIterator(heap);
    }
  }

  template <typename BinaryHeap, typename AdvanceFuncType>
  void AdvanceIterator(BinaryHeap& heap, AdvanceFuncType advance_func) {
    reset_func_();
    // It is possible for one or more child iters are at invalid keys due to
    // manual prefix iteration. For such cases, we consider the result of the
    // multi-cf-iter is also undefined.
    // https://github.com/facebook/rocksdb/wiki/Prefix-Seek#manual-prefix-iterating
    // for details about manual prefix iteration
    if (heap.empty()) {
      return;
    }

    // 1. Keep the top iterator (by popping it from the heap)
    // 2. Make sure all others have iterated past the top iterator key slice
    // 3. Advance the top iterator, and add it back to the heap if valid
    auto top = heap.top();
    heap.pop();
    if (!heap.empty()) {
      auto current = heap.top();
      assert(current.iterator);
      while (current.iterator->Valid() &&
             comparator_->Compare(top.iterator->key(),
                                  current.iterator->key()) == 0) {
        assert(current.iterator->status().ok());
        advance_func(current.iterator);
        if (current.iterator->Valid()) {
          heap.replace_top(heap.top());
        } else {
          considerStatus(current.iterator->status());
          if (!status_.ok()) {
            heap.clear();
            return;
          } else {
            heap.pop();
          }
        }
        if (!heap.empty()) {
          current = heap.top();
        }
      }
    }
    advance_func(top.iterator);
    if (top.iterator->Valid()) {
      assert(top.iterator->status().ok());
      heap.push(top);
    } else {
      considerStatus(top.iterator->status());
      if (!status_.ok()) {
        heap.clear();
        return;
      }
    }

    if (!heap.empty()) {
      PopulateIterator(heap);
    }
  }

  template <typename BinaryHeap>
  void PopulateIterator(BinaryHeap& heap) {
    // 1. Keep the top iterator (by popping it from the heap) and add it to list
    //    to populate
    // 2. For all non-top iterators having the same key as top iter popped
    //    from the previous step, add them to the same list and pop it
    //    temporarily from the heap
    // 3. Once no other iters have the same key as the top iter from step 1,
    //    populate the value/columns and attribute_groups from the list
    //    collected in step 1 and 2 and add all the iters back to the heap
    assert(!heap.empty());
    auto top = heap.top();
    heap.pop();
    autovector<MultiCfIteratorInfo> to_populate;
    to_populate.push_back(top);
    if (!heap.empty()) {
      auto current = heap.top();
      assert(current.iterator);
      while (current.iterator->Valid() &&
             comparator_->Compare(top.iterator->key(),
                                  current.iterator->key()) == 0) {
        assert(current.iterator->status().ok());
        to_populate.push_back(current);
        heap.pop();
        if (!heap.empty()) {
          current = heap.top();
        } else {
          break;
        }
      }
    }
    // Add the items back to the heap
    for (auto& item : to_populate) {
      heap.push(item);
    }
    populate_func_(to_populate);
  }
};

}  // namespace ROCKSDB_NAMESPACE
