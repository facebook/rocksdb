//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merging_iterator.h"
#include <string>
#include <vector>
#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "memory/arena.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "table/internal_iterator.h"
#include "table/iter_heap.h"
#include "table/iterator_wrapper.h"
#include "test_util/sync_point.h"
#include "util/autovector.h"
#include "util/heap.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

#if defined(_MSC_VER) /* Visual Studio */
#define FORCE_INLINE __forceinline
#elif defined(__GNUC__)
#define FORCE_INLINE __attribute__((always_inline))
#pragma GCC diagnostic ignored "-Wattribute"
#else
#define inline
#endif

static FORCE_INLINE uint64_t GetUnalignedU64(const void* ptr) noexcept {
  uint64_t x;
  memcpy(&x, ptr, sizeof(uint64_t));
  return x;
}

static FORCE_INLINE bool BytewiseCompareInternalKey(Slice x, Slice y) noexcept {
  size_t n = std::min(x.size_, y.size_) - 8;
  int cmp = memcmp(x.data_, y.data_, n);
  if (0 != cmp) return cmp < 0;
  if (x.size_ != y.size_) return x.size_ < y.size_;
  return GetUnalignedU64(x.data_ + n) > GetUnalignedU64(y.data_ + n);
}

static FORCE_INLINE bool RevBytewiseCompareInternalKey(Slice x,
                                                       Slice y) noexcept {
  size_t n = std::min(x.size_, y.size_) - 8;
  int cmp = memcmp(x.data_, y.data_, n);
  if (0 != cmp) return cmp > 0;
  if (x.size_ != y.size_) return x.size_ > y.size_;
  return GetUnalignedU64(x.data_ + n) > GetUnalignedU64(y.data_ + n);
}

struct MaxInlineBytewiseComp {
  FORCE_INLINE
  bool operator()(const IteratorWrapper* a,
                  const IteratorWrapper* b) const noexcept {
    return BytewiseCompareInternalKey(a->key(), b->key());
  }
  MaxInlineBytewiseComp(const InternalKeyComparator*) {}
};
struct MinInlineBytewiseComp {
  FORCE_INLINE
  bool operator()(const IteratorWrapper* a,
                  const IteratorWrapper* b) const noexcept {
    return BytewiseCompareInternalKey(b->key(), a->key());
  }
  MinInlineBytewiseComp(const InternalKeyComparator*) {}
};

struct MaxInlineRevBytewiseComp {
  FORCE_INLINE
  bool operator()(const IteratorWrapper* a,
                  const IteratorWrapper* b) const noexcept {
    return RevBytewiseCompareInternalKey(a->key(), b->key());
  }
  MaxInlineRevBytewiseComp(const InternalKeyComparator*) {}
};
struct MinInlineRevBytewiseComp {
  FORCE_INLINE
  bool operator()(const IteratorWrapper* a,
                  const IteratorWrapper* b) const noexcept {
    return RevBytewiseCompareInternalKey(b->key(), a->key());
  }
  MinInlineRevBytewiseComp(const InternalKeyComparator*) {}
};

const size_t kNumIterReserve = 4;

class MergingIterator : public InternalIterator {
 public:
  virtual void AddIterator(InternalIterator* iter) = 0;
};

template <class MinHeapComparator, class MaxHeapComparator>
class MergingIterTmpl : public MergingIterator {
  using MergerMaxIterHeap = BinaryHeap<IteratorWrapper*, MaxHeapComparator>;
  using MergerMinIterHeap = BinaryHeap<IteratorWrapper*, MinHeapComparator>;

 public:
  MergingIterTmpl(const InternalKeyComparator* comparator,
                  InternalIterator** children, int n, bool is_arena_mode,
                  bool prefix_seek_mode)
      : is_arena_mode_(is_arena_mode),
        prefix_seek_mode_(prefix_seek_mode),
        direction_(kForward),
        comparator_(comparator),
        current_(nullptr),
        minHeap_(comparator_),
        pinned_iters_mgr_(nullptr) {
    children_.resize(n);
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  void considerStatus(Status s) {
    if (!s.ok() && status_.ok()) {
      status_ = s;
    }
  }

  virtual void AddIterator(InternalIterator* iter) {
    children_.emplace_back(iter);
    if (pinned_iters_mgr_) {
      iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
    // Invalidate to ensure `Seek*()` is called to construct the heaps before
    // use.
    current_ = nullptr;
  }

  ~MergingIterTmpl() override {
    for (auto& child : children_) {
      child.DeleteIter(is_arena_mode_);
    }
    status_.PermitUncheckedError();
    minHeap_.~MergerMinIterHeap();
  }

  bool Valid() const override { return current_ != nullptr && status_.ok(); }

  Status status() const override { return status_; }

  void SeekToFirst() override {
    InitMinHeap();
    status_ = Status::OK();
    for (auto& child : children_) {
      child.SeekToFirst();
      AddToMinHeapOrCheckStatus(&child);
    }
    direction_ = kForward;
    current_ = CurrentForward();
  }

  void SeekToLast() override {
    InitMaxHeap();
    status_ = Status::OK();
    for (auto& child : children_) {
      child.SeekToLast();
      AddToMaxHeapOrCheckStatus(&child);
    }
    direction_ = kReverse;
    current_ = CurrentReverse();
  }

  void Seek(const Slice& target) override {
    InitMinHeap();
    status_ = Status::OK();
    for (auto& child : children_) {
      {
        PERF_TIMER_GUARD(seek_child_seek_time);
        child.Seek(target);
      }

      PERF_COUNTER_ADD(seek_child_seek_count, 1);

      // child.status() is set to Status::TryAgain indicating asynchronous
      // request for retrieval of data blocks has been submitted. So it should
      // return at this point and Seek should be called again to retrieve the
      // requested block and add the child to min heap.
      if (child.status() == Status::TryAgain()) {
        continue;
      }
      {
        // Strictly, we timed slightly more than min heap operation,
        // but these operations are very cheap.
        PERF_TIMER_GUARD(seek_min_heap_time);
        AddToMinHeapOrCheckStatus(&child);
      }
    }

    for (auto& child : children_) {
      if (child.status() == Status::TryAgain()) {
        child.Seek(target);
        {
          PERF_TIMER_GUARD(seek_min_heap_time);
          AddToMinHeapOrCheckStatus(&child);
        }
        PERF_COUNTER_ADD(number_async_seek, 1);
      }
    }

    direction_ = kForward;
    {
      PERF_TIMER_GUARD(seek_min_heap_time);
      current_ = CurrentForward();
    }
  }

  void SeekForPrev(const Slice& target) override {
    InitMaxHeap();
    status_ = Status::OK();

    for (auto& child : children_) {
      {
        PERF_TIMER_GUARD(seek_child_seek_time);
        child.SeekForPrev(target);
      }
      PERF_COUNTER_ADD(seek_child_seek_count, 1);

      {
        PERF_TIMER_GUARD(seek_max_heap_time);
        AddToMaxHeapOrCheckStatus(&child);
      }
    }
    direction_ = kReverse;
    {
      PERF_TIMER_GUARD(seek_max_heap_time);
      current_ = CurrentReverse();
    }
  }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current children since current_ is
    // the smallest child and key() == current_->key().
    if (direction_ != kForward) {
      SwitchToForward();
      // The loop advanced all non-current children to be > key() so current_
      // should still be strictly the smallest key.
    }

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentForward());

    // as the current points to the current record. move the iterator forward.
    current_->Next();
    if (current_->Valid()) {
      // current is still valid after the Next() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      assert(current_->status().ok());
      minHeap_.replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->status());
      minHeap_.pop();
    }
    current_ = CurrentForward();
  }

  bool NextAndGetResult(IterateResult* result) override {
    Next();
    bool is_valid = Valid();
    if (is_valid) {
      result->key = key();
      result->bound_check_result = UpperBoundCheckResult();
      result->value_prepared = current_->IsValuePrepared();
    }
    return is_valid;
  }

  void Prev() override {
    assert(Valid());
    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current children since current_ is
    // the largest child and key() == current_->key().
    if (direction_ != kReverse) {
      // Otherwise, retreat the non-current children.  We retreat current_
      // just after the if-block.
      SwitchToBackward();
    }

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentReverse());

    current_->Prev();
    if (current_->Valid()) {
      // current is still valid after the Prev() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      assert(current_->status().ok());
      maxHeap_.replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->status());
      maxHeap_.pop();
    }
    current_ = CurrentReverse();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  bool PrepareValue() override {
    assert(Valid());
    if (current_->PrepareValue()) {
      return true;
    }

    considerStatus(current_->status());
    assert(!status_.ok());
    return false;
  }

  // Here we simply relay MayBeOutOfLowerBound/MayBeOutOfUpperBound result
  // from current child iterator. Potentially as long as one of child iterator
  // report out of bound is not possible, we know current key is within bound.

  bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return current_->MayBeOutOfLowerBound();
  }

  IterBoundCheck UpperBoundCheckResult() override {
    assert(Valid());
    return current_->UpperBoundCheckResult();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    for (auto& child : children_) {
      child.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  bool IsKeyPinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsValuePinned();
  }

 private:
  void InitMaxHeap();
  void InitMinHeap();

  bool is_arena_mode_;
  bool prefix_seek_mode_;
  // Which direction is the iterator moving?
  enum Direction : uint8_t { kForward, kReverse };
  Direction direction_;
  const InternalKeyComparator* comparator_;
  autovector<IteratorWrapper, kNumIterReserve> children_;

  // Cached pointer to child iterator with the current key, or nullptr if no
  // child iterators are valid.  This is the top of minHeap_ or maxHeap_
  // depending on the direction.
  IteratorWrapper* current_;
  // If any of the children have non-ok status, this is one of them.
  Status status_;
  union {
    MergerMinIterHeap minHeap_;
    MergerMaxIterHeap maxHeap_;
  };

  PinnedIteratorsManager* pinned_iters_mgr_;

  // In forward direction, process a child that is not in the min heap.
  // If valid, add to the min heap. Otherwise, check status.
  void AddToMinHeapOrCheckStatus(IteratorWrapper*);

  // In backward direction, process a child that is not in the max heap.
  // If valid, add to the min heap. Otherwise, check status.
  void AddToMaxHeapOrCheckStatus(IteratorWrapper*);

  void SwitchToForward();

  // Switch the direction from forward to backward without changing the
  // position. Iterator should still be valid.
  void SwitchToBackward();

  IteratorWrapper* CurrentForward() const {
    assert(direction_ == kForward);
    return !minHeap_.empty() ? minHeap_.top() : nullptr;
  }

  IteratorWrapper* CurrentReverse() const {
    assert(direction_ == kReverse);
    return !maxHeap_.empty() ? maxHeap_.top() : nullptr;
  }
};

template <class MinHeapComparator, class MaxHeapComparator>
void MergingIterTmpl<MinHeapComparator, MaxHeapComparator>::
    AddToMinHeapOrCheckStatus(IteratorWrapper* child) {
  if (child->Valid()) {
    assert(child->status().ok());
    minHeap_.push(child);
  } else {
    considerStatus(child->status());
  }
}

template <class MinHeapComparator, class MaxHeapComparator>
void MergingIterTmpl<MinHeapComparator, MaxHeapComparator>::MergingIterTmpl::
    AddToMaxHeapOrCheckStatus(IteratorWrapper* child) {
  if (child->Valid()) {
    assert(child->status().ok());
    maxHeap_.push(child);
  } else {
    considerStatus(child->status());
  }
}

template <class MinHeapComparator, class MaxHeapComparator>
void MergingIterTmpl<MinHeapComparator,
                     MaxHeapComparator>::MergingIterTmpl::SwitchToForward() {
  // Otherwise, advance the non-current children.  We advance current_
  // just after the if-block.
  InitMinHeap();
  Slice target = key();
  for (auto& child : children_) {
    if (&child != current_) {
      child.Seek(target);
      // child.status() is set to Status::TryAgain indicating asynchronous
      // request for retrieval of data blocks has been submitted. So it should
      // return at this point and Seek should be called again to retrieve the
      // requested block and add the child to min heap.
      if (child.status() == Status::TryAgain()) {
        continue;
      }
      if (child.Valid() && comparator_->Equal(target, child.key())) {
        assert(child.status().ok());
        child.Next();
      }
    }
    AddToMinHeapOrCheckStatus(&child);
  }

  for (auto& child : children_) {
    if (child.status() == Status::TryAgain()) {
      child.Seek(target);
      if (child.Valid() && comparator_->Equal(target, child.key())) {
        assert(child.status().ok());
        child.Next();
      }
      AddToMinHeapOrCheckStatus(&child);
    }
  }

  direction_ = kForward;
}

template <class MinHeapComparator, class MaxHeapComparator>
void MergingIterTmpl<MinHeapComparator,
                     MaxHeapComparator>::MergingIterTmpl::SwitchToBackward() {
  InitMaxHeap();
  Slice target = key();
  for (auto& child : children_) {
    if (&child != current_) {
      child.SeekForPrev(target);
      TEST_SYNC_POINT_CALLBACK("MergeIterator::Prev:BeforePrev", &child);
      if (child.Valid() && comparator_->Equal(target, child.key())) {
        assert(child.status().ok());
        child.Prev();
      }
    }
    AddToMaxHeapOrCheckStatus(&child);
  }
  direction_ = kReverse;
  if (!prefix_seek_mode_) {
    // Note that we don't do assert(current_ == CurrentReverse()) here
    // because it is possible to have some keys larger than the seek-key
    // inserted between Seek() and SeekToLast(), which makes current_ not
    // equal to CurrentReverse().
    current_ = CurrentReverse();
  }
  assert(current_ == CurrentReverse());
}

template <class MinHeapComparator, class MaxHeapComparator>
void MergingIterTmpl<MinHeapComparator,
                     MaxHeapComparator>::MergingIterTmpl::InitMinHeap() {
  minHeap_.clear();
}

template <class MinHeapComparator, class MaxHeapComparator>
void MergingIterTmpl<MinHeapComparator,
                     MaxHeapComparator>::MergingIterTmpl::InitMaxHeap() {
  // use InitMinHeap(), because maxHeap_ and minHeap_ are physical identical
  InitMinHeap();
}

InternalIterator* NewMergingIterator(const InternalKeyComparator* cmp,
                                     InternalIterator** list, int n,
                                     Arena* arena, bool prefix_seek_mode) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyInternalIterator<Slice>(arena);
  } else if (n == 1) {
    return list[0];
  } else if (IsForwardBytewiseComparator(cmp->user_comparator())) {
    using MergingIterInst =
        MergingIterTmpl<MinInlineBytewiseComp, MaxInlineBytewiseComp>;
    if (arena == nullptr) {
      return new MergingIterInst(cmp, list, n, false, prefix_seek_mode);
    } else {
      auto mem = arena->AllocateAligned(sizeof(MergingIterInst));
      return new (mem) MergingIterInst(cmp, list, n, true, prefix_seek_mode);
    }
  } else if (IsBytewiseComparator(
                 cmp->user_comparator())) {  // must is rev bytewise
    using MergingIterInst =
        MergingIterTmpl<MinInlineRevBytewiseComp, MaxInlineRevBytewiseComp>;
    if (arena == nullptr) {
      return new MergingIterInst(cmp, list, n, false, prefix_seek_mode);
    } else {
      auto mem = arena->AllocateAligned(sizeof(MergingIterInst));
      return new (mem) MergingIterInst(cmp, list, n, true, prefix_seek_mode);
    }
  } else {
    using MergingIterInst =
        MergingIterTmpl<MinIteratorComparator, MaxIteratorComparator>;
    if (arena == nullptr) {
      return new MergingIterInst(cmp, list, n, false, prefix_seek_mode);
    } else {
      auto mem = arena->AllocateAligned(sizeof(MergingIterInst));
      return new (mem) MergingIterInst(cmp, list, n, true, prefix_seek_mode);
    }
  }
}

MergeIteratorBuilder::MergeIteratorBuilder(
    const InternalKeyComparator* comparator, Arena* a, bool prefix_seek_mode)
    : first_iter(nullptr), use_merging_iter(false), arena(a) {
  if (IsForwardBytewiseComparator(comparator->user_comparator())) {
    using MergingIterInst =
        MergingIterTmpl<MinInlineBytewiseComp, MaxInlineBytewiseComp>;
    auto mem = arena->AllocateAligned(sizeof(MergingIterInst));
    merge_iter = new (mem)
        MergingIterInst(comparator, nullptr, 0, true, prefix_seek_mode);
  } else if (IsBytewiseComparator(comparator->user_comparator())) {
    // must is rev bytewise
    using MergingIterInst =
        MergingIterTmpl<MinInlineRevBytewiseComp, MaxInlineRevBytewiseComp>;
    auto mem = arena->AllocateAligned(sizeof(MergingIterInst));
    merge_iter = new (mem)
        MergingIterInst(comparator, nullptr, 0, true, prefix_seek_mode);
  } else {
    using MergingIterInst =
        MergingIterTmpl<MinIteratorComparator, MaxIteratorComparator>;
    auto mem = arena->AllocateAligned(sizeof(MergingIterInst));
    merge_iter = new (mem)
        MergingIterInst(comparator, nullptr, 0, true, prefix_seek_mode);
  }
}

MergeIteratorBuilder::~MergeIteratorBuilder() {
  if (first_iter != nullptr) {
    first_iter->~InternalIterator();
  }
  if (merge_iter != nullptr) {
    merge_iter->~MergingIterator();
  }
}

void MergeIteratorBuilder::AddIterator(InternalIterator* iter) {
  if (!use_merging_iter && first_iter != nullptr) {
    merge_iter->AddIterator(first_iter);
    use_merging_iter = true;
    first_iter = nullptr;
  }
  if (use_merging_iter) {
    merge_iter->AddIterator(iter);
  } else {
    first_iter = iter;
  }
}

InternalIterator* MergeIteratorBuilder::Finish() {
  InternalIterator* ret = nullptr;
  if (!use_merging_iter) {
    ret = first_iter;
    first_iter = nullptr;
  } else {
    ret = merge_iter;
    merge_iter = nullptr;
  }
  return ret;
}

}  // namespace ROCKSDB_NAMESPACE
