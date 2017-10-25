//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "async/async_status_capture.h"
#include "table/merging_iterator.h"
#include <string>
#include <vector>
#include "db/pinned_iterators_manager.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "table/internal_iterator.h"
#include "table/iter_heap.h"
#include "table/iterator_wrapper.h"
#include "util/arena.h"
#include "util/autovector.h"
#include "util/heap.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

namespace rocksdb {
// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace {
typedef BinaryHeap<IteratorWrapper*, MaxIteratorComparator> MergerMaxIterHeap;
typedef BinaryHeap<IteratorWrapper*, MinIteratorComparator> MergerMinIterHeap;
}  // namespace

const size_t kNumIterReserve = 4;

class MergingIteratorAsync : public InternalIterator,
  private async::AsyncStatusCapture {
 public:
  MergingIteratorAsync(const Comparator* comparator, InternalIterator** children,
                       int n, bool is_arena_mode, bool prefix_seek_mode)
    : is_arena_mode_(is_arena_mode),
      comparator_(comparator),
      current_(nullptr),
      direction_(kForward),
      minHeap_(comparator_),
      prefix_seek_mode_(prefix_seek_mode),
      pinned_iters_mgr_(nullptr),
      child_num_(0) {
    children_.resize(n);
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    for (auto& child : children_) {
      if (child.Valid()) {
        minHeap_.push(&child);
      }
    }
    current_ = CurrentForward();
  }

  virtual void AddIterator(InternalIterator* iter) {
    assert(direction_ == kForward);
    children_.emplace_back(iter);
    if (pinned_iters_mgr_) {
      iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
    auto new_wrapper = children_.back();
    if (new_wrapper.Valid()) {
      minHeap_.push(&new_wrapper);
      current_ = CurrentForward();
    }
  }

  virtual ~MergingIteratorAsync() {
    for (auto& child : children_) {
      child.DeleteIter(is_arena_mode_);
    }
  }

  virtual bool Valid() const override {
    return (current_ != nullptr);
  }

  virtual void SeekToFirst() override {
    InitSync();
    ClearHeaps();
    IterateSeekToFirst();
  }

  Status RequestSeekToFirst(const Callback& cb) override {
    InitAsync(cb);
    ClearHeaps();
    return IterateSeekToFirst();
  }

  virtual void SeekToLast() override {
    InitSync();
    ClearHeaps();
    InitMaxHeap();
    IterateSeekToLast();
  }

  Status RequestSeekToLast(const Callback& cb) override {
    InitAsync(cb);
    ClearHeaps();
    InitMaxHeap();
    return IterateSeekToLast();
  }

  virtual void Seek(const Slice& target) override {
    InitSync(target);
    ClearHeaps();
    IterateSeek();
  }

  Status RequestSeek(const Callback& cb, const Slice& target) override {
    InitAsync(cb, target);
    ClearHeaps();
    return IterateSeek();
  }

  virtual void SeekForPrev(const Slice& target) override {
    InitSync(target);
    ClearHeaps();
    InitMaxHeap();
    IterateSeekForPrev();
  }

  Status RequestSeekForPrev(const Callback& cb, const Slice& target) override {
    InitAsync(cb, target);
    ClearHeaps();
    InitMaxHeap();
    return IterateSeekForPrev();
  }

  virtual void Next() override {
    assert(Valid());
    InitSync();
    Status s;
    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current children since current_ is
    // the smallest child and key() == current_->key().
    if (direction_ != kForward) {
      // Otherwise, advance the non-current children.  We advance current_
      // just after the if-block.
      ClearHeaps();
      IterateNext();
    } else {
      NextContinue(s);
    }
  }

  Status RequestNext(const Callback& cb) override {
    assert(Valid());
    InitAsync(cb);
    Status s;
    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current children since current_ is
    // the smallest child and key() == current_->key().
    if (direction_ != kForward) {
      // Otherwise, advance the non-current children.  We advance current_
      // just after the if-block.
      ClearHeaps();
      return IterateNext();
    }
    return NextContinue(s);
  }

  virtual void Prev() override {
    assert(Valid());
    InitSync();
    PrevAsyncImpl();
  }

  Status RequestPrev(const Callback& cb) override {
    assert(Valid());
    InitAsync(cb);
    return PrevAsyncImpl();
  }

  virtual Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  virtual Status status() const override {
    Status s;
    for (auto& child : children_) {
      s = child.status();
      if (!s.ok()) {
        break;
      }
    }
    return s;
  }

  virtual void SetPinnedItersMgr(
    PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    for (auto& child : children_) {
      child.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  virtual bool IsKeyPinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsKeyPinned();
  }

  virtual bool IsValuePinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsValuePinned();
  }

 private:
  // Clears heaps for both directions, used when changing direction or seeking
  void ClearHeaps() {
    minHeap_.clear();
    if (maxHeap_) {
      maxHeap_->clear();
    }
  }
  // Ensures that maxHeap_ is initialized when starting to go in the reverse
  // direction
  void InitMaxHeap() {
    if (!maxHeap_) {
      maxHeap_.reset(new MergerMaxIterHeap(comparator_));
    }
  }

  bool is_arena_mode_;
  const Comparator* comparator_;
  autovector<IteratorWrapper, kNumIterReserve> children_;

  // Cached pointer to child iterator with the current key, or nullptr if no
  // child iterators are valid.  This is the top of minHeap_ or maxHeap_
  // depending on the direction.
  IteratorWrapper* current_;
  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
  MergerMinIterHeap minHeap_;
  bool prefix_seek_mode_;

  // Max heap is used for reverse iteration, which is way less common than
  // forward.  Lazily initialize it to save memory.
  std::unique_ptr<MergerMaxIterHeap> maxHeap_;
  PinnedIteratorsManager* pinned_iters_mgr_;

  IteratorWrapper* CurrentForward() const {
    assert(direction_ == kForward);
    return !minHeap_.empty() ? minHeap_.top() : nullptr;
  }

  IteratorWrapper* CurrentReverse() const {
    assert(direction_ == kReverse);
    assert(maxHeap_);
    return !maxHeap_->empty() ? maxHeap_->top() : nullptr;
  }

  // Async additions
  Callback    cb_;
  size_t      child_num_;
  Slice       target_;
  PERF_METER_DECL(seek_child_seek_time);

  using MI = MergingIteratorAsync;

  void InitSync(const Slice& target) {
    cb_.Clear();
    child_num_ = 0;
    target_ = target;
    async(false);
  }

  void InitSync() {
    cb_.Clear();
    child_num_ = 0;
    target_.clear();
    async(false);
  }

  void InitAsync(const Callback& cb, const Slice& target) {
    cb_ = cb;
    child_num_ = 0;
    target_ = target;
    async(false);
  }

  void InitAsync(const Callback& cb) {
    InitAsync(cb, Slice());
  }

  bool AsyncSetup() const {
    return cb_;
  }

  void Complete(const Status& status) {
    if (async()) {
      assert(cb_);
      Status s(status);
      s.async(true);
      cb_.Invoke(s);
    }
  }

  void UpdateAndMinPush() {
    auto& child = children_[child_num_];
    child.Update();
    if (child.Valid()) {
      minHeap_.push(&child);
    }
  }
  void UpdateAndMinPushWithTimer() {
    auto& child = children_[child_num_];
    child.Update();
    if (child.Valid()) {
      PERF_TIMER_GUARD(seek_min_heap_time);
      minHeap_.push(&child);
    }
  }
  void UpdateAndMaxPush() {
    auto& child = children_[child_num_];
    child.Update();
    if (child.Valid()) {
      maxHeap_->push(&child);
    }
  }
  void UpdateAndMaxPushWithTimer() {
    auto& child = children_[child_num_];
    child.Update();
    if (child.Valid()) {
      PERF_TIMER_GUARD(seek_min_heap_time);
      maxHeap_->push(&child);
    }
  }

  /////////////////////////////////////////////////////////////
  /// SeekToFirst()
  Status IterateSeekToFirst() {
    Status s;
    async::CallableFactory<MI, Status, const Status&> f(this);
    auto on_seek_first = f.GetCallable<&MI::OnSeekFirstIteration>();
    for (; child_num_ < children_.size(); ++child_num_) {
      auto& child = children_[child_num_];
      if (AsyncSetup()) {
        s = child.RequestSeekToFirst(on_seek_first);
        if (s.IsIOPending()) {
          return s;
        }
      } else {
        child.SeekToFirst();
      }
      if (child.Valid()) {
        minHeap_.push(&child);
      }
    }
    CompleteSeekToFirst(s);
    return s;
  }

  Status OnSeekFirstIteration(const Status& status) {
    assert(status.async());
    async(status);
    UpdateAndMinPush();
    ++child_num_;
    if (child_num_ < children_.size()) {
      IterateSeekToFirst();
    } else {
      CompleteSeekToFirst(status);
    }
    return status;
  }
  void CompleteSeekToFirst(const Status& status) {
    direction_ = kForward;
    current_ = CurrentForward();
    Complete(status);
  }
  ////////////////////////////////////////////////////////////
  // SeekToLast()
  Status IterateSeekToLast() {
    Status s;
    async::CallableFactory<MI, Status, const Status&> f(this);
    auto on_seek_last = f.GetCallable<&MI::OnSeekLastIteration>();
    for (; child_num_ < children_.size(); ++child_num_) {
      auto& child = children_[child_num_];
      if (AsyncSetup()) {
        s = child.RequestSeekToLast(on_seek_last);
        if (s.IsIOPending()) {
          return s;
        }
      } else {
        child.SeekToLast();
      }
      if (child.Valid()) {
        maxHeap_->push(&child);
      }
    }
    CompleteSeekToLast(s);
    return s;
  }

  Status OnSeekLastIteration(const Status& status) {
    assert(status.async());
    async(status);
    UpdateAndMaxPush();
    ++child_num_;
    if (child_num_ < children_.size()) {
      IterateSeekToLast();
    } else {
      CompleteSeekToLast(status);
    }
    return status;
  }

  void CompleteSeekToLast(const Status& status) {
    direction_ = kReverse;
    current_ = CurrentReverse();
    Complete(status);
  }
  /////////////////////////////////////////////////
  /// Seek()
  Status IterateSeek() {
    Status s;
    async::CallableFactory<MI, Status, const Status&> f(this);
    auto on_seek_seek = f.GetCallable<&MI::OnSeekSeek>();
    for (; child_num_ < children_.size(); ++child_num_) {
      auto& child = children_[child_num_];
      PERF_METER_START(seek_child_seek_time);
      if (AsyncSetup()) {
        s = child.RequestSeek(on_seek_seek, target_);
        if (s.IsIOPending()) {
          return s;
        }
      } else {
        child.Seek(target_);
      }
      PERF_METER_STOP(seek_child_seek_time);
      PERF_COUNTER_ADD(seek_child_seek_count, 1);
      if (child.Valid()) {
        PERF_TIMER_GUARD(seek_min_heap_time);
        minHeap_.push(&child);
      }
    }
    CompleteSeek(s);
    return s;
  }

  Status OnSeekSeek(const Status& status) {
    PERF_METER_STOP(seek_child_seek_time);
    assert(status.async());
    async(status);
    PERF_COUNTER_ADD(seek_child_seek_count, 1);
    UpdateAndMinPushWithTimer();
    ++child_num_;
    if (child_num_ < children_.size()) {
      IterateSeek();
    } else {
      CompleteSeek(status);
    }
    return status;
  }
  void CompleteSeek(const Status& status) {
    direction_ = kForward;
    {
      PERF_TIMER_GUARD(seek_min_heap_time);
      current_ = CurrentForward();
    }
    Complete(status);
  }

  ////////////////////////////////////////////
  // SeekForPrev()
  Status IterateSeekForPrev() {
    Status s;
    async::CallableFactory<MI, Status, const Status&> f(this);
    auto on_seek_prev = f.GetCallable<&MI::OnSeekForPrev>();
    for (; child_num_ < children_.size(); ++child_num_) {
      auto& child = children_[child_num_];
      PERF_METER_START(seek_child_seek_time);
      if (AsyncSetup()) {
        s = child.RequestSeekForPrev(on_seek_prev, target_);
        if (s.IsIOPending()) {
          return s;
        }
      } else {
        child.SeekForPrev(target_);
      }
      PERF_METER_STOP(seek_child_seek_time);
      PERF_COUNTER_ADD(seek_child_seek_count, 1);
      if (child.Valid()) {
        PERF_TIMER_GUARD(seek_min_heap_time);
        maxHeap_->push(&child);
      }
    }
    CompleteSeekForPrev(s);
    return s;
  }
  Status OnSeekForPrev(const Status& status) {
    PERF_METER_STOP(seek_child_seek_time);
    assert(status.async());
    async(status);
    PERF_COUNTER_ADD(seek_child_seek_count, 1);
    UpdateAndMaxPushWithTimer();
    ++child_num_;
    if (child_num_ < children_.size()) {
      IterateSeekForPrev();
    } else {
      CompleteSeekForPrev(status);
    }
    return status;
  }
  void CompleteSeekForPrev(const Status& status) {
    direction_ = kReverse;
    {
      PERF_TIMER_GUARD(seek_max_heap_time);
      current_ = CurrentReverse();
    }
    Complete(status);
  }
  ///////////////////////////////////////////////
  // Next()
  Status IterateNext() {
    Status s;
    async::CallableFactory<MI, Status, const Status&> f(this);
    auto next_seek_continue = f.GetCallable<&MI::NextSeekContinue>();
    auto next_seek_next_continue = f.GetCallable<&MI::NextSeekNextContinue>();
    for (; child_num_ < children_.size(); ++child_num_) {
      auto& child = children_[child_num_];
      if (&child != current_) {
        if (AsyncSetup()) {
          s = child.RequestSeek(next_seek_continue, key());
          if (s.IsIOPending()) {
            return s;
          }
        } else {
          child.Seek(key());
        }
        if (child.Valid() && comparator_->Equal(key(), child.key())) {
          if (AsyncSetup()) {
            s = child.RequestNext(next_seek_next_continue);
            if (s.IsIOPending()) {
              return s;
            }
          } else {
            child.Next();
          }
        }
      }
      if (child.Valid()) {
        minHeap_.push(&child);
      }
    }

    direction_ = kForward;
    // The loop advanced all non-current children to be > key() so current_
    // should still be strictly the smallest key.
    assert(current_ == CurrentForward());

    return NextContinue(s);
  }

  Status NextSeekContinue(const Status& status) {
    assert(status.async());
    async(status);
    Status s;
    auto& child = children_[child_num_];
    child.Update();
    if (child.Valid() && comparator_->Equal(key(), child.key())) {
      if (AsyncSetup()) {
        async::CallableFactory<MI, Status, const Status&> f(this);
        auto next_seek_next_continue = f.GetCallable<&MI::NextSeekNextContinue>();
        s = child.RequestNext(next_seek_next_continue);
        if (s.IsIOPending()) {
          return s;
        }
      } else {
        child.Next();
      }
    }
    assert(!s.IsIOPending());
    if (child.Valid()) {
      minHeap_.push(&child);
    }
    ++child_num_;
    return IterateNext();
  }

  // Invoked after next is performed when seek results in
  // equal keys
  Status NextSeekNextContinue(const Status& status) {
    assert(status.async());
    async(status);
    UpdateAndMinPush();
    ++child_num_;
    return IterateNext();
  }
  // Second half of Next
  Status NextContinue(const Status& status) {
    Status s;
    // The loop advanced all non-current children to be > key() so current_
    // should still be strictly the smallest key.
    assert(current_ == CurrentForward());
    // as the current points to the current record. move the iterator forward.
    if (AsyncSetup()) {
      async::CallableFactory<MI, Status, const Status&> f(this);
      auto complete_next_async = f.GetCallable<&MI::CompleteNextAsync>();
      s = current_->RequestNext(complete_next_async);
      if (s.IsIOPending()) {
        return s;
      }
    } else {
      current_->Next();
    }
    CompleteNextSync(s);
    return s;
  }
  // Complete Next() synchronously
  void CompleteNextSync(const Status& status) {
    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentForward());
    if (current_->Valid()) {
      // current is still valid after the Next() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      minHeap_.replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      minHeap_.pop();
    }
    current_ = CurrentForward();
    Complete(status);
  }
  // Async callback, needs to call Update()
  Status CompleteNextAsync(const Status& status) {
    assert(status.async());
    async(status);
    current_->Update();
    CompleteNextSync(status);
    return status;
  }
  /////////////////////////////////////////////////////////////
  // Prev()
  Status PrevAsyncImpl() {
    Status s;
    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current children since current_ is
    // the largest child and key() == current_->key().
    if (direction_ != kReverse) {
      // Otherwise, retreat the non-current children.  We retreat current_
      // just after the if-block.
      ClearHeaps();
      InitMaxHeap();
      s =  IteratePrev();
    } else {
      s = PrevContinue();
    }
    return s;
  }

  // Second half of Prev()
  Status PrevContinue() {
    Status s;
    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentReverse());
    if (AsyncSetup()) {
      async::CallableFactory<MI, Status, const Status&> f(this);
      auto complete_async = f.GetCallable<&MI::CompletePrevAsync>();
      s = current_->RequestPrev(complete_async);
      if (s.IsIOPending()) {
        return s;
      }
    } else {
      current_->Prev();
    }
    CompletePrevSync(s);
    return s;
  }

  // Inline helper
  Status ChildValidInvalidAfterSeek() {
    Status s;
    async::CallableFactory<MI, Status, const Status&> f(this);
    auto prev_prev_seek_tolast_continue =
      f.GetCallable<&MI::PrevPrevOrSeekToLastContinue>();
    auto& child = children_[child_num_];
    if (child.Valid()) {
      // Child is at first entry >= key().  Step back one to be < key()
      TEST_SYNC_POINT_CALLBACK("MergeIterator::Prev:BeforePrev",
                               &child);
      if (AsyncSetup()) {
        s = child.RequestPrev(prev_prev_seek_tolast_continue);
      } else {
        child.Prev();
      }
    } else {
      // Child has no entries >= key().  Position at last entry.
      TEST_SYNC_POINT("MergeIterator::Prev:BeforeSeekToLast");
      if (AsyncSetup()) {
        s = child.RequestSeekToLast(prev_prev_seek_tolast_continue);
      } else {
        child.SeekToLast();
      }
    }
    return s;
  }
  // Inline helper
  Status PrefixModeChildNeedPrev() {
    Status s;
    auto& child = children_[child_num_];
    if (child.Valid() && comparator_->Equal(key(), child.key())) {
      if (AsyncSetup()) {
        async::CallableFactory<MI, Status, const Status&> f(this);
        auto prev_prev_seek_tolast_continue =
          f.GetCallable<&MI::PrevPrevOrSeekToLastContinue>();
        s = child.RequestPrev(prev_prev_seek_tolast_continue);
      } else {
        child.Prev();
      }
    }
    return s;
  }

  Status IteratePrev() {
    Status s;
    async::CallableFactory<MI, Status, const Status&> f(this);
    auto prev_seek_continue = f.GetCallable<&MI::PrevSeekContinue>();
    auto seek_forprev_continue = f.GetCallable<&MI::PrevSeekForPrevContinue>();
    for (; child_num_ < children_.size(); ++child_num_) {
      auto& child = children_[child_num_];
      if (&child != current_) {
        if (!prefix_seek_mode_) {
          if (AsyncSetup()) {
            s = child.RequestSeek(prev_seek_continue, key());
            if (s.IsIOPending()) {
              return s;
            }
          } else {
            child.Seek(key());
          }
          s = ChildValidInvalidAfterSeek();
        } else {
          if (AsyncSetup()) {
            s = child.RequestSeekForPrev(seek_forprev_continue, key());
            if (s.IsIOPending()) {
              return s;
            }
          } else {
            child.SeekForPrev(key());
          }
          s = PrefixModeChildNeedPrev();
        }
      }
      if (s.IsIOPending()) {
        return s;
      }
      if (child.Valid()) {
        maxHeap_->push(&child);
      }
    }
    assert(!s.IsIOPending());
    direction_ = kReverse;
    if (!prefix_seek_mode_) {
      // Note that we don't do assert(current_ == CurrentReverse()) here
      // because it is possible to have some keys larger than the seek-key
      // inserted between Seek() and SeekToLast(), which makes current_ not
      // equal to CurrentReverse().
      current_ = CurrentReverse();
    }
    return PrevContinue();
  }
  Status PrevSeekContinue(const Status& status) {
    assert(status.async());
    async(status);
    Status s;
    children_[child_num_].Update();
    s = ChildValidInvalidAfterSeek();
    if (s.IsIOPending()) {
      return s;
    }
    auto& child = children_[child_num_];
    if (child.Valid()) {
      maxHeap_->push(&child);
    }
    ++child_num_;
    return IteratePrev();
  }

  Status PrevSeekForPrevContinue(const Status& status) {
    assert(status.async());
    async(status);
    Status s;
    children_[child_num_].Update();
    s = PrefixModeChildNeedPrev();
    if (s.IsIOPending()) {
      return s;
    }
    auto& child = children_[child_num_];
    if (child.Valid()) {
      maxHeap_->push(&child);
    }
    ++child_num_;
    return IteratePrev();
  }

  Status PrevPrevOrSeekToLastContinue(const Status& status) {
    assert(status.async());
    assert(status.async());
    async(status);
    Status s;
    auto& child = children_[child_num_];
    child.Update();
    if (child.Valid()) {
      maxHeap_->push(&child);
    }
    ++child_num_;
    return IteratePrev();
  }

  // Complete Prev() synchroniously
  void CompletePrevSync(const Status& status) {
    if (current_->Valid()) {
      // current is still valid after the Prev() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      maxHeap_->replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      maxHeap_->pop();
    }
    current_ = CurrentReverse();
    Complete(status);
  }
  Status CompletePrevAsync(const Status& status) {
    assert(status.async());
    async(status);
    current_->Update();
    CompletePrevSync(status);
    return status;
  }
};

InternalIterator * NewMergingIteratorAsync(const Comparator * cmp,
    InternalIterator** list, int n,
    Arena * arena,
    bool prefix_seek_mode) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyInternalIterator(arena);
  } else if (n == 1) {
    return list[0];
  } else {
    if (arena == nullptr) {
      return new MergingIteratorAsync(cmp, list, n, false, prefix_seek_mode);
    } else {
      auto mem = arena->AllocateAligned(sizeof(MergingIteratorAsync));
      return new (mem) MergingIteratorAsync(cmp, list, n, true, prefix_seek_mode);
    }
  }
}

MergeIteratorBuilderAsync::MergeIteratorBuilderAsync(const Comparator*
    comparator, Arena* a, bool prefix_seek_mode)
  : first_iter(nullptr), use_merging_iter(false), arena(a) {
  auto mem = arena->AllocateAligned(sizeof(MergingIteratorAsync));
  merge_iter = new (mem) MergingIteratorAsync(comparator, nullptr, 0, true,
      prefix_seek_mode);
}

void MergeIteratorBuilderAsync::AddIterator(InternalIterator* iter) {
  if (!use_merging_iter && first_iter != nullptr) {
    merge_iter->AddIterator(first_iter);
    use_merging_iter = true;
  }
  if (use_merging_iter) {
    merge_iter->AddIterator(iter);
  } else {
    first_iter = iter;
  }
}

InternalIterator* MergeIteratorBuilderAsync::Finish() {
  if (!use_merging_iter) {
    return first_iter;
  } else {
    auto ret = merge_iter;
    merge_iter = nullptr;
    return ret;
  }
}

}  // namespace rocksdb
