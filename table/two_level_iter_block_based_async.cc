//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "async/async_status_capture.h"
#include "db/pinned_iterators_manager.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "util/arena.h"

namespace rocksdb {

class TwoLevelIteratorBlockBasedAsync : public InternalIterator,
  private async::AsyncStatusCapture {
 public:

   using Iter = TwoLevelIteratorBlockBasedAsync;

  TwoLevelIteratorBlockBasedAsync(TwoLevelIteratorState* state,
                                  InternalIterator* first_level_iter,
                                  bool need_free_iter_and_state) : state_(state),
    first_level_iter_(first_level_iter),
    need_free_iter_and_state_(need_free_iter_and_state),
    pinned_iters_mgr_(nullptr),
    db_cb_fac_(this),
    iter_cb_fac_(this) {
    on_skip_forward_ =
      db_cb_fac_.GetCallable<&Iter::OnSkipBlocksForward>();
    on_skip_backward_ =
      db_cb_fac_.GetCallable<&Iter::OnSkipBlocksBackward>();
  }

  ~TwoLevelIteratorBlockBasedAsync() {
    // Assert that the TwoLevelIterator is never deleted while Pinning is
    // Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
    first_level_iter_.DeleteIter(!need_free_iter_and_state_);
    second_level_iter_.DeleteIter(false);
    if (need_free_iter_and_state_) {
      delete state_;
    } else {
      state_->~TwoLevelIteratorState();
    }
  }

  void Seek(const Slice& target) override;

  void SeekForPrev(const Slice& target);

  void SeekToFirst() override;

  void SeekToLast() override;

  void Next() override;

  void Prev() override;

  // Async methods. They all have Status as a ret
  // to either indicate async or another condition took
  // place lower in the stack
  Status RequestSeekToFirst(const Callback&) override;

  Status RequestSeekToLast(const Callback&) override;

  Status RequestSeek(const Callback&, const Slice& target) override;

  Status RequestSeekForPrev(const Callback&, const Slice& target) override;

  Status RequestNext(const Callback&) override;

  Status RequestPrev(const Callback&) override;


  virtual bool Valid() const override { return second_level_iter_.Valid(); }
  virtual Slice key() const override {
    assert(Valid());
    return second_level_iter_.key();
  }
  virtual Slice value() const override {
    assert(Valid());
    return second_level_iter_.value();
  }
  virtual Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!first_level_iter_.status().ok()) {
      return first_level_iter_.status();
    } else if (second_level_iter_.iter() != nullptr &&
               !second_level_iter_.status().ok()) {
      return second_level_iter_.status();
    } else {
      return status_;
    }
  }
  virtual void SetPinnedItersMgr(
    PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    first_level_iter_.SetPinnedItersMgr(pinned_iters_mgr);
    if (second_level_iter_.iter()) {
      second_level_iter_.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }
  virtual bool IsKeyPinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           second_level_iter_.iter() && second_level_iter_.IsKeyPinned();
  }
  virtual bool IsValuePinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           second_level_iter_.iter() && second_level_iter_.IsValuePinned();
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }

  using
  SecondaryIteratorCB =
    async::Callable<Status, const Status&, InternalIterator*>;
  Status InitDataBlock(const SecondaryIteratorCB& cb, InternalIterator** iter);

  using IterCB = InternalIterator::Callback;

  Status SkipEmptyDataBlocksForward(const SecondaryIteratorCB&);
  Status SkipEmptyDataBlocksBackward(const SecondaryIteratorCB&);
  void SetSecondLevelIterator(InternalIterator* iter);

  TwoLevelIteratorState* state_;
  IteratorWrapper first_level_iter_;
  IteratorWrapper second_level_iter_;  // May be nullptr
  bool need_free_iter_and_state_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  Status status_;
  // If second_level_iter is non-nullptr, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the second_level_iter.
  std::string data_block_handle_;

  ////////////////////////////////////////////
  // Async operation state
  // Since this iterator is not thread safe and it is not
  // intended to be operated by multiple threads at the same time
  // we expect only one async operation to be in progress at any given time
  // Thus we simplify and eliminate dynamically allocated async
  // contexts and put all the asyn state and callbacks directly into the
  // the iterator
  Callback      cb_;
  Slice         seek_target_;
  // These are initialized once in the ctor
  SecondaryIteratorCB on_skip_forward_;
  SecondaryIteratorCB on_skip_backward_;
  async::CallableFactory<Iter, Status, const Status&, 
    InternalIterator*> db_cb_fac_;
  async::CallableFactory<Iter, Status, const Status&> iter_cb_fac_;

  // Either of these two methods must be called before
  // the async op is initiated
  void InitSync(const Slice& target) {
    cb_.Clear();
    seek_target_ = target;
    async(false);
  }

  void InitSync() {
    cb_.Clear();
    seek_target_.clear();
    async(false);
  }

  void InitAsyncState(const Callback& cb, const Slice& target) {
    cb_ = cb;
    seek_target_ = target;
    async(false);
  }

  void InitAsyncState(const Callback& cb) {
    InitAsyncState(cb, Slice());
  }

  Status SecondarySeek(const IterCB& iter_cb, const Slice& target) {
    Status s;
    if (second_level_iter_.iter() != nullptr) {
      if (cb_) {
       s = second_level_iter_.RequestSeek(iter_cb, target);
      } else {
        second_level_iter_.Seek(target);
      }
    }
    return s;
  }

  Status SecondarySeekForPrev(const IterCB& iter_cb, const Slice& target) {
    Status s;
    if (second_level_iter_.iter() != nullptr) {
      if (cb_) {
        s = second_level_iter_.RequestSeekForPrev(iter_cb, target);
      } else {
        second_level_iter_.SeekForPrev(target);
      }
    }
    return s;
  }

  Status SecondarySeekToFirst(const IterCB& iter_cb) {
    Status s;
    if (second_level_iter_.iter() != nullptr) {
      if (cb_) {
        s = second_level_iter_.RequestSeekToFirst(iter_cb);
      } else {
        second_level_iter_.SeekToFirst();
      }
    }
    return s;
  }

  Status SecondarySeekToLast(const IterCB& iter_cb) {
    Status s;
    if (second_level_iter_.iter() != nullptr) {
      if (cb_) {
        s = second_level_iter_.RequestSeekToLast(iter_cb);
      } else {
        second_level_iter_.SeekToLast();
      }
    }
    return s;
  }

  // To complete InitDataBlock
  void CompleteInitDataBlock(InternalIterator* iter) {
    if (iter != nullptr) {
      Slice handle = first_level_iter_.value();
      data_block_handle_.assign(handle.data(), handle.size());
      SetSecondLevelIterator(iter);
    }
  }

  Status CompleteSkipForward(const IterCB& iter_cb, InternalIterator* iter) {
    Status s;
    CompleteInitDataBlock(iter);
    if (second_level_iter_.iter() != nullptr) {
      if (cb_) {
        s = second_level_iter_.RequestSeekToFirst(iter_cb);
      } else {
        second_level_iter_.SeekToFirst();
      }
    }
    return s;
  }

  Status CompleteSkipBackward(const IterCB& iter_cb, InternalIterator* iter) {
    Status s;
    CompleteInitDataBlock(iter);
    if (second_level_iter_.iter() != nullptr) {
      if (cb_) {
        s = second_level_iter_.RequestSeekToLast(iter_cb);
      } else {
        second_level_iter_.SeekToLast();
      }
    }
    return s;
  }

  // End of all async ops
  Status OnComplete(const Status& status) {
    if (async()) {
      Status s(status);
      s.async(true);
      cb_.Invoke(s);
    }
    return status;
  }

  // This helper expects status instance that was passed
  // to a callback from which this is invoked
  // and thus status should actually reflect if the
  // callback was invoked async or sync
  void UpdateSecondary(const Status& status) {
    // Need to update IteratorWrapper on async cb
    if (status.async()) {
      assert(second_level_iter_.iter() != nullptr);
      second_level_iter_.Update();
    }
  }

  Status OnSkipForwardSeekToFirst(const Status& status) {
    async(status);
    Status s;
    UpdateSecondary(status);
    if (status.ok()) {
      s = SkipEmptyDataBlocksForward(on_skip_forward_);
      if (s.IsIOPending()) {
        return s;
      }
    } else {
      s = status;
    }
    // Sync return indicates everything is done and
    // we go away
    return OnComplete(s);
  }

  Status OnSkipBlocksForward(const Status& status, InternalIterator* iterator) {
    async(status);
    auto on_seek_tofirst = 
      iter_cb_fac_.GetCallable<&Iter::OnSkipForwardSeekToFirst>();
    Status s = CompleteSkipForward(on_seek_tofirst, iterator);
    if (!s.IsIOPending()) {
      s = OnSkipForwardSeekToFirst(s);
    }
    return s;
  }

  Status OnSkipBackwardSeekTolast(const Status& status) {
    async(status);
    Status s;
    UpdateSecondary(status);
    if (status.ok()) {
      s = SkipEmptyDataBlocksBackward(on_skip_backward_);
      if (s.IsIOPending()) {
        return s;
      }
    } else {
      s = status;
    }
    // Sync return indicates everything is done and
    // we go away 
    return OnComplete(s);
  }

  Status OnSkipBlocksBackward(const Status& status, InternalIterator* iterator) {
    async(status);
    Status s;
    auto on_seek_tolast = 
      iter_cb_fac_.GetCallable <&Iter::OnSkipBackwardSeekTolast>();
    s = CompleteSkipBackward(on_seek_tolast, iterator);
    if (!s.IsIOPending()) {
      s = OnSkipBackwardSeekTolast(s);
    }
    return s;
  }

  // Seek
  Status OnSeekInitBlock(const Status& status, InternalIterator* iterator) {
    async(status);
    CompleteInitDataBlock(iterator);
    auto seek_cb = iter_cb_fac_.GetCallable<&Iter::OnSeekCompleteSeek>();
    Status s  = SecondarySeek(seek_cb, seek_target_);
    if (!s.IsIOPending()) {
      s = OnSeekCompleteSeek(s);
    }
    return s;
  }

  Status OnSeekCompleteSeek(const Status& status) {
    async(status);
    Status s;
    UpdateSecondary(status);
    s = SkipEmptyDataBlocksForward(on_skip_forward_);
    if (!s.IsIOPending()) {
      s = OnComplete(s);
    }
    return s;
  }

  // SeekForprev
  Status OnSeekForPrevInitBlock(const Status& status, InternalIterator* iterator) {
    async(status);
    CompleteInitDataBlock(iterator);
    auto on_seek_forprev = 
      iter_cb_fac_.GetCallable<&Iter::OnSeekForPrevCheckValidity>();
    Status s = SecondarySeekForPrev(on_seek_forprev, seek_target_);
    if (!s.IsIOPending()) {
      s = OnSeekForPrevCheckValidity(s);
    }
    return s;
  }

  Status OnSeekForPrevCheckValidity(const Status& status) {
    async(status);
    UpdateSecondary(status);
    Status s;
    if (!Valid()) {
      if (!first_level_iter_.Valid()) {
        first_level_iter_.SeekToLast();
        auto on_init =
          db_cb_fac_.GetCallable<&Iter::OnSeekToPrevSecondInit>();
        InternalIterator* iter = nullptr;
        s = InitDataBlock(on_init, &iter);
        if (!s.IsIOPending()) {
          s = OnSeekToPrevSecondInit(s, iter);
        }
      } else {
        s = SkipEmptyDataBlocksBackward(on_skip_backward_);
      }
    }
    if (!s.IsIOPending()) {
      s = OnComplete(s);
    }
    return s;
  }

  Status OnSeekToPrevSecondInit(const Status& status, InternalIterator* iterator) {
    async(status);
    // Do it again on the last block
    CompleteInitDataBlock(iterator);
    auto on_secondary_prev = 
      iter_cb_fac_.GetCallable<&Iter::OnSeekToPrevSecondaryPrev>();
    Status s = SecondarySeekForPrev(on_secondary_prev, seek_target_);
    if (!s.IsIOPending()) {
      s = SkipEmptyDataBlocksBackward(on_skip_backward_);
    }
    if (!s.IsIOPending()) {
      s = OnComplete(s);
    }
    return s;
  }

  Status OnSeekToPrevSecondaryPrev(const Status& status) {
    async(status);
    UpdateSecondary(status);
    Status s = SkipEmptyDataBlocksBackward(on_skip_backward_);
    if (!s.IsIOPending()) {
      s = OnComplete(s);
    }
    return s;
  }

  // SeekToFirst
  Status OnSeekToFirstInitBlock(const Status& status, InternalIterator* iterator) {
    async(status);
    CompleteInitDataBlock(iterator);
    auto on_secondary_tofirst = 
      iter_cb_fac_.GetCallable<&Iter::OnSecondarySeekToFirst>();
    Status s = SecondarySeekToFirst(on_secondary_tofirst);
    if (!s.IsIOPending()) {
      s = OnSecondarySeekToFirst(s);
    }
    return s;
  }

  Status OnSecondarySeekToFirst(const Status& status) {
    async(status);
    UpdateSecondary(status);
    Status s = SkipEmptyDataBlocksForward(on_skip_forward_);
    if (!s.IsIOPending()) {
      s = OnComplete(s);
    }
    return s;
  }

  /// SeekToLast
  Status OnSkeekToLastInitBlock(const Status& status, InternalIterator* iterator) {
    async(status);
    CompleteInitDataBlock(iterator);
    auto on_secondary_last = 
      iter_cb_fac_.GetCallable<&Iter::OnSecondarySeekToLast>();
    Status s = SecondarySeekToLast(on_secondary_last);
    if (!s.IsIOPending()) {
      s = OnSecondarySeekToLast(s);
    }
    return s;
  }
  Status OnSecondarySeekToLast(const Status& status) {
    async(status);
    UpdateSecondary(status);
    Status s = SkipEmptyDataBlocksBackward(on_skip_backward_);
    if (!s.IsIOPending()) {
      s = OnComplete(s);
    }
    return s;
  }

  // Next
  Status OnNextSecondaryNext(const Status& status) {
    async(status);
    UpdateSecondary(status);
    Status s = SkipEmptyDataBlocksForward(on_skip_forward_);
    if (!s.IsIOPending()) {
      s = OnComplete(s);
    }
    return s;
  }
  // Prev
  Status OnPreevSecondaryPrev(const Status& status) {
    async(status);
    UpdateSecondary(status);
    Status s = SkipEmptyDataBlocksBackward(on_skip_backward_);
    if (!s.IsIOPending()) {
      s = OnComplete(s);
    }
    return s;

  }
};

void TwoLevelIteratorBlockBasedAsync::Seek(const Slice& target) {

  if (state_->check_prefix_may_match &&
      !state_->PrefixMayMatch(target)) {
    SetSecondLevelIterator(nullptr);
    return;
  }
  InitSync(target);
  first_level_iter_.Seek(target);
  InternalIterator* iter = nullptr;
  Status s = InitDataBlock(SecondaryIteratorCB(), &iter);
  assert(!s.IsIOPending());
  OnSeekInitBlock(s, iter);
}

Status TwoLevelIteratorBlockBasedAsync::RequestSeek(const Callback& cb,
    const Slice& target) {

  Status s;
  if (state_->check_prefix_may_match &&
      !state_->PrefixMayMatch(target)) {
    SetSecondLevelIterator(nullptr);
    return s;
  }
  assert(cb);
  InitAsyncState(cb, target);
  first_level_iter_.Seek(target);

  auto on_init_block =
    db_cb_fac_.GetCallable<&Iter::OnSeekInitBlock>();

  InternalIterator* iter = nullptr;
  s = InitDataBlock(on_init_block, &iter);
  if (!s.IsIOPending()) {
    s = OnSeekInitBlock(s, iter);
  }
  return s;
}

void TwoLevelIteratorBlockBasedAsync::SeekForPrev(const Slice& target) {

  if (state_->check_prefix_may_match && !state_->PrefixMayMatch(target)) {
    SetSecondLevelIterator(nullptr);
    return ;
  }
  InitSync(target);
  first_level_iter_.Seek(target);
  InternalIterator* iter = nullptr;
  Status s = InitDataBlock(SecondaryIteratorCB(), &iter);
  assert(!s.IsIOPending());
  OnSeekForPrevInitBlock(s, iter);
}

Status TwoLevelIteratorBlockBasedAsync::RequestSeekForPrev(const Callback& cb,
    const Slice& target) {

  Status s;
  if (state_->check_prefix_may_match && !state_->PrefixMayMatch(target)) {
    SetSecondLevelIterator(nullptr);
    return s;
  }

  InitAsyncState(cb, target);
  first_level_iter_.Seek(target);

  auto on_init_block =
    db_cb_fac_.GetCallable<&Iter::OnSeekForPrevInitBlock>();

  InternalIterator* iter = nullptr;
  s = InitDataBlock(on_init_block, &iter);
  if (!s.IsIOPending()) {
    s = OnSeekForPrevInitBlock(s, iter);
  }
  return s;
}

void TwoLevelIteratorBlockBasedAsync::SeekToFirst() {
  first_level_iter_.SeekToFirst();
  InitSync();
  InternalIterator* iter = nullptr;
  Status s = InitDataBlock(SecondaryIteratorCB(), &iter);
  assert(!s.IsIOPending());
  OnSeekToFirstInitBlock(s, iter);
}

Status TwoLevelIteratorBlockBasedAsync::RequestSeekToFirst(const Callback& cb) {
  first_level_iter_.SeekToFirst();
  InitAsyncState(cb);
  auto on_seek_to_first = 
    db_cb_fac_.GetCallable<&Iter::OnSeekToFirstInitBlock>();
  InternalIterator* iter = nullptr;
  Status s = InitDataBlock(on_seek_to_first, &iter);
  if (!s.IsIOPending()) {
    s = OnSeekToFirstInitBlock(s, iter);
  }
  return s;
}

void TwoLevelIteratorBlockBasedAsync::SeekToLast() {
  InitSync();
  first_level_iter_.SeekToLast();
  InternalIterator* iter = nullptr;
  Status s = InitDataBlock(SecondaryIteratorCB(), &iter);
  assert(!s.IsIOPending());
  OnSkeekToLastInitBlock(s, iter);
}

Status TwoLevelIteratorBlockBasedAsync::RequestSeekToLast(const Callback& cb) {
  InitAsyncState(cb);
  first_level_iter_.SeekToLast();

  auto on_seek_tolast = 
    db_cb_fac_.GetCallable<&Iter::OnSkeekToLastInitBlock>();
  InternalIterator* iter = nullptr;
  Status s = InitDataBlock(on_seek_tolast, &iter);
  if (!s.IsIOPending()) {
    s = OnSkeekToLastInitBlock(s, iter);
  }
  return s;
}

void TwoLevelIteratorBlockBasedAsync::Next() {
  assert(Valid());
  InitSync();
  second_level_iter_.Next();
  Status s = SkipEmptyDataBlocksForward(SecondaryIteratorCB());
  assert(!s.IsIOPending());
  OnComplete(s);
}

Status TwoLevelIteratorBlockBasedAsync::RequestNext(const Callback& cb) {
  assert(Valid());
  InitAsyncState(cb);
  auto on_secondary_next = 
    iter_cb_fac_.GetCallable<&Iter::OnNextSecondaryNext>();
  Status s  = second_level_iter_.RequestNext(on_secondary_next);
  if (!s.IsIOPending()) {
    s = OnNextSecondaryNext(s);
  }
  return s;
}

void TwoLevelIteratorBlockBasedAsync::Prev() {
  assert(Valid());
  InitSync();
  second_level_iter_.Prev();
  Status s = SkipEmptyDataBlocksBackward(SecondaryIteratorCB());
  assert(!s.IsIOPending());
  OnComplete(s);
}

Status TwoLevelIteratorBlockBasedAsync::RequestPrev(const Callback& cb) {
  assert(Valid());
  InitAsyncState(cb);
  auto on_secondary_prev = 
    iter_cb_fac_.GetCallable<&Iter::OnPreevSecondaryPrev>();
  Status s  = second_level_iter_.RequestPrev(on_secondary_prev);
  if (!s.IsIOPending()) {
    s = OnPreevSecondaryPrev(s);
  }
  return s;
}

Status TwoLevelIteratorBlockBasedAsync::SkipEmptyDataBlocksForward(
  const SecondaryIteratorCB& cb) {

  Status s;
  auto on_seek_tofirst =
    iter_cb_fac_.GetCallable<&Iter::OnSkipForwardSeekToFirst>();
  while (second_level_iter_.iter() == nullptr ||
         (!second_level_iter_.Valid() &&
          !second_level_iter_.status().IsIncomplete())) {
    // Move to next block
    if (!first_level_iter_.Valid() ||
        state_->KeyReachedUpperBound(first_level_iter_.key())) {
      SetSecondLevelIterator(nullptr);
      break;
    }
    first_level_iter_.Next();
    InternalIterator* iter = nullptr;
    s = InitDataBlock(cb, &iter);
    if (!s.IsIOPending()) {
      s = CompleteSkipForward(on_seek_tofirst, iter);
    }
    if (s.IsIOPending()) {
      break;
    }
  }
  return s;
}

Status TwoLevelIteratorBlockBasedAsync::SkipEmptyDataBlocksBackward(
  const SecondaryIteratorCB& cb) {
  Status s;
  auto on_seek_tolast =
    iter_cb_fac_.GetCallable <&Iter::OnSkipBackwardSeekTolast>();
  while (second_level_iter_.iter() == nullptr ||
         (!second_level_iter_.Valid() &&
          !second_level_iter_.status().IsIncomplete())) {
    // Move to next block
    if (!first_level_iter_.Valid()) {
      SetSecondLevelIterator(nullptr);
      break;
    }
    first_level_iter_.Prev();
    InternalIterator* iter = nullptr;
    s = InitDataBlock(cb, &iter);
    if (!s.IsIOPending()) {
      s = CompleteSkipBackward(on_seek_tolast, iter);
    }
    if (s.IsIOPending()) {
      break;
    }
  }
  return s;
}

void TwoLevelIteratorBlockBasedAsync::SetSecondLevelIterator(
  InternalIterator* iter) {
  if (second_level_iter_.iter() != nullptr) {
    SaveError(second_level_iter_.status());
  }

  if (pinned_iters_mgr_ && iter) {
    iter->SetPinnedItersMgr(pinned_iters_mgr_);
  }

  InternalIterator* old_iter = second_level_iter_.Set(iter);
  if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
    pinned_iters_mgr_->PinIterator(old_iter);
  } else {
    delete old_iter;
  }
}

Status TwoLevelIteratorBlockBasedAsync::InitDataBlock(
  const SecondaryIteratorCB& cb, InternalIterator** iter) {
  Status s;
  assert(iter != nullptr);
  *iter = nullptr;
  if (!first_level_iter_.Valid()) {
    SetSecondLevelIterator(nullptr);
  } else {
    Slice handle = first_level_iter_.value();
    if (second_level_iter_.iter() != nullptr &&
        !second_level_iter_.status().IsIncomplete() &&
        handle.compare(data_block_handle_) == 0) {
      // second_level_iter is already constructed with this iterator, so
      // no need to change anything
    } else {
      if (cb_) {
        s = state_->NewSecondaryIterator(cb, handle, iter);
      } else {
        *iter = state_->NewSecondaryIterator(handle);
      }
    }
  }
  return s;
}

InternalIterator* NewTwoLevelBlockAsyncIterator(TwoLevelIteratorState* state,
    InternalIterator* first_level_iter,
    Arena* arena,
    bool need_free_iter_and_state) {
  if (arena == nullptr) {
    return new TwoLevelIteratorBlockBasedAsync(state, first_level_iter,
           need_free_iter_and_state);
  } else {
    auto mem = arena->AllocateAligned(sizeof(TwoLevelIteratorBlockBasedAsync));
    return new (mem)
           TwoLevelIteratorBlockBasedAsync(state, first_level_iter,
                                           need_free_iter_and_state);
  }
}

}  // namespace rocksdb
