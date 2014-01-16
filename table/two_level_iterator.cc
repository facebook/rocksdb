//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"
#include "port/port.h"
#include "util/mutexlock.h"


namespace rocksdb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&,
                                   const EnvOptions& soptions, const Slice&,
                                   bool for_compaction);

struct PrefetchState {
  port::Mutex mu;
  bool outstanding;
  std::string data_block_handle_to_prefetch;

  PrefetchState() : outstanding(false) {}
};

class TwoLevelIterator: public Iterator {
 public:
  TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options,
    const EnvOptions& soptions,
    Env* env,
    bool for_compaction,
    bool can_prefetch);

  virtual ~TwoLevelIterator();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();

  virtual bool Valid() const {
    return data_iter_.Valid();
  }
  virtual Slice key() const {
    assert(Valid());
    return data_iter_.key();
  }
  virtual Slice value() const {
    assert(Valid());
    return data_iter_.value();
  }
  virtual Status status() const {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  const EnvOptions& soptions_;
  Env* env_;
  Status status_;
  PeekingIteratorWrapper index_iter_;
  IteratorWrapper data_iter_; // May be nullptr
  // If data_iter_ is non-nullptr, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
  bool for_compaction_;
  bool can_prefetch_;

  PrefetchState* prefetch_;
  void MaybeSchedulePrefetch();
  void PrefetchThread();
  static void PrefetchThreadEntry(void*);
};

TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options,
    const EnvOptions& soptions,
    Env *env,
    bool for_compaction,
    bool can_prefetch)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      soptions_(soptions),
      env_(env),
      index_iter_(index_iter),
      data_iter_(nullptr),
      for_compaction_(for_compaction),
      can_prefetch_(can_prefetch),
      prefetch_(nullptr) {
}

TwoLevelIterator::~TwoLevelIterator() {
  if (prefetch_ != nullptr) {
    // Await completion of any outstanding background prefetching task, since
    // it assumes existence of this
    // FIXME: avoid spinlock
    // FIXME: deadlocks if background thread pool size is 0.
    //        env_->Schedule doesn't give any feedback if this is the case!
    {
      MutexLock l0(&prefetch_->mu);
      prefetch_->data_block_handle_to_prefetch.clear();
    }
    while (true) {
      MutexLock l1(&prefetch_->mu);
      if (!prefetch_->outstanding) {
        break;
      }
    }
    delete prefetch_;
  }
}

void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == nullptr || (!data_iter_.Valid() &&
        !data_iter_.status().IsIncomplete())) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Next();
    MaybeSchedulePrefetch();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || (!data_iter_.Valid() &&
        !data_iter_.status().IsIncomplete())) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
  } else {
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != nullptr
        && handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter = (*block_function_)(arg_, options_, soptions_, handle,
                                          for_compaction_);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

void TwoLevelIterator::PrefetchThreadEntry(void* p) {
  TwoLevelIterator* it = reinterpret_cast<TwoLevelIterator*>(p);
  it->PrefetchThread();
}

// If called for, schedule prefetching (cache-priming) of the next data block
void TwoLevelIterator::MaybeSchedulePrefetch() {
  if (can_prefetch_ && options_.prefetch && !for_compaction_ && index_iter_.HasNext()) {
    // peek at next data block handle
    Slice next_data_block_handle = index_iter_.NextValue();
    if (prefetch_ == nullptr) {
      // lazy initialization of prefetch state
      prefetch_ = new PrefetchState;
    }
    // Update the internal state to signal which block we'd like prefetched
    MutexLock l(&prefetch_->mu);
    prefetch_->data_block_handle_to_prefetch.assign(next_data_block_handle.data(),
                                                    next_data_block_handle.size());
    // Schedule a prefetching task if there is not already one outstanding.
    // Note we do not wait for any outstanding prefetch task to complete,
    // avoiding the need for additional synchronization here. However,
    // depending on the speed of the iterator's consumer vs. the speed of
    // prefetching, some prefetching effort may be wasted, and/or some
    // prefetching opportunities may be missed. That's OK since this is just a
    // cache-priming optimization.
    if (!prefetch_->outstanding) {
      env_->Schedule(&TwoLevelIterator::PrefetchThreadEntry, this, Env::Priority::LOW);
      prefetch_->outstanding = true;
    }
  }
}

// The prefetching background task
void TwoLevelIterator::PrefetchThread() {
  std::string data_block_handle_to_prefetch;
  assert(prefetch_);

  while (true) {
    // Loop for as long as we find a new block to prefetch.
    { // scope MutexLock l
      MutexLock l(&prefetch_->mu);
      if (prefetch_->data_block_handle_to_prefetch.empty() || data_block_handle_to_prefetch == prefetch_->data_block_handle_to_prefetch) {
        prefetch_->outstanding = false;
        return;
      }
      data_block_handle_to_prefetch.assign(prefetch_->data_block_handle_to_prefetch);
    }

    // Open an iterator for the block and immediately throw it away, thus
    // priming various caches: HDD, OS/filesystem, compressed blocks,
    // uncompressed blocks. If the iterator ends up needing the block in the
    // main thread before we finish prefetching it, it will duplicate some of
    // our effort, especially decompression. However, we will at least have
    // helped it by initiating any necessary disk read in advance.
    Iterator* iter = (*block_function_)(arg_, options_, soptions_, Slice(data_block_handle_to_prefetch),
                                          for_compaction_);
    delete iter;
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options,
    const EnvOptions& soptions,
    Env *env,
    bool for_compaction,
    bool can_prefetch) {
  return new TwoLevelIterator(index_iter, block_function, arg,
                              options, soptions, env, for_compaction, can_prefetch);
}

}  // namespace rocksdb
