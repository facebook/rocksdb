//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <set>

#include "table/internal_iterator.h"

namespace rocksdb {

// A internal wrapper class with an interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
class IteratorWrapper {
 public:
  IteratorWrapper() : iter_(nullptr), iters_pinned_(false), valid_(false) {}
  explicit IteratorWrapper(InternalIterator* _iter)
      : iter_(nullptr), iters_pinned_(false) {
    Set(_iter);
  }
  ~IteratorWrapper() {}
  InternalIterator* iter() const { return iter_; }

  // Takes the ownership of "_iter" and will delete it when destroyed.
  // Next call to Set() will destroy "_iter" except if PinData() was called.
  void Set(InternalIterator* _iter) {
    if (iters_pinned_ && iter_) {
      // keep old iterator until ReleasePinnedData() is called
      pinned_iters_.insert(iter_);
    } else {
      delete iter_;
    }

    iter_ = _iter;
    if (iter_ == nullptr) {
      valid_ = false;
    } else {
      Update();
      if (iters_pinned_) {
        // Pin new iterator
        Status s = iter_->PinData();
        assert(s.ok());
      }
    }
  }

  Status PinData() {
    Status s;
    if (iters_pinned_) {
      return s;
    }

    if (iter_) {
      s = iter_->PinData();
    }

    if (s.ok()) {
      iters_pinned_ = true;
    }

    return s;
  }

  Status ReleasePinnedData() {
    Status s;
    if (!iters_pinned_) {
      return s;
    }

    if (iter_) {
      s = iter_->ReleasePinnedData();
    }

    if (s.ok()) {
      iters_pinned_ = false;
      // No need to call ReleasePinnedData() for pinned_iters_
      // since we will delete them
      DeletePinnedIterators(false);
    }

    return s;
  }

  bool IsKeyPinned() const {
    assert(iter_);
    return iters_pinned_ && iter_->IsKeyPinned();
  }

  void DeleteIter(bool is_arena_mode) {
    if (iter_ && pinned_iters_.find(iter_) == pinned_iters_.end()) {
      DestroyIterator(iter_, is_arena_mode);
    }
    DeletePinnedIterators(is_arena_mode);
  }

  // Iterator interface methods
  bool Valid() const        { return valid_; }
  Slice key() const         { assert(Valid()); return key_; }
  Slice value() const       { assert(Valid()); return iter_->value(); }
  // Methods below require iter() != nullptr
  Status status() const     { assert(iter_); return iter_->status(); }
  void Next()               { assert(iter_); iter_->Next();        Update(); }
  void Prev()               { assert(iter_); iter_->Prev();        Update(); }
  void Seek(const Slice& k) { assert(iter_); iter_->Seek(k);       Update(); }
  void SeekToFirst()        { assert(iter_); iter_->SeekToFirst(); Update(); }
  void SeekToLast()         { assert(iter_); iter_->SeekToLast();  Update(); }

 private:
  void Update() {
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
    }
  }

  void DeletePinnedIterators(bool is_arena_mode) {
    for (auto it : pinned_iters_) {
      DestroyIterator(it, is_arena_mode);
    }
    pinned_iters_.clear();
  }

  inline void DestroyIterator(InternalIterator* it, bool is_arena_mode) {
    if (!is_arena_mode) {
      delete it;
    } else {
      it->~InternalIterator();
    }
  }

  InternalIterator* iter_;
  // If set to true, current and future iterators wont be deleted.
  bool iters_pinned_;
  // List of past iterators that are pinned and wont be deleted as long as
  // iters_pinned_ is true. When we are pinning iterators this set will contain
  // iterators of previous data blocks to keep them from being deleted.
  std::set<InternalIterator*> pinned_iters_;
  bool valid_;
  Slice key_;
};

class Arena;
// Return an empty iterator (yields nothing) allocated from arena.
extern InternalIterator* NewEmptyInternalIterator(Arena* arena);

// Return an empty iterator with the specified status, allocated arena.
extern InternalIterator* NewErrorInternalIterator(const Status& status,
                                                  Arena* arena);

}  // namespace rocksdb
