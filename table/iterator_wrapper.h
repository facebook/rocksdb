//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
namespace rocksdb {

// A internal wrapper class with an interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
class IteratorWrapper {
 public:
  IteratorWrapper(): iter_(nullptr), valid_(false) { }
  explicit IteratorWrapper(Iterator* iter): iter_(nullptr) {
    Set(iter);
  }
  ~IteratorWrapper() { delete iter_; }
  Iterator* iter() const { return iter_; }

  // Takes ownership of "iter" and will delete it when destroyed, or
  // when Set() is invoked again.
  void Set(Iterator* iter) {
    delete iter_;
    iter_ = iter;
    if (iter_ == nullptr) {
      valid_ = false;
    } else {
      Update();
    }
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

  Iterator* iter_;
  bool valid_;
  Slice key_;
};

// A variant of IteratorWrapper allowing peeks at the next key/value before
// calling Next()
class PeekingIteratorWrapper {
 public:
  PeekingIteratorWrapper(): iter_(nullptr), valid_(false), on_next_(false) { }
  explicit PeekingIteratorWrapper(Iterator* iter): iter_(nullptr), on_next_(false) {
    Set(iter);
  }
  ~PeekingIteratorWrapper() { delete iter_; }
  //PeekingIterator* iter() const { return iter_; }

  // Takes ownership of "iter" and will delete it when destroyed, or
  // when Set() is invoked again.
  void Set(Iterator* iter) {
    delete iter_;
    iter_ = iter;
    if (iter_ == nullptr) {
      valid_ = false;
    } else {
      Update();
    }
  }

  // Iterator interface methods
  bool Valid() const        { return valid_; }
  bool HasNext()            { MaybeNext(); return iter_->Valid(); }
  Slice key() const         { assert(Valid()); return key_; }
  Slice value() const       { assert(Valid()); return value_; }
  Slice NextKey()           { MaybeNext(); assert(HasNext()); return iter_->key(); }
  Slice NextValue()         { MaybeNext(); assert(HasNext()); return iter_->value(); }
  // Methods below require iter() != nullptr
  Status status() const     { assert(iter_); return status_; }
  void Next()               { assert(iter_); MaybeNext();                                Update(); }
  void Prev()               { assert(iter_); iter_->Prev(); if (on_next_) iter_->Prev(); Update(); }
  void Seek(const Slice& k) { assert(iter_); iter_->Seek(k);                             Update(); }
  void SeekToFirst()        { assert(iter_); iter_->SeekToFirst();                       Update(); }
  void SeekToLast()         { assert(iter_); iter_->SeekToLast();                        Update(); }

 private:
  void MaybeNext() {
    if (!on_next_ && iter_->Valid()) {
      iter_->Next();
      on_next_ = true;
    }
  }

  void Update() {
    status_ = iter_->status();
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
      value_ = iter_->value();
    }
    on_next_ = false;
  }

  Iterator* iter_;
  bool valid_;
  Status status_;
  bool on_next_;
  Slice key_;
  Slice value_;
}; 

}  // namespace rocksdb
