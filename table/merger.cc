//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "table/iter_heap.h"
#include "table/iterator_wrapper.h"

#include <vector>

namespace rocksdb {

namespace {

class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(n),
        current_(nullptr),
        direction_(kForward),
        maxHeap_(NewMaxIterHeap(comparator_)),
        minHeap_ (NewMinIterHeap(comparator_)) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    for (auto& child : children_) {
      if (child.Valid()) {
        minHeap_.push(&child);
      }
    }
  }

  virtual ~MergingIterator() { }

  virtual bool Valid() const {
    return (current_ != nullptr);
  }

  virtual void SeekToFirst() {
    ClearHeaps();
    for (auto& child : children_) {
      child.SeekToFirst();
      if (child.Valid()) {
        minHeap_.push(&child);
      }
    }
    FindSmallest();
    direction_ = kForward;
  }

  virtual void SeekToLast() {
    ClearHeaps();
    for (auto& child : children_) {
      child.SeekToLast();
      if (child.Valid()) {
        maxHeap_.push(&child);
      }
    }
    FindLargest();
    direction_ = kReverse;
  }

  virtual void Seek(const Slice& target) {
    ClearHeaps();
    for (auto& child : children_) {
      child.Seek(target);
      if (child.Valid()) {
        minHeap_.push(&child);
      }
    }
    FindSmallest();
    direction_ = kForward;
  }

  virtual void Next() {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      ClearHeaps();
      for (auto& child : children_) {
        if (&child != current_) {
          child.Seek(key());
          if (child.Valid() &&
              comparator_->Compare(key(), child.key()) == 0) {
            child.Next();
          }
          if (child.Valid()) {
            minHeap_.push(&child);
          }
        }
      }
      direction_ = kForward;
    }

    // as the current points to the current record. move the iterator forward.
    // and if it is valid add it to the heap.
    current_->Next();
    if (current_->Valid()){
      minHeap_.push(current_);
    }
    FindSmallest();
  }

  virtual void Prev() {
    assert(Valid());
    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      ClearHeaps();
      for (auto& child : children_) {
        if (&child != current_) {
          child.Seek(key());
          if (child.Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child.Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child.SeekToLast();
          }
          if (child.Valid()) {
            maxHeap_.push(&child);
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    if (current_->Valid()) {
      maxHeap_.push(current_);
    }
    FindLargest();
  }

  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const {
    assert(Valid());
    return current_->value();
  }

  virtual Status status() const {
    Status status;
    for (auto& child : children_) {
      status = child.status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  void FindSmallest();
  void FindLargest();
  void ClearHeaps();

  const Comparator* comparator_;
  std::vector<IteratorWrapper> children_;
  IteratorWrapper* current_;
  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
  MaxIterHeap maxHeap_;
  MinIterHeap minHeap_;
};

void MergingIterator::FindSmallest() {
  if (minHeap_.empty()) {
    current_ = nullptr;
  } else {
    current_ = minHeap_.top();
    assert(current_->Valid());
    minHeap_.pop();
  }
}

void MergingIterator::FindLargest() {
  if (maxHeap_.empty()) {
    current_ = nullptr;
  } else {
    current_ = maxHeap_.top();
    assert(current_->Valid());
    maxHeap_.pop();
  }
}

void MergingIterator::ClearHeaps() {
  maxHeap_ = NewMaxIterHeap(comparator_);
  minHeap_ = NewMinIterHeap(comparator_);
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, n);
  }
}

}  // namespace rocksdb
