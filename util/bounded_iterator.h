// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.

#pragma once

#include <string>

#include "rocksdb/iterator.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
class Comparator;
class Slice;
struct ReadOptions;

// A BoundedIterator is an Iterator which guarantees that the values returned
// are within the bounds. If a lower bound is set, then no key less than the
// lower bound will be returned by any of the Seek or traversal operations. If
// an upper bound is set, then no key greater than or equal to the upper bound
// will be returned. Seek or traversal operations.
class BoundedIterator : public Iterator {
 private:
  Iterator* base_;
  const Comparator* comparator_;
  const Slice* lower_bound_;
  const Slice* upper_bound_;
  bool in_bounds_;

  // Compares the two slices and returns the result.
  // If this iterator has a comparator, it is used.
  // Otherwise Slice comparison is invoked.
  int Compare(const Slice& a, const Slice& b) const;

  BoundedIterator(Iterator* base, const Comparator* comp, const Slice* lower,
                  const Slice* upper)
      : base_(base),
        comparator_(comp),
        lower_bound_(lower),
        upper_bound_(upper),
        in_bounds_(false) {}

 public:
  // Returns an Iterator that respects the iterator bounds checks in the input
  // ReadOptions
  static Iterator* Create(Iterator* base, const Comparator* comp,
                          const ReadOptions* ro);
  static Iterator* Create(Iterator* base, const Comparator* comp,
                          const ReadOptions& ro);

  // Returns an Iterator that respects the lower and upper bounds.
  // Takes ownership of the input base Iterator.
  // If a Comparator is specified, the comparator is used to compare the bounds.
  // Otherwise slice comparison is used.
  //
  // The returned Iterator will only return keys between lower and upper bounds.
  // If the input iterator already has a bound, the more restrictive of the
  // input bounds or the iterator bounds are used.
  static Iterator* Create(Iterator* base, const Comparator* comp,
                          const Slice* lower, const Slice* upper);

  ~BoundedIterator() { delete base_; }

  bool Valid() const override { return in_bounds_ && base_->Valid(); }

  void SeekToFirst() override;

  void SeekToLast() override;

  void Seek(const Slice& target) override;

  void SeekForPrev(const Slice& target) override;

  void Next() override;

  void Prev() override;

  Slice key() const override { return base_->key(); }

  Slice value() const override { return base_->value(); }

  Status status() const override { return base_->status(); }

  Status Refresh() override { return base_->Refresh(); }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    return base_->GetProperty(prop_name, prop);
  }

  Slice timestamp() const override { return base_->timestamp(); }

  const Slice* lower_bound() const override {
    if (lower_bound_ != nullptr) {
      return lower_bound_;
    } else {
      return base_->lower_bound();
    }
  }

  const Slice* upper_bound() const override {
    if (upper_bound_ != nullptr) {
      return upper_bound_;
    } else {
      return base_->upper_bound();
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
