//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/bounded_iterator.h"

#include "rocksdb/comparator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
Iterator* BoundedIterator::Create(Iterator* base, const Comparator* comp,
                                  const ReadOptions& ro) {
  if (ro.iterate_lower_bound != nullptr || ro.iterate_upper_bound != nullptr) {
    return BoundedIterator::Create(base, comp, ro.iterate_lower_bound,
                                   ro.iterate_upper_bound);
  } else {
    return base;
  }
}

Iterator* BoundedIterator::Create(Iterator* base, const Comparator* comp,
                                  const ReadOptions* ro) {
  if (ro != nullptr) {
    return Create(base, comp, *ro);
  } else {
    return base;
  }
}

Iterator* BoundedIterator::Create(Iterator* base, const Comparator* comp,
                                  const Slice* lower, const Slice* upper) {
  bool use_lower = (lower != nullptr);
  if (use_lower) {
    const Slice* lower_base = base->lower_bound();
    if (lower_base != nullptr) {
      if (comp != nullptr) {
        use_lower = comp->Compare(*lower, *lower_base) > 0;
      } else {
        use_lower = lower->compare(*lower_base) > 0;
      }
    }
  }
  bool use_upper = (upper != nullptr);
  if (use_upper) {
    const Slice* upper_base = base->upper_bound();
    if (upper_base != nullptr) {
      if (comp != nullptr) {
        use_upper = comp->Compare(*upper, *upper_base) < 0;
      } else {
        use_upper = upper->compare(*upper_base) < 0;
      }
    }
  }
  if (use_lower || use_upper) {
    return new BoundedIterator(base, comp, (use_lower) ? lower : nullptr,
                               (use_upper) ? upper : nullptr);
  } else {
    return base;
  }
}

int BoundedIterator::Compare(const Slice& a, const Slice& b) const {
  if (comparator_ != nullptr) {
    return comparator_->Compare(a, b);
  } else {
    return a.compare(b);
  }
}

void BoundedIterator::SeekToFirst() {
  in_bounds_ = true;
  if (lower_bound_ != nullptr) {
    base_->Seek(*lower_bound_);
  } else {
    base_->SeekToFirst();
  }
}

void BoundedIterator::SeekToLast() {
  in_bounds_ = true;
  if (upper_bound_ != nullptr) {
    base_->SeekForPrev(*upper_bound_);
    if (base_->Valid()) {
      base_->Prev();
    } else {
      in_bounds_ = false;
    }
  } else {
    base_->SeekToLast();
  }
}

void BoundedIterator::Seek(const Slice& target) {
  in_bounds_ = true;
  if (upper_bound_ != nullptr && Compare(target, *upper_bound_) >= 0) {
    in_bounds_ = false;
  } else if (lower_bound_ != nullptr && Compare(*lower_bound_, target) > 0) {
    base_->Seek(*lower_bound_);
  } else {
    base_->Seek(target);
  }
}

void BoundedIterator::SeekForPrev(const Slice& target) {
  if (lower_bound_ != nullptr && Compare(target, *lower_bound_) < 0) {
    in_bounds_ = false;
  } else if (upper_bound_ != nullptr && Compare(target, *upper_bound_) >= 0) {
    SeekToLast();
  } else {
    in_bounds_ = true;
    base_->SeekForPrev(target);
  }
}

void BoundedIterator::Next() {
  if (Valid()) {
    base_->Next();
    if (upper_bound_ != nullptr && base_->Valid()) {
      in_bounds_ = Compare(base_->key(), *upper_bound_) < 0;
    }
  }
}

void BoundedIterator::Prev() {
  if (Valid()) {
    base_->Prev();
    if (lower_bound_ != nullptr && base_->Valid()) {
      in_bounds_ = Compare(base_->key(), *lower_bound_) >= 0;
    }
  }
}
}  // namespace ROCKSDB_NAMESPACE
