//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>

#include "rocksdb/comparator.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

// An internal iterator that wraps another one and ensures that any keys
// returned are strictly within a range [start, end). If the underlying
// iterator has already performed the bounds checking, it relies on that result;
// otherwise, it performs the necessary key comparisons itself. Both bounds
// are optional.
class ClippingIterator : public InternalIterator {
 public:
  ClippingIterator(InternalIterator* iter, const Slice* start, const Slice* end,
                   const CompareInterface* cmp)
      : iter_(iter), start_(start), end_(end), cmp_(cmp), valid_(false) {
    assert(iter_);
    assert(cmp_);
    assert(!start_ || !end_ || cmp_->Compare(*start_, *end_) <= 0);

    UpdateAndEnforceBounds();
  }

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    if (start_) {
      iter_->Seek(*start_);
    } else {
      iter_->SeekToFirst();
    }

    UpdateAndEnforceUpperBound();
  }

  void SeekToLast() override {
    if (end_) {
      iter_->SeekForPrev(*end_);

      // Upper bound is exclusive, so we need a key which is strictly smaller
      if (iter_->Valid() && cmp_->Compare(iter_->key(), *end_) == 0) {
        iter_->Prev();
      }
    } else {
      iter_->SeekToLast();
    }

    UpdateAndEnforceLowerBound();
  }

  void Seek(const Slice& target) override {
    if (start_ && cmp_->Compare(target, *start_) < 0) {
      iter_->Seek(*start_);
      UpdateAndEnforceUpperBound();
      return;
    }

    if (end_ && cmp_->Compare(target, *end_) >= 0) {
      valid_ = false;
      return;
    }

    iter_->Seek(target);
    UpdateAndEnforceUpperBound();
  }

  void SeekForPrev(const Slice& target) override {
    if (start_ && cmp_->Compare(target, *start_) < 0) {
      valid_ = false;
      return;
    }

    if (end_ && cmp_->Compare(target, *end_) >= 0) {
      iter_->SeekForPrev(*end_);

      // Upper bound is exclusive, so we need a key which is strictly smaller
      if (iter_->Valid() && cmp_->Compare(iter_->key(), *end_) == 0) {
        iter_->Prev();
      }

      UpdateAndEnforceLowerBound();
      return;
    }

    iter_->SeekForPrev(target);
    UpdateAndEnforceLowerBound();
  }

  void Next() override {
    assert(valid_);
    iter_->Next();
    UpdateAndEnforceUpperBound();
  }

  bool NextAndGetResult(IterateResult* result) override {
    assert(valid_);
    assert(result);

    IterateResult res;
    valid_ = iter_->NextAndGetResult(&res);

    if (!valid_) {
      return false;
    }

    if (end_) {
      EnforceUpperBoundImpl(res.bound_check_result);

      if (!valid_) {
        return false;
      }
    }

    res.bound_check_result = IterBoundCheck::kInbound;
    *result = res;

    return true;
  }

  void Prev() override {
    assert(valid_);
    iter_->Prev();
    UpdateAndEnforceLowerBound();
  }

  Slice key() const override {
    assert(valid_);
    return iter_->key();
  }

  Slice user_key() const override {
    assert(valid_);
    return iter_->user_key();
  }

  Slice value() const override {
    assert(valid_);
    return iter_->value();
  }

  Status status() const override { return iter_->status(); }

  bool PrepareValue() override {
    assert(valid_);

    if (iter_->PrepareValue()) {
      return true;
    }

    assert(!iter_->Valid());
    valid_ = false;
    return false;
  }

  bool MayBeOutOfLowerBound() override {
    assert(valid_);
    return false;
  }

  IterBoundCheck UpperBoundCheckResult() override {
    assert(valid_);
    return IterBoundCheck::kInbound;
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    iter_->SetPinnedItersMgr(pinned_iters_mgr);
  }

  bool IsKeyPinned() const override {
    assert(valid_);
    return iter_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    assert(valid_);
    return iter_->IsValuePinned();
  }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    return iter_->GetProperty(prop_name, prop);
  }

 private:
  void UpdateValid() {
    assert(!iter_->Valid() || iter_->status().ok());

    valid_ = iter_->Valid();
  }

  void EnforceUpperBoundImpl(IterBoundCheck bound_check_result) {
    if (bound_check_result == IterBoundCheck::kInbound) {
      return;
    }

    if (bound_check_result == IterBoundCheck::kOutOfBound) {
      valid_ = false;
      return;
    }

    assert(bound_check_result == IterBoundCheck::kUnknown);

    if (cmp_->Compare(key(), *end_) >= 0) {
      valid_ = false;
    }
  }

  void EnforceUpperBound() {
    if (!valid_) {
      return;
    }

    if (!end_) {
      return;
    }

    EnforceUpperBoundImpl(iter_->UpperBoundCheckResult());
  }

  void EnforceLowerBound() {
    if (!valid_) {
      return;
    }

    if (!start_) {
      return;
    }

    if (!iter_->MayBeOutOfLowerBound()) {
      return;
    }

    if (cmp_->Compare(key(), *start_) < 0) {
      valid_ = false;
    }
  }

  void AssertBounds() {
    assert(!valid_ || !start_ || cmp_->Compare(key(), *start_) >= 0);
    assert(!valid_ || !end_ || cmp_->Compare(key(), *end_) < 0);
  }

  void UpdateAndEnforceBounds() {
    UpdateValid();
    EnforceUpperBound();
    EnforceLowerBound();
    AssertBounds();
  }

  void UpdateAndEnforceUpperBound() {
    UpdateValid();
    EnforceUpperBound();
    AssertBounds();
  }

  void UpdateAndEnforceLowerBound() {
    UpdateValid();
    EnforceLowerBound();
    AssertBounds();
  }

  InternalIterator* iter_;
  const Slice* start_;
  const Slice* end_;
  const CompareInterface* cmp_;
  bool valid_;
};

}  // namespace ROCKSDB_NAMESPACE
