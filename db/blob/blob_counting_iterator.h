//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>

#include "db/blob/blob_garbage_meter.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

class BlobCountingIterator : public InternalIterator {
 public:
  BlobCountingIterator(InternalIterator* iter,
                       BlobGarbageMeter* blob_garbage_meter)
      : iter_(iter), blob_garbage_meter_(blob_garbage_meter) {
    assert(iter_);
    assert(blob_garbage_meter_);
  }

  bool Valid() const override {
    assert(iter_);
    return iter_->Valid();
  }

  void SeekToFirst() override {
    assert(iter_);
    iter_->SeekToFirst();
    CountBlobIfNeeded();
  }

  void SeekToLast() override {
    assert(iter_);
    iter_->SeekToLast();
    CountBlobIfNeeded();
  }

  void Seek(const Slice& target) override {
    assert(iter_);
    iter_->Seek(target);
    CountBlobIfNeeded();
  }

  void SeekForPrev(const Slice& target) override {
    assert(iter_);
    iter_->SeekForPrev(target);
    CountBlobIfNeeded();
  }

  void Next() override {
    assert(iter_);
    iter_->Next();
    CountBlobIfNeeded();
  }

  bool NextAndGetResult(IterateResult* result) override {
    assert(iter_);
    const bool res = iter_->NextAndGetResult(result);
    CountBlobIfNeeded();
    return res;
  }

  void Prev() override {
    assert(iter_);
    iter_->Prev();
    CountBlobIfNeeded();
  }

  Slice key() const override {
    assert(iter_);
    return iter_->key();
  }

  Slice user_key() const override {
    assert(iter_);
    return iter_->user_key();
  }

  Slice value() const override {
    assert(iter_);
    return iter_->value();
  }

  Status status() const override {
    assert(iter_);
    return iter_->status();
  }

  bool PrepareValue() override {
    assert(iter_);
    return iter_->PrepareValue();
  }

  bool MayBeOutOfLowerBound() override {
    assert(iter_);
    return iter_->MayBeOutOfLowerBound();
  }

  IterBoundCheck UpperBoundCheckResult() override {
    assert(iter_);
    return iter_->UpperBoundCheckResult();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    assert(iter_);
    iter_->SetPinnedItersMgr(pinned_iters_mgr);
  }

  bool IsKeyPinned() const override {
    assert(iter_);
    return iter_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    assert(iter_);
    return iter_->IsValuePinned();
  }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    assert(iter_);
    return iter_->GetProperty(prop_name, prop);
  }

 private:
  void CountBlobIfNeeded() {
    if (!Valid()) {
      return;
    }

    assert(blob_garbage_meter_);
    blob_garbage_meter_->ProcessInFlow(key(), value());
  }

  InternalIterator* iter_;
  BlobGarbageMeter* blob_garbage_meter_;
};

}  // namespace ROCKSDB_NAMESPACE
