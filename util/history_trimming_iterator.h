// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
#pragma once

#include <algorithm>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

class HistoryTrimmingIterator : public InternalIterator {
 public:
  HistoryTrimmingIterator(InternalIterator* input, const Comparator* cmp,
                          const Slice& ts)
      : input_(input), filter_ts_(ts), cmp_(cmp) {}

  bool filter() {
    if (!input_->Valid()) {
      return true;
    }
    if (cmp_->timestamp_size() == 0) {
      return true;
    }
    Slice current_ts = ExtractTimestampFromKey(key(), cmp_->timestamp_size());
    return cmp_->CompareTimestamp(current_ts, filter_ts_) < 0;
  }

  virtual bool Valid() const override { return input_->Valid(); }

  virtual void SeekToFirst() override {
    input_->SeekToFirst();
    while (!filter()) {
      input_->Next();
    }
  }

  virtual void SeekToLast() override {
    input_->SeekToLast();
    while (!filter()) {
      input_->Prev();
    }
  }

  virtual void Seek(const Slice& target) override {
    input_->Seek(target);
    if (!filter()) {
      input_->Next();
    }
  }

  virtual void SeekForPrev(const Slice& target) override {
    input_->SeekForPrev(target);
    if (!filter()) {
      input_->Prev();
    }
  }

  virtual void Next() override {
    do {
      input_->Next();
    } while (!filter());
  }

  virtual void Prev() override {
    do {
      input_->Prev();
    } while (!filter());
  }

  virtual Slice key() const override { return input_->key(); }

  virtual Slice value() const override { return input_->value(); }

  virtual Status status() const override { return input_->status(); }

  virtual bool IsKeyPinned() const override { return input_->IsKeyPinned(); }

  virtual bool IsValuePinned() const override {
    return input_->IsValuePinned();
  }

 private:
  std::unique_ptr<InternalIterator> input_;
  Slice filter_ts_;
  const Comparator* cmp_;
};

}  // namespace ROCKSDB_NAMESPACE
