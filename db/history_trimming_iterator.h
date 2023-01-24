// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

class HistoryTrimmingIterator : public InternalIterator {
 public:
  explicit HistoryTrimmingIterator(InternalIterator* input,
                                   const Comparator* cmp, const std::string& ts)
      : input_(input), filter_ts_(ts), cmp_(cmp) {
    assert(cmp_->timestamp_size() > 0 && !ts.empty());
  }

  bool filter() const {
    if (!input_->Valid()) {
      return true;
    }
    Slice current_ts = ExtractTimestampFromKey(key(), cmp_->timestamp_size());
    return cmp_->CompareTimestamp(current_ts, Slice(filter_ts_)) <= 0;
  }

  bool Valid() const override { return input_->Valid(); }

  void SeekToFirst() override {
    input_->SeekToFirst();
    while (!filter()) {
      input_->Next();
    }
  }

  void SeekToLast() override {
    input_->SeekToLast();
    while (!filter()) {
      input_->Prev();
    }
  }

  void Seek(const Slice& target) override {
    input_->Seek(target);
    while (!filter()) {
      input_->Next();
    }
  }

  void SeekForPrev(const Slice& target) override {
    input_->SeekForPrev(target);
    while (!filter()) {
      input_->Prev();
    }
  }

  void Next() override {
    do {
      input_->Next();
    } while (!filter());
  }

  void Prev() override {
    do {
      input_->Prev();
    } while (!filter());
  }

  Slice key() const override { return input_->key(); }

  Slice value() const override { return input_->value(); }

  Status status() const override { return input_->status(); }

  bool IsKeyPinned() const override { return input_->IsKeyPinned(); }

  bool IsValuePinned() const override { return input_->IsValuePinned(); }

 private:
  InternalIterator* input_;
  const std::string filter_ts_;
  const Comparator* const cmp_;
};

}  // namespace ROCKSDB_NAMESPACE
