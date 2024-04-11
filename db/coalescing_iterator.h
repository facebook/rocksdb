//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "db/multi_cf_iterator_impl.h"

namespace ROCKSDB_NAMESPACE {

// UNDER CONSTRUCTION - DO NOT USE
class CoalescingIterator : public Iterator {
 public:
  CoalescingIterator(const Comparator* comparator,
                     const std::vector<ColumnFamilyHandle*>& column_families,
                     const std::vector<Iterator*>& child_iterators)
      : impl_(
            comparator, column_families, child_iterators, [this]() { Reset(); },
            [this](ColumnFamilyHandle*, Iterator* iter) {
              Coalesce(iter->columns());
            }) {}
  ~CoalescingIterator() override {}

  // No copy allowed
  CoalescingIterator(const CoalescingIterator&) = delete;
  CoalescingIterator& operator=(const CoalescingIterator&) = delete;

  bool Valid() const override { return impl_.Valid(); }
  void SeekToFirst() override { impl_.SeekToFirst(); }
  void SeekToLast() override { impl_.SeekToLast(); }
  void Seek(const Slice& target) override { impl_.Seek(target); }
  void SeekForPrev(const Slice& target) override { impl_.SeekForPrev(target); }
  void Next() override { impl_.Next(); }
  void Prev() override { impl_.Prev(); }
  Slice key() const override { return impl_.key(); }
  Status status() const override { return impl_.status(); }

  Slice value() const override {
    assert(Valid());
    return value_;
  }
  const WideColumns& columns() const override {
    assert(Valid());
    return wide_columns_;
  }

  void Reset() {
    value_.clear();
    wide_columns_.clear();
  }

 private:
  MultiCfIteratorImpl impl_;
  Slice value_;
  WideColumns wide_columns_;

  void Coalesce(const WideColumns& columns);
};

}  // namespace ROCKSDB_NAMESPACE
