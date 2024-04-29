//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "db/multi_cf_iterator_impl.h"

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL
class CoalescingIterator : public Iterator {
 public:
  CoalescingIterator(const Comparator* comparator,
                     const std::vector<ColumnFamilyHandle*>& column_families,
                     const std::vector<Iterator*>& child_iterators)
      : impl_(
            comparator, column_families, child_iterators, [this]() { Reset(); },
            [this](const autovector<MultiCfIteratorInfo>& items) {
              Coalesce(items);
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

  struct WideColumnWithOrder {
    const WideColumn* column;
    int order;
  };

  class WideColumnWithOrderComparator {
   public:
    explicit WideColumnWithOrderComparator() {}
    bool operator()(const WideColumnWithOrder& a,
                    const WideColumnWithOrder& b) const {
      int c = a.column->name().compare(b.column->name());
      return c == 0 ? a.order - b.order > 0 : c > 0;
    }
  };

  using MinHeap =
      BinaryHeap<WideColumnWithOrder, WideColumnWithOrderComparator>;

  void Coalesce(const autovector<MultiCfIteratorInfo>& items);
};

}  // namespace ROCKSDB_NAMESPACE
