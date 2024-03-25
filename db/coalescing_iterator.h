//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <variant>

#include "multi_cf_iterator_impl.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "util/heap.h"
#include "util/overload.h"
#include "db/multi_cf_iterator_impl.h"

namespace ROCKSDB_NAMESPACE {

// UNDER CONSTRUCTION - DO NOT USE
// A cross-column-family iterator from a consistent database state.
// When the same key exists in more than one column families, the iterator
// selects the value from the first column family containing the key, in the
// order provided in the `column_families` parameter.
class CoalescingIterator : public Iterator {
 public:
  CoalescingIterator(const Comparator* comparator,
                  const std::vector<ColumnFamilyHandle*>& column_families,
                  const std::vector<Iterator*>& child_iterators)
      : impl_(comparator, column_families, child_iterators) {}
  ~CoalescingIterator() override {}

  // No copy allowed
  CoalescingIterator(const CoalescingIterator&) = delete;
  CoalescingIterator& operator=(const CoalescingIterator&) = delete;

  bool Valid() const override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void Next() override;
  void Prev() override;
  Slice key() const override;
  Status status() const override;

  Slice value() const override {
    // TODO - Implement
    assert(false);
    return Slice();
  }
  const WideColumns& columns() const override {
    // TODO - Implement
    assert(false);
    return kNoWideColumns;
  }

 private:
  MultiCfIteratorImpl impl_;
};

}  // namespace ROCKSDB_NAMESPACE
