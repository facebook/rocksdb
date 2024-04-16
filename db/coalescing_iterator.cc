//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/coalescing_iterator.h"

#include "db/wide/wide_columns_helper.h"

namespace ROCKSDB_NAMESPACE {

void CoalescingIterator::Coalesce(
    const autovector<MultiCfIteratorInfo>& items) {
  assert(wide_columns_.empty());
  MinHeap heap;
  for (const auto& item : items) {
    assert(item.iterator);
    for (auto& column : item.iterator->columns()) {
      heap.push(WideColumnWithOrder{&column, item.order});
    }
  }
  if (heap.empty()) {
    return;
  }
  wide_columns_.reserve(heap.size());
  auto current = heap.top();
  heap.pop();
  while (!heap.empty()) {
    int comparison = current.column->name().compare(heap.top().column->name());
    if (comparison < 0) {
      wide_columns_.push_back(*current.column);
    } else if (comparison > 0) {
      // Shouldn't reach here.
      // Current item in the heap is greater than the top item in the min heap
      assert(false);
    }
    current = heap.top();
    heap.pop();
  }
  wide_columns_.push_back(*current.column);

  if (WideColumnsHelper::HasDefaultColumn(wide_columns_)) {
    value_ = WideColumnsHelper::GetDefaultColumn(wide_columns_);
  }
}

}  // namespace ROCKSDB_NAMESPACE
