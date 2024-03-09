//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/multi_cf_iterator.h"

#include <cassert>

namespace ROCKSDB_NAMESPACE {

void MultiCfIterator::SeekToFirst() {
  Reset();
  int i = 0;
  for (auto& cfh_iter_pair : cfh_iter_pairs_) {
    auto& cfh = cfh_iter_pair.first;
    auto& iter = cfh_iter_pair.second;
    iter->SeekToFirst();
    if (iter->Valid()) {
      assert(iter->status().ok());
      min_heap_.push(MultiCfIteratorInfo{iter.get(), cfh, i});
    } else {
      considerStatus(iter->status());
    }
    ++i;
  }
}

void MultiCfIterator::Next() {
  assert(Valid());
  // 1. Keep the top iterator (by popping it from the heap)
  // 2. Make sure all others have iterated past the top iterator key slice
  // 3. Advance the top iterator, and add it back to the heap if valid
  auto top = min_heap_.top();
  min_heap_.pop();
  if (!min_heap_.empty()) {
    auto* current = min_heap_.top().iterator;
    while (current->Valid() &&
           comparator_->Compare(top.iterator->key(), current->key()) == 0) {
      assert(current->status().ok());
      current->Next();
      if (current->Valid()) {
        min_heap_.replace_top(min_heap_.top());
      } else {
        considerStatus(current->status());
        min_heap_.pop();
      }
      if (!min_heap_.empty()) {
        current = min_heap_.top().iterator;
      }
    }
  }
  top.iterator->Next();
  if (top.iterator->Valid()) {
    assert(top.iterator->status().ok());
    min_heap_.push(top);
  } else {
    considerStatus(top.iterator->status());
  }
}

}  // namespace ROCKSDB_NAMESPACE
