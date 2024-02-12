//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/multi_cf_iterator_impl.h"

#include <cassert>

namespace ROCKSDB_NAMESPACE {

void MultiCfIteratorImpl::SeekToFirst() {
  Reset();
  assert(cfhs_.size() == iterators_.size());
  int i = 0;
  for (auto& iter : iterators_) {
    auto& cfh = cfhs_[i];
    iter->SeekToFirst();
    if (iter->Valid()) {
      assert(iter->status().ok());
      min_heap_.push(MultiCfIteratorInfo{iter, cfh, i});
    } else {
      considerStatus(iter->status());
    }
    ++i;
  }
}

void MultiCfIteratorImpl::Next() {
  assert(Valid());

  auto* current = min_heap_.top().iterator;
  std::string current_key_copy =
      std::string(current->key().data(), current->key().size());
  while (!min_heap_.empty() &&
         comparator_->Compare(current->key(), current_key_copy) == 0) {
    current->Next();
    if (current->Valid()) {
      assert(current->status().ok());
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

const AttributeGroups kNoAttributeGroups;

}  // namespace ROCKSDB_NAMESPACE
