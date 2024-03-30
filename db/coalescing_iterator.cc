//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/coalescing_iterator.h"

#include "db/wide/wide_columns_helper.h"

namespace ROCKSDB_NAMESPACE {

void CoalescingIterator::Coalesce(const WideColumns& columns) {
  WideColumns coalesced;
  coalesced.reserve(wide_columns_.size() + columns.size());
  auto base_cols = wide_columns_.begin();
  auto new_cols = columns.begin();
  while (base_cols != wide_columns_.end() && new_cols != columns.end()) {
    auto comparison = base_cols->name().compare(new_cols->name());
    if (comparison < 0) {
      coalesced.push_back(*base_cols);
      ++base_cols;
    } else if (comparison > 0) {
      coalesced.push_back(*new_cols);
      ++new_cols;
    } else {
      coalesced.push_back(*new_cols);
      ++new_cols;
      ++base_cols;
    }
  }
  while (base_cols != wide_columns_.end()) {
    coalesced.push_back(*base_cols);
    ++base_cols;
  }
  while (new_cols != columns.end()) {
    coalesced.push_back(*new_cols);
    ++new_cols;
  }
  wide_columns_.swap(coalesced);

  if (WideColumnsHelper::HasDefaultColumn(wide_columns_)) {
    value_ = WideColumnsHelper::GetDefaultColumn(wide_columns_);
  }
}

}  // namespace ROCKSDB_NAMESPACE
