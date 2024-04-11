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
  auto base_col_iter = wide_columns_.begin();
  auto new_col_iter = columns.begin();
  while (base_col_iter != wide_columns_.end() &&
         new_col_iter != columns.end()) {
    auto comparison = base_col_iter->name().compare(new_col_iter->name());
    if (comparison < 0) {
      coalesced.push_back(*base_col_iter);
      ++base_col_iter;
    } else if (comparison > 0) {
      coalesced.push_back(*new_col_iter);
      ++new_col_iter;
    } else {
      coalesced.push_back(*new_col_iter);
      ++new_col_iter;
      ++base_col_iter;
    }
  }
  while (base_col_iter != wide_columns_.end()) {
    coalesced.push_back(*base_col_iter);
    ++base_col_iter;
  }
  while (new_col_iter != columns.end()) {
    coalesced.push_back(*new_col_iter);
    ++new_col_iter;
  }
  wide_columns_.swap(coalesced);

  if (WideColumnsHelper::HasDefaultColumn(wide_columns_)) {
    value_ = WideColumnsHelper::GetDefaultColumn(wide_columns_);
  }
}

}  // namespace ROCKSDB_NAMESPACE
