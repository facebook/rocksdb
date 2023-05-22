//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/write_batch_util.h"

namespace ROCKSDB_NAMESPACE {

Status CollectColumnFamilyIdsFromWriteBatch(
    const WriteBatch& batch, std::vector<uint32_t>* column_family_ids) {
  assert(column_family_ids != nullptr);
  column_family_ids->clear();
  ColumnFamilyCollector handler;
  Status s = batch.Iterate(&handler);
  if (s.ok()) {
    for (const auto& cf : handler.column_families()) {
      column_family_ids->push_back(cf);
    }
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
