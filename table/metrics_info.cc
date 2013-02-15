// Copyright (c) 2013 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/metrics_info.h"

#include "leveldb/status.h"
#include "table/block.h"

namespace leveldb {

bool IsRecordHot(const Iterator* iter, DB* metrics_db,
                 const ReadOptions& metrics_read_opts,
                 BlockMetrics** block_metrics_store) {
  assert(iter != NULL);
  assert(metrics_db != NULL);
  assert(block_metrics_store != NULL);

  static const bool kDefaultHotness = false;

  bool has_block_info = false;
  uint64_t file_number;
  uint64_t block_offset;
  uint32_t restart_index;
  uint32_t restart_offset;

  do {
    if (Block::GetBlockIterInfo(iter, file_number, block_offset, restart_index,
                                restart_offset)) {
      has_block_info = true;
      continue;
    }
  } while ((iter = iter->FindSubIterator()) != NULL);

  if (!has_block_info) {
    return kDefaultHotness;
  }

  if ((*block_metrics_store) == NULL ||
      !(*block_metrics_store)->IsSameBlock(file_number, block_offset)) {
    // Stored block metrics is invalid so we have to load a new one.

    // Free previous block metrics if any
    if ((*block_metrics_store) != NULL) {
      delete *block_metrics_store;
      *block_metrics_store = NULL;
    }

    std::string db_key;
    BlockMetrics::CreateDBKey(file_number, block_offset, &db_key);

    std::string db_value;
    Status s = metrics_db->Get(metrics_read_opts, db_key, &db_value);
    if (!s.ok()) {
      return kDefaultHotness;
    }

    *block_metrics_store = BlockMetrics::Create(file_number, block_offset,
                                                db_value);
  }

  if ((*block_metrics_store) != NULL) {
    return (*block_metrics_store)->IsHot(restart_index, restart_offset);
  }

  return kDefaultHotness;
}

}  // namespace leveldb
