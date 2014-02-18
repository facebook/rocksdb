// Copyright (c) 2013, Facebook, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>

#include "table/block_hash_index.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"

namespace rocksdb {

BlockHashIndex* CreateBlockHashIndex(Iterator* index_iter, Iterator* data_iter,
                                     const uint32_t num_restarts,
                                     const Comparator* comparator,
                                     const SliceTransform* hash_key_extractor) {
  assert(hash_key_extractor);
  auto hash_index = new BlockHashIndex(hash_key_extractor);
  uint64_t current_restart_index = 0;

  std::string pending_entry_prefix;
  // pending_block_num == 0 also implies there is no entry inserted at all.
  uint32_t pending_block_num = 0;
  uint32_t pending_entry_index = 0;

  // scan all the entries and create a hash index based on their prefixes.
  data_iter->SeekToFirst();
  for (index_iter->SeekToFirst();
       index_iter->Valid() && current_restart_index < num_restarts;
       index_iter->Next()) {
    Slice last_key_in_block = index_iter->key();
    assert(data_iter->Valid() && data_iter->status().ok());

    // scan through all entries within a data block.
    while (data_iter->Valid() &&
           comparator->Compare(data_iter->key(), last_key_in_block) <= 0) {
      auto key_prefix = hash_key_extractor->Transform(data_iter->key());
      bool is_first_entry = pending_block_num == 0;

      // Keys may share the prefix
      if (is_first_entry || pending_entry_prefix != key_prefix) {
        if (!is_first_entry) {
          bool succeeded = hash_index->Add(
              pending_entry_prefix, pending_entry_index, pending_block_num);
          if (!succeeded) {
            delete hash_index;
            return nullptr;
          }
        }

        // update the status.
        // needs a hard copy otherwise the underlying data changes all the time.
        pending_entry_prefix = key_prefix.ToString();
        pending_block_num = 1;
        pending_entry_index = current_restart_index;
      } else {
        // entry number increments when keys share the prefix reside in
        // differnt data blocks.
        auto last_restart_index = pending_entry_index + pending_block_num - 1;
        assert(last_restart_index <= current_restart_index);
        if (last_restart_index != current_restart_index) {
          ++pending_block_num;
        }
      }
      data_iter->Next();
    }

    ++current_restart_index;
  }

  // make sure all entries has been scaned.
  assert(!index_iter->Valid());
  assert(!data_iter->Valid());

  if (pending_block_num > 0) {
    auto succeeded = hash_index->Add(pending_entry_prefix, pending_entry_index,
                                     pending_block_num);
    if (!succeeded) {
      delete hash_index;
      return nullptr;
    }
  }

  return hash_index;
}

bool BlockHashIndex::Add(const Slice& prefix, uint32_t restart_index,
                         uint32_t num_blocks) {
  auto prefix_ptr = arena_.Allocate(prefix.size());
  std::copy(prefix.data() /* begin */, prefix.data() + prefix.size() /* end */,
            prefix_ptr /* destination */);
  auto result =
      restart_indices_.insert({Slice(prefix_ptr, prefix.size()),
                               RestartIndex(restart_index, num_blocks)});
  return result.second;
}

const BlockHashIndex::RestartIndex* BlockHashIndex::GetRestartIndex(
    const Slice& key) {
  auto key_prefix = hash_key_extractor_->Transform(key);

  auto pos = restart_indices_.find(key_prefix);
  if (pos == restart_indices_.end()) {
    return nullptr;
  }

  return &pos->second;
}

}  // namespace rocksdb
