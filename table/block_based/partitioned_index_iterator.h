//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include "table/block_based/block_based_table_reader.h"

#include "table/block_based/block_based_table_reader_impl.h"
#include "table/block_based/block_prefetcher.h"
#include "table/block_based/reader_common.h"

namespace ROCKSDB_NAMESPACE {
// Iterator that iterates over partitioned index.
// Some upper and lower bound tricks played in block based table iterators
// could be played here, but it's too complicated to reason about index
// keys with upper or lower bound, so we skip it for simplicity.
class ParititionedIndexIterator : public InternalIteratorBase<IndexValue> {
  // compaction_readahead_size: its value will only be used if for_compaction =
  // true
 public:
  ParititionedIndexIterator(
      const BlockBasedTable* table, const ReadOptions& read_options,
      const InternalKeyComparator& icomp,
      std::unique_ptr<InternalIteratorBase<IndexValue>>&& index_iter,
      TableReaderCaller caller, size_t compaction_readahead_size = 0)
      : table_(table),
        read_options_(read_options),
#ifndef NDEBUG
        icomp_(icomp),
#endif
        user_comparator_(icomp.user_comparator()),
        index_iter_(std::move(index_iter)),
        block_iter_points_to_real_block_(false),
        lookup_context_(caller),
        block_prefetcher_(compaction_readahead_size) {}

  ~ParititionedIndexIterator() {}

  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice&) override {
    // Shouldn't be called.
    assert(false);
  }
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() final override;
  bool NextAndGetResult(IterateResult*) override {
    assert(false);
    return false;
  }
  void Prev() override;
  bool Valid() const override {
    return block_iter_points_to_real_block_ && block_iter_.Valid();
  }
  Slice key() const override {
    assert(Valid());
    return block_iter_.key();
  }
  Slice user_key() const override {
    assert(Valid());
    return block_iter_.user_key();
  }
  IndexValue value() const override {
    assert(Valid());
    return block_iter_.value();
  }
  Status status() const override {
    // Prefix index set status to NotFound when the prefix does not exist
    if (!index_iter_->status().ok() && !index_iter_->status().IsNotFound()) {
      return index_iter_->status();
    } else if (block_iter_points_to_real_block_) {
      return block_iter_.status();
    } else {
      return Status::OK();
    }
  }
  inline IterBoundCheck UpperBoundCheckResult() override {
    // Shouldn't be called.
    assert(false);
    return IterBoundCheck::kUnknown;
  }
  void SetPinnedItersMgr(PinnedIteratorsManager*) override {
    // Shouldn't be called.
    assert(false);
  }
  bool IsKeyPinned() const override {
    // Shouldn't be called.
    assert(false);
    return false;
  }
  bool IsValuePinned() const override {
    // Shouldn't be called.
    assert(false);
    return false;
  }

  void ResetPartitionedIndexIter() {
    if (block_iter_points_to_real_block_) {
      block_iter_.Invalidate(Status::OK());
      block_iter_points_to_real_block_ = false;
    }
  }

  void SavePrevIndexValue() {
    if (block_iter_points_to_real_block_) {
      // Reseek. If they end up with the same data block, we shouldn't re-fetch
      // the same data block.
      prev_block_offset_ = index_iter_->value().handle.offset();
    }
  }

 private:
  friend class BlockBasedTableReaderTestVerifyChecksum_ChecksumMismatch_Test;
  const BlockBasedTable* table_;
  const ReadOptions read_options_;
#ifndef NDEBUG
  const InternalKeyComparator& icomp_;
#endif
  UserComparatorWrapper user_comparator_;
  std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter_;
  IndexBlockIter block_iter_;

  // True if block_iter_ is initialized and points to the same block
  // as index iterator.
  bool block_iter_points_to_real_block_;
  uint64_t prev_block_offset_ = std::numeric_limits<uint64_t>::max();
  BlockCacheLookupContext lookup_context_;
  BlockPrefetcher block_prefetcher_;

  // If `target` is null, seek to first.
  void SeekImpl(const Slice* target);

  void InitPartitionedIndexBlock();
  void FindKeyForward();
  void FindBlockForward();
  void FindKeyBackward();
};
}  // namespace ROCKSDB_NAMESPACE
