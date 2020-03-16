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
// Iterates over the contents of BlockBasedTable.
class BlockBasedTableIterator : public InternalIteratorBase<Slice> {
  // compaction_readahead_size: its value will only be used if for_compaction =
  // true
 public:
  BlockBasedTableIterator(
      const BlockBasedTable* table, const ReadOptions& read_options,
      const InternalKeyComparator& icomp,
      std::unique_ptr<InternalIteratorBase<IndexValue>>&& index_iter,
      bool check_filter, bool need_upper_bound_check,
      const SliceTransform* prefix_extractor, TableReaderCaller caller,
      size_t compaction_readahead_size = 0)
      : table_(table),
        read_options_(read_options),
        icomp_(icomp),
        user_comparator_(icomp.user_comparator()),
        index_iter_(std::move(index_iter)),
        pinned_iters_mgr_(nullptr),
        block_iter_points_to_real_block_(false),
        check_filter_(check_filter),
        need_upper_bound_check_(need_upper_bound_check),
        prefix_extractor_(prefix_extractor),
        lookup_context_(caller),
        block_prefetcher_(compaction_readahead_size) {}

  ~BlockBasedTableIterator() {}

  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() final override;
  bool NextAndGetResult(IterateResult* result) override;
  void Prev() override;
  bool Valid() const override {
    return !is_out_of_bound_ &&
           (is_at_first_key_from_index_ ||
            (block_iter_points_to_real_block_ && block_iter_.Valid()));
  }
  Slice key() const override {
    assert(Valid());
    if (is_at_first_key_from_index_) {
      return index_iter_->value().first_internal_key;
    } else {
      return block_iter_.key();
    }
  }
  Slice user_key() const override {
    assert(Valid());
    if (is_at_first_key_from_index_) {
      return ExtractUserKey(index_iter_->value().first_internal_key);
    } else {
      return block_iter_.user_key();
    }
  }
  Slice value() const override {
    assert(Valid());

    // Load current block if not loaded.
    if (is_at_first_key_from_index_ &&
        !const_cast<BlockBasedTableIterator*>(this)
             ->MaterializeCurrentBlock()) {
      // Oops, index is not consistent with block contents, but we have
      // no good way to report error at this point. Let's return empty value.
      return Slice();
    }

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

  // Whether iterator invalidated for being out of bound.
  bool IsOutOfBound() override { return is_out_of_bound_; }

  inline bool MayBeOutOfUpperBound() override {
    assert(Valid());
    return !data_block_within_upper_bound_;
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  bool IsKeyPinned() const override {
    // Our key comes either from block_iter_'s current key
    // or index_iter_'s current *value*.
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           ((is_at_first_key_from_index_ && index_iter_->IsValuePinned()) ||
            (block_iter_points_to_real_block_ && block_iter_.IsKeyPinned()));
  }
  bool IsValuePinned() const override {
    // Load current block if not loaded.
    if (is_at_first_key_from_index_) {
      const_cast<BlockBasedTableIterator*>(this)->MaterializeCurrentBlock();
    }
    // BlockIter::IsValuePinned() is always true. No need to check
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           block_iter_points_to_real_block_;
  }

  void ResetDataIter() {
    if (block_iter_points_to_real_block_) {
      if (pinned_iters_mgr_ != nullptr && pinned_iters_mgr_->PinningEnabled()) {
        block_iter_.DelegateCleanupsTo(pinned_iters_mgr_);
      }
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
  enum class IterDirection {
    kForward,
    kBackward,
  };

  const BlockBasedTable* table_;
  const ReadOptions read_options_;
  const InternalKeyComparator& icomp_;
  UserComparatorWrapper user_comparator_;
  std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  DataBlockIter block_iter_;

  // True if block_iter_ is initialized and points to the same block
  // as index iterator.
  bool block_iter_points_to_real_block_;
  // See InternalIteratorBase::IsOutOfBound().
  bool is_out_of_bound_ = false;
  // Whether current data block being fully within iterate upper bound.
  bool data_block_within_upper_bound_ = false;
  // True if we're standing at the first key of a block, and we haven't loaded
  // that block yet. A call to value() will trigger loading the block.
  bool is_at_first_key_from_index_ = false;
  bool check_filter_;
  // TODO(Zhongyi): pick a better name
  bool need_upper_bound_check_;
  const SliceTransform* prefix_extractor_;
  uint64_t prev_block_offset_ = std::numeric_limits<uint64_t>::max();
  BlockCacheLookupContext lookup_context_;

  BlockPrefetcher block_prefetcher_;

  // If `target` is null, seek to first.
  void SeekImpl(const Slice* target);

  void InitDataBlock();
  bool MaterializeCurrentBlock();
  void FindKeyForward();
  void FindBlockForward();
  void FindKeyBackward();
  void CheckOutOfBound();

  // Check if data block is fully within iterate_upper_bound.
  //
  // Note MyRocks may update iterate bounds between seek. To workaround it,
  // we need to check and update data_block_within_upper_bound_ accordingly.
  void CheckDataBlockWithinUpperBound();

  bool CheckPrefixMayMatch(const Slice& ikey, IterDirection direction) {
    if (need_upper_bound_check_ && direction == IterDirection::kBackward) {
      // Upper bound check isn't sufficnet for backward direction to
      // guarantee the same result as total order, so disable prefix
      // check.
      return true;
    }
    if (check_filter_ &&
        !table_->PrefixMayMatch(ikey, read_options_, prefix_extractor_,
                                need_upper_bound_check_, &lookup_context_)) {
      // TODO remember the iterator is invalidated because of prefix
      // match. This can avoid the upper level file iterator to falsely
      // believe the position is the end of the SST file and move to
      // the first key of the next file.
      ResetDataIter();
      return false;
    }
    return true;
  }
};
}  // namespace ROCKSDB_NAMESPACE
