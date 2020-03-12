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
#include "table/block_based/reader_common.h"

namespace ROCKSDB_NAMESPACE {
// Iterates over the contents of BlockBasedTable.
template <class TBlockIter, typename TValue = Slice>
class BlockBasedTableIterator : public InternalIteratorBase<TValue> {
  // compaction_readahead_size: its value will only be used if for_compaction =
  // true
 public:
  BlockBasedTableIterator(const BlockBasedTable* table,
                          const ReadOptions& read_options,
                          const InternalKeyComparator& icomp,
                          InternalIteratorBase<IndexValue>* index_iter,
                          bool check_filter, bool need_upper_bound_check,
                          const SliceTransform* prefix_extractor,
                          BlockType block_type, TableReaderCaller caller,
                          size_t compaction_readahead_size = 0)
      : table_(table),
        read_options_(read_options),
        icomp_(icomp),
        user_comparator_(icomp.user_comparator()),
        index_iter_(index_iter),
        pinned_iters_mgr_(nullptr),
        block_iter_points_to_real_block_(false),
        check_filter_(check_filter),
        need_upper_bound_check_(need_upper_bound_check),
        prefix_extractor_(prefix_extractor),
        block_type_(block_type),
        lookup_context_(caller),
        compaction_readahead_size_(compaction_readahead_size) {}

  ~BlockBasedTableIterator() { delete index_iter_; }

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
  TValue value() const override {
    assert(Valid());

    // Load current block if not loaded.
    if (is_at_first_key_from_index_ &&
        !const_cast<BlockBasedTableIterator*>(this)
             ->MaterializeCurrentBlock()) {
      // Oops, index is not consistent with block contents, but we have
      // no good way to report error at this point. Let's return empty value.
      return TValue();
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
  InternalIteratorBase<IndexValue>* index_iter_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  TBlockIter block_iter_;

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
  BlockType block_type_;
  uint64_t prev_block_offset_ = std::numeric_limits<uint64_t>::max();
  BlockCacheLookupContext lookup_context_;
  // Readahead size used in compaction, its value is used only if
  // lookup_context_.caller = kCompaction.
  size_t compaction_readahead_size_;

  size_t readahead_size_ = BlockBasedTable::kInitAutoReadaheadSize;
  size_t readahead_limit_ = 0;
  int64_t num_file_reads_ = 0;
  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer_;

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

// Functions below cannot be moved to .cc file because the class is a template
// The template is in place so that block based table iterator can be served
// partitioned index too. However, the logic is kind of different between the
// two. So we may think of de-template them by having a separate iterator
// for partitioned index.

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::Seek(const Slice& target) {
  SeekImpl(&target);
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::SeekToFirst() {
  SeekImpl(nullptr);
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::SeekImpl(
    const Slice* target) {
  is_out_of_bound_ = false;
  is_at_first_key_from_index_ = false;
  if (target && !CheckPrefixMayMatch(*target, IterDirection::kForward)) {
    ResetDataIter();
    return;
  }

  bool need_seek_index = true;
  if (block_iter_points_to_real_block_ && block_iter_.Valid()) {
    // Reseek.
    prev_block_offset_ = index_iter_->value().handle.offset();

    if (target) {
      // We can avoid an index seek if:
      // 1. The new seek key is larger than the current key
      // 2. The new seek key is within the upper bound of the block
      // Since we don't necessarily know the internal key for either
      // the current key or the upper bound, we check user keys and
      // exclude the equality case. Considering internal keys can
      // improve for the boundary cases, but it would complicate the
      // code.
      if (user_comparator_.Compare(ExtractUserKey(*target),
                                   block_iter_.user_key()) > 0 &&
          user_comparator_.Compare(ExtractUserKey(*target),
                                   index_iter_->user_key()) < 0) {
        need_seek_index = false;
      }
    }
  }

  if (need_seek_index) {
    if (target) {
      index_iter_->Seek(*target);
    } else {
      index_iter_->SeekToFirst();
    }

    if (!index_iter_->Valid()) {
      ResetDataIter();
      return;
    }
  }

  IndexValue v = index_iter_->value();
  const bool same_block = block_iter_points_to_real_block_ &&
                          v.handle.offset() == prev_block_offset_;

  // TODO(kolmike): Remove the != kBlockCacheTier condition.
  if (!v.first_internal_key.empty() && !same_block &&
      (!target || icomp_.Compare(*target, v.first_internal_key) <= 0) &&
      read_options_.read_tier != kBlockCacheTier) {
    // Index contains the first key of the block, and it's >= target.
    // We can defer reading the block.
    is_at_first_key_from_index_ = true;
    // ResetDataIter() will invalidate block_iter_. Thus, there is no need to
    // call CheckDataBlockWithinUpperBound() to check for iterate_upper_bound
    // as that will be done later when the data block is actually read.
    ResetDataIter();
  } else {
    // Need to use the data block.
    if (!same_block) {
      InitDataBlock();
    } else {
      // When the user does a reseek, the iterate_upper_bound might have
      // changed. CheckDataBlockWithinUpperBound() needs to be called
      // explicitly if the reseek ends up in the same data block.
      // If the reseek ends up in a different block, InitDataBlock() will do
      // the iterator upper bound check.
      CheckDataBlockWithinUpperBound();
    }

    if (target) {
      block_iter_.Seek(*target);
    } else {
      block_iter_.SeekToFirst();
    }
    FindKeyForward();
  }

  CheckOutOfBound();

  if (target) {
    assert(!Valid() || ((block_type_ == BlockType::kIndex &&
                         !table_->get_rep()->index_key_includes_seq)
                            ? (user_comparator_.Compare(ExtractUserKey(*target),
                                                        key()) <= 0)
                            : (icomp_.Compare(*target, key()) <= 0)));
  }
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::SeekForPrev(
    const Slice& target) {
  is_out_of_bound_ = false;
  is_at_first_key_from_index_ = false;
  // For now totally disable prefix seek in auto prefix mode because we don't
  // have logic
  if (!CheckPrefixMayMatch(target, IterDirection::kBackward)) {
    ResetDataIter();
    return;
  }

  SavePrevIndexValue();

  // Call Seek() rather than SeekForPrev() in the index block, because the
  // target data block will likely to contain the position for `target`, the
  // same as Seek(), rather than than before.
  // For example, if we have three data blocks, each containing two keys:
  //   [2, 4]  [6, 8] [10, 12]
  //  (the keys in the index block would be [4, 8, 12])
  // and the user calls SeekForPrev(7), we need to go to the second block,
  // just like if they call Seek(7).
  // The only case where the block is difference is when they seek to a position
  // in the boundary. For example, if they SeekForPrev(5), we should go to the
  // first block, rather than the second. However, we don't have the information
  // to distinguish the two unless we read the second block. In this case, we'll
  // end up with reading two blocks.
  index_iter_->Seek(target);

  if (!index_iter_->Valid()) {
    auto seek_status = index_iter_->status();
    // Check for IO error
    if (!seek_status.IsNotFound() && !seek_status.ok()) {
      ResetDataIter();
      return;
    }

    // With prefix index, Seek() returns NotFound if the prefix doesn't exist
    if (seek_status.IsNotFound()) {
      // Any key less than the target is fine for prefix seek
      ResetDataIter();
      return;
    } else {
      index_iter_->SeekToLast();
    }
    // Check for IO error
    if (!index_iter_->Valid()) {
      ResetDataIter();
      return;
    }
  }

  InitDataBlock();

  block_iter_.SeekForPrev(target);

  FindKeyBackward();
  CheckDataBlockWithinUpperBound();
  assert(!block_iter_.Valid() ||
         icomp_.Compare(target, block_iter_.key()) >= 0);
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::SeekToLast() {
  is_out_of_bound_ = false;
  is_at_first_key_from_index_ = false;
  SavePrevIndexValue();
  index_iter_->SeekToLast();
  if (!index_iter_->Valid()) {
    ResetDataIter();
    return;
  }
  InitDataBlock();
  block_iter_.SeekToLast();
  FindKeyBackward();
  CheckDataBlockWithinUpperBound();
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::Next() {
  if (is_at_first_key_from_index_ && !MaterializeCurrentBlock()) {
    return;
  }
  assert(block_iter_points_to_real_block_);
  block_iter_.Next();
  FindKeyForward();
  CheckOutOfBound();
}

template <class TBlockIter, typename TValue>
bool BlockBasedTableIterator<TBlockIter, TValue>::NextAndGetResult(
    IterateResult* result) {
  Next();
  bool is_valid = Valid();
  if (is_valid) {
    result->key = key();
    result->may_be_out_of_upper_bound = MayBeOutOfUpperBound();
  }
  return is_valid;
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::Prev() {
  if (is_at_first_key_from_index_) {
    is_at_first_key_from_index_ = false;

    index_iter_->Prev();
    if (!index_iter_->Valid()) {
      return;
    }

    InitDataBlock();
    block_iter_.SeekToLast();
  } else {
    assert(block_iter_points_to_real_block_);
    block_iter_.Prev();
  }

  FindKeyBackward();
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::InitDataBlock() {
  BlockHandle data_block_handle = index_iter_->value().handle;
  if (!block_iter_points_to_real_block_ ||
      data_block_handle.offset() != prev_block_offset_ ||
      // if previous attempt of reading the block missed cache, try again
      block_iter_.status().IsIncomplete()) {
    if (block_iter_points_to_real_block_) {
      ResetDataIter();
    }
    auto* rep = table_->get_rep();

    // Prefetch additional data for range scans (iterators). Enabled only for
    // user reads.
    // Implicit auto readahead:
    //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
    // Explicit user requested readahead:
    //   Enabled from the very first IO when ReadOptions.readahead_size is set.
    if (lookup_context_.caller != TableReaderCaller::kCompaction) {
      if (read_options_.readahead_size == 0) {
        // Implicit auto readahead
        num_file_reads_++;
        if (num_file_reads_ >
            BlockBasedTable::kMinNumFileReadsToStartAutoReadahead) {
          if (!rep->file->use_direct_io() &&
              (data_block_handle.offset() +
                   static_cast<size_t>(block_size(data_block_handle)) >
               readahead_limit_)) {
            // Buffered I/O
            // Discarding the return status of Prefetch calls intentionally, as
            // we can fallback to reading from disk if Prefetch fails.
            rep->file->Prefetch(data_block_handle.offset(), readahead_size_);
            readahead_limit_ = static_cast<size_t>(data_block_handle.offset() +
                                                   readahead_size_);
            // Keep exponentially increasing readahead size until
            // kMaxAutoReadaheadSize.
            readahead_size_ = std::min(BlockBasedTable::kMaxAutoReadaheadSize,
                                       readahead_size_ * 2);
          } else if (rep->file->use_direct_io() && !prefetch_buffer_) {
            // Direct I/O
            // Let FilePrefetchBuffer take care of the readahead.
            rep->CreateFilePrefetchBuffer(
                BlockBasedTable::kInitAutoReadaheadSize,
                BlockBasedTable::kMaxAutoReadaheadSize, &prefetch_buffer_);
          }
        }
      } else if (!prefetch_buffer_) {
        // Explicit user requested readahead
        // The actual condition is:
        // if (read_options_.readahead_size != 0 && !prefetch_buffer_)
        rep->CreateFilePrefetchBuffer(read_options_.readahead_size,
                                      read_options_.readahead_size,
                                      &prefetch_buffer_);
      }
    } else if (!prefetch_buffer_) {
      rep->CreateFilePrefetchBuffer(compaction_readahead_size_,
                                    compaction_readahead_size_,
                                    &prefetch_buffer_);
    }

    Status s;
    table_->NewDataBlockIterator<TBlockIter>(
        read_options_, data_block_handle, &block_iter_, block_type_,
        /*get_context=*/nullptr, &lookup_context_, s, prefetch_buffer_.get(),
        /*for_compaction=*/lookup_context_.caller ==
            TableReaderCaller::kCompaction);
    block_iter_points_to_real_block_ = true;
    CheckDataBlockWithinUpperBound();
  }
}

template <class TBlockIter, typename TValue>
bool BlockBasedTableIterator<TBlockIter, TValue>::MaterializeCurrentBlock() {
  assert(is_at_first_key_from_index_);
  assert(!block_iter_points_to_real_block_);
  assert(index_iter_->Valid());

  is_at_first_key_from_index_ = false;
  InitDataBlock();
  assert(block_iter_points_to_real_block_);
  block_iter_.SeekToFirst();

  if (!block_iter_.Valid() ||
      icomp_.Compare(block_iter_.key(),
                     index_iter_->value().first_internal_key) != 0) {
    // Uh oh.
    block_iter_.Invalidate(Status::Corruption(
        "first key in index doesn't match first key in block"));
    return false;
  }

  return true;
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::FindKeyForward() {
  // This method's code is kept short to make it likely to be inlined.

  assert(!is_out_of_bound_);
  assert(block_iter_points_to_real_block_);

  if (!block_iter_.Valid()) {
    // This is the only call site of FindBlockForward(), but it's extracted into
    // a separate method to keep FindKeyForward() short and likely to be
    // inlined. When transitioning to a different block, we call
    // FindBlockForward(), which is much longer and is probably not inlined.
    FindBlockForward();
  } else {
    // This is the fast path that avoids a function call.
  }
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::FindBlockForward() {
  // TODO the while loop inherits from two-level-iterator. We don't know
  // whether a block can be empty so it can be replaced by an "if".
  do {
    if (!block_iter_.status().ok()) {
      return;
    }
    // Whether next data block is out of upper bound, if there is one.
    const bool next_block_is_out_of_bound =
        read_options_.iterate_upper_bound != nullptr &&
        block_iter_points_to_real_block_ && !data_block_within_upper_bound_;
    assert(!next_block_is_out_of_bound ||
           user_comparator_.CompareWithoutTimestamp(
               *read_options_.iterate_upper_bound, /*a_has_ts=*/false,
               index_iter_->user_key(), /*b_has_ts=*/true) <= 0);
    ResetDataIter();
    index_iter_->Next();
    if (next_block_is_out_of_bound) {
      // The next block is out of bound. No need to read it.
      TEST_SYNC_POINT_CALLBACK("BlockBasedTableIterator:out_of_bound", nullptr);
      // We need to make sure this is not the last data block before setting
      // is_out_of_bound_, since the index key for the last data block can be
      // larger than smallest key of the next file on the same level.
      if (index_iter_->Valid()) {
        is_out_of_bound_ = true;
      }
      return;
    }

    if (!index_iter_->Valid()) {
      return;
    }

    IndexValue v = index_iter_->value();

    // TODO(kolmike): Remove the != kBlockCacheTier condition.
    if (!v.first_internal_key.empty() &&
        read_options_.read_tier != kBlockCacheTier) {
      // Index contains the first key of the block. Defer reading the block.
      is_at_first_key_from_index_ = true;
      return;
    }

    InitDataBlock();
    block_iter_.SeekToFirst();
  } while (!block_iter_.Valid());
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::FindKeyBackward() {
  while (!block_iter_.Valid()) {
    if (!block_iter_.status().ok()) {
      return;
    }

    ResetDataIter();
    index_iter_->Prev();

    if (index_iter_->Valid()) {
      InitDataBlock();
      block_iter_.SeekToLast();
    } else {
      return;
    }
  }

  // We could have check lower bound here too, but we opt not to do it for
  // code simplicity.
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter, TValue>::CheckOutOfBound() {
  if (read_options_.iterate_upper_bound != nullptr && Valid()) {
    is_out_of_bound_ =
        user_comparator_.CompareWithoutTimestamp(
            *read_options_.iterate_upper_bound, /*a_has_ts=*/false, user_key(),
            /*b_has_ts=*/true) <= 0;
  }
}

template <class TBlockIter, typename TValue>
void BlockBasedTableIterator<TBlockIter,
                             TValue>::CheckDataBlockWithinUpperBound() {
  if (read_options_.iterate_upper_bound != nullptr &&
      block_iter_points_to_real_block_) {
    data_block_within_upper_bound_ =
        (user_comparator_.CompareWithoutTimestamp(
             *read_options_.iterate_upper_bound, /*a_has_ts=*/false,
             index_iter_->user_key(),
             /*b_has_ts=*/true) > 0);
  }
}
}  // namespace ROCKSDB_NAMESPACE
