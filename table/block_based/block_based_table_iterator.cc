//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/block_based_table_iterator.h"

namespace ROCKSDB_NAMESPACE {

void BlockBasedTableIterator::SeekToFirst() { SeekImpl(nullptr, false); }

void BlockBasedTableIterator::Seek(const Slice& target) {
  SeekImpl(&target, true);
}

void BlockBasedTableIterator::SeekImpl(const Slice* target,
                                       bool async_prefetch) {
  ResetBlockCacheLookupVar();
  bool is_first_pass = !async_read_in_progress_;
  bool autotune_readaheadsize = is_first_pass &&
                                read_options_.auto_readahead_size &&
                                read_options_.iterate_upper_bound;

  if (autotune_readaheadsize &&
      table_->get_rep()->table_options.block_cache.get() &&
      !read_options_.async_io && direction_ == IterDirection::kForward) {
    readahead_cache_lookup_ = true;
  }

  // Second pass.
  if (async_read_in_progress_) {
    AsyncInitDataBlock(false);
  }

  is_out_of_bound_ = false;
  is_at_first_key_from_index_ = false;
  seek_stat_state_ = kNone;
  bool filter_checked = false;
  if (target &&
      !CheckPrefixMayMatch(*target, IterDirection::kForward, &filter_checked)) {
    ResetDataIter();
    RecordTick(table_->GetStatistics(), is_last_level_
                                            ? LAST_LEVEL_SEEK_FILTERED
                                            : NON_LAST_LEVEL_SEEK_FILTERED);
    return;
  }
  if (filter_checked) {
    seek_stat_state_ = kFilterUsed;
    RecordTick(table_->GetStatistics(), is_last_level_
                                            ? LAST_LEVEL_SEEK_FILTER_MATCH
                                            : NON_LAST_LEVEL_SEEK_FILTER_MATCH);
  }

  bool need_seek_index = true;

  //  In case of readahead_cache_lookup_, index_iter_ could change to find the
  //  readahead size in BlockCacheLookupForReadAheadSize so it needs to reseek.
  if (IsIndexAtCurr() && block_iter_points_to_real_block_ &&
      block_iter_.Valid()) {
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
    is_index_at_curr_block_ = true;
    if (!index_iter_->Valid()) {
      ResetDataIter();
      return;
    }
  }

  if (autotune_readaheadsize) {
    FindReadAheadSizeUpperBound();
    if (target) {
      index_iter_->Seek(*target);
    } else {
      index_iter_->SeekToFirst();
    }

    // Check for IO error.
    if (!index_iter_->Valid()) {
      ResetDataIter();
      return;
    }
  }

  // After reseek, index_iter_ point to the right key i.e. target in
  // case of readahead_cache_lookup_. So index_iter_ can be used directly.

  IndexValue v = index_iter_->value();
  const bool same_block = block_iter_points_to_real_block_ &&
                          v.handle.offset() == prev_block_offset_;

  if (!v.first_internal_key.empty() && !same_block &&
      (!target || icomp_.Compare(*target, v.first_internal_key) <= 0) &&
      allow_unprepared_value_) {
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
      if (read_options_.async_io && async_prefetch) {
        if (is_first_pass) {
          AsyncInitDataBlock(is_first_pass);
        }
        if (async_read_in_progress_) {
          // Status::TryAgain indicates asynchronous request for retrieval of
          // data blocks has been submitted. So it should return at this point
          // and Seek should be called again to retrieve the requested block and
          // execute the remaining code.
          return;
        }
      } else {
        InitDataBlock();
      }
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
    assert(!Valid() || icomp_.Compare(*target, key()) <= 0);
  }
}

void BlockBasedTableIterator::SeekForPrev(const Slice& target) {
  direction_ = IterDirection::kBackward;
  ResetBlockCacheLookupVar();
  is_out_of_bound_ = false;
  is_at_first_key_from_index_ = false;
  seek_stat_state_ = kNone;
  bool filter_checked = false;
  // For now totally disable prefix seek in auto prefix mode because we don't
  // have logic
  if (!CheckPrefixMayMatch(target, IterDirection::kBackward, &filter_checked)) {
    ResetDataIter();
    RecordTick(table_->GetStatistics(), is_last_level_
                                            ? LAST_LEVEL_SEEK_FILTERED
                                            : NON_LAST_LEVEL_SEEK_FILTERED);
    return;
  }
  if (filter_checked) {
    seek_stat_state_ = kFilterUsed;
    RecordTick(table_->GetStatistics(), is_last_level_
                                            ? LAST_LEVEL_SEEK_FILTER_MATCH
                                            : NON_LAST_LEVEL_SEEK_FILTER_MATCH);
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
  is_index_at_curr_block_ = true;

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

void BlockBasedTableIterator::SeekToLast() {
  direction_ = IterDirection::kBackward;
  ResetBlockCacheLookupVar();
  is_out_of_bound_ = false;
  is_at_first_key_from_index_ = false;
  seek_stat_state_ = kNone;

  SavePrevIndexValue();

  index_iter_->SeekToLast();
  is_index_at_curr_block_ = true;

  if (!index_iter_->Valid()) {
    ResetDataIter();
    return;
  }

  InitDataBlock();
  block_iter_.SeekToLast();
  FindKeyBackward();
  CheckDataBlockWithinUpperBound();
}

void BlockBasedTableIterator::Next() {
  if (is_at_first_key_from_index_ && !MaterializeCurrentBlock()) {
    return;
  }
  assert(block_iter_points_to_real_block_);
  block_iter_.Next();
  FindKeyForward();
  CheckOutOfBound();
}

bool BlockBasedTableIterator::NextAndGetResult(IterateResult* result) {
  Next();
  bool is_valid = Valid();
  if (is_valid) {
    result->key = key();
    result->bound_check_result = UpperBoundCheckResult();
    result->value_prepared = !is_at_first_key_from_index_;
  }
  return is_valid;
}

void BlockBasedTableIterator::Prev() {
  // Return Error.
  if (readahead_cache_lookup_) {
    block_iter_.Invalidate(Status::NotSupported(
        "auto tuning of readahead_size is not supported with Prev operation."));
    return;
  }

  ResetBlockCacheLookupVar();
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

void BlockBasedTableIterator::InitDataBlock() {
  BlockHandle data_block_handle;
  bool is_in_cache = false;
  bool use_block_cache_for_lookup = true;

  if (DoesContainBlockHandles()) {
    data_block_handle = block_handles_.front().handle_;
    is_in_cache = block_handles_.front().is_cache_hit_;
    use_block_cache_for_lookup = false;
  } else {
    data_block_handle = index_iter_->value().handle;
  }

  if (!block_iter_points_to_real_block_ ||
      data_block_handle.offset() != prev_block_offset_ ||
      // if previous attempt of reading the block missed cache, try again
      block_iter_.status().IsIncomplete()) {
    if (block_iter_points_to_real_block_) {
      ResetDataIter();
    }

    bool is_for_compaction =
        lookup_context_.caller == TableReaderCaller::kCompaction;

    // Initialize Data Block From CacheableEntry.
    if (is_in_cache) {
      Status s;
      block_iter_.Invalidate(Status::OK());
      table_->NewDataBlockIterator<DataBlockIter>(
          read_options_, (block_handles_.front().cachable_entry_).As<Block>(),
          &block_iter_, s);
    } else {
      auto* rep = table_->get_rep();

      std::function<void(uint64_t offset, size_t, size_t&)> readaheadsize_cb =
          nullptr;
      if (readahead_cache_lookup_) {
        readaheadsize_cb = std::bind(
            &BlockBasedTableIterator::BlockCacheLookupForReadAheadSize, this,
            std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3);
      }

      // Prefetch additional data for range scans (iterators).
      // Implicit auto readahead:
      //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
      // Explicit user requested readahead:
      //   Enabled from the very first IO when ReadOptions.readahead_size is
      //   set.
      block_prefetcher_.PrefetchIfNeeded(
          rep, data_block_handle, read_options_.readahead_size,
          is_for_compaction,
          /*no_sequential_checking=*/false, read_options_, readaheadsize_cb);

      Status s;
      table_->NewDataBlockIterator<DataBlockIter>(
          read_options_, data_block_handle, &block_iter_, BlockType::kData,
          /*get_context=*/nullptr, &lookup_context_,
          block_prefetcher_.prefetch_buffer(),
          /*for_compaction=*/is_for_compaction, /*async_read=*/false, s,
          use_block_cache_for_lookup);
    }
    block_iter_points_to_real_block_ = true;

    CheckDataBlockWithinUpperBound();
    if (!is_for_compaction &&
        (seek_stat_state_ & kDataBlockReadSinceLastSeek) == 0) {
      RecordTick(table_->GetStatistics(), is_last_level_
                                              ? LAST_LEVEL_SEEK_DATA
                                              : NON_LAST_LEVEL_SEEK_DATA);
      seek_stat_state_ = static_cast<SeekStatState>(
          seek_stat_state_ | kDataBlockReadSinceLastSeek | kReportOnUseful);
    }
  }
}

void BlockBasedTableIterator::AsyncInitDataBlock(bool is_first_pass) {
  BlockHandle data_block_handle = index_iter_->value().handle;
  bool is_for_compaction =
      lookup_context_.caller == TableReaderCaller::kCompaction;
  if (is_first_pass) {
    if (!block_iter_points_to_real_block_ ||
        data_block_handle.offset() != prev_block_offset_ ||
        // if previous attempt of reading the block missed cache, try again
        block_iter_.status().IsIncomplete()) {
      if (block_iter_points_to_real_block_) {
        ResetDataIter();
      }
      auto* rep = table_->get_rep();

      std::function<void(uint64_t offset, size_t, size_t&)> readaheadsize_cb =
          nullptr;
      if (readahead_cache_lookup_) {
        readaheadsize_cb = std::bind(
            &BlockBasedTableIterator::BlockCacheLookupForReadAheadSize, this,
            std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3);
      }

      // Prefetch additional data for range scans (iterators).
      // Implicit auto readahead:
      //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
      // Explicit user requested readahead:
      //   Enabled from the very first IO when ReadOptions.readahead_size is
      //   set.
      // In case of async_io with Implicit readahead, block_prefetcher_ will
      // always the create the prefetch buffer by setting no_sequential_checking
      // = true.
      block_prefetcher_.PrefetchIfNeeded(
          rep, data_block_handle, read_options_.readahead_size,
          is_for_compaction, /*no_sequential_checking=*/read_options_.async_io,
          read_options_, readaheadsize_cb);

      Status s;
      table_->NewDataBlockIterator<DataBlockIter>(
          read_options_, data_block_handle, &block_iter_, BlockType::kData,
          /*get_context=*/nullptr, &lookup_context_,
          block_prefetcher_.prefetch_buffer(),
          /*for_compaction=*/is_for_compaction, /*async_read=*/true, s,
          /*use_block_cache_for_lookup=*/true);

      if (s.IsTryAgain()) {
        async_read_in_progress_ = true;
        return;
      }
    }
  } else {
    // Second pass will call the Poll to get the data block which has been
    // requested asynchronously.
    Status s;
    table_->NewDataBlockIterator<DataBlockIter>(
        read_options_, data_block_handle, &block_iter_, BlockType::kData,
        /*get_context=*/nullptr, &lookup_context_,
        block_prefetcher_.prefetch_buffer(),
        /*for_compaction=*/is_for_compaction, /*async_read=*/false, s,
        /*use_block_cache_for_lookup=*/false);
  }
  block_iter_points_to_real_block_ = true;
  CheckDataBlockWithinUpperBound();

  if (!is_for_compaction &&
      (seek_stat_state_ & kDataBlockReadSinceLastSeek) == 0) {
    RecordTick(table_->GetStatistics(), is_last_level_
                                            ? LAST_LEVEL_SEEK_DATA
                                            : NON_LAST_LEVEL_SEEK_DATA);
    seek_stat_state_ = static_cast<SeekStatState>(
        seek_stat_state_ | kDataBlockReadSinceLastSeek | kReportOnUseful);
  }
  async_read_in_progress_ = false;
}

bool BlockBasedTableIterator::MaterializeCurrentBlock() {
  assert(is_at_first_key_from_index_);
  assert(!block_iter_points_to_real_block_);
  assert(index_iter_->Valid());

  is_at_first_key_from_index_ = false;
  InitDataBlock();
  assert(block_iter_points_to_real_block_);

  if (!block_iter_.status().ok()) {
    return false;
  }

  block_iter_.SeekToFirst();

  // MaterializeCurrentBlock is called when block is actually read by
  // calling InitDataBlock. is_at_first_key_from_index_ will be false for block
  // handles placed in blockhandle. So index_ will be pointing to current block.
  // After InitDataBlock, index_iter_ can point to different block if
  // BlockCacheLookupForReadAheadSize is called.
  Slice first_internal_key;
  if (DoesContainBlockHandles()) {
    first_internal_key = block_handles_.front().first_internal_key_;
  } else {
    first_internal_key = index_iter_->value().first_internal_key;
  }

  if (!block_iter_.Valid() ||
      icomp_.Compare(block_iter_.key(), first_internal_key) != 0) {
    block_iter_.Invalidate(Status::Corruption(
        "first key in index doesn't match first key in block"));
    return false;
  }
  return true;
}

void BlockBasedTableIterator::FindKeyForward() {
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

void BlockBasedTableIterator::FindBlockForward() {
  // TODO the while loop inherits from two-level-iterator. We don't know
  // whether a block can be empty so it can be replaced by an "if".
  do {
    if (!block_iter_.status().ok()) {
      return;
    }
    // Whether next data block is out of upper bound, if there is one.
    //  index_iter_ can point to different block in case of
    //  readahead_cache_lookup_. readahead_cache_lookup_ will be handle the
    //  upper_bound check.
    bool next_block_is_out_of_bound =
        IsIndexAtCurr() && read_options_.iterate_upper_bound != nullptr &&
        block_iter_points_to_real_block_ &&
        block_upper_bound_check_ == BlockUpperBound::kUpperBoundInCurBlock;

    assert(!next_block_is_out_of_bound ||
           user_comparator_.CompareWithoutTimestamp(
               *read_options_.iterate_upper_bound, /*a_has_ts=*/false,
               index_iter_->user_key(), /*b_has_ts=*/true) <= 0);

    ResetDataIter();

    if (DoesContainBlockHandles()) {
      // Advance and point to that next Block handle to make that block handle
      // current.
      block_handles_.pop_front();
    }

    if (!DoesContainBlockHandles()) {
      // For readahead_cache_lookup_ enabled scenario -
      // 1. In case of Seek, block_handle will be empty and it should be follow
      //    as usual doing index_iter_->Next().
      // 2. If block_handles is empty and index is not at current because of
      //    lookup (during Next), it should skip doing index_iter_->Next(), as
      //    it's already pointing to next block;
      // 3. Last block could be out of bound and it won't iterate over that
      // during BlockCacheLookup. We need to set for that block here.
      if (IsIndexAtCurr() || is_index_out_of_bound_) {
        index_iter_->Next();
        if (is_index_out_of_bound_) {
          next_block_is_out_of_bound = is_index_out_of_bound_;
          is_index_out_of_bound_ = false;
        }
      } else {
        // Skip Next as index_iter_ already points to correct index when it
        // iterates in BlockCacheLookupForReadAheadSize.
        is_index_at_curr_block_ = true;
      }

      if (next_block_is_out_of_bound) {
        // The next block is out of bound. No need to read it.
        TEST_SYNC_POINT_CALLBACK("BlockBasedTableIterator:out_of_bound",
                                 nullptr);
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

      if (!v.first_internal_key.empty() && allow_unprepared_value_) {
        // Index contains the first key of the block. Defer reading the block.
        is_at_first_key_from_index_ = true;
        return;
      }
    }
    InitDataBlock();
    block_iter_.SeekToFirst();
  } while (!block_iter_.Valid());
}

void BlockBasedTableIterator::FindKeyBackward() {
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

void BlockBasedTableIterator::CheckOutOfBound() {
  if (read_options_.iterate_upper_bound != nullptr &&
      block_upper_bound_check_ != BlockUpperBound::kUpperBoundBeyondCurBlock &&
      Valid()) {
    is_out_of_bound_ =
        user_comparator_.CompareWithoutTimestamp(
            *read_options_.iterate_upper_bound, /*a_has_ts=*/false, user_key(),
            /*b_has_ts=*/true) <= 0;
  }
}

void BlockBasedTableIterator::CheckDataBlockWithinUpperBound() {
  if (IsIndexAtCurr() && read_options_.iterate_upper_bound != nullptr &&
      block_iter_points_to_real_block_) {
    block_upper_bound_check_ = (user_comparator_.CompareWithoutTimestamp(
                                    *read_options_.iterate_upper_bound,
                                    /*a_has_ts=*/false, index_iter_->user_key(),
                                    /*b_has_ts=*/true) > 0)
                                   ? BlockUpperBound::kUpperBoundBeyondCurBlock
                                   : BlockUpperBound::kUpperBoundInCurBlock;
  }
}

void BlockBasedTableIterator::FindReadAheadSizeUpperBound() {
  size_t total_bytes_till_upper_bound = 0;
  size_t footer = table_->get_rep()->footer.GetBlockTrailerSize();
  uint64_t start_offset = index_iter_->value().handle.offset();

  do {
    BlockHandle block_handle = index_iter_->value().handle;
    total_bytes_till_upper_bound += block_handle.size();
    total_bytes_till_upper_bound += footer;

    // Can't figure out for current block if current block
    // is out of bound. But for next block we can find that.
    // If curr block's index key >= iterate_upper_bound, it
    // means all the keys in next block or above are out of
    // bound.
    if (IsNextBlockOutOfBound()) {
      break;
    }

    // Since next block is not out of bound, iterate to that
    // index block and add it's Data block size to
    // readahead_size.
    index_iter_->Next();

    if (!index_iter_->Valid()) {
      break;
    }

  } while (true);

  block_prefetcher_.SetUpperBoundOffset(start_offset +
                                        total_bytes_till_upper_bound);
}

void BlockBasedTableIterator::BlockCacheLookupForReadAheadSize(
    uint64_t offset, size_t readahead_size, size_t& updated_readahead_size) {
  updated_readahead_size = readahead_size;

  // readahead_cache_lookup_ can be set false after Seek, if after Seek or Next
  // there is SeekForPrev or any other backward operation.
  if (!readahead_cache_lookup_) {
    return;
  }

  assert(!DoesContainBlockHandles());
  assert(index_iter_->value().handle.offset() == offset);

  // Error. current offset should be equal to what's requested for prefetching.
  if (index_iter_->value().handle.offset() != offset) {
    return;
  }

  if (IsNextBlockOutOfBound()) {
    updated_readahead_size = 0;
    return;
  }

  size_t current_readahead_size = 0;
  size_t footer = table_->get_rep()->footer.GetBlockTrailerSize();

  // Add the current block to block_handles_.
  {
    BlockHandleInfo block_handle_info;
    block_handle_info.handle_ = index_iter_->value().handle;
    block_handle_info.SetFirstInternalKey(
        index_iter_->value().first_internal_key);
    block_handles_.emplace_back(std::move(block_handle_info));
  }

  // Current block is included in length. Readahead should start from next
  // block.
  index_iter_->Next();
  is_index_at_curr_block_ = false;

  while (index_iter_->Valid()) {
    BlockHandle block_handle = index_iter_->value().handle;

    // Adding this data block exceeds passed down readahead_size. So this data
    // block won't be added.
    if (current_readahead_size + block_handle.size() + footer >
        readahead_size) {
      break;
    }

    current_readahead_size += block_handle.size();
    current_readahead_size += footer;

    // For current data block, do the lookup in the cache. Lookup should pin the
    // data block and add the placeholder for cache.
    BlockHandleInfo block_handle_info;
    block_handle_info.handle_ = index_iter_->value().handle;
    block_handle_info.SetFirstInternalKey(
        index_iter_->value().first_internal_key);

    Status s = table_->LookupAndPinBlocksInCache<Block_kData>(
        read_options_, block_handle,
        &(block_handle_info.cachable_entry_).As<Block_kData>());
    if (!s.ok()) {
      break;
    }

    block_handle_info.is_cache_hit_ =
        (block_handle_info.cachable_entry_.GetValue() ||
         block_handle_info.cachable_entry_.GetCacheHandle());

    // Add the handle to the queue.
    block_handles_.emplace_back(std::move(block_handle_info));

    // Can't figure out for current block if current block
    // is out of bound. But for next block we can find that.
    // If curr block's index key >= iterate_upper_bound, it
    // means all the keys in next block or above are out of
    // bound.
    if (IsNextBlockOutOfBound()) {
      is_index_out_of_bound_ = true;
      break;
    }
    index_iter_->Next();
  };

  // Iterate cache hit block handles from the end till a Miss is there, to
  // update the readahead_size.
  for (auto it = block_handles_.rbegin();
       it != block_handles_.rend() && (*it).is_cache_hit_ == true; ++it) {
    current_readahead_size -= (*it).handle_.size();
    current_readahead_size -= footer;
  }
  updated_readahead_size = current_readahead_size;
  ResetPreviousBlockOffset();
}

}  // namespace ROCKSDB_NAMESPACE
