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

void BlockBasedTableIterator::SeekSecondPass(const Slice* target) {
  AsyncInitDataBlock(/*is_first_pass=*/false);

  if (target) {
    block_iter_.Seek(*target);
  } else {
    block_iter_.SeekToFirst();
  }
  FindKeyForward();

  CheckOutOfBound();

  if (target) {
    assert(!Valid() || icomp_.Compare(*target, key()) <= 0);
  }
}

void BlockBasedTableIterator::SeekImpl(const Slice* target,
                                       bool async_prefetch) {
  // TODO(hx235): set `seek_key_prefix_for_readahead_trimming_`
  // even when `target == nullptr` that is when `SeekToFirst()` is called
  if (!multi_scan_status_.ok()) {
    return;
  }
  if (multi_scan_) {
    SeekMultiScan(target);
    return;
  }

  if (target != nullptr && prefix_extractor_ &&
      read_options_.prefix_same_as_start) {
    const Slice& seek_user_key = ExtractUserKey(*target);
    seek_key_prefix_for_readahead_trimming_ =
        prefix_extractor_->InDomain(seek_user_key)
            ? prefix_extractor_->Transform(seek_user_key).ToString()
            : "";
  }

  bool is_first_pass = !async_read_in_progress_;

  if (!is_first_pass) {
    SeekSecondPass(target);
    return;
  }

  ResetBlockCacheLookupVar();

  bool autotune_readaheadsize =
      read_options_.auto_readahead_size &&
      (read_options_.iterate_upper_bound || read_options_.prefix_same_as_start);

  if (autotune_readaheadsize &&
      table_->get_rep()->table_options.block_cache.get() &&
      direction_ == IterDirection::kForward) {
    readahead_cache_lookup_ = true;
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
  //  readahead size in BlockCacheLookupForReadAheadSize so it needs to
  //  reseek.
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
        AsyncInitDataBlock(/*is_first_pass=*/true);
        if (async_read_in_progress_) {
          // Status::TryAgain indicates asynchronous request for retrieval of
          // data blocks has been submitted. So it should return at this point
          // and Seek should be called again to retrieve the requested block
          // and execute the remaining code.
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
  multi_scan_.reset();
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
  multi_scan_.reset();
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
  assert(Valid());
  if (is_at_first_key_from_index_ && !MaterializeCurrentBlock()) {
    assert(!multi_scan_);
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
  assert(!multi_scan_);
  if ((readahead_cache_lookup_ && !IsIndexAtCurr()) || multi_scan_) {
    multi_scan_.reset();
    // In case of readahead_cache_lookup_, index_iter_ has moved forward. So we
    // need to reseek the index_iter_ to point to current block by using
    // block_iter_'s key.
    if (Valid()) {
      ResetBlockCacheLookupVar();
      direction_ = IterDirection::kBackward;
      Slice last_key = key();

      index_iter_->Seek(last_key);
      is_index_at_curr_block_ = true;

      // Check for IO error.
      if (!index_iter_->Valid()) {
        ResetDataIter();
        return;
      }
    }

    if (!Valid()) {
      ResetDataIter();
      return;
    }
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
    data_block_handle = block_handles_->front().handle_;
    is_in_cache = block_handles_->front().is_cache_hit_;
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
          read_options_, (block_handles_->front().cachable_entry_).As<Block>(),
          &block_iter_, s);
    } else {
      auto* rep = table_->get_rep();

      std::function<void(bool, uint64_t&, uint64_t&)> readaheadsize_cb =
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
          /*no_sequential_checking=*/false, read_options_, readaheadsize_cb,
          read_options_.async_io);

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
  BlockHandle data_block_handle;
  bool is_for_compaction =
      lookup_context_.caller == TableReaderCaller::kCompaction;
  if (is_first_pass) {
    data_block_handle = index_iter_->value().handle;
    if (!block_iter_points_to_real_block_ ||
        data_block_handle.offset() != prev_block_offset_ ||
        // if previous attempt of reading the block missed cache, try again
        block_iter_.status().IsIncomplete()) {
      if (block_iter_points_to_real_block_) {
        ResetDataIter();
      }
      auto* rep = table_->get_rep();

      std::function<void(bool, uint64_t&, uint64_t&)> readaheadsize_cb =
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
          read_options_, readaheadsize_cb, read_options_.async_io);

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
    bool is_in_cache = false;

    if (DoesContainBlockHandles()) {
      data_block_handle = block_handles_->front().handle_;
      is_in_cache = block_handles_->front().is_cache_hit_;
    } else {
      data_block_handle = index_iter_->value().handle;
    }

    Status s;
    // Initialize Data Block From CacheableEntry.
    if (is_in_cache) {
      block_iter_.Invalidate(Status::OK());
      table_->NewDataBlockIterator<DataBlockIter>(
          read_options_, (block_handles_->front().cachable_entry_).As<Block>(),
          &block_iter_, s);
    } else {
      table_->NewDataBlockIterator<DataBlockIter>(
          read_options_, data_block_handle, &block_iter_, BlockType::kData,
          /*get_context=*/nullptr, &lookup_context_,
          block_prefetcher_.prefetch_buffer(),
          /*for_compaction=*/is_for_compaction, /*async_read=*/false, s,
          /*use_block_cache_for_lookup=*/false);
    }
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
    first_internal_key = block_handles_->front().first_internal_key_;
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
  if (multi_scan_) {
    FindBlockForwardInMultiScan();
    return;
  }
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
      block_handles_->pop_front();
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

void BlockBasedTableIterator::InitializeStartAndEndOffsets(
    bool read_curr_block, bool& found_first_miss_block,
    uint64_t& start_updated_offset, uint64_t& end_updated_offset,
    size_t& prev_handles_size) {
  assert(block_handles_ != nullptr);
  prev_handles_size = block_handles_->size();
  size_t footer = table_->get_rep()->footer.GetBlockTrailerSize();

  // It initialize start and end offset to begin which is covered by following
  // scenarios
  if (read_curr_block) {
    if (!DoesContainBlockHandles()) {
      // Scenario 1 : read_curr_block (callback made on miss block which caller
      //              was reading) and it has no existing handles in queue. i.e.
      //              index_iter_ is pointing to block that is being read by
      //              caller.
      //
      // Add current block here as it doesn't need any lookup.
      BlockHandleInfo block_handle_info;
      block_handle_info.handle_ = index_iter_->value().handle;
      block_handle_info.SetFirstInternalKey(
          index_iter_->value().first_internal_key);

      end_updated_offset = block_handle_info.handle_.offset() + footer +
                           block_handle_info.handle_.size();
      block_handles_->emplace_back(std::move(block_handle_info));

      index_iter_->Next();
      is_index_at_curr_block_ = false;
      found_first_miss_block = true;
    } else {
      // Scenario 2 : read_curr_block (callback made on miss block which caller
      //              was reading) but the queue already has some handles.
      //
      // It can be due to reading error in second buffer in FilePrefetchBuffer.
      // BlockHandles already added to the queue but there was error in fetching
      // those data blocks. So in this call they need to be read again.
      found_first_miss_block = true;
      // Initialize prev_handles_size to 0 as all those handles need to be read
      // again.
      prev_handles_size = 0;
      start_updated_offset = block_handles_->front().handle_.offset();
      end_updated_offset = block_handles_->back().handle_.offset() + footer +
                           block_handles_->back().handle_.size();
    }
  } else {
    // Scenario 3 : read_curr_block is false (callback made to do additional
    //              prefetching in buffers) and the queue already has some
    //              handles from first buffer.
    if (DoesContainBlockHandles()) {
      start_updated_offset = block_handles_->back().handle_.offset() + footer +
                             block_handles_->back().handle_.size();
      end_updated_offset = start_updated_offset;
    } else {
      // Scenario 4 : read_curr_block is false (callback made to do additional
      //              prefetching in buffers) but the queue has no handle
      //              from first buffer.
      //
      // It can be when Reseek is from block cache (which doesn't clear the
      // buffers in FilePrefetchBuffer but clears block handles from queue) and
      // reseek also lies within the buffer. So Next will get data from
      // existing buffers until this callback is made to prefetch additional
      // data. All handles need to be added to the queue starting from
      // index_iter_.
      assert(index_iter_->Valid());
      start_updated_offset = index_iter_->value().handle.offset();
      end_updated_offset = start_updated_offset;
    }
  }
}

// BlockCacheLookupForReadAheadSize API lookups in the block cache and tries to
// reduce the start and end offset passed.
//
// Implementation -
// This function looks into the block cache for the blocks between start_offset
// and end_offset and add all the handles in the queue.
// It then iterates from the end to find first miss block and update the end
// offset to that block.
// It also iterates from the start and find first miss block and update the
// start offset to that block.
//
// Arguments -
// start_offset    : Offset from which the caller wants to read.
// end_offset      : End offset till which the caller wants to read.
// read_curr_block : True if this call was due to miss in the cache and
//                   caller wants to read that block.
//                   False if current call is to prefetch additional data in
//                   extra buffers.
void BlockBasedTableIterator::BlockCacheLookupForReadAheadSize(
    bool read_curr_block, uint64_t& start_offset, uint64_t& end_offset) {
  uint64_t start_updated_offset = start_offset;

  // readahead_cache_lookup_ can be set false, if after Seek and Next
  // there is SeekForPrev or any other backward operation.
  if (!readahead_cache_lookup_) {
    return;
  }

  size_t footer = table_->get_rep()->footer.GetBlockTrailerSize();
  if (read_curr_block && !DoesContainBlockHandles() &&
      IsNextBlockOutOfReadaheadBound()) {
    end_offset = index_iter_->value().handle.offset() + footer +
                 index_iter_->value().handle.size();
    return;
  }

  uint64_t end_updated_offset = start_updated_offset;
  bool found_first_miss_block = false;
  size_t prev_handles_size;

  // Initialize start and end offsets based on exisiting handles in the queue
  // and read_curr_block argument passed.
  if (block_handles_ == nullptr) {
    block_handles_.reset(new std::deque<BlockHandleInfo>());
  }
  InitializeStartAndEndOffsets(read_curr_block, found_first_miss_block,
                               start_updated_offset, end_updated_offset,
                               prev_handles_size);

  while (index_iter_->Valid() && !is_index_out_of_bound_) {
    BlockHandle block_handle = index_iter_->value().handle;

    // Adding this data block exceeds end offset. So this data
    // block won't be added.
    // There can be a case where passed end offset is smaller than
    // block_handle.size() + footer because of readahead_size truncated to
    // upper_bound. So we prefer to read the block rather than skip it to avoid
    // sync read calls in case of async_io.
    if (start_updated_offset != end_updated_offset &&
        (end_updated_offset + block_handle.size() + footer > end_offset)) {
      break;
    }

    // For current data block, do the lookup in the cache. Lookup should pin the
    // data block in cache.
    BlockHandleInfo block_handle_info;
    block_handle_info.handle_ = index_iter_->value().handle;
    block_handle_info.SetFirstInternalKey(
        index_iter_->value().first_internal_key);
    end_updated_offset += footer + block_handle_info.handle_.size();

    Status s = table_->LookupAndPinBlocksInCache<Block_kData>(
        read_options_, block_handle,
        &(block_handle_info.cachable_entry_).As<Block_kData>());
    if (!s.ok()) {
#ifndef NDEBUG
      // To allow fault injection verification to pass since non-okay status in
      // `BlockCacheLookupForReadAheadSize()` won't fail the read but to have
      // less or no readahead
      IGNORE_STATUS_IF_ERROR(s);
#endif
      break;
    }

    block_handle_info.is_cache_hit_ =
        (block_handle_info.cachable_entry_.GetValue() ||
         block_handle_info.cachable_entry_.GetCacheHandle());

    // If this is the first miss block, update start offset to this block.
    if (!found_first_miss_block && !block_handle_info.is_cache_hit_) {
      found_first_miss_block = true;
      start_updated_offset = block_handle_info.handle_.offset();
    }

    // Add the handle to the queue.
    block_handles_->emplace_back(std::move(block_handle_info));

    // Can't figure out for current block if current block
    // is out of bound. But for next block we can find that.
    // If curr block's index key >= iterate_upper_bound, it
    // means all the keys in next block or above are out of
    // bound.
    if (IsNextBlockOutOfReadaheadBound()) {
      is_index_out_of_bound_ = true;
      break;
    }
    index_iter_->Next();
    is_index_at_curr_block_ = false;
  }

#ifndef NDEBUG
  // To allow fault injection verification to pass since non-okay status in
  // `BlockCacheLookupForReadAheadSize()` won't fail the read but to have less
  // or no readahead
  if (!index_iter_->status().ok()) {
    IGNORE_STATUS_IF_ERROR(index_iter_->status());
  }
#endif

  if (found_first_miss_block) {
    // Iterate cache hit block handles from the end till a Miss is there, to
    // truncate and update the end offset till that Miss.
    auto it = block_handles_->rbegin();
    auto it_end =
        block_handles_->rbegin() + (block_handles_->size() - prev_handles_size);

    while (it != it_end && (*it).is_cache_hit_ &&
           start_updated_offset != (*it).handle_.offset()) {
      it++;
    }
    end_updated_offset = (*it).handle_.offset() + footer + (*it).handle_.size();
  } else {
    // Nothing to read. Can be because of IOError in index_iter_->Next() or
    // reached upper_bound.
    end_updated_offset = start_updated_offset;
  }

  end_offset = end_updated_offset;
  start_offset = start_updated_offset;
  ResetPreviousBlockOffset();
}

// Note:
// - Iterator should not be reused for multiple multiscans or mixing
// multiscan with regular iterator usage.
// - scan ranges should be non-overlapping, and have increasing start keys.
// If a scan range's limit is not set, then there should only be one scan range.
// - After Prepare(), the iterator expects Seek to be called on the start key
// of each ScanOption in order. If any other Seek is done, an error status is
// returned
// - Whenever all blocks of a scan opt are exhausted, the iterator will become
// invalid and UpperBoundCheckResult() will return kOutOfBound. So that the
// upper layer (LevelIterator) will stop scanning instead thinking EOF is
// reached and continue into the next file. The only exception is for the last
// scan opt. If we reach the end of the last scan opt, UpperBoundCheckResult()
// will return kUnknown instead of kOutOfBound. This mechanism requires that
// scan opts are properly pruned such that there is no scan opt that is after
// this file's key range.
// FIXME: DBIter and MergingIterator may
// internally do Seek() on child iterators, e.g. due to
// ReadOptions::max_skippable_internal_keys or reseeking into range deletion
// end key. These Seeks will be handled properly, as long as the target is
// moving forward.
void BlockBasedTableIterator::Prepare(const MultiScanArgs* multiscan_opts) {
  assert(!multi_scan_);
  RecordTick(table_->GetStatistics(), MULTISCAN_PREPARE_CALLS);
  StopWatch sw(table_->get_rep()->ioptions.clock, table_->GetStatistics(),
               MULTISCAN_PREPARE_MICROS);

  if (!index_iter_->status().ok()) {
    multi_scan_status_ = index_iter_->status();
    RecordTick(table_->GetStatistics(), MULTISCAN_PREPARE_ERRORS);
    return;
  }
  if (multi_scan_) {
    multi_scan_.reset();
    multi_scan_status_ = Status::InvalidArgument("Prepare already called");
    RecordTick(table_->GetStatistics(), MULTISCAN_PREPARE_ERRORS);
    return;
  }

  index_iter_->Prepare(multiscan_opts);

  std::vector<BlockHandle> scan_block_handles;
  std::vector<std::string> data_block_separators;
  std::vector<std::tuple<size_t, size_t>> block_index_ranges_per_scan;
  const std::vector<ScanOptions>& scan_opts = multiscan_opts->GetScanRanges();
  multi_scan_status_ =
      CollectBlockHandles(scan_opts, &scan_block_handles,
                          &block_index_ranges_per_scan, &data_block_separators);
  if (!multi_scan_status_.ok()) {
    RecordTick(table_->GetStatistics(), MULTISCAN_PREPARE_ERRORS);
    return;
  }

  // Calculate prefetch_max_idx (enforces max_prefetch_size)
  size_t prefetch_max_idx = scan_block_handles.size();
  if (multiscan_opts->max_prefetch_size > 0) {
    uint64_t total_size = 0;
    for (size_t i = 0; i < scan_block_handles.size(); ++i) {
      total_size +=
          BlockBasedTable::BlockSizeWithTrailer(scan_block_handles[i]);
      if (total_size > multiscan_opts->max_prefetch_size) {
        prefetch_max_idx = i;
        break;
      }
    }
  }

  // Create block handles vector for IODispatcher (limited to prefetch_max_idx)
  std::vector<BlockHandle> blocks_to_prefetch;
  if (prefetch_max_idx > 0) {
    blocks_to_prefetch.assign(scan_block_handles.begin(),
                              scan_block_handles.begin() + prefetch_max_idx);
  }

  // Submit to IODispatcher
  auto job = std::make_shared<IOJob>();
  job->table = const_cast<BlockBasedTable*>(table_);
  job->block_handles = std::move(blocks_to_prefetch);
  job->job_options.io_coalesce_threshold =
      multiscan_opts->io_coalesce_threshold;
  job->job_options.read_options = read_options_;
  job->job_options.read_options.async_io = multiscan_opts->use_async_io;

  std::shared_ptr<ReadSet> read_set;
  // IODispatcher should be provided by DBIter::Prepare() to enable sharing
  // across all BlockBasedTableIterators in the scan. Create one if not
  // provided (for direct calls to Prepare, e.g., in unit tests).
  std::shared_ptr<IODispatcher> dispatcher = multiscan_opts->io_dispatcher;
  if (!dispatcher) {
    dispatcher.reset(NewIODispatcher());
  }
  multi_scan_status_ = dispatcher->SubmitJob(job, &read_set);
  if (!multi_scan_status_.ok()) {
    RecordTick(table_->GetStatistics(), MULTISCAN_PREPARE_ERRORS);
    return;
  }

  // Successful Prepare, init related states so the iterator reads from prepared
  // blocks. Note: data_block_separators keeps full size for seek logic.
  multi_scan_ = std::make_unique<MultiScanState>(
      table_->get_rep()->ioptions.env->GetFileSystem(), multiscan_opts,
      std::move(read_set), std::move(data_block_separators),
      std::move(block_index_ranges_per_scan), prefetch_max_idx,
      table_->GetStatistics());

  is_index_at_curr_block_ = false;
  block_iter_points_to_real_block_ = false;
}

void BlockBasedTableIterator::SeekMultiScan(const Slice* seek_target) {
  assert(multi_scan_ && multi_scan_status_.ok());
  // This is a MultiScan and Prepare() has been called.

  // Reset out of bound on seek, if it is out of bound again, it will be set
  // properly later in the code path
  is_out_of_bound_ = false;

  // Validate seek key with scan options
  if (!seek_target) {
    // start key must be set for multi-scan
    multi_scan_status_ = Status::InvalidArgument("No seek key for MultiScan");
    RecordTick(table_->GetStatistics(), MULTISCAN_SEEK_ERRORS);
    return;
  }

  // Check the case where there is no range prepared on this table
  if (multi_scan_->scan_opts->size() == 0) {
    // out of bound
    MarkPreparedRangeExhausted();
    return;
  }

  // Check whether seek key is moving forward.
  if (multi_scan_->prev_seek_key_.empty() ||
      icomp_.Compare(*seek_target, multi_scan_->prev_seek_key_) > 0) {
    // If seek key is empty or is larger than previous seek key, update the
    // previous seek key. Otherwise use the previous seek key as the adjusted
    // seek target moving forward. This prevents seek target going backward,
    // which would visit pages that have been unpinned.
    // This issue is caused by sub-optimal range delete handling inside merge
    // iterator.
    // TODO xingbo issues:14068 : Optimize the handling of range delete iterator
    // inside merge iterator, so that it doesn't move seek key backward. After
    // that we could return error if the key moves backward here.
    multi_scan_->prev_seek_key_ = seek_target->ToString();
  } else {
    // Seek key is adjusted to previous one, we can return here directly.
    return;
  }

  // There are 3 different Cases we need to handle:
  // The following diagram explain different seek targets seeking at various
  // position on the table, while the next_scan_idx points to the PreparedRange
  // 2.
  //
  // next_scan_idx: -------------------┐
  //                                   ▼
  // table:     : __[PreparedRange 1]__[PreparedRange 2]__[PreparedRange 3]__
  // Seek target: <----- Case 1 ------>▲<------------- Case 2 -------------->
  //                                   │
  //                                 Case 3
  //
  // Case 1: seek before the start of next prepared ranges. This could happen
  //    due to too many delete tomestone triggered reseek or delete range.
  // Case 2: seek after the start of next prepared range.
  //    This could happen due to seek key adjustment from delete range file.
  //    E.g. LSM has 3 levels, each level has only 1 file:
  //    L1 : key :              0---10
  //    L2 : Delete range key : 0-5
  //    L3 : key :              0---10
  //    When a range 2-8 was prepared, the prepared key would be 2 on L3 file,
  //    but the seek key would be 5, as the seek key was updated by the largest
  //    key of delete range. This causes all of the cases above to be possible,
  //    when the ranges are adjusted in the above examples.
  // Case 3: seek at the beginning of a prepared range (expected case)

  // Allow reseek on the start of the last prepared range due to too many
  // tombstone
  multi_scan_->next_scan_idx =
      std::min(multi_scan_->next_scan_idx,
               multi_scan_->block_index_ranges_per_scan.size() - 1);

  auto user_seek_target = ExtractUserKey(*seek_target);

  auto compare_next_scan_start_result =
      user_comparator_.CompareWithoutTimestamp(
          user_seek_target, /*a_has_ts=*/true,
          multi_scan_->scan_opts->GetScanRanges()[multi_scan_->next_scan_idx]
              .range.start.value(),
          /*b_has_ts=*/false);

  if (compare_next_scan_start_result != 0) {
    // The seek target is not exactly same as what was prepared.
    if (compare_next_scan_start_result < 0) {
      // Case 1:
      if (multi_scan_->next_scan_idx == 0) {
        // This should not happen, even when seek target is adjusted by delete
        // range. The reason is that if the seek target is before the start key
        // of the first prepared range, its end key needs to be >= the smallest
        // key of this file, otherwise it is skipped in level iterator. If its
        // end key is >= the smallest key of this file, then this range will be
        // prepared for this file. As delete range could only adjust seek
        // target forward, so it would never be before the start key of the
        // first prepared range.
        assert(false && "Seek target before the first prepared range");
        MarkPreparedRangeExhausted();
        return;
      }
      auto seek_target_before_previous_prepared_range =
          user_comparator_.CompareWithoutTimestamp(
              user_seek_target, /*a_has_ts=*/true,
              multi_scan_->scan_opts
                  ->GetScanRanges()[multi_scan_->next_scan_idx - 1]
                  .range.start.value(),
              /*b_has_ts=*/false) < 0;
      // Not expected to happen
      // This should never happen, the reason is that the
      // multi_scan_->next_scan_idx is set to a non zero value is due to a seek
      // target larger or equal to the start key of multi_scan_->next_scan_idx-1
      // happened earlier. If a seek happens before the start key of
      // multi_scan_->next_scan_idx-1, it would seek a key that is less than
      // what was seeked before.
      assert(!seek_target_before_previous_prepared_range);
      if (seek_target_before_previous_prepared_range) {
        multi_scan_status_ = Status::InvalidArgument(
            "Seek target is before the previous prepared range at index " +
            std::to_string(multi_scan_->next_scan_idx));
        RecordTick(table_->GetStatistics(), MULTISCAN_SEEK_ERRORS);
        return;
      }
      // It should only be possible to seek a key between the start of current
      // prepared scan and start of next prepared range.
      MultiScanUnexpectedSeekTarget(seek_target, &user_seek_target);
    } else {
      // Case 2:
      MultiScanUnexpectedSeekTarget(seek_target, &user_seek_target);
    }
  } else {
    // Case 2:
    assert(multi_scan_->next_scan_idx <
           multi_scan_->block_index_ranges_per_scan.size());

    auto [cur_scan_start_idx, cur_scan_end_idx] =
        multi_scan_->block_index_ranges_per_scan[multi_scan_->next_scan_idx];
    // We should have the data block already loaded
    ++multi_scan_->next_scan_idx;
    if (cur_scan_start_idx >= cur_scan_end_idx) {
      // No blocks are prepared for this range at current file.
      MarkPreparedRangeExhausted();
      return;
    }

    // max_sequential_skip_in_iterations can trigger a reseek on the start
    // key of a scan range, even though the multiscan is already past
    // `cur_scan_start_idx` (e.g., a user key spans multiple data blocks).
    size_t block_idx =
        std::max(cur_scan_start_idx, multi_scan_->cur_data_block_idx);
    MultiScanSeekTargetFromBlock(seek_target, block_idx);
  }
}

void BlockBasedTableIterator::MultiScanUnexpectedSeekTarget(
    const Slice* seek_target, const Slice* user_seek_target) {
  // linear search the block that contains the seek target, and unpin blocks
  // that are before it.

  // The logic here could be confusing when there is a delete range involved.
  // E.g. we have an LSM with 3 levels, each level has only 1 file:
  // L1: data file :    0---10
  // L2: Delete range : 0-5
  // L3: data file :    0---10
  //
  // MultiScan on ranges 1-2, 3-4, and 5-6.
  // When user first do Seek(1), on level 2, due to delete range 0-5, the seek
  // key is adjusted to 5 at level 3. Therefore, we will internally do Seek(5)
  // and unpins all blocks until 5 at level 3. Then the next scan's blocks from
  // 3-4 are unpinned at level 3. It is confusing that maybe block 3-4 should
  // not be unpinned, as next scan would need it. But Seek(5) implies that these
  // keys are all covered by some range deletion, so the next Seek(3) will also
  // do Seek(5) internally, so the blocks from 3-4 could be safely unpinned.

  // advance to the right prepared range
  while (
      multi_scan_->next_scan_idx <
          multi_scan_->block_index_ranges_per_scan.size() &&
      (user_comparator_.CompareWithoutTimestamp(
           *user_seek_target, /*a_has_ts=*/true,
           multi_scan_->scan_opts->GetScanRanges()[multi_scan_->next_scan_idx]
               .range.start.value(),
           /*b_has_ts=*/false) >= 0)) {
    multi_scan_->next_scan_idx++;
  }

  // next_scan_idx is guaranteed to be higher than 0. If the seek key is before
  // the start key of first prepared range, it is already handled by caller
  // SeekMultiScan. It is equal, it would not call this funciton. If it is
  // after, next_scan_idx would be advanced by the loop above.
  assert(multi_scan_->next_scan_idx > 0);
  // Get the current range
  auto cur_scan_idx = multi_scan_->next_scan_idx - 1;
  auto [cur_scan_start_idx, cur_scan_end_idx] =
      multi_scan_->block_index_ranges_per_scan[cur_scan_idx];

  if (cur_scan_start_idx >= cur_scan_end_idx) {
    // No blocks are prepared for this range at current file.
    MarkPreparedRangeExhausted();
    return;
  }

  // Unpin all the blocks from multi_scan_->cur_data_block_idx to
  // cur_scan_start_idx - these are wasted (prefetched but skipped)
  for (auto unpin_block_idx = multi_scan_->cur_data_block_idx;
       unpin_block_idx < cur_scan_start_idx; unpin_block_idx++) {
    // Count as wasted if it was prefetched
    if (unpin_block_idx < multi_scan_->prefetch_max_idx) {
      multi_scan_->wasted_blocks_count++;
    }
    multi_scan_->read_set->ReleaseBlock(unpin_block_idx);
  }

  // Take the max here to ensure we don't move backwards.
  size_t block_idx =
      std::max(cur_scan_start_idx, multi_scan_->cur_data_block_idx);
  auto const& data_block_separators = multi_scan_->data_block_separators;
  while (block_idx < data_block_separators.size() &&
         (user_comparator_.CompareWithoutTimestamp(
              *user_seek_target, /*a_has_ts=*/true,
              data_block_separators[block_idx],
              /*b_has_ts=*/false) > 0)) {
    // Unpin the blocks that are passed - count as wasted if prefetched
    if (block_idx < multi_scan_->prefetch_max_idx) {
      multi_scan_->wasted_blocks_count++;
    }
    multi_scan_->read_set->ReleaseBlock(block_idx);
    block_idx++;
  }

  if (block_idx >= data_block_separators.size()) {
    // All of the prepared blocks for this file is exhausted.
    MarkPreparedRangeExhausted();
    return;
  }

  // The current block may contain the data for the target key
  MultiScanSeekTargetFromBlock(seek_target, block_idx);
}

void BlockBasedTableIterator::MultiScanSeekTargetFromBlock(
    const Slice* seek_target, size_t block_idx) {
  assert(multi_scan_->cur_data_block_idx <= block_idx);

  if (!block_iter_points_to_real_block_ ||
      multi_scan_->cur_data_block_idx != block_idx) {
    if (block_iter_points_to_real_block_) {
      // Should be scan in increasing key range.
      // All blocks before cur_data_block_idx_ are not pinned anymore.
      assert(multi_scan_->cur_data_block_idx < block_idx);
    }

    ResetDataIter();

    if (MultiScanLoadDataBlock(block_idx)) {
      return;
    }
  }

  // Move current data block index forward until block_idx, meantime, unpin all
  // the blocks in between - these are wasted (prefetched but skipped)
  while (multi_scan_->cur_data_block_idx < block_idx) {
    // Count as wasted if it was prefetched
    if (multi_scan_->cur_data_block_idx < multi_scan_->prefetch_max_idx) {
      multi_scan_->wasted_blocks_count++;
    }
    multi_scan_->read_set->ReleaseBlock(multi_scan_->cur_data_block_idx);
    multi_scan_->cur_data_block_idx++;
  }
  block_iter_points_to_real_block_ = true;
  block_iter_.Seek(*seek_target);
  FindKeyForward();
  CheckOutOfBound();
}

void BlockBasedTableIterator::FindBlockForwardInMultiScan() {
  assert(multi_scan_);
  assert(multi_scan_->next_scan_idx >= 1);
  const auto cur_scan_end_idx = std::get<1>(
      multi_scan_->block_index_ranges_per_scan[multi_scan_->next_scan_idx - 1]);
  do {
    if (!block_iter_.status().ok()) {
      return;
    }

    // If is_out_of_bound_ is true, upper layer (LevelIterator) considers this
    // level has reached iterate_upper_bound_ and will not continue to iterate
    // into the next file. When we are doing the last scan within a MultiScan
    // for this file, it may need to continue to scan into the next file, so
    // we do not set is_out_of_bound_ in this case.
    if (multi_scan_->cur_data_block_idx + 1 >= cur_scan_end_idx) {
      MarkPreparedRangeExhausted();
      return;
    }
    // Move to the next pinned data block
    ResetDataIter();
    // Unpin previous block via ReadSet
    multi_scan_->read_set->ReleaseBlock(multi_scan_->cur_data_block_idx);
    ++multi_scan_->cur_data_block_idx;

    if (MultiScanLoadDataBlock(multi_scan_->cur_data_block_idx)) {
      return;
    }

    block_iter_points_to_real_block_ = true;
    block_iter_.SeekToFirst();
  } while (!block_iter_.Valid());
}

constexpr auto kVerbose = false;

Status BlockBasedTableIterator::CollectBlockHandles(
    const std::vector<ScanOptions>& scan_opts,
    std::vector<BlockHandle>* scan_block_handles,
    std::vector<std::tuple<size_t, size_t>>* block_index_ranges_per_scan,
    std::vector<std::string>* data_block_separators) {
  // print file name and level
  if (UNLIKELY(kVerbose)) {
    auto file_name = table_->get_rep()->file->file_name();
    auto level = table_->get_rep()->level;
    printf("file name : %s, level %d\n", file_name.c_str(), level);
  }
  for (const auto& scan_opt : scan_opts) {
    size_t num_blocks = 0;
    bool check_overlap = !scan_block_handles->empty();

    InternalKey start_key;
    const size_t timestamp_size =
        user_comparator_.user_comparator()->timestamp_size();
    if (timestamp_size == 0) {
      start_key = InternalKey(scan_opt.range.start.value(), kMaxSequenceNumber,
                              kValueTypeForSeek);
    } else {
      std::string seek_key;
      AppendKeyWithMaxTimestamp(&seek_key, scan_opt.range.start.value(),
                                timestamp_size);
      start_key = InternalKey(seek_key, kMaxSequenceNumber, kValueTypeForSeek);
    }
    index_iter_->Seek(start_key.Encode());
    while (index_iter_->status().ok() && index_iter_->Valid() &&
           (!scan_opt.range.limit.has_value() ||
            user_comparator_.CompareWithoutTimestamp(index_iter_->user_key(),
                                                     /*a_has_ts*/ true,
                                                     *scan_opt.range.limit,
                                                     /*b_has_ts=*/false) < 0)) {
      // Only add the block if the index separator is smaller than limit. When
      // they are equal or larger, it will be handled later below.
      if (check_overlap &&
          scan_block_handles->back() == index_iter_->value().handle) {
        // Skip the current block since it's already in the list
      } else {
        scan_block_handles->push_back(index_iter_->value().handle);
        // clone the Slice to avoid the lifetime issue
        data_block_separators->push_back(index_iter_->user_key().ToString());
      }
      ++num_blocks;
      index_iter_->Next();
      check_overlap = false;
    }

    if (!index_iter_->status().ok()) {
      // Abort: index iterator error
      return index_iter_->status();
    }

    if (index_iter_->Valid()) {
      // Handle the last block when its separator is equal or larger than limit
      if (check_overlap &&
          scan_block_handles->back() == index_iter_->value().handle) {
        // Skip adding the current block since it's already in the list
      } else {
        scan_block_handles->push_back(index_iter_->value().handle);
        data_block_separators->push_back(index_iter_->user_key().ToString());
      }
      ++num_blocks;
    }
    block_index_ranges_per_scan->emplace_back(
        scan_block_handles->size() - num_blocks, scan_block_handles->size());
    if (UNLIKELY(kVerbose)) {
      printf("separators :");
      for (const auto& separator : *data_block_separators) {
        printf("%s, ", separator.c_str());
      }
      printf("\nblock_index_ranges_per_scan :");
      for (auto const& block_index_range : *block_index_ranges_per_scan) {
        printf("[%zu, %zu], ", std::get<0>(block_index_range),
               std::get<1>(block_index_range));
      }
      printf("\n");
    }
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
