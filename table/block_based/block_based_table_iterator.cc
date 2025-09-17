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
  if (multi_scan_) {
    if (SeekMultiScan(target)) {
      return;
    }
  }

  assert(!multi_scan_);

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
      // exisiting buffers untill this callback is made to prefetch additional
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

BlockBasedTableIterator::MultiScanState::~MultiScanState() {
  // Abort any pending async IO operations to prevent callback being called
  // after async read states are destructed.
  if (!async_states.empty()) {
    std::vector<void*> io_handles_to_abort;
    std::vector<AsyncReadState*> states_to_cleanup;

    // Collect all pending IO handles
    for (size_t i = 0; i < async_states.size(); ++i) {
      auto& async_read = async_states[i];

      if (async_read.io_handle != nullptr) {
        assert(!async_read.finished);
        io_handles_to_abort.push_back(async_read.io_handle);
        states_to_cleanup.push_back(&async_read);
      }
    }

    if (!io_handles_to_abort.empty()) {
      IOStatus abort_status = fs->AbortIO(io_handles_to_abort);
      if (!abort_status.ok()) {
#ifndef NDEBUG
        fprintf(stderr, "Error aborting async IO operations: %s\n",
                abort_status.ToString().c_str());
#endif
        assert(false);
      }
      (void)abort_status;  // Suppress unused variable warning
    }

    for (auto async_read : states_to_cleanup) {
      async_read->CleanUpIOHandle();
    }
  }
}

// Note:
// - Iterator should not be reused for multiple multiscans or mixing
// multiscan with regular iterator usage.
// - scan ranges should be non-overlapping, and have increasing start keys.
// If a scan range's limit is not set, then there should only be one scan range.
// - After Prepare(), the iterator expects Seek to be called on the start key
// of each ScanOption in order. If any other seek is done, the optimization here
// is aborted and fall back to vanilla iterator.
// FIXME: DBIter and MergingIterator may
// internally do Seek() on child iterators, e.g. due to
// ReadOptions::max_skippable_internal_keys or reseeking into range deletion
// end key. So these Seeks can cause iterator to fall back to normal
// (non-prepared) iterator and ignore the optimizations done in Prepare().
void BlockBasedTableIterator::Prepare(const MultiScanArgs* multiscan_opts) {
  index_iter_->Prepare(multiscan_opts);

  assert(!multi_scan_);
  if (multi_scan_) {
    multi_scan_.reset();
    return;
  }
  if (!ValidateScanOptions(multiscan_opts)) {
    return;
  }

  std::vector<BlockHandle> scan_block_handles;
  std::vector<std::tuple<size_t, size_t>> block_index_ranges_per_scan;
  const std::vector<ScanOptions>& scan_opts = multiscan_opts->GetScanRanges();
  if (!CollectBlockHandles(scan_opts, &scan_block_handles,
                           &block_index_ranges_per_scan)) {
    return;
  }

  // Pin already cached blocks, collect remaining blocks to read
  std::vector<size_t> block_indices_to_read;
  std::vector<CachableEntry<Block>> pinned_data_blocks_guard(
      scan_block_handles.size());
  size_t prefetched_max_idx;
  if (!FilterAndPinCachedBlocks(
          scan_block_handles, multiscan_opts, &block_indices_to_read,
          &pinned_data_blocks_guard, &prefetched_max_idx)) {
    return;
  }

  std::vector<AsyncReadState> async_states;
  // Maps from block index into async read request (index into async_states[])
  UnorderedMap<size_t, size_t> block_idx_to_readreq_idx;
  if (!block_indices_to_read.empty()) {
    std::vector<FSReadRequest> read_reqs;
    std::vector<std::vector<size_t>> coalesced_block_indices;
    PrepareIORequests(block_indices_to_read, scan_block_handles, multiscan_opts,
                      &read_reqs, &block_idx_to_readreq_idx,
                      &coalesced_block_indices);

    if (!ExecuteIO(scan_block_handles, multiscan_opts, coalesced_block_indices,
                   &read_reqs, &async_states, &pinned_data_blocks_guard)) {
      return;
    }
  }

  // Successful Prepare, init related states so the iterator reads from prepared
  // blocks.
  multi_scan_ = std::make_unique<MultiScanState>(
      table_->get_rep()->ioptions.env->GetFileSystem(), multiscan_opts,
      std::move(pinned_data_blocks_guard),
      std::move(block_index_ranges_per_scan),
      std::move(block_idx_to_readreq_idx), std::move(async_states),
      prefetched_max_idx);

  is_index_at_curr_block_ = false;
  block_iter_points_to_real_block_ = false;
}

bool BlockBasedTableIterator::SeekMultiScan(const Slice* target) {
  assert(multi_scan_);
  // This is a MultiScan and Preapre() has been called.
  //
  // Validate seek key with scan options
  if (multi_scan_->next_scan_idx >= multi_scan_->scan_opts->size()) {
    multi_scan_.reset();
  } else if (!target) {
    // start key must be set for multi-scan
    multi_scan_.reset();
  } else if (user_comparator_.CompareWithoutTimestamp(
                 ExtractUserKey(*target), /*a_has_ts=*/true,
                 multi_scan_->scan_opts
                     ->GetScanRanges()[multi_scan_->next_scan_idx]
                     .range.start.value(),
                 /*b_has_ts=*/false) != 0) {
    // Unexpected seek key
    multi_scan_.reset();
  } else {
    auto [cur_scan_start_idx, cur_scan_end_idx] =
        multi_scan_->block_index_ranges_per_scan[multi_scan_->next_scan_idx];
    // We should have the data block already loaded
    ++multi_scan_->next_scan_idx;
    if (cur_scan_start_idx >= cur_scan_end_idx) {
      is_out_of_bound_ = true;
      assert(!Valid());
      return true;
    } else {
      is_out_of_bound_ = false;
    }

    if (!block_iter_points_to_real_block_ ||
        multi_scan_->cur_data_block_idx != cur_scan_start_idx) {
      if (block_iter_points_to_real_block_) {
        // Should be scan in increasing key range.
        // All blocks before cur_data_block_idx_ are not pinned anymore.
        assert(multi_scan_->cur_data_block_idx < cur_scan_start_idx);
      }

      ResetDataIter();

      multi_scan_->cur_data_block_idx = cur_scan_start_idx;
      multi_scan_->status = MultiScanLoadDataBlock(cur_scan_start_idx);
      if (!multi_scan_->status.ok()) {
        assert(!Valid());
        assert(status() == multi_scan_->status);
        return true;
      }
    }
    multi_scan_->cur_data_block_idx = cur_scan_start_idx;
    block_iter_points_to_real_block_ = true;
    block_iter_.Seek(*target);
    FindKeyForward();
    return true;
  }

  // We are aborting MultiScan.
  ResetDataIter();
  assert(!is_index_at_curr_block_);
  assert(!block_iter_points_to_real_block_);
  return false;
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
      if (multi_scan_->next_scan_idx >=
          multi_scan_->block_index_ranges_per_scan.size()) {
        // We are done with this file, should let LevelIter advance to the next
        // file instead of ending the scan
        ResetDataIter();
        assert(!is_out_of_bound_);
        assert(!Valid());
        return;
      }
      // We don't ResetDataIter() here since next scan might be reading from
      // the same block. ResetDataIter() will free the underlying block cache
      // handle and we don't want the block to be unpinned.
      is_out_of_bound_ = true;
      assert(!Valid());
      return;
    }
    // Move to the next pinned data block
    ResetDataIter();
    ++multi_scan_->cur_data_block_idx;

    multi_scan_->status =
        MultiScanLoadDataBlock(multi_scan_->cur_data_block_idx);
    if (!multi_scan_->status.ok()) {
      assert(!Valid());
      assert(status() == multi_scan_->status);
      return;
    }

    block_iter_points_to_real_block_ = true;
    block_iter_.SeekToFirst();
  } while (!block_iter_.Valid());
}

Status BlockBasedTableIterator::PollForBlock(size_t idx) {
  assert(multi_scan_);
  const auto async_idx = multi_scan_->block_idx_to_readreq_idx.find(idx);
  if (async_idx == multi_scan_->block_idx_to_readreq_idx.end()) {
    // Did not require async read, should already be pinned.
    assert(multi_scan_->pinned_data_blocks[idx].GetValue());
    return Status::OK();
  }

  AsyncReadState& async_read = multi_scan_->async_states[async_idx->second];
  if (async_read.finished) {
    assert(async_read.io_handle == nullptr);
    assert(async_read.status.ok());
    return async_read.status;
  }

  {
    std::vector<void*> handles = {async_read.io_handle};
    Status poll_s =
        table_->get_rep()->ioptions.env->GetFileSystem()->Poll(handles, 1);
    if (!poll_s.ok()) {
      return poll_s;
    }
  }
  assert(async_read.status.ok());
  if (!async_read.status.ok()) {
    return async_read.status;
  }
  async_read.CleanUpIOHandle();

  // Initialize and pin blocks from async read result.
  for (size_t i = 0; i < async_read.blocks.size(); ++i) {
    const auto& block = async_read.blocks[i];

    Status s = CreateAndPinBlockFromBuffer(
        block, async_read.offset, async_read.result,
        multi_scan_->pinned_data_blocks[async_read.block_indices[i]]);

    if (!s.ok()) {
      return s;
    }
    assert(multi_scan_->pinned_data_blocks[async_read.block_indices[i]]
               .GetValue());
  }
  assert(multi_scan_->pinned_data_blocks[idx].GetValue());
  return Status::OK();
}

Status BlockBasedTableIterator::CreateAndPinBlockFromBuffer(
    const BlockHandle& block, uint64_t buffer_start_offset,
    const Slice& buffer_data, CachableEntry<Block>& pinned_block_entry) {
  // Get decompressor and handle dictionary loading
  UnownedPtr<Decompressor> decompressor = table_->get_rep()->decompressor.get();
  CachableEntry<DecompressorDict> cached_dict;

  if (table_->get_rep()->uncompression_dict_reader) {
    {
      Status s =
          table_->get_rep()
              ->uncompression_dict_reader->GetOrReadUncompressionDictionary(
                  /* prefetch_buffer= */ nullptr, read_options_,
                  /* get_context= */ nullptr, /* lookup_context= */ nullptr,
                  &cached_dict);
      if (!s.ok()) {
#ifndef NDEBUG
        fprintf(stdout, "Prepare dictionary loading failed with %s\n",
                s.ToString().c_str());
#endif
        return s;
      }
    }
    if (!cached_dict.GetValue()) {
#ifndef NDEBUG
      fprintf(stdout, "Success but no dictionary read\n");
#endif
      return Status::InvalidArgument("No dictionary found");
    }
    decompressor = cached_dict.GetValue()->decompressor_.get();
  }

  // Create block from buffer data
  const auto block_size_with_trailer =
      BlockBasedTable::BlockSizeWithTrailer(block);
  const auto block_offset_in_buffer = block.offset() - buffer_start_offset;

  CacheAllocationPtr data =
      AllocateBlock(block_size_with_trailer,
                    GetMemoryAllocator(table_->get_rep()->table_options));
  memcpy(data.get(), buffer_data.data() + block_offset_in_buffer,
         block_size_with_trailer);
  BlockContents tmp_contents(std::move(data), block.size());

#ifndef NDEBUG
  tmp_contents.has_trailer =
      table_->get_rep()->footer.GetBlockTrailerSize() > 0;
#endif

  return table_->CreateAndPinBlockInCache<Block_kData>(
      read_options_, block, decompressor, &tmp_contents,
      &pinned_block_entry.As<Block_kData>());
}

bool BlockBasedTableIterator::ValidateScanOptions(
    const MultiScanArgs* multiscan_opts) {
  if (multiscan_opts == nullptr || multiscan_opts->empty()) {
    return false;
  }

  const std::vector<ScanOptions>& scan_opts = multiscan_opts->GetScanRanges();
  const bool has_limit = scan_opts.front().range.limit.has_value();
  if (!has_limit && scan_opts.size() > 1) {
    // Abort: overlapping ranges
    return false;
  }

  for (size_t i = 0; i < scan_opts.size(); ++i) {
    const auto& scan_range = scan_opts[i].range;
    if (!scan_range.start.has_value()) {
      // Abort: no start key
      return false;
    }

    if (scan_range.limit.has_value()) {
      assert(user_comparator_.CompareWithoutTimestamp(
                 scan_range.start.value(), /*a_has_ts=*/false,
                 scan_range.limit.value(), /*b_has_ts=*/false) <= 0);
    }

    if (i > 0) {
      if (!scan_range.limit.has_value()) {
        // multiple no limit scan ranges
        return false;
      }

      const auto& last_end_key = scan_opts[i - 1].range.limit.value();
      if (user_comparator_.CompareWithoutTimestamp(
              scan_range.start.value(), /*a_has_ts=*/false, last_end_key,
              /*b_has_ts=*/false) < 0) {
        // Abort: overlapping ranges
        return false;
      }
    }
  }
  return true;
}

bool BlockBasedTableIterator::CollectBlockHandles(
    const std::vector<ScanOptions>& scan_opts,
    std::vector<BlockHandle>* scan_block_handles,
    std::vector<std::tuple<size_t, size_t>>* block_index_ranges_per_scan) {
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
            user_comparator_.CompareWithoutTimestamp(
                index_iter_->user_key(),
                /*a_has_ts*/ true, *scan_opt.range.limit,
                /*b_has_ts=*/false) <= 0)) {
      if (check_overlap &&
          scan_block_handles->back() == index_iter_->value().handle) {
        // Skip the current block since it's already in the list
      } else {
        scan_block_handles->push_back(index_iter_->value().handle);
      }
      ++num_blocks;
      index_iter_->Next();
      check_overlap = false;
    }

    if (!index_iter_->status().ok()) {
      // Abort: index iterator error
      return false;
    }

    if (index_iter_->Valid()) {
      if (check_overlap &&
          scan_block_handles->back() == index_iter_->value().handle) {
        // Skip adding the current block since it's already in the list
      } else {
        scan_block_handles->push_back(index_iter_->value().handle);
      }
      ++num_blocks;
    } else if (num_blocks == 0 && index_iter_->UpperBoundCheckResult() !=
                                      IterBoundCheck::kOutOfBound) {
      // We should not have scan ranges that are completely after the file's
      // range. This is important for FindBlockForwardInMultiScan() which only
      // lets the upper layer (LevelIterator) advance to the next SST file when
      // the last scan range is exhausted.
      return false;
    }
    block_index_ranges_per_scan->emplace_back(
        scan_block_handles->size() - num_blocks, scan_block_handles->size());
  }
  return true;
}

bool BlockBasedTableIterator::FilterAndPinCachedBlocks(
    const std::vector<BlockHandle>& scan_block_handles,
    const MultiScanArgs* multiscan_opts,
    std::vector<size_t>* block_indices_to_read,
    std::vector<CachableEntry<Block>>* pinned_data_blocks_guard,
    size_t* prefetched_max_idx) {
  uint64_t total_prefetch_size = 0;
  *prefetched_max_idx = scan_block_handles.size();

  for (size_t i = 0; i < scan_block_handles.size(); ++i) {
    const auto& data_block_handle = scan_block_handles[i];

    total_prefetch_size +=
        BlockBasedTable::BlockSizeWithTrailer(data_block_handle);
    if (multiscan_opts->max_prefetch_size > 0 &&
        total_prefetch_size > multiscan_opts->max_prefetch_size) {
      for (size_t j = i; j < scan_block_handles.size(); ++j) {
        assert((*pinned_data_blocks_guard)[j].IsEmpty());
      }
      *prefetched_max_idx = i;
      break;
    }

    Status s = table_->LookupAndPinBlocksInCache<Block_kData>(
        read_options_, data_block_handle,
        &(*pinned_data_blocks_guard)[i].As<Block_kData>());

    if (!s.ok()) {
      // Abort: block cache look up failed.
      return false;
    }
    if (!(*pinned_data_blocks_guard)[i].GetValue()) {
      // Block not in cache
      block_indices_to_read->emplace_back(i);
    }
  }
  return true;
}

void BlockBasedTableIterator::PrepareIORequests(
    const std::vector<size_t>& block_indices_to_read,
    const std::vector<BlockHandle>& scan_block_handles,
    const MultiScanArgs* multiscan_opts, std::vector<FSReadRequest>* read_reqs,
    UnorderedMap<size_t, size_t>* block_idx_to_readreq_idx,
    std::vector<std::vector<size_t>>* coalesced_block_indices) {
  assert(coalesced_block_indices->empty());
  coalesced_block_indices->resize(1);

  for (const auto& block_idx : block_indices_to_read) {
    if (!coalesced_block_indices->back().empty()) {
      // Check if we can coalesce.
      const auto& last_block_handle =
          scan_block_handles[coalesced_block_indices->back().back()];
      uint64_t last_block_end =
          last_block_handle.offset() +
          BlockBasedTable::BlockSizeWithTrailer(last_block_handle);
      uint64_t current_start = scan_block_handles[block_idx].offset();

      if (current_start >
          last_block_end + multiscan_opts->io_coalesce_threshold) {
        // new IO
        coalesced_block_indices->emplace_back();
      }
    }
    coalesced_block_indices->back().emplace_back(block_idx);
  }

  assert(read_reqs->empty());
  read_reqs->reserve(coalesced_block_indices->size());
  for (const auto& block_indices : *coalesced_block_indices) {
    assert(block_indices.size());
    const auto& first_block_handle = scan_block_handles[block_indices[0]];
    const auto& last_block_handle = scan_block_handles[block_indices.back()];

    const auto start_offset = first_block_handle.offset();
    const auto end_offset =
        last_block_handle.offset() +
        BlockBasedTable::BlockSizeWithTrailer(last_block_handle);
#ifndef NDEBUG
    // Debug print for failing the assertion below.
    if (start_offset >= end_offset) {
      fprintf(stderr, "scan_block_handles: ");
      for (const auto& block : scan_block_handles) {
        fprintf(stderr, "offset: %" PRIu64 ", size: %" PRIu64 "; ",
                block.offset(), block.size());
      }
      fprintf(stderr,
              "\nfirst block - offset: %" PRIu64 ", size: %" PRIu64 "\n",
              first_block_handle.offset(), first_block_handle.size());
      fprintf(stderr, "last block - offset: %" PRIu64 ", size: %" PRIu64 "\n",
              last_block_handle.offset(), last_block_handle.size());

      fprintf(stderr, "coalesced_block_indices: ");
      for (const auto& b : *coalesced_block_indices) {
        fprintf(stderr, "[");
        for (const auto& block_idx : b) {
          fprintf(stderr, "%zu ", block_idx);
        }
        fprintf(stderr, "] ");
      }
      fprintf(stderr, "\ncurrent blocks: ");
      for (const auto& block_idx : block_indices) {
        fprintf(stderr, "offset: %" PRIu64 ", size: %" PRIu64 "; ",
                scan_block_handles[block_idx].offset(),
                scan_block_handles[block_idx].size());
      }
      fprintf(stderr, "\n");
    }
#endif  // NDEBUG
    assert(end_offset > start_offset);

    read_reqs->emplace_back();
    read_reqs->back().offset = start_offset;
    read_reqs->back().len = end_offset - start_offset;

    if (multiscan_opts->use_async_io) {
      for (const auto& block_idx : block_indices) {
        (*block_idx_to_readreq_idx)[block_idx] = read_reqs->size() - 1;
      }
    }
  }
}

bool BlockBasedTableIterator::ExecuteIO(
    const std::vector<BlockHandle>& scan_block_handles,
    const MultiScanArgs* multiscan_opts,
    const std::vector<std::vector<size_t>>& coalesced_block_indices,
    std::vector<FSReadRequest>* read_reqs,
    std::vector<AsyncReadState>* async_states,
    std::vector<CachableEntry<Block>>* pinned_data_blocks_guard) {
  IOOptions io_opts;
  if (!table_->get_rep()->file->PrepareIOOptions(read_options_, io_opts).ok()) {
    // Abort: PrepareIOOptions failed
    return false;
  }
  const bool direct_io = table_->get_rep()->file->use_direct_io();

  if (multiscan_opts->use_async_io) {
    async_states->resize(read_reqs->size());
    for (size_t i = 0; i < read_reqs->size(); ++i) {
      auto& read_req = (*read_reqs)[i];
      auto& async_read = (*async_states)[i];

      async_read.finished = false;
      async_read.offset = read_req.offset;
      async_read.block_indices = coalesced_block_indices[i];
      for (const auto idx : coalesced_block_indices[i]) {
        async_read.blocks.emplace_back(scan_block_handles[idx]);
      }

      if (direct_io) {
        read_req.scratch = nullptr;
      } else {
        async_read.buf.reset(new char[read_req.len]);
        read_req.scratch = async_read.buf.get();
      }

      auto cb = std::bind(&BlockBasedTableIterator::PrepareReadAsyncCallBack,
                          this, std::placeholders::_1, std::placeholders::_2);
      // TODO: for mmap, io_handle will not be set but callback will already
      // be called.
      Status s = table_->get_rep()->file.get()->ReadAsync(
          read_req, io_opts, cb, &async_read, &async_read.io_handle,
          &async_read.del_fn, direct_io ? &async_read.aligned_buf : nullptr);
      if (!s.ok()) {
#ifndef NDEBUG
        fprintf(stderr, "ReadAsync failed with %s\n", s.ToString().c_str());
#endif
        assert(false);
        return false;
      }
      assert(async_read.io_handle);
      for (auto& req : *read_reqs) {
        if (!req.status.ok()) {
          assert(false);
          return false;
        }
      }
    }
  } else {
    // Synchronous IO using MultiRead
    std::unique_ptr<char[]> buf;

    if (direct_io) {
      for (auto& read_req : *read_reqs) {
        read_req.scratch = nullptr;
      }
    } else {
      // TODO: optimize if FSSupportedOps::kFSBuffer is supported.
      size_t total_len = 0;
      for (const auto& req : *read_reqs) {
        total_len += req.len;
      }
      buf.reset(new char[total_len]);
      size_t offset = 0;
      for (auto& read_req : *read_reqs) {
        read_req.scratch = buf.get() + offset;
        offset += read_req.len;
      }
    }

    AlignedBuf aligned_buf;
    Status s = table_->get_rep()->file->MultiRead(
        io_opts, read_reqs->data(), read_reqs->size(),
        direct_io ? &aligned_buf : nullptr);
    if (!s.ok()) {
      return false;
    }
    for (auto& req : *read_reqs) {
      if (!req.status.ok()) {
        return false;
      }
    }

    // Init blocks and pin them in block cache.
    assert(read_reqs->size() == coalesced_block_indices.size());
    for (size_t i = 0; i < coalesced_block_indices.size(); i++) {
      const auto& read_req = (*read_reqs)[i];
      for (const auto& block_idx : coalesced_block_indices[i]) {
        const auto& block = scan_block_handles[block_idx];

        assert((*pinned_data_blocks_guard)[block_idx].IsEmpty());
        s = CreateAndPinBlockFromBuffer(block, read_req.offset, read_req.result,
                                        (*pinned_data_blocks_guard)[block_idx]);
        if (!s.ok()) {
          assert(false);
          // Abort: failed to create and pin block in cache
          return false;
        }
        assert((*pinned_data_blocks_guard)[block_idx].GetValue());
      }
    }
  }
  return true;
}

}  // namespace ROCKSDB_NAMESPACE
