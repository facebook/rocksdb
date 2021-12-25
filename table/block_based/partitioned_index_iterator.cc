//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/partitioned_index_iterator.h"

namespace ROCKSDB_NAMESPACE {
void PartitionedIndexIterator::Seek(const Slice& target) { SeekImpl(&target); }

void PartitionedIndexIterator::SeekToFirst() { SeekImpl(nullptr); }

void PartitionedIndexIterator::SeekImpl(const Slice* target) {
  SavePrevIndexValue();

  if (target) {
    index_iter_->Seek(*target);
  } else {
    index_iter_->SeekToFirst();
  }

  if (!index_iter_->Valid()) {
    ResetPartitionedIndexIter();
    return;
  }

  InitPartitionedIndexBlock();

  if (target) {
    block_iter_.Seek(*target);
  } else {
    block_iter_.SeekToFirst();
  }
  FindKeyForward();

  // We could check upper bound here, but that would be too complicated
  // and checking index upper bound is less useful than for data blocks.

  if (target) {
    assert(!Valid() || (table_->get_rep()->index_key_includes_seq
                            ? (icomp_.Compare(*target, key()) <= 0)
                            : (user_comparator_.Compare(ExtractUserKey(*target),
                                                        key()) <= 0)));
  }
}

void PartitionedIndexIterator::SeekToLast() {
  SavePrevIndexValue();
  index_iter_->SeekToLast();
  if (!index_iter_->Valid()) {
    ResetPartitionedIndexIter();
    return;
  }
  InitPartitionedIndexBlock();
  block_iter_.SeekToLast();
  FindKeyBackward();
}

void PartitionedIndexIterator::Next() {
  assert(block_iter_points_to_real_block_);
  block_iter_.Next();
  FindKeyForward();
}

void PartitionedIndexIterator::Prev() {
  assert(block_iter_points_to_real_block_);
  block_iter_.Prev();

  FindKeyBackward();
}

void PartitionedIndexIterator::InitPartitionedIndexBlock() {
  BlockHandle partitioned_index_handle = index_iter_->value().handle;
  if (!block_iter_points_to_real_block_ ||
      partitioned_index_handle.offset() != prev_block_offset_ ||
      // if previous attempt of reading the block missed cache, try again
      block_iter_.status().IsIncomplete()) {
    if (block_iter_points_to_real_block_) {
      ResetPartitionedIndexIter();
    }
    auto* rep = table_->get_rep();
    bool is_for_compaction =
        lookup_context_.caller == TableReaderCaller::kCompaction;
    // Prefetch additional data for range scans (iterators).
    // Implicit auto readahead:
    //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
    // Explicit user requested readahead:
    //   Enabled from the very first IO when ReadOptions.readahead_size is set.
    block_prefetcher_.PrefetchIfNeeded(rep, partitioned_index_handle,
                                       read_options_.readahead_size,
                                       is_for_compaction);
    Status s;
    table_->NewDataBlockIterator<IndexBlockIter>(
        read_options_, partitioned_index_handle, &block_iter_,
        BlockType::kIndex,
        /*get_context=*/nullptr, &lookup_context_, s,
        block_prefetcher_.prefetch_buffer(),
        /*for_compaction=*/is_for_compaction);
    block_iter_points_to_real_block_ = true;
    // We could check upper bound here but it is complicated to reason about
    // upper bound in index iterator. On the other than, in large scans, index
    // iterators are moved much less frequently compared to data blocks. So
    // the upper bound check is skipped for simplicity.
  }
}

void PartitionedIndexIterator::FindKeyForward() {
  // This method's code is kept short to make it likely to be inlined.

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

void PartitionedIndexIterator::FindBlockForward() {
  // TODO the while loop inherits from two-level-iterator. We don't know
  // whether a block can be empty so it can be replaced by an "if".
  do {
    if (!block_iter_.status().ok()) {
      return;
    }
    ResetPartitionedIndexIter();
    index_iter_->Next();

    if (!index_iter_->Valid()) {
      return;
    }

    InitPartitionedIndexBlock();
    block_iter_.SeekToFirst();
  } while (!block_iter_.Valid());
}

void PartitionedIndexIterator::FindKeyBackward() {
  while (!block_iter_.Valid()) {
    if (!block_iter_.status().ok()) {
      return;
    }

    ResetPartitionedIndexIter();
    index_iter_->Prev();

    if (index_iter_->Valid()) {
      InitPartitionedIndexBlock();
      block_iter_.SeekToLast();
    } else {
      return;
    }
  }
}
}  // namespace ROCKSDB_NAMESPACE
