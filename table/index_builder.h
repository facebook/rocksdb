//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <assert.h>
#include <inttypes.h>

#include <list>
#include <string>
#include <unordered_map>

#include "rocksdb/comparator.h"
#include "table/block_based_table_factory.h"
#include "table/block_builder.h"
#include "table/format.h"

namespace rocksdb {
// The interface for building index.
// Instruction for adding a new concrete IndexBuilder:
//  1. Create a subclass instantiated from IndexBuilder.
//  2. Add a new entry associated with that subclass in TableOptions::IndexType.
//  3. Add a create function for the new subclass in CreateIndexBuilder.
// Note: we can devise more advanced design to simplify the process for adding
// new subclass, which will, on the other hand, increase the code complexity and
// catch unwanted attention from readers. Given that we won't add/change
// indexes frequently, it makes sense to just embrace a more straightforward
// design that just works.
class IndexBuilder {
 public:
  static IndexBuilder* CreateIndexBuilder(
      BlockBasedTableOptions::IndexType index_type,
      const rocksdb::InternalKeyComparator* comparator,
      const InternalKeySliceTransform* int_key_slice_transform,
      const BlockBasedTableOptions& table_opt);

  // Index builder will construct a set of blocks which contain:
  //  1. One primary index block.
  //  2. (Optional) a set of metablocks that contains the metadata of the
  //     primary index.
  struct IndexBlocks {
    Slice index_block_contents;
    std::unordered_map<std::string, Slice> meta_blocks;
  };
  explicit IndexBuilder(const InternalKeyComparator* comparator)
      : comparator_(comparator) {}

  virtual ~IndexBuilder() {}

  // Add a new index entry to index block.
  // To allow further optimization, we provide `last_key_in_current_block` and
  // `first_key_in_next_block`, based on which the specific implementation can
  // determine the best index key to be used for the index block.
  // @last_key_in_current_block: this parameter maybe overridden with the value
  //                             "substitute key".
  // @first_key_in_next_block: it will be nullptr if the entry being added is
  //                           the last one in the table
  //
  // REQUIRES: Finish() has not yet been called.
  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) = 0;

  // This method will be called whenever a key is added. The subclasses may
  // override OnKeyAdded() if they need to collect additional information.
  virtual void OnKeyAdded(const Slice& key) {}

  // Inform the index builder that all entries has been written. Block builder
  // may therefore perform any operation required for block finalization.
  //
  // REQUIRES: Finish() has not yet been called.
  inline Status Finish(IndexBlocks* index_blocks) {
    // Throw away the changes to last_partition_block_handle. It has no effect
    // on the first call to Finish anyway.
    BlockHandle last_partition_block_handle;
    return Finish(index_blocks, last_partition_block_handle);
  }

  // This override of Finish can be utilized to build the 2nd level index in
  // PartitionIndexBuilder.
  //
  // index_blocks will be filled with the resulting index data. If the return
  // value is Status::InComplete() then it means that the index is partitioned
  // and the callee should keep calling Finish until Status::OK() is returned.
  // In that case, last_partition_block_handle is pointer to the block written
  // with the result of the last call to Finish. This can be utilized to build
  // the second level index pointing to each block of partitioned indexes. The
  // last call to Finish() that returns Status::OK() populates index_blocks with
  // the 2nd level index content.
  virtual Status Finish(IndexBlocks* index_blocks,
                        const BlockHandle& last_partition_block_handle) = 0;

  // Get the estimated size for index block.
  virtual size_t EstimatedSize() const = 0;

 protected:
  const InternalKeyComparator* comparator_;
};

// This index builder builds space-efficient index block.
//
// Optimizations:
//  1. Made block's `block_restart_interval` to be 1, which will avoid linear
//     search when doing index lookup (can be disabled by setting
//     index_block_restart_interval).
//  2. Shorten the key length for index block. Other than honestly using the
//     last key in the data block as the index key, we instead find a shortest
//     substitute key that serves the same function.
class ShortenedIndexBuilder : public IndexBuilder {
 public:
  explicit ShortenedIndexBuilder(const InternalKeyComparator* comparator,
                                 int index_block_restart_interval)
      : IndexBuilder(comparator),
        index_block_builder_(index_block_restart_interval) {}

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override {
    if (first_key_in_next_block != nullptr) {
      comparator_->FindShortestSeparator(last_key_in_current_block,
                                         *first_key_in_next_block);
    } else {
      comparator_->FindShortSuccessor(last_key_in_current_block);
    }

    std::string handle_encoding;
    block_handle.EncodeTo(&handle_encoding);
    index_block_builder_.Add(*last_key_in_current_block, handle_encoding);
  }

  using IndexBuilder::Finish;
  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override {
    index_blocks->index_block_contents = index_block_builder_.Finish();
    return Status::OK();
  }

  virtual size_t EstimatedSize() const override {
    return index_block_builder_.CurrentSizeEstimate();
  }

  friend class PartitionedIndexBuilder;

 private:
  BlockBuilder index_block_builder_;
};

// HashIndexBuilder contains a binary-searchable primary index and the
// metadata for secondary hash index construction.
// The metadata for hash index consists two parts:
//  - a metablock that compactly contains a sequence of prefixes. All prefixes
//    are stored consectively without any metadata (like, prefix sizes) being
//    stored, which is kept in the other metablock.
//  - a metablock contains the metadata of the prefixes, including prefix size,
//    restart index and number of block it spans. The format looks like:
//
// +-----------------+---------------------------+---------------------+
// <=prefix 1
// | length: 4 bytes | restart interval: 4 bytes | num-blocks: 4 bytes |
// +-----------------+---------------------------+---------------------+
// <=prefix 2
// | length: 4 bytes | restart interval: 4 bytes | num-blocks: 4 bytes |
// +-----------------+---------------------------+---------------------+
// |                                                                   |
// | ....                                                              |
// |                                                                   |
// +-----------------+---------------------------+---------------------+
// <=prefix n
// | length: 4 bytes | restart interval: 4 bytes | num-blocks: 4 bytes |
// +-----------------+---------------------------+---------------------+
//
// The reason of separating these two metablocks is to enable the efficiently
// reuse the first metablock during hash index construction without unnecessary
// data copy or small heap allocations for prefixes.
class HashIndexBuilder : public IndexBuilder {
 public:
  explicit HashIndexBuilder(const InternalKeyComparator* comparator,
                            const SliceTransform* hash_key_extractor,
                            int index_block_restart_interval)
      : IndexBuilder(comparator),
        primary_index_builder_(comparator, index_block_restart_interval),
        hash_key_extractor_(hash_key_extractor) {}

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override {
    ++current_restart_index_;
    primary_index_builder_.AddIndexEntry(last_key_in_current_block,
                                         first_key_in_next_block, block_handle);
  }

  virtual void OnKeyAdded(const Slice& key) override {
    auto key_prefix = hash_key_extractor_->Transform(key);
    bool is_first_entry = pending_block_num_ == 0;

    // Keys may share the prefix
    if (is_first_entry || pending_entry_prefix_ != key_prefix) {
      if (!is_first_entry) {
        FlushPendingPrefix();
      }

      // need a hard copy otherwise the underlying data changes all the time.
      // TODO(kailiu) ToString() is expensive. We may speed up can avoid data
      // copy.
      pending_entry_prefix_ = key_prefix.ToString();
      pending_block_num_ = 1;
      pending_entry_index_ = static_cast<uint32_t>(current_restart_index_);
    } else {
      // entry number increments when keys share the prefix reside in
      // different data blocks.
      auto last_restart_index = pending_entry_index_ + pending_block_num_ - 1;
      assert(last_restart_index <= current_restart_index_);
      if (last_restart_index != current_restart_index_) {
        ++pending_block_num_;
      }
    }
  }

  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override {
    FlushPendingPrefix();
    primary_index_builder_.Finish(index_blocks, last_partition_block_handle);
    index_blocks->meta_blocks.insert(
        {kHashIndexPrefixesBlock.c_str(), prefix_block_});
    index_blocks->meta_blocks.insert(
        {kHashIndexPrefixesMetadataBlock.c_str(), prefix_meta_block_});
    return Status::OK();
  }

  virtual size_t EstimatedSize() const override {
    return primary_index_builder_.EstimatedSize() + prefix_block_.size() +
           prefix_meta_block_.size();
  }

 private:
  void FlushPendingPrefix() {
    prefix_block_.append(pending_entry_prefix_.data(),
                         pending_entry_prefix_.size());
    PutVarint32Varint32Varint32(
        &prefix_meta_block_,
        static_cast<uint32_t>(pending_entry_prefix_.size()),
        pending_entry_index_, pending_block_num_);
  }

  ShortenedIndexBuilder primary_index_builder_;
  const SliceTransform* hash_key_extractor_;

  // stores a sequence of prefixes
  std::string prefix_block_;
  // stores the metadata of prefixes
  std::string prefix_meta_block_;

  // The following 3 variables keeps unflushed prefix and its metadata.
  // The details of block_num and entry_index can be found in
  // "block_hash_index.{h,cc}"
  uint32_t pending_block_num_ = 0;
  uint32_t pending_entry_index_ = 0;
  std::string pending_entry_prefix_;

  uint64_t current_restart_index_ = 0;
};

/**
 * IndexBuilder for two-level indexing. Internally it creates a new index for
 * each partition and Finish then in order when Finish is called on it
 * continiously until Status::OK() is returned.
 *
 * The format on the disk would be I I I I I I IP where I is block containing a
 * partition of indexes built using ShortenedIndexBuilder and IP is a block
 * containing a secondary index on the partitions, built using
 * ShortenedIndexBuilder.
 */
class PartitionedIndexBuilder : public IndexBuilder {
 public:
  static PartitionedIndexBuilder* CreateIndexBuilder(
      const rocksdb::InternalKeyComparator* comparator,
      const BlockBasedTableOptions& table_opt);

  explicit PartitionedIndexBuilder(const InternalKeyComparator* comparator,
                                   const BlockBasedTableOptions& table_opt);

  virtual ~PartitionedIndexBuilder();

  virtual void AddIndexEntry(std::string* last_key_in_current_block,
                             const Slice* first_key_in_next_block,
                             const BlockHandle& block_handle) override;

  virtual Status Finish(
      IndexBlocks* index_blocks,
      const BlockHandle& last_partition_block_handle) override;

  virtual size_t EstimatedSize() const override;

  inline bool ShouldCutFilterBlock() {
    // Current policy is to align the partitions of index and filters
    if (cut_filter_block) {
      cut_filter_block = false;
      return true;
    }
    return false;
  }

  std::string& GetPartitionKey() { return sub_index_last_key_; }

 private:
  void MakeNewSubIndexBuilder();

  struct Entry {
    std::string key;
    std::unique_ptr<ShortenedIndexBuilder> value;
  };
  std::list<Entry> entries_;  // list of partitioned indexes and their keys
  BlockBuilder index_block_builder_;  // top-level index builder
  // the active partition index builder
  ShortenedIndexBuilder* sub_index_builder_;
  // the last key in the active partition index builder
  std::string sub_index_last_key_;
  std::unique_ptr<FlushBlockPolicy> flush_policy_;
  // true if Finish is called once but not complete yet.
  bool finishing_indexes = false;
  const BlockBasedTableOptions& table_opt_;
  // true if it should cut the next filter partition block
  bool cut_filter_block = false;
};
}  // namespace rocksdb
