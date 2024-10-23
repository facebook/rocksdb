//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cinttypes>
#include <list>
#include <string>
#include <unordered_map>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_builder.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {
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
      const InternalKeyComparator* comparator,
      const InternalKeySliceTransform* int_key_slice_transform,
      bool use_value_delta_encoding, const BlockBasedTableOptions& table_opt,
      size_t ts_sz, bool persist_user_defined_timestamps);

  // Index builder will construct a set of blocks which contain:
  //  1. One primary index block.
  //  2. (Optional) a set of metablocks that contains the metadata of the
  //     primary index.
  struct IndexBlocks {
    Slice index_block_contents;
    std::unordered_map<std::string, Slice> meta_blocks;
  };
  IndexBuilder(const InternalKeyComparator* comparator, size_t ts_sz,
               bool persist_user_defined_timestamps)
      : comparator_(comparator),
        ts_sz_(ts_sz),
        persist_user_defined_timestamps_(persist_user_defined_timestamps) {}

  virtual ~IndexBuilder() = default;

  // Add a new index entry to index block.
  //
  // To allow further optimization, we provide `last_key_in_current_block` and
  // `first_key_in_next_block`, based on which the specific implementation can
  // determine the best index key to be used for the index block.
  // Called before the OnKeyAdded() call for first_key_in_next_block.
  // @last_key_in_current_block: TODO lifetime details
  // @first_key_in_next_block: it will be nullptr if the entry being added is
  //                           the last one in the table
  // @separator_scratch: a scratch buffer to back a computed separator between
  //                     those, as needed. May be modified on each call.
  // @return: the key or separator stored in the index, which could be
  //          last_key_in_current_block or a computed separator backed by
  //          separator_scratch or last_key_in_current_block.
  // REQUIRES: Finish() has not yet been called.
  virtual Slice AddIndexEntry(const Slice& last_key_in_current_block,
                              const Slice* first_key_in_next_block,
                              const BlockHandle& block_handle,
                              std::string* separator_scratch) = 0;

  // This method will be called whenever a key is added. The subclasses may
  // override OnKeyAdded() if they need to collect additional information.
  virtual void OnKeyAdded(const Slice& /*key*/) {}

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

  // Get the size for index block. Must be called after ::Finish.
  virtual size_t IndexSize() const = 0;

  virtual bool seperator_is_key_plus_seq() { return true; }

 protected:
  // Given the last key in current block and the first key in the next block,
  // return true if internal key should be used as separator, false if user key
  // can be used as separator.
  inline bool ShouldUseKeyPlusSeqAsSeparator(
      const Slice& last_key_in_current_block,
      const Slice& first_key_in_next_block) {
    Slice l_user_key = ExtractUserKey(last_key_in_current_block);
    Slice r_user_key = ExtractUserKey(first_key_in_next_block);
    // If user defined timestamps are not persisted. All the user keys will
    // act like they have minimal timestamp. Only having user key is not
    // sufficient, even if they are different user keys for now, they have to be
    // different user keys without the timestamp part.
    return persist_user_defined_timestamps_
               ? comparator_->user_comparator()->Compare(l_user_key,
                                                         r_user_key) == 0
               : comparator_->user_comparator()->CompareWithoutTimestamp(
                     l_user_key, r_user_key) == 0;
  }

  const InternalKeyComparator* comparator_;
  // Size of user-defined timestamp in bytes.
  size_t ts_sz_;
  // Whether user-defined timestamp in the user key should be persisted when
  // creating index block. If this flag is false, user-defined timestamp will
  // be stripped from user key for each index entry, and the
  // `first_internal_key` in `IndexValue` if it's included.
  bool persist_user_defined_timestamps_;
  // Set after ::Finish is called
  size_t index_size_ = 0;
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
  ShortenedIndexBuilder(
      const InternalKeyComparator* comparator,
      const int index_block_restart_interval, const uint32_t format_version,
      const bool use_value_delta_encoding,
      BlockBasedTableOptions::IndexShorteningMode shortening_mode,
      bool include_first_key, size_t ts_sz,
      const bool persist_user_defined_timestamps)
      : IndexBuilder(comparator, ts_sz, persist_user_defined_timestamps),
        index_block_builder_(
            index_block_restart_interval, true /*use_delta_encoding*/,
            use_value_delta_encoding,
            BlockBasedTableOptions::kDataBlockBinarySearch /* index_type */,
            0.75 /* data_block_hash_table_util_ratio */, ts_sz,
            persist_user_defined_timestamps, false /* is_user_key */),
        index_block_builder_without_seq_(
            index_block_restart_interval, true /*use_delta_encoding*/,
            use_value_delta_encoding,
            BlockBasedTableOptions::kDataBlockBinarySearch /* index_type */,
            0.75 /* data_block_hash_table_util_ratio */, ts_sz,
            persist_user_defined_timestamps, true /* is_user_key */),
        use_value_delta_encoding_(use_value_delta_encoding),
        include_first_key_(include_first_key),
        shortening_mode_(shortening_mode) {
    // Making the default true will disable the feature for old versions
    seperator_is_key_plus_seq_ = (format_version <= 2);
  }

  void OnKeyAdded(const Slice& key) override {
    if (include_first_key_ && current_block_first_internal_key_.empty()) {
      current_block_first_internal_key_.assign(key.data(), key.size());
    }
  }

  Slice AddIndexEntry(const Slice& last_key_in_current_block,
                      const Slice* first_key_in_next_block,
                      const BlockHandle& block_handle,
                      std::string* separator_scratch) override {
    Slice separator;
    if (first_key_in_next_block != nullptr) {
      if (shortening_mode_ !=
          BlockBasedTableOptions::IndexShorteningMode::kNoShortening) {
        separator = FindShortestInternalKeySeparator(
            *comparator_->user_comparator(), last_key_in_current_block,
            *first_key_in_next_block, separator_scratch);
      } else {
        separator = last_key_in_current_block;
      }
      if (!seperator_is_key_plus_seq_ &&
          ShouldUseKeyPlusSeqAsSeparator(last_key_in_current_block,
                                         *first_key_in_next_block)) {
        seperator_is_key_plus_seq_ = true;
      }
    } else {
      if (shortening_mode_ == BlockBasedTableOptions::IndexShorteningMode::
                                  kShortenSeparatorsAndSuccessor) {
        separator = FindShortInternalKeySuccessor(
            *comparator_->user_comparator(), last_key_in_current_block,
            separator_scratch);
      } else {
        separator = last_key_in_current_block;
      }
    }

    assert(!include_first_key_ || !current_block_first_internal_key_.empty());
    // When UDT should not be persisted, the index block builders take care of
    // stripping UDT from the key, for the first internal key contained in the
    // IndexValue, we need to explicitly do the stripping here before passing
    // it to the block builders.
    std::string first_internal_key_buf;
    Slice first_internal_key = current_block_first_internal_key_;
    if (!current_block_first_internal_key_.empty() && ts_sz_ > 0 &&
        !persist_user_defined_timestamps_) {
      StripTimestampFromInternalKey(&first_internal_key_buf,
                                    current_block_first_internal_key_, ts_sz_);
      first_internal_key = first_internal_key_buf;
    }
    IndexValue entry(block_handle, first_internal_key);
    std::string encoded_entry;
    std::string delta_encoded_entry;
    entry.EncodeTo(&encoded_entry, include_first_key_, nullptr);
    if (use_value_delta_encoding_ && !last_encoded_handle_.IsNull()) {
      entry.EncodeTo(&delta_encoded_entry, include_first_key_,
                     &last_encoded_handle_);
    } else {
      // If it's the first block, or delta encoding is disabled,
      // BlockBuilder::Add() below won't use delta-encoded slice.
    }
    last_encoded_handle_ = block_handle;
    const Slice delta_encoded_entry_slice(delta_encoded_entry);

    // TODO(yuzhangyu): fix this when "FindShortInternalKeySuccessor"
    //  optimization is available.
    // Timestamp aware comparator currently doesn't provide override for
    // "FindShortInternalKeySuccessor" optimization. So the actual
    // last key in current block is used as the key for indexing the current
    // block. As a result, when UDTs should not be persisted, it's safe to strip
    // away the UDT from key in index block as data block does the same thing.
    // What are the implications if a "FindShortInternalKeySuccessor"
    // optimization is provided.
    index_block_builder_.Add(separator, encoded_entry,
                             &delta_encoded_entry_slice);
    if (!seperator_is_key_plus_seq_) {
      index_block_builder_without_seq_.Add(
          ExtractUserKey(separator), encoded_entry, &delta_encoded_entry_slice);
    }

    current_block_first_internal_key_.clear();
    return separator;
  }

  using IndexBuilder::Finish;
  Status Finish(IndexBlocks* index_blocks,
                const BlockHandle& /*last_partition_block_handle*/) override {
    if (seperator_is_key_plus_seq_) {
      index_blocks->index_block_contents = index_block_builder_.Finish();
    } else {
      index_blocks->index_block_contents =
          index_block_builder_without_seq_.Finish();
    }
    index_size_ = index_blocks->index_block_contents.size();
    return Status::OK();
  }

  size_t IndexSize() const override { return index_size_; }

  bool seperator_is_key_plus_seq() override {
    return seperator_is_key_plus_seq_;
  }

  // Changes *key to a short string >= *key.
  //
  static Slice FindShortestInternalKeySeparator(const Comparator& comparator,
                                                const Slice& start,
                                                const Slice& limit,
                                                std::string* scratch);

  static Slice FindShortInternalKeySuccessor(const Comparator& comparator,
                                             const Slice& key,
                                             std::string* scratch);

  friend class PartitionedIndexBuilder;

 private:
  BlockBuilder index_block_builder_;
  BlockBuilder index_block_builder_without_seq_;
  const bool use_value_delta_encoding_;
  bool seperator_is_key_plus_seq_;
  const bool include_first_key_;
  BlockBasedTableOptions::IndexShorteningMode shortening_mode_;
  BlockHandle last_encoded_handle_ = BlockHandle::NullBlockHandle();
  std::string current_block_first_internal_key_;
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
  HashIndexBuilder(const InternalKeyComparator* comparator,
                   const SliceTransform* hash_key_extractor,
                   int index_block_restart_interval, int format_version,
                   bool use_value_delta_encoding,
                   BlockBasedTableOptions::IndexShorteningMode shortening_mode,
                   size_t ts_sz, const bool persist_user_defined_timestamps)
      : IndexBuilder(comparator, ts_sz, persist_user_defined_timestamps),
        primary_index_builder_(comparator, index_block_restart_interval,
                               format_version, use_value_delta_encoding,
                               shortening_mode, /* include_first_key */ false,
                               ts_sz, persist_user_defined_timestamps),
        hash_key_extractor_(hash_key_extractor) {}

  Slice AddIndexEntry(const Slice& last_key_in_current_block,
                      const Slice* first_key_in_next_block,
                      const BlockHandle& block_handle,
                      std::string* separator_scratch) override {
    ++current_restart_index_;
    return primary_index_builder_.AddIndexEntry(
        last_key_in_current_block, first_key_in_next_block, block_handle,
        separator_scratch);
  }

  void OnKeyAdded(const Slice& key) override {
    auto key_prefix = hash_key_extractor_->Transform(key);
    bool is_first_entry = pending_block_num_ == 0;

    // Keys may share the prefix
    if (is_first_entry || pending_entry_prefix_ != key_prefix) {
      if (!is_first_entry) {
        FlushPendingPrefix();
      }

      // need a hard copy otherwise the underlying data changes all the time.
      // TODO(kailiu) std::to_string() is expensive. We may speed up can avoid
      // data copy.
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

  Status Finish(IndexBlocks* index_blocks,
                const BlockHandle& last_partition_block_handle) override {
    if (pending_block_num_ != 0) {
      FlushPendingPrefix();
    }
    Status s = primary_index_builder_.Finish(index_blocks,
                                             last_partition_block_handle);
    index_blocks->meta_blocks.insert(
        {kHashIndexPrefixesBlock.c_str(), prefix_block_});
    index_blocks->meta_blocks.insert(
        {kHashIndexPrefixesMetadataBlock.c_str(), prefix_meta_block_});
    return s;
  }

  size_t IndexSize() const override {
    return primary_index_builder_.IndexSize() + prefix_block_.size() +
           prefix_meta_block_.size();
  }

  bool seperator_is_key_plus_seq() override {
    return primary_index_builder_.seperator_is_key_plus_seq();
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
      const InternalKeyComparator* comparator, bool use_value_delta_encoding,
      const BlockBasedTableOptions& table_opt, size_t ts_sz,
      bool persist_user_defined_timestamps);

  PartitionedIndexBuilder(const InternalKeyComparator* comparator,
                          const BlockBasedTableOptions& table_opt,
                          bool use_value_delta_encoding, size_t ts_sz,
                          bool persist_user_defined_timestamps);

  Slice AddIndexEntry(const Slice& last_key_in_current_block,
                      const Slice* first_key_in_next_block,
                      const BlockHandle& block_handle,
                      std::string* separator_scratch) override;

  Status Finish(IndexBlocks* index_blocks,
                const BlockHandle& last_partition_block_handle) override;

  size_t IndexSize() const override { return index_size_; }
  size_t TopLevelIndexSize(uint64_t) const { return top_level_index_size_; }
  size_t NumPartitions() const;

  inline bool ShouldCutFilterBlock() {
    // Current policy is to align the partitions of index and filters
    if (cut_filter_block) {
      cut_filter_block = false;
      return true;
    }
    return false;
  }

  const std::string& GetPartitionKey() {
    static const std::string kEmptyKey;
    return entries_.empty() ? kEmptyKey : entries_.back().key;
  }

  // Called when an external entity (such as filter partition builder) request
  // cutting the next partition
  void RequestPartitionCut();

  bool seperator_is_key_plus_seq() override {
    return seperator_is_key_plus_seq_;
  }

  bool get_use_value_delta_encoding() const {
    return use_value_delta_encoding_;
  }

 private:
  // Set after ::Finish is called
  size_t top_level_index_size_ = 0;
  // Set after ::Finish is called
  size_t partition_cnt_ = 0;

  void MakeNewSubIndexBuilder();

  struct Entry {
    std::string key;
    std::unique_ptr<ShortenedIndexBuilder> value;
  };
  // List of partitioned indexes and their keys. Note that when
  // sub_index_builder_ is not null (during construction), there
  // will be a placeholder entry at the back of this list tracking
  // the possible key for that next entry.
  std::list<Entry> entries_;
  BlockBuilder index_block_builder_;              // top-level index builder
  BlockBuilder index_block_builder_without_seq_;  // same for user keys
  // the active partition index builder
  std::unique_ptr<ShortenedIndexBuilder> sub_index_builder_;
  // the last key in the active partition index builder
  std::unique_ptr<FlushBlockPolicy> flush_policy_;
  // true if Finish is called once but not complete yet.
  bool finishing_indexes = false;
  const BlockBasedTableOptions& table_opt_;
  bool seperator_is_key_plus_seq_;
  bool use_value_delta_encoding_;
  // true if an external entity (such as filter partition builder) request
  // cutting the next partition
  bool partition_cut_requested_ = true;
  // true if it should cut the next filter partition block
  bool cut_filter_block = false;
  BlockHandle last_encoded_handle_;
};
}  // namespace ROCKSDB_NAMESPACE
