//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "table/block_based/filter_block_reader_common.h"
#include "table/format.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

// A BlockBasedFilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to BlockBasedFilterBlockBuilder must match the regexp:
//      (StartBlock Add*)* Finish
class BlockBasedFilterBlockBuilder : public FilterBlockBuilder {
 public:
  BlockBasedFilterBlockBuilder(const SliceTransform* prefix_extractor,
                               const BlockBasedTableOptions& table_opt);
  // No copying allowed
  BlockBasedFilterBlockBuilder(const BlockBasedFilterBlockBuilder&) = delete;
  void operator=(const BlockBasedFilterBlockBuilder&) = delete;

  virtual bool IsBlockBased() override { return true; }
  virtual void StartBlock(uint64_t block_offset) override;
  virtual void Add(const Slice& key_without_ts) override;
  virtual bool IsEmpty() const override {
    return start_.empty() && filter_offsets_.empty();
  }
  virtual size_t EstimateEntriesAdded() override;
  virtual Slice Finish(const BlockHandle& tmp, Status* status) override;
  using FilterBlockBuilder::Finish;

 private:
  void AddKey(const Slice& key);
  void AddPrefix(const Slice& key);
  void GenerateFilter();

  // important: all of these might point to invalid addresses
  // at the time of destruction of this filter block. destructor
  // should NOT dereference them.
  const FilterPolicy* policy_;
  const SliceTransform* prefix_extractor_;
  bool whole_key_filtering_;

  size_t prev_prefix_start_;        // the position of the last appended prefix
                                    // to "entries_".
  size_t prev_prefix_size_;         // the length of the last appended prefix to
                                    // "entries_".
  std::string entries_;             // Flattened entry contents
  std::vector<size_t> start_;       // Starting index in entries_ of each entry
  std::string result_;              // Filter data computed so far
  std::vector<Slice> tmp_entries_;  // policy_->CreateFilter() argument
  std::vector<uint32_t> filter_offsets_;
  uint64_t total_added_in_built_;  // Total keys added to filters built so far
};

// A FilterBlockReader is used to parse filter from SST table.
// KeyMayMatch and PrefixMayMatch would trigger filter checking
class BlockBasedFilterBlockReader
    : public FilterBlockReaderCommon<BlockContents> {
 public:
  BlockBasedFilterBlockReader(const BlockBasedTable* t,
                              CachableEntry<BlockContents>&& filter_block);
  // No copying allowed
  BlockBasedFilterBlockReader(const BlockBasedFilterBlockReader&) = delete;
  void operator=(const BlockBasedFilterBlockReader&) = delete;

  static std::unique_ptr<FilterBlockReader> Create(
      const BlockBasedTable* table, const ReadOptions& ro,
      FilePrefetchBuffer* prefetch_buffer, bool use_cache, bool prefetch,
      bool pin, BlockCacheLookupContext* lookup_context);

  bool IsBlockBased() override { return true; }

  bool KeyMayMatch(const Slice& key, const SliceTransform* prefix_extractor,
                   uint64_t block_offset, const bool no_io,
                   const Slice* const const_ikey_ptr, GetContext* get_context,
                   BlockCacheLookupContext* lookup_context) override;
  bool PrefixMayMatch(const Slice& prefix,
                      const SliceTransform* prefix_extractor,
                      uint64_t block_offset, const bool no_io,
                      const Slice* const const_ikey_ptr,
                      GetContext* get_context,
                      BlockCacheLookupContext* lookup_context) override;
  size_t ApproximateMemoryUsage() const override;

  // convert this object to a human readable form
  std::string ToString() const override;

 private:
  static bool ParseFieldsFromBlock(const BlockContents& contents,
                                   const char** data, const char** offset,
                                   size_t* num, size_t* base_lg);

  bool MayMatch(const Slice& entry, uint64_t block_offset, bool no_io,
                GetContext* get_context,
                BlockCacheLookupContext* lookup_context) const;
};

}  // namespace ROCKSDB_NAMESPACE
