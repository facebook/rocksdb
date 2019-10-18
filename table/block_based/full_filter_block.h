//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "table/block_based/filter_block_reader_common.h"
#include "table/block_based/parsed_full_filter_block.h"
#include "util/hash.h"

namespace rocksdb {

class FilterPolicy;
class FilterBitsBuilder;
class FilterBitsReader;

// A FullFilterBlockBuilder is used to construct a full filter for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
// The format of full filter block is:
// +----------------------------------------------------------------+
// |              full filter for all keys in sst file              |
// +----------------------------------------------------------------+
// The full filter can be very large. At the end of it, we put
// num_probes: how many hash functions are used in bloom filter
//
class FullFilterBlockBuilder : public FilterBlockBuilder {
 public:
  explicit FullFilterBlockBuilder(const SliceTransform* prefix_extractor,
                                  bool whole_key_filtering,
                                  FilterBitsBuilder* filter_bits_builder);
  // No copying allowed
  FullFilterBlockBuilder(const FullFilterBlockBuilder&) = delete;
  void operator=(const FullFilterBlockBuilder&) = delete;

  // bits_builder is created in filter_policy, it should be passed in here
  // directly. and be deleted here
  ~FullFilterBlockBuilder() {}

  virtual bool IsBlockBased() override { return false; }
  virtual void StartBlock(uint64_t /*block_offset*/) override {}
  virtual void Add(const Slice& key) override;
  virtual size_t NumAdded() const override { return num_added_; }
  virtual Slice Finish(const BlockHandle& tmp, Status* status) override;
  using FilterBlockBuilder::Finish;

 protected:
  virtual void AddKey(const Slice& key);
  std::unique_ptr<FilterBitsBuilder> filter_bits_builder_;
  virtual void Reset();
  void AddPrefix(const Slice& key);
  const SliceTransform* prefix_extractor() { return prefix_extractor_; }

 private:
  // important: all of these might point to invalid addresses
  // at the time of destruction of this filter block. destructor
  // should NOT dereference them.
  const SliceTransform* prefix_extractor_;
  bool whole_key_filtering_;
  bool last_whole_key_recorded_;
  std::string last_whole_key_str_;
  bool last_prefix_recorded_;
  std::string last_prefix_str_;

  uint32_t num_added_;
  std::unique_ptr<const char[]> filter_data_;

};

// A FilterBlockReader is used to parse filter from SST table.
// KeyMayMatch and PrefixMayMatch would trigger filter checking
class FullFilterBlockReader
    : public FilterBlockReaderCommon<ParsedFullFilterBlock> {
 public:
  FullFilterBlockReader(const BlockBasedTable* t,
                        CachableEntry<ParsedFullFilterBlock>&& filter_block);

  static std::unique_ptr<FilterBlockReader> Create(
      const BlockBasedTable* table, FilePrefetchBuffer* prefetch_buffer,
      bool use_cache, bool prefetch, bool pin,
      BlockCacheLookupContext* lookup_context);

  bool IsBlockBased() override { return false; }

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

  void KeysMayMatch(MultiGetRange* range,
                    const SliceTransform* prefix_extractor,
                    uint64_t block_offset, const bool no_io,
                    BlockCacheLookupContext* lookup_context) override;

  void PrefixesMayMatch(MultiGetRange* range,
                        const SliceTransform* prefix_extractor,
                        uint64_t block_offset, const bool no_io,
                        BlockCacheLookupContext* lookup_context) override;
  size_t ApproximateMemoryUsage() const override;
  bool RangeMayExist(const Slice* iterate_upper_bound, const Slice& user_key,
                     const SliceTransform* prefix_extractor,
                     const Comparator* comparator,
                     const Slice* const const_ikey_ptr, bool* filter_checked,
                     bool need_upper_bound_check,
                     BlockCacheLookupContext* lookup_context) override;

 private:
  bool MayMatch(const Slice& entry, bool no_io, GetContext* get_context,
                BlockCacheLookupContext* lookup_context) const;
  void MayMatch(MultiGetRange* range, bool no_io,
                const SliceTransform* prefix_extractor,
                BlockCacheLookupContext* lookup_context) const;
  bool IsFilterCompatible(const Slice* iterate_upper_bound, const Slice& prefix,
                          const Comparator* comparator) const;

 private:
  bool full_length_enabled_;
  size_t prefix_extractor_full_length_;
};

}  // namespace rocksdb
