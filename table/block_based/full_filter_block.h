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

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "table/block_based/filter_block_reader_common.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/parsed_full_filter_block.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

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

  virtual void Add(const Slice& key_without_ts) override;
  virtual bool IsEmpty() const override { return !any_added_; }
  virtual size_t EstimateEntriesAdded() override;
  virtual Slice Finish(
      const BlockHandle& tmp, Status* status,
      std::unique_ptr<const char[]>* filter_data = nullptr) override;
  using FilterBlockBuilder::Finish;

  virtual void ResetFilterBitsBuilder() override {
    filter_bits_builder_.reset();
  }

  virtual Status MaybePostVerifyFilter(const Slice& filter_content) override {
    return filter_bits_builder_->MaybePostVerify(filter_content);
  }

 protected:
  virtual void AddKey(const Slice& key);
  std::unique_ptr<FilterBitsBuilder> filter_bits_builder_;
  virtual void Reset();
  void AddPrefix(const Slice& key);
  const SliceTransform* prefix_extractor() { return prefix_extractor_; }
  const std::string& last_prefix_str() const { return last_prefix_str_; }

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
  // Whether prefix_extractor_->InDomain(last_whole_key_) is true.
  // Used in partitioned filters so that the last prefix from the previous
  // filter partition will be added to the current partition if
  // last_key_in_domain_ is true, regardless of the current key.
  bool last_key_in_domain_;
  bool any_added_;
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
      const BlockBasedTable* table, const ReadOptions& ro,
      FilePrefetchBuffer* prefetch_buffer, bool use_cache, bool prefetch,
      bool pin, BlockCacheLookupContext* lookup_context);

  bool KeyMayMatch(const Slice& key, const bool no_io,
                   const Slice* const const_ikey_ptr, GetContext* get_context,
                   BlockCacheLookupContext* lookup_context,
                   Env::IOPriority rate_limiter_priority) override;

  bool PrefixMayMatch(const Slice& prefix, const bool no_io,
                      const Slice* const const_ikey_ptr,
                      GetContext* get_context,
                      BlockCacheLookupContext* lookup_context,
                      Env::IOPriority rate_limiter_priority) override;

  void KeysMayMatch(MultiGetRange* range, const bool no_io,
                    BlockCacheLookupContext* lookup_context,
                    Env::IOPriority rate_limiter_priority) override;
  // Used in partitioned filter code
  void KeysMayMatch2(MultiGetRange* range,
                     const SliceTransform* /*prefix_extractor*/,
                     const bool no_io, BlockCacheLookupContext* lookup_context,
                     Env::IOPriority rate_limiter_priority) {
    KeysMayMatch(range, no_io, lookup_context, rate_limiter_priority);
  }

  void PrefixesMayMatch(MultiGetRange* range,
                        const SliceTransform* prefix_extractor,
                        const bool no_io,
                        BlockCacheLookupContext* lookup_context,
                        Env::IOPriority rate_limiter_priority) override;
  size_t ApproximateMemoryUsage() const override;

 private:
  bool MayMatch(const Slice& entry, bool no_io, GetContext* get_context,
                BlockCacheLookupContext* lookup_context,
                Env::IOPriority rate_limiter_priority) const;
  void MayMatch(MultiGetRange* range, bool no_io,
                const SliceTransform* prefix_extractor,
                BlockCacheLookupContext* lookup_context,
                Env::IOPriority rate_limiter_priority) const;
};

}  // namespace ROCKSDB_NAMESPACE
