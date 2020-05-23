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
//
// It is a base class for BlockBasedFilter and FullFilter.
// These two are both used in BlockBasedTable. The first one contain filter
// For a part of keys in sst file, the second contain filter for all keys
// in sst file.

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
#include "rocksdb/table.h"
#include "table/format.h"
#include "table/multiget_context.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

const uint64_t kNotValid = ULLONG_MAX;
class FilterPolicy;

class GetContext;
using MultiGetRange = MultiGetContext::Range;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock Add*)* Finish
//
// BlockBased/Full FilterBlock would be called in the same way.
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder() {}
  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  void operator=(const FilterBlockBuilder&) = delete;

  virtual ~FilterBlockBuilder() {}

  virtual bool IsBlockBased() = 0;                    // If is blockbased filter
  virtual void StartBlock(uint64_t block_offset) = 0;  // Start new block filter
  virtual void Add(const Slice& key) = 0;      // Add a key to current filter
  virtual size_t NumAdded() const = 0;         // Number of keys added
  Slice Finish() {                             // Generate Filter
    const BlockHandle empty_handle;
    Status dont_care_status;
    auto ret = Finish(empty_handle, &dont_care_status);
    assert(dont_care_status.ok());
    return ret;
  }
  virtual Slice Finish(const BlockHandle& tmp, Status* status) = 0;
};

// A FilterBlockReader is used to parse filter from SST table.
// KeyMayMatch and PrefixMayMatch would trigger filter checking
//
// BlockBased/Full FilterBlock would be called in the same way.
class FilterBlockReader {
 public:
  FilterBlockReader() = default;
  virtual ~FilterBlockReader() = default;

  FilterBlockReader(const FilterBlockReader&) = delete;
  FilterBlockReader& operator=(const FilterBlockReader&) = delete;

  virtual bool IsBlockBased() = 0;  // If is blockbased filter

  /**
   * If no_io is set, then it returns true if it cannot answer the query without
   * reading data from disk. This is used in PartitionedFilterBlockReader to
   * avoid reading partitions that are not in block cache already
   *
   * Normally filters are built on only the user keys and the InternalKey is not
   * needed for a query. The index in PartitionedFilterBlockReader however is
   * built upon InternalKey and must be provided via const_ikey_ptr when running
   * queries.
   */
  virtual bool KeyMayMatch(const Slice& key,
                           const SliceTransform* prefix_extractor,
                           uint64_t block_offset, const bool no_io,
                           const Slice* const const_ikey_ptr,
                           GetContext* get_context,
                           BlockCacheLookupContext* lookup_context) = 0;

  virtual void KeysMayMatch(MultiGetRange* range,
                            const SliceTransform* prefix_extractor,
                            uint64_t block_offset, const bool no_io,
                            BlockCacheLookupContext* lookup_context) {
    for (auto iter = range->begin(); iter != range->end(); ++iter) {
      const Slice ukey = iter->ukey;
      const Slice ikey = iter->ikey;
      GetContext* const get_context = iter->get_context;
      if (!KeyMayMatch(ukey, prefix_extractor, block_offset, no_io, &ikey,
                       get_context, lookup_context)) {
        range->SkipKey(iter);
      }
    }
  }

  /**
   * no_io and const_ikey_ptr here means the same as in KeyMayMatch
   */
  virtual bool PrefixMayMatch(const Slice& prefix,
                              const SliceTransform* prefix_extractor,
                              uint64_t block_offset, const bool no_io,
                              const Slice* const const_ikey_ptr,
                              GetContext* get_context,
                              BlockCacheLookupContext* lookup_context) = 0;

  virtual void PrefixesMayMatch(MultiGetRange* range,
                                const SliceTransform* prefix_extractor,
                                uint64_t block_offset, const bool no_io,
                                BlockCacheLookupContext* lookup_context) {
    for (auto iter = range->begin(); iter != range->end(); ++iter) {
      const Slice ukey = iter->ukey;
      const Slice ikey = iter->ikey;
      GetContext* const get_context = iter->get_context;
      if (prefix_extractor->InDomain(ukey) &&
          !PrefixMayMatch(prefix_extractor->Transform(ukey), prefix_extractor,
                          block_offset, no_io, &ikey, get_context,
                          lookup_context)) {
        range->SkipKey(iter);
      }
    }
  }

  virtual size_t ApproximateMemoryUsage() const = 0;

  // convert this object to a human readable form
  virtual std::string ToString() const {
    std::string error_msg("Unsupported filter \n");
    return error_msg;
  }

  virtual void CacheDependencies(bool /*pin*/) {}

  virtual bool RangeMayExist(const Slice* /*iterate_upper_bound*/,
                             const Slice& user_key,
                             const SliceTransform* prefix_extractor,
                             const Comparator* /*comparator*/,
                             const Slice* const const_ikey_ptr,
                             bool* filter_checked, bool need_upper_bound_check,
                             bool no_io,
                             BlockCacheLookupContext* lookup_context) {
    if (need_upper_bound_check) {
      return true;
    }
    *filter_checked = true;
    Slice prefix = prefix_extractor->Transform(user_key);
    return PrefixMayMatch(prefix, prefix_extractor, kNotValid, no_io,
                          const_ikey_ptr, /* get_context */ nullptr,
                          lookup_context);
  }
};

}  // namespace ROCKSDB_NAMESPACE
