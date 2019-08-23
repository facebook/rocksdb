//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <cassert>
#include "table/block_based/cachable_entry.h"
#include "table/format.h"

namespace rocksdb {

class BlockBasedTable;
struct BlockCacheLookupContext;
class FilePrefetchBuffer;
class GetContext;
struct ReadOptions;
struct UncompressionDict;

// Provides access to the uncompression dictionary regardless of whether
// it is owned by the reader or stored in the cache, or whether it is pinned
// in the cache or not.
class UncompressionDictReader {
 public:
  static Status Create(
      const BlockBasedTable* table, FilePrefetchBuffer* prefetch_buffer,
      bool use_cache, bool prefetch, bool pin,
      BlockCacheLookupContext* lookup_context,
      std::unique_ptr<UncompressionDictReader>* uncompression_dict_reader);

  Status GetOrReadUncompressionDictionary(
      FilePrefetchBuffer* prefetch_buffer, bool no_io, GetContext* get_context,
      BlockCacheLookupContext* lookup_context,
      CachableEntry<UncompressionDict>* uncompression_dict) const;

  size_t ApproximateMemoryUsage() const;

 private:
  UncompressionDictReader(const BlockBasedTable* t,
                          CachableEntry<UncompressionDict>&& uncompression_dict)
      : table_(t), uncompression_dict_(std::move(uncompression_dict)) {
    assert(table_);
  }

  bool cache_dictionary_blocks() const;

  static Status ReadUncompressionDictionary(
      const BlockBasedTable* table, FilePrefetchBuffer* prefetch_buffer,
      const ReadOptions& read_options, bool use_cache, GetContext* get_context,
      BlockCacheLookupContext* lookup_context,
      CachableEntry<UncompressionDict>* uncompression_dict);

  const BlockBasedTable* table_;
  CachableEntry<UncompressionDict> uncompression_dict_;
};

}  // namespace rocksdb
