//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <memory>
#include <stdint.h>
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table_stats.h"
#include "rocksdb/table.h"
#include "util/coding.h"

namespace rocksdb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;
class TableReader;
class FilterBlockReader;

using std::unique_ptr;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class BlockBasedTable : public TableReader {
 public:
  static const std::string kFilterBlockPrefix;
  static const std::string kStatsBlock;

  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table_reader" to the newly opened
  // table.  The client should delete "*table_reader" when no longer needed.
  // If there was an error while initializing the table, sets "*table_reader"
  // to nullptr and returns a non-ok status.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options& options,
                     const EnvOptions& soptions,
                     unique_ptr<RandomAccessFile>&& file,
                     uint64_t file_size,
                     unique_ptr<TableReader>* table_reader);

  bool PrefixMayMatch(const Slice& internal_prefix) override;

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  Iterator* NewIterator(const ReadOptions&) override;

  Status Get(
        const ReadOptions& readOptions,
        const Slice& key,
        void* handle_context,
        bool (*result_handler)(void* handle_context, const Slice& k,
                               const Slice& v, bool didIO),
        void (*mark_key_may_exist_handler)(void* handle_context) = nullptr)
    override;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice& key) override;

  // Returns true if the block for the specified key is in cache.
  // REQUIRES: key is in this table.
  bool TEST_KeyInCache(const ReadOptions& options, const Slice& key) override;

  // Set up the table for Compaction. Might change some parameters with
  // posix_fadvise
  void SetupForCompaction() override;

  TableStats& GetTableStats() override;

  ~BlockBasedTable();

 private:
  template <class TValue>
  struct CachableEntry;

  struct Rep;
  Rep* rep_;
  bool compaction_optimized_;

  static Iterator* BlockReader(void*, const ReadOptions&,
                               const EnvOptions& soptions, const Slice&,
                               bool for_compaction);

  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&,
                               bool* didIO, bool for_compaction = false);

  // if `no_io == true`, we will not try to read filter from sst file
  // if it is not cached yet.
  CachableEntry<FilterBlockReader> GetFilter(bool no_io = false) const;

  Iterator* IndexBlockReader(const ReadOptions& options) const;

  // Read the block, either from sst file or from cache. This method will try
  // to read from cache only when block_cache is set or ReadOption doesn't
  // explicitly prohibit storage IO.
  //
  // If the block is read from cache, the statistics for cache miss/hit of the
  // the given type of block will be updated. User can specify
  // `block_cache_miss_ticker` and `block_cache_hit_ticker` for the statistics
  // update.
  //
  // On success, the `result` parameter will be populated, which contains a
  // pointer to the block and its cache handle, which will be nullptr if it's
  // not read from the cache.
  static Status GetBlock(const BlockBasedTable* table,
                         const BlockHandle& handle,
                         const ReadOptions& options,
                         bool for_compaction,
                         Tickers block_cache_miss_ticker,
                         Tickers block_cache_hit_ticker,
                         bool* didIO,
                         CachableEntry<Block>* result);

  // Calls (*handle_result)(arg, ...) repeatedly, starting with the entry found
  // after a call to Seek(key), until handle_result returns false.
  // May not make such a call if filter policy says that key is not present.
  friend class TableCache;
  friend class BlockBasedTableBuilder;

  void ReadMeta(const Footer& footer);
  void ReadFilter(const Slice& filter_handle_value);
  static Status ReadStats(const Slice& handle_value, Rep* rep);

  // Read the meta block from sst.
  static Status ReadMetaBlock(
      Rep* rep,
      std::unique_ptr<Block>* meta_block,
      std::unique_ptr<Iterator>* iter);

  // Create the filter from the filter block.
  static FilterBlockReader* ReadFilter(
      const Slice& filter_handle_value,
      Rep* rep,
      size_t* filter_size = nullptr);

  // Read the table stats from stats block.
  static Status ReadStats(
      const Slice& handle_value, Rep* rep, TableStats* stats);

  static void SetupCacheKeyPrefix(Rep* rep);

  explicit BlockBasedTable(Rep* rep) :
      compaction_optimized_(false) {
    rep_ = rep;
  }
  // Generate a cache key prefix from the file
  static void GenerateCachePrefix(shared_ptr<Cache> cc,
    RandomAccessFile* file, char* buffer, size_t* size);
  static void GenerateCachePrefix(shared_ptr<Cache> cc,
    WritableFile* file, char* buffer, size_t* size);

  // The longest prefix of the cache key used to identify blocks.
  // For Posix files the unique ID is three varints.
  static const size_t kMaxCacheKeyPrefixSize = kMaxVarint64Length*3+1;

  // No copying allowed
  explicit BlockBasedTable(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;
};

struct BlockBasedTableStatsNames {
  static const std::string kDataSize;
  static const std::string kIndexSize;
  static const std::string kRawKeySize;
  static const std::string kRawValueSize;
  static const std::string kNumDataBlocks;
  static const std::string kNumEntries;
  static const std::string kFilterPolicy;
};

}  // namespace rocksdb
