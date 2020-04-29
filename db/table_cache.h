//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#pragma once
#include <string>
#include <vector>
#include <stdint.h>

#include "db/dbformat.h"
#include "db/range_del_aggregator.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/table_reader.h"
#include "trace_replay/block_cache_tracer.h"

namespace ROCKSDB_NAMESPACE {

class Env;
class Arena;
struct FileDescriptor;
class GetContext;
class HistogramImpl;

// Manages caching for TableReader objects for a column family. The actual
// cache is allocated separately and passed to the constructor. TableCache
// wraps around the underlying SST file readers by providing Get(),
// MultiGet() and NewIterator() methods that hide the instantiation,
// caching and access to the TableReader. The main purpose of this is
// performance - by caching the TableReader, it avoids unnecessary file opens
// and object allocation and instantiation. One exception is compaction, where
// a new TableReader may be instantiated - see NewIterator() comments
//
// Another service provided by TableCache is managing the row cache - if the
// DB is configured with a row cache, and the lookup key is present in the row
// cache, lookup is very fast. The row cache is obtained from
// ioptions.row_cache
class TableCache {
 public:
  TableCache(const ImmutableCFOptions& ioptions,
             const FileOptions& storage_options, Cache* cache,
             BlockCacheTracer* const block_cache_tracer);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "table_reader_ptr"
  // is non-nullptr, also sets "*table_reader_ptr" to point to the Table object
  // underlying the returned iterator, or nullptr if no Table object underlies
  // the returned iterator.  The returned "*table_reader_ptr" object is owned
  // by the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  // @param range_del_agg If non-nullptr, adds range deletions to the
  //    aggregator. If an error occurs, returns it in a NewErrorInternalIterator
  // @param for_compaction If true, a new TableReader may be allocated (but
  //                       not cached), depending on the CF options
  // @param skip_filters Disables loading/accessing the filter block
  // @param level The level this table is at, -1 for "not set / don't know"
  InternalIterator* NewIterator(
      const ReadOptions& options, const FileOptions& toptions,
      const InternalKeyComparator& internal_comparator,
      const FileMetaData& file_meta, RangeDelAggregator* range_del_agg,
      const SliceTransform* prefix_extractor, TableReader** table_reader_ptr,
      HistogramImpl* file_read_hist, TableReaderCaller caller, Arena* arena,
      bool skip_filters, int level, const InternalKey* smallest_compaction_key,
      const InternalKey* largest_compaction_key, bool allow_unprepared_value);

  // If a seek to internal key "k" in specified file finds an entry,
  // call get_context->SaveValue() repeatedly until
  // it returns false. As a side effect, it will insert the TableReader
  // into the cache and potentially evict another entry
  // @param get_context Context for get operation. The result of the lookup
  //                    can be retrieved by calling get_context->State()
  // @param file_read_hist If non-nullptr, the file reader statistics are
  //                       recorded
  // @param skip_filters Disables loading/accessing the filter block
  // @param level The level this table is at, -1 for "not set / don't know"
  Status Get(const ReadOptions& options,
             const InternalKeyComparator& internal_comparator,
             const FileMetaData& file_meta, const Slice& k,
             GetContext* get_context,
             const SliceTransform* prefix_extractor = nullptr,
             HistogramImpl* file_read_hist = nullptr, bool skip_filters = false,
             int level = -1);

  // Return the range delete tombstone iterator of the file specified by
  // `file_meta`.
  Status GetRangeTombstoneIterator(
      const ReadOptions& options,
      const InternalKeyComparator& internal_comparator,
      const FileMetaData& file_meta,
      std::unique_ptr<FragmentedRangeTombstoneIterator>* out_iter);

  // If a seek to internal key "k" in specified file finds an entry,
  // call get_context->SaveValue() repeatedly until
  // it returns false. As a side effect, it will insert the TableReader
  // into the cache and potentially evict another entry
  // @param mget_range Pointer to the structure describing a batch of keys to
  //                   be looked up in this table file. The result is stored
  //                   in the embedded GetContext
  // @param skip_filters Disables loading/accessing the filter block
  // @param level The level this table is at, -1 for "not set / don't know"
  Status MultiGet(const ReadOptions& options,
                  const InternalKeyComparator& internal_comparator,
                  const FileMetaData& file_meta,
                  const MultiGetContext::Range* mget_range,
                  const SliceTransform* prefix_extractor = nullptr,
                  HistogramImpl* file_read_hist = nullptr,
                  bool skip_filters = false, int level = -1);

  // Evict any entry for the specified file number
  static void Evict(Cache* cache, uint64_t file_number);

  // Clean table handle and erase it from the table cache
  // Used in DB close, or the file is not live anymore.
  void EraseHandle(const FileDescriptor& fd, Cache::Handle* handle);

  // Find table reader
  // @param skip_filters Disables loading/accessing the filter block
  // @param level == -1 means not specified
  Status FindTable(const FileOptions& toptions,
                   const InternalKeyComparator& internal_comparator,
                   const FileDescriptor& file_fd, Cache::Handle**,
                   const SliceTransform* prefix_extractor = nullptr,
                   const bool no_io = false, bool record_read_stats = true,
                   HistogramImpl* file_read_hist = nullptr,
                   bool skip_filters = false, int level = -1,
                   bool prefetch_index_and_filter_in_cache = true);

  // Get TableReader from a cache handle.
  TableReader* GetTableReaderFromHandle(Cache::Handle* handle);

  // Get the table properties of a given table.
  // @no_io: indicates if we should load table to the cache if it is not present
  //         in table cache yet.
  // @returns: `properties` will be reset on success. Please note that we will
  //            return Status::Incomplete() if table is not present in cache and
  //            we set `no_io` to be true.
  Status GetTableProperties(const FileOptions& toptions,
                            const InternalKeyComparator& internal_comparator,
                            const FileDescriptor& file_meta,
                            std::shared_ptr<const TableProperties>* properties,
                            const SliceTransform* prefix_extractor = nullptr,
                            bool no_io = false);

  // Return total memory usage of the table reader of the file.
  // 0 if table reader of the file is not loaded.
  size_t GetMemoryUsageByTableReader(
      const FileOptions& toptions,
      const InternalKeyComparator& internal_comparator,
      const FileDescriptor& fd,
      const SliceTransform* prefix_extractor = nullptr);

  // Returns approximated offset of a key in a file represented by fd.
  uint64_t ApproximateOffsetOf(
      const Slice& key, const FileDescriptor& fd, TableReaderCaller caller,
      const InternalKeyComparator& internal_comparator,
      const SliceTransform* prefix_extractor = nullptr);

  // Returns approximated data size between start and end keys in a file
  // represented by fd (the start key must not be greater than the end key).
  uint64_t ApproximateSize(const Slice& start, const Slice& end,
                           const FileDescriptor& fd, TableReaderCaller caller,
                           const InternalKeyComparator& internal_comparator,
                           const SliceTransform* prefix_extractor = nullptr);

  // Release the handle from a cache
  void ReleaseHandle(Cache::Handle* handle);

  Cache* get_cache() const { return cache_; }

  // Capacity of the backing Cache that indicates inifinite TableCache capacity.
  // For example when max_open_files is -1 we set the backing Cache to this.
  static const int kInfiniteCapacity = 0x400000;

  // The tables opened with this TableCache will be immortal, i.e., their
  // lifetime is as long as that of the DB.
  void SetTablesAreImmortal() {
    if (cache_->GetCapacity() >= kInfiniteCapacity) {
      immortal_tables_ = true;
    }
  }

 private:
  // Build a table reader
  Status GetTableReader(const FileOptions& file_options,
                        const InternalKeyComparator& internal_comparator,
                        const FileDescriptor& fd, bool sequential_mode,
                        bool record_read_stats, HistogramImpl* file_read_hist,
                        std::unique_ptr<TableReader>* table_reader,
                        const SliceTransform* prefix_extractor = nullptr,
                        bool skip_filters = false, int level = -1,
                        bool prefetch_index_and_filter_in_cache = true);

  // Create a key prefix for looking up the row cache. The prefix is of the
  // format row_cache_id + fd_number + seq_no. Later, the user key can be
  // appended to form the full key
  void CreateRowCacheKeyPrefix(const ReadOptions& options,
                               const FileDescriptor& fd,
                               const Slice& internal_key,
                               GetContext* get_context, IterKey& row_cache_key);

  // Helper function to lookup the row cache for a key. It appends the
  // user key to row_cache_key at offset prefix_size
  bool GetFromRowCache(const Slice& user_key, IterKey& row_cache_key,
                       size_t prefix_size, GetContext* get_context);

  const ImmutableCFOptions& ioptions_;
  const FileOptions& file_options_;
  Cache* const cache_;
  std::string row_cache_id_;
  bool immortal_tables_;
  BlockCacheTracer* const block_cache_tracer_;
  Striped<port::Mutex, Slice> loader_mutex_;
};

}  // namespace ROCKSDB_NAMESPACE
