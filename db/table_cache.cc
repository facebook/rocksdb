//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/dbformat.h"
#include "db/table_cache_request.h"
#include "db/version_edit.h"


#include "monitoring/perf_context_imp.h"
#include "rocksdb/statistics.h"

#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

namespace rocksdb {
namespace table_cache_detail {
  void UnrefEntry(void* arg1, void* arg2) {
    Cache* cache = reinterpret_cast<Cache*>(arg1);
    Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
    cache->Release(h);
  }

  void DeleteTableReader(void* arg1, void* arg2) {
    TableReader* table_reader = reinterpret_cast<TableReader*>(arg1);
    delete table_reader;
  }
} // namespace table_cache_detail

using namespace table_cache_detail;

TableCache::TableCache(const ImmutableCFOptions& ioptions,
                       const EnvOptions& env_options, Cache* const cache)
    : ioptions_(ioptions), env_options_(env_options), cache_(cache) {
  if (ioptions_.row_cache) {
    // If the same cache is shared by multiple instances, we need to
    // disambiguate its entries.
    PutVarint64(&row_cache_id_, ioptions_.row_cache->NewId());
  }
}

TableCache::~TableCache() {
}

TableReader* TableCache::GetTableReaderFromHandle(Cache::Handle* handle) const {
  return reinterpret_cast<TableReader*>(cache_->Value(handle));
}

void TableCache::ReleaseHandle(Cache::Handle* handle) {
  cache_->Release(handle);
}

Status TableCache::GetTableReader(
  const EnvOptions& env_options,
  const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
  bool sequential_mode, size_t readahead, bool record_read_stats,
  HistogramImpl* file_read_hist, std::unique_ptr<TableReader>* table_reader,
  bool skip_filters, int level, bool prefetch_index_and_filter_in_cache) {

  using namespace async;
  Status s;

  const TableCacheGetReaderHelper::Callback empty_cb;
  TableCacheGetReaderHelper gr_helper(ioptions_);
  s = gr_helper.GetTableReader(empty_cb, env_options,
    internal_comparator, fd,
    sequential_mode, readahead, record_read_stats,
    file_read_hist, table_reader,
    skip_filters, level, prefetch_index_and_filter_in_cache);

  s = gr_helper.OnGetReaderComplete(s);

  return s;
}


void TableCache::EraseHandle(const FileDescriptor& fd, Cache::Handle* handle) {
  ReleaseHandle(handle);
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  cache_->Erase(key);
}

Status TableCache::FindTable(
  const EnvOptions& env_options,
  const InternalKeyComparator& internal_comparator,
  const FileDescriptor& fd, Cache::Handle** handle,
  const bool no_io, bool record_read_stats,
  HistogramImpl* file_read_hist, bool skip_filters,
  int level,
  bool prefetch_index_and_filter_in_cache) {

  return async::TableCacheFindTableContext::Find(this, env_options,
         internal_comparator, fd, handle,
         no_io, record_read_stats, file_read_hist, skip_filters,
         level, prefetch_index_and_filter_in_cache);
}

Status TableCache::FindTable(
  const async::Callable<Status, const Status&, Cache::Handle*>& cb,
  const EnvOptions & env_options,
  const InternalKeyComparator & internal_comparator,
  const FileDescriptor & file_fd, Cache::Handle ** handle, const bool no_io,
  bool record_read_stats, HistogramImpl * file_read_hist, bool skip_filters,
  int level, bool prefetch_index_and_filter_in_cache) {
  return async::TableCacheFindTableContext::RequestFind(cb, this, env_options,
         internal_comparator, file_fd, handle,
         no_io, record_read_stats, file_read_hist, skip_filters,
         level, prefetch_index_and_filter_in_cache);
}

InternalIterator* TableCache::NewIterator(
    const ReadOptions& options, const EnvOptions& env_options,
    const InternalKeyComparator& icomparator, const FileDescriptor& fd,
    RangeDelAggregator* range_del_agg, TableReader** table_reader_ptr,
    HistogramImpl* file_read_hist, bool for_compaction, Arena* arena,
    bool skip_filters, int level) {

  InternalIterator* result = nullptr;
  async::TableCacheNewIteratorContext::Create(this,
             options, env_options, icomparator, fd, range_del_agg, &result,
             table_reader_ptr, file_read_hist, for_compaction, arena, skip_filters);
  return result;
}

Status TableCache::NewIterator(const NewIteratorCallback& cb, const ReadOptions& options,
                   const EnvOptions& eoptions, const InternalKeyComparator& internal_comparator,
                   const FileDescriptor& file_fd, RangeDelAggregator* range_del_agg,
                   InternalIterator** iterator, TableReader** table_reader_ptr,
                   HistogramImpl* file_read_hist, bool for_compaction, Arena* arena,
                   bool skip_filters, int level) {
  return async::TableCacheNewIteratorContext::RequestCreate(cb, this, options,
         eoptions,
         internal_comparator, file_fd, range_del_agg, iterator, table_reader_ptr,
         file_read_hist, for_compaction, arena, skip_filters, level);
}

Status TableCache::Get(const ReadOptions& options,
    const InternalKeyComparator& internal_comparator,
    const FileDescriptor& fd, const Slice& k,
    GetContext* get_context, HistogramImpl* file_read_hist,
    bool skip_filters, int level) {

  return async::TableCacheGetContext::Get(this, options, internal_comparator, fd,
                                          k, get_context, file_read_hist, skip_filters, level);
}

Status TableCache::GetTableProperties(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    std::shared_ptr<const TableProperties>* properties, bool no_io) {

  return async::TableCacheGetPropertiesContext::GetProps(this, env_options,
    internal_comparator, fd, properties, no_io);
}

Status TableCache::GetTableProperties(async::Callable<Status, const Status&,
  std::shared_ptr<const TableProperties>&&>& cb,
  const EnvOptions& env_options,
  const InternalKeyComparator& internal_comparator,
  const FileDescriptor& fd,
  std::shared_ptr<const TableProperties>* properties,
  bool no_io) {
  return async::TableCacheGetPropertiesContext::RequestGetProps(cb, this, env_options,
    internal_comparator, fd, properties, no_io);
}


size_t TableCache::GetMemoryUsageByTableReader(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator,
    const FileDescriptor& fd) {
  Status s;
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    return table_reader->ApproximateMemoryUsage();
  }

  Cache::Handle* table_handle = nullptr;
  s = FindTable(env_options, internal_comparator, fd, &table_handle, true);
  if (!s.ok()) {
    return 0;
  }
  assert(table_handle);
  auto table = GetTableReaderFromHandle(table_handle);
  auto ret = table->ApproximateMemoryUsage();
  ReleaseHandle(table_handle);
  return ret;
}

void TableCache::Evict(Cache* cache, uint64_t file_number) {
  cache->Erase(GetSliceForFileNumber(&file_number));
}

}  // namespace rocksdb
