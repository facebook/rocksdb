//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//


#include "db/table_cache.h"
#include "db/table_cache_request.h"

#include "db/version_edit.h"

#include "rocksdb/env.h"

#include "table/get_context.h"
#include "table/table_builder.h"

#include "util/filename.h"
#include "util/file_reader_writer.h"
#include "util/sync_point.h"

namespace rocksdb {
namespace async {
////////////////////////////////////////////////////////////////////////
/// TableCacheGetReaderHelper
Status TableCacheGetReaderHelper::GetTableReader(
  const Callback& cb,
  const EnvOptions& env_options,
  const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
  bool sequential_mode, size_t readahead, bool record_read_stats,
  HistogramImpl* file_read_hist, std::unique_ptr<TableReader>* table_reader,
  bool skip_filters, int level, bool prefetch_index_and_filter_in_cache) {

  std::string fname =
    TableFileName(ioptions_.db_paths, fd.GetNumber(), fd.GetPathId());

  // Do not perform async IO on compaction
  EnvOptions ra_options(env_options);
  if (readahead > 0) {
    ra_options.use_async_reads = false;
  }

  std::unique_ptr<RandomAccessFile> file;
  Status s = ioptions_.env->NewRandomAccessFile(fname, &file, ra_options);

  RecordTick(ioptions_.statistics, NO_FILE_OPENS);
  if (s.ok()) {
    if (readahead > 0) {
      file = NewReadaheadRandomAccessFile(std::move(file), readahead);
    }
    if (!sequential_mode && ioptions_.advise_random_on_open) {
      file->Hint(RandomAccessFile::RANDOM);
    }

    sw_.start();

    std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(file), ioptions_.env,
                                 ioptions_.statistics, record_read_stats,
                                 file_read_hist));

    if (cb) {
      s = ioptions_.table_factory->NewTableReader(
            cb,
            TableReaderOptions(ioptions_, ra_options, internal_comparator,
                               skip_filters, level),
            std::move(file_reader), fd.GetFileSize(), table_reader,
            prefetch_index_and_filter_in_cache);
    } else {
      s = ioptions_.table_factory->NewTableReader(
            TableReaderOptions(ioptions_, ra_options, internal_comparator,
                               skip_filters, level),
            std::move(file_reader), fd.GetFileSize(), table_reader,
            prefetch_index_and_filter_in_cache);
    }
  }
  return s;

}

Status TableCacheGetReaderHelper::OnGetReaderComplete(const Status& s) {

  if (sw_.start_time() > 0) {
    sw_.elapsed();
  }

  TEST_SYNC_POINT("TableCache::GetTableReader:0");
  return s;
}

//////////////////////////////////////////////////////////////////////
// TableCacheFindTableHelper
Status TableCacheFindTableHelper::LookupCache(const FileDescriptor& fd,
    Cache* cache, Cache::Handle** handle, bool no_io) {

  // We will accumulate this metric in two additions if necessary
  PERF_TIMER_GUARD(find_table_nanos);
  Status s;
  uint64_t number = fd.GetNumber();
  Slice key = table_cache_detail::GetSliceForFileNumber(&number);
  *handle = cache->Lookup(key);

  if (*handle == nullptr) {
    if (no_io) {
      s = Status::Incomplete();
    } else {
      s = Status::NotFound();
    }
  }
  TEST_SYNC_POINT_CALLBACK("TableCache::FindTable:0",
                           const_cast<bool*>(&no_io));
  return s;
}

Status TableCacheFindTableHelper::OnGetReaderComplete(const Status& status,
    Cache::Handle** handle, std::unique_ptr<TableReader>& table_reader) {

  *handle = nullptr;
  Status s = gr_helper_.OnGetReaderComplete(status);
  if (!s.ok()) {
    assert(table_reader == nullptr);
    RecordTick(gr_helper_.GetIOptions().statistics, NO_FILE_ERRORS);
    // We do not cache error results so that if the error is transient,
    // or somebody repairs the file, we recover automatically.
  } else {
    using namespace table_cache_detail;
    Slice key = GetSliceForFileNumber(&file_number_);
    s = cache_->Insert(key, table_reader.get(), 1, &DeleteEntry<TableReader>,
                       handle);
    if (s.ok()) {
      // Release ownership of table reader.
      table_reader.release();
    }
  }
  PERF_TIMER_STOP(find_table_nanos);
  return s;
}

//////////////////////////////////////////////////////////////////////////////
/// TableCacheFindTableContext

Status TableCacheFindTableContext::Find(TableCache* table_cache,
                                        const EnvOptions& env_options,
                                        const InternalKeyComparator& internal_comparator,
                                        const FileDescriptor& file_fd, Cache::Handle** handle, const bool no_io,
                                        bool record_read_stats, HistogramImpl* file_read_hist, bool skip_filters,
                                        int level, bool prefetch_index_and_filter_in_cache) {

  assert(handle != nullptr);
  *handle = nullptr;

  Status s = TableCacheFindTableHelper::LookupCache(file_fd,
             table_cache->GetCache(), handle, no_io);

  if (s.IsNotFound()) {
    Callback empty_cb;
    TableCacheFindTableContext context(empty_cb, table_cache->GetIOptions(),
                                       file_fd.GetNumber(), table_cache->GetCache());

    TableCacheFindTableHelper::Callback empty_helper_cb;

    std::unique_ptr<TableReader> table_reader;
    s = context.ft_helper_.GetReader(empty_helper_cb,
      env_options, internal_comparator, file_fd,
      &table_reader, record_read_stats, file_read_hist, skip_filters,
      level, prefetch_index_and_filter_in_cache);

    s = context.OnFindReaderComplete(s, std::move(table_reader));
    if (s.ok()) {
      *handle = context.GetHandle();
    }
  }
  return s;
}

Status TableCacheFindTableContext::RequestFind(const Callback& cb,
    TableCache* table_cache, const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator,
    const FileDescriptor& file_fd, Cache::Handle** handle, const bool no_io,
    bool record_read_stats, HistogramImpl* file_read_hist, bool skip_filters,
    int level, bool prefetch_index_and_filter_in_cache) {

  assert(handle != nullptr);
  *handle = nullptr;

  Status s = TableCacheFindTableHelper::LookupCache(file_fd,
    table_cache->GetCache(), handle, no_io);

  if (s.IsNotFound()) {
    std::unique_ptr<TableCacheFindTableContext> context(new
        TableCacheFindTableContext(cb, table_cache->GetIOptions(),
                                   file_fd.GetNumber(), table_cache->GetCache()));

    CallableFactory<TableCacheFindTableContext, Status, const Status&,
                    std::unique_ptr<TableReader>&&> f(context.get());
    auto on_find_reader_cb =
      f.GetCallable<&TableCacheFindTableContext::OnFindReaderComplete>();

    std::unique_ptr<TableReader> table_reader;
    s = context->ft_helper_.GetReader(on_find_reader_cb,
      env_options, internal_comparator, file_fd,
      &table_reader, record_read_stats, file_read_hist, skip_filters,
      level, prefetch_index_and_filter_in_cache);

    if (s.IsIOPending()) {
      context.release();
    } else {
      s = context->OnFindReaderComplete(s, std::move(table_reader));
      if (s.ok()) {
        *handle = context->GetHandle();
      }
    }
  }
  return s;
}

Status TableCacheFindTableContext::OnFindReaderComplete(const Status& status,
    std::unique_ptr<TableReader>&& table_reader) {
  async(status);

  std::unique_ptr<TableReader> t(std::move(table_reader));
  Status s = ft_helper_.OnGetReaderComplete(status, &handle_, t);

  s.async(async());
  return OnComplete(s);
}

Status TableCacheFindTableContext::OnComplete(const Status& status) {
  if (cb_ && async()) {
    ROCKS_LOG_DEBUG(
      ft_helper_.GetIOptions().info_log,
      "TableCacheFindTableContext async completion: %s",
      status.ToString().c_str());

    Status s(status);
    s.async(true);

    auto handle = GetHandle();
    cb_.Invoke(s, handle);

    delete this;
    return s;
  }

  ROCKS_LOG_DEBUG(
    ft_helper_.GetIOptions().info_log,
    "TableCacheFindTableContext sync completion: %s",
    status.ToString().c_str());

  return status;
}

//////////////////////////////////////////////////////////////////////////
// TableCacheGetPropertiesContext
//

Status TableCacheGetPropertiesContext::GetProps(TableCache* table_cache,
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    std::shared_ptr<const TableProperties>* properties, bool no_io) {

  assert(properties);
  Status s = GetFromDescriptor(fd, properties);
  if (s.IsNotFound()) {
    Cache::Handle* table_reader_handle = nullptr;
    s = TableCacheFindTableHelper::LookupCache(fd, table_cache->GetCache(),
        &table_reader_handle, no_io);

    if (s.ok()) {
      GetPropertiesFromCacheHandle(table_cache, table_reader_handle, properties);
    } else if (s.IsNotFound()) {
      Callback empty_cb;
      TableCacheGetPropertiesContext context(empty_cb, table_cache,
           table_cache->GetIOptions(), fd.GetNumber(), table_cache->GetCache());

      TableCacheFindTableHelper::Callback empty_helper_cb;
      std::unique_ptr<TableReader> table_reader;
      s = context.ft_helper_.GetReader(empty_helper_cb,
        table_cache->GetEnvOptions(),
        internal_comparator, fd, &table_reader, true /* record_stats */,
        nullptr /* file_read_hist */, false /* skip_filters */,
        -1 /* level */,
        true /* prefetch_index_and_filter_in_cache */);

      s = context.OnFindReaderComplete(s, std::move(table_reader));

      if (s.ok()) {
        *properties = context.GetProperties();
      }
    }
  }
  return s;
}

Status TableCacheGetPropertiesContext::RequestGetProps(const Callback& cb,
    TableCache* table_cache, const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    std::shared_ptr<const TableProperties>* properties, bool no_io) {

  assert(properties);
  Status s = GetFromDescriptor(fd, properties);
  if (s.IsNotFound()) {

    Cache::Handle* table_reader_handle = nullptr;
    s = TableCacheFindTableHelper::LookupCache(fd, table_cache->GetCache(),
      &table_reader_handle, no_io);

    if (s.ok()) {
      GetPropertiesFromCacheHandle(table_cache, table_reader_handle, properties);
    } else if (s.IsNotFound()) {

      std::unique_ptr<TableCacheGetPropertiesContext> context(
        new TableCacheGetPropertiesContext(cb, table_cache,
               table_cache->GetIOptions(), fd.GetNumber(), table_cache->GetCache()));

      CallableFactory<TableCacheGetPropertiesContext, Status, const Status&,
                      std::unique_ptr<TableReader>&&> f(context.get());
      auto helper_cb =
        f.GetCallable<&TableCacheGetPropertiesContext::OnFindReaderComplete>();

      std::unique_ptr<TableReader> table_reader;
      s = context->ft_helper_.GetReader(helper_cb,
        table_cache->GetEnvOptions(),
        internal_comparator, fd, &table_reader, true /* record_stats */,
        nullptr /* file_read_hist */, false /* skip_filters */,
        -1 /* level */,
        true /* prefetch_index_and_filter_in_cache */);

      if (s.IsIOPending()) {
        context.release();
      } else {
        s = context->OnFindReaderComplete(s, std::move(table_reader));
        if (s.ok()) {
          *properties = context->GetProperties();
        }
      }
    }
  }
  return s;
}


Status TableCacheGetPropertiesContext::OnFindReaderComplete(const Status& status,
  std::unique_ptr<TableReader>&& table_reader) {
  async(status);

  Cache::Handle* table_handle = nullptr;
  std::unique_ptr<TableReader> t(std::move(table_reader));
  Status s = ft_helper_.OnGetReaderComplete(status, &table_handle, t);

  if (s.ok()) {
    GetPropertiesFromCacheHandle(table_cache_, table_handle, &props_);
  }

  s.async(async());
  return OnComplete(s);
}

Status TableCacheGetPropertiesContext::OnComplete(const Status& status) {

  if (cb_ && async()) {
    ROCKS_LOG_DEBUG(
      ft_helper_.GetIOptions().info_log,
      "TableCacheGetPropertiesContext async completion: %s",
      status.ToString().c_str());

    Status s(status);
    s.async(true);

    cb_.Invoke(s, std::move(props_));

    delete this;
    return s;
  }

  ROCKS_LOG_DEBUG(
    ft_helper_.GetIOptions().info_log,
    "TableCacheGetPropertiesContext sync completion: %s",
    status.ToString().c_str());

  return status;
}


//////////////////////////////////////////////////////////////////////////
/// TableCacheGetContext

inline
void TableCacheGetContext::ComputeCacheKey(const std::string& row_cache_id,
    const ReadOptions& options,
    uint64_t fd_number,
    const Slice& k, IterKey& row_cache_key) {

  auto user_key = ExtractUserKey(k);
  // We use the user key as cache key instead of the internal key,
  // otherwise the whole cache would be invalidated every time the
  // sequence key increases. However, to support caching snapshot
  // reads, we append the sequence number (incremented by 1 to
  // distinguish from 0) only in this case.
  uint64_t seq_no =
    options.snapshot == nullptr ? 0 : 1 + GetInternalKeySeqno(k);

  // Compute row cache key.
  row_cache_key.TrimAppend(row_cache_key.Size(), row_cache_id.data(),
                           row_cache_id.size());

  using namespace table_cache_detail;
  AppendVarint64(&row_cache_key, fd_number);
  AppendVarint64(&row_cache_key, seq_no);
  row_cache_key.TrimAppend(row_cache_key.Size(), user_key.data(),
                           user_key.size());
}

inline
void TableCacheGetContext::InsertRowCache(const ImmutableCFOptions& ioptions,
    const IterKey& row_cache_key, std::string&& row_cache_entry_buffer) {

  assert(row_cache_key.Size() > 0);
  assert(!row_cache_entry_buffer.empty());

  size_t charge =
    row_cache_key.Size() + row_cache_entry_buffer.size() + sizeof(std::string);
  void* row_ptr = new std::string(std::move(row_cache_entry_buffer));
  ioptions.row_cache->Insert(row_cache_key.GetUserKey(), row_ptr, charge,
                             &table_cache_detail::DeleteEntry<std::string>);
}

Status TableCacheGetContext::LookupRowCache(TableCache* table_cache,
    const ReadOptions& options, const FileDescriptor& fd, const Slice& k,
    GetContext* get_context, bool& raw_cache_enabled) {

  Status s(Status::NotSupported());
  raw_cache_enabled = false;

#ifndef ROCKSDB_LITE
  IterKey row_cache_key;
  using namespace table_cache_detail;
  const auto& ioptions = table_cache->GetIOptions();
  // Check row cache if enabled. Since row cache does not currently store
  // sequence numbers, we cannot use it if we need to fetch the sequence.
  if (ioptions.row_cache && !get_context->NeedToReadSequence()) {
    raw_cache_enabled = true;
    uint64_t fd_number = fd.GetNumber();
    ComputeCacheKey(table_cache->GetRowCacheId(), options, fd_number, k,
                    row_cache_key);

    auto user_key = ExtractUserKey(k);
    if (auto row_handle =
          ioptions.row_cache->Lookup(row_cache_key.GetUserKey())) {
      auto found_row_cache_entry = static_cast<const std::string*>(
                                     ioptions.row_cache->Value(row_handle));
      replayGetContextLog(*found_row_cache_entry, user_key, get_context);
      ioptions.row_cache->Release(row_handle);
      RecordTick(ioptions.statistics, ROW_CACHE_HIT);
      s = Status::OK();
    } else {
      // Not found, setting up the replay log.
      RecordTick(ioptions.statistics, ROW_CACHE_MISS);
      s = Status::NotFound();
    }
  }
#endif  // ROCKSDB_LITE

  return s;
}

Status TableCacheGetContext::LookupTableReader(TableCache* table_cache,
    const ReadOptions& options,
    const FileDescriptor& fd,
    Cache::Handle** table_reader_handle,
    TableReader** table_reader) {

  assert(table_reader_handle != nullptr);
  assert(table_reader != nullptr);

  Status s;
  *table_reader = fd.table_reader;
  *table_reader_handle = nullptr;

  if (*table_reader == nullptr) {
    s = TableCacheFindTableHelper::LookupCache(fd, table_cache->GetCache(),
        table_reader_handle,
        TableCacheGetContext::IsNoIo(options));

    if (s.ok() && *table_reader_handle != nullptr) {
      *table_reader = table_cache->GetTableReaderFromHandle(*table_reader_handle);
    }
  }

  return s;
}

Status TableCacheGetContext::CreateTableReader(const InternalKeyComparator&
    internal_comparator,
    const FileDescriptor& fd, HistogramImpl* file_read_hist, bool skip_filters,
    int level) {

  TableCacheFindTableHelper::Callback on_reader_create;

  if (cb_) {
    CallableFactory<TableCacheGetContext, Status, const Status&,
                    std::unique_ptr<TableReader>&&> f(this);
    on_reader_create =
      f.GetCallable<&TableCacheGetContext::OnCreateTableReader>();
  }

  // Receives a pointer in case of sync completion
  std::unique_ptr<TableReader> table_reader;
  Status s = ft_helper_.GetReader(on_reader_create,
                                  table_cache_->GetEnvOptions(),
                                  internal_comparator, fd, &table_reader, true /* record_stats */,
                                  file_read_hist, skip_filters,
                                  level,
                                  true /* prefetch_index_and_filter_in_cache */);

  if (!s.IsIOPending()) {
    s = OnCreateTableReader(s, std::move(table_reader));
  }

  return s;
}

Status TableCacheGetContext::OnCreateTableReader(const Status& status,
    std::unique_ptr<TableReader>&& table_reader) {
  async(status);

  Status s(status);

  assert(table_reader_handle_ == nullptr);
  std::unique_ptr<TableReader> t(std::move(table_reader));

  s = ft_helper_.OnGetReaderComplete(s, &table_reader_handle_, t);

  if (s.ok()) {
    assert(table_reader_handle_ != nullptr);
    auto table_reader = table_cache_->GetTableReaderFromHandle(table_reader_handle_);
    assert(table_reader != nullptr);

    if (s.ok() && get_context_->range_del_agg() != nullptr &&
        !options_->ignore_range_deletions) {
      CreateTombstoneIterator(table_reader);
    } else if (s.ok()) {
      return Get(table_reader);
    }
  }

  // Incomplete is handled at the cache lookup stage.
  // If we are here this means IO is allowed
  assert(!s.IsIncomplete());

  return OnComplete(s);
}

Status TableCacheGetContext::CreateTombstoneIterator(TableReader*
    table_reader) {

  Status s;
  InternalIterator* range_del_iter = nullptr;

  // Save for get to come next
  table_reader_ = table_reader;

  if (cb_) {
    CallableFactory<TableCacheGetContext, Status, const Status&, InternalIterator*>
    f(this);
    auto on_ts_iterator =
      f.GetCallable<&TableCacheGetContext::OnTombstoneIterator>();
    s = table_reader->NewRangeTombstoneIterator(on_ts_iterator, *options_,
        &range_del_iter);

    if (s.IsIOPending()) {
      return s;
    }
  } else {
    range_del_iter = table_reader->NewRangeTombstoneIterator(*options_);
  }

  s = OnTombstoneIterator(s, range_del_iter);

  return s;
}

Status TableCacheGetContext::OnTombstoneIterator(const Status& status,
    InternalIterator* iter) {

  async(status);
  Status s(status);

  std::unique_ptr<InternalIterator> range_del_iter(iter);

  if (range_del_iter) {
    s = range_del_iter->status();
    if (s.ok()) {
      s = get_context_->range_del_agg()->AddTombstones(
            std::move(range_del_iter));
    }
  }

  assert(table_reader_ != nullptr);
  return Get(table_reader_);
}

Status TableCacheGetContext::Get(TableReader* table_reader) {

  Status s;
  assert(table_reader != nullptr);

  // nullptr if no cache.
  get_context_->SetReplayLog((row_cache_present_) ? &row_cache_entry_buffer_ :
                             nullptr);

  if (cb_) {
    CallableFactory<TableCacheGetContext, Status, const Status&> f(this);
    auto on_get_complete = f.GetCallable<&TableCacheGetContext::OnGetComplete>();

    s = table_reader->Get(on_get_complete, *options_, k_, get_context_,
                          skip_filters_);
    if (s.IsIOPending()) {
      return s;
    }
  } else {
    s = table_reader->Get(*options_, k_, get_context_, skip_filters_);
  }

  return OnGetComplete(s);
}

Status TableCacheGetContext::OnGetComplete(const Status& status) {
  async(status);

  Status s(status);
  get_context_->SetReplayLog(nullptr);

#ifndef ROCKSDB_LITE

  if (s.ok() && row_cache_present_ && !row_cache_entry_buffer_.empty()) {
    // Recompute the key as I had trouble storing it
    IterKey row_cache_key;
    ComputeCacheKey(table_cache_->GetRowCacheId(), *options_,
                    ft_helper_.GetFileNumber(), k_, row_cache_key);
    InsertRowCache(table_cache_->GetIOptions(),
                   row_cache_key, std::move(row_cache_entry_buffer_));
  }

#endif  // ROCKSDB_LITE

  return OnComplete(s);
}

Status TableCacheGetContext::OnComplete(const Status& status) {

  ReleaseCacheHandle();

  if (cb_ && async()) {
    ROCKS_LOG_DEBUG(
      table_cache_->GetIOptions().info_log,
      "TableCacheGetContext async completion: %s",
      status.ToString().c_str());

    Status s(status);
    s.async(true);
    cb_.Invoke(s);
    delete this;
    return s;
  }

  ROCKS_LOG_DEBUG(
    table_cache_->GetIOptions().info_log,
    "TableCacheGetContext sync completion: %s",
    status.ToString().c_str());

  return status;
}

Status TableCacheGetContext::Get(TableCache* table_cache,
                                 const ReadOptions& options, const InternalKeyComparator& internal_comparator,
                                 const FileDescriptor& fd, const Slice& k, GetContext* get_context,
                                 HistogramImpl* file_read_hist, bool skip_filters, int level) {

  bool row_cache_present = false;
  Status s = LookupRowCache(table_cache,
                            options, fd,
                            k, get_context,
                            row_cache_present);

  if (s.ok()) {
    return s;
  }

// Not supported is for ROCKSDB_LIGHT
  if (s.IsNotFound() || s.IsNotSupported()) {

    Cache::Handle* handle = nullptr;
    TableReader* table_reader = nullptr;

    s = LookupTableReader(table_cache, options, fd, &handle, &table_reader);

    if (s.ok() || s.IsNotFound()) {
      Callback empty_cb;
      TableCacheGetContext context(empty_cb, table_cache, options, k, get_context,
                                   skip_filters, fd.GetNumber(), handle, row_cache_present);
      if (s.ok()) {
        assert(table_reader != nullptr);
        // If we need to create tombstone iterator then
        // do it, otherwise proceed directly to get
        if (get_context->range_del_agg() != nullptr &&
            !options.ignore_range_deletions) {
          s = context.CreateTombstoneIterator(table_reader);
        } else {
          s = context.Get(table_reader);
        }
      } else {
        s = context.CreateTableReader(internal_comparator,
                                      fd, file_read_hist, skip_filters, level);
      }
    }
  } else if (s.IsIncomplete()) {
    // KeyMayExist() is called within LookupTableReader
    s = Status::OK();
  }
  return s;
}

Status TableCacheGetContext::RequestGet(const Callback& cb,
                                        TableCache* table_cache, const ReadOptions& options,
                                        const InternalKeyComparator& internal_comparator,
                                        const FileDescriptor& fd, const Slice& k, GetContext* get_context,
                                        HistogramImpl * file_read_hist, bool skip_filters, int level) {
  bool row_cache_present = false;
  Status s = LookupRowCache(table_cache,
    options, fd,
    k, get_context,
    row_cache_present);

  if (s.ok()) {
    return s;
  }

  // Not supported is for ROCKSDB_LIGHT
  if (s.IsNotFound() || s.IsNotSupported()) {

    Cache::Handle* handle = nullptr;
    TableReader* table_reader = nullptr;

    s = LookupTableReader(table_cache, options, fd, &handle, &table_reader);

    if (s.ok() || s.IsNotFound()) {
      std::unique_ptr<TableCacheGetContext> context(new TableCacheGetContext(cb,
          table_cache, options, k, get_context,
          skip_filters, fd.GetNumber(), handle, row_cache_present));
      if (s.ok()) {
        assert(table_reader != nullptr);
        // If we need to create tombstone iterator then
        // do it, otherwise proceed directly to get
        if (get_context->range_del_agg() != nullptr &&
          !options.ignore_range_deletions) {
          s = context->CreateTombstoneIterator(table_reader);
        } else {
          s = context->Get(table_reader);
        }
      } else {
        s = context->CreateTableReader(internal_comparator,
          fd, file_read_hist, skip_filters, level);
      }

      if (s.IsIOPending()) {
        context.release();
      }
    }
  } else if (s.IsIncomplete()) {
    // KeyMayExist() is called within LookupTableReader
    s = Status::OK();
  }
  return s;
}

} //namespace async
} // namespace rocksdb
