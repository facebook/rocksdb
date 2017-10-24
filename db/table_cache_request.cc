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
#include "table/iterator_wrapper.h"
#include "table/table_builder.h"

#include "util/filename.h"
#include "util/file_reader_writer.h"
#include "util/sync_point.h"

namespace rocksdb {
namespace async {
/////////////////////////////////////////////////////////////////////////
/// TableCacheNewTombIterContext
Status TableCacheNewTombIterContext::Create(const EnvOptions& env_options,
    const InternalKeyComparator& icomparator, const FileDescriptor& fd,
    HistogramImpl* file_read_hist,
    bool skip_filters, int level) {

  Status s;
  table_reader_ = fd.table_reader;

  if (table_reader_ == nullptr) {
    handle_ = nullptr;

    s = TableCacheFindTableHelper::LookupCache(fd,
        table_cache_->GetCache(), &handle_,
        options_->read_tier == kBlockCacheTier /* no_io */);

    if (s.IsNotFound()) {
      TableCacheGetReaderHelper::Callback on_get_tr;
      if (cb_) {
        CallableFactory<TableCacheNewTombIterContext, Status, const Status&,
                        std::unique_ptr<TableReader>&&> f(this);
        on_get_tr = f.GetCallable<&TableCacheNewTombIterContext::OnFindTableReader>();
      }

      std::unique_ptr<TableReader> table_reader;
      s = fr_helper_.GetReader(on_get_tr,
                               env_options, icomparator, fd,
                               &table_reader, true /* record read_stats */,
                               file_read_hist, skip_filters,
                               level, true /*prefetch_index_and_filter_in_cache*/);

      if (!s.IsIOPending()) {
        s = OnFindTableReader(s, std::move(table_reader));
      }
    } else if (s.ok()) {
      assert(handle_ != nullptr);
      table_reader_ = table_cache_->GetTableReaderFromHandle(handle_);
      s = CreateIterator();
    } else {
      // We failed
      s = OnComplete(s);
    }
  } else {
    // TableReader is available from the FileDescriptor
    s = CreateIterator();
  }

  return s;
}

////////////////////////////////////////////////////////////////////////
/// TableCacheGetReaderHelper
Status TableCacheGetReaderHelper::GetTableReader(
  const Callback& cb_in,
  const EnvOptions& env_options,
  const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
  bool sequential_mode, size_t readahead, bool record_read_stats,
  HistogramImpl* file_read_hist, std::unique_ptr<TableReader>* table_reader,
  bool skip_filters, int level, bool prefetch_index_and_filter_in_cache) {

  Callback cb(cb_in);

  std::string fname =
    TableFileName(ioptions_.db_paths, fd.GetNumber(), fd.GetPathId());

  // Do not perform async IO on compaction
  EnvOptions ra_options(env_options);
  if (readahead > 0) {
    ra_options.use_async_reads = false;
    // Zero this out and open sync since we
    // opening the file in sync
    cb = Callback();
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

//////////////////////////////////////////////////////////////////////////
/// TableCacheGetContext
inline
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
    auto table_reader_cached = table_cache_->GetTableReaderFromHandle(
                          table_reader_handle_);
    assert(table_reader_cached != nullptr);

    if (s.ok() && get_context_->range_del_agg() != nullptr &&
        !options_->ignore_range_deletions) {
      return CreateTombstoneIterator(table_reader_cached);
    } else if (s.ok()) {
      return Get(table_reader_cached);
    }
  }

  // Incomplete is handled at the cache lookup stage.
  // If we are here this means IO is allowed
  assert(!s.IsIncomplete());

  return OnComplete(s);
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
        table_cache, options, k, get_context, skip_filters, fd.GetNumber(),
        handle, row_cache_present));

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

///////////////////////////////////////////////////////////////////////
/// TableCacheNewIteratorContext
//
Status TableCacheNewIteratorContext::Create(TableCache* table_cache,
    const ReadOptions& options, const EnvOptions& eoptions,
    const InternalKeyComparator& internal_comparator,
    const FileDescriptor& file_fd, RangeDelAggregator* range_del_agg,
    InternalIterator** iterator, TableReader** table_reader_ptr,
    HistogramImpl* file_read_hist, bool for_compaction, Arena* arena,
    bool skip_filters, int level) {

  assert(iterator != nullptr);
  *iterator = nullptr;

  if (table_reader_ptr != nullptr) {
    *table_reader_ptr = nullptr;
  }

  const Callback empty_cb;
  TableCacheNewIteratorContext context(empty_cb, table_cache, options,
                                       internal_comparator,
                                       file_fd.GetNumber(), range_del_agg, for_compaction, arena, skip_filters);

  Status s = context.CreateImpl(eoptions, internal_comparator, file_fd,
                                file_read_hist, level);

  // Iterator is created even on error
  // to contain and report the status
  *iterator = context.GetResult();
  if (s.ok() && table_reader_ptr) {
    *table_reader_ptr = context.GetReader();
  }
  return s;
}

Status TableCacheNewIteratorContext::RequestCreate(const Callback& cb,
    TableCache* table_cache, const ReadOptions& options,
    const EnvOptions& eoptions, const InternalKeyComparator& internal_comparator,
    const FileDescriptor& file_fd, RangeDelAggregator* range_del_agg,
    InternalIterator** iterator, TableReader** table_reader_ptr,
    HistogramImpl* file_read_hist, bool for_compaction, Arena* arena,
    bool skip_filters, int level) {

  assert(iterator != nullptr);
  *iterator = nullptr;

  if (table_reader_ptr != nullptr) {
    *table_reader_ptr = nullptr;
  }

  std::unique_ptr<TableCacheNewIteratorContext> context(new
      TableCacheNewIteratorContext(cb, table_cache, options,
                                   internal_comparator, file_fd.GetNumber(), range_del_agg, for_compaction, arena,
                                   skip_filters));

  Status s = context->CreateImpl(eoptions, internal_comparator, file_fd,
                                 file_read_hist, level);

  if (s.IsIOPending()) {
    context.release();
  } else {
    // Iterator is created even on error
    // to contain and report the status
    *iterator = context->GetResult();
    if (s.ok() && table_reader_ptr) {
      *table_reader_ptr = context->GetReader();
    }
  }
  return s;
}


Status TableCacheNewIteratorContext::CreateImpl(const EnvOptions& eoptions,
    const InternalKeyComparator& internal_comparator,
    const FileDescriptor& fd, HistogramImpl* file_read_hist,
    int level) {

  PERF_METER_START(new_table_iterator_nanos);

  Status s;
  size_t readahead = 0;
  if (for_compaction_) {
#ifndef NDEBUG
    bool use_direct_reads_for_compaction = eoptions.use_direct_reads;
    TEST_SYNC_POINT_CALLBACK("TableCache::NewIterator:for_compaction",
                             &use_direct_reads_for_compaction);
#endif  // !NDEBUG
    // XXX: Due to the async prototype we always choose
    // to create a new table reader for compactions and make it operate
    // in a sync mode
    readahead = table_cache_->GetIOptions().compaction_readahead_size;
    create_new_table_reader_ = true;
  } else {
    readahead = options_->readahead_size;
    create_new_table_reader_ = readahead > 0;
  }

  // We still want compactions to run in sync mode
  // Therefore, we want a table_reader that opens the file
  // in a sync mode and performs sync.
  // Thus, disable the callback and reset the async mode
  EnvOptions env_options(eoptions);
  if (for_compaction_) {
    cb_ = Callback();
    env_options.use_async_reads = false;
  }

  if (create_new_table_reader_) {

    TableCacheGetReaderHelper::Callback on_get_tr;
    if (cb_) {
      CallableFactory<TableCacheNewIteratorContext, Status, const Status&,
                      std::unique_ptr<TableReader>&&> f(this);
      on_get_tr = f.GetCallable<&TableCacheNewIteratorContext::OnTableReader>();
    }

    PERF_METER_MEASURE(new_table_iterator_nanos);
    std::unique_ptr<TableReader> table_reader;
    s = gr_helper_.GetTableReader(
          on_get_tr, env_options, internal_comparator, fd,
          true /* sequential_mode */, readahead, !for_compaction_ /* record stats */,
          nullptr /* hist */, &table_reader, false /* skip_filters */, level,
          true /* prefetch_index_and_filter */);

    if (!s.IsIOPending()) {
      s = OnTableReader(s, std::move(table_reader));
    }
  } else {
    table_reader_ = fd.table_reader;
    if (table_reader_ == nullptr) {
      s = TableCacheFindTableHelper::LookupCache(fd,
          table_cache_->GetCache(), &handle_,
          options_->read_tier == kBlockCacheTier /* no_io */);

      if (s.IsNotFound()) {
        TableCacheGetReaderHelper::Callback on_get_tr;
        if (cb_) {
          CallableFactory<TableCacheNewIteratorContext, Status, const Status&,
                          std::unique_ptr<TableReader>&&> f(this);
          on_get_tr = f.GetCallable<&TableCacheNewIteratorContext::OnTableReader>();
        }

        PERF_METER_MEASURE(new_table_iterator_nanos);
        std::unique_ptr<TableReader> table_reader;
        s = fr_helper_.GetReader(on_get_tr,
                                 env_options, internal_comparator, fd,
                                 &table_reader, !for_compaction_ /* record read_stats */,
                                 file_read_hist, skip_filters_,
                                 level, true /*prefetch_index_and_filter_in_cache*/);

        if (!s.IsIOPending()) {
          s = OnTableReader(s, std::move(table_reader));
        }
      } else if (s.ok()) {
        assert(handle_ != nullptr);
        table_reader_ = table_cache_->GetTableReaderFromHandle(handle_);
        s = CreateTableIterator();
      }
    } else {
      s = CreateTableIterator();
    }
  }
  return s;
}

Status TableCacheNewIteratorContext::OnTableReader(const Status& status,
    std::unique_ptr<TableReader>&& table_reader) {

  async(status);

  Status s;

  if (create_new_table_reader_) {
    // Handle GetReader call
    s = gr_helper_.OnGetReaderComplete(status);
    if (s.ok()) {
      table_reader_ = table_reader.release();
    } else {
      assert(!table_reader);
    }
  } else {
    // Handle FindReader call
    // At this point table_reader is not in cache
    // so we own it
    assert(handle_ == nullptr);
    std::unique_ptr<TableReader> tr(std::move(table_reader));
    s = fr_helper_.OnGetReaderComplete(status, &handle_, tr);
    if (s.ok()) {
      assert(handle_ != nullptr);
      table_reader_ = table_cache_->GetTableReaderFromHandle(handle_);
    } else {
      assert(!tr);
    }
  }

  // If we failed, then we can not proceed
  if (s.ok()) {
    return CreateTableIterator();
  } else {
    return OnComplete(s);
  }
}

Status TableCacheNewIteratorContext::OnNewTableIterator(const Status& status,
    InternalIterator* iterator) {

  Status s(status);
  async(status);

  assert(result_ == nullptr);
  assert(iterator != nullptr);
  assert(table_reader_);
  result_ = iterator;

  if (create_new_table_reader_) {
    assert(handle_ == nullptr);
    result_->RegisterCleanup(&table_cache_detail::DeleteTableReader, table_reader_,
                             nullptr);
  } else if (handle_ != nullptr) {
    result_->RegisterCleanup(&table_cache_detail::UnrefEntry,
                             table_cache_->GetCache(), handle_);
    handle_ = nullptr;  // prevent from releasing below
  }

  if (s.ok()) {

    if (for_compaction_) {
      table_reader_->SetupForCompaction();
    }

    if (range_del_agg_ != nullptr && !options_->ignore_range_deletions) {
      InternalIterator* range_del_iter = nullptr;
      TableCacheNewTombStoneIteratorHelper::Callback on_tomb_stone;
      if (cb_) {
        CallableFactory<TableCacheNewIteratorContext, Status, const Status&, InternalIterator*>
        f(this);
        on_tomb_stone =
          f.GetCallable<&TableCacheNewIteratorContext::OnTombstoneIterator>();
      }

      s = tmb_stone_helper_.Create(on_tomb_stone, table_reader_, *options_,
                                   &range_del_iter);

      if (!s.IsIOPending()) {
        s = OnTombstoneIterator(s, range_del_iter);
      }
    } else {
      s = OnComplete(s);
    }

  } else {
    s = OnComplete(s);
  }

  return s;
}
} //namespace async
} // namespace rocksdb
