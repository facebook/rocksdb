//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#pragma once

#include <memory>

#include "async/async_status_capture.h"
#include "monitoring/perf_context_imp.h"
#include "options/cf_options.h"
#include "rocksdb/async/callables.h"
#include "rocksdb/cache.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/table_reader.h"
#include "util/stop_watch.h"

namespace rocksdb {

class Cache;
struct EnvOptions;
struct FileDescriptor;
class  HistogramImpl;
class  InternalKeyComparator;
struct ReadOptions;
struct TableProperties;

namespace async {


class TableCacheNewTombStoneIteratorHelper {
 public:

  using
  Callback = async::Callable<Status, const Status&, InternalIterator*>;

  TableCacheNewTombStoneIteratorHelper() {}

  TableCacheNewTombStoneIteratorHelper(const
                                       TableCacheNewTombStoneIteratorHelper&) = delete;
  TableCacheNewTombStoneIteratorHelper& operator=(const
      TableCacheNewTombStoneIteratorHelper&) = delete;

  Status Create(const Callback& on_tomb_stone, TableReader* table_reader,
                const ReadOptions& options, InternalIterator** range_del_iter) {
    Status s;

    if (on_tomb_stone) {
      s = table_reader->NewRangeTombstoneIterator(on_tomb_stone, options,
          range_del_iter);
    } else {
      *range_del_iter = table_reader->NewRangeTombstoneIterator(options);
    }
    return s;
  }

  Status AddTombstones(const Status& status, InternalIterator* iterator,
                       RangeDelAggregator* range_del_agg) {
    Status s(status);
    std::unique_ptr<InternalIterator> range_del_iter(iterator);
    if (s.ok()) {
      if (range_del_iter) {
        s = range_del_iter->status();
        if (s.ok()) {
          s = range_del_agg->AddTombstones(std::move(range_del_iter));
        }
      }
    }
    return s;
  }
};

// This is a helper class to be used with other
// context. Helper classes provide a piece of
// re-usable functionality that other async contexts
// can re-use. Thus, helpers do not provide callbacks of their own
// but rely on the context class callbacks to call their
// completion that is necessary after the IO operation completes sync or async
class  TableCacheGetReaderHelper {
 public:

  // This must match the callback for TableOpenRequestContext
  // as we are going to call factory interface
  using
  Callback = async::Callable<Status, const Status&,
  std::unique_ptr<TableReader>&&>;

  TableCacheGetReaderHelper(const TableCacheGetReaderHelper&) = delete;
  TableCacheGetReaderHelper& operator=(const TableCacheGetReaderHelper&) =
    delete;

  explicit
  TableCacheGetReaderHelper(const ImmutableCFOptions& ioptions) :
    ioptions_(ioptions),
    sw_(ioptions.env, ioptions.statistics, TABLE_OPEN_IO_MICROS,
        true /* don't start */) {
  }

  ~TableCacheGetReaderHelper() {
    sw_.disarm();
  }

  Status GetTableReader(
    const Callback& cb,
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    bool sequential_mode, size_t readahead, bool record_read_stats,
    HistogramImpl* file_read_hist, std::unique_ptr<TableReader>* table_reader,
    bool skip_filters, int level, bool prefetch_index_and_filter_in_cache);

  // Callback that must be invoked on the GetReader() completion.
  // either directly on sync completion or via callback on async completion
  Status OnGetReaderComplete(const Status& s) {
    if (sw_.start_time() > 0) {
      sw_.elapsed();
    }
    TEST_SYNC_POINT("TableCache::GetTableReader:0");
    return s;
  }

  const ImmutableCFOptions& GetIOptions() const {
    return ioptions_;
  }

 private:

  const ImmutableCFOptions& ioptions_;
  StopWatch sw_;
};

// This is a helper class that can be used
// as a part of other async contexts to implement sync/async
// FindTable functionality
class TableCacheFindTableHelper {
 public:

  using
  Callback = TableCacheGetReaderHelper::Callback;

  TableCacheFindTableHelper(const TableCacheFindTableHelper&) = delete;
  TableCacheFindTableHelper& operator=(const TableCacheFindTableHelper&) =
    delete;

  TableCacheFindTableHelper(const ImmutableCFOptions& ioptions, uint64_t fileno,
                            Cache* cache) :
    PERF_METER_INIT(find_table_nanos),
    gr_helper_(ioptions),
    file_number_(fileno),
    cache_(cache) {
  }

  // We can check if it is in the cache w/o instantiating
  // the class.The function returns success when the entry was found in the cache,
  // NoFound with no entry and the IO is allowed. If no IO is allowed the function
  // returns Incomplete.
  static
  Status LookupCache(const FileDescriptor& fd, Cache* cache,
                     Cache::Handle** handle, bool no_io) {
    // This is a static method, we collect this separate
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

  // This function will request to open
  // the table and create a table reader
  // If no IO is allowed do not call this function
  // If the supplied callback is not empty the function
  // will perform any IO necessary in async manner
  // otherwise it will return sync
  // OnGetReaderComplete() must be invoked either
  // after sync return of GetReader() or via a callback
  // supplied
  Status GetReader(const Callback& cb,
                   const EnvOptions& env_options,
                   const InternalKeyComparator& internal_comparator,
                   const FileDescriptor& fd,
                   std::unique_ptr<TableReader>* table_reader,
                   bool record_read_stats,
                   HistogramImpl* file_read_hist, bool skip_filters,
                   int level,
                   bool prefetch_index_and_filter_in_cache) {

    PERF_METER_START(find_table_nanos);

    Status s = gr_helper_.GetTableReader(cb, env_options, internal_comparator, fd,
                                         false /* sequential mode */, 0 /* readahead */,
                                         record_read_stats, file_read_hist, table_reader,
                                         skip_filters, level, prefetch_index_and_filter_in_cache);

    return s;
  }


  // This must be called if GetReader() was invoked either synchronously
  // or from the supplied callback
  Status OnGetReaderComplete(const Status& status,
                             Cache::Handle** handle,
                             std::unique_ptr<TableReader>& table_reader) {
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
    PERF_METER_STOP(find_table_nanos);
    return s;
  }

  const ImmutableCFOptions& GetIOptions() const {
    return gr_helper_.GetIOptions();
  }

  uint64_t GetFileNumber() const {
    return file_number_;
  }

 private:

  PERF_METER_DECL(find_table_nanos);
  TableCacheGetReaderHelper gr_helper_;
  uint64_t                  file_number_;
  Cache*                    cache_;
};

// This class looks up a table in the cache and if not in the
// cache creates and caches it. It returns a Cache::Handle which
// the client must convert to a TableReader pointer and when done
// the cache handle must be released.
// This class completly relies on the above TableCacheFindTableHelper
// functionality but async public API FindTable require a context of its own
class TableCacheFindTableContext : private AsyncStatusCapture {
 public:

  using
  Callback = Callable<Status, const Status&, Cache::Handle*>;

  TableCacheFindTableContext(const TableCacheFindTableContext&) = delete;
  TableCacheFindTableContext& operator=(const TableCacheFindTableContext&) =
    delete;

  static Status Find(TableCache* table_cache,
                     const EnvOptions& env_options,
                     const InternalKeyComparator& internal_comparator,
                     const FileDescriptor& file_fd, Cache::Handle** handle,
                     const bool no_io = false, bool record_read_stats = true,
                     HistogramImpl* file_read_hist = nullptr,
                     bool skip_filters = false, int level = -1,
                     bool prefetch_index_and_filter_in_cache = true);

  static Status RequestFind(const Callback& cb,
                            TableCache* table_cache,
                            const EnvOptions& env_options,
                            const InternalKeyComparator& internal_comparator,
                            const FileDescriptor& file_fd, Cache::Handle** handle,
                            const bool no_io = false, bool record_read_stats = true,
                            HistogramImpl* file_read_hist = nullptr,
                            bool skip_filters = false, int level = -1,
                            bool prefetch_index_and_filter_in_cache = true);

  Cache::Handle* GetHandle() {
    Cache::Handle* result = handle_;
    handle_ = nullptr;
    return result;
  }

 private:

  Status OnFindReaderComplete(const Status& status,
                              std::unique_ptr<TableReader>&& table_reader) {
    async(status);
    std::unique_ptr<TableReader> t(std::move(table_reader));
    Status s = ft_helper_.OnGetReaderComplete(status, &handle_, t);
    s.async(async());
    return OnComplete(s);
  }

  Status OnComplete(const Status& status) {
    if (cb_ && async()) {
      Status s(status);
      s.async(true);
      auto handle = GetHandle();
      cb_.Invoke(s, handle);
      delete this;
      return s;
    }
    return status;
  }

  TableCacheFindTableContext(const Callback& cb,
                             const ImmutableCFOptions& ioptions, uint64_t fileno, Cache* cache) :
    cb_(cb), ft_helper_(ioptions, fileno, cache),
    handle_(nullptr) {
  }

  Callback                  cb_;
  TableCacheFindTableHelper ft_helper_;
  Cache::Handle*            handle_;
};

class TableCacheNewTombIterContext : private async::AsyncStatusCapture {
 public:
  using
  Callback = async::Callable<Status, const Status&, InternalIterator*>;

  TableCacheNewTombIterContext(const TableCacheNewTombIterContext&) = delete;
  TableCacheNewTombIterContext& operator=(const TableCacheNewTombIterContext&) =
    delete;

  static InternalIterator* Create(TableCache* table_cache,
                                  const ReadOptions& options,
                                  const EnvOptions& env_options, const InternalKeyComparator& icomparator,
                                  const FileDescriptor& fd,
                                  HistogramImpl* file_read_hist, bool skip_filters, int level) {
    TableCacheNewTombIterContext ctx(Callback(), table_cache, options,
                                     fd.GetNumber());
    ctx.Create(env_options, icomparator, fd, file_read_hist, skip_filters, level);
    return ctx.GetResult();
  }

  static Status RequestCreate(const Callback& cb,
                              InternalIterator** iterator, TableCache* table_cache,
                              const ReadOptions& options, const EnvOptions& env_options,
                              const InternalKeyComparator& icomparator, const FileDescriptor& fd,
                              HistogramImpl* file_read_hist, bool skip_filters, int level) {
    Status s;
    assert(iterator != nullptr);
    *iterator = nullptr;
    std::unique_ptr<TableCacheNewTombIterContext> ctx(new
        TableCacheNewTombIterContext(cb, table_cache,
                                     options, fd.GetNumber()));
    s = ctx->Create(env_options, icomparator, fd, file_read_hist, skip_filters,
                    level);
    if (s.IsIOPending()) {
      ctx.release();
    } else {
      *iterator = ctx->GetResult();
    }
    return s;
  }

 private:

  TableCacheNewTombIterContext(const Callback& cb, TableCache* table_cache,
                               const ReadOptions& options,
                               uint64_t fileno) :
    cb_(cb),
    table_cache_(table_cache),
    options_(&options),
    fr_helper_(table_cache_->GetIOptions(), fileno, table_cache_->GetCache()),
    table_reader_(nullptr),
    handle_(nullptr),
    result_(nullptr) {}

  Status Create(const EnvOptions& env_options,
                const InternalKeyComparator& icomparator, const FileDescriptor& fd,
                HistogramImpl* file_read_hist, bool skip_filters, int level);

  InternalIterator* GetResult() {
    InternalIterator* result = nullptr;
    std::swap(result, result_);
    return result;
  }

  Status OnFindTableReader(const Status& status,
                           std::unique_ptr<TableReader>&& table_reader) {
    async(status);
    Status s;
    // Handle FindReader call
    // At this point table_reader is not in cache
    // so we own it. Lets put it in the cache and reacquire from
    // cache handle
    assert(handle_ == nullptr);
    std::unique_ptr<TableReader> tr(std::move(table_reader));
    s = fr_helper_.OnGetReaderComplete(status, &handle_, tr);

    if (s.ok()) {
      assert(handle_ != nullptr);
      table_reader_ = table_cache_->GetTableReaderFromHandle(handle_);
      s = CreateIterator();
    } else {
      assert(!tr);
      s = OnComplete(s);
    }
    return s;
  }

  Status CreateIterator() {
    TableCacheNewTombStoneIteratorHelper::Callback on_ts_iter;
    if (cb_) {
      async::CallableFactory<TableCacheNewTombIterContext, Status, const Status&, InternalIterator*>
      f(this);
      on_ts_iter =
        f.GetCallable<&TableCacheNewTombIterContext::OnTombstoneIteratorCreate>();
    }
    InternalIterator* result = nullptr;
    Status s = ts_helper_.Create(on_ts_iter, table_reader_, *options_, &result);
    if (!s.IsIOPending()) {
      s = OnTombstoneIteratorCreate(s, result);
    }
    return s;
  }

  Status OnTombstoneIteratorCreate(const Status& status,
                                   InternalIterator* iterator) {
    async(status);
    if (status.ok()) {
      // The result can be nullptr
      result_ = iterator;
    }
    return OnComplete(status);
  }

  Status OnComplete(const Status& status) {
    if (status.ok()) {
      if (result_ != nullptr) {
        if (handle_ != nullptr) {
          result_->RegisterCleanup(&table_cache_detail::UnrefEntry,
                                   table_cache_->GetCache(), handle_);
        }
      }
    }
    if (result_ == nullptr && handle_ != nullptr) {
      // the range deletion block didn't exist, or there was a failure between
      // getting handle and getting iterator.
      table_cache_->ReleaseHandle(handle_);
      handle_ = nullptr;
    }
    if (!status.ok()) {
      assert(result_ == nullptr);
      result_ = NewErrorInternalIterator(status);
    }
    if (async()) {
      Status s(status);
      s.async(true);
      cb_.Invoke(s, GetResult());
      delete this;
    }
    return status;
  }

  Callback          cb_;
  TableCache*       table_cache_;
  const ReadOptions* options_;
  TableCacheFindTableHelper  fr_helper_;
  TableCacheNewTombStoneIteratorHelper ts_helper_;

  TableReader*      table_reader_;
  Cache::Handle*    handle_;
  InternalIterator* result_;
};

class TableCacheGetPropertiesContext : private AsyncStatusCapture {
 public:

  using
  Callback = Callable<Status, const Status&,
  std::shared_ptr<const TableProperties>&&>;

  TableCacheGetPropertiesContext(const TableCacheGetPropertiesContext&) = delete;
  TableCacheGetPropertiesContext& operator=(const
      TableCacheGetPropertiesContext&) = delete;

  static Status GetProps(TableCache* table_cache,
                         const EnvOptions& env_options,
                         const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
                         std::shared_ptr<const TableProperties>* properties, bool no_io);

  static Status RequestGetProps(const Callback& cb,
                                TableCache* table_cache,
                                const EnvOptions& env_options,
                                const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
                                std::shared_ptr<const TableProperties>* properties, bool no_io);

  std::shared_ptr<const TableProperties> GetProperties() {
    auto result(std::move(props_));
    return result;
  }

 private:

  TableCacheGetPropertiesContext(const Callback& cb,
                                 TableCache* table_cache,
                                 const ImmutableCFOptions& ioptions, uint64_t fileno, Cache* cache) :
    cb_(cb), table_cache_(table_cache),
    ft_helper_(ioptions, fileno, cache),
    props_() {
  }

  static Status GetFromDescriptor(const FileDescriptor& fd,
                                  std::shared_ptr<const TableProperties>* properties) {

    assert(properties);
    Status s;
    auto table_reader = fd.table_reader;
    // table already been pre-loaded?
    if (table_reader) {
      *properties = table_reader->GetTableProperties();
      return s;
    }
    return Status::NotFound();
  }

  static void GetPropertiesFromCacheHandle(TableCache* table_cache,
      Cache::Handle* table_reader_handle,
      std::shared_ptr<const TableProperties>* properties) {
    assert(table_reader_handle);
    auto table = table_cache->GetTableReaderFromHandle(table_reader_handle);
    assert(table);
    *properties = table->GetTableProperties();
    table_cache->ReleaseHandle(table_reader_handle);
  }

  Status OnFindReaderComplete(const Status& status,
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

  Status OnComplete(const Status& status) {
    if (cb_ && async()) {
      Status s(status);
      s.async(true);
      cb_.Invoke(s, std::move(props_));
      delete this;
      return s;
    }
    return status;
  }

  Callback                               cb_;
  TableCache*                            table_cache_;
  TableCacheFindTableHelper              ft_helper_;
  // Result to pass to the callback
  std::shared_ptr<const TableProperties> props_;
};

// This class facilitates asynchronous get using TableCache
// and the underlying TableReader
// In the process the context will attempt to Find/or if not
// in cache asynchronously create a new table reader by
// opening the table in async manner
// It will then execute potentially asynchronous Get on that
// table reader
// If  range deletion tombstones are present we then
// get an iterator on it (potentially async)
class TableCacheGetContext : private AsyncStatusCapture {
 public:
  // Callback to be supplied by the client
  using
  Callback = Callable<Status, const Status&>;

  TableCacheGetContext(const TableCacheGetContext&) = delete;
  TableCacheGetContext& operator=(const TableCacheGetContext&) = delete;

  ~TableCacheGetContext() {
    ReleaseCacheHandle();
  }

  static Status Get(TableCache* table_cache,
                    const ReadOptions& options,
                    const InternalKeyComparator& internal_comparator,
                    const FileDescriptor& file_fd, const Slice& k,
                    GetContext* get_context, HistogramImpl* file_read_hist = nullptr,
                    bool skip_filters = false, int level = -1);

  static Status RequestGet(const Callback& cb,
                           TableCache* table_cache,
                           const ReadOptions& options,
                           const InternalKeyComparator& internal_comparator,
                           const FileDescriptor& file_fd, const Slice& k,
                           GetContext* get_context, HistogramImpl* file_read_hist = nullptr,
                           bool skip_filters = false, int level = -1);

  TableCacheGetContext(const Callback& cb,
                       TableCache* table_cache, const ReadOptions& options,
                       const Slice& k, GetContext* get_context, bool skip_filters,
                       uint64_t fileno,
                       Cache::Handle* table_reader_handle,
                       bool row_cache_present) :
    cb_(cb),
    table_cache_(table_cache),
    ft_helper_(table_cache_->GetIOptions(), fileno, table_cache_->GetCache()),
    options_(&options),
    k_(k),
    get_context_(get_context),
    skip_filters_(skip_filters),
    table_reader_handle_(table_reader_handle),
    row_cache_present_(row_cache_present),
    row_cache_entry_buffer_(),
    table_reader_(nullptr) {
  }

 private:

  void ReleaseCacheHandle() {
    if (table_reader_handle_ != nullptr) {
      table_cache_->ReleaseHandle(table_reader_handle_);
      table_reader_handle_ = nullptr;
    }
  }

  static bool IsNoIo(const ReadOptions& options) {
    return options.read_tier == kBlockCacheTier;
  }

  static void ComputeCacheKey(const std::string& row_cache_id,
                              const ReadOptions& options,
                              uint64_t fd_number,
                              const Slice& k,
                              IterKey& row_cache_key) {
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

  static void InsertRowCache(const ImmutableCFOptions& ioptions,
                             const IterKey& row_cache_key, std::string&& row_cache_entry_buffer) {
    assert(row_cache_key.Size() > 0);
    assert(!row_cache_entry_buffer.empty());

    size_t charge =
      row_cache_key.Size() + row_cache_entry_buffer.size() + sizeof(std::string);
    void* row_ptr = new std::string(std::move(row_cache_entry_buffer));
    ioptions.row_cache->Insert(row_cache_key.GetUserKey(), row_ptr, charge,
                               &table_cache_detail::DeleteEntry<std::string>);
  }

  // Returns NotFound otherwise OK()
  // The result is placed into get_context
  static Status LookupRowCache(TableCache* table_cache,
                               const ReadOptions& options, const FileDescriptor& fd,
                               const Slice& k, GetContext* get_context,
                               bool& raw_cache_enabled);

  // Returns OK, if the TableReader is found either in the FileDescriptor
  // or in the cache.
  // Returns Incomplete if TableReader is not in the cache
  // and no_io is true. Thus we can not proceed with the lookup.
  // We must return OK() and set KeyMaybePresent
  static Status LookupTableReader(TableCache* table_cache,
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

  // If TableReader is not in the cache
  // and no_io false
  // Requires context instance
  // Returns ok on sync completion, IOPending() on async submission
  // or any other error
  Status CreateTableReader(const InternalKeyComparator& internal_comparator,
                           const FileDescriptor& fd, HistogramImpl* file_read_hist, bool skip_filters,
                           int level);

  Status OnCreateTableReader(const Status&, std::unique_ptr<TableReader>&&);

  Status CreateTombstoneIterator(TableReader* table_reader) {
    Status s;
    InternalIterator* range_del_iter = nullptr;
    // Save for get to come next
    table_reader_ = table_reader;
    TableCacheNewTombStoneIteratorHelper::Callback on_ts_iterator;
    if (cb_) {
      CallableFactory<TableCacheGetContext, Status, const Status&, InternalIterator*>
      f(this);
      on_ts_iterator = f.GetCallable<&TableCacheGetContext::OnTombstoneIterator>();
    }
    s = tstn_helper_.Create(on_ts_iterator, table_reader, *options_,
                            &range_del_iter);
    if (!s.IsIOPending()) {
      s = OnTombstoneIterator(s, range_del_iter);
    }
    return s;
  }

  Status OnTombstoneIterator(const Status& status, InternalIterator* iter) {
    async(status);
    tstn_helper_.AddTombstones(status, iter,
                               get_context_->range_del_agg());

    assert(table_reader_ != nullptr);
    return Get(table_reader_);
  }

  Status Get(TableReader* table_reader) {
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

  Status OnGetComplete(const Status& status) {
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

  Status OnComplete(const Status& status) {
    ReleaseCacheHandle();
    if (cb_ && async()) {
      Status s(status);
      s.async(true);
      cb_.Invoke(s);
      delete this;
      return s;
    }
    return status;
  }

  Callback           cb_;
  TableCache*        table_cache_;
  TableCacheFindTableHelper ft_helper_;
  TableCacheNewTombStoneIteratorHelper tstn_helper_;
  const ReadOptions* options_;
  Slice              k_;
  GetContext*        get_context_;
  bool               skip_filters_;

  Cache::Handle*     table_reader_handle_;
  bool               row_cache_present_;
  std::string        row_cache_entry_buffer_;
  TableReader*       table_reader_;
};

// This class creates an iterator in async way if
// any IO is encountered using a corresponding TableReader
class TableCacheNewIteratorContext : private AsyncStatusCapture {
 public:

  using
  Callback = Callable<Status, const Status&, InternalIterator*,TableReader*>;

  TableCacheNewIteratorContext(const TableCacheNewIteratorContext&) = delete;
  TableCacheNewIteratorContext& operator=(const TableCacheNewIteratorContext&) =
    delete;

  static Status Create(TableCache* table_cache, const ReadOptions& options,
                       const EnvOptions& eoptions,
                       const InternalKeyComparator& internal_comparator,
                       const FileDescriptor& file_fd, RangeDelAggregator* range_del_agg,
                       InternalIterator** iterator,
                       TableReader** table_reader_ptr = nullptr,
                       HistogramImpl* file_read_hist = nullptr, bool for_compaction = false,
                       Arena* arena = nullptr, bool skip_filters = false, int level = -1);

  static Status RequestCreate(const Callback& cb, TableCache* table_cache,
                              const ReadOptions& options, const EnvOptions& eoptions,
                              const InternalKeyComparator& internal_comparator,
                              const FileDescriptor& file_fd, RangeDelAggregator* range_del_agg,
                              InternalIterator** iterator,
                              TableReader** table_reader_ptr = nullptr,
                              HistogramImpl* file_read_hist = nullptr, bool for_compaction = false,
                              Arena* arena = nullptr, bool skip_filters = false, int level = -1);

  ~TableCacheNewIteratorContext() {
    ResetHandle();
  }

  InternalIterator* GetResult() {
    return result_;
  }

  TableReader* GetReader() const {
    return table_reader_;
  }

 private:

  TableCacheNewIteratorContext(const Callback& cb, TableCache* table_cache,
                               const ReadOptions& options, const InternalKeyComparator& icomparator,
                               uint64_t fileno, RangeDelAggregator* range_del_agg, bool for_compaction,
                               Arena* arena, bool skip_filters) :
    PERF_METER_INIT(new_table_iterator_nanos),
    cb_(cb),
    table_cache_(table_cache),
    options_(&options),
    icomparator_(&icomparator),
    gr_helper_(table_cache->GetIOptions()),
    fr_helper_(table_cache->GetIOptions(), fileno, table_cache->GetCache()),
    range_del_agg_(range_del_agg),
    for_compaction_(for_compaction),
    arena_(arena),
    skip_filters_(skip_filters),
    create_new_table_reader_(false),
    table_reader_(nullptr),
    handle_(nullptr),
    result_(nullptr) {
  }

  void ResetHandle() {
    if (handle_ != nullptr) {
      table_cache_->ReleaseHandle(handle_);
      handle_ = nullptr;
    }
  }

  Status CreateImpl(const EnvOptions& env_options,
                    const InternalKeyComparator& internal_comparator,
                    const FileDescriptor& file_fd, HistogramImpl* file_read_hist,
                    int level);

  Status OnTableReader(const Status&, std::unique_ptr<TableReader>&&);

  Status CreateTableIterator() {
    Status s;
    InternalIterator* internal_iterator = nullptr;

    if (cb_) {
      CallableFactory<TableCacheNewIteratorContext, Status, const Status&, InternalIterator*>
      f(this);
      auto on_new_table_iterator =
        f.GetCallable<&TableCacheNewIteratorContext::OnNewTableIterator>();

      s = table_reader_->NewIterator(on_new_table_iterator, *options_,
                                     &internal_iterator, arena_, icomparator_, skip_filters_);
    } else {
      internal_iterator = table_reader_->NewIterator(*options_, arena_, icomparator_,
                          skip_filters_);
    }
    if (!s.IsIOPending()) {
      s = OnNewTableIterator(s, internal_iterator);
    }
    return s;
  }

  Status OnNewTableIterator(const Status&, InternalIterator* iterator);

  Status OnTombstoneIterator(const Status& status, InternalIterator* iterator) {
    async(status);
    Status s = tmb_stone_helper_.AddTombstones(status, iterator, range_del_agg_);
    return OnComplete(s);
  }

  Status OnComplete(const Status& status) {
    ResetHandle();
    if (!status.ok()) {
      assert(result_ == nullptr);
      result_ = NewErrorInternalIterator(status, arena_);
    }
    PERF_METER_STOP(new_table_iterator_nanos);
    if (cb_ && async()) {
      Status s(status);
      s.async(true);
      cb_.Invoke(s, result_, table_reader_);
      delete this;
      return status;
    }
    return status;
  }

  PERF_METER_DECL(new_table_iterator_nanos);
  Callback                      cb_;
  TableCache*                   table_cache_;
  const ReadOptions*            options_;
  const InternalKeyComparator*  icomparator_;
  TableCacheGetReaderHelper     gr_helper_;
  TableCacheFindTableHelper     fr_helper_;
  TableCacheNewTombStoneIteratorHelper tmb_stone_helper_;
  RangeDelAggregator*           range_del_agg_;
  const bool                    for_compaction_;
  Arena*                        arena_;
  bool                          skip_filters_;

  bool                          create_new_table_reader_;

  TableReader*                  table_reader_;
  Cache::Handle*                handle_;
  InternalIterator*             result_;
};

}
}
