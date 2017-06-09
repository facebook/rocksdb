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

#include "util/stop_watch.h"

namespace rocksdb {

class Cache;
struct EnvOptions;
struct FileDescriptor;
class  HistogramImpl;
class  InternalIterator;
class  InternalKeyComparator;
struct ReadOptions;
struct TableProperties;
class  TableReader;

namespace async {

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
  Status OnGetReaderComplete(const Status& s);

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

  TableCacheFindTableHelper(const ImmutableCFOptions& ioptions, uint64_t fileno, Cache* cache) :
    PERF_TIMER_INIT(find_table_nanos),
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
                     Cache::Handle** handle, bool noio);

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

    PERF_TIMER_START(find_table_nanos);

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
                             std::unique_ptr<TableReader>&);

  const ImmutableCFOptions& GetIOptions() const {
    return gr_helper_.GetIOptions();
  }

  uint64_t GetFileNumber() const {
    return file_number_;
  }

 private:

  PERF_TIMER_DECL(find_table_nanos);
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
class TableCacheFindTableContext : protected AsyncStatusCapture {
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

  Status OnFindReaderComplete(const Status&, std::unique_ptr<TableReader>&&);

  Status OnComplete(const Status&);

  TableCacheFindTableContext(const Callback& cb,
    const ImmutableCFOptions& ioptions, uint64_t fileno, Cache* cache) :
    cb_(cb), ft_helper_(ioptions, fileno, cache),
    handle_(nullptr) {
  }

  Callback                  cb_;
  TableCacheFindTableHelper ft_helper_;
  Cache::Handle*            handle_;
};

class TableCacheGetPropertiesContext : protected AsyncStatusCapture {
public:

  using
  Callback = Callable<Status, const Status&, std::shared_ptr<const TableProperties>&&>;

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
    std::unique_ptr<TableReader>&& table_reader);

  Status OnComplete(const Status&);

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
class TableCacheGetContext : protected AsyncStatusCapture {
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

 private:

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
                           IterKey& row_cache_key);

   static void InsertRowCache(const ImmutableCFOptions& ioptions,
     const IterKey& row_cache_key, std::string&& row_cache_entry_buffer);

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
     TableReader** table_reader);

   // If TableReader is not in the cache
   // and no_io false
   // Requires context instance
   // Returns ok on sync completion, IOPending() on async submission
   // or any other error
   Status CreateTableReader(const InternalKeyComparator& internal_comparator,
                            const FileDescriptor& fd, HistogramImpl* file_read_hist, bool skip_filters,
                            int level);

   Status OnCreateTableReader(const Status&, std::unique_ptr<TableReader>&&);

   Status CreateTombstoneIterator(TableReader*);

   Status OnTombstoneIterator(const Status&, InternalIterator*);

   Status Get(TableReader* table_reader);

   Status OnGetComplete(const Status&);

   Status OnComplete(const Status&);

   Callback           cb_;
   TableCache*        table_cache_;
   TableCacheFindTableHelper ft_helper_;
   const ReadOptions* options_;
   Slice              k_;
   GetContext*        get_context_;
   bool               skip_filters_;

   Cache::Handle*     table_reader_handle_;
   bool               row_cache_present_;
   std::string        row_cache_entry_buffer_;
   TableReader*       table_reader_;
};

}
}
