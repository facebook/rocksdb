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

 private:

  PERF_TIMER_DECL(find_table_nanos);
  TableCacheGetReaderHelper gr_helper_;
  uint64_t                  file_number_;
  Cache*                    cache_;
};

}
}
