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



}
}
