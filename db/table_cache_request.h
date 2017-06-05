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
#include "rocksdb/async/callables.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace rocksdb {

struct EnvOptions;
struct FileDescriptor;
class HistogramImpl;
struct ImmutableCFOptions;
class  InternalIterator;
struct InternalKeyComparator;
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

  TableCacheGetReaderHelper() {}

  Status TableCache::GetTableReader(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    bool sequential_mode, size_t readahead, bool record_read_stats,
    HistogramImpl* file_read_hist, std::unique_ptr<TableReader>* table_reader,
    bool skip_filters, int level, bool prefetch_index_and_filter_in_cache);

  // Callback that must be invoked on the GetReader() completion.
  // either directly on sync completion or via callback on async completion
  Status OnGetReaderComplete(const Status& s) {
    return s;
  }

private:

};

}
}
