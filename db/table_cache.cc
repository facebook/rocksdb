//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "db/version_edit.h"

#include "rocksdb/statistics.h"
#include "table/iterator_wrapper.h"
#include "table/table_reader.h"
#include "table/get_context.h"
#include "util/coding.h"
#include "util/stop_watch.h"

namespace rocksdb {

static void DeleteEntry(const Slice& key, void* value) {
  TableReader* table_reader = reinterpret_cast<TableReader*>(value);
  delete table_reader;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

static Slice GetSliceForFileNumber(const uint64_t* file_number) {
  return Slice(reinterpret_cast<const char*>(file_number),
               sizeof(*file_number));
}

TableCache::TableCache(const ImmutableCFOptions& ioptions,
                       const EnvOptions& env_options, Cache* const cache)
    : ioptions_(ioptions),
      env_options_(env_options),
      cache_(cache) {}

TableCache::~TableCache() {
}

TableReader* TableCache::GetTableReaderFromHandle(Cache::Handle* handle) {
  return reinterpret_cast<TableReader*>(cache_->Value(handle));
}

void TableCache::ReleaseHandle(Cache::Handle* handle) {
  cache_->Release(handle);
}

Status TableCache::FindTable(const EnvOptions& env_options,
                             const InternalKeyComparator& internal_comparator,
                             const FileDescriptor& fd, Cache::Handle** handle,
                             const bool no_io) {
  Status s;
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    if (no_io) { // Dont do IO and return a not-found status
      return Status::Incomplete("Table not found in table_cache, no_io is set");
    }
    std::string fname =
        TableFileName(ioptions_.db_paths, fd.GetNumber(), fd.GetPathId());
    unique_ptr<RandomAccessFile> file;
    unique_ptr<TableReader> table_reader;
    s = ioptions_.env->NewRandomAccessFile(fname, &file, env_options);
    RecordTick(ioptions_.statistics, NO_FILE_OPENS);
    if (s.ok()) {
      if (ioptions_.advise_random_on_open) {
        file->Hint(RandomAccessFile::RANDOM);
      }
      StopWatch sw(ioptions_.env, ioptions_.statistics, TABLE_OPEN_IO_MICROS);
      s = ioptions_.table_factory->NewTableReader(
          ioptions_, env_options, internal_comparator, std::move(file),
          fd.GetFileSize(), &table_reader);
    }

    if (!s.ok()) {
      assert(table_reader == nullptr);
      RecordTick(ioptions_.statistics, NO_FILE_ERRORS);
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      *handle = cache_->Insert(key, table_reader.release(), 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  const EnvOptions& env_options,
                                  const InternalKeyComparator& icomparator,
                                  const FileDescriptor& fd,
                                  TableReader** table_reader_ptr,
                                  bool for_compaction, Arena* arena) {
  if (table_reader_ptr != nullptr) {
    *table_reader_ptr = nullptr;
  }
  TableReader* table_reader = fd.table_reader;
  Cache::Handle* handle = nullptr;
  Status s;
  if (table_reader == nullptr) {
    s = FindTable(env_options, icomparator, fd, &handle,
                  options.read_tier == kBlockCacheTier);
    if (!s.ok()) {
      return NewErrorIterator(s, arena);
    }
    table_reader = GetTableReaderFromHandle(handle);
  }

  Iterator* result = table_reader->NewIterator(options, arena);
  if (handle != nullptr) {
    result->RegisterCleanup(&UnrefEntry, cache_, handle);
  }
  if (table_reader_ptr != nullptr) {
    *table_reader_ptr = table_reader;
  }

  if (for_compaction) {
    table_reader->SetupForCompaction();
  }

  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       const InternalKeyComparator& internal_comparator,
                       const FileDescriptor& fd, const Slice& k,
                       GetContext* get_context) {
  TableReader* t = fd.table_reader;
  Status s;
  Cache::Handle* handle = nullptr;
  if (!t) {
    s = FindTable(env_options_, internal_comparator, fd, &handle,
                  options.read_tier == kBlockCacheTier);
    if (s.ok()) {
      t = GetTableReaderFromHandle(handle);
    }
  }
  if (s.ok()) {
    s = t->Get(options, k, get_context);
    if (handle != nullptr) {
      ReleaseHandle(handle);
    }
  } else if (options.read_tier && s.IsIncomplete()) {
    // Couldnt find Table in cache but treat as kFound if no_io set
    get_context->MarkKeyMayExist();
    return Status::OK();
  }
  return s;
}

Status TableCache::GetTableProperties(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    std::shared_ptr<const TableProperties>* properties, bool no_io) {
  Status s;
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    *properties = table_reader->GetTableProperties();

    return s;
  }

  Cache::Handle* table_handle = nullptr;
  s = FindTable(env_options, internal_comparator, fd, &table_handle, no_io);
  if (!s.ok()) {
    return s;
  }
  assert(table_handle);
  auto table = GetTableReaderFromHandle(table_handle);
  *properties = table->GetTableProperties();
  ReleaseHandle(table_handle);
  return s;
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
