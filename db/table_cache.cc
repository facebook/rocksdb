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
#include "table/table_reader.h"
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

static Slice GetSliceForFileNumber(uint64_t* file_number) {
  return Slice(reinterpret_cast<const char*>(file_number),
               sizeof(*file_number));
}

TableCache::TableCache(const std::string& dbname, const Options* options,
                       const EnvOptions& storage_options, Cache* const cache)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      storage_options_(storage_options),
      cache_(cache) {}

TableCache::~TableCache() {
}

TableReader* TableCache::GetTableReaderFromHandle(Cache::Handle* handle) {
  return reinterpret_cast<TableReader*>(cache_->Value(handle));
}

void TableCache::ReleaseHandle(Cache::Handle* handle) {
  cache_->Release(handle);
}

Status TableCache::FindTable(const EnvOptions& toptions,
                             const InternalKeyComparator& internal_comparator,
                             uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle, bool* table_io,
                             const bool no_io) {
  Status s;
  Slice key = GetSliceForFileNumber(&file_number);
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    if (no_io) { // Dont do IO and return a not-found status
      return Status::Incomplete("Table not found in table_cache, no_io is set");
    }
    if (table_io != nullptr) {
      *table_io = true;    // we had to do IO from storage
    }
    std::string fname = TableFileName(dbname_, file_number);
    unique_ptr<RandomAccessFile> file;
    unique_ptr<TableReader> table_reader;
    s = env_->NewRandomAccessFile(fname, &file, toptions);
    RecordTick(options_->statistics.get(), NO_FILE_OPENS);
    if (s.ok()) {
      if (options_->advise_random_on_open) {
        file->Hint(RandomAccessFile::RANDOM);
      }
      StopWatch sw(env_, options_->statistics.get(), TABLE_OPEN_IO_MICROS);
      s = options_->table_factory->NewTableReader(
          *options_, toptions, internal_comparator, std::move(file), file_size,
          &table_reader);
    }

    if (!s.ok()) {
      assert(table_reader == nullptr);
      RecordTick(options_->statistics.get(), NO_FILE_ERRORS);
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      assert(file.get() == nullptr);
      *handle = cache_->Insert(key, table_reader.release(), 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  const EnvOptions& toptions,
                                  const InternalKeyComparator& icomparator,
                                  const FileMetaData& file_meta,
                                  TableReader** table_reader_ptr,
                                  bool for_compaction) {
  if (table_reader_ptr != nullptr) {
    *table_reader_ptr = nullptr;
  }
  TableReader* table_reader = file_meta.table_reader;
  Cache::Handle* handle = nullptr;
  Status s;
  if (table_reader == nullptr) {
    s = FindTable(toptions, icomparator, file_meta.number, file_meta.file_size,
                  &handle, nullptr, options.read_tier == kBlockCacheTier);
    if (!s.ok()) {
      return NewErrorIterator(s);
    }
    table_reader = GetTableReaderFromHandle(handle);
  }

  Iterator* result = table_reader->NewIterator(options);
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
                       const FileMetaData& file_meta, const Slice& k, void* arg,
                       bool (*saver)(void*, const ParsedInternalKey&,
                                     const Slice&, bool),
                       bool* table_io, void (*mark_key_may_exist)(void*)) {
  TableReader* t = file_meta.table_reader;
  Status s;
  Cache::Handle* handle = nullptr;
  if (!t) {
    s = FindTable(storage_options_, internal_comparator, file_meta.number,
                  file_meta.file_size, &handle, table_io,
                  options.read_tier == kBlockCacheTier);
    if (s.ok()) {
      t = GetTableReaderFromHandle(handle);
    }
  }
  if (s.ok()) {
    s = t->Get(options, k, arg, saver, mark_key_may_exist);
    if (handle != nullptr) {
      ReleaseHandle(handle);
    }
  } else if (options.read_tier && s.IsIncomplete()) {
    // Couldnt find Table in cache but treat as kFound if no_io set
    (*mark_key_may_exist)(arg);
    return Status::OK();
  }
  return s;
}
Status TableCache::GetTableProperties(
    const EnvOptions& toptions,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta,
    std::shared_ptr<const TableProperties>* properties, bool no_io) {
  Status s;
  auto table_reader = file_meta.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    *properties = table_reader->GetTableProperties();

    return s;
  }

  bool table_io;
  Cache::Handle* table_handle = nullptr;
  s = FindTable(toptions, internal_comparator, file_meta.number,
                file_meta.file_size, &table_handle, &table_io, no_io);
  if (!s.ok()) {
    return s;
  }
  assert(table_handle);
  auto table = GetTableReaderFromHandle(table_handle);
  *properties = table->GetTableProperties();
  ReleaseHandle(table_handle);
  return s;
}

void TableCache::Evict(Cache* cache, uint64_t file_number) {
  cache->Erase(GetSliceForFileNumber(&file_number));
}

}  // namespace rocksdb
