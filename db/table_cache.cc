// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"

#include "leveldb/table.h"
#include "leveldb/statistics.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  unique_ptr<Table> table;
};

static class Statistics* dbstatistics;

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  RecordTick(dbstatistics, NO_FILE_CLOSES);
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       const StorageOptions& storage_options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      storage_options_(storage_options),
      cache_(NewLRUCache(entries, options->table_cache_numshardbits)) {
  dbstatistics = options->statistics;
}

TableCache::~TableCache() {
}

Status TableCache::FindTable(const EnvOptions& toptions,
                             uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle, bool* tableIO) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    if (tableIO != nullptr) {
      *tableIO = true;    // we had to do IO from storage
    }
    std::string fname = TableFileName(dbname_, file_number);
    unique_ptr<RandomAccessFile> file;
    unique_ptr<Table> table;
    s = env_->NewRandomAccessFile(fname, &file, toptions);
    RecordTick(options_->statistics, NO_FILE_OPENS);
    if (s.ok()) {
      s = Table::Open(*options_, toptions, std::move(file), file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      RecordTick(options_->statistics, NO_FILE_ERRORS);
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->table = std::move(table);
      assert(file.get() == nullptr);
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  const EnvOptions& toptions,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(toptions, file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table =
    reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table.get();
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_.get(), handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&, bool),
                       bool* tableIO) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(storage_options_, file_number, file_size,
                       &handle, tableIO);
  if (s.ok()) {
    Table* t =
      reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table.get();
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
