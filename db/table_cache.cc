// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"

#include "rocksdb/statistics.h"
#include "table/table.h"
#include "util/coding.h"
#include "util/stop_watch.h"

namespace leveldb {

static void DeleteEntry(const Slice& key, void* value) {
  Table* table = reinterpret_cast<Table*>(value);
  delete table;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       const EnvOptions& storage_options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      storage_options_(storage_options),
      cache_(NewLRUCache(entries, options->table_cache_numshardbits)) {}

TableCache::~TableCache() {
}

Status TableCache::FindTable(const EnvOptions& toptions,
                             uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle, bool* table_io,
                             const bool no_io) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    if (no_io) { // Dont do IO and return a not-found status
      return Status::NotFound("Table not found in table_cache, no_io is set");
    }
    if (table_io != nullptr) {
      *table_io = true;    // we had to do IO from storage
    }
    std::string fname = TableFileName(dbname_, file_number);
    unique_ptr<RandomAccessFile> file;
    unique_ptr<Table> table;
    s = env_->NewRandomAccessFile(fname, &file, toptions);
    RecordTick(options_->statistics, NO_FILE_OPENS);
    if (s.ok()) {
      if (options_->advise_random_on_open) {
        file->Hint(RandomAccessFile::RANDOM);
      }
      StopWatch sw(env_, options_->statistics, TABLE_OPEN_IO_MICROS);
      s = Table::Open(*options_, toptions, std::move(file), file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      RecordTick(options_->statistics, NO_FILE_ERRORS);
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      assert(file.get() == nullptr);
      *handle = cache_->Insert(key, table.release(), 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  const EnvOptions& toptions,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr,
                                  bool for_compaction) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(toptions, file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table =
    reinterpret_cast<Table*>(cache_->Value(handle));
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_.get(), handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }

  if (for_compaction) {
    table->SetupForCompaction();
  }

  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       bool (*saver)(void*, const Slice&, const Slice&, bool),
                       bool* table_io,
                       void (*mark_key_may_exist)(void*),
                       const bool no_io) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(storage_options_, file_number, file_size,
                       &handle, table_io, no_io);
  if (s.ok()) {
    Table* t =
      reinterpret_cast<Table*>(cache_->Value(handle));
    s = t->InternalGet(options, k, arg, saver, mark_key_may_exist, no_io);
    cache_->Release(handle);
  } else if (no_io && s.IsNotFound()) {
    // Couldnt find Table in cache but treat as kFound if no_io set
    (*mark_key_may_exist)(arg);
    return Status::OK();
  }
  return s;
}

bool TableCache::PrefixMayMatch(const ReadOptions& options,
                                uint64_t file_number,
                                uint64_t file_size,
                                const Slice& internal_prefix,
                                bool* table_io) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(storage_options_, file_number,
                       file_size, &handle, table_io);
  bool may_match = true;
  if (s.ok()) {
    Table* t =
      reinterpret_cast<Table*>(cache_->Value(handle));
    may_match = t->PrefixMayMatch(internal_prefix);
    cache_->Release(handle);
  }
  return may_match;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
