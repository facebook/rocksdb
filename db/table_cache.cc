//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/dbformat.h"
#include "db/table_cache_request.h"
#include "db/version_edit.h"


#include "monitoring/perf_context_imp.h"
#include "rocksdb/statistics.h"

#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

namespace rocksdb {

namespace table_cache_detail  {

void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

void DeleteTableReader(void* arg1, void* arg2) {
  TableReader* table_reader = reinterpret_cast<TableReader*>(arg1);
  delete table_reader;
}

#ifndef ROCKSDB_LITE

void AppendVarint64(IterKey* key, uint64_t v) {
  char buf[10];
  auto ptr = EncodeVarint64(buf, v);
  key->TrimAppend(key->Size(), buf, ptr - buf);
}

#endif  // ROCKSDB_LITE

}  // namespace table_cache_detail

using namespace table_cache_detail;

TableCache::TableCache(const ImmutableCFOptions& ioptions,
                       const EnvOptions& env_options, Cache* const cache)
    : ioptions_(ioptions), env_options_(env_options), cache_(cache) {
  if (ioptions_.row_cache) {
    // If the same cache is shared by multiple instances, we need to
    // disambiguate its entries.
    PutVarint64(&row_cache_id_, ioptions_.row_cache->NewId());
  }
}

TableCache::~TableCache() {
}

TableReader* TableCache::GetTableReaderFromHandle(Cache::Handle* handle) {
  return reinterpret_cast<TableReader*>(cache_->Value(handle));
}

void TableCache::ReleaseHandle(Cache::Handle* handle) {
  cache_->Release(handle);
}

Status TableCache::GetTableReader(
  const EnvOptions& env_options,
  const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
  bool sequential_mode, size_t readahead, bool record_read_stats,
  HistogramImpl* file_read_hist, std::unique_ptr<TableReader>* table_reader,
  bool skip_filters, int level, bool prefetch_index_and_filter_in_cache) {

  using namespace async;
  Status s;

  const TableCacheGetReaderHelper::Callback empty_cb;
  TableCacheGetReaderHelper gr_helper(ioptions_);
  s = gr_helper.GetTableReader(empty_cb, env_options,
    internal_comparator, fd,
    sequential_mode, readahead, record_read_stats,
    file_read_hist, table_reader,
    skip_filters, level, prefetch_index_and_filter_in_cache);

  s = gr_helper.OnGetReaderComplete(s);

  return s;
}


void TableCache::EraseHandle(const FileDescriptor& fd, Cache::Handle* handle) {
  ReleaseHandle(handle);
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  cache_->Erase(key);
}

Status TableCache::FindTable(
  const EnvOptions& env_options,
  const InternalKeyComparator& internal_comparator,
  const FileDescriptor& fd, Cache::Handle** handle,
  const bool no_io, bool record_read_stats,
  HistogramImpl* file_read_hist, bool skip_filters,
  int level,
  bool prefetch_index_and_filter_in_cache) {

  using namespace async;

  Status s = TableCacheFindTableHelper::LookupCache(fd, cache_, handle, no_io);

  // Lookup returns Incomplete if no_io is true
  if (s.IsNotFound()) {
    std::unique_ptr<TableReader> table_reader;
    TableCacheGetReaderHelper::Callback empty_cb;
    TableCacheFindTableHelper find_helper(ioptions_, fd.GetNumber(), cache_);
    s = find_helper.GetReader(empty_cb, env_options, internal_comparator, fd,
      &table_reader, record_read_stats, file_read_hist, skip_filters, level,
      prefetch_index_and_filter_in_cache);

    s = find_helper.OnGetReaderComplete(s, handle, table_reader);
  }

  return s;
}

InternalIterator* TableCache::NewIterator(
    const ReadOptions& options, const EnvOptions& env_options,
    const InternalKeyComparator& icomparator, const FileDescriptor& fd,
    RangeDelAggregator* range_del_agg, TableReader** table_reader_ptr,
    HistogramImpl* file_read_hist, bool for_compaction, Arena* arena,
    bool skip_filters, int level) {
  PERF_TIMER_GUARD(new_table_iterator_nanos);

  Status s;
  bool create_new_table_reader = false;
  TableReader* table_reader = nullptr;
  Cache::Handle* handle = nullptr;
  if (s.ok()) {
    if (table_reader_ptr != nullptr) {
      *table_reader_ptr = nullptr;
    }
    size_t readahead = 0;
    if (for_compaction) {
#ifndef NDEBUG
      bool use_direct_reads_for_compaction = env_options.use_direct_reads;
      TEST_SYNC_POINT_CALLBACK("TableCache::NewIterator:for_compaction",
                               &use_direct_reads_for_compaction);
#endif  // !NDEBUG
      if (ioptions_.new_table_reader_for_compaction_inputs) {
        readahead = ioptions_.compaction_readahead_size;
        create_new_table_reader = true;
      }
    } else {
      readahead = options.readahead_size;
      create_new_table_reader = readahead > 0;
    }

    if (create_new_table_reader) {
      unique_ptr<TableReader> table_reader_unique_ptr;
      s = GetTableReader(
          env_options, icomparator, fd, true /* sequential_mode */, readahead,
          !for_compaction /* record stats */, nullptr, &table_reader_unique_ptr,
          false /* skip_filters */, level);
      if (s.ok()) {
        table_reader = table_reader_unique_ptr.release();
      }
    } else {
      table_reader = fd.table_reader;
      if (table_reader == nullptr) {
        s = FindTable(env_options, icomparator, fd, &handle,
                      options.read_tier == kBlockCacheTier /* no_io */,
                      !for_compaction /* record read_stats */, file_read_hist,
                      skip_filters, level);
        if (s.ok()) {
          table_reader = GetTableReaderFromHandle(handle);
        }
      }
    }
  }
  InternalIterator* result = nullptr;
  if (s.ok()) {
    result = table_reader->NewIterator(options, arena, skip_filters);
    if (create_new_table_reader) {
      assert(handle == nullptr);
      result->RegisterCleanup(&DeleteTableReader, table_reader, nullptr);
    } else if (handle != nullptr) {
      result->RegisterCleanup(&UnrefEntry, cache_, handle);
      handle = nullptr;  // prevent from releasing below
    }

    if (for_compaction) {
      table_reader->SetupForCompaction();
    }
    if (table_reader_ptr != nullptr) {
      *table_reader_ptr = table_reader;
    }
  }
  if (s.ok() && range_del_agg != nullptr && !options.ignore_range_deletions) {
    std::unique_ptr<InternalIterator> range_del_iter(
        table_reader->NewRangeTombstoneIterator(options));
    if (range_del_iter != nullptr) {
      s = range_del_iter->status();
    }
    if (s.ok()) {
      s = range_del_agg->AddTombstones(std::move(range_del_iter));
    }
  }

  if (handle != nullptr) {
    ReleaseHandle(handle);
  }
  if (!s.ok()) {
    assert(result == nullptr);
    result = NewErrorInternalIterator(s, arena);
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       const InternalKeyComparator& internal_comparator,
                       const FileDescriptor& fd, const Slice& k,
                       GetContext* get_context, HistogramImpl* file_read_hist,
                       bool skip_filters, int level) {
  std::string* row_cache_entry = nullptr;
  bool done = false;
#ifndef ROCKSDB_LITE
  IterKey row_cache_key;
  std::string row_cache_entry_buffer;
  // Check row cache if enabled. Since row cache does not currently store
  // sequence numbers, we cannot use it if we need to fetch the sequence.
  if (ioptions_.row_cache && !get_context->NeedToReadSequence()) {
    uint64_t fd_number = fd.GetNumber();
    auto user_key = ExtractUserKey(k);
    // We use the user key as cache key instead of the internal key,
    // otherwise the whole cache would be invalidated every time the
    // sequence key increases. However, to support caching snapshot
    // reads, we append the sequence number (incremented by 1 to
    // distinguish from 0) only in this case.
    uint64_t seq_no =
        options.snapshot == nullptr ? 0 : 1 + GetInternalKeySeqno(k);

    // Compute row cache key.
    row_cache_key.TrimAppend(row_cache_key.Size(), row_cache_id_.data(),
                             row_cache_id_.size());
    AppendVarint64(&row_cache_key, fd_number);
    AppendVarint64(&row_cache_key, seq_no);
    row_cache_key.TrimAppend(row_cache_key.Size(), user_key.data(),
                             user_key.size());

    if (auto row_handle =
            ioptions_.row_cache->Lookup(row_cache_key.GetUserKey())) {
      auto found_row_cache_entry = static_cast<const std::string*>(
          ioptions_.row_cache->Value(row_handle));
      replayGetContextLog(*found_row_cache_entry, user_key, get_context);
      ioptions_.row_cache->Release(row_handle);
      RecordTick(ioptions_.statistics, ROW_CACHE_HIT);
      done = true;
    } else {
      // Not found, setting up the replay log.
      RecordTick(ioptions_.statistics, ROW_CACHE_MISS);
      row_cache_entry = &row_cache_entry_buffer;
    }
  }
#endif  // ROCKSDB_LITE
  Status s;
  TableReader* t = fd.table_reader;
  Cache::Handle* handle = nullptr;
  if (!done && s.ok()) {
    if (t == nullptr) {
      s = FindTable(env_options_, internal_comparator, fd, &handle,
                    options.read_tier == kBlockCacheTier /* no_io */,
                    true /* record_read_stats */, file_read_hist, skip_filters,
                    level);
      if (s.ok()) {
        t = GetTableReaderFromHandle(handle);
      }
    }
    if (s.ok() && get_context->range_del_agg() != nullptr &&
        !options.ignore_range_deletions) {
      std::unique_ptr<InternalIterator> range_del_iter(
          t->NewRangeTombstoneIterator(options));
      if (range_del_iter != nullptr) {
        s = range_del_iter->status();
      }
      if (s.ok()) {
        s = get_context->range_del_agg()->AddTombstones(
            std::move(range_del_iter));
      }
    }
    if (s.ok()) {
      get_context->SetReplayLog(row_cache_entry);  // nullptr if no cache.
      s = t->Get(options, k, get_context, skip_filters);
      get_context->SetReplayLog(nullptr);
    } else if (options.read_tier == kBlockCacheTier && s.IsIncomplete()) {
      // Couldn't find Table in cache but treat as kFound if no_io set
      get_context->MarkKeyMayExist();
      s = Status::OK();
      done = true;
    }
  }

#ifndef ROCKSDB_LITE
  // Put the replay log in row cache only if something was found.
  if (!done && s.ok() && row_cache_entry && !row_cache_entry->empty()) {
    size_t charge =
        row_cache_key.Size() + row_cache_entry->size() + sizeof(std::string);
    void* row_ptr = new std::string(std::move(*row_cache_entry));
    ioptions_.row_cache->Insert(row_cache_key.GetUserKey(), row_ptr, charge,
                                &DeleteEntry<std::string>);
  }
#endif  // ROCKSDB_LITE

  if (handle != nullptr) {
    ReleaseHandle(handle);
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
