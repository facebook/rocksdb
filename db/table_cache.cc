//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/dbformat.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/statistics.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/multiget_context.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

namespace {

template <class T>
static void DeleteEntry(const Slice& /*key*/, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
  delete typed_value;
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

#ifndef ROCKSDB_LITE

void AppendVarint64(IterKey* key, uint64_t v) {
  char buf[10];
  auto ptr = EncodeVarint64(buf, v);
  key->TrimAppend(key->Size(), buf, ptr - buf);
}

#endif  // ROCKSDB_LITE

}  // namespace

const int kLoadConcurency = 128;

TableCache::TableCache(const ImmutableCFOptions& ioptions,
                       const FileOptions& file_options, Cache* const cache,
                       BlockCacheTracer* const block_cache_tracer)
    : ioptions_(ioptions),
      file_options_(file_options),
      cache_(cache),
      immortal_tables_(false),
      block_cache_tracer_(block_cache_tracer),
      loader_mutex_(kLoadConcurency, GetSliceNPHash64) {
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
    const FileOptions& file_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    bool sequential_mode, bool record_read_stats, HistogramImpl* file_read_hist,
    std::unique_ptr<TableReader>* table_reader,
    const SliceTransform* prefix_extractor, bool skip_filters, int level,
    bool prefetch_index_and_filter_in_cache) {
  std::string fname =
      TableFileName(ioptions_.cf_paths, fd.GetNumber(), fd.GetPathId());
  std::unique_ptr<FSRandomAccessFile> file;
  Status s = ioptions_.fs->NewRandomAccessFile(fname, file_options, &file,
                                               nullptr);
  RecordTick(ioptions_.statistics, NO_FILE_OPENS);
  if (s.IsPathNotFound()) {
    fname = Rocks2LevelTableFileName(fname);
    s = ioptions_.fs->NewRandomAccessFile(fname, file_options, &file, nullptr);
    RecordTick(ioptions_.statistics, NO_FILE_OPENS);
  }

  if (s.ok()) {
    if (!sequential_mode && ioptions_.advise_random_on_open) {
      file->Hint(FSRandomAccessFile::kRandom);
    }
    StopWatch sw(ioptions_.env, ioptions_.statistics, TABLE_OPEN_IO_MICROS);
    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(
            std::move(file), fname, ioptions_.env,
            record_read_stats ? ioptions_.statistics : nullptr, SST_READ_MICROS,
            file_read_hist, ioptions_.rate_limiter, ioptions_.listeners));
    s = ioptions_.table_factory->NewTableReader(
        TableReaderOptions(ioptions_, prefix_extractor, file_options,
                           internal_comparator, skip_filters, immortal_tables_,
                           level, fd.largest_seqno, block_cache_tracer_),
        std::move(file_reader), fd.GetFileSize(), table_reader,
        prefetch_index_and_filter_in_cache);
    TEST_SYNC_POINT("TableCache::GetTableReader:0");
  }
  return s;
}

void TableCache::EraseHandle(const FileDescriptor& fd, Cache::Handle* handle) {
  ReleaseHandle(handle);
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  cache_->Erase(key);
}

Status TableCache::FindTable(const FileOptions& file_options,
                             const InternalKeyComparator& internal_comparator,
                             const FileDescriptor& fd, Cache::Handle** handle,
                             const SliceTransform* prefix_extractor,
                             const bool no_io, bool record_read_stats,
                             HistogramImpl* file_read_hist, bool skip_filters,
                             int level,
                             bool prefetch_index_and_filter_in_cache) {
  PERF_TIMER_GUARD_WITH_ENV(find_table_nanos, ioptions_.env);
  Status s;
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  *handle = cache_->Lookup(key);
  TEST_SYNC_POINT_CALLBACK("TableCache::FindTable:0",
                           const_cast<bool*>(&no_io));

  if (*handle == nullptr) {
    if (no_io) {  // Don't do IO and return a not-found status
      return Status::Incomplete("Table not found in table_cache, no_io is set");
    }
    MutexLock load_lock(loader_mutex_.get(key));
    // We check the cache again under loading mutex
    *handle = cache_->Lookup(key);
    if (*handle != nullptr) {
      return s;
    }

    std::unique_ptr<TableReader> table_reader;
    s = GetTableReader(file_options, internal_comparator, fd,
                       false /* sequential mode */, record_read_stats,
                       file_read_hist, &table_reader, prefix_extractor,
                       skip_filters, level, prefetch_index_and_filter_in_cache);
    if (!s.ok()) {
      assert(table_reader == nullptr);
      RecordTick(ioptions_.statistics, NO_FILE_ERRORS);
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      s = cache_->Insert(key, table_reader.get(), 1, &DeleteEntry<TableReader>,
                         handle);
      if (s.ok()) {
        // Release ownership of table reader.
        table_reader.release();
      }
    }
  }
  return s;
}

InternalIterator* TableCache::NewIterator(
    const ReadOptions& options, const FileOptions& file_options,
    const InternalKeyComparator& icomparator, const FileMetaData& file_meta,
    RangeDelAggregator* range_del_agg, const SliceTransform* prefix_extractor,
    TableReader** table_reader_ptr, HistogramImpl* file_read_hist,
    TableReaderCaller caller, Arena* arena, bool skip_filters, int level,
    const InternalKey* smallest_compaction_key,
    const InternalKey* largest_compaction_key, bool allow_unprepared_value) {
  PERF_TIMER_GUARD(new_table_iterator_nanos);

  Status s;
  TableReader* table_reader = nullptr;
  Cache::Handle* handle = nullptr;
  if (table_reader_ptr != nullptr) {
    *table_reader_ptr = nullptr;
  }
  bool for_compaction = caller == TableReaderCaller::kCompaction;
  auto& fd = file_meta.fd;
  table_reader = fd.table_reader;
  if (table_reader == nullptr) {
    s = FindTable(file_options, icomparator, fd, &handle, prefix_extractor,
                  options.read_tier == kBlockCacheTier /* no_io */,
                  !for_compaction /* record_read_stats */, file_read_hist,
                  skip_filters, level);
    if (s.ok()) {
      table_reader = GetTableReaderFromHandle(handle);
    }
  }
  InternalIterator* result = nullptr;
  if (s.ok()) {
    if (options.table_filter &&
        !options.table_filter(*table_reader->GetTableProperties())) {
      result = NewEmptyInternalIterator<Slice>(arena);
    } else {
      result = table_reader->NewIterator(options, prefix_extractor, arena,
                                   skip_filters, caller,
                                   file_options.compaction_readahead_size,
                                   allow_unprepared_value);
    }
    if (handle != nullptr) {
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
    if (range_del_agg->AddFile(fd.GetNumber())) {
      std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
          static_cast<FragmentedRangeTombstoneIterator*>(
              table_reader->NewRangeTombstoneIterator(options)));
      if (range_del_iter != nullptr) {
        s = range_del_iter->status();
      }
      if (s.ok()) {
        const InternalKey* smallest = &file_meta.smallest;
        const InternalKey* largest = &file_meta.largest;
        if (smallest_compaction_key != nullptr) {
          smallest = smallest_compaction_key;
        }
        if (largest_compaction_key != nullptr) {
          largest = largest_compaction_key;
        }
        range_del_agg->AddTombstones(std::move(range_del_iter), smallest,
                                     largest);
      }
    }
  }

  if (handle != nullptr) {
    ReleaseHandle(handle);
  }
  if (!s.ok()) {
    assert(result == nullptr);
    result = NewErrorInternalIterator<Slice>(s, arena);
  }
  return result;
}

Status TableCache::GetRangeTombstoneIterator(
    const ReadOptions& options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta,
    std::unique_ptr<FragmentedRangeTombstoneIterator>* out_iter) {
  const FileDescriptor& fd = file_meta.fd;
  Status s;
  TableReader* t = fd.table_reader;
  Cache::Handle* handle = nullptr;
  if (t == nullptr) {
    s = FindTable(file_options_, internal_comparator, fd, &handle);
    if (s.ok()) {
      t = GetTableReaderFromHandle(handle);
    }
  }
  if (s.ok()) {
    out_iter->reset(t->NewRangeTombstoneIterator(options));
    assert(out_iter);
  }
  return s;
}

#ifndef ROCKSDB_LITE
void TableCache::CreateRowCacheKeyPrefix(const ReadOptions& options,
                                         const FileDescriptor& fd,
                                         const Slice& internal_key,
                                         GetContext* get_context,
                                         IterKey& row_cache_key) {
  uint64_t fd_number = fd.GetNumber();
  // We use the user key as cache key instead of the internal key,
  // otherwise the whole cache would be invalidated every time the
  // sequence key increases. However, to support caching snapshot
  // reads, we append the sequence number (incremented by 1 to
  // distinguish from 0) only in this case.
  // If the snapshot is larger than the largest seqno in the file,
  // all data should be exposed to the snapshot, so we treat it
  // the same as there is no snapshot. The exception is that if
  // a seq-checking callback is registered, some internal keys
  // may still be filtered out.
  uint64_t seq_no = 0;
  // Maybe we can include the whole file ifsnapshot == fd.largest_seqno.
  if (options.snapshot != nullptr &&
      (get_context->has_callback() ||
       static_cast_with_check<const SnapshotImpl>(options.snapshot)
               ->GetSequenceNumber() <= fd.largest_seqno)) {
    // We should consider to use options.snapshot->GetSequenceNumber()
    // instead of GetInternalKeySeqno(k), which will make the code
    // easier to understand.
    seq_no = 1 + GetInternalKeySeqno(internal_key);
  }

  // Compute row cache key.
  row_cache_key.TrimAppend(row_cache_key.Size(), row_cache_id_.data(),
                           row_cache_id_.size());
  AppendVarint64(&row_cache_key, fd_number);
  AppendVarint64(&row_cache_key, seq_no);
}

bool TableCache::GetFromRowCache(const Slice& user_key, IterKey& row_cache_key,
                                 size_t prefix_size, GetContext* get_context) {
  bool found = false;

  row_cache_key.TrimAppend(prefix_size, user_key.data(), user_key.size());
  if (auto row_handle =
          ioptions_.row_cache->Lookup(row_cache_key.GetUserKey())) {
    // Cleanable routine to release the cache entry
    Cleanable value_pinner;
    auto release_cache_entry_func = [](void* cache_to_clean,
                                       void* cache_handle) {
      ((Cache*)cache_to_clean)->Release((Cache::Handle*)cache_handle);
    };
    auto found_row_cache_entry =
        static_cast<const std::string*>(ioptions_.row_cache->Value(row_handle));
    // If it comes here value is located on the cache.
    // found_row_cache_entry points to the value on cache,
    // and value_pinner has cleanup procedure for the cached entry.
    // After replayGetContextLog() returns, get_context.pinnable_slice_
    // will point to cache entry buffer (or a copy based on that) and
    // cleanup routine under value_pinner will be delegated to
    // get_context.pinnable_slice_. Cache entry is released when
    // get_context.pinnable_slice_ is reset.
    value_pinner.RegisterCleanup(release_cache_entry_func,
                                 ioptions_.row_cache.get(), row_handle);
    replayGetContextLog(*found_row_cache_entry, user_key, get_context,
                        &value_pinner);
    RecordTick(ioptions_.statistics, ROW_CACHE_HIT);
    found = true;
  } else {
    RecordTick(ioptions_.statistics, ROW_CACHE_MISS);
  }
  return found;
}
#endif  // ROCKSDB_LITE

Status TableCache::Get(const ReadOptions& options,
                       const InternalKeyComparator& internal_comparator,
                       const FileMetaData& file_meta, const Slice& k,
                       GetContext* get_context,
                       const SliceTransform* prefix_extractor,
                       HistogramImpl* file_read_hist, bool skip_filters,
                       int level) {
  auto& fd = file_meta.fd;
  std::string* row_cache_entry = nullptr;
  bool done = false;
#ifndef ROCKSDB_LITE
  IterKey row_cache_key;
  std::string row_cache_entry_buffer;

  // Check row cache if enabled. Since row cache does not currently store
  // sequence numbers, we cannot use it if we need to fetch the sequence.
  if (ioptions_.row_cache && !get_context->NeedToReadSequence()) {
    auto user_key = ExtractUserKey(k);
    CreateRowCacheKeyPrefix(options, fd, k, get_context, row_cache_key);
    done = GetFromRowCache(user_key, row_cache_key, row_cache_key.Size(),
                           get_context);
    if (!done) {
      row_cache_entry = &row_cache_entry_buffer;
    }
  }
#endif  // ROCKSDB_LITE
  Status s;
  TableReader* t = fd.table_reader;
  Cache::Handle* handle = nullptr;
  if (!done && s.ok()) {
    if (t == nullptr) {
      s = FindTable(
          file_options_, internal_comparator, fd, &handle, prefix_extractor,
          options.read_tier == kBlockCacheTier /* no_io */,
          true /* record_read_stats */, file_read_hist, skip_filters, level);
      if (s.ok()) {
        t = GetTableReaderFromHandle(handle);
      }
    }
    SequenceNumber* max_covering_tombstone_seq =
        get_context->max_covering_tombstone_seq();
    if (s.ok() && max_covering_tombstone_seq != nullptr &&
        !options.ignore_range_deletions) {
      std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
          t->NewRangeTombstoneIterator(options));
      if (range_del_iter != nullptr) {
        *max_covering_tombstone_seq = std::max(
            *max_covering_tombstone_seq,
            range_del_iter->MaxCoveringTombstoneSeqnum(ExtractUserKey(k)));
      }
    }
    if (s.ok()) {
      get_context->SetReplayLog(row_cache_entry);  // nullptr if no cache.
      s = t->Get(options, k, get_context, prefix_extractor, skip_filters);
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

// Batched version of TableCache::MultiGet.
Status TableCache::MultiGet(const ReadOptions& options,
                            const InternalKeyComparator& internal_comparator,
                            const FileMetaData& file_meta,
                            const MultiGetContext::Range* mget_range,
                            const SliceTransform* prefix_extractor,
                            HistogramImpl* file_read_hist, bool skip_filters,
                            int level) {
  auto& fd = file_meta.fd;
  Status s;
  TableReader* t = fd.table_reader;
  Cache::Handle* handle = nullptr;
  MultiGetRange table_range(*mget_range, mget_range->begin(),
                            mget_range->end());
#ifndef ROCKSDB_LITE
  autovector<std::string, MultiGetContext::MAX_BATCH_SIZE> row_cache_entries;
  IterKey row_cache_key;
  size_t row_cache_key_prefix_size = 0;
  KeyContext& first_key = *table_range.begin();
  bool lookup_row_cache =
      ioptions_.row_cache && !first_key.get_context->NeedToReadSequence();

  // Check row cache if enabled. Since row cache does not currently store
  // sequence numbers, we cannot use it if we need to fetch the sequence.
  if (lookup_row_cache) {
    GetContext* first_context = first_key.get_context;
    CreateRowCacheKeyPrefix(options, fd, first_key.ikey, first_context,
                            row_cache_key);
    row_cache_key_prefix_size = row_cache_key.Size();

    for (auto miter = table_range.begin(); miter != table_range.end();
         ++miter) {
      const Slice& user_key = miter->ukey;
      ;
      GetContext* get_context = miter->get_context;

      if (GetFromRowCache(user_key, row_cache_key, row_cache_key_prefix_size,
                          get_context)) {
        table_range.SkipKey(miter);
      } else {
        row_cache_entries.emplace_back();
        get_context->SetReplayLog(&(row_cache_entries.back()));
      }
    }
  }
#endif  // ROCKSDB_LITE

  // Check that table_range is not empty. Its possible all keys may have been
  // found in the row cache and thus the range may now be empty
  if (s.ok() && !table_range.empty()) {
    if (t == nullptr) {
      s = FindTable(
          file_options_, internal_comparator, fd, &handle, prefix_extractor,
          options.read_tier == kBlockCacheTier /* no_io */,
          true /* record_read_stats */, file_read_hist, skip_filters, level);
      TEST_SYNC_POINT_CALLBACK("TableCache::MultiGet:FindTable", &s);
      if (s.ok()) {
        t = GetTableReaderFromHandle(handle);
        assert(t);
      }
    }
    if (s.ok() && !options.ignore_range_deletions) {
      std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
          t->NewRangeTombstoneIterator(options));
      if (range_del_iter != nullptr) {
        for (auto iter = table_range.begin(); iter != table_range.end();
             ++iter) {
          SequenceNumber* max_covering_tombstone_seq =
              iter->get_context->max_covering_tombstone_seq();
          *max_covering_tombstone_seq =
              std::max(*max_covering_tombstone_seq,
                       range_del_iter->MaxCoveringTombstoneSeqnum(iter->ukey));
        }
      }
    }
    if (s.ok()) {
      t->MultiGet(options, &table_range, prefix_extractor, skip_filters);
    } else if (options.read_tier == kBlockCacheTier && s.IsIncomplete()) {
      for (auto iter = table_range.begin(); iter != table_range.end(); ++iter) {
        Status* status = iter->s;
        if (status->IsIncomplete()) {
          // Couldn't find Table in cache but treat as kFound if no_io set
          iter->get_context->MarkKeyMayExist();
          s = Status::OK();
        }
      }
    }
  }

#ifndef ROCKSDB_LITE
  if (lookup_row_cache) {
    size_t row_idx = 0;

    for (auto miter = table_range.begin(); miter != table_range.end();
         ++miter) {
      std::string& row_cache_entry = row_cache_entries[row_idx++];
      const Slice& user_key = miter->ukey;
      ;
      GetContext* get_context = miter->get_context;

      get_context->SetReplayLog(nullptr);
      // Compute row cache key.
      row_cache_key.TrimAppend(row_cache_key_prefix_size, user_key.data(),
                               user_key.size());
      // Put the replay log in row cache only if something was found.
      if (s.ok() && !row_cache_entry.empty()) {
        size_t charge =
            row_cache_key.Size() + row_cache_entry.size() + sizeof(std::string);
        void* row_ptr = new std::string(std::move(row_cache_entry));
        ioptions_.row_cache->Insert(row_cache_key.GetUserKey(), row_ptr, charge,
                                    &DeleteEntry<std::string>);
      }
    }
  }
#endif  // ROCKSDB_LITE

  if (handle != nullptr) {
    ReleaseHandle(handle);
  }
  return s;
}

Status TableCache::GetTableProperties(
    const FileOptions& file_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    std::shared_ptr<const TableProperties>* properties,
    const SliceTransform* prefix_extractor, bool no_io) {
  Status s;
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    *properties = table_reader->GetTableProperties();

    return s;
  }

  Cache::Handle* table_handle = nullptr;
  s = FindTable(file_options, internal_comparator, fd, &table_handle,
                prefix_extractor, no_io);
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
    const FileOptions& file_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    const SliceTransform* prefix_extractor) {
  Status s;
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    return table_reader->ApproximateMemoryUsage();
  }

  Cache::Handle* table_handle = nullptr;
  s = FindTable(file_options, internal_comparator, fd, &table_handle,
                prefix_extractor, true);
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

uint64_t TableCache::ApproximateOffsetOf(
    const Slice& key, const FileDescriptor& fd, TableReaderCaller caller,
    const InternalKeyComparator& internal_comparator,
    const SliceTransform* prefix_extractor) {
  uint64_t result = 0;
  TableReader* table_reader = fd.table_reader;
  Cache::Handle* table_handle = nullptr;
  if (table_reader == nullptr) {
    const bool for_compaction = (caller == TableReaderCaller::kCompaction);
    Status s = FindTable(file_options_, internal_comparator, fd, &table_handle,
                         prefix_extractor, false /* no_io */,
                         !for_compaction /* record_read_stats */);
    if (s.ok()) {
      table_reader = GetTableReaderFromHandle(table_handle);
    }
  }

  if (table_reader != nullptr) {
    result = table_reader->ApproximateOffsetOf(key, caller);
  }
  if (table_handle != nullptr) {
    ReleaseHandle(table_handle);
  }

  return result;
}

uint64_t TableCache::ApproximateSize(
    const Slice& start, const Slice& end, const FileDescriptor& fd,
    TableReaderCaller caller, const InternalKeyComparator& internal_comparator,
    const SliceTransform* prefix_extractor) {
  uint64_t result = 0;
  TableReader* table_reader = fd.table_reader;
  Cache::Handle* table_handle = nullptr;
  if (table_reader == nullptr) {
    const bool for_compaction = (caller == TableReaderCaller::kCompaction);
    Status s = FindTable(file_options_, internal_comparator, fd, &table_handle,
                         prefix_extractor, false /* no_io */,
                         !for_compaction /* record_read_stats */);
    if (s.ok()) {
      table_reader = GetTableReaderFromHandle(table_handle);
    }
  }

  if (table_reader != nullptr) {
    result = table_reader->ApproximateSize(start, end, caller);
  }
  if (table_handle != nullptr) {
    ReleaseHandle(table_handle);
  }

  return result;
}
}  // namespace ROCKSDB_NAMESPACE
