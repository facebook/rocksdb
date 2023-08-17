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
#include "file/file_util.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/advanced_options.h"
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

// Generate the regular and coroutine versions of some methods by
// including table_cache_sync_and_async.h twice
// Macros in the header will expand differently based on whether
// WITH_COROUTINES or WITHOUT_COROUTINES is defined
// clang-format off
#define WITHOUT_COROUTINES
#include "db/table_cache_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "db/table_cache_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on

namespace ROCKSDB_NAMESPACE {

namespace {

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

}  // anonymous namespace

const int kLoadConcurency = 128;

TableCache::TableCache(const ImmutableOptions& ioptions,
                       const FileOptions* file_options, Cache* const cache,
                       BlockCacheTracer* const block_cache_tracer,
                       const std::shared_ptr<IOTracer>& io_tracer,
                       const std::string& db_session_id)
    : ioptions_(ioptions),
      file_options_(*file_options),
      cache_(cache),
      immortal_tables_(false),
      block_cache_tracer_(block_cache_tracer),
      loader_mutex_(kLoadConcurency, kGetSliceNPHash64UnseededFnPtr),
      io_tracer_(io_tracer),
      db_session_id_(db_session_id) {
  if (ioptions_.row_cache) {
    // If the same cache is shared by multiple instances, we need to
    // disambiguate its entries.
    PutVarint64(&row_cache_id_, ioptions_.row_cache->NewId());
  }
}

TableCache::~TableCache() {}

Status TableCache::GetTableReader(
    const ReadOptions& ro, const FileOptions& file_options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta, bool sequential_mode, bool record_read_stats,
    HistogramImpl* file_read_hist, std::unique_ptr<TableReader>* table_reader,
    const std::shared_ptr<const SliceTransform>& prefix_extractor,
    bool skip_filters, int level, bool prefetch_index_and_filter_in_cache,
    size_t max_file_size_for_l0_meta_pin, Temperature file_temperature) {
  std::string fname = TableFileName(
      ioptions_.cf_paths, file_meta.fd.GetNumber(), file_meta.fd.GetPathId());
  std::unique_ptr<FSRandomAccessFile> file;
  FileOptions fopts = file_options;
  fopts.temperature = file_temperature;
  Status s = PrepareIOFromReadOptions(ro, ioptions_.clock, fopts.io_options);
  TEST_SYNC_POINT_CALLBACK("TableCache::GetTableReader:BeforeOpenFile",
                           const_cast<Status*>(&s));
  if (s.ok()) {
    s = ioptions_.fs->NewRandomAccessFile(fname, fopts, &file, nullptr);
  }
  if (s.ok()) {
    RecordTick(ioptions_.stats, NO_FILE_OPENS);
  } else if (s.IsPathNotFound()) {
    fname = Rocks2LevelTableFileName(fname);
    s = PrepareIOFromReadOptions(ro, ioptions_.clock, fopts.io_options);
    if (s.ok()) {
      s = ioptions_.fs->NewRandomAccessFile(fname, file_options, &file,
                                            nullptr);
    }
    if (s.ok()) {
      RecordTick(ioptions_.stats, NO_FILE_OPENS);
    }
  }

  if (s.ok()) {
    if (!sequential_mode && ioptions_.advise_random_on_open) {
      file->Hint(FSRandomAccessFile::kRandom);
    }
    StopWatch sw(ioptions_.clock, ioptions_.stats, TABLE_OPEN_IO_MICROS);
    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(
            std::move(file), fname, ioptions_.clock, io_tracer_,
            record_read_stats ? ioptions_.stats : nullptr, SST_READ_MICROS,
            file_read_hist, ioptions_.rate_limiter.get(), ioptions_.listeners,
            file_temperature, level == ioptions_.num_levels - 1));
    UniqueId64x2 expected_unique_id;
    if (ioptions_.verify_sst_unique_id_in_manifest) {
      expected_unique_id = file_meta.unique_id;
    } else {
      expected_unique_id = kNullUniqueId64x2;  // null ID == no verification
    }
    s = ioptions_.table_factory->NewTableReader(
        ro,
        TableReaderOptions(ioptions_, prefix_extractor, file_options,
                           internal_comparator, skip_filters, immortal_tables_,
                           false /* force_direct_prefetch */, level,
                           block_cache_tracer_, max_file_size_for_l0_meta_pin,
                           db_session_id_, file_meta.fd.GetNumber(),
                           expected_unique_id, file_meta.fd.largest_seqno),
        std::move(file_reader), file_meta.fd.GetFileSize(), table_reader,
        prefetch_index_and_filter_in_cache);
    TEST_SYNC_POINT("TableCache::GetTableReader:0");
  }
  return s;
}

Status TableCache::FindTable(
    const ReadOptions& ro, const FileOptions& file_options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta, TypedHandle** handle,
    const std::shared_ptr<const SliceTransform>& prefix_extractor,
    const bool no_io, bool record_read_stats, HistogramImpl* file_read_hist,
    bool skip_filters, int level, bool prefetch_index_and_filter_in_cache,
    size_t max_file_size_for_l0_meta_pin, Temperature file_temperature) {
  PERF_TIMER_GUARD_WITH_CLOCK(find_table_nanos, ioptions_.clock);
  uint64_t number = file_meta.fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  *handle = cache_.Lookup(key);
  TEST_SYNC_POINT_CALLBACK("TableCache::FindTable:0",
                           const_cast<bool*>(&no_io));

  if (*handle == nullptr) {
    if (no_io) {
      return Status::Incomplete("Table not found in table_cache, no_io is set");
    }
    MutexLock load_lock(loader_mutex_.get(key));
    // We check the cache again under loading mutex
    *handle = cache_.Lookup(key);
    if (*handle != nullptr) {
      return Status::OK();
    }

    std::unique_ptr<TableReader> table_reader;
    Status s =
        GetTableReader(ro, file_options, internal_comparator, file_meta,
                       false /* sequential mode */, record_read_stats,
                       file_read_hist, &table_reader, prefix_extractor,
                       skip_filters, level, prefetch_index_and_filter_in_cache,
                       max_file_size_for_l0_meta_pin, file_temperature);
    if (!s.ok()) {
      assert(table_reader == nullptr);
      RecordTick(ioptions_.stats, NO_FILE_ERRORS);
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      s = cache_.Insert(key, table_reader.get(), 1, handle);
      if (s.ok()) {
        // Release ownership of table reader.
        table_reader.release();
      }
    }
    return s;
  }
  return Status::OK();
}

InternalIterator* TableCache::NewIterator(
    const ReadOptions& options, const FileOptions& file_options,
    const InternalKeyComparator& icomparator, const FileMetaData& file_meta,
    RangeDelAggregator* range_del_agg,
    const std::shared_ptr<const SliceTransform>& prefix_extractor,
    TableReader** table_reader_ptr, HistogramImpl* file_read_hist,
    TableReaderCaller caller, Arena* arena, bool skip_filters, int level,
    size_t max_file_size_for_l0_meta_pin,
    const InternalKey* smallest_compaction_key,
    const InternalKey* largest_compaction_key, bool allow_unprepared_value,
    TruncatedRangeDelIterator** range_del_iter) {
  PERF_TIMER_GUARD(new_table_iterator_nanos);

  Status s;
  TableReader* table_reader = nullptr;
  TypedHandle* handle = nullptr;
  if (table_reader_ptr != nullptr) {
    *table_reader_ptr = nullptr;
  }
  bool for_compaction = caller == TableReaderCaller::kCompaction;
  auto& fd = file_meta.fd;
  table_reader = fd.table_reader;
  if (table_reader == nullptr) {
    s = FindTable(
        options, file_options, icomparator, file_meta, &handle,
        prefix_extractor, options.read_tier == kBlockCacheTier /* no_io */,
        !for_compaction /* record_read_stats */, file_read_hist, skip_filters,
        level, true /* prefetch_index_and_filter_in_cache */,
        max_file_size_for_l0_meta_pin, file_meta.temperature);
    if (s.ok()) {
      table_reader = cache_.Value(handle);
    }
  }
  InternalIterator* result = nullptr;
  if (s.ok()) {
    if (options.table_filter &&
        !options.table_filter(*table_reader->GetTableProperties())) {
      result = NewEmptyInternalIterator<Slice>(arena);
    } else {
      result = table_reader->NewIterator(
          options, prefix_extractor.get(), arena, skip_filters, caller,
          file_options.compaction_readahead_size, allow_unprepared_value);
    }
    if (handle != nullptr) {
      cache_.RegisterReleaseAsCleanup(handle, *result);
      handle = nullptr;  // prevent from releasing below
    }

    if (for_compaction) {
      table_reader->SetupForCompaction();
    }
    if (table_reader_ptr != nullptr) {
      *table_reader_ptr = table_reader;
    }
  }
  if (s.ok() && !options.ignore_range_deletions) {
    if (range_del_iter != nullptr) {
      auto new_range_del_iter =
          table_reader->NewRangeTombstoneIterator(options);
      if (new_range_del_iter == nullptr || new_range_del_iter->empty()) {
        delete new_range_del_iter;
        *range_del_iter = nullptr;
      } else {
        *range_del_iter = new TruncatedRangeDelIterator(
            std::unique_ptr<FragmentedRangeTombstoneIterator>(
                new_range_del_iter),
            &icomparator, &file_meta.smallest, &file_meta.largest);
      }
    }
    if (range_del_agg != nullptr) {
      if (range_del_agg->AddFile(fd.GetNumber())) {
        std::unique_ptr<FragmentedRangeTombstoneIterator> new_range_del_iter(
            static_cast<FragmentedRangeTombstoneIterator*>(
                table_reader->NewRangeTombstoneIterator(options)));
        if (new_range_del_iter != nullptr) {
          s = new_range_del_iter->status();
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
          range_del_agg->AddTombstones(std::move(new_range_del_iter), smallest,
                                       largest);
        }
      }
    }
  }

  if (handle != nullptr) {
    cache_.Release(handle);
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
  assert(out_iter);
  const FileDescriptor& fd = file_meta.fd;
  Status s;
  TableReader* t = fd.table_reader;
  TypedHandle* handle = nullptr;
  if (t == nullptr) {
    s = FindTable(options, file_options_, internal_comparator, file_meta,
                  &handle);
    if (s.ok()) {
      t = cache_.Value(handle);
    }
  }
  if (s.ok()) {
    // Note: NewRangeTombstoneIterator could return nullptr
    out_iter->reset(t->NewRangeTombstoneIterator(options));
  }
  if (handle) {
    if (*out_iter) {
      cache_.RegisterReleaseAsCleanup(handle, **out_iter);
    } else {
      cache_.Release(handle);
    }
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
  RowCacheInterface row_cache{ioptions_.row_cache.get()};
  if (auto row_handle = row_cache.Lookup(row_cache_key.GetUserKey())) {
    // Cleanable routine to release the cache entry
    Cleanable value_pinner;
    // If it comes here value is located on the cache.
    // found_row_cache_entry points to the value on cache,
    // and value_pinner has cleanup procedure for the cached entry.
    // After replayGetContextLog() returns, get_context.pinnable_slice_
    // will point to cache entry buffer (or a copy based on that) and
    // cleanup routine under value_pinner will be delegated to
    // get_context.pinnable_slice_. Cache entry is released when
    // get_context.pinnable_slice_ is reset.
    row_cache.RegisterReleaseAsCleanup(row_handle, value_pinner);
    replayGetContextLog(*row_cache.Value(row_handle), user_key, get_context,
                        &value_pinner);
    RecordTick(ioptions_.stats, ROW_CACHE_HIT);
    found = true;
  } else {
    RecordTick(ioptions_.stats, ROW_CACHE_MISS);
  }
  return found;
}
#endif  // ROCKSDB_LITE

Status TableCache::Get(
    const ReadOptions& options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta, const Slice& k, GetContext* get_context,
    const std::shared_ptr<const SliceTransform>& prefix_extractor,
    HistogramImpl* file_read_hist, bool skip_filters, int level,
    size_t max_file_size_for_l0_meta_pin) {
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
  TypedHandle* handle = nullptr;
  if (!done) {
    assert(s.ok());
    if (t == nullptr) {
      s = FindTable(options, file_options_, internal_comparator, file_meta,
                    &handle, prefix_extractor,
                    options.read_tier == kBlockCacheTier /* no_io */,
                    true /* record_read_stats */, file_read_hist, skip_filters,
                    level, true /* prefetch_index_and_filter_in_cache */,
                    max_file_size_for_l0_meta_pin, file_meta.temperature);
      if (s.ok()) {
        t = cache_.Value(handle);
      }
    }
    SequenceNumber* max_covering_tombstone_seq =
        get_context->max_covering_tombstone_seq();
    if (s.ok() && max_covering_tombstone_seq != nullptr &&
        !options.ignore_range_deletions) {
      std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
          t->NewRangeTombstoneIterator(options));
      if (range_del_iter != nullptr) {
        SequenceNumber seq =
            range_del_iter->MaxCoveringTombstoneSeqnum(ExtractUserKey(k));
        if (seq > *max_covering_tombstone_seq) {
          *max_covering_tombstone_seq = seq;
          if (get_context->NeedTimestamp()) {
            get_context->SetTimestampFromRangeTombstone(
                range_del_iter->timestamp());
          }
        }
      }
    }
    if (s.ok()) {
      get_context->SetReplayLog(row_cache_entry);  // nullptr if no cache.
      s = t->Get(options, k, get_context, prefix_extractor.get(), skip_filters);
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
    RowCacheInterface row_cache{ioptions_.row_cache.get()};
    size_t charge = row_cache_entry->capacity() + sizeof(std::string);
    auto row_ptr = new std::string(std::move(*row_cache_entry));
    // If row cache is full, it's OK to continue.
    row_cache.Insert(row_cache_key.GetUserKey(), row_ptr, charge)
        .PermitUncheckedError();
  }
#endif  // ROCKSDB_LITE

  if (handle != nullptr) {
    cache_.Release(handle);
  }
  return s;
}

void TableCache::UpdateRangeTombstoneSeqnums(
    const ReadOptions& options, TableReader* t,
    MultiGetContext::Range& table_range) {
  std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
      t->NewRangeTombstoneIterator(options));
  if (range_del_iter != nullptr) {
    for (auto iter = table_range.begin(); iter != table_range.end(); ++iter) {
      SequenceNumber* max_covering_tombstone_seq =
          iter->get_context->max_covering_tombstone_seq();
      SequenceNumber seq =
          range_del_iter->MaxCoveringTombstoneSeqnum(iter->ukey_with_ts);
      if (seq > *max_covering_tombstone_seq) {
        *max_covering_tombstone_seq = seq;
        if (iter->get_context->NeedTimestamp()) {
          iter->get_context->SetTimestampFromRangeTombstone(
              range_del_iter->timestamp());
        }
      }
    }
  }
}

Status TableCache::MultiGetFilter(
    const ReadOptions& options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta,
    const std::shared_ptr<const SliceTransform>& prefix_extractor,
    HistogramImpl* file_read_hist, int level,
    MultiGetContext::Range* mget_range, TypedHandle** table_handle) {
  auto& fd = file_meta.fd;
#ifndef ROCKSDB_LITE
  IterKey row_cache_key;
  std::string row_cache_entry_buffer;

  // Check if we need to use the row cache. If yes, then we cannot do the
  // filtering here, since the filtering needs to happen after the row cache
  // lookup.
  KeyContext& first_key = *mget_range->begin();
  if (ioptions_.row_cache && !first_key.get_context->NeedToReadSequence()) {
    return Status::NotSupported();
  }
#endif  // ROCKSDB_LITE
  Status s;
  TableReader* t = fd.table_reader;
  TypedHandle* handle = nullptr;
  MultiGetContext::Range tombstone_range(*mget_range, mget_range->begin(),
                                         mget_range->end());
  if (t == nullptr) {
    s = FindTable(
        options, file_options_, internal_comparator, file_meta, &handle,
        prefix_extractor, options.read_tier == kBlockCacheTier /* no_io */,
        true /* record_read_stats */, file_read_hist, /*skip_filters=*/false,
        level, true /* prefetch_index_and_filter_in_cache */,
        /*max_file_size_for_l0_meta_pin=*/0, file_meta.temperature);
    if (s.ok()) {
      t = cache_.Value(handle);
    }
    *table_handle = handle;
  }
  if (s.ok()) {
    s = t->MultiGetFilter(options, prefix_extractor.get(), mget_range);
  }
  if (s.ok() && !options.ignore_range_deletions) {
    // Update the range tombstone sequence numbers for the keys here
    // as TableCache::MultiGet may or may not be called, and even if it
    // is, it may be called with fewer keys in the rangedue to filtering.
    UpdateRangeTombstoneSeqnums(options, t, tombstone_range);
  }
  if (mget_range->empty() && handle) {
    cache_.Release(handle);
    *table_handle = nullptr;
  }

  return s;
}

Status TableCache::GetTableProperties(
    const FileOptions& file_options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta,
    std::shared_ptr<const TableProperties>* properties,
    const std::shared_ptr<const SliceTransform>& prefix_extractor, bool no_io) {
  auto table_reader = file_meta.fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    *properties = table_reader->GetTableProperties();

    return Status::OK();
  }

  TypedHandle* table_handle = nullptr;
  Status s = FindTable(ReadOptions(), file_options, internal_comparator,
                       file_meta, &table_handle, prefix_extractor, no_io);
  if (!s.ok()) {
    return s;
  }
  assert(table_handle);
  auto table = cache_.Value(table_handle);
  *properties = table->GetTableProperties();
  cache_.Release(table_handle);
  return s;
}

Status TableCache::ApproximateKeyAnchors(
    const ReadOptions& ro, const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta, std::vector<TableReader::Anchor>& anchors) {
  Status s;
  TableReader* t = file_meta.fd.table_reader;
  TypedHandle* handle = nullptr;
  if (t == nullptr) {
    s = FindTable(ro, file_options_, internal_comparator, file_meta, &handle);
    if (s.ok()) {
      t = cache_.Value(handle);
    }
  }
  if (s.ok() && t != nullptr) {
    s = t->ApproximateKeyAnchors(ro, anchors);
  }
  if (handle != nullptr) {
    cache_.Release(handle);
  }
  return s;
}

size_t TableCache::GetMemoryUsageByTableReader(
    const FileOptions& file_options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta,
    const std::shared_ptr<const SliceTransform>& prefix_extractor) {
  auto table_reader = file_meta.fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    return table_reader->ApproximateMemoryUsage();
  }

  TypedHandle* table_handle = nullptr;
  Status s = FindTable(ReadOptions(), file_options, internal_comparator,
                       file_meta, &table_handle, prefix_extractor, true);
  if (!s.ok()) {
    return 0;
  }
  assert(table_handle);
  auto table = cache_.Value(table_handle);
  auto ret = table->ApproximateMemoryUsage();
  cache_.Release(table_handle);
  return ret;
}

void TableCache::Evict(Cache* cache, uint64_t file_number) {
  cache->Erase(GetSliceForFileNumber(&file_number));
}

uint64_t TableCache::ApproximateOffsetOf(
    const Slice& key, const FileMetaData& file_meta, TableReaderCaller caller,
    const InternalKeyComparator& internal_comparator,
    const std::shared_ptr<const SliceTransform>& prefix_extractor) {
  uint64_t result = 0;
  TableReader* table_reader = file_meta.fd.table_reader;
  TypedHandle* table_handle = nullptr;
  if (table_reader == nullptr) {
    const bool for_compaction = (caller == TableReaderCaller::kCompaction);
    Status s =
        FindTable(ReadOptions(), file_options_, internal_comparator, file_meta,
                  &table_handle, prefix_extractor, false /* no_io */,
                  !for_compaction /* record_read_stats */);
    if (s.ok()) {
      table_reader = cache_.Value(table_handle);
    }
  }

  if (table_reader != nullptr) {
    result = table_reader->ApproximateOffsetOf(key, caller);
  }
  if (table_handle != nullptr) {
    cache_.Release(table_handle);
  }

  return result;
}

uint64_t TableCache::ApproximateSize(
    const Slice& start, const Slice& end, const FileMetaData& file_meta,
    TableReaderCaller caller, const InternalKeyComparator& internal_comparator,
    const std::shared_ptr<const SliceTransform>& prefix_extractor) {
  uint64_t result = 0;
  TableReader* table_reader = file_meta.fd.table_reader;
  TypedHandle* table_handle = nullptr;
  if (table_reader == nullptr) {
    const bool for_compaction = (caller == TableReaderCaller::kCompaction);
    Status s =
        FindTable(ReadOptions(), file_options_, internal_comparator, file_meta,
                  &table_handle, prefix_extractor, false /* no_io */,
                  !for_compaction /* record_read_stats */);
    if (s.ok()) {
      table_reader = cache_.Value(table_handle);
    }
  }

  if (table_reader != nullptr) {
    result = table_reader->ApproximateSize(start, end, caller);
  }
  if (table_handle != nullptr) {
    cache_.Release(table_handle);
  }

  return result;
}
}  // namespace ROCKSDB_NAMESPACE
