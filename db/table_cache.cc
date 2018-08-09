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
#include "db/version_edit.h"
#include "util/filename.h"

#include "monitoring/perf_context_imp.h"
#include "rocksdb/statistics.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

namespace rocksdb {

namespace {

template<class Lambda, class R, class... Args>
R c_style_callback_impl(void* vlamb, Args... args) {
  return (*(Lambda*)vlamb)(std::forward<Args>(args)...);
}

template<class Lambda>
struct c_style_callback_fetcher {
  template<class R, class ...Args>
  using target_callback = R(*)(void*, Args...);

  template<class R, class ...Args>
  operator target_callback<R, Args...>() {
    return &c_style_callback_impl<Lambda, R, Args...>;
  }
};

template<class Lambda>
c_style_callback_fetcher<Lambda> gen_c_style_callback(Lambda&) {
  return c_style_callback_fetcher<Lambda>();
}

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

static void DeleteTableReader(void* arg1, void* /*arg2*/) {
  TableReader* table_reader = reinterpret_cast<TableReader*>(arg1);
  delete table_reader;
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

InternalIterator* TranslateVarietySstIterator(
    const FileMetaData& file_meta, InternalIterator* variety_sst_iter,
    const InternalKeyComparator& icomp,
    const std::function<InternalIterator*(uint64_t, Arena*)>& create_iter,
    Arena* arena) {
  InternalIterator* result;
  switch (file_meta.sst_variety) {
  case kLinkSst:
    result = NewLinkSstIterator(variety_sst_iter, icomp, create_iter, arena);
    break;
  case kMapSst:
    result = NewMapSstIterator(variety_sst_iter, icomp, create_iter, arena);
    break;
  default:
    assert(false);
    return variety_sst_iter;
  }
  result->RegisterCleanup([](void* arg1, void* arg2) {
    auto iter = (InternalIterator*)arg1;
    auto arena = (Arena*)arg2;
    if (arena == nullptr) {
      delete iter;
    } else {
      iter->~InternalIterator();
    }
  }, variety_sst_iter, arena);
  return result;
}

Status GetFromVarietySst(
    const FileMetaData& file_meta, TableReader* table_reader,
    const InternalKeyComparator& icomp, const Slice& k,
    GetContext* get_context, void* arg,
    bool(*get_from_sst)(void* arg, const Slice& find_k, uint64_t sst_id,
                        Status& status)) {
  Status s;
  switch (file_meta.sst_variety) {
  case kLinkSst: {
    InternalKey smallest_key;
    auto get_from_link = [&](const Slice& largest_key,
                             const Slice& link_value) {
      // Manual inline LinkSstElement::Decode
      Slice link_input = link_value;
      uint64_t sst_id;
      Slice find_k = k;

      if (!GetFixed64(&link_input, &sst_id)) {
        s = Status::Corruption("Link sst invalid link_value");
        return false;
      }
      if (smallest_key.size() != 0) {
        assert(icomp.user_comparator()->Compare(
            ExtractUserKey(k), ExtractUserKey(smallest_key.Encode())) == 0);
        // shrink to smallest_key
        find_k = smallest_key.Encode();
      }
      if (!get_from_sst(arg, find_k, sst_id, s)) {
        // error or found
        return false;
      }
      if (icomp.user_comparator()->Compare(ExtractUserKey(largest_key),
                                           ExtractUserKey(k)) != 0) {
        // next smallest_key is current largest_key
        // k less than next link smallest_key
        return false;
      }
      // store largest_key
      smallest_key.DecodeFrom(largest_key);
      return true;
    };
    table_reader->RangeScan(&k, &get_from_link,
                            gen_c_style_callback(get_from_link));
    return s;
  }
  case kMapSst: {  
    auto get_from_map = [&](const Slice& largest_key,
                            const Slice& map_value) {
      // Manual inline MapSstElement::Decode
      const char* error_msg = "Map sst invalid link_value";
      Slice map_input = map_value;
      Slice smallest_key;
      uint64_t link_count;
      uint64_t sst_id;
      Slice find_k = k;
      
      if (!GetLengthPrefixedSlice(&map_input, &smallest_key)) {
        s = Status::Corruption(error_msg);
        return false;
      }
      if (icomp.Compare(k, smallest_key) < 0) {
        if (icomp.user_comparator()->Compare(ExtractUserKey(smallest_key),
                                             ExtractUserKey(k)) != 0) {
          // less than smallest_key
          return false;
        }
        assert(ExtractSequence(k) > ExtractSequence(smallest_key));
        // same user_key, shrink to smallest_key
        find_k = smallest_key;
      }
      if (!GetVarint64(&map_input, &link_count) ||
          map_input.size() < link_count * sizeof(uint64_t)) {
        s = Status::Corruption(error_msg);
        return false;
      }

      bool is_bound_key =
          icomp.user_comparator()->Compare(ExtractUserKey(largest_key),
                                           ExtractUserKey(k)) == 0;
      SequenceNumber min_seq_backup = get_context->GetMinSequenceNumber();
      if (is_bound_key) {
        // shrink seqno to largest_key, make sure can't read greater keys
        get_context->SetMinSequenceNumber(
            std::max(min_seq_backup, ExtractSequence(largest_key)));
      }

      for (uint64_t i = 0; i < link_count; ++i) {
        GetFixed64(&map_input, &sst_id);
        if (!get_from_sst(arg, find_k, sst_id, s)) {
          // error or found, recovery min_seq_backup is unnecessary
          return false;
        }
      }
      // recovery min_seq_backup
      get_context->SetMinSequenceNumber(min_seq_backup);
      return is_bound_key;
    };
    table_reader->RangeScan(&k, &get_from_map,
                            gen_c_style_callback(get_from_map));
    return s;
  }
  default:
    assert(false);
    return s;
  }
}

}  // namespace

TableCache::TableCache(const ImmutableCFOptions& ioptions,
                       const EnvOptions& env_options, Cache* const cache)
    : ioptions_(ioptions),
      env_options_(env_options),
      cache_(cache),
      immortal_tables_(false) {
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
    HistogramImpl* file_read_hist, unique_ptr<TableReader>* table_reader,
    const SliceTransform* prefix_extractor, bool skip_filters, int level,
    bool prefetch_index_and_filter_in_cache, bool for_compaction) {
  std::string fname =
      TableFileName(ioptions_.cf_paths, fd.GetNumber(), fd.GetPathId());
  unique_ptr<RandomAccessFile> file;
  Status s = ioptions_.env->NewRandomAccessFile(fname, &file, env_options);

  RecordTick(ioptions_.statistics, NO_FILE_OPENS);
  if (s.ok()) {
    if (readahead > 0 && !env_options.use_mmap_reads) {
      // Not compatible with mmap files since ReadaheadRandomAccessFile requires
      // its wrapped file's Read() to copy data into the provided scratch
      // buffer, which mmap files don't use.
      // TODO(ajkr): try madvise for mmap files in place of buffered readahead.
      file = NewReadaheadRandomAccessFile(std::move(file), readahead);
    }
    if (!sequential_mode && ioptions_.advise_random_on_open) {
      file->Hint(RandomAccessFile::RANDOM);
    }
    StopWatch sw(ioptions_.env, ioptions_.statistics, TABLE_OPEN_IO_MICROS);
    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(
            std::move(file), fname, ioptions_.env,
            record_read_stats ? ioptions_.statistics : nullptr, SST_READ_MICROS,
            file_read_hist, ioptions_.rate_limiter, for_compaction));
    s = ioptions_.table_factory->NewTableReader(
        TableReaderOptions(ioptions_, prefix_extractor, env_options,
                           internal_comparator, skip_filters, immortal_tables_,
                           level, fd.largest_seqno),
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

Status TableCache::FindTable(const EnvOptions& env_options,
                             const InternalKeyComparator& internal_comparator,
                             const FileDescriptor& fd, Cache::Handle** handle,
                             const SliceTransform* prefix_extractor,
                             const bool no_io, bool record_read_stats,
                             HistogramImpl* file_read_hist, bool skip_filters,
                             int level,
                             bool prefetch_index_and_filter_in_cache) {
  PERF_TIMER_GUARD(find_table_nanos);
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
    unique_ptr<TableReader> table_reader;
    s = GetTableReader(env_options, internal_comparator, fd,
                       false /* sequential mode */, 0 /* readahead */,
                       record_read_stats, file_read_hist, &table_reader,
                       prefix_extractor, skip_filters, level,
                       prefetch_index_and_filter_in_cache);
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
    const ReadOptions& options, const EnvOptions& env_options,
    const InternalKeyComparator& icomparator, const FileMetaData& file_meta,
    const std::unordered_map<uint64_t, FileMetaData*>& depend_files,
    RangeDelAggregator* range_del_agg, const SliceTransform* prefix_extractor,
    TableReader** table_reader_ptr, HistogramImpl* file_read_hist,
    bool for_compaction, Arena* arena, bool skip_filters, int level) {
  PERF_TIMER_GUARD(new_table_iterator_nanos);

  Status s;
  bool create_new_table_reader = false;
  TableReader* table_reader = nullptr;
  Cache::Handle* handle = nullptr;
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
      // get compaction_readahead_size from env_options allows us to set the
      // value dynamically
      readahead = env_options.compaction_readahead_size;
      create_new_table_reader = true;
    }
  } else {
    readahead = options.readahead_size;
    create_new_table_reader = readahead > 0;
  }

  auto& fd = file_meta.fd;
  if (create_new_table_reader) {
    unique_ptr<TableReader> table_reader_unique_ptr;
    s = GetTableReader(
        env_options, icomparator, fd, true /* sequential_mode */, readahead,
        !for_compaction /* record stats */, nullptr, &table_reader_unique_ptr,
        prefix_extractor, false /* skip_filters */, level,
        true /* prefetch_index_and_filter_in_cache */, for_compaction);
    if (s.ok()) {
      table_reader = table_reader_unique_ptr.release();
    }
  } else {
    table_reader = fd.table_reader;
    if (table_reader == nullptr) {
      s = FindTable(env_options, icomparator, fd, &handle, prefix_extractor,
                    options.read_tier == kBlockCacheTier /* no_io */,
                    !for_compaction /* record read_stats */, file_read_hist,
                    skip_filters, level);
      if (s.ok()) {
        table_reader = GetTableReaderFromHandle(handle);
      }
    }
  }
  InternalIterator* result = nullptr;
  if (s.ok()) {
    if (options.table_filter &&
        !options.table_filter(*table_reader->GetTableProperties())) {
      result = NewEmptyInternalIterator(arena);
    } else {
      SourceInternalIterator* source_result;
      source_result = table_reader->NewIterator(options, prefix_extractor,
                                                arena, skip_filters,
                                                for_compaction);
      source_result->SetSource(
          IteratorSource(IteratorSource::kSST, (uintptr_t)&file_meta));
      result = source_result;
      if (file_meta.sst_variety != 0 && !depend_files.empty()) {
        // Store params for create depend table iterator in future
        // DON'T REF THIS OBJECT, DEEP COPY IT !
        struct {
          TableCache* table_cache;
          ReadOptions options;  // deep copy
          const EnvOptions& env_options;
          const InternalKeyComparator& icomparator;
          const std::unordered_map<uint64_t, FileMetaData*>& depend_files;
          const SliceTransform* prefix_extractor;
          bool for_compaction;
          bool skip_filters;
          int level;

          InternalIterator* operator()(uint64_t sst_id, Arena* iter_arena) {
            auto find = depend_files.find(sst_id);
            if (find == depend_files.end()) {
              return nullptr;
            }
            return table_cache->NewIterator(
                       options, env_options, icomparator, *find->second,
                       depend_files, nullptr, prefix_extractor, nullptr,
                       nullptr, for_compaction, iter_arena, skip_filters,
                       level);
          }
        } create_iterator {
          this, options, env_options, icomparator, depend_files,
          prefix_extractor, for_compaction, skip_filters, level,
        };
        result = TranslateVarietySstIterator(file_meta, result, icomparator,
                                             create_iterator, arena);
      }
    }
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
    if (range_del_agg->AddFile(fd.GetNumber())) {
      std::unique_ptr<InternalIterator> range_del_iter(
          table_reader->NewRangeTombstoneIterator(options));
      if (range_del_iter != nullptr) {
        s = range_del_iter->status();
      }
      if (s.ok()) {
        s = range_del_agg->AddTombstones(
            std::move(range_del_iter),
            &file_meta.smallest,
            &file_meta.largest);
      }
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

Status TableCache::Get(
    const ReadOptions& options,
    const InternalKeyComparator& internal_comparator,
    const FileMetaData& file_meta,
    const std::unordered_map<uint64_t, FileMetaData*>& depend_files,
    const Slice& k, GetContext* get_context,
    const SliceTransform* prefix_extractor,
    HistogramImpl* file_read_hist, bool skip_filters, int level) {
  auto& fd = file_meta.fd;
  std::string* row_cache_entry = nullptr;
  bool done = false;
#ifndef ROCKSDB_LITE
  IterKey row_cache_key;
  std::string row_cache_entry_buffer;

  // Check row cache if enabled. Since row cache does not currently store
  // sequence numbers, we cannot use it if we need to fetch the sequence.
  if (ioptions_.row_cache && !get_context->NeedToReadSequence() &&
      file_meta.sst_variety == 0) {
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
      // Cleanable routine to release the cache entry
      Cleanable value_pinner;
      auto release_cache_entry_func = [](void* cache_to_clean,
                                         void* cache_handle) {
        ((Cache*)cache_to_clean)->Release((Cache::Handle*)cache_handle);
      };
      auto found_row_cache_entry = static_cast<const std::string*>(
          ioptions_.row_cache->Value(row_handle));
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
      s = FindTable(
          env_options_, internal_comparator, fd, &handle, prefix_extractor,
          options.read_tier == kBlockCacheTier /* no_io */,
          true /* record_read_stats */, file_read_hist, skip_filters, level);
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
            std::move(range_del_iter),
            &file_meta.smallest,
            &file_meta.largest);
      }
    }
    if (s.ok()) {
      get_context->SetReplayLog(row_cache_entry);  // nullptr if no cache.
      if (file_meta.sst_variety == 0 || depend_files.empty()) {
        s = t->Get(options, k, get_context, prefix_extractor, skip_filters);
      } else {
        ReadOptions options_copy = options;
        options_copy.ignore_range_deletions = true;
        // Forward query to target sst
        auto get_from_sst = [&](const Slice& find_k, uint64_t sst_id,
                                Status& status) {
          auto find = depend_files.find(sst_id);
          if (find == depend_files.end()) {
            status = Status::Corruption("Variety sst depend files missing");
            return false;
          }
          status = Get(options_copy, internal_comparator, *find->second,
                       depend_files, find_k, get_context, prefix_extractor,
                       file_read_hist, skip_filters, level);

          return status.ok() && !get_context->is_finished();
        };
        s = GetFromVarietySst(file_meta, t, internal_comparator, k,
                              get_context, &get_from_sst,
                              gen_c_style_callback(get_from_sst));
      }
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
  s = FindTable(env_options, internal_comparator, fd, &table_handle,
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
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    const SliceTransform* prefix_extractor) {
  Status s;
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    return table_reader->ApproximateMemoryUsage();
  }

  Cache::Handle* table_handle = nullptr;
  s = FindTable(env_options, internal_comparator, fd, &table_handle,
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

}  // namespace rocksdb
