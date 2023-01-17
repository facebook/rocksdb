//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/coro_utils.h"

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))
namespace ROCKSDB_NAMESPACE {

#if defined(WITHOUT_COROUTINES)
#endif

// Batched version of TableCache::MultiGet.
DEFINE_SYNC_AND_ASYNC(Status, TableCache::MultiGet)
(const ReadOptions& options, const InternalKeyComparator& internal_comparator,
 const FileMetaData& file_meta, const MultiGetContext::Range* mget_range,
 const std::shared_ptr<const SliceTransform>& prefix_extractor,
 HistogramImpl* file_read_hist, bool skip_filters, bool skip_range_deletions,
 int level, TypedHandle* handle) {
  auto& fd = file_meta.fd;
  Status s;
  TableReader* t = fd.table_reader;
  MultiGetRange table_range(*mget_range, mget_range->begin(),
                            mget_range->end());
  if (handle != nullptr && t == nullptr) {
    t = cache_.Value(handle);
  }
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
      const Slice& user_key = miter->ukey_with_ts;

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
      assert(handle == nullptr);
      s = FindTable(options, file_options_, internal_comparator, file_meta,
                    &handle, prefix_extractor,
                    options.read_tier == kBlockCacheTier /* no_io */,
                    true /* record_read_stats */, file_read_hist, skip_filters,
                    level, true /* prefetch_index_and_filter_in_cache */,
                    0 /*max_file_size_for_l0_meta_pin*/, file_meta.temperature);
      TEST_SYNC_POINT_CALLBACK("TableCache::MultiGet:FindTable", &s);
      if (s.ok()) {
        t = cache_.Value(handle);
        assert(t);
      }
    }
    if (s.ok() && !options.ignore_range_deletions && !skip_range_deletions) {
      UpdateRangeTombstoneSeqnums(options, t, table_range);
    }
    if (s.ok()) {
      CO_AWAIT(t->MultiGet)
      (options, &table_range, prefix_extractor.get(), skip_filters);
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
    RowCacheInterface row_cache{ioptions_.row_cache.get()};

    for (auto miter = table_range.begin(); miter != table_range.end();
         ++miter) {
      std::string& row_cache_entry = row_cache_entries[row_idx++];
      const Slice& user_key = miter->ukey_with_ts;
      ;
      GetContext* get_context = miter->get_context;

      get_context->SetReplayLog(nullptr);
      // Compute row cache key.
      row_cache_key.TrimAppend(row_cache_key_prefix_size, user_key.data(),
                               user_key.size());
      // Put the replay log in row cache only if something was found.
      if (s.ok() && !row_cache_entry.empty()) {
        size_t charge = row_cache_entry.capacity() + sizeof(std::string);
        auto row_ptr = new std::string(std::move(row_cache_entry));
        // If row cache is full, it's OK.
        row_cache.Insert(row_cache_key.GetUserKey(), row_ptr, charge)
            .PermitUncheckedError();
      }
    }
  }
#endif  // ROCKSDB_LITE

  if (handle != nullptr) {
    cache_.Release(handle);
  }
  CO_RETURN s;
}
}  // namespace ROCKSDB_NAMESPACE
#endif
