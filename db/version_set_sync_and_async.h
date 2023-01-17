//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/coro_utils.h"

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

// Lookup a batch of keys in a single SST file
DEFINE_SYNC_AND_ASYNC(Status, Version::MultiGetFromSST)
(const ReadOptions& read_options, MultiGetRange file_range, int hit_file_level,
 bool skip_filters, bool skip_range_deletions, FdWithKeyRange* f,
 std::unordered_map<uint64_t, BlobReadContexts>& blob_ctxs,
 TableCache::TypedHandle* table_handle, uint64_t& num_filter_read,
 uint64_t& num_index_read, uint64_t& num_sst_read) {
  bool timer_enabled = GetPerfLevel() >= PerfLevel::kEnableTimeExceptForMutex &&
                       get_perf_context()->per_level_perf_context_enabled;

  Status s;
  StopWatchNano timer(clock_, timer_enabled /* auto_start */);
  s = CO_AWAIT(table_cache_->MultiGet)(
      read_options, *internal_comparator(), *f->file_metadata, &file_range,
      mutable_cf_options_.prefix_extractor,
      cfd_->internal_stats()->GetFileReadHist(hit_file_level), skip_filters,
      skip_range_deletions, hit_file_level, table_handle);
  // TODO: examine the behavior for corrupted key
  if (timer_enabled) {
    PERF_COUNTER_BY_LEVEL_ADD(get_from_table_nanos, timer.ElapsedNanos(),
                              hit_file_level);
  }
  if (!s.ok()) {
    // TODO: Set status for individual keys appropriately
    for (auto iter = file_range.begin(); iter != file_range.end(); ++iter) {
      *iter->s = s;
      file_range.MarkKeyDone(iter);
    }
    CO_RETURN s;
  }
  uint64_t batch_size = 0;
  for (auto iter = file_range.begin(); s.ok() && iter != file_range.end();
       ++iter) {
    GetContext& get_context = *iter->get_context;
    Status* status = iter->s;
    // The Status in the KeyContext takes precedence over GetContext state
    // Status may be an error if there were any IO errors in the table
    // reader. We never expect Status to be NotFound(), as that is
    // determined by get_context
    assert(!status->IsNotFound());
    if (!status->ok()) {
      file_range.MarkKeyDone(iter);
      continue;
    }

    if (get_context.sample()) {
      sample_file_read_inc(f->file_metadata);
    }
    batch_size++;
    num_index_read += get_context.get_context_stats_.num_index_read;
    num_filter_read += get_context.get_context_stats_.num_filter_read;
    num_sst_read += get_context.get_context_stats_.num_sst_read;
    // Reset these stats since they're specific to a level
    get_context.get_context_stats_.num_index_read = 0;
    get_context.get_context_stats_.num_filter_read = 0;
    get_context.get_context_stats_.num_sst_read = 0;

    // report the counters before returning
    if (get_context.State() != GetContext::kNotFound &&
        get_context.State() != GetContext::kMerge &&
        db_statistics_ != nullptr) {
      get_context.ReportCounters();
    } else {
      if (iter->max_covering_tombstone_seq > 0) {
        // The remaining files we look at will only contain covered keys, so
        // we stop here for this key
        file_range.SkipKey(iter);
      }
    }
    switch (get_context.State()) {
      case GetContext::kNotFound:
        // Keep searching in other files
        break;
      case GetContext::kMerge:
        // TODO: update per-level perfcontext user_key_return_count for kMerge
        break;
      case GetContext::kFound:
        if (hit_file_level == 0) {
          RecordTick(db_statistics_, GET_HIT_L0);
        } else if (hit_file_level == 1) {
          RecordTick(db_statistics_, GET_HIT_L1);
        } else if (hit_file_level >= 2) {
          RecordTick(db_statistics_, GET_HIT_L2_AND_UP);
        }

        PERF_COUNTER_BY_LEVEL_ADD(user_key_return_count, 1, hit_file_level);

        file_range.MarkKeyDone(iter);

        if (iter->is_blob_index) {
          if (iter->value) {
            TEST_SYNC_POINT_CALLBACK("Version::MultiGet::TamperWithBlobIndex",
                                     &(*iter));

            const Slice& blob_index_slice = *(iter->value);
            BlobIndex blob_index;
            Status tmp_s = blob_index.DecodeFrom(blob_index_slice);
            if (tmp_s.ok()) {
              const uint64_t blob_file_num = blob_index.file_number();
              blob_ctxs[blob_file_num].emplace_back(
                  std::make_pair(blob_index, std::cref(*iter)));
            } else {
              *(iter->s) = tmp_s;
            }
          }
        } else {
          file_range.AddValueSize(iter->value->size());
          if (file_range.GetValueSize() > read_options.value_size_soft_limit) {
            s = Status::Aborted();
            break;
          }
        }
        continue;
      case GetContext::kDeleted:
        // Use empty error message for speed
        *status = Status::NotFound();
        file_range.MarkKeyDone(iter);
        continue;
      case GetContext::kCorrupt:
        *status =
            Status::Corruption("corrupted key for ", iter->lkey->user_key());
        file_range.MarkKeyDone(iter);
        continue;
      case GetContext::kUnexpectedBlobIndex:
        ROCKS_LOG_ERROR(info_log_, "Encounter unexpected blob index.");
        *status = Status::NotSupported(
            "Encounter unexpected blob index. Please open DB with "
            "ROCKSDB_NAMESPACE::blob_db::BlobDB instead.");
        file_range.MarkKeyDone(iter);
        continue;
    }
  }

  RecordInHistogram(db_statistics_, SST_BATCH_SIZE, batch_size);
  CO_RETURN s;
}
}  // namespace ROCKSDB_NAMESPACE
#endif
