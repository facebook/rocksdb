//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/coro_utils.h"

#if defined(USE_COROUTINES) && defined(WITH_COROUTINES)
#include "util/coro_stats_util.h"
#endif  // USE_COROUTINES && WITH_COROUTINES

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

DEFINE_SYNC_AND_ASYNC(Status, CompactedDBImpl::Get)
(const ReadOptions& _read_options, ColumnFamilyHandle*, const Slice& key,
 PinnableSlice* value, std::string* timestamp) {
#ifdef WITH_COROUTINES
  INSTALL_COROUTINE_STATS_CONTEXT_SCOPE(
      immutable_db_options_.fs->GetReadExecutor(), immutable_db_options_.env);
#endif
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kGet) {
    CO_RETURN Status::InvalidArgument(
        "Can only call Get with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kGet`");
  }
  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGet;
  }

  assert(user_comparator_);
  if (read_options.timestamp) {
    Status s =
        FailIfTsMismatchCf(DefaultColumnFamily(), *(read_options.timestamp));
    if (!s.ok()) {
      CO_RETURN s;
    }
    if (read_options.timestamp->size() > 0) {
      s = FailIfReadCollapsedHistory(cfd_, cfd_->GetSuperVersion(),
                                     *(read_options.timestamp));
      if (!s.ok()) {
        CO_RETURN s;
      }
    }
  } else {
    const Status s = FailIfCfHasTs(DefaultColumnFamily());
    if (!s.ok()) {
      CO_RETURN s;
    }
  }

  // Clear the timestamps for returning results so that we can distinguish
  // between tombstone or key that has never been written
  if (timestamp) {
    timestamp->clear();
  }

  GetWithTimestampReadCallback read_cb(kMaxSequenceNumber);
  VersionBlobFetcher blob_fetcher(version_, read_options);
  std::string* ts =
      user_comparator_->timestamp_size() > 0 ? timestamp : nullptr;
  LookupKey lkey(key, kMaxSequenceNumber, read_options.timestamp);
  bool is_blob_index = false;
  GetContext get_context(user_comparator_, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, lkey.user_key(), value,
                         /*columns=*/nullptr, ts, nullptr, nullptr, true,
                         nullptr, nullptr, nullptr, nullptr, &read_cb,
                         &is_blob_index, 0 /* tracing_get_id */, &blob_fetcher);

  const FdWithKeyRange& f = files_.files[FindFile(lkey.user_key())];
  if (user_comparator_->CompareWithoutTimestamp(
          key, /*a_has_ts=*/false,
          ExtractUserKeyAndStripTimestamp(f.smallest_key,
                                          user_comparator_->timestamp_size()),
          /*b_has_ts=*/false) < 0) {
    CO_RETURN Status::NotFound();
  }
  TableReader* t = nullptr;
  TableCache::TypedHandle* handle = nullptr;
  Status s = cfd_->table_cache()->FindTable(
      read_options, cfd_->table_cache()->file_options(),
      cfd_->internal_comparator(), *f.file_metadata, &handle,
      version_->GetMutableCFOptions(), &t, false /* no_io */,
      nullptr /* file_read_hist */, false /* skip_filters */, files_level_,
      true /* prefetch_index_and_filter_in_cache */,
      0 /* max_file_size_for_l0_meta_pin */, f.file_metadata->temperature,
      true /* pin_table_handle */);
  if (s.ok()) {
    assert(handle == nullptr);
    // Use TableReader directly to avoid extra table_cache->Get() overheads
    s = CO_AWAIT(t->Get, read_options, lkey.internal_key(), &get_context,
                 nullptr);
  }
  if (!s.ok() && !s.IsNotFound()) {
    CO_RETURN s;
  }
  if (get_context.State() == GetContext::kFound) {
    if (is_blob_index) {
      // The TableReader above only decoded the blob reference into *value;
      // resolve it to the actual blob content, mirroring Version::Get.
      constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
      constexpr uint64_t* bytes_read = nullptr;
      PinnableSlice blob_value;
      s = blob_fetcher.FetchBlob(get_context.ukey_to_get_blob_value(), *value,
                                 prefetch_buffer, &blob_value, bytes_read);
      if (!s.ok()) {
        CO_RETURN s;
      }
      *value = std::move(blob_value);
    }
    CO_RETURN Status::OK();
  }
  CO_RETURN Status::NotFound();
}

DEFINE_SYNC_AND_ASYNC(void, CompactedDBImpl::MultiGet)
(const ReadOptions& _read_options, size_t num_keys,
 ColumnFamilyHandle** /*column_families*/, const Slice* keys,
 PinnableSlice* values, std::string* timestamps, Status* statuses,
 const bool /*sorted_input*/) {
#ifdef WITH_COROUTINES
  INSTALL_COROUTINE_STATS_CONTEXT_SCOPE(
      immutable_db_options_.fs->GetReadExecutor(), immutable_db_options_.env);
#endif
  assert(user_comparator_);
  Status s;
  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kMultiGet) {
    s = Status::InvalidArgument(
        "Can only call MultiGet with `ReadOptions::io_activity` is "
        "`Env::IOActivity::kUnknown` or `Env::IOActivity::kMultiGet`");
  }

  ReadOptions read_options(_read_options);
  if (s.ok()) {
    if (read_options.io_activity == Env::IOActivity::kUnknown) {
      read_options.io_activity = Env::IOActivity::kMultiGet;
    }

    if (read_options.timestamp) {
      s = FailIfTsMismatchCf(DefaultColumnFamily(), *(read_options.timestamp));
      if (s.ok()) {
        if (read_options.timestamp->size() > 0) {
          s = FailIfReadCollapsedHistory(cfd_, cfd_->GetSuperVersion(),
                                         *(read_options.timestamp));
        }
      }
    } else {
      s = FailIfCfHasTs(DefaultColumnFamily());
    }
  }

  if (!s.ok()) {
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }
    CO_RETURN;
  }

  // Clear the timestamps for returning results so that we can distinguish
  // between tombstone or key that has never been written
  if (timestamps) {
    for (size_t i = 0; i < num_keys; ++i) {
      timestamps[i].clear();
    }
  }

  GetWithTimestampReadCallback read_cb(kMaxSequenceNumber);
  VersionBlobFetcher blob_fetcher(version_, read_options);
  for (size_t i = 0; i < num_keys; ++i) {
    const Slice& key = keys[i];
    LookupKey lkey(key, kMaxSequenceNumber, read_options.timestamp);
    const FdWithKeyRange& f = files_.files[FindFile(lkey.user_key())];
    if (user_comparator_->CompareWithoutTimestamp(
            key, /*a_has_ts=*/false,
            ExtractUserKeyAndStripTimestamp(f.smallest_key,
                                            user_comparator_->timestamp_size()),
            /*b_has_ts=*/false) < 0) {
      statuses[i] = Status::NotFound();
      continue;
    }

    PinnableSlice& pinnable_val = values[i];
    std::string* timestamp = timestamps ? &timestamps[i] : nullptr;
    bool is_blob_index = false;
    GetContext get_context(
        user_comparator_, nullptr, nullptr, nullptr, GetContext::kNotFound,
        lkey.user_key(), &pinnable_val, /*columns=*/nullptr,
        user_comparator_->timestamp_size() > 0 ? timestamp : nullptr, nullptr,
        nullptr, true, nullptr, nullptr, nullptr, nullptr, &read_cb,
        &is_blob_index, 0 /* tracing_get_id */, &blob_fetcher);
    TableReader* t = nullptr;
    TableCache::TypedHandle* handle = nullptr;
    Status status = cfd_->table_cache()->FindTable(
        read_options, cfd_->table_cache()->file_options(),
        cfd_->internal_comparator(), *f.file_metadata, &handle,
        version_->GetMutableCFOptions(), &t, false /* no_io */,
        nullptr /* file_read_hist */, false /* skip_filters */, files_level_,
        true /* prefetch_index_and_filter_in_cache */,
        0 /* max_file_size_for_l0_meta_pin */, f.file_metadata->temperature,
        true /* pin_table_handle */);
    if (status.ok()) {
      assert(handle == nullptr);
      // Use TableReader directly to avoid extra table_cache->Get() overheads
      status = CO_AWAIT(t->Get, read_options, lkey.internal_key(), &get_context,
                        nullptr);
    }
    if (!status.ok() && !status.IsNotFound()) {
      statuses[i] = status;
    } else if (get_context.State() == GetContext::kFound) {
      if (is_blob_index) {
        // The TableReader above only decoded the blob reference into
        // pinnable_val; resolve it to the actual blob content, mirroring
        // Version::Get.
        constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
        constexpr uint64_t* bytes_read = nullptr;
        PinnableSlice blob_value;
        statuses[i] =
            blob_fetcher.FetchBlob(lkey.user_key(), pinnable_val,
                                   prefetch_buffer, &blob_value, bytes_read);
        if (statuses[i].ok()) {
          pinnable_val = std::move(blob_value);
        }
      } else {
        statuses[i] = Status::OK();
      }
    } else {
      statuses[i] = Status::NotFound();
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif
