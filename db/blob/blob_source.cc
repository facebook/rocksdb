//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_source.h"

#include <cassert>
#include <string>

#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_log_format.h"
#include "monitoring/statistics.h"
#include "options/cf_options.h"
#include "table/get_context.h"
#include "table/multiget_context.h"

namespace ROCKSDB_NAMESPACE {

BlobSource::BlobSource(const ImmutableOptions* immutable_options,
                       const std::string& db_id,
                       const std::string& db_session_id,
                       BlobFileCache* blob_file_cache)
    : db_id_(db_id),
      db_session_id_(db_session_id),
      statistics_(immutable_options->statistics.get()),
      blob_file_cache_(blob_file_cache),
      blob_cache_(immutable_options->blob_cache) {}

BlobSource::~BlobSource() = default;

Status BlobSource::GetBlobFromCache(const Slice& cache_key,
                                    CacheHandleGuard<std::string>* blob) const {
  assert(blob);
  assert(blob->IsEmpty());
  assert(blob_cache_);
  assert(!cache_key.empty());

  Cache::Handle* cache_handle = nullptr;
  cache_handle = GetEntryFromCache(cache_key);
  if (cache_handle != nullptr) {
    *blob = CacheHandleGuard<std::string>(blob_cache_.get(), cache_handle);
    return Status::OK();
  }

  assert(blob->IsEmpty());

  return Status::NotFound("Blob not found in cache");
}

Status BlobSource::PutBlobIntoCache(const Slice& cache_key,
                                    CacheHandleGuard<std::string>* cached_blob,
                                    PinnableSlice* blob) const {
  assert(blob);
  assert(!cache_key.empty());
  assert(blob_cache_);

  Status s;
  const Cache::Priority priority = Cache::Priority::LOW;

  // Objects to be put into the cache have to be heap-allocated and
  // self-contained, i.e. own their contents. The Cache has to be able to take
  // unique ownership of them. Therefore, we copy the blob into a string
  // directly, and insert that into the cache.
  std::string* buf = new std::string();
  buf->assign(blob->data(), blob->size());

  // TODO: support custom allocators and provide a better estimated memory
  // usage using malloc_usable_size.
  Cache::Handle* cache_handle = nullptr;
  s = InsertEntryIntoCache(cache_key, buf, buf->size(), &cache_handle,
                           priority);
  if (s.ok()) {
    assert(cache_handle != nullptr);
    *cached_blob =
        CacheHandleGuard<std::string>(blob_cache_.get(), cache_handle);
  }

  return s;
}

Cache::Handle* BlobSource::GetEntryFromCache(const Slice& key) const {
  Cache::Handle* cache_handle = nullptr;
  cache_handle = blob_cache_->Lookup(key, statistics_);
  if (cache_handle != nullptr) {
    PERF_COUNTER_ADD(blob_cache_hit_count, 1);
    RecordTick(statistics_, BLOB_DB_CACHE_HIT);
    RecordTick(statistics_, BLOB_DB_CACHE_BYTES_READ,
               blob_cache_->GetUsage(cache_handle));
  } else {
    RecordTick(statistics_, BLOB_DB_CACHE_MISS);
  }
  return cache_handle;
}

Status BlobSource::InsertEntryIntoCache(const Slice& key, std::string* value,
                                        size_t charge,
                                        Cache::Handle** cache_handle,
                                        Cache::Priority priority) const {
  const Status s =
      blob_cache_->Insert(key, value, charge, &DeleteCacheEntry<std::string>,
                          cache_handle, priority);
  if (s.ok()) {
    assert(*cache_handle != nullptr);
    RecordTick(statistics_, BLOB_DB_CACHE_ADD);
    RecordTick(statistics_, BLOB_DB_CACHE_BYTES_WRITE,
               blob_cache_->GetUsage(*cache_handle));
  } else {
    RecordTick(statistics_, BLOB_DB_CACHE_ADD_FAILURES);
  }
  return s;
}

Status BlobSource::GetBlob(const ReadOptions& read_options,
                           const Slice& user_key, uint64_t file_number,
                           uint64_t offset, uint64_t file_size,
                           uint64_t value_size,
                           CompressionType compression_type,
                           FilePrefetchBuffer* prefetch_buffer,
                           PinnableSlice* value, uint64_t* bytes_read) {
  assert(value);

  Status s;

  const CacheKey cache_key = GetCacheKey(file_number, file_size, offset);

  CacheHandleGuard<std::string> blob_handle;

  // First, try to get the blob from the cache
  //
  // If blob cache is enabled, we'll try to read from it.
  if (blob_cache_) {
    Slice key = cache_key.AsSlice();
    s = GetBlobFromCache(key, &blob_handle);
    if (s.ok() && blob_handle.GetValue()) {
      {
        value->Reset();
        // To avoid copying the cached blob into the buffer provided by the
        // application, we can simply transfer ownership of the cache handle to
        // the target PinnableSlice. This has the potential to save a lot of
        // CPU, especially with large blob values.
        value->PinSlice(
            *blob_handle.GetValue(),
            [](void* arg1, void* arg2) {
              Cache* const cache = static_cast<Cache*>(arg1);
              Cache::Handle* const handle = static_cast<Cache::Handle*>(arg2);
              cache->Release(handle);
            },
            blob_handle.GetCache(), blob_handle.GetCacheHandle());
        // Make the CacheHandleGuard relinquish ownership of the handle.
        blob_handle.TransferTo(nullptr);
      }

      // For consistency, the size of on-disk (possibly compressed) blob record
      // is assigned to bytes_read.
      uint64_t adjustment =
          read_options.verify_checksums
              ? BlobLogRecord::CalculateAdjustmentForRecordHeader(
                    user_key.size())
              : 0;
      assert(offset >= adjustment);

      uint64_t record_size = value_size + adjustment;
      if (bytes_read) {
        *bytes_read = record_size;
      }
      return s;
    }
  }

  assert(blob_handle.IsEmpty());

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    s = Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
    return s;
  }

  // Can't find the blob from the cache. Since I/O is allowed, read from the
  // file.
  {
    CacheHandleGuard<BlobFileReader> blob_file_reader;
    s = blob_file_cache_->GetBlobFileReader(file_number, &blob_file_reader);
    if (!s.ok()) {
      return s;
    }

    assert(blob_file_reader.GetValue());

    if (compression_type != blob_file_reader.GetValue()->GetCompressionType()) {
      return Status::Corruption("Compression type mismatch when reading blob");
    }

    uint64_t read_size = 0;
    s = blob_file_reader.GetValue()->GetBlob(
        read_options, user_key, offset, value_size, compression_type,
        prefetch_buffer, value, &read_size);
    if (!s.ok()) {
      return s;
    }
    if (bytes_read) {
      *bytes_read = read_size;
    }
  }

  if (blob_cache_ && read_options.fill_cache) {
    // If filling cache is allowed and a cache is configured, try to put the
    // blob to the cache.
    Slice key = cache_key.AsSlice();
    s = PutBlobIntoCache(key, &blob_handle, value);
    if (!s.ok()) {
      return s;
    }
  }

  assert(s.ok());
  return s;
}

void BlobSource::MultiGetBlob(const ReadOptions& read_options,
                              autovector<BlobFileReadRequests>& blob_reqs,
                              uint64_t* bytes_read) {
  assert(blob_reqs.size() > 0);

  uint64_t total_bytes_read = 0;
  uint64_t bytes_read_in_file = 0;

  for (auto& [file_number, file_size, blob_reqs_in_file] : blob_reqs) {
    // sort blob_reqs_in_file by file offset.
    std::sort(
        blob_reqs_in_file.begin(), blob_reqs_in_file.end(),
        [](const BlobReadRequest& lhs, const BlobReadRequest& rhs) -> bool {
          return lhs.offset < rhs.offset;
        });

    MultiGetBlobFromOneFile(read_options, file_number, file_size,
                            blob_reqs_in_file, &bytes_read_in_file);

    total_bytes_read += bytes_read_in_file;
  }

  if (bytes_read) {
    *bytes_read = total_bytes_read;
  }
}

void BlobSource::MultiGetBlobFromOneFile(const ReadOptions& read_options,
                                         uint64_t file_number,
                                         uint64_t file_size,
                                         autovector<BlobReadRequest>& blob_reqs,
                                         uint64_t* bytes_read) {
  const size_t num_blobs = blob_reqs.size();
  assert(num_blobs > 0);
  assert(num_blobs <= MultiGetContext::MAX_BATCH_SIZE);

#ifndef NDEBUG
  for (size_t i = 0; i < num_blobs - 1; ++i) {
    assert(blob_reqs[i].offset <= blob_reqs[i + 1].offset);
  }
#endif  // !NDEBUG

  using Mask = uint64_t;
  Mask cache_hit_mask = 0;

  uint64_t total_bytes = 0;
  const OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number,
                                          file_size);

  if (blob_cache_) {
    size_t cached_blob_count = 0;
    for (size_t i = 0; i < num_blobs; ++i) {
      auto& req = blob_reqs[i];

      CacheHandleGuard<std::string> blob_handle;
      const CacheKey cache_key = base_cache_key.WithOffset(req.offset);
      const Slice key = cache_key.AsSlice();

      const Status s = GetBlobFromCache(key, &blob_handle);

      if (s.ok() && blob_handle.GetValue()) {
        assert(req.status);
        *req.status = s;

        {
          req.result->Reset();
          // To avoid copying the cached blob into the buffer provided by the
          // application, we can simply transfer ownership of the cache handle
          // to the target PinnableSlice. This has the potential to save a lot
          // of CPU, especially with large blob values.
          req.result->PinSlice(
              *blob_handle.GetValue(),
              [](void* arg1, void* arg2) {
                Cache* const cache = static_cast<Cache*>(arg1);
                Cache::Handle* const handle = static_cast<Cache::Handle*>(arg2);
                cache->Release(handle);
              },
              blob_handle.GetCache(), blob_handle.GetCacheHandle());
          // Make the CacheHandleGuard relinquish ownership of the handle.
          blob_handle.TransferTo(nullptr);
        }

        // Update the counter for the number of valid blobs read from the cache.
        ++cached_blob_count;

        // For consistency, the size of each on-disk (possibly compressed) blob
        // record is accumulated to total_bytes.
        uint64_t adjustment =
            read_options.verify_checksums
                ? BlobLogRecord::CalculateAdjustmentForRecordHeader(
                      req.user_key->size())
                : 0;
        assert(req.offset >= adjustment);
        total_bytes += req.len + adjustment;
        cache_hit_mask |= (Mask{1} << i);  // cache hit
      }
    }

    // All blobs were read from the cache.
    if (cached_blob_count == num_blobs) {
      if (bytes_read) {
        *bytes_read = total_bytes;
      }
      return;
    }
  }

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    for (size_t i = 0; i < num_blobs; ++i) {
      if (!(cache_hit_mask & (Mask{1} << i))) {
        assert(blob_reqs[i].status);
        *blob_reqs[i].status =
            Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
      }
    }
    return;
  }

  {
    // Find the rest of blobs from the file since I/O is allowed.
    autovector<BlobReadRequest*> _blob_reqs;
    uint64_t _bytes_read = 0;

    for (size_t i = 0; i < num_blobs; ++i) {
      if (!(cache_hit_mask & (Mask{1} << i))) {
        _blob_reqs.push_back(&blob_reqs[i]);
      }
    }

    CacheHandleGuard<BlobFileReader> blob_file_reader;
    Status s =
        blob_file_cache_->GetBlobFileReader(file_number, &blob_file_reader);
    if (!s.ok()) {
      for (size_t i = 0; i < _blob_reqs.size(); ++i) {
        assert(_blob_reqs[i]->status);
        *_blob_reqs[i]->status = s;
      }
      return;
    }

    assert(blob_file_reader.GetValue());

    blob_file_reader.GetValue()->MultiGetBlob(read_options, _blob_reqs,
                                              &_bytes_read);

    if (blob_cache_ && read_options.fill_cache) {
      // If filling cache is allowed and a cache is configured, try to put
      // the blob(s) to the cache.
      for (size_t i = 0; i < _blob_reqs.size(); ++i) {
        if (_blob_reqs[i]->status->ok()) {
          CacheHandleGuard<std::string> blob_handle;
          const CacheKey cache_key =
              base_cache_key.WithOffset(_blob_reqs[i]->offset);
          const Slice key = cache_key.AsSlice();
          s = PutBlobIntoCache(key, &blob_handle, _blob_reqs[i]->result);
          if (!s.ok()) {
            *_blob_reqs[i]->status = s;
          }
        }
      }
    }

    total_bytes += _bytes_read;
    if (bytes_read) {
      *bytes_read = total_bytes;
    }
  }
}

bool BlobSource::TEST_BlobInCache(uint64_t file_number, uint64_t file_size,
                                  uint64_t offset) const {
  const CacheKey cache_key = GetCacheKey(file_number, file_size, offset);
  const Slice key = cache_key.AsSlice();

  CacheHandleGuard<std::string> blob_handle;
  const Status s = GetBlobFromCache(key, &blob_handle);

  if (s.ok() && blob_handle.GetValue() != nullptr) {
    return true;
  }

  return false;
}

}  // namespace ROCKSDB_NAMESPACE
