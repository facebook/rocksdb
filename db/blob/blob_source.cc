//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_source.h"

#include <cassert>
#include <string>

#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_log_format.h"
#include "options/cf_options.h"
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
                                    CachableEntry<std::string>* blob) const {
  assert(blob);
  assert(blob->IsEmpty());
  assert(blob_cache_);
  assert(!cache_key.empty());

  Cache::Handle* cache_handle = nullptr;
  cache_handle = GetEntryFromCache(cache_key);
  if (cache_handle != nullptr) {
    blob->SetCachedValue(
        static_cast<std::string*>(blob_cache_->Value(cache_handle)),
        blob_cache_.get(), cache_handle);
    return Status::OK();
  }

  assert(blob->IsEmpty());

  return Status::NotFound("Blob not found in cache");
}

Status BlobSource::PutBlobIntoCache(const Slice& cache_key,
                                    CachableEntry<std::string>* cached_blob,
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
    cached_blob->SetCachedValue(buf, blob_cache_.get(), cache_handle);
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

  CachableEntry<std::string> blob_entry;

  // First, try to get the blob from the cache
  //
  // If blob cache is enabled, we'll try to read from it.
  if (blob_cache_) {
    Slice key = cache_key.AsSlice();
    s = GetBlobFromCache(key, &blob_entry);
    if (s.ok() && blob_entry.GetValue()) {
      // For consistency, the size of on-disk (possibly compressed) blob record
      // is assigned to bytes_read.
      if (bytes_read) {
        uint64_t adjustment =
            read_options.verify_checksums
                ? BlobLogRecord::CalculateAdjustmentForRecordHeader(
                      user_key.size())
                : 0;
        assert(offset >= adjustment);
        *bytes_read = value_size + adjustment;
      }
      value->PinSelf(*blob_entry.GetValue());
      return s;
    }
  }

  assert(blob_entry.IsEmpty());

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

    s = blob_file_reader.GetValue()->GetBlob(
        read_options, user_key, offset, value_size, compression_type,
        prefetch_buffer, value, bytes_read);
    if (!s.ok()) {
      return s;
    }
  }

  if (blob_cache_ && read_options.fill_cache) {
    // If filling cache is allowed and a cache is configured, try to put the
    // blob to the cache.
    Slice key = cache_key.AsSlice();
    s = PutBlobIntoCache(key, &blob_entry, value);
    if (!s.ok()) {
      return s;
    }
  }

  assert(s.ok());
  return s;
}

void BlobSource::MultiGetBlob(
    const ReadOptions& read_options,
    const autovector<std::reference_wrapper<const Slice>>& user_keys,
    uint64_t file_number, uint64_t file_size,
    const autovector<uint64_t>& offsets,
    const autovector<uint64_t>& value_sizes, autovector<Status*>& statuses,
    autovector<PinnableSlice*>& blobs, uint64_t* bytes_read) {
  size_t num_blobs = user_keys.size();
  assert(num_blobs > 0);
  assert(num_blobs <= MultiGetContext::MAX_BATCH_SIZE);
  assert(num_blobs == offsets.size());
  assert(num_blobs == value_sizes.size());
  assert(num_blobs == statuses.size());
  assert(num_blobs == blobs.size());

#ifndef NDEBUG
  for (size_t i = 0; i < offsets.size() - 1; ++i) {
    assert(offsets[i] <= offsets[i + 1]);
  }
#endif  // !NDEBUG

  using Mask = uint64_t;
  Mask cache_hit_mask = 0;

  Status s;
  uint64_t total_bytes = 0;
  const OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number,
                                          file_size);

  if (blob_cache_) {
    size_t cached_blob_count = 0;
    for (size_t i = 0; i < num_blobs; ++i) {
      CachableEntry<std::string> blob_entry;
      const CacheKey cache_key = base_cache_key.WithOffset(offsets[i]);
      const Slice key = cache_key.AsSlice();

      s = GetBlobFromCache(key, &blob_entry);
      if (s.ok() && blob_entry.GetValue()) {
        assert(statuses[i]);
        *statuses[i] = s;
        blobs[i]->PinSelf(*blob_entry.GetValue());

        // Update the counter for the number of valid blobs read from the cache.
        ++cached_blob_count;
        // For consistency, the size of each on-disk (possibly compressed) blob
        // record is accumulated to total_bytes.
        uint64_t adjustment =
            read_options.verify_checksums
                ? BlobLogRecord::CalculateAdjustmentForRecordHeader(
                      user_keys[i].get().size())
                : 0;
        assert(offsets[i] >= adjustment);
        total_bytes += value_sizes[i] + adjustment;
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
        assert(statuses[i]);
        *statuses[i] =
            Status::Incomplete("Cannot read blob(s): no disk I/O allowed");
      }
    }
    return;
  }

  {
    // Find the rest of blobs from the file since I/O is allowed.
    autovector<std::reference_wrapper<const Slice>> _user_keys;
    autovector<uint64_t> _offsets;
    autovector<uint64_t> _value_sizes;
    autovector<Status*> _statuses;
    autovector<PinnableSlice*> _blobs;
    uint64_t _bytes_read = 0;

    for (size_t i = 0; i < num_blobs; ++i) {
      if (!(cache_hit_mask & (Mask{1} << i))) {
        _user_keys.emplace_back(user_keys[i]);
        _offsets.push_back(offsets[i]);
        _value_sizes.push_back(value_sizes[i]);
        _statuses.push_back(statuses[i]);
        _blobs.push_back(blobs[i]);
      }
    }

    CacheHandleGuard<BlobFileReader> blob_file_reader;
    s = blob_file_cache_->GetBlobFileReader(file_number, &blob_file_reader);
    if (!s.ok()) {
      for (size_t i = 0; i < _blobs.size(); ++i) {
        assert(_statuses[i]);
        *_statuses[i] = s;
      }
      return;
    }

    assert(blob_file_reader.GetValue());

    blob_file_reader.GetValue()->MultiGetBlob(read_options, _user_keys,
                                              _offsets, _value_sizes, _statuses,
                                              _blobs, &_bytes_read);

    if (read_options.fill_cache) {
      // If filling cache is allowed and a cache is configured, try to put
      // the blob(s) to the cache.
      for (size_t i = 0; i < _blobs.size(); ++i) {
        if (_statuses[i]->ok()) {
          CachableEntry<std::string> blob_entry;
          const CacheKey cache_key = base_cache_key.WithOffset(_offsets[i]);
          const Slice key = cache_key.AsSlice();
          s = PutBlobIntoCache(key, &blob_entry, _blobs[i]);
          if (!s.ok()) {
            *_statuses[i] = s;
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

  CachableEntry<std::string> blob_entry;
  const Status s = GetBlobFromCache(key, &blob_entry);

  if (s.ok() && blob_entry.GetValue() != nullptr) {
    return true;
  }

  return false;
}

}  // namespace ROCKSDB_NAMESPACE
