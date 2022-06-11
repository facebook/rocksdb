//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_source.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <string>

#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_log_format.h"
#include "file/file_prefetch_buffer.h"
#include "monitoring/statistics.h"
#include "options/cf_options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

BlobSource::BlobSource(const BlobFileReader* reader,
                       const ImmutableOptions& immutable_options,
                       const std::string& db_id,
                       const std::string& db_session_id, uint64_t file_number)
    : blob_file_reader_(reader),
      statistics_(immutable_options.statistics.get()),
      clock_(immutable_options.clock),
      blob_cache_(immutable_options.blob_cache),
      lowest_used_cache_tier_(immutable_options.lowest_used_cache_tier),
      base_cache_key_(OffsetableCacheKey(db_id, db_session_id, file_number,
                                         reader->GetFileSize())){};

BlobSource::~BlobSource() { delete blob_file_reader_; }

Status BlobSource::Create(const ImmutableOptions& immutable_options,
                          const FileOptions& file_options,
                          uint32_t column_family_id,
                          HistogramImpl* blob_file_read_hist,
                          uint64_t blob_file_number, const std::string& db_id,
                          const std::string& db_session_id,
                          const std::shared_ptr<IOTracer>& io_tracer,
                          std::unique_ptr<BlobSource>* blob_source) {
  Statistics* const statistics = immutable_options.stats;

  RecordTick(statistics, NO_FILE_OPENS);

  std::unique_ptr<BlobFileReader> reader;

  {
    const Status s = BlobFileReader::Create(
        immutable_options, file_options, column_family_id, blob_file_read_hist,
        blob_file_number, io_tracer, &reader);
    if (!s.ok()) {
      RecordTick(statistics, NO_FILE_ERRORS);
      return s;
    }
  }

  assert(reader);

  blob_source->reset(new BlobSource(reader.get(), immutable_options, db_id,
                                    db_session_id, blob_file_number));

  reader.release();

  return Status::OK();
}

Status BlobSource::MaybeReadBlobFromCache(
    FilePrefetchBuffer* prefetch_buffer, const ReadOptions& read_options,
    uint64_t offset, const bool wait,
    CachableEntry<PinnableSlice>* blob_entry) const {
  assert(blob_entry != nullptr);
  assert(blob_entry->IsEmpty());

  // First, try to get the blob from the cache
  //
  // If either blob cache is enabled, we'll try to read from it.
  Status s;
  Slice key;
  if (blob_cache_ != nullptr) {
    // create key for blob cache
    CacheKey key_data = base_cache_key_.WithOffset(offset);
    key = key_data.AsSlice();

    s = GetBlobFromCache(key, blob_cache_.get(), blob_entry, wait);

    if (blob_entry->GetValue()) {
      if (prefetch_buffer) {
        // Update the blob details so that PrefetchBuffer can use the read
        // pattern to determine if reads are sequential or not for prefetching.
        // It should also take in account blob read from cache.
        prefetch_buffer->UpdateReadPattern(
            offset, blob_entry->GetValue()->size(),
            read_options.adaptive_readahead /* decrease_readahead_size */);
      }
    }
  }

  assert(s.ok() || blob_entry->GetValue() == nullptr);

  return s;
}

Cache::Handle* BlobSource::GetEntryFromCache(
    const CacheTier& cache_tier, Cache* blob_cache, const Slice& key,
    const bool wait, const Cache::CacheItemHelper* cache_helper,
    const Cache::CreateCallback& create_cb, Cache::Priority priority) const {
  assert(blob_cache);

  Cache::Handle* cache_handle = nullptr;

  if (cache_tier == CacheTier::kNonVolatileBlockTier) {
    cache_handle = blob_cache->Lookup(key, cache_helper, create_cb, priority,
                                      wait, statistics_);
  } else {
    cache_handle = blob_cache->Lookup(key, statistics_);
  }

  return cache_handle;
}

Status BlobSource::InsertEntryToCache(
    const CacheTier& cache_tier, Cache* blob_cache, const Slice& key,
    const Cache::CacheItemHelper* cache_helper, const Slice* blob,
    size_t charge, Cache::Handle** cache_handle,
    Cache::Priority priority) const {
  Status s;

  if (cache_tier == CacheTier::kNonVolatileBlockTier) {
    s = blob_cache->Insert(
        key, const_cast<void*>(static_cast<const void*>(blob->data())),
        cache_helper, charge, cache_handle, priority);
  } else {
    s = blob_cache->Insert(
        key, const_cast<void*>(static_cast<const void*>(blob->data())), charge,
        nullptr, cache_handle, priority);
  }

  return s;
}

Status BlobSource::GetBlobFromCache(const Slice& cache_key, Cache* blob_cache,
                                    CachableEntry<PinnableSlice>* blob,
                                    bool wait) const {
  assert(blob);
  assert(blob->IsEmpty());
  const Cache::Priority priority = Cache::Priority::LOW;

  // Lookup uncompressed cache first
  if (blob_cache != nullptr) {
    assert(!cache_key.empty());
    Cache::Handle* cache_handle = nullptr;
    cache_handle = GetEntryFromCache(
        lowest_used_cache_tier_, blob_cache, cache_key, wait,
        nullptr /* cache_helper */, nullptr /* create_db */, priority);
    if (cache_handle != nullptr) {
      blob->SetCachedValue(
          reinterpret_cast<PinnableSlice*>(blob_cache->Value(cache_handle)),
          blob_cache, cache_handle);
      return Status::OK();
    }
  }

  assert(blob->IsEmpty());

  // TODO: If not found, search from the compressed blob cache.

  return Status::NotFound("Blob record not found in cache");
}

Status BlobSource::PutBlobToCache(const Slice& cache_key, Cache* blob_cache,
                                  CachableEntry<PinnableSlice>* cached_blob,
                                  PinnableSlice* blob) const {
  assert(blob);
  assert(!cache_key.empty());

  const Cache::Priority priority = Cache::Priority::LOW;

  Status s;

  // TODO: Insert compressed blob into compressed blob cache.
  // Release the hold on the compressed cache entry immediately.

  // Insert into uncompressed blob cache
  if (blob_cache != nullptr) {
    Cache::Handle* cache_handle = nullptr;
    s = InsertEntryToCache(lowest_used_cache_tier_, blob_cache, cache_key,
                           GetCacheItemHelper(), blob, blob->size(),
                           &cache_handle, priority);
    if (s.ok()) {
      assert(cache_handle != nullptr);
      cached_blob->SetCachedValue(blob, blob_cache, cache_handle);
    }
  }

  return s;
}

Status BlobSource::GetBlob(const ReadOptions& read_options,
                           const Slice& user_key, uint64_t offset,
                           uint64_t value_size,
                           CompressionType compression_type,
                           FilePrefetchBuffer* prefetch_buffer,
                           PinnableSlice* value, uint64_t* bytes_read) {
  assert(value);

  const uint64_t key_size = user_key.size();

  if (!IsValidBlobOffset(offset, key_size, value_size,
                         blob_file_reader_->GetFileSize())) {
    return Status::Corruption("Invalid blob offset");
  }

  if (compression_type != blob_file_reader_->GetCompressionType()) {
    return Status::Corruption("Compression type mismatch when reading blob");
  }

  // Note: if verify_checksum is set, we read the entire blob record to be able
  // to perform the verification; otherwise, we just read the blob itself. Since
  // the offset in BlobIndex actually points to the blob value, we need to make
  // an adjustment in the former case.
  const uint64_t adjustment =
      read_options.verify_checksums
          ? BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size)
          : 0;
  assert(offset >= adjustment);

  const uint64_t record_size = value_size + adjustment;

  CachableEntry<PinnableSlice> blob_entry;

  // TODO: We haven't support cache tiering for blob files yet, but will do it
  // later.
  if (blob_cache_ &&
      lowest_used_cache_tier_ != CacheTier::kNonVolatileBlockTier) {
    const Status s =
        MaybeReadBlobFromCache(prefetch_buffer, read_options, offset,
                               false /* wait_for_cache */, &blob_entry);

    if (s.ok() && blob_entry.GetValue() != nullptr) {
      assert(blob_entry.GetValue()->size() == value_size);
      if (bytes_read) {
        *bytes_read = value_size;
      }
      value = blob_entry.GetValue();
      blob_entry.TransferTo(this);
      return s;
    }
  }

  assert(blob_entry.IsEmpty());

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    return Status::Incomplete("no blocking io");
  }

  // Can't find the blob from the cache. Since I/O is allowed, read from the
  // file.
  {
    const Status s = blob_file_reader_->GetBlob(
        read_options, user_key, offset, value_size, compression_type,
        prefetch_buffer, value, bytes_read);
    if (!s.ok()) {
      return s;
    }

    if (bytes_read) {
      *bytes_read = record_size;
    }
  }

  if (read_options.fill_cache) {
    // If filling cache is allowed and a cache is configured, try to put the
    // blob to the cache.
    CacheKey key_data = base_cache_key_.WithOffset(offset);
    Slice key = key_data.AsSlice();
    const Status s = PutBlobToCache(key, blob_cache_.get(), &blob_entry, value);
    if (!s.ok()) {
      return s;
    }
    blob_entry.TransferTo(this);
  }

  return Status::OK();
}

void BlobSource::MultiGetBlob(
    const ReadOptions& read_options,
    const autovector<std::reference_wrapper<const Slice>>& user_keys,
    const autovector<uint64_t>& offsets,
    const autovector<uint64_t>& value_sizes, autovector<Status*>& statuses,
    autovector<PinnableSlice*>& blobs, uint64_t* bytes_read) {
  size_t num_blobs = user_keys.size();
  assert(num_blobs > 0);
  assert(num_blobs == offsets.size());
  assert(num_blobs == value_sizes.size());
  assert(num_blobs == statuses.size());
  assert(num_blobs == blobs.size());

#ifndef NDEBUG
  for (size_t i = 0; i < offsets.size() - 1; ++i) {
    assert(offsets[i] <= offsets[i + 1]);
  }
#endif  // !NDEBUG

  uint64_t total_bytes = 0;

  if (blob_cache_ &&
      lowest_used_cache_tier_ != CacheTier::kNonVolatileBlockTier) {
    size_t cached_blob_count = 0;
    for (size_t i = 0; i < num_blobs; ++i) {
      CachableEntry<PinnableSlice> blob_entry;

      Status s = MaybeReadBlobFromCache(
          nullptr /* prefetch_buffer */, read_options, offsets[i],
          false /* wait_for_cache */, &blob_entry);
      if (!s.ok()) {
        assert(statuses[i]);
        *statuses[i] = s;
        continue;
      }

      if (blob_entry.GetValue()) {
        blobs[i] = blob_entry.GetValue();
        blob_entry.TransferTo(this);

        total_bytes += blobs[i]->size();

        // Update the counter for the number of valid blobs read from the cache.
        assert(statuses[i]);
        *statuses[i] = Status::OkOverwritten();  // cache hit
        ++cached_blob_count;
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
      assert(statuses[i]);
      if (!statuses[i]->ok()) {
        *statuses[i] = Status::Incomplete(
            "A cache miss occurs if blob cache is enabled, and no blocking io "
            "is allowed further.");
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
      if (!statuses[i]->IsOkOverwritten()) {
        _user_keys.emplace_back(user_keys[i]);
        _offsets.push_back(offsets[i]);
        _value_sizes.push_back(value_sizes[i]);
        _statuses.push_back(statuses[i]);
        _blobs.push_back(blobs[i]);
      }
    }

    blob_file_reader_->MultiGetBlob(read_options, _user_keys, _offsets,
                                    _value_sizes, _statuses, _blobs,
                                    &_bytes_read);

    if (read_options.fill_cache) {
      // If filling cache is allowed and a cache is configured, try to put
      // the blob to the cache.
      for (size_t i = 0; i < _blobs.size(); ++i) {
        if (_statuses[i]->ok()) {
          CachableEntry<PinnableSlice> blob_entry;
          CacheKey key_data = base_cache_key_.WithOffset(_offsets[i]);
          Slice key = key_data.AsSlice();
          const Status s =
              PutBlobToCache(key, blob_cache_.get(), &blob_entry, _blobs[i]);
          if (!s.ok()) {
            *_statuses[i] = s;
          }
          blob_entry.TransferTo(this);
        }
      }
    }

    total_bytes += _bytes_read;
    if (bytes_read) {
      *bytes_read = total_bytes;
    }

    RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, total_bytes);
  }
}

bool BlobSource::TEST_BlobInCache(uint64_t offset) const {
  assert(blob_cache_ != nullptr);

  Cache* const cache = blob_cache_.get();
  if (cache == nullptr) {
    return false;
  }

  CacheKey key_data = base_cache_key_.WithOffset(offset);
  Slice key = key_data.AsSlice();

  Cache::Handle* const cache_handle = cache->Lookup(key);
  if (cache_handle == nullptr) {
    return false;
  }

  cache->Release(cache_handle);

  return true;
}

}  // namespace ROCKSDB_NAMESPACE
