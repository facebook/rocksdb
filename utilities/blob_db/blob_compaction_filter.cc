//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_compaction_filter.h"
#include "db/dbformat.h"

#include <cinttypes>

namespace ROCKSDB_NAMESPACE {
namespace blob_db {

CompactionFilter::Decision BlobIndexCompactionFilterBase::FilterV2(
    int level, const Slice& key, ValueType value_type, const Slice& value,
    std::string* new_value, std::string* skip_until) const {
  if (value_type != kBlobIndex) {
    if (user_filter_ != nullptr) {
      // Apply user compaction filter for non-blobindex.
      return user_filter_->FilterV2(level, key, value_type, value, new_value,
                                    skip_until);
    }
    return Decision::kKeep;
  }
  BlobIndex blob_index;
  Status s = blob_index.DecodeFrom(value);
  if (!s.ok()) {
    // Unable to decode blob index. Keeping the value.
    return Decision::kKeep;
  }
  if (blob_index.HasTTL() && blob_index.expiration() <= current_time_) {
    // Expired
    expired_count_++;
    expired_size_ += key.size() + value.size();
    return Decision::kRemove;
  }
  if (!blob_index.IsInlined() &&
      blob_index.file_number() < context_.next_file_number &&
      context_.current_blob_files.count(blob_index.file_number()) == 0) {
    // Corresponding blob file gone (most likely, evicted by FIFO eviction).
    evicted_count_++;
    evicted_size_ += key.size() + value.size();
    return Decision::kRemove;
  }
  if (context_.fifo_eviction_seq > 0 && blob_index.HasTTL() &&
      blob_index.expiration() < context_.evict_expiration_up_to) {
    // Hack: Internal key is passed to BlobIndexCompactionFilter for it to
    // get sequence number.
    ParsedInternalKey ikey;
    bool ok = ParseInternalKey(key, &ikey);
    // Remove keys that could have been remove by last FIFO eviction.
    // If get error while parsing key, ignore and continue.
    if (ok && ikey.sequence < context_.fifo_eviction_seq) {
      evicted_count_++;
      evicted_size_ += key.size() + value.size();
      return Decision::kRemove;
    }
  }
  if (blob_index.IsInlined() && user_filter_ != nullptr) {
    // Apply user compaction filter for inlined data.
    return user_filter_->FilterV2(level, key, value_type, value, new_value,
                                  skip_until);
  }
  return Decision::kKeep;
}

void BlobIndexCompactionFilterBase::SetUserCompactionFilter(
    const CompactionFilter* user_filter) {
  user_filter_ = user_filter;
}

void BlobIndexCompactionFilterBase::SetUserCompactionFilter(
    std::unique_ptr<CompactionFilter>* user_filter) {
  user_filter_ptr_ = std::move(*user_filter);
  user_filter_ = user_filter_ptr_.get();
}

BlobIndexCompactionFilterGC::~BlobIndexCompactionFilterGC() {
  if (blob_file_) {
    CloseAndRegisterNewBlobFile();
  }

  assert(context_gc_.blob_db_impl);

  ROCKS_LOG_INFO(context_gc_.blob_db_impl->db_options_.info_log,
                 "GC pass finished %s: encountered %" PRIu64 " blobs (%" PRIu64
                 " bytes), relocated %" PRIu64 " blobs (%" PRIu64
                 " bytes), created %" PRIu64 " new blob file(s)",
                 !gc_stats_.HasError() ? "successfully" : "with failure",
                 gc_stats_.AllBlobs(), gc_stats_.AllBytes(),
                 gc_stats_.RelocatedBlobs(), gc_stats_.RelocatedBytes(),
                 gc_stats_.NewFiles());

  RecordTick(statistics(), BLOB_DB_GC_NUM_KEYS_RELOCATED,
             gc_stats_.RelocatedBlobs());
  RecordTick(statistics(), BLOB_DB_GC_BYTES_RELOCATED,
             gc_stats_.RelocatedBytes());
  RecordTick(statistics(), BLOB_DB_GC_NUM_NEW_FILES, gc_stats_.NewFiles());
  RecordTick(statistics(), BLOB_DB_GC_FAILURES, gc_stats_.HasError());
}

CompactionFilter::BlobDecision BlobIndexCompactionFilterGC::PrepareBlobOutput(
    const Slice& key, const Slice& existing_value,
    std::string* new_value) const {
  assert(new_value);

  const BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
  (void)blob_db_impl;

  assert(blob_db_impl);
  assert(blob_db_impl->bdb_options_.enable_garbage_collection);

  BlobIndex blob_index;
  const Status s = blob_index.DecodeFrom(existing_value);
  if (!s.ok()) {
    gc_stats_.SetError();
    return BlobDecision::kCorruption;
  }

  if (blob_index.IsInlined()) {
    gc_stats_.AddBlob(blob_index.value().size());

    return BlobDecision::kKeep;
  }

  gc_stats_.AddBlob(blob_index.size());

  if (blob_index.HasTTL()) {
    return BlobDecision::kKeep;
  }

  if (blob_index.file_number() >= context_gc_.cutoff_file_number) {
    return BlobDecision::kKeep;
  }

  // Note: each compaction generates its own blob files, which, depending on the
  // workload, might result in many small blob files. The total number of files
  // is bounded though (determined by the number of compactions and the blob
  // file size option).
  if (!OpenNewBlobFileIfNeeded()) {
    gc_stats_.SetError();
    return BlobDecision::kIOError;
  }

  const CompactionFilter* udf = user_data_filter();

  PinnableSlice blob;
  PinnableSlice blob_raw;  // decompressed blob
  CompressionType compression_type = kNoCompression;
  if (!ReadBlobFromOldFile(key, blob_index, &blob,
                           udf == nullptr ? nullptr : &blob_raw,
                           &compression_type)) {
    gc_stats_.SetError();
    return BlobDecision::kIOError;
  }

  bool value_changed = false;
  if (udf != nullptr) {
    std::string new_filter_value;
    if (udf->Filter(0, key,
                    compression_type == kNoCompression ? blob : blob_raw,
                    &new_filter_value, &value_changed)) {
      return BlobDecision::kRemove;
    }
    if (value_changed) {
      Slice new_blob(new_filter_value);
      std::string compression_output;
      if (compression_type != kNoCompression) {
        new_blob =
            blob_db_impl->GetCompressedSlice(new_blob, &compression_output);
      }
      blob.PinSelf(new_blob);
    }
  }

  uint64_t new_blob_file_number = 0;
  uint64_t new_blob_offset = 0;
  if (!WriteBlobToNewFile(key, blob, &new_blob_file_number, &new_blob_offset)) {
    gc_stats_.SetError();
    return BlobDecision::kIOError;
  }

  if (!CloseAndRegisterNewBlobFileIfNeeded()) {
    gc_stats_.SetError();
    return BlobDecision::kIOError;
  }

  BlobIndex::EncodeBlob(new_value, new_blob_file_number, new_blob_offset,
                        blob.size(), compression_type);

  gc_stats_.AddRelocatedBlob(blob_index.size());

  return BlobDecision::kChangeValue;
}

bool BlobIndexCompactionFilterGC::OpenNewBlobFileIfNeeded() const {
  if (blob_file_) {
    assert(writer_);
    return true;
  }

  BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
  assert(blob_db_impl);

  const Status s = blob_db_impl->CreateBlobFileAndWriter(
      /* has_ttl */ false, ExpirationRange(), "GC", &blob_file_, &writer_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(blob_db_impl->db_options_.info_log,
                    "Error opening new blob file during GC, status: %s",
                    s.ToString().c_str());

    return false;
  }

  assert(blob_file_);
  assert(writer_);

  gc_stats_.AddNewFile();

  return true;
}

bool BlobIndexCompactionFilterGC::ReadBlobFromOldFile(
    const Slice& key, const BlobIndex& blob_index, PinnableSlice* blob,
    PinnableSlice* blob_dc, CompressionType* compression_type) const {
  BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
  assert(blob_db_impl);

  Status s = blob_db_impl->GetRawBlobFromFile(
      key, blob_index.file_number(), blob_index.offset(), blob_index.size(),
      blob, compression_type);

  if (!s.ok()) {
    ROCKS_LOG_ERROR(blob_db_impl->db_options_.info_log,
                    "Error reading blob during GC, key: %s (%s), status: %s",
                    key.ToString(/* output_hex */ true).c_str(),
                    blob_index.DebugString(/* output_hex */ true).c_str(),
                    s.ToString().c_str());

    return false;
  }

  if (blob_dc != nullptr && *compression_type != kNoCompression) {
    s = blob_db_impl->DecompressSlice(*blob, *compression_type, blob_dc);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(
          blob_db_impl->db_options_.info_log,
          "Uncompression error during blob read from file: %" PRIu64
          " blob_offset: %" PRIu64 " blob_size: %" PRIu64
          " key: %s status: '%s'",
          blob_index.file_number(), blob_index.offset(), blob_index.size(),
          key.ToString(/* output_hex */ true).c_str(), s.ToString().c_str());

      return false;
    }
  }

  return true;
}

bool BlobIndexCompactionFilterGC::WriteBlobToNewFile(
    const Slice& key, const Slice& blob, uint64_t* new_blob_file_number,
    uint64_t* new_blob_offset) const {
  assert(new_blob_file_number);
  assert(new_blob_offset);

  assert(blob_file_);
  *new_blob_file_number = blob_file_->BlobFileNumber();

  assert(writer_);
  uint64_t new_key_offset = 0;
  const Status s = writer_->AddRecord(key, blob, kNoExpiration, &new_key_offset,
                                      new_blob_offset);

  if (!s.ok()) {
    const BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
    assert(blob_db_impl);

    ROCKS_LOG_ERROR(
        blob_db_impl->db_options_.info_log,
        "Error writing blob to new file %s during GC, key: %s, status: %s",
        blob_file_->PathName().c_str(),
        key.ToString(/* output_hex */ true).c_str(), s.ToString().c_str());
    return false;
  }

  const uint64_t new_size =
      BlobLogRecord::kHeaderSize + key.size() + blob.size();
  blob_file_->BlobRecordAdded(new_size);

  BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
  assert(blob_db_impl);

  blob_db_impl->total_blob_size_ += new_size;

  return true;
}

bool BlobIndexCompactionFilterGC::CloseAndRegisterNewBlobFileIfNeeded() const {
  const BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
  assert(blob_db_impl);

  assert(blob_file_);
  if (blob_file_->GetFileSize() < blob_db_impl->bdb_options_.blob_file_size) {
    return true;
  }

  return CloseAndRegisterNewBlobFile();
}

bool BlobIndexCompactionFilterGC::CloseAndRegisterNewBlobFile() const {
  BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
  assert(blob_db_impl);
  assert(blob_file_);

  Status s;

  {
    WriteLock wl(&blob_db_impl->mutex_);

    s = blob_db_impl->CloseBlobFile(blob_file_);

    // Note: we delay registering the new blob file until it's closed to
    // prevent FIFO eviction from processing it during the GC run.
    blob_db_impl->RegisterBlobFile(blob_file_);
  }

  assert(blob_file_->Immutable());
  blob_file_.reset();

  if (!s.ok()) {
    ROCKS_LOG_ERROR(blob_db_impl->db_options_.info_log,
                    "Error closing new blob file %s during GC, status: %s",
                    blob_file_->PathName().c_str(), s.ToString().c_str());

    return false;
  }

  return true;
}

std::unique_ptr<CompactionFilter>
BlobIndexCompactionFilterFactory::CreateCompactionFilter(
    const CompactionFilter::Context& context) {
  assert(env());

  int64_t current_time = 0;
  Status s = env()->GetCurrentTime(&current_time);
  if (!s.ok()) {
    return nullptr;
  }
  assert(current_time >= 0);

  assert(blob_db_impl());

  BlobCompactionContext bcontext;
  blob_db_impl()->GetCompactionContext(&bcontext);

  auto rv = new BlobIndexCompactionFilter(std::move(bcontext), current_time,
                                          statistics());

  // compaction_filter takes precedence over compaction_filter_factory if
  // client specifies both. (refer to include\rocksdb\options.h)
  if (user_filter_ != nullptr) {
    rv->SetUserCompactionFilter(user_filter_);
  } else if (user_filter_factory_ != nullptr) {
    auto user_filter = user_filter_factory_->CreateCompactionFilter(context);
    rv->SetUserCompactionFilter(&user_filter);
  }

  return std::unique_ptr<CompactionFilter>(rv);
}

std::unique_ptr<CompactionFilter>
BlobIndexCompactionFilterFactoryGC::CreateCompactionFilter(
    const CompactionFilter::Context& context) {
  assert(env());

  int64_t current_time = 0;
  Status s = env()->GetCurrentTime(&current_time);
  if (!s.ok()) {
    return nullptr;
  }
  assert(current_time >= 0);

  assert(blob_db_impl());

  BlobCompactionContext bcontext;
  BlobCompactionContextGC bcontext_gc;
  blob_db_impl()->GetCompactionContext(&bcontext, &bcontext_gc);

  auto rv = new BlobIndexCompactionFilterGC(
      std::move(bcontext), std::move(bcontext_gc), current_time, statistics());

  // compaction_filter takes precedence over compaction_filter_factory if
  // client specifies both. (refer to include\rocksdb\options.h)
  if (user_filter_ != nullptr) {
    rv->SetUserCompactionFilter(user_filter_);
  } else if (user_filter_factory_ != nullptr) {
    auto user_filter = user_filter_factory_->CreateCompactionFilter(context);
    rv->SetUserCompactionFilter(&user_filter);
  }

  return std::unique_ptr<CompactionFilter>(rv);
}

}  // namespace blob_db
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
