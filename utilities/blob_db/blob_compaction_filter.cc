//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_compaction_filter.h"
#include "db/dbformat.h"

namespace rocksdb {
namespace blob_db {

// CompactionFilter to delete expired blob index from base DB.
class BlobIndexCompactionFilterBase : public CompactionFilter {
 public:
  BlobIndexCompactionFilterBase(BlobCompactionContext&& context,
                                uint64_t current_time, Statistics* statistics)
      : context_(std::move(context)),
        current_time_(current_time),
        statistics_(statistics) {}

  ~BlobIndexCompactionFilterBase() override {
    RecordTick(statistics_, BLOB_DB_BLOB_INDEX_EXPIRED_COUNT, expired_count_);
    RecordTick(statistics_, BLOB_DB_BLOB_INDEX_EXPIRED_SIZE, expired_size_);
    RecordTick(statistics_, BLOB_DB_BLOB_INDEX_EVICTED_COUNT, evicted_count_);
    RecordTick(statistics_, BLOB_DB_BLOB_INDEX_EVICTED_SIZE, evicted_size_);
  }

  // Filter expired blob indexes regardless of snapshots.
  bool IgnoreSnapshots() const override { return true; }

  Decision FilterV2(int /*level*/, const Slice& key, ValueType value_type,
                    const Slice& value, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    if (value_type != kBlobIndex) {
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
      // Corresponding blob file gone. Could have been garbage collected or
      // evicted by FIFO eviction.
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
    return Decision::kKeep;
  }

 private:
  BlobCompactionContext context_;
  const uint64_t current_time_;
  Statistics* statistics_;
  // It is safe to not using std::atomic since the compaction filter, created
  // from a compaction filter factroy, will not be called from multiple threads.
  mutable uint64_t expired_count_ = 0;
  mutable uint64_t expired_size_ = 0;
  mutable uint64_t evicted_count_ = 0;
  mutable uint64_t evicted_size_ = 0;
};

class BlobIndexCompactionFilter : public BlobIndexCompactionFilterBase {
 public:
  BlobIndexCompactionFilter(BlobCompactionContext&& context,
                            uint64_t current_time, Statistics* statistics)
      : BlobIndexCompactionFilterBase(std::move(context), current_time,
                                      statistics) {}

  const char* Name() const override { return "BlobIndexCompactionFilter"; }
};

class BlobIndexCompactionFilterGC : public BlobIndexCompactionFilterBase {
 public:
  BlobIndexCompactionFilterGC(BlobCompactionContext&& context,
                              BlobCompactionContextGC&& context_gc,
                              uint64_t current_time, Statistics* statistics)
      : BlobIndexCompactionFilterBase(std::move(context), current_time,
                                      statistics),
        context_gc_(std::move(context_gc)) {}

  ~BlobIndexCompactionFilterGC() override {
    if (blob_file_) {
      BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
      assert(blob_db_impl);

      MutexLock l(&blob_db_impl->write_mutex_);
      WriteLock lock(&blob_db_impl->mutex_);
      WriteLock file_lock(&blob_file_->mutex_);
      blob_db_impl->CloseBlobFile(blob_file_);
    }
  }

  const char* Name() const override { return "BlobIndexCompactionFilterGC"; }

  bool PrepareBlobOutput(const Slice& key, const Slice& existing_value,
                         std::string* new_value) const override {
    assert(new_value);

    BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
    assert(blob_db_impl);
    assert(blob_db_impl->bdb_options_.enable_garbage_collection);

    BlobIndex blob_index;
    const Status s = blob_index.DecodeFrom(existing_value);
    if (!s.ok()) {
      return false;
    }

    if (blob_index.IsInlined()) {
      return false;
    }

    if (blob_index.HasTTL()) {
      return false;
    }

    if (blob_index.file_number() >= context_gc_.cutoff_file_number) {
      return false;
    }

    if (!OpenNewBlobFileIfNeeded()) {
      return false;
    }

    PinnableSlice blob;
    CompressionType compression_type = kNoCompression;
    if (!ReadBlobFromOldFile(key, blob_index, &blob, &compression_type)) {
      return false;
    }

    uint64_t new_blob_file_number = 0;
    uint64_t new_blob_offset = 0;
    if (!WriteBlobToNewFile(key, blob, &new_blob_file_number,
                            &new_blob_offset)) {
      return false;
    }

    if (!CloseNewBlobFileIfNeeded()) {
      return false;
    }

    BlobIndex::EncodeBlob(new_value, new_blob_file_number, new_blob_offset,
                          blob.size(), compression_type);

    return true;
  }

 private:
  bool OpenNewBlobFileIfNeeded() const {
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

    WriteLock wl(&blob_db_impl->mutex_);
    blob_db_impl->RegisterBlobFile(blob_file_);

    return true;
  }

  bool ReadBlobFromOldFile(const Slice& key, const BlobIndex& blob_index,
                           PinnableSlice* blob,
                           CompressionType* compression_type) const {
    BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
    assert(blob_db_impl);

    const Status s = blob_db_impl->GetRawBlobFromFile(
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

    return true;
  }

  bool WriteBlobToNewFile(const Slice& key, const Slice& blob,
                          uint64_t* new_blob_file_number,
                          uint64_t* new_blob_offset) const {
    assert(new_blob_file_number);
    assert(new_blob_offset);

    assert(blob_file_);
    *new_blob_file_number = blob_file_->BlobFileNumber();

    assert(writer_);
    uint64_t new_key_offset = 0;
    const Status s = writer_->AddRecord(key, blob, kNoExpiration,
                                        &new_key_offset, new_blob_offset);

    if (!s.ok()) {
      BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
      assert(blob_db_impl);

      ROCKS_LOG_ERROR(
          blob_db_impl->db_options_.info_log,
          "Error writing blob to new file %s during GC, key: %s, status: %s",
          blob_file_->PathName().c_str(),
          key.ToString(/* output_hex */ true).c_str(), s.ToString().c_str());
      return false;
    }

    blob_file_->blob_count_++;

    const uint64_t new_size =
        BlobLogRecord::kHeaderSize + key.size() + blob.size();
    blob_file_->file_size_ += new_size;

    BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
    assert(blob_db_impl);

    blob_db_impl->total_blob_size_ += new_size;

    return true;
  }

  bool CloseNewBlobFileIfNeeded() const {
    BlobDBImpl* const blob_db_impl = context_gc_.blob_db_impl;
    assert(blob_db_impl);

    Status s;

    {
      MutexLock l(&blob_db_impl->write_mutex_);
      s = blob_db_impl->CloseBlobFileIfNeeded(blob_file_);
    }

    if (!s.ok()) {
      ROCKS_LOG_ERROR(blob_db_impl->db_options_.info_log,
                      "Error closing new blob file %s during GC, status: %s",
                      blob_file_->PathName().c_str(), s.ToString().c_str());
    }

    assert(blob_file_);
    if (blob_file_->Immutable()) {
      blob_file_.reset();
    }

    return s.ok();
  }

 private:
  BlobCompactionContextGC context_gc_;
  mutable std::shared_ptr<BlobFile> blob_file_;
  mutable std::shared_ptr<Writer> writer_;
};

template <typename Filter>
class FilterTraits;

template <>
class FilterTraits<BlobIndexCompactionFilter> {
 public:
  static std::unique_ptr<CompactionFilter> Create(BlobDBImpl* blob_db_impl,
                                                  uint64_t current_time,
                                                  Statistics* statistics) {
    assert(blob_db_impl);

    BlobCompactionContext context;
    blob_db_impl->GetCompactionContext(&context);

    return std::unique_ptr<CompactionFilter>(new BlobIndexCompactionFilter(
        std::move(context), current_time, statistics));
  }
};

template <>
class FilterTraits<BlobIndexCompactionFilterGC> {
 public:
  static std::unique_ptr<CompactionFilter> Create(BlobDBImpl* blob_db_impl,
                                                  uint64_t current_time,
                                                  Statistics* statistics) {
    assert(blob_db_impl);

    BlobCompactionContext context;
    BlobCompactionContextGC context_gc;
    blob_db_impl->GetCompactionContext(&context, &context_gc);

    return std::unique_ptr<CompactionFilter>(new BlobIndexCompactionFilterGC(
        std::move(context), std::move(context_gc), current_time, statistics));
  }
};

template <typename Filter>
std::unique_ptr<CompactionFilter>
BlobIndexCompactionFilterFactoryBase<Filter>::CreateCompactionFilter(
    const CompactionFilter::Context& /*context*/) {
  int64_t current_time = 0;
  Status s = env_->GetCurrentTime(&current_time);
  if (!s.ok()) {
    return nullptr;
  }
  assert(current_time >= 0);

  return FilterTraits<Filter>::Create(
      blob_db_impl_, static_cast<uint64_t>(current_time), statistics_);
}

template class BlobIndexCompactionFilterFactoryBase<BlobIndexCompactionFilter>;
template class BlobIndexCompactionFilterFactoryBase<
    BlobIndexCompactionFilterGC>;

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
