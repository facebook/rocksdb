//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE

#include <unordered_set>

#include "db/blob_index.h"
#include "monitoring/statistics.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "utilities/blob_db/blob_db_gc_stats.h"
#include "utilities/blob_db/blob_db_impl.h"

namespace ROCKSDB_NAMESPACE {
namespace blob_db {

struct BlobCompactionContext {
  uint64_t next_file_number = 0;
  std::unordered_set<uint64_t> current_blob_files;
  SequenceNumber fifo_eviction_seq = 0;
  uint64_t evict_expiration_up_to = 0;
};

struct BlobCompactionContextGC {
  BlobDBImpl* blob_db_impl = nullptr;
  uint64_t cutoff_file_number = 0;
};

// Compaction filter that deletes expired blob indexes from the base DB.
// Comes into two varieties, one for the non-GC case and one for the GC case.
class BlobIndexCompactionFilterBase : public CompactionFilter {
 public:
  BlobIndexCompactionFilterBase(BlobCompactionContext&& context,
                                uint64_t current_time, Statistics* stats)
      : context_(std::move(context)),
        current_time_(current_time),
        statistics_(stats) {}

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
                    std::string* /*skip_until*/) const override;

 protected:
  Statistics* statistics() const { return statistics_; }

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
                            uint64_t current_time, Statistics* stats)
      : BlobIndexCompactionFilterBase(std::move(context), current_time, stats) {
  }

  const char* Name() const override { return "BlobIndexCompactionFilter"; }
};

class BlobIndexCompactionFilterGC : public BlobIndexCompactionFilterBase {
 public:
  BlobIndexCompactionFilterGC(BlobCompactionContext&& context,
                              BlobCompactionContextGC&& context_gc,
                              uint64_t current_time, Statistics* stats)
      : BlobIndexCompactionFilterBase(std::move(context), current_time, stats),
        context_gc_(std::move(context_gc)) {}

  ~BlobIndexCompactionFilterGC() override;

  const char* Name() const override { return "BlobIndexCompactionFilterGC"; }

  BlobDecision PrepareBlobOutput(const Slice& key, const Slice& existing_value,
                                 std::string* new_value) const override;

 private:
  bool OpenNewBlobFileIfNeeded() const;
  bool ReadBlobFromOldFile(const Slice& key, const BlobIndex& blob_index,
                           PinnableSlice* blob,
                           CompressionType* compression_type) const;
  bool WriteBlobToNewFile(const Slice& key, const Slice& blob,
                          uint64_t* new_blob_file_number,
                          uint64_t* new_blob_offset) const;
  bool CloseAndRegisterNewBlobFileIfNeeded() const;
  bool CloseAndRegisterNewBlobFile() const;

 private:
  BlobCompactionContextGC context_gc_;
  mutable std::shared_ptr<BlobFile> blob_file_;
  mutable std::shared_ptr<Writer> writer_;
  mutable BlobDBGarbageCollectionStats gc_stats_;
};

// Compaction filter factory; similarly to the filters above, it comes
// in two flavors, one that creates filters that support GC, and one
// that creates non-GC filters.
class BlobIndexCompactionFilterFactoryBase : public CompactionFilterFactory {
 public:
  BlobIndexCompactionFilterFactoryBase(BlobDBImpl* _blob_db_impl, Env* _env,
                                       Statistics* _statistics)
      : blob_db_impl_(_blob_db_impl), env_(_env), statistics_(_statistics) {}

 protected:
  BlobDBImpl* blob_db_impl() const { return blob_db_impl_; }
  Env* env() const { return env_; }
  Statistics* statistics() const { return statistics_; }

 private:
  BlobDBImpl* blob_db_impl_;
  Env* env_;
  Statistics* statistics_;
};

class BlobIndexCompactionFilterFactory
    : public BlobIndexCompactionFilterFactoryBase {
 public:
  BlobIndexCompactionFilterFactory(BlobDBImpl* _blob_db_impl, Env* _env,
                                   Statistics* _statistics)
      : BlobIndexCompactionFilterFactoryBase(_blob_db_impl, _env, _statistics) {
  }

  const char* Name() const override {
    return "BlobIndexCompactionFilterFactory";
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override;
};

class BlobIndexCompactionFilterFactoryGC
    : public BlobIndexCompactionFilterFactoryBase {
 public:
  BlobIndexCompactionFilterFactoryGC(BlobDBImpl* _blob_db_impl, Env* _env,
                                     Statistics* _statistics)
      : BlobIndexCompactionFilterFactoryBase(_blob_db_impl, _env, _statistics) {
  }

  const char* Name() const override {
    return "BlobIndexCompactionFilterFactoryGC";
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override;
};

}  // namespace blob_db
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
