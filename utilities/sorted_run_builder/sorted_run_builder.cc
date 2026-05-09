//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/sorted_run_builder.h"

#include <atomic>

#include "file/filename.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"
#include "util/atomic.h"

namespace ROCKSDB_NAMESPACE {

class SortedRunBuilderImpl : public SortedRunBuilder {
 public:
  explicit SortedRunBuilderImpl(const SortedRunBuilderOptions& options)
      : options_(options),
        finished_(false),
        cleaned_up_(false),
        num_entries_(0),
        data_size_(0) {}

  SortedRunBuilderImpl(const SortedRunBuilderImpl&) = delete;
  SortedRunBuilderImpl& operator=(const SortedRunBuilderImpl&) = delete;
  SortedRunBuilderImpl(SortedRunBuilderImpl&&) = delete;
  SortedRunBuilderImpl& operator=(SortedRunBuilderImpl&&) = delete;

  ~SortedRunBuilderImpl() override {
    if (!cleaned_up_ && !options_.keep_temp_db) {
      CleanupInternal().PermitUncheckedError();
    }
  }

  Status Open() {
    Options db_options;

    // Use VectorRepFactory: accepts unsorted writes, sorts on flush
    db_options.memtable_factory = std::make_shared<VectorRepFactory>();
    db_options.allow_concurrent_memtable_write = false;

    // Universal compaction for merging all L0 files
    db_options.compaction_style = kCompactionStyleUniversal;

    // Disable auto compaction — we compact manually at the end
    db_options.disable_auto_compactions = true;

    // Memory budget
    db_options.write_buffer_size = options_.write_buffer_size;
    db_options.max_write_buffer_number = options_.max_write_buffer_number;

    // Output file size
    db_options.target_file_size_base = options_.target_file_size_bytes;

    // Compression
    db_options.compression = options_.compression;

    // Compaction threads and subcompactions
    db_options.max_background_jobs = options_.max_compaction_threads;
    db_options.max_subcompactions =
        static_cast<uint32_t>(options_.max_compaction_threads);

    // Comparator
    if (options_.comparator != nullptr) {
      db_options.comparator = options_.comparator;
    }

    // Table factory
    if (options_.table_factory != nullptr) {
      db_options.table_factory = options_.table_factory;
    }

    // WAL is not needed — resumability comes from flushed SSTs
    db_options.manual_wal_flush = true;
    db_options.wal_bytes_per_sync = 0;

    // Create the DB directory; fail if stale data exists
    db_options.create_if_missing = true;
    db_options.error_if_exists = true;

    // Optimize for bulk-load workload
    db_options.max_open_files = -1;
    db_options.info_log_level = HEADER_LEVEL;
    db_options.stats_dump_period_sec = 0;
    db_options.avoid_unnecessary_blocking_io = true;

    return DB::Open(db_options, options_.temp_dir, &db_);
  }

  Status Add(const Slice& key, const Slice& value) override {
    if (finished_) {
      return Status::InvalidArgument("Cannot Add() after Finish()");
    }
    WriteOptions wo;
    wo.disableWAL = true;
    Status s = db_->Put(wo, key, value);
    if (s.ok()) {
      num_entries_.FetchAddRelaxed(1);
      data_size_.FetchAddRelaxed(key.size() + value.size());
    }
    return s;
  }

  Status AddBatch(WriteBatch* batch) override {
    if (finished_) {
      return Status::InvalidArgument("Cannot AddBatch() after Finish()");
    }
    WriteOptions wo;
    wo.disableWAL = true;
    Status s = db_->Write(wo, batch);
    if (s.ok()) {
      num_entries_.FetchAddRelaxed(batch->Count());
      data_size_.FetchAddRelaxed(batch->GetDataSize());
    }
    return s;
  }

  Status Finish() override {
    if (finished_) {
      return Status::InvalidArgument("Finish() already called");
    }

    // Flush all memtables
    FlushOptions flush_opts;
    flush_opts.wait = true;
    Status s = db_->Flush(flush_opts);

    // Compact everything to produce sorted, non-overlapping SST files
    // with seqno=0
    if (s.ok()) {
      CompactRangeOptions cro;
      cro.bottommost_level_compaction =
          BottommostLevelCompaction::kForceOptimized;
      cro.max_subcompactions =
          static_cast<uint32_t>(options_.max_compaction_threads);
      s = db_->CompactRange(cro, nullptr, nullptr);
    }

    // Collect output file paths and accurate post-compaction stats
    if (s.ok()) {
      ColumnFamilyMetaData cf_meta;
      db_->GetColumnFamilyMetaData(&cf_meta);
      output_files_.clear();
      uint64_t total_entries = 0;
      uint64_t total_size = 0;
      for (const auto& level_meta : cf_meta.levels) {
        for (const auto& file_meta : level_meta.files) {
          output_files_.push_back(file_meta.directory + kFilePathSeparator +
                                  file_meta.relative_filename);
          total_entries += file_meta.num_entries;
          total_size += file_meta.size;
        }
      }
      num_entries_.StoreRelaxed(total_entries);
      data_size_.StoreRelaxed(total_size);

      finished_ = true;
    }
    return s;
  }

  const std::vector<std::string>& GetOutputFiles() const override {
    return output_files_;
  }

  uint64_t GetNumEntries() const override { return num_entries_.LoadRelaxed(); }

  uint64_t GetDataSize() const override { return data_size_.LoadRelaxed(); }

  Iterator* NewIterator(const ReadOptions& options) override {
    if (!finished_) {
      return NewErrorIterator(
          Status::InvalidArgument("Must call Finish() before NewIterator()"));
    }
    return db_->NewIterator(options);
  }

  Status Cleanup() override { return CleanupInternal(); }

 private:
  Status CleanupInternal() {
    if (cleaned_up_) {
      return Status::OK();
    }
    cleaned_up_ = true;

    Status s;
    if (db_) {
      const std::string& db_path = options_.temp_dir;
      Options db_options;
      if (options_.comparator != nullptr) {
        db_options.comparator = options_.comparator;
      }
      db_.reset();  // Close the DB
      s = DestroyDB(db_path, db_options);
    }
    return s;
  }

  SortedRunBuilderOptions options_;
  std::unique_ptr<DB> db_;
  std::atomic<bool> finished_;
  std::atomic<bool> cleaned_up_;
  RelaxedAtomic<uint64_t> num_entries_;
  RelaxedAtomic<uint64_t> data_size_;
  std::vector<std::string> output_files_;
};

SortedRunBuilder::~SortedRunBuilder() = default;

Status SortedRunBuilder::Create(const SortedRunBuilderOptions& options,
                                std::unique_ptr<SortedRunBuilder>* result) {
  if (options.temp_dir.empty()) {
    return Status::InvalidArgument("temp_dir must not be empty");
  }
  if (options.max_compaction_threads <= 0) {
    return Status::InvalidArgument("max_compaction_threads must be positive");
  }
  if (options.write_buffer_size == 0) {
    return Status::InvalidArgument("write_buffer_size must be non-zero");
  }
  if (options.max_write_buffer_number < 2) {
    return Status::InvalidArgument("max_write_buffer_number must be >= 2");
  }
  if (options.target_file_size_bytes == 0) {
    return Status::InvalidArgument("target_file_size_bytes must be non-zero");
  }

  auto builder = std::make_unique<SortedRunBuilderImpl>(options);
  Status s = builder->Open();
  if (s.ok()) {
    *result = std::move(builder);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
