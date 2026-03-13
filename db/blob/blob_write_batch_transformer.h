//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "rocksdb/advanced_options.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {

class BlobFilePartitionManager;
class Cache;

// Callback to look up per-CF blob settings.
struct BlobDirectWriteSettings {
  bool enable_blob_direct_write = false;
  uint64_t min_blob_size = 0;
  CompressionType compression_type = kNoCompression;
  // Raw pointer — the Cache is owned by ColumnFamilyOptions and outlives all
  // settings snapshots. Using raw avoids 2 atomic ref-count ops per Put().
  Cache* blob_cache = nullptr;
  PrepopulateBlobCache prepopulate_blob_cache = PrepopulateBlobCache::kDisable;
};

using BlobDirectWriteSettingsProvider =
    std::function<BlobDirectWriteSettings(uint32_t column_family_id)>;

// Transforms a WriteBatch by writing large values directly to blob files
// and replacing them with BlobIndex entries. Non-qualifying entries
// (small values, deletes, merges, etc.) are passed through unchanged.
class BlobWriteBatchTransformer : public WriteBatch::Handler {
 public:
  BlobWriteBatchTransformer(
      BlobFilePartitionManager* partition_manager, WriteBatch* output_batch,
      const BlobDirectWriteSettingsProvider& settings_provider,
      const WriteOptions& write_options);

  // Transform a WriteBatch. If no values qualify for blob separation,
  // output_batch will be empty and the caller should use the original batch.
  // If any values are separated, output_batch contains the full transformed
  // batch.
  static Status TransformBatch(
      const WriteOptions& write_options, WriteBatch* input_batch,
      WriteBatch* output_batch, BlobFilePartitionManager* partition_manager,
      const BlobDirectWriteSettingsProvider& settings_provider,
      bool* transformed);

  // WriteBatch::Handler overrides
  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override;

  Status TimedPutCF(uint32_t column_family_id, const Slice& key,
                    const Slice& value, uint64_t write_time) override;

  Status PutEntityCF(uint32_t column_family_id, const Slice& key,
                     const Slice& entity) override;

  Status DeleteCF(uint32_t column_family_id, const Slice& key) override;

  Status SingleDeleteCF(uint32_t column_family_id, const Slice& key) override;

  Status DeleteRangeCF(uint32_t column_family_id, const Slice& begin_key,
                       const Slice& end_key) override;

  Status MergeCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override;

  Status PutBlobIndexCF(uint32_t column_family_id, const Slice& key,
                        const Slice& value) override;

  void LogData(const Slice& blob) override;

  Status MarkBeginPrepare(bool unprepared = false) override;
  Status MarkEndPrepare(const Slice& xid) override;
  Status MarkCommit(const Slice& xid) override;
  Status MarkCommitWithTimestamp(const Slice& xid, const Slice& ts) override;
  Status MarkRollback(const Slice& xid) override;
  Status MarkNoop(bool empty_batch) override;

  bool HasTransformed() const { return has_transformed_; }

 private:
  BlobFilePartitionManager* partition_manager_;
  WriteBatch* output_batch_;
  BlobDirectWriteSettingsProvider settings_provider_;
  const WriteOptions& write_options_;
  bool has_transformed_ = false;
  std::string blob_index_buf_;
  // Per-batch settings cache: avoids re-acquiring settings for each entry
  // when a batch has many entries for the same column family.
  uint32_t cached_cf_id_ = UINT32_MAX;
  BlobDirectWriteSettings cached_settings_;
};

}  // namespace ROCKSDB_NAMESPACE
