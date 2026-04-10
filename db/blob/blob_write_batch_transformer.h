//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/advanced_options.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

class BlobFilePartitionManager;
class Cache;

// Callback to look up per-CF blob settings.
struct BlobDirectWriteSettings {
  // Whether this column family should attempt direct-write blob separation.
  bool enable_blob_direct_write = false;
  // Minimum value size eligible for direct-write blob separation.
  uint64_t min_blob_size = 0;
  // Compression to use for newly written blob records.
  CompressionType compression_type = kNoCompression;
  // Raw pointer — the Cache is owned by ColumnFamilyOptions and outlives all
  // settings snapshots. Using raw avoids 2 atomic ref-count ops per Put().
  Cache* blob_cache = nullptr;
  // Blob-cache prepopulation policy for direct-write records.
  PrepopulateBlobCache prepopulate_blob_cache = PrepopulateBlobCache::kDisable;
};

using BlobDirectWriteSettingsProvider =
    std::function<BlobDirectWriteSettings(uint32_t column_family_id)>;

// Callback to look up per-CF partition manager.
using BlobPartitionManagerProvider =
    std::function<BlobFilePartitionManager*(uint32_t column_family_id)>;

// Transforms a WriteBatch by writing large values directly to blob files
// and replacing them with BlobIndex entries. Non-qualifying entries
// (small values, deletes, merges, etc.) are passed through unchanged.
class BlobWriteBatchTransformer : public WriteBatch::Handler {
 public:
  struct RollbackInfo {
    // Manager that owns the file updated during transform.
    BlobFilePartitionManager* partition_mgr = nullptr;
    // Blob file number written during transform.
    uint64_t file_number = 0;
    // Number of blob records appended to that file.
    uint64_t count = 0;
    // Number of bytes appended to that file.
    uint64_t bytes = 0;
  };

  // Builds the final serialized entity for one PutEntity write.
  //
  // `columns` must already be sorted by column name. When direct-write blob
  // separation is enabled and one or more columns meet `min_blob_size`, this
  // writes those columns to blob files and returns a V2 entity containing
  // BlobIndex references. Otherwise, if `serialize_inline_entity` is true, it
  // returns a normal V1 entity with all columns inline.
  //
  // `rollback_infos`, when provided, is appended with the exact blob writes
  // performed before the method returns, even if it later fails, so callers
  // can account those bytes as initial garbage.
  static Status MaybePreprocessWideColumns(
      const WriteOptions& write_options, uint32_t column_family_id,
      const Slice& key, const WideColumns& columns,
      BlobFilePartitionManager* partition_mgr,
      const BlobDirectWriteSettings& settings, bool serialize_inline_entity,
      std::string* entity, bool* transformed,
      std::vector<RollbackInfo>* rollback_infos = nullptr);

  // Constructs a per-batch transformer. Most callers should use
  // TransformBatch() instead of driving the handler directly.
  BlobWriteBatchTransformer(
      const BlobPartitionManagerProvider& partition_mgr_provider,
      WriteBatch* output_batch,
      const BlobDirectWriteSettingsProvider& settings_provider,
      const WriteOptions& write_options);

  // Transform a WriteBatch. If no values qualify for blob separation,
  // output_batch will be empty and the caller should use the original batch.
  // If any values are separated, output_batch contains the full transformed
  // batch. used_managers (if non-null) receives the set of partition managers
  // that had data written to them, so the caller can flush/sync them.
  // rollback_infos (if non-null) receives the exact file/count/byte writes so
  // a failed transformed attempt can be accounted as initial garbage. Partial
  // results are returned even if this method fails after some blob writes.
  static Status TransformBatch(
      const WriteOptions& write_options, WriteBatch* input_batch,
      WriteBatch* output_batch,
      const BlobPartitionManagerProvider& partition_mgr_provider,
      const BlobDirectWriteSettingsProvider& settings_provider,
      bool* transformed,
      std::vector<BlobFilePartitionManager*>* used_managers = nullptr,
      std::vector<RollbackInfo>* rollback_infos = nullptr);

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

  // Returns true once at least one value has been rewritten to a BlobIndex.
  bool HasTransformed() const { return has_transformed_; }

 private:
  // Callback to look up the partition manager for a given column family ID.
  BlobPartitionManagerProvider partition_mgr_provider_;
  // Output batch that receives transformed entries (BlobIndex for qualifying
  // values, passthrough for everything else).
  WriteBatch* output_batch_;
  // Callback to look up blob direct write settings for a given CF ID.
  BlobDirectWriteSettingsProvider settings_provider_;
  // Write options from the caller, forwarded to WriteBlob calls.
  const WriteOptions& write_options_;
  // True once at least one value has been separated into a blob file.
  bool has_transformed_ = false;
  // Reusable buffer for encoding BlobIndex entries (avoids per-Put alloc).
  std::string blob_index_buf_;
  // Per-batch cache of the last CF's settings and manager, avoiding
  // redundant provider lookups when consecutive entries share the same CF.
  uint32_t cached_cf_id_ = UINT32_MAX;
  // Cached settings for `cached_cf_id_`.
  BlobDirectWriteSettings cached_settings_;
  // Cached partition manager for `cached_cf_id_`.
  BlobFilePartitionManager* cached_partition_mgr_ = nullptr;
  // Set of partition managers that received data during this batch,
  // returned to the caller so it can flush/sync them.
  UnorderedSet<BlobFilePartitionManager*> used_managers_;
  // Exact blob writes performed during this batch. We only aggregate these
  // entries if rollback is needed so the normal path keeps minimal overhead.
  std::vector<RollbackInfo> rollback_infos_;
};

}  // namespace ROCKSDB_NAMESPACE
