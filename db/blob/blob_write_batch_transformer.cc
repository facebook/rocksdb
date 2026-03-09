//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_write_batch_transformer.h"

#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_index.h"
#include "db/write_batch_internal.h"

namespace ROCKSDB_NAMESPACE {

BlobWriteBatchTransformer::BlobWriteBatchTransformer(
    BlobFilePartitionManager* partition_manager,
    WriteBatch* output_batch,
    const BlobDirectWriteSettingsProvider& settings_provider)
    : partition_manager_(partition_manager),
      output_batch_(output_batch),
      settings_provider_(settings_provider) {
  assert(partition_manager_);
  assert(output_batch_);
  assert(settings_provider_);
}

Status BlobWriteBatchTransformer::TransformBatch(
    const WriteOptions& /*write_options*/,
    WriteBatch* input_batch,
    WriteBatch* output_batch,
    BlobFilePartitionManager* partition_manager,
    const BlobDirectWriteSettingsProvider& settings_provider,
    bool* transformed) {
  assert(input_batch);
  assert(output_batch);
  assert(transformed);

  output_batch->Clear();
  *transformed = false;

  BlobWriteBatchTransformer transformer(partition_manager, output_batch,
                                        settings_provider);

  Status s = input_batch->Iterate(&transformer);
  if (!s.ok()) {
    return s;
  }

  *transformed = transformer.HasTransformed();

  return Status::OK();
}

Status BlobWriteBatchTransformer::PutCF(uint32_t column_family_id,
                                        const Slice& key,
                                        const Slice& value) {
  // Use cached settings for the same CF to avoid per-entry lookup overhead.
  if (column_family_id != cached_cf_id_) {
    cached_settings_ = settings_provider_(column_family_id);
    cached_cf_id_ = column_family_id;
  }
  const auto& settings = cached_settings_;

  if (!settings.enable_blob_direct_write ||
      value.size() < settings.min_blob_size) {
    return WriteBatchInternal::Put(output_batch_, column_family_id, key, value);
  }

  uint64_t blob_file_number = 0;
  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteOptions wo;
  Status s = partition_manager_->WriteBlob(
      wo, column_family_id, settings.compression_type, key, value,
      &blob_file_number, &blob_offset, &blob_size, &settings);
  if (!s.ok()) {
    return s;
  }

  BlobIndex::EncodeBlob(&blob_index_buf_, blob_file_number, blob_offset,
                        blob_size, settings.compression_type);

  has_transformed_ = true;
  return WriteBatchInternal::PutBlobIndex(output_batch_, column_family_id,
                                               key, blob_index_buf_);
}

Status BlobWriteBatchTransformer::TimedPutCF(uint32_t column_family_id,
                                             const Slice& key,
                                             const Slice& value,
                                             uint64_t write_time) {
  // TimedPut: pass through without blob separation for now.
  return WriteBatchInternal::TimedPut(output_batch_, column_family_id, key,
                                     value, write_time);
}

Status BlobWriteBatchTransformer::PutEntityCF(uint32_t column_family_id,
                                              const Slice& key,
                                              const Slice& entity) {
  // Wide column entities: pass through unchanged.
  return WriteBatchInternal::PutEntityRaw(output_batch_, column_family_id, key,
                                          entity);
}

Status BlobWriteBatchTransformer::DeleteCF(uint32_t column_family_id,
                                           const Slice& key) {
  return WriteBatchInternal::Delete(output_batch_, column_family_id, key);
}

Status BlobWriteBatchTransformer::SingleDeleteCF(uint32_t column_family_id,
                                                  const Slice& key) {
  return WriteBatchInternal::SingleDelete(output_batch_, column_family_id, key);
}

Status BlobWriteBatchTransformer::DeleteRangeCF(uint32_t column_family_id,
                                                const Slice& begin_key,
                                                const Slice& end_key) {
  return WriteBatchInternal::DeleteRange(output_batch_, column_family_id,
                                         begin_key, end_key);
}

Status BlobWriteBatchTransformer::MergeCF(uint32_t column_family_id,
                                          const Slice& key,
                                          const Slice& value) {
  return WriteBatchInternal::Merge(output_batch_, column_family_id, key, value);
}

Status BlobWriteBatchTransformer::PutBlobIndexCF(uint32_t column_family_id,
                                                 const Slice& key,
                                                 const Slice& value) {
  // Already a blob index — pass through unchanged.
  return WriteBatchInternal::PutBlobIndex(output_batch_, column_family_id, key,
                                         value);
}

void BlobWriteBatchTransformer::LogData(const Slice& blob) {
  output_batch_->PutLogData(blob);
}

Status BlobWriteBatchTransformer::MarkBeginPrepare(bool unprepared) {
  return WriteBatchInternal::InsertBeginPrepare(
      output_batch_, !unprepared /* write_after_commit */, unprepared);
}

Status BlobWriteBatchTransformer::MarkEndPrepare(const Slice& xid) {
  return WriteBatchInternal::InsertEndPrepare(output_batch_, xid);
}

Status BlobWriteBatchTransformer::MarkCommit(const Slice& xid) {
  return WriteBatchInternal::MarkCommit(output_batch_, xid);
}

Status BlobWriteBatchTransformer::MarkCommitWithTimestamp(const Slice& xid,
                                                         const Slice& ts) {
  return WriteBatchInternal::MarkCommitWithTimestamp(output_batch_, xid, ts);
}

Status BlobWriteBatchTransformer::MarkRollback(const Slice& xid) {
  return WriteBatchInternal::MarkRollback(output_batch_, xid);
}

Status BlobWriteBatchTransformer::MarkNoop(bool /*empty_batch*/) {
  return WriteBatchInternal::InsertNoop(output_batch_);
}

}  // namespace ROCKSDB_NAMESPACE
