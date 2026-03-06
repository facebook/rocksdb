//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_write_batch_transformer.h"

#include "db/blob/blob_file_partition_manager.h"
#include "rocksdb/system_clock.h"
#include <atomic>
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

  static std::atomic<uint64_t> tb_clear_ns{0}, tb_iterate_ns{0};
  static std::atomic<uint64_t> tb_setup_ns{0}, tb_count{0};

  auto* clock = SystemClock::Default().get();
  uint64_t t0 = clock->NowNanos();

  output_batch->Clear();
  *transformed = false;

  BlobWriteBatchTransformer transformer(partition_manager, output_batch,
                                        settings_provider);

  uint64_t t1 = clock->NowNanos();

  Status s = input_batch->Iterate(&transformer);
  if (!s.ok()) {
    return s;
  }

  uint64_t t2 = clock->NowNanos();

  *transformed = transformer.HasTransformed();

  tb_setup_ns.fetch_add(t1 - t0, std::memory_order_relaxed);
  tb_iterate_ns.fetch_add(t2 - t1, std::memory_order_relaxed);
  uint64_t cnt = tb_count.fetch_add(1, std::memory_order_relaxed) + 1;
  if (cnt % 20000 == 0) {
    fprintf(stderr,
            "TRANSFORM_BATCH: %lu calls | setup+clear=%lu ns | "
            "iterate=%lu ns | total=%lu ns\n",
            (unsigned long)cnt,
            (unsigned long)(tb_setup_ns.load() / cnt),
            (unsigned long)(tb_iterate_ns.load() / cnt),
            (unsigned long)((tb_setup_ns.load() + tb_iterate_ns.load()) / cnt));
  }

  return Status::OK();
}

Status BlobWriteBatchTransformer::PutCF(uint32_t column_family_id,
                                        const Slice& key,
                                        const Slice& value) {
  static std::atomic<uint64_t> pc_settings_ns{0}, pc_writeblob_ns{0};
  static std::atomic<uint64_t> pc_encode_ns{0}, pc_putidx_ns{0}, pc_count{0};
  auto* clock = SystemClock::Default().get();
  uint64_t t0 = clock->NowNanos();

  const BlobDirectWriteSettings settings = settings_provider_(column_family_id);

  uint64_t t1 = clock->NowNanos();

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
      &blob_file_number, &blob_offset, &blob_size);
  if (!s.ok()) {
    return s;
  }

  uint64_t t2 = clock->NowNanos();

  BlobIndex::EncodeBlob(&blob_index_buf_, blob_file_number, blob_offset,
                        blob_size, settings.compression_type);

  uint64_t t3 = clock->NowNanos();

  has_transformed_ = true;
  Status rs = WriteBatchInternal::PutBlobIndex(output_batch_, column_family_id,
                                               key, blob_index_buf_);

  uint64_t t4 = clock->NowNanos();

  pc_settings_ns.fetch_add(t1 - t0, std::memory_order_relaxed);
  pc_writeblob_ns.fetch_add(t2 - t1, std::memory_order_relaxed);
  pc_encode_ns.fetch_add(t3 - t2, std::memory_order_relaxed);
  pc_putidx_ns.fetch_add(t4 - t3, std::memory_order_relaxed);
  uint64_t cnt = pc_count.fetch_add(1, std::memory_order_relaxed) + 1;
  if (cnt % 20000 == 0) {
    fprintf(stderr,
            "PUTCF: %lu calls | settings=%lu ns | writeblob=%lu ns | "
            "encode=%lu ns | putblobidx=%lu ns | total=%lu ns\n",
            (unsigned long)cnt,
            (unsigned long)(pc_settings_ns.load() / cnt),
            (unsigned long)(pc_writeblob_ns.load() / cnt),
            (unsigned long)(pc_encode_ns.load() / cnt),
            (unsigned long)(pc_putidx_ns.load() / cnt),
            (unsigned long)((pc_settings_ns.load() + pc_writeblob_ns.load() +
                             pc_encode_ns.load() + pc_putidx_ns.load()) / cnt));
  }

  return rs;
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
