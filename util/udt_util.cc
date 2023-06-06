//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/udt_util.h"

#include "db/dbformat.h"
#include "rocksdb/types.h"
#include "util/write_batch_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
enum class RecoveryType {
  kNoop,
  kUnrecoverable,
  kStripTimestamp,
  kPadTimestamp,
};

RecoveryType GetRecoveryType(const size_t running_ts_sz,
                             const std::optional<size_t>& recorded_ts_sz) {
  if (running_ts_sz == 0) {
    if (!recorded_ts_sz.has_value()) {
      // A column family id not recorded is equivalent to that column family has
      // zero timestamp size.
      return RecoveryType::kNoop;
    }
    return RecoveryType::kStripTimestamp;
  }

  assert(running_ts_sz != 0);

  if (!recorded_ts_sz.has_value()) {
    return RecoveryType::kPadTimestamp;
  }

  if (running_ts_sz != recorded_ts_sz.value()) {
    return RecoveryType::kUnrecoverable;
  }

  return RecoveryType::kNoop;
}

bool AllRunningColumnFamiliesConsistent(
    const UnorderedMap<uint32_t, size_t>& running_ts_sz,
    const UnorderedMap<uint32_t, size_t>& record_ts_sz) {
  for (const auto& [cf_id, ts_sz] : running_ts_sz) {
    auto record_it = record_ts_sz.find(cf_id);
    RecoveryType recovery_type =
        GetRecoveryType(ts_sz, record_it != record_ts_sz.end()
                                   ? std::optional<size_t>(record_it->second)
                                   : std::nullopt);
    if (recovery_type != RecoveryType::kNoop) {
      return false;
    }
  }
  return true;
}

Status CheckWriteBatchTimestampSizeConsistency(
    const WriteBatch* batch,
    const UnorderedMap<uint32_t, size_t>& running_ts_sz,
    const UnorderedMap<uint32_t, size_t>& record_ts_sz,
    TimestampSizeConsistencyMode check_mode, bool* ts_need_recovery) {
  std::vector<uint32_t> column_family_ids;
  Status status =
      CollectColumnFamilyIdsFromWriteBatch(*batch, &column_family_ids);
  if (!status.ok()) {
    return status;
  }
  for (const auto& cf_id : column_family_ids) {
    auto running_iter = running_ts_sz.find(cf_id);
    if (running_iter == running_ts_sz.end()) {
      // Ignore dropped column family referred to in a WriteBatch regardless of
      // its consistency.
      continue;
    }
    auto record_iter = record_ts_sz.find(cf_id);
    RecoveryType recovery_type = GetRecoveryType(
        running_iter->second, record_iter != record_ts_sz.end()
                                  ? std::optional<size_t>(record_iter->second)
                                  : std::nullopt);
    if (recovery_type != RecoveryType::kNoop) {
      if (check_mode == TimestampSizeConsistencyMode::kVerifyConsistency) {
        return Status::InvalidArgument(
            "WriteBatch contains timestamp size inconsistency.");
      }

      if (recovery_type == RecoveryType::kUnrecoverable) {
        return Status::InvalidArgument(
            "WriteBatch contains unrecoverable timestamp size inconsistency.");
      }

      // If any column family needs reconciliation, it will mark the whole
      // WriteBatch to need recovery and rebuilt.
      *ts_need_recovery = true;
    }
  }
  return Status::OK();
}
}  // namespace

TimestampRecoveryHandler::TimestampRecoveryHandler(
    const UnorderedMap<uint32_t, size_t>& running_ts_sz,
    const UnorderedMap<uint32_t, size_t>& record_ts_sz)
    : running_ts_sz_(running_ts_sz),
      record_ts_sz_(record_ts_sz),
      new_batch_(new WriteBatch()),
      handler_valid_(true),
      new_batch_diff_from_orig_batch_(false) {}

Status TimestampRecoveryHandler::PutCF(uint32_t cf, const Slice& key,
                                       const Slice& value) {
  std::string new_key_buf;
  Slice new_key;
  Status status =
      ReconcileTimestampDiscrepancy(cf, key, &new_key_buf, &new_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::Put(new_batch_.get(), cf, new_key, value);
}

Status TimestampRecoveryHandler::DeleteCF(uint32_t cf, const Slice& key) {
  std::string new_key_buf;
  Slice new_key;
  Status status =
      ReconcileTimestampDiscrepancy(cf, key, &new_key_buf, &new_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::Delete(new_batch_.get(), cf, new_key);
}

Status TimestampRecoveryHandler::SingleDeleteCF(uint32_t cf, const Slice& key) {
  std::string new_key_buf;
  Slice new_key;
  Status status =
      ReconcileTimestampDiscrepancy(cf, key, &new_key_buf, &new_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::SingleDelete(new_batch_.get(), cf, new_key);
}

Status TimestampRecoveryHandler::DeleteRangeCF(uint32_t cf,
                                               const Slice& begin_key,
                                               const Slice& end_key) {
  std::string new_begin_key_buf;
  Slice new_begin_key;
  std::string new_end_key_buf;
  Slice new_end_key;
  Status status = ReconcileTimestampDiscrepancy(
      cf, begin_key, &new_begin_key_buf, &new_begin_key);
  if (!status.ok()) {
    return status;
  }
  status = ReconcileTimestampDiscrepancy(cf, end_key, &new_end_key_buf,
                                         &new_end_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::DeleteRange(new_batch_.get(), cf, new_begin_key,
                                         new_end_key);
}

Status TimestampRecoveryHandler::MergeCF(uint32_t cf, const Slice& key,
                                         const Slice& value) {
  std::string new_key_buf;
  Slice new_key;
  Status status =
      ReconcileTimestampDiscrepancy(cf, key, &new_key_buf, &new_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::Merge(new_batch_.get(), cf, new_key, value);
}

Status TimestampRecoveryHandler::PutBlobIndexCF(uint32_t cf, const Slice& key,
                                                const Slice& value) {
  std::string new_key_buf;
  Slice new_key;
  Status status =
      ReconcileTimestampDiscrepancy(cf, key, &new_key_buf, &new_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::PutBlobIndex(new_batch_.get(), cf, new_key, value);
}

Status TimestampRecoveryHandler::ReconcileTimestampDiscrepancy(
    uint32_t cf, const Slice& key, std::string* new_key_buf, Slice* new_key) {
  assert(handler_valid_);
  auto running_iter = running_ts_sz_.find(cf);
  if (running_iter == running_ts_sz_.end()) {
    // The column family referred to by the WriteBatch is no longer running.
    // Copy over the entry as is to the new WriteBatch.
    *new_key = key;
    return Status::OK();
  }
  size_t running_ts_sz = running_iter->second;
  auto record_iter = record_ts_sz_.find(cf);
  std::optional<size_t> record_ts_sz =
      record_iter != record_ts_sz_.end()
          ? std::optional<size_t>(record_iter->second)
          : std::nullopt;
  RecoveryType recovery_type = GetRecoveryType(running_ts_sz, record_ts_sz);

  switch (recovery_type) {
    case RecoveryType::kNoop:
      *new_key = key;
      break;
    case RecoveryType::kStripTimestamp:
      assert(record_ts_sz.has_value());
      *new_key = StripTimestampFromUserKey(key, record_ts_sz.value());
      new_batch_diff_from_orig_batch_ = true;
      break;
    case RecoveryType::kPadTimestamp:
      AppendKeyWithMinTimestamp(new_key_buf, key, running_ts_sz);
      *new_key = *new_key_buf;
      new_batch_diff_from_orig_batch_ = true;
      break;
    case RecoveryType::kUnrecoverable:
      return Status::InvalidArgument(
          "Unrecoverable timestamp size inconsistency encountered by "
          "TimestampRecoveryHandler.");
    default:
      assert(false);
  }
  return Status::OK();
}

Status HandleWriteBatchTimestampSizeDifference(
    const WriteBatch* batch,
    const UnorderedMap<uint32_t, size_t>& running_ts_sz,
    const UnorderedMap<uint32_t, size_t>& record_ts_sz,
    TimestampSizeConsistencyMode check_mode,
    std::unique_ptr<WriteBatch>* new_batch) {
  // Quick path to bypass checking the WriteBatch.
  if (AllRunningColumnFamiliesConsistent(running_ts_sz, record_ts_sz)) {
    return Status::OK();
  }
  bool need_recovery = false;
  Status status = CheckWriteBatchTimestampSizeConsistency(
      batch, running_ts_sz, record_ts_sz, check_mode, &need_recovery);
  if (!status.ok()) {
    return status;
  } else if (need_recovery) {
    assert(new_batch);
    SequenceNumber sequence = WriteBatchInternal::Sequence(batch);
    TimestampRecoveryHandler recovery_handler(running_ts_sz, record_ts_sz);
    status = batch->Iterate(&recovery_handler);
    if (!status.ok()) {
      return status;
    } else {
      *new_batch = recovery_handler.TransferNewBatch();
      WriteBatchInternal::SetSequence(new_batch->get(), sequence);
    }
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
