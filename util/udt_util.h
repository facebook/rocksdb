//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <memory>
#include <optional>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "db/wide/wide_column_serialization.h"
#include "db/write_batch_internal.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "util/coding.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

// Dummy record in WAL logs signaling user-defined timestamp sizes for
// subsequent records.
class UserDefinedTimestampSizeRecord {
 public:
  UserDefinedTimestampSizeRecord() {}
  explicit UserDefinedTimestampSizeRecord(
      std::vector<std::pair<uint32_t, size_t>>&& cf_to_ts_sz)
      : cf_to_ts_sz_(std::move(cf_to_ts_sz)) {}

  const std::vector<std::pair<uint32_t, size_t>>& GetUserDefinedTimestampSize()
      const {
    return cf_to_ts_sz_;
  }

  inline void EncodeTo(std::string* dst) const {
    assert(dst != nullptr);
    for (const auto& [cf_id, ts_sz] : cf_to_ts_sz_) {
      assert(ts_sz != 0);
      PutFixed32(dst, cf_id);
      PutFixed16(dst, static_cast<uint16_t>(ts_sz));
    }
  }

  inline Status DecodeFrom(Slice* src) {
    const size_t total_size = src->size();
    if ((total_size % kSizePerColumnFamily) != 0) {
      std::ostringstream oss;
      oss << "User-defined timestamp size record length: " << total_size
          << " is not a multiple of " << kSizePerColumnFamily << std::endl;
      return Status::Corruption(oss.str());
    }
    int num_of_entries = static_cast<int>(total_size / kSizePerColumnFamily);
    for (int i = 0; i < num_of_entries; i++) {
      uint32_t cf_id = 0;
      uint16_t ts_sz = 0;
      if (!GetFixed32(src, &cf_id) || !GetFixed16(src, &ts_sz)) {
        return Status::Corruption(
            "Error decoding user-defined timestamp size record entry");
      }
      cf_to_ts_sz_.emplace_back(cf_id, static_cast<size_t>(ts_sz));
    }
    return Status::OK();
  }

  inline std::string DebugString() const {
    std::ostringstream oss;

    for (const auto& [cf_id, ts_sz] : cf_to_ts_sz_) {
      oss << "Column family: " << cf_id
          << ", user-defined timestamp size: " << ts_sz << std::endl;
    }
    return oss.str();
  }

 private:
  // 4 bytes for column family id, 2 bytes for user-defined timestamp size.
  static constexpr size_t kSizePerColumnFamily = 4 + 2;

  std::vector<std::pair<uint32_t, size_t>> cf_to_ts_sz_;
};

// This handler is used to recover a WriteBatch read from WAL logs during
// recovery. It does a best-effort recovery if the column families contained in
// the WriteBatch have inconsistency between the recorded timestamp size and the
// running timestamp size. And creates a new WriteBatch that are consistent with
// the running timestamp size with entries from the original WriteBatch.
//
// Note that for a WriteBatch with no inconsistency, a new WriteBatch is created
// nonetheless, and it should be exactly the same as the original WriteBatch.
//
// To access the new WriteBatch, invoke `TransferNewBatch` after calling
// `Iterate`. The handler becomes invalid afterwards.
//
// For the user key in each entry, the best effort recovery means:
// 1) If recorded timestamp size is 0, running timestamp size is > 0, a min
// timestamp of length running timestamp size is padded to the user key.
// 2) If recorded timestamp size is > 0, running timestamp size is 0, the last
// bytes of length recorded timestamp size is stripped from user key.
// 3) If recorded timestamp size is the same as running timestamp size, no-op.
// 4) If recorded timestamp size and running timestamp size are both non-zero
// but not equal, return Status::InvalidArgument.
class TimestampRecoveryHandler : public WriteBatch::Handler {
 public:
  TimestampRecoveryHandler(const UnorderedMap<uint32_t, size_t>& running_ts_sz,
                           const UnorderedMap<uint32_t, size_t>& record_ts_sz,
                           bool seq_per_batch, bool batch_per_txn);

  ~TimestampRecoveryHandler() override {}

  // No copy or move.
  TimestampRecoveryHandler(const TimestampRecoveryHandler&) = delete;
  TimestampRecoveryHandler(TimestampRecoveryHandler&&) = delete;
  TimestampRecoveryHandler& operator=(const TimestampRecoveryHandler&) = delete;
  TimestampRecoveryHandler& operator=(TimestampRecoveryHandler&&) = delete;

  Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override;

  Status PutEntityCF(uint32_t cf, const Slice& key,
                     const Slice& entity) override;

  Status TimedPutCF(uint32_t cf, const Slice& key, const Slice& value,
                    uint64_t write_time) override;

  Status DeleteCF(uint32_t cf, const Slice& key) override;

  Status SingleDeleteCF(uint32_t cf, const Slice& key) override;

  Status DeleteRangeCF(uint32_t cf, const Slice& begin_key,
                       const Slice& end_key) override;

  Status MergeCF(uint32_t cf, const Slice& key, const Slice& value) override;

  Status PutBlobIndexCF(uint32_t cf, const Slice& key,
                        const Slice& value) override;

  Status MarkBeginPrepare(bool unprepare) override;

  Status MarkEndPrepare(const Slice& name) override;

  Status MarkCommit(const Slice& name) override;

  Status MarkCommitWithTimestamp(const Slice& name,
                                 const Slice& commit_ts) override;

  Status MarkRollback(const Slice& name) override;

  Status MarkNoop(bool empty_batch) override;

  std::unique_ptr<WriteBatch>&& TransferNewBatch() {
    assert(new_batch_diff_from_orig_batch_);
    handler_valid_ = false;
    return std::move(new_batch_);
  }

 protected:
  Handler::OptionState WriteBeforePrepare() const override {
    return write_before_prepare_ ? Handler::OptionState::kEnabled
                                 : Handler::OptionState::kDisabled;
  }
  Handler::OptionState WriteAfterCommit() const override {
    return write_after_commit_ ? Handler::OptionState::kEnabled
                               : Handler::OptionState::kDisabled;
  }

 private:
  Status ReconcileTimestampDiscrepancy(uint32_t cf, const Slice& key,
                                       std::string* new_key_buf,
                                       Slice* new_key);

  // Mapping from column family id to user-defined timestamp size for all
  // running column families including the ones with zero timestamp size.
  const UnorderedMap<uint32_t, size_t>& running_ts_sz_;

  // Mapping from column family id to user-defined timestamp size as recorded
  // in the WAL. This only contains non-zero user-defined timestamp size.
  const UnorderedMap<uint32_t, size_t>& record_ts_sz_;

  bool write_after_commit_;
  bool write_before_prepare_;

  std::unique_ptr<WriteBatch> new_batch_;
  // Handler is valid upon creation and becomes invalid after its `new_batch_`
  // is transferred.
  bool handler_valid_;

  // False upon creation, and become true if at least one user key from the
  // original batch is updated when creating the new batch.
  bool new_batch_diff_from_orig_batch_;
};

// Mode for checking and handling timestamp size inconsistency encountered in a
// WriteBatch read from WAL log.
enum class TimestampSizeConsistencyMode {
  // Verified that the recorded user-defined timestamp size is consistent with
  // the running one for all the column families involved in a WriteBatch.
  // Column families referred to in the WriteBatch but are dropped are ignored.
  kVerifyConsistency,
  // Verified that if any inconsistency exists in a WriteBatch, it's all
  // tolerable by a best-effort reconciliation. And optionally creates a new
  // WriteBatch from the original WriteBatch that is consistent with the running
  // timestamp size. Column families referred to in the WriteBatch but are
  // dropped are ignored. If a new WriteBatch is created, such entries are
  // copied over as is.
  kReconcileInconsistency,
};

// Handles the inconsistency between recorded timestamp sizes and running
// timestamp sizes for a WriteBatch. A non-OK `status` indicates there are
// intolerable inconsistency with the specified `check_mode`.
//
// If `check_mode` is `kVerifyConsistency`, intolerable inconsistency means any
// running column family has an inconsistent user-defined timestamp size.
//
// If `check_mode` is `kReconcileInconsistency`, intolerable inconsistency means
// any running column family has an inconsistent user-defined timestamp size
// that cannot be reconciled with a best-effort recovery. Check
// `TimestampRecoveryHandler` for what a best-effort recovery is capable of. In
// this mode, output argument `new_batch` should be set, a new WriteBatch is
// created on the heap and transferred to `new_batch` if there is tolerable
// inconsistency.
//
// An invariant that WAL logging ensures is that all timestamp size info
// is logged prior to a WriteBatch that needed this info. And zero timestamp
// size is skipped. So `record_ts_sz` only contains column family with non-zero
// timestamp size and a column family id absent from `record_ts_sz` will be
// interpreted as that column family has zero timestamp size. On the other hand,
// `running_ts_sz` should contain the timestamp size for all running column
// families including the ones with zero timestamp size.
Status HandleWriteBatchTimestampSizeDifference(
    const WriteBatch* batch,
    const UnorderedMap<uint32_t, size_t>& running_ts_sz,
    const UnorderedMap<uint32_t, size_t>& record_ts_sz,
    TimestampSizeConsistencyMode check_mode, bool seq_per_batch,
    bool batch_per_txn, std::unique_ptr<WriteBatch>* new_batch = nullptr);

// This util function is used when opening an existing column family and
// processing its VersionEdit. It does a sanity check for the column family's
// old user comparator and the persist_user_defined_timestamps flag as recorded
// in the VersionEdit, against its new settings from the column family's
// ImmutableCFOptions.
//
// Valid settings change include:
// 1) no user comparator change and no effective persist_user_defined_timestamp
// flag change.
// 2) switch user comparator to enable user-defined timestamps feature provided
// the immediately effective persist_user_defined_timestamps flag is false.
// 3) switch user comparator to disable user-defined timestamps feature provided
// that the before-change persist_user_defined_timestamps is already false.
//
// Switch user comparator to disable/enable UDT is only sanity checked by a user
// comparator name comparison. The full check includes enforcing the new user
// comparator ranks user keys exactly the same as the old user comparator and
// only add / remove the user-defined timestamp comparison. We don't have ways
// to strictly enforce this so currently only the RocksDB builtin comparator
// wrapper `ComparatorWithU64TsImpl` is  supported to enable / disable
// user-defined timestamps. It formats user-defined timestamps as uint64_t.
//
// When the settings indicate a legit change to enable user-defined timestamps
// feature on a column family, `mark_sst_files_has_no_udt` will be set to true
// to indicate marking all existing SST files has no user-defined timestamps
// when re-writing the manifest.
Status ValidateUserDefinedTimestampsOptions(
    const Comparator* new_comparator, const std::string& old_comparator_name,
    bool new_persist_udt, bool old_persist_udt,
    bool* mark_sst_files_has_no_udt);

// Given a cutoff user-defined timestamp formatted as uint64_t, get the
// effective `full_history_ts_low` timestamp, which is the next immediately
// bigger timestamp. Used by the UDT in memtable only feature when flushing
// memtables and remove timestamps. This process collapses history and increase
// the effective `full_history_ts_low`.
void GetFullHistoryTsLowFromU64CutoffTs(Slice* cutoff_ts,
                                        std::string* full_history_ts_low);

// `start` is the inclusive lower user key bound without user-defined timestamp.
// `end` is the upper user key bound without user-defined timestamp.
// By default, `end` is treated as being exclusive. If `exclusive_end` is set to
// false, it's treated as an inclusive upper bound.
// If any of these two bounds is nullptr, an empty std::optional<Slice> is
// returned for that bound.
std::tuple<std::optional<Slice>, std::optional<Slice>>
MaybeAddTimestampsToRange(const Slice* start, const Slice* end, size_t ts_sz,
                          std::string* start_with_ts, std::string* end_with_ts,
                          bool exclusive_end = true);
}  // namespace ROCKSDB_NAMESPACE
