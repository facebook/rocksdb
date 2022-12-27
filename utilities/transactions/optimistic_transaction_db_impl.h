//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <algorithm>
#include <mutex>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"

namespace ROCKSDB_NAMESPACE {

class OptimisticTransactionDBImpl : public OptimisticTransactionDB {
 public:
  explicit OptimisticTransactionDBImpl(
      DB* db, const OptimisticTransactionDBOptions& occ_options,
      bool take_ownership = true)
      : OptimisticTransactionDB(db),
        db_owner_(take_ownership),
        validate_policy_(occ_options.validate_policy) {
    if (validate_policy_ == OccValidationPolicy::kValidateParallel) {
      uint32_t bucket_size = std::max(16u, occ_options.occ_lock_buckets);
      bucketed_locks_.reserve(bucket_size);
      for (size_t i = 0; i < bucket_size; ++i) {
        bucketed_locks_.emplace_back(
            std::unique_ptr<std::mutex>(new std::mutex));
      }
    }
  }

  ~OptimisticTransactionDBImpl() {
    // Prevent this stackable from destroying
    // base db
    if (!db_owner_) {
      db_ = nullptr;
    }
  }

  Transaction* BeginTransaction(const WriteOptions& write_options,
                                const OptimisticTransactionOptions& txn_options,
                                Transaction* old_txn) override;

  // Transactional `DeleteRange()` is not yet supported.
  using StackableDB::DeleteRange;
  virtual Status DeleteRange(const WriteOptions&, ColumnFamilyHandle*,
                             const Slice&, const Slice&) override {
    return Status::NotSupported();
  }

  // Range deletions also must not be snuck into `WriteBatch`es as they are
  // incompatible with `OptimisticTransactionDB`.
  virtual Status Write(const WriteOptions& write_opts,
                       WriteBatch* batch) override {
    if (batch->HasDeleteRange()) {
      return Status::NotSupported();
    }
    return OptimisticTransactionDB::Write(write_opts, batch);
  }

  size_t GetLockBucketsSize() const { return bucketed_locks_.size(); }

  OccValidationPolicy GetValidatePolicy() const { return validate_policy_; }

  std::unique_lock<std::mutex> LockBucket(size_t idx);

 private:
  // NOTE: used in validation phase. Each key is hashed into some
  // bucket. We then take the lock in the hash value order to avoid deadlock.
  std::vector<std::unique_ptr<std::mutex>> bucketed_locks_;

  bool db_owner_;

  const OccValidationPolicy validate_policy_;

  void ReinitializeTransaction(Transaction* txn,
                               const WriteOptions& write_options,
                               const OptimisticTransactionOptions& txn_options =
                                   OptimisticTransactionOptions());
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
