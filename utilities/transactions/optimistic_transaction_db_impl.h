//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <mutex>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"

namespace rocksdb {

class OptimisticTransactionDBImpl : public OptimisticTransactionDB {
 public:
  explicit OptimisticTransactionDBImpl(DB* db, bool take_ownership = true, size_t lock_bucket_size = 1024*1024)
      : OptimisticTransactionDB(db), db_owner_(take_ownership) {
    bucketed_locks_.reserve(lock_bucket_size);
    for (size_t i = 0; i < lock_bucket_size; ++i) {
      bucketed_locks_.emplace_back(
	std::unique_ptr<std::mutex>(new std::mutex));
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

  size_t GetLockBucketsSize() const {
    return bucketed_locks_.size();
  }

  std::unique_lock<std::mutex> LockBucket(size_t idx);

 private:

  // NOTE(deyukong): used in validation phase. Each key is hashed into some
  // bucket. We then take the lock in the hash value order to avoid deadlock.
  // TODO(deyukong): padding to avoid false sharing.
  std::vector<std::unique_ptr<std::mutex>> bucketed_locks_;

  bool db_owner_;

  void ReinitializeTransaction(Transaction* txn,
                               const WriteOptions& write_options,
                               const OptimisticTransactionOptions& txn_options =
                                   OptimisticTransactionOptions());
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
