// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#ifndef ROCKSDB_LITE

#include <atomic>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/write_callback.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "utilities/transactions/transaction_base.h"
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

using TransactionID = uint64_t;

class TransactionDBImpl;

class TransactionImpl : public TransactionBaseImpl {
 public:
  TransactionImpl(TransactionDB* db, const WriteOptions& write_options,
                  const TransactionOptions& txn_options);

  virtual ~TransactionImpl();

  void Reinitialize(TransactionDB* txn_db, const WriteOptions& write_options,
                    const TransactionOptions& txn_options);

  Status Prepare() override;

  Status Commit() override;

  Status CommitBatch(WriteBatch* batch);

  Status Rollback() override;

  Status RollbackToSavePoint() override;

  Status SetName(const TransactionName& name) override;

  // Generate a new unique transaction identifier
  static TransactionID GenTxnID();

  TransactionID GetTxnID() const { return txn_id_; }

  // Returns the time (in microseconds according to Env->GetMicros())
  // that this transaction will be expired.  Returns 0 if this transaction does
  // not expire.
  uint64_t GetExpirationTime() const { return expiration_time_; }

  // returns true if this transaction has an expiration_time and has expired.
  bool IsExpired() const;

  // Returns the number of microseconds a transaction can wait on acquiring a
  // lock or -1 if there is no timeout.
  int64_t GetLockTimeout() const { return lock_timeout_; }
  void SetLockTimeout(int64_t timeout) override {
    lock_timeout_ = timeout * 1000;
  }

  // Returns true if locks were stolen successfully, false otherwise.
  bool TryStealingLocks();

 protected:
  Status TryLock(ColumnFamilyHandle* column_family, const Slice& key,
                 bool read_only, bool untracked = false) override;

 private:
  TransactionDBImpl* txn_db_impl_;
  DBImpl* db_impl_;

  // Used to create unique ids for transactions.
  static std::atomic<TransactionID> txn_id_counter_;

  // Unique ID for this transaction
  TransactionID txn_id_;

  // If non-zero, this transaction should not be committed after this time (in
  // microseconds according to Env->NowMicros())
  uint64_t expiration_time_;

  // Timeout in microseconds when locking a key or -1 if there is no timeout.
  int64_t lock_timeout_;

  void Clear() override;

  void Initialize(const TransactionOptions& txn_options);

  Status ValidateSnapshot(ColumnFamilyHandle* column_family, const Slice& key,
                          SequenceNumber prev_seqno, SequenceNumber* new_seqno);

  Status LockBatch(WriteBatch* batch, TransactionKeyMap* keys_to_unlock);

  Status DoCommit(WriteBatch* batch);

  void RollbackLastN(size_t num);

  void UnlockGetForUpdate(ColumnFamilyHandle* column_family,
                          const Slice& key) override;

  // No copying allowed
  TransactionImpl(const TransactionImpl&);
  void operator=(const TransactionImpl&);
};

// Used at commit time to check whether transaction is committing before its
// expiration time.
class TransactionCallback : public WriteCallback {
 public:
  explicit TransactionCallback(TransactionImpl* txn) : txn_(txn) {}

  Status Callback(DB* db) override {
    if (txn_->IsExpired()) {
      return Status::Expired();
    } else {
      return Status::OK();
    }
  }

  bool AllowWriteBatching() override { return true; }

 private:
  TransactionImpl* txn_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
