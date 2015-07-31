// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
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
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

using TransactionID = uint64_t;

class TransactionDBImpl;

class TransactionImpl : public Transaction {
 public:
  TransactionImpl(TransactionDB* db, const WriteOptions& write_options,
                  const TransactionOptions& txn_options);

  virtual ~TransactionImpl();

  Status Commit() override;

  Status CommitBatch(WriteBatch* batch);

  void Rollback() override;

  void SetSavePoint() override;

  Status RollbackToSavePoint() override;

  Status Get(const ReadOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, std::string* value) override;

  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override {
    return Get(options, db_->DefaultColumnFamily(), key, value);
  }

  Status GetForUpdate(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      std::string* value) override;

  Status GetForUpdate(const ReadOptions& options, const Slice& key,
                      std::string* value) override {
    return GetForUpdate(options, db_->DefaultColumnFamily(), key, value);
  }

  std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  std::vector<Status> MultiGet(const ReadOptions& options,
                               const std::vector<Slice>& keys,
                               std::vector<std::string>* values) override {
    return MultiGet(options, std::vector<ColumnFamilyHandle*>(
                                 keys.size(), db_->DefaultColumnFamily()),
                    keys, values);
  }

  std::vector<Status> MultiGetForUpdate(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  std::vector<Status> MultiGetForUpdate(
      const ReadOptions& options, const std::vector<Slice>& keys,
      std::vector<std::string>* values) override {
    return MultiGetForUpdate(options,
                             std::vector<ColumnFamilyHandle*>(
                                 keys.size(), db_->DefaultColumnFamily()),
                             keys, values);
  }

  Iterator* GetIterator(const ReadOptions& read_options) override;
  Iterator* GetIterator(const ReadOptions& read_options,
                        ColumnFamilyHandle* column_family) override;

  Status Put(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value) override;
  Status Put(const Slice& key, const Slice& value) override {
    return Put(nullptr, key, value);
  }

  Status Put(ColumnFamilyHandle* column_family, const SliceParts& key,
             const SliceParts& value) override;
  Status Put(const SliceParts& key, const SliceParts& value) override {
    return Put(nullptr, key, value);
  }

  Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
               const Slice& value) override;
  Status Merge(const Slice& key, const Slice& value) override {
    return Merge(nullptr, key, value);
  }

  Status Delete(ColumnFamilyHandle* column_family, const Slice& key) override;
  Status Delete(const Slice& key) override { return Delete(nullptr, key); }
  Status Delete(ColumnFamilyHandle* column_family,
                const SliceParts& key) override;
  Status Delete(const SliceParts& key) override { return Delete(nullptr, key); }

  Status PutUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& value) override;
  Status PutUntracked(const Slice& key, const Slice& value) override {
    return PutUntracked(nullptr, key, value);
  }

  Status PutUntracked(ColumnFamilyHandle* column_family, const SliceParts& key,
                      const SliceParts& value) override;
  Status PutUntracked(const SliceParts& key, const SliceParts& value) override {
    return PutUntracked(nullptr, key, value);
  }

  Status MergeUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                        const Slice& value) override;
  Status MergeUntracked(const Slice& key, const Slice& value) override {
    return MergeUntracked(nullptr, key, value);
  }

  Status DeleteUntracked(ColumnFamilyHandle* column_family,
                         const Slice& key) override;
  Status DeleteUntracked(const Slice& key) override {
    return DeleteUntracked(nullptr, key);
  }
  Status DeleteUntracked(ColumnFamilyHandle* column_family,
                         const SliceParts& key) override;
  Status DeleteUntracked(const SliceParts& key) override {
    return DeleteUntracked(nullptr, key);
  }

  void PutLogData(const Slice& blob) override;

  const Snapshot* GetSnapshot() const override {
    return snapshot_ ? snapshot_->snapshot() : nullptr;
  }

  void SetSnapshot() override;

  WriteBatchWithIndex* GetWriteBatch() override;

  // Generate a new unique transaction identifier
  static TransactionID GenTxnID();

  TransactionID GetTxnID() const { return txn_id_; }

  // Returns the time (in milliseconds according to Env->GetMicros()*1000)
  // that this transaction will be expired.  Returns 0 if this transaction does
  // not expire.
  uint64_t GetExpirationTime() const { return expiration_time_; }

  // returns true if this transaction has an expiration_time and has expired.
  bool IsExpired() const;

  // Returns the number of milliseconds a transaction can wait on acquiring a
  // lock or -1 if there is no timeout.
  int64_t GetLockTimeout() const { return lock_timeout_; }
  void SetLockTimeout(int64_t timeout) { lock_timeout_ = timeout; }

 private:
  TransactionDB* const db_;

  TransactionDBImpl* txn_db_impl_;

  // Used to create unique ids for transactions.
  static std::atomic<TransactionID> txn_id_counter_;

  // Unique ID for this transaction
  const TransactionID txn_id_;

  const WriteOptions write_options_;

  // If snapshot_ is set, all keys that locked must also have not been written
  // since this snapshot
  std::shared_ptr<ManagedSnapshot> snapshot_;

  const Comparator* cmp_;

  std::unique_ptr<WriteBatchWithIndex> write_batch_;

  // If expiration_ is non-zero, start_time_ stores that time the txn was
  // constructed,
  // in milliseconds.
  const uint64_t start_time_;

  // If non-zero, this transaction should not be committed after this time (in
  // milliseconds)
  const uint64_t expiration_time_;

  // Timeout in microseconds when locking a key or -1 if there is no timeout.
  int64_t lock_timeout_;

  // Map from column_family_id to map of keys to Sequence Numbers.  Stores keys
  // that have been locked.
  // The key is known to not have been modified after the Sequence Number
  // stored.
  TransactionKeyMap tracked_keys_;

  // Stack of the Snapshot saved at each save point.  Saved snapshots may be
  // nullptr if there was no snapshot at the time SetSavePoint() was called.
  std::unique_ptr<std::stack<std::shared_ptr<ManagedSnapshot>>> save_points_;

  Status TryLock(ColumnFamilyHandle* column_family, const Slice& key,
                 bool check_snapshot = true);
  Status TryLock(ColumnFamilyHandle* column_family, const SliceParts& key,
                 bool check_snapshot = true);
  void Cleanup();

  Status CheckKeySequence(ColumnFamilyHandle* column_family, const Slice& key);

  Status LockBatch(WriteBatch* batch, TransactionKeyMap* keys_to_unlock);

  Status DoCommit(WriteBatch* batch);

  void RollbackLastN(size_t num);

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

 private:
  TransactionImpl* txn_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
