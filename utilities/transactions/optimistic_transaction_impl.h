// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#ifndef ROCKSDB_LITE

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
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

class OptimisticTransactionImpl : public Transaction {
 public:
  OptimisticTransactionImpl(OptimisticTransactionDB* db,
                            const WriteOptions& write_options,
                            const OptimisticTransactionOptions& txn_options);

  virtual ~OptimisticTransactionImpl();

  Status Commit() override;

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

  const TransactionKeyMap* GetTrackedKeys() const { return &tracked_keys_; }

  const Snapshot* GetSnapshot() const override {
        return snapshot_ ? snapshot_->snapshot() : nullptr;
  }

  void SetSnapshot() override;

  WriteBatchWithIndex* GetWriteBatch() override;

 protected:
  OptimisticTransactionDB* const txn_db_;
  DB* db_;
  const WriteOptions write_options_;
  std::shared_ptr<ManagedSnapshot> snapshot_;
  const Comparator* cmp_;
  std::unique_ptr<WriteBatchWithIndex> write_batch_;

 private:
  // Map of Column Family IDs to keys and corresponding sequence numbers.
  // The sequence number stored for a key will be used during commit to make
  // sure this key has
  // not changed since this sequence number.
  TransactionKeyMap tracked_keys_;

  // Stack of the Snapshot saved at each save point.  Saved snapshots may be
  // nullptr if there was no snapshot at the time SetSavePoint() was called.
  std::unique_ptr<std::stack<std::shared_ptr<ManagedSnapshot>>> save_points_;

  friend class OptimisticTransactionCallback;

  // Returns OK if it is safe to commit this transaction.  Returns Status::Busy
  // if there are read or write conflicts that would prevent us from committing
  // OR if we can not determine whether there would be any such conflicts.
  //
  // Should only be called on writer thread.
  Status CheckTransactionForConflicts(DB* db);

  void RecordOperation(ColumnFamilyHandle* column_family, const Slice& key);
  void RecordOperation(ColumnFamilyHandle* column_family,
                       const SliceParts& key);

  void Cleanup();

  // No copying allowed
  OptimisticTransactionImpl(const OptimisticTransactionImpl&);
  void operator=(const OptimisticTransactionImpl&);
};

// Used at commit time to trigger transaction validation
class OptimisticTransactionCallback : public WriteCallback {
 public:
  explicit OptimisticTransactionCallback(OptimisticTransactionImpl* txn)
      : txn_(txn) {}

  Status Callback(DB* db) override {
    return txn_->CheckTransactionForConflicts(db);
  }

 private:
  OptimisticTransactionImpl* txn_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
