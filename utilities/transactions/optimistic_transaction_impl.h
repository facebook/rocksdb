// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#ifndef ROCKSDB_LITE

#include <string>
#include <unordered_map>
#include <vector>

#include "db/write_callback.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/optimistic_transaction.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace rocksdb {

using TransactionKeyMap =
    std::unordered_map<uint32_t,
                       std::unordered_map<std::string, SequenceNumber>>;

class OptimisticTransactionImpl : public OptimisticTransaction {
 public:
  OptimisticTransactionImpl(OptimisticTransactionDB* db,
                            const WriteOptions& write_options,
                            const OptimisticTransactionOptions& txn_options);

  virtual ~OptimisticTransactionImpl();

  Status Commit() override;

  void Rollback() override;

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

  void Put(ColumnFamilyHandle* column_family, const Slice& key,
           const Slice& value) override;
  void Put(const Slice& key, const Slice& value) override {
    Put(nullptr, key, value);
  }

  void Put(ColumnFamilyHandle* column_family, const SliceParts& key,
           const SliceParts& value) override;
  void Put(const SliceParts& key, const SliceParts& value) override {
    Put(nullptr, key, value);
  }

  void Merge(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value) override;
  void Merge(const Slice& key, const Slice& value) override {
    Merge(nullptr, key, value);
  }

  void Delete(ColumnFamilyHandle* column_family, const Slice& key) override;
  void Delete(const Slice& key) override { Delete(nullptr, key); }
  void Delete(ColumnFamilyHandle* column_family,
              const SliceParts& key) override;
  void Delete(const SliceParts& key) override { Delete(nullptr, key); }

  void PutUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                    const Slice& value) override;
  void PutUntracked(const Slice& key, const Slice& value) override {
    PutUntracked(nullptr, key, value);
  }

  void PutUntracked(ColumnFamilyHandle* column_family, const SliceParts& key,
                    const SliceParts& value) override;
  void PutUntracked(const SliceParts& key, const SliceParts& value) override {
    PutUntracked(nullptr, key, value);
  }

  void MergeUntracked(ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& value) override;
  void MergeUntracked(const Slice& key, const Slice& value) override {
    MergeUntracked(nullptr, key, value);
  }

  void DeleteUntracked(ColumnFamilyHandle* column_family,
                       const Slice& key) override;
  void DeleteUntracked(const Slice& key) override {
    DeleteUntracked(nullptr, key);
  }
  void DeleteUntracked(ColumnFamilyHandle* column_family,
                       const SliceParts& key) override;
  void DeleteUntracked(const SliceParts& key) override {
    DeleteUntracked(nullptr, key);
  }

  void PutLogData(const Slice& blob) override;

  const TransactionKeyMap* GetTrackedKeys() const { return &tracked_keys_; }

  const Snapshot* GetSnapshot() const override { return snapshot_; }

  void SetSnapshot() override;

  WriteBatchWithIndex* GetWriteBatch() override;

 protected:
  OptimisticTransactionDB* const txn_db_;
  DB* db_;
  const WriteOptions write_options_;
  const Snapshot* snapshot_;
  SequenceNumber start_sequence_number_;
  WriteBatchWithIndex write_batch_;

 private:
  // Map of Column Family IDs to keys and their sequence numbers
  TransactionKeyMap tracked_keys_;

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
