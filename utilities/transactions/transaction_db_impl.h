//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/transactions/transaction_impl.h"
#include "utilities/transactions/transaction_lock_mgr.h"

namespace rocksdb {

class TransactionDBImpl : public TransactionDB {
 public:
  explicit TransactionDBImpl(DB* db,
                             const TransactionDBOptions& txn_db_options);

  explicit TransactionDBImpl(StackableDB* db,
                             const TransactionDBOptions& txn_db_options);

  ~TransactionDBImpl();

  Status Initialize(const std::vector<size_t>& compaction_enabled_cf_indices,
                    const std::vector<ColumnFamilyHandle*>& handles);

  Transaction* BeginTransaction(const WriteOptions& write_options,
                                const TransactionOptions& txn_options,
                                Transaction* old_txn) override;

  using StackableDB::Put;
  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& val) override;

  using StackableDB::Delete;
  virtual Status Delete(const WriteOptions& wopts,
                        ColumnFamilyHandle* column_family,
                        const Slice& key) override;

  using StackableDB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) override;

  using StackableDB::Write;
  virtual Status Write(const WriteOptions& opts, WriteBatch* updates) override;

  using StackableDB::CreateColumnFamily;
  virtual Status CreateColumnFamily(const ColumnFamilyOptions& options,
                                    const std::string& column_family_name,
                                    ColumnFamilyHandle** handle) override;

  using StackableDB::DropColumnFamily;
  virtual Status DropColumnFamily(ColumnFamilyHandle* column_family) override;

  Status TryLock(TransactionImpl* txn, uint32_t cfh_id, const std::string& key,
                 bool exclusive);

  void UnLock(TransactionImpl* txn, const TransactionKeyMap* keys);
  void UnLock(TransactionImpl* txn, uint32_t cfh_id, const std::string& key);

  void AddColumnFamily(const ColumnFamilyHandle* handle);

  static TransactionDBOptions ValidateTxnDBOptions(
      const TransactionDBOptions& txn_db_options);

  const TransactionDBOptions& GetTxnDBOptions() const {
    return txn_db_options_;
  }

  void InsertExpirableTransaction(TransactionID tx_id, TransactionImpl* tx);
  void RemoveExpirableTransaction(TransactionID tx_id);

  // If transaction is no longer available, locks can be stolen
  // If transaction is available, try stealing locks directly from transaction
  // It is the caller's responsibility to ensure that the referred transaction
  // is expirable (GetExpirationTime() > 0) and that it is expired.
  bool TryStealingExpiredTransactionLocks(TransactionID tx_id);

  Transaction* GetTransactionByName(const TransactionName& name) override;

  void RegisterTransaction(Transaction* txn);
  void UnregisterTransaction(Transaction* txn);

  // not thread safe. current use case is during recovery (single thread)
  void GetAllPreparedTransactions(std::vector<Transaction*>* trans) override;

  TransactionLockMgr::LockStatusData GetLockStatusData() override;

 private:
  void ReinitializeTransaction(
      Transaction* txn, const WriteOptions& write_options,
      const TransactionOptions& txn_options = TransactionOptions());

  DBImpl* db_impl_;
  const TransactionDBOptions txn_db_options_;
  TransactionLockMgr lock_mgr_;

  // Must be held when adding/dropping column families.
  InstrumentedMutex column_family_mutex_;
  Transaction* BeginInternalTransaction(const WriteOptions& options);
  Status WriteHelper(WriteBatch* updates, TransactionImpl* txn_impl);

  // Used to ensure that no locks are stolen from an expirable transaction
  // that has started a commit. Only transactions with an expiration time
  // should be in this map.
  std::mutex map_mutex_;
  std::unordered_map<TransactionID, TransactionImpl*>
      expirable_transactions_map_;

  // map from name to two phase transaction instance
  std::mutex name_map_mutex_;
  std::unordered_map<TransactionName, Transaction*> transactions_;
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
