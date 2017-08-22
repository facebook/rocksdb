//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/transaction_lock_mgr.h"
#include "utilities/transactions/write_prepared_txn.h"

namespace rocksdb {

class PessimisticTransactionDB : public TransactionDB {
 public:
  explicit PessimisticTransactionDB(DB* db,
                                    const TransactionDBOptions& txn_db_options);

  explicit PessimisticTransactionDB(StackableDB* db,
                                    const TransactionDBOptions& txn_db_options);

  virtual ~PessimisticTransactionDB();

  Status Initialize(const std::vector<size_t>& compaction_enabled_cf_indices,
                    const std::vector<ColumnFamilyHandle*>& handles);

  Transaction* BeginTransaction(const WriteOptions& write_options,
                                const TransactionOptions& txn_options,
                                Transaction* old_txn) override = 0;

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

  Status TryLock(PessimisticTransaction* txn, uint32_t cfh_id,
                 const std::string& key, bool exclusive);

  void UnLock(PessimisticTransaction* txn, const TransactionKeyMap* keys);
  void UnLock(PessimisticTransaction* txn, uint32_t cfh_id,
              const std::string& key);

  void AddColumnFamily(const ColumnFamilyHandle* handle);

  static TransactionDBOptions ValidateTxnDBOptions(
      const TransactionDBOptions& txn_db_options);

  const TransactionDBOptions& GetTxnDBOptions() const {
    return txn_db_options_;
  }

  void InsertExpirableTransaction(TransactionID tx_id,
                                  PessimisticTransaction* tx);
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

  std::vector<DeadlockPath> GetDeadlockInfoBuffer() override;
  void SetDeadlockInfoBufferSize(uint32_t target_size) override;

  struct CommitEntry {
    uint64_t prep_seq;
    uint64_t commit_seq;
  };

 protected:
  void ReinitializeTransaction(
      Transaction* txn, const WriteOptions& write_options,
      const TransactionOptions& txn_options = TransactionOptions());
  DBImpl* db_impl_;

 private:
  const TransactionDBOptions txn_db_options_;
  TransactionLockMgr lock_mgr_;

  // Must be held when adding/dropping column families.
  InstrumentedMutex column_family_mutex_;
  Transaction* BeginInternalTransaction(const WriteOptions& options);

  // Used to ensure that no locks are stolen from an expirable transaction
  // that has started a commit. Only transactions with an expiration time
  // should be in this map.
  std::mutex map_mutex_;
  std::unordered_map<TransactionID, PessimisticTransaction*>
      expirable_transactions_map_;

  // map from name to two phase transaction instance
  std::mutex name_map_mutex_;
  std::unordered_map<TransactionName, Transaction*> transactions_;
};

// A PessimisticTransactionDB that writes the data to the DB after the commit.
// In this way the DB only contains the committed data.
class WriteCommittedTxnDB : public PessimisticTransactionDB {
 public:
  explicit WriteCommittedTxnDB(DB* db,
                               const TransactionDBOptions& txn_db_options)
      : PessimisticTransactionDB(db, txn_db_options) {}

  explicit WriteCommittedTxnDB(StackableDB* db,
                               const TransactionDBOptions& txn_db_options)
      : PessimisticTransactionDB(db, txn_db_options) {}

  virtual ~WriteCommittedTxnDB() {}

  Transaction* BeginTransaction(const WriteOptions& write_options,
                                const TransactionOptions& txn_options,
                                Transaction* old_txn) override;
};

// A PessimisticTransactionDB that writes data to DB after prepare phase of 2PC.
// In this way some data in the DB might not be committed. The DB provides
// mechanisms to tell such data apart from committed data.
class WritePreparedTxnDB : public PessimisticTransactionDB {
 public:
  explicit WritePreparedTxnDB(DB* db,
                              const TransactionDBOptions& txn_db_options)
      : PessimisticTransactionDB(db, txn_db_options) {}

  explicit WritePreparedTxnDB(StackableDB* db,
                              const TransactionDBOptions& txn_db_options)
      : PessimisticTransactionDB(db, txn_db_options) {}

  virtual ~WritePreparedTxnDB() {}

  Transaction* BeginTransaction(const WriteOptions& write_options,
                                const TransactionOptions& txn_options,
                                Transaction* old_txn) override;

  // Check whether the transaction that wrote the value with seqeunce number seq
  // is visible to the snapshot with sequence number snapshot_seq
  bool IsInSnapshot(uint64_t seq, uint64_t snapshot_seq);
  // Add the trasnaction with prepare sequence seq to the prepared list
  void AddPrepared(uint64_t seq);
  // Add the transaction with prepare sequence prepare_seq and commit sequence
  // commit_seq to the commit map
  void AddCommitted(uint64_t prepare_seq, uint64_t commit_seq);

 private:
  // A heap with the amortized O(1) complexity for erase. It uses one extra heap
  // to keep track of erased entries that are not yet on top of the main heap.
  class PreparedHeap {
    std::priority_queue<uint64_t> heap_;
    std::priority_queue<uint64_t> erased_heap_;

   public:
    bool empty() { return heap_.empty(); }
    uint64_t top() { return heap_.top(); }
    void push(uint64_t v) { heap_.push(v); }
    void pop() {
      heap_.pop();
      while (!heap_.empty() && !erased_heap_.empty() &&
             heap_.top() == erased_heap_.top()) {
        heap_.pop();
        erased_heap_.pop();
      }
    }
    void erase(uint64_t seq) {
      if (!heap_.empty()) {
        if (heap_.top() < seq) {
          // Already popped, ignore it.
        } else if (heap_.top() == seq) {
          heap_.pop();
        } else {  // (heap_.top() > seq)
          // Down the heap, remember to pop it later
          erased_heap_.push(seq);
        }
      }
    }
  };

  // Get the commit entry with index indexed_seq from the commit table. It
  // returns true if such entry exists.
  bool GetCommitEntry(uint64_t indexed_seq, CommitEntry* entry);
  // Rewrite the entry with the index indexed_seq in the commit table with the
  // commit entry <prep_seq, commit_seq>. If the rewrite results into eviction,
  // sets the evicted_entry and returns true.
  bool AddCommitEntry(uint64_t indexed_seq, CommitEntry& new_entry,
                      CommitEntry* evicted_entry);
  // Rewrite the entry with the index indexed_seq in the commit table with the
  // commit entry new_entry only if the existing entry matches the
  // expected_entry. Returns false otherwise.
  bool ExchangeCommitEntry(uint64_t indexed_seq, CommitEntry& expected_entry,
                           CommitEntry new_entry);

  // The list of live snapshots at the last time that max_evicted_seq_ advanced.
  // The list sorted in ascending order. Thread-safety is provided with
  // snapshots_mutex_.
  std::vector<SequenceNumber> snapshots_;
  // A heap of prepared transactions. Thread-safety is provided with
  // prepared_mutex_.
  PreparedHeap prepared_txns_;
  // 10m entry, 80MB size
  static const uint64_t COMMIT_CACHE_SIZE = static_cast<uint64_t>(1 << 21);
  // commit_cache_ is initialized to zero to tell apart an empty index from a
  // filled one. Thread-safety is provided with commit_cache_mutex_.
  CommitEntry commit_cache_[COMMIT_CACHE_SIZE] = {};
  // The largest evicted *commit* sequence number from the commit_cache_
  std::atomic<uint64_t> max_evicted_seq_ = {};
  // A map of the evicted entries from commit_cache_ that has to be kept around
  // to service the old snapshots. This is expected to be empty normally.
  // Thread-safety is provided with old_commit_map_mutex_.
  std::map<uint64_t, uint64_t> old_commit_map_;
  // A set of long-running prepared transactions that are not finished by the
  // time max_evicted_seq_ advances their sequence number. This is expected to
  // be empty normally. Thread-safety is provided with prepared_mutex_.
  std::set<uint64_t> delayed_prepared_;
  // Update when delayed_prepared_.empty() changes. Expected to be true
  // normally.
  std::atomic<bool> delayed_prepared_empty_ = {true};
  // Update when old_commit_map_.empty() changes. Expected to be true normally.
  std::atomic<bool> old_commit_map_empty_ = {true};
  port::RWMutex prepared_mutex_;
  port::RWMutex old_commit_map_mutex_;
  port::RWMutex commit_cache_mutex_;
  port::RWMutex snapshots_mutex_;
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
