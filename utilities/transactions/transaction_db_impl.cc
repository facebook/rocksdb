//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include "utilities/transactions/transaction_db_impl.h"

#include <string>
#include <unordered_set>
#include <vector>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"
#include "utilities/transactions/transaction_impl.h"

namespace rocksdb {

TransactionDBImpl::TransactionDBImpl(DB* db,
                                     const TransactionDBOptions& txn_db_options)
    : TransactionDB(db),
      db_impl_(dynamic_cast<DBImpl*>(db)),
      txn_db_options_(txn_db_options),
      lock_mgr_(this, txn_db_options_.num_stripes, txn_db_options.max_num_locks,
                txn_db_options_.custom_mutex_factory
                    ? txn_db_options_.custom_mutex_factory
                    : std::shared_ptr<TransactionDBMutexFactory>(
                          new TransactionDBMutexFactoryImpl())) {
  assert(db_impl_ != nullptr);
}

TransactionDBImpl::~TransactionDBImpl() {
  while (!transactions_.empty()) {
    delete transactions_.begin()->second;
  }
}

Transaction* TransactionDBImpl::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new TransactionImpl(this, write_options, txn_options);
  }
}

TransactionDBOptions TransactionDBImpl::ValidateTxnDBOptions(
    const TransactionDBOptions& txn_db_options) {
  TransactionDBOptions validated = txn_db_options;

  if (txn_db_options.num_stripes == 0) {
    validated.num_stripes = 1;
  }

  return validated;
}

Status TransactionDB::Open(const Options& options,
                           const TransactionDBOptions& txn_db_options,
                           const std::string& dbname, TransactionDB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = TransactionDB::Open(db_options, txn_db_options, dbname,
                                 column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }

  return s;
}

Status TransactionDB::Open(
    const DBOptions& db_options, const TransactionDBOptions& txn_db_options,
    const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, TransactionDB** dbptr) {
  Status s;
  DB* db;

  std::vector<ColumnFamilyDescriptor> column_families_copy = column_families;
  std::vector<size_t> compaction_enabled_cf_indices;

  // Enable MemTable History if not already enabled
  for (size_t i = 0; i < column_families_copy.size(); i++) {
    ColumnFamilyOptions* options = &column_families_copy[i].options;

    if (options->max_write_buffer_number_to_maintain == 0) {
      // Setting to -1 will set the History size to max_write_buffer_number.
      options->max_write_buffer_number_to_maintain = -1;
    }

    if (!options->disable_auto_compactions) {
      // Disable compactions momentarily to prevent race with DB::Open
      options->disable_auto_compactions = true;
      compaction_enabled_cf_indices.push_back(i);
    }
  }

  DBOptions db_options_2pc = db_options;
  db_options_2pc.allow_2pc = true;
  s = DB::Open(db_options_2pc, dbname, column_families_copy, handles, &db);

  if (s.ok()) {
    TransactionDBImpl* txn_db = new TransactionDBImpl(
        db, TransactionDBImpl::ValidateTxnDBOptions(txn_db_options));
    *dbptr = txn_db;

    for (auto cf_ptr : *handles) {
      txn_db->AddColumnFamily(cf_ptr);
    }

    // Re-enable compaction for the column families that initially had
    // compaction enabled.
    assert(column_families_copy.size() == (*handles).size());
    std::vector<ColumnFamilyHandle*> compaction_enabled_cf_handles;
    compaction_enabled_cf_handles.reserve(compaction_enabled_cf_indices.size());
    for (auto index : compaction_enabled_cf_indices) {
      compaction_enabled_cf_handles.push_back((*handles)[index]);
    }

    s = txn_db->EnableAutoCompaction(compaction_enabled_cf_handles);

    // create 'real' transactions from recovered shell transactions
    assert(dynamic_cast<DBImpl*>(db) != nullptr);
    auto dbimpl = reinterpret_cast<DBImpl*>(db);
    auto rtrxs = dbimpl->recovered_transactions();

    for (auto it = rtrxs.begin(); it != rtrxs.end(); it++) {
      auto recovered_trx = it->second;
      assert(recovered_trx);
      assert(recovered_trx->log_number_);
      assert(recovered_trx->name_.length());

      WriteOptions w_options;
      w_options.sync = true;
      TransactionOptions t_options;

      Transaction* real_trx =
          txn_db->BeginTransaction(w_options, t_options, nullptr);
      assert(real_trx);
      real_trx->SetLogNumber(recovered_trx->log_number_);

      s = real_trx->SetName(recovered_trx->name_);
      if (!s.ok()) {
        break;
      }

      s = real_trx->RebuildFromWriteBatch(recovered_trx->batch_);
      real_trx->exec_status_ = Transaction::PREPARED;
      if (!s.ok()) {
        break;
      }
    }
    if (s.ok()) {
      dbimpl->DeleteAllRecoveredTransactions();
    }
  }

  return s;
}

// Let TransactionLockMgr know that this column family exists so it can
// allocate a LockMap for it.
void TransactionDBImpl::AddColumnFamily(const ColumnFamilyHandle* handle) {
  lock_mgr_.AddColumnFamily(handle->GetID());
}

Status TransactionDBImpl::CreateColumnFamily(
    const ColumnFamilyOptions& options, const std::string& column_family_name,
    ColumnFamilyHandle** handle) {
  InstrumentedMutexLock l(&column_family_mutex_);

  Status s = db_->CreateColumnFamily(options, column_family_name, handle);
  if (s.ok()) {
    lock_mgr_.AddColumnFamily((*handle)->GetID());
  }

  return s;
}

// Let TransactionLockMgr know that it can deallocate the LockMap for this
// column family.
Status TransactionDBImpl::DropColumnFamily(ColumnFamilyHandle* column_family) {
  InstrumentedMutexLock l(&column_family_mutex_);

  Status s = db_->DropColumnFamily(column_family);
  if (s.ok()) {
    lock_mgr_.RemoveColumnFamily(column_family->GetID());
  }

  return s;
}

Status TransactionDBImpl::TryLock(TransactionImpl* txn, uint32_t cfh_id,
                                  const std::string& key) {
  return lock_mgr_.TryLock(txn, cfh_id, key, GetEnv());
}

void TransactionDBImpl::UnLock(TransactionImpl* txn,
                               const TransactionKeyMap* keys) {
  lock_mgr_.UnLock(txn, keys, GetEnv());
}

void TransactionDBImpl::UnLock(TransactionImpl* txn, uint32_t cfh_id,
                               const std::string& key) {
  lock_mgr_.UnLock(txn, cfh_id, key, GetEnv());
}

// Used when wrapping DB write operations in a transaction
Transaction* TransactionDBImpl::BeginInternalTransaction(
    const WriteOptions& options) {
  TransactionOptions txn_options;
  Transaction* txn = BeginTransaction(options, txn_options, nullptr);

  // Use default timeout for non-transactional writes
  txn->SetLockTimeout(txn_db_options_.default_lock_timeout);
  return txn;
}

// All user Put, Merge, Delete, and Write requests must be intercepted to make
// sure that they lock all keys that they are writing to avoid causing conflicts
// with any concurent transactions. The easiest way to do this is to wrap all
// write operations in a transaction.
//
// Put(), Merge(), and Delete() only lock a single key per call.  Write() will
// sort its keys before locking them.  This guarantees that TransactionDB write
// methods cannot deadlock with eachother (but still could deadlock with a
// Transaction).
Status TransactionDBImpl::Put(const WriteOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice& key, const Slice& val) {
  Status s;

  Transaction* txn = BeginInternalTransaction(options);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do PutUntracked().
  s = txn->PutUntracked(column_family, key, val);

  if (s.ok()) {
    s = txn->Commit();
  }

  delete txn;

  return s;
}

Status TransactionDBImpl::Delete(const WriteOptions& wopts,
                                 ColumnFamilyHandle* column_family,
                                 const Slice& key) {
  Status s;

  Transaction* txn = BeginInternalTransaction(wopts);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do
  // DeleteUntracked().
  s = txn->DeleteUntracked(column_family, key);

  if (s.ok()) {
    s = txn->Commit();
  }

  delete txn;

  return s;
}

Status TransactionDBImpl::Merge(const WriteOptions& options,
                                ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value) {
  Status s;

  Transaction* txn = BeginInternalTransaction(options);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do
  // MergeUntracked().
  s = txn->MergeUntracked(column_family, key, value);

  if (s.ok()) {
    s = txn->Commit();
  }

  delete txn;

  return s;
}

Status TransactionDBImpl::Write(const WriteOptions& opts, WriteBatch* updates) {
  // Need to lock all keys in this batch to prevent write conflicts with
  // concurrent transactions.
  Transaction* txn = BeginInternalTransaction(opts);
  txn->DisableIndexing();

  assert(dynamic_cast<TransactionImpl*>(txn) != nullptr);
  auto txn_impl = reinterpret_cast<TransactionImpl*>(txn);

  // Since commitBatch sorts the keys before locking, concurrent Write()
  // operations will not cause a deadlock.
  // In order to avoid a deadlock with a concurrent Transaction, Transactions
  // should use a lock timeout.
  Status s = txn_impl->CommitBatch(updates);

  delete txn;

  return s;
}

void TransactionDBImpl::InsertExpirableTransaction(TransactionID tx_id,
                                                   TransactionImpl* tx) {
  assert(tx->GetExpirationTime() > 0);
  std::lock_guard<std::mutex> lock(map_mutex_);
  expirable_transactions_map_.insert({tx_id, tx});
}

void TransactionDBImpl::RemoveExpirableTransaction(TransactionID tx_id) {
  std::lock_guard<std::mutex> lock(map_mutex_);
  expirable_transactions_map_.erase(tx_id);
}

bool TransactionDBImpl::TryStealingExpiredTransactionLocks(
    TransactionID tx_id) {
  std::lock_guard<std::mutex> lock(map_mutex_);

  auto tx_it = expirable_transactions_map_.find(tx_id);
  if (tx_it == expirable_transactions_map_.end()) {
    return true;
  }
  TransactionImpl& tx = *(tx_it->second);
  return tx.TryStealingLocks();
}

void TransactionDBImpl::ReinitializeTransaction(
    Transaction* txn, const WriteOptions& write_options,
    const TransactionOptions& txn_options) {
  assert(dynamic_cast<TransactionImpl*>(txn) != nullptr);
  auto txn_impl = reinterpret_cast<TransactionImpl*>(txn);

  txn_impl->Reinitialize(this, write_options, txn_options);
}

Transaction* TransactionDBImpl::GetTransactionByName(
    const TransactionName& name) {
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  auto it = transactions_.find(name);
  if (it == transactions_.end()) {
    return nullptr;
  } else {
    return it->second;
  }
}

void TransactionDBImpl::GetAllPreparedTransactions(
    std::vector<Transaction*>* transv) {
  assert(transv);
  transv->clear();
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  for (auto it = transactions_.begin(); it != transactions_.end(); it++) {
    if (it->second->exec_status_ == Transaction::PREPARED) {
      transv->push_back(it->second);
    }
  }
}

void TransactionDBImpl::RegisterTransaction(Transaction* txn) {
  assert(txn);
  assert(txn->GetName().length() > 0);
  assert(GetTransactionByName(txn->GetName()) == nullptr);
  assert(txn->exec_status_ == Transaction::STARTED);
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  transactions_[txn->GetName()] = txn;
}

void TransactionDBImpl::UnregisterTransaction(Transaction* txn) {
  assert(txn);
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  auto it = transactions_.find(txn->GetName());
  assert(it != transactions_.end());
  transactions_.erase(it);
}

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
