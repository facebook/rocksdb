//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/pessimistic_transaction_db.h"

#include <inttypes.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/cast_util.h"
#include "util/mutexlock.h"
#include "util/sync_point.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"

namespace rocksdb {

PessimisticTransactionDB::PessimisticTransactionDB(
    DB* db, const TransactionDBOptions& txn_db_options)
    : TransactionDB(db),
      db_impl_(static_cast_with_check<DBImpl, DB>(db)),
      txn_db_options_(txn_db_options),
      lock_mgr_(this, txn_db_options_.num_stripes, txn_db_options.max_num_locks,
                txn_db_options_.max_num_deadlocks,
                txn_db_options_.custom_mutex_factory
                    ? txn_db_options_.custom_mutex_factory
                    : std::shared_ptr<TransactionDBMutexFactory>(
                          new TransactionDBMutexFactoryImpl())) {
  assert(db_impl_ != nullptr);
  info_log_ = db_impl_->GetDBOptions().info_log;
}

// Support initiliazing PessimisticTransactionDB from a stackable db
//
//    PessimisticTransactionDB
//     ^        ^
//     |        |
//     |        +
//     |   StackableDB
//     |   ^
//     |   |
//     +   +
//     DBImpl
//       ^
//       |(inherit)
//       +
//       DB
//
PessimisticTransactionDB::PessimisticTransactionDB(
    StackableDB* db, const TransactionDBOptions& txn_db_options)
    : TransactionDB(db),
      db_impl_(static_cast_with_check<DBImpl, DB>(db->GetRootDB())),
      txn_db_options_(txn_db_options),
      lock_mgr_(this, txn_db_options_.num_stripes, txn_db_options.max_num_locks,
                txn_db_options_.max_num_deadlocks,
                txn_db_options_.custom_mutex_factory
                    ? txn_db_options_.custom_mutex_factory
                    : std::shared_ptr<TransactionDBMutexFactory>(
                          new TransactionDBMutexFactoryImpl())) {
  assert(db_impl_ != nullptr);
}

PessimisticTransactionDB::~PessimisticTransactionDB() {
  while (!transactions_.empty()) {
    delete transactions_.begin()->second;
    // TODO(myabandeh): this seems to be an unsafe approach as it is not quite
    // clear whether delete would also remove the entry from transactions_.
  }
}

Status PessimisticTransactionDB::Initialize(
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles) {
  for (auto cf_ptr : handles) {
    AddColumnFamily(cf_ptr);
  }
  // Re-enable compaction for the column families that initially had
  // compaction enabled.
  std::vector<ColumnFamilyHandle*> compaction_enabled_cf_handles;
  compaction_enabled_cf_handles.reserve(compaction_enabled_cf_indices.size());
  for (auto index : compaction_enabled_cf_indices) {
    compaction_enabled_cf_handles.push_back(handles[index]);
  }

  Status s = EnableAutoCompaction(compaction_enabled_cf_handles);

  // create 'real' transactions from recovered shell transactions
  auto dbimpl = reinterpret_cast<DBImpl*>(GetRootDB());
  assert(dbimpl != nullptr);
  auto rtrxs = dbimpl->recovered_transactions();

  for (auto it = rtrxs.begin(); it != rtrxs.end(); it++) {
    auto recovered_trx = it->second;
    assert(recovered_trx);
    assert(recovered_trx->log_number_);
    assert(recovered_trx->name_.length());

    WriteOptions w_options;
    w_options.sync = true;
    TransactionOptions t_options;

    Transaction* real_trx = BeginTransaction(w_options, t_options, nullptr);
    assert(real_trx);
    real_trx->SetLogNumber(recovered_trx->log_number_);
    assert(recovered_trx->seq_ != kMaxSequenceNumber);
    real_trx->SetId(recovered_trx->seq_);

    s = real_trx->SetName(recovered_trx->name_);
    if (!s.ok()) {
      break;
    }

    s = real_trx->RebuildFromWriteBatch(recovered_trx->batch_);
    real_trx->SetState(Transaction::PREPARED);
    if (!s.ok()) {
      break;
    }
  }
  if (s.ok()) {
    dbimpl->DeleteAllRecoveredTransactions();
  }
  return s;
}

Status WritePreparedTxnDB::Initialize(
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles) {
  auto dbimpl = reinterpret_cast<DBImpl*>(GetRootDB());
  assert(dbimpl != nullptr);
  auto rtxns = dbimpl->recovered_transactions();
  for (auto rtxn : rtxns) {
    AddPrepared(rtxn.second->seq_);
  }
  SequenceNumber prev_max = max_evicted_seq_;
  SequenceNumber last_seq = db_impl_->GetLatestSequenceNumber();
  AdvanceMaxEvictedSeq(prev_max, last_seq);

  db_impl_->SetSnapshotChecker(new SnapshotChecker(this));

  auto s = PessimisticTransactionDB::Initialize(compaction_enabled_cf_indices,
                                                handles);
  return s;
}

Transaction* WriteCommittedTxnDB::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new WriteCommittedTxn(this, write_options, txn_options);
  }
}

Transaction* WritePreparedTxnDB::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new WritePreparedTxn(this, write_options, txn_options);
  }
}

TransactionDBOptions PessimisticTransactionDB::ValidateTxnDBOptions(
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
  DBOptions db_options_2pc = db_options;
  if (txn_db_options.write_policy == WRITE_PREPARED) {
    db_options_2pc.seq_per_batch = true;
  }
  PrepareWrap(&db_options_2pc, &column_families_copy,
              &compaction_enabled_cf_indices);
  s = DB::Open(db_options_2pc, dbname, column_families_copy, handles, &db);
  if (s.ok()) {
    s = WrapDB(db, txn_db_options, compaction_enabled_cf_indices, *handles,
               dbptr);
  }
  return s;
}

void TransactionDB::PrepareWrap(
    DBOptions* db_options, std::vector<ColumnFamilyDescriptor>* column_families,
    std::vector<size_t>* compaction_enabled_cf_indices) {
  compaction_enabled_cf_indices->clear();

  // Enable MemTable History if not already enabled
  for (size_t i = 0; i < column_families->size(); i++) {
    ColumnFamilyOptions* cf_options = &(*column_families)[i].options;

    if (cf_options->max_write_buffer_number_to_maintain == 0) {
      // Setting to -1 will set the History size to max_write_buffer_number.
      cf_options->max_write_buffer_number_to_maintain = -1;
    }
    if (!cf_options->disable_auto_compactions) {
      // Disable compactions momentarily to prevent race with DB::Open
      cf_options->disable_auto_compactions = true;
      compaction_enabled_cf_indices->push_back(i);
    }
  }
  db_options->allow_2pc = true;
}

Status TransactionDB::WrapDB(
    // make sure this db is already opened with memtable history enabled,
    // auto compaction distabled and 2 phase commit enabled
    DB* db, const TransactionDBOptions& txn_db_options,
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles, TransactionDB** dbptr) {
  PessimisticTransactionDB* txn_db;
  switch (txn_db_options.write_policy) {
    case WRITE_UNPREPARED:
      return Status::NotSupported("WRITE_UNPREPARED is not implemented yet");
    case WRITE_PREPARED:
      txn_db = new WritePreparedTxnDB(
          db, PessimisticTransactionDB::ValidateTxnDBOptions(txn_db_options));
      break;
    case WRITE_COMMITTED:
    default:
      txn_db = new WriteCommittedTxnDB(
          db, PessimisticTransactionDB::ValidateTxnDBOptions(txn_db_options));
  }
  *dbptr = txn_db;
  Status s = txn_db->Initialize(compaction_enabled_cf_indices, handles);
  return s;
}

Status TransactionDB::WrapStackableDB(
    // make sure this stackable_db is already opened with memtable history
    // enabled,
    // auto compaction distabled and 2 phase commit enabled
    StackableDB* db, const TransactionDBOptions& txn_db_options,
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles, TransactionDB** dbptr) {
  PessimisticTransactionDB* txn_db;
  switch (txn_db_options.write_policy) {
    case WRITE_UNPREPARED:
      return Status::NotSupported("WRITE_UNPREPARED is not implemented yet");
    case WRITE_PREPARED:
      txn_db = new WritePreparedTxnDB(
          db, PessimisticTransactionDB::ValidateTxnDBOptions(txn_db_options));
      break;
    case WRITE_COMMITTED:
    default:
      txn_db = new WriteCommittedTxnDB(
          db, PessimisticTransactionDB::ValidateTxnDBOptions(txn_db_options));
  }
  *dbptr = txn_db;
  Status s = txn_db->Initialize(compaction_enabled_cf_indices, handles);
  return s;
}

// Let TransactionLockMgr know that this column family exists so it can
// allocate a LockMap for it.
void PessimisticTransactionDB::AddColumnFamily(
    const ColumnFamilyHandle* handle) {
  lock_mgr_.AddColumnFamily(handle->GetID());
}

Status PessimisticTransactionDB::CreateColumnFamily(
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
Status PessimisticTransactionDB::DropColumnFamily(
    ColumnFamilyHandle* column_family) {
  InstrumentedMutexLock l(&column_family_mutex_);

  Status s = db_->DropColumnFamily(column_family);
  if (s.ok()) {
    lock_mgr_.RemoveColumnFamily(column_family->GetID());
  }

  return s;
}

Status PessimisticTransactionDB::TryLock(PessimisticTransaction* txn,
                                         uint32_t cfh_id,
                                         const std::string& key,
                                         bool exclusive) {
  return lock_mgr_.TryLock(txn, cfh_id, key, GetEnv(), exclusive);
}

void PessimisticTransactionDB::UnLock(PessimisticTransaction* txn,
                                      const TransactionKeyMap* keys) {
  lock_mgr_.UnLock(txn, keys, GetEnv());
}

void PessimisticTransactionDB::UnLock(PessimisticTransaction* txn,
                                      uint32_t cfh_id, const std::string& key) {
  lock_mgr_.UnLock(txn, cfh_id, key, GetEnv());
}

// Used when wrapping DB write operations in a transaction
Transaction* PessimisticTransactionDB::BeginInternalTransaction(
    const WriteOptions& options) {
  TransactionOptions txn_options;
  Transaction* txn = BeginTransaction(options, txn_options, nullptr);

  // Use default timeout for non-transactional writes
  txn->SetLockTimeout(txn_db_options_.default_lock_timeout);
  return txn;
}

// All user Put, Merge, Delete, and Write requests must be intercepted to make
// sure that they lock all keys that they are writing to avoid causing conflicts
// with any concurrent transactions. The easiest way to do this is to wrap all
// write operations in a transaction.
//
// Put(), Merge(), and Delete() only lock a single key per call.  Write() will
// sort its keys before locking them.  This guarantees that TransactionDB write
// methods cannot deadlock with eachother (but still could deadlock with a
// Transaction).
Status PessimisticTransactionDB::Put(const WriteOptions& options,
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

Status PessimisticTransactionDB::Delete(const WriteOptions& wopts,
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

Status PessimisticTransactionDB::SingleDelete(const WriteOptions& wopts,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key) {
  Status s;

  Transaction* txn = BeginInternalTransaction(wopts);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do
  // SingleDeleteUntracked().
  s = txn->SingleDeleteUntracked(column_family, key);

  if (s.ok()) {
    s = txn->Commit();
  }

  delete txn;

  return s;
}

Status PessimisticTransactionDB::Merge(const WriteOptions& options,
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

Status PessimisticTransactionDB::Write(const WriteOptions& opts,
                                       WriteBatch* updates) {
  // Need to lock all keys in this batch to prevent write conflicts with
  // concurrent transactions.
  Transaction* txn = BeginInternalTransaction(opts);
  txn->DisableIndexing();
  // TODO(myabandeh): indexing being disabled we need another machanism to
  // detect duplicattes in the input patch

  auto txn_impl =
      static_cast_with_check<PessimisticTransaction, Transaction>(txn);

  // Since commitBatch sorts the keys before locking, concurrent Write()
  // operations will not cause a deadlock.
  // In order to avoid a deadlock with a concurrent Transaction, Transactions
  // should use a lock timeout.
  Status s = txn_impl->CommitBatch(updates);

  delete txn;

  return s;
}

void PessimisticTransactionDB::InsertExpirableTransaction(
    TransactionID tx_id, PessimisticTransaction* tx) {
  assert(tx->GetExpirationTime() > 0);
  std::lock_guard<std::mutex> lock(map_mutex_);
  expirable_transactions_map_.insert({tx_id, tx});
}

void PessimisticTransactionDB::RemoveExpirableTransaction(TransactionID tx_id) {
  std::lock_guard<std::mutex> lock(map_mutex_);
  expirable_transactions_map_.erase(tx_id);
}

bool PessimisticTransactionDB::TryStealingExpiredTransactionLocks(
    TransactionID tx_id) {
  std::lock_guard<std::mutex> lock(map_mutex_);

  auto tx_it = expirable_transactions_map_.find(tx_id);
  if (tx_it == expirable_transactions_map_.end()) {
    return true;
  }
  PessimisticTransaction& tx = *(tx_it->second);
  return tx.TryStealingLocks();
}

void PessimisticTransactionDB::ReinitializeTransaction(
    Transaction* txn, const WriteOptions& write_options,
    const TransactionOptions& txn_options) {
  auto txn_impl =
      static_cast_with_check<PessimisticTransaction, Transaction>(txn);

  txn_impl->Reinitialize(this, write_options, txn_options);
}

Transaction* PessimisticTransactionDB::GetTransactionByName(
    const TransactionName& name) {
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  auto it = transactions_.find(name);
  if (it == transactions_.end()) {
    return nullptr;
  } else {
    return it->second;
  }
}

void PessimisticTransactionDB::GetAllPreparedTransactions(
    std::vector<Transaction*>* transv) {
  assert(transv);
  transv->clear();
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  for (auto it = transactions_.begin(); it != transactions_.end(); it++) {
    if (it->second->GetState() == Transaction::PREPARED) {
      transv->push_back(it->second);
    }
  }
}

TransactionLockMgr::LockStatusData
PessimisticTransactionDB::GetLockStatusData() {
  return lock_mgr_.GetLockStatusData();
}

std::vector<DeadlockPath> PessimisticTransactionDB::GetDeadlockInfoBuffer() {
  return lock_mgr_.GetDeadlockInfoBuffer();
}

void PessimisticTransactionDB::SetDeadlockInfoBufferSize(uint32_t target_size) {
  lock_mgr_.Resize(target_size);
}

void PessimisticTransactionDB::RegisterTransaction(Transaction* txn) {
  assert(txn);
  assert(txn->GetName().length() > 0);
  assert(GetTransactionByName(txn->GetName()) == nullptr);
  assert(txn->GetState() == Transaction::STARTED);
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  transactions_[txn->GetName()] = txn;
}

void PessimisticTransactionDB::UnregisterTransaction(Transaction* txn) {
  assert(txn);
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  auto it = transactions_.find(txn->GetName());
  assert(it != transactions_.end());
  transactions_.erase(it);
}

Status WritePreparedTxnDB::Get(const ReadOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, PinnableSlice* value) {
  // We are fine with the latest committed value. This could be done by
  // specifying the snapshot as kMaxSequenceNumber.
  SequenceNumber seq = kMaxSequenceNumber;
  if (options.snapshot != nullptr) {
    seq = options.snapshot->GetSequenceNumber();
  }
  WritePreparedTxnReadCallback callback(this, seq);
  bool* dont_care = nullptr;
  // Note: no need to specify a snapshot for read options as no specific
  // snapshot is requested by the user.
  return db_impl_->GetImpl(options, column_family, key, value, dont_care,
                           &callback);
}

void WritePreparedTxnDB::Init(const TransactionDBOptions& /* unused */) {
  // Adcance max_evicted_seq_ no more than 100 times before the cache wraps
  // around.
  INC_STEP_FOR_MAX_EVICTED =
      std::max(SNAPSHOT_CACHE_SIZE / 100, static_cast<size_t>(1));
  snapshot_cache_ = unique_ptr<std::atomic<SequenceNumber>[]>(
      new std::atomic<SequenceNumber>[SNAPSHOT_CACHE_SIZE] {});
  commit_cache_ = unique_ptr<std::atomic<CommitEntry64b>[]>(
      new std::atomic<CommitEntry64b>[COMMIT_CACHE_SIZE] {});
}

// Returns true if commit_seq <= snapshot_seq
bool WritePreparedTxnDB::IsInSnapshot(uint64_t prep_seq,
                                      uint64_t snapshot_seq) const {
  // Here we try to infer the return value without looking into prepare list.
  // This would help avoiding synchronization over a shared map.
  // TODO(myabandeh): read your own writes
  // TODO(myabandeh): optimize this. This sequence of checks must be correct but
  // not necessary efficient
  if (prep_seq == 0) {
    // Compaction will output keys to bottom-level with sequence number 0 if
    // it is visible to the earliest snapshot.
    return true;
  }
  if (snapshot_seq < prep_seq) {
    // snapshot_seq < prep_seq <= commit_seq => snapshot_seq < commit_seq
    return false;
  }
  if (!delayed_prepared_empty_.load(std::memory_order_acquire)) {
    // We should not normally reach here
    ReadLock rl(&prepared_mutex_);
    if (delayed_prepared_.find(prep_seq) != delayed_prepared_.end()) {
      // Then it is not committed yet
      return false;
    }
  }
  auto indexed_seq = prep_seq % COMMIT_CACHE_SIZE;
  CommitEntry64b dont_care;
  CommitEntry cached;
  bool exist = GetCommitEntry(indexed_seq, &dont_care, &cached);
  if (exist && prep_seq == cached.prep_seq) {
    // It is committed and also not evicted from commit cache
    return cached.commit_seq <= snapshot_seq;
  }
  // else it could be committed but not inserted in the map which could happen
  // after recovery, or it could be committed and evicted by another commit, or
  // never committed.

  // At this point we dont know if it was committed or it is still prepared
  auto max_evicted_seq = max_evicted_seq_.load(std::memory_order_acquire);
  if (max_evicted_seq < prep_seq) {
    // Not evicted from cache and also not present, so must be still prepared
    return false;
  }
  // When advancing max_evicted_seq_, we move older entires from prepared to
  // delayed_prepared_. Also we move evicted entries from commit cache to
  // old_commit_map_ if it overlaps with any snapshot. Since prep_seq <=
  // max_evicted_seq_, we have three cases: i) in delayed_prepared_, ii) in
  // old_commit_map_, iii) committed with no conflict with any snapshot (i)
  // delayed_prepared_ is checked above
  if (max_evicted_seq < snapshot_seq) {  // then (ii) cannot be the case
    // only (iii) is the case: committed
    // commit_seq <= max_evicted_seq_ < snapshot_seq => commit_seq <
    // snapshot_seq
    return true;
  }
  // else (ii) might be the case: check the commit data saved for this snapshot.
  // If there was no overlapping commit entry, then it is committed with a
  // commit_seq lower than any live snapshot, including snapshot_seq.
  if (old_commit_map_empty_.load(std::memory_order_acquire)) {
    return true;
  }
  {
    // We should not normally reach here
    ReadLock rl(&old_commit_map_mutex_);
    auto old_commit_entry = old_commit_map_.find(prep_seq);
    if (old_commit_entry == old_commit_map_.end() ||
        old_commit_entry->second <= snapshot_seq) {
      return true;
    }
  }
  // (ii) it the case: it is committed but after the snapshot_seq
  return false;
}

void WritePreparedTxnDB::AddPrepared(uint64_t seq) {
  ROCKS_LOG_DEBUG(info_log_, "Txn %" PRIu64 " Prepareing", seq);
  assert(seq > max_evicted_seq_);
  WriteLock wl(&prepared_mutex_);
  prepared_txns_.push(seq);
}

void WritePreparedTxnDB::RollbackPrepared(uint64_t prep_seq,
                                          uint64_t rollback_seq) {
  ROCKS_LOG_DEBUG(
      info_log_, "Txn %" PRIu64 " rolling back with rollback seq of " PRIu64 "",
      prep_seq, rollback_seq);
  std::vector<SequenceNumber> snapshots =
      GetSnapshotListFromDB(kMaxSequenceNumber);
  // TODO(myabandeh): currently we are assuming that there is no snapshot taken
  // when a transaciton is rolled back. This is the case the way MySQL does
  // rollback which is after recovery. We should extend it to be able to
  // rollback txns that overlap with exsiting snapshots.
  assert(snapshots.size() == 0);
  if (snapshots.size()) {
    throw std::runtime_error(
        "Rollback reqeust while there are live snapshots.");
  }
  WriteLock wl(&prepared_mutex_);
  prepared_txns_.erase(prep_seq);
  bool was_empty = delayed_prepared_.empty();
  if (!was_empty) {
    delayed_prepared_.erase(prep_seq);
    bool is_empty = delayed_prepared_.empty();
    if (was_empty != is_empty) {
      delayed_prepared_empty_.store(is_empty, std::memory_order_release);
    }
  }
}

void WritePreparedTxnDB::AddCommitted(uint64_t prepare_seq,
                                      uint64_t commit_seq) {
  ROCKS_LOG_DEBUG(info_log_, "Txn %" PRIu64 " Committing with %" PRIu64,
                  prepare_seq, commit_seq);
  auto indexed_seq = prepare_seq % COMMIT_CACHE_SIZE;
  CommitEntry64b evicted_64b;
  CommitEntry evicted;
  bool to_be_evicted = GetCommitEntry(indexed_seq, &evicted_64b, &evicted);
  if (to_be_evicted) {
    auto prev_max = max_evicted_seq_.load(std::memory_order_acquire);
    if (prev_max < evicted.commit_seq) {
      // Inc max in larger steps to avoid frequent updates
      auto max_evicted_seq = evicted.commit_seq + INC_STEP_FOR_MAX_EVICTED;
      AdvanceMaxEvictedSeq(prev_max, max_evicted_seq);
    }
    // After each eviction from commit cache, check if the commit entry should
    // be kept around because it overlaps with a live snapshot.
    CheckAgainstSnapshots(evicted);
  }
  bool succ =
      ExchangeCommitEntry(indexed_seq, evicted_64b, {prepare_seq, commit_seq});
  if (!succ) {
    // A very rare event, in which the commit entry is updated before we do.
    // Here we apply a very simple solution of retrying.
    // TODO(myabandeh): do precautions to detect bugs that cause infinite loops
    AddCommitted(prepare_seq, commit_seq);
    return;
  }
  {
    WriteLock wl(&prepared_mutex_);
    prepared_txns_.erase(prepare_seq);
    bool was_empty = delayed_prepared_.empty();
    if (!was_empty) {
      delayed_prepared_.erase(prepare_seq);
      bool is_empty = delayed_prepared_.empty();
      if (was_empty != is_empty) {
        delayed_prepared_empty_.store(is_empty, std::memory_order_release);
      }
    }
  }
}

bool WritePreparedTxnDB::GetCommitEntry(const uint64_t indexed_seq,
                                        CommitEntry64b* entry_64b,
                                        CommitEntry* entry) const {
  *entry_64b = commit_cache_[indexed_seq].load(std::memory_order_acquire);
  bool valid = entry_64b->Parse(indexed_seq, entry, FORMAT);
  return valid;
}

bool WritePreparedTxnDB::AddCommitEntry(const uint64_t indexed_seq,
                                        const CommitEntry& new_entry,
                                        CommitEntry* evicted_entry) {
  CommitEntry64b new_entry_64b(new_entry, FORMAT);
  CommitEntry64b evicted_entry_64b = commit_cache_[indexed_seq].exchange(
      new_entry_64b, std::memory_order_acq_rel);
  bool valid = evicted_entry_64b.Parse(indexed_seq, evicted_entry, FORMAT);
  return valid;
}

bool WritePreparedTxnDB::ExchangeCommitEntry(const uint64_t indexed_seq,
                                             CommitEntry64b& expected_entry_64b,
                                             const CommitEntry& new_entry) {
  auto& atomic_entry = commit_cache_[indexed_seq];
  CommitEntry64b new_entry_64b(new_entry, FORMAT);
  bool succ = atomic_entry.compare_exchange_strong(
      expected_entry_64b, new_entry_64b, std::memory_order_acq_rel,
      std::memory_order_acquire);
  return succ;
}

void WritePreparedTxnDB::AdvanceMaxEvictedSeq(SequenceNumber& prev_max,
                                              SequenceNumber& new_max) {
  // When max_evicted_seq_ advances, move older entries from prepared_txns_
  // to delayed_prepared_. This guarantees that if a seq is lower than max,
  // then it is not in prepared_txns_ ans save an expensive, synchronized
  // lookup from a shared set. delayed_prepared_ is expected to be empty in
  // normal cases.
  {
    WriteLock wl(&prepared_mutex_);
    while (!prepared_txns_.empty() && prepared_txns_.top() <= new_max) {
      auto to_be_popped = prepared_txns_.top();
      delayed_prepared_.insert(to_be_popped);
      prepared_txns_.pop();
      delayed_prepared_empty_.store(false, std::memory_order_release);
    }
  }

  // With each change to max_evicted_seq_ fetch the live snapshots behind it.
  // We use max as the version of snapshots to identify how fresh are the
  // snapshot list. This works because the snapshots are between 0 and
  // max, so the larger the max, the more complete they are.
  SequenceNumber new_snapshots_version = new_max;
  std::vector<SequenceNumber> snapshots;
  bool update_snapshots = false;
  if (new_snapshots_version > snapshots_version_) {
    // This is to avoid updating the snapshots_ if it already updated
    // with a more recent vesion by a concrrent thread
    update_snapshots = true;
    // We only care about snapshots lower then max
    snapshots = GetSnapshotListFromDB(new_max);
  }
  if (update_snapshots) {
    UpdateSnapshots(snapshots, new_snapshots_version);
  }
  while (prev_max < new_max && !max_evicted_seq_.compare_exchange_weak(
                                   prev_max, new_max, std::memory_order_acq_rel,
                                   std::memory_order_relaxed)) {
  };
}

const std::vector<SequenceNumber> WritePreparedTxnDB::GetSnapshotListFromDB(
    SequenceNumber max) {
  InstrumentedMutex(db_impl_->mutex());
  return db_impl_->snapshots().GetAll(nullptr, max);
}

void WritePreparedTxnDB::UpdateSnapshots(
    const std::vector<SequenceNumber>& snapshots,
    const SequenceNumber& version) {
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:start");
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:start");
#ifndef NDEBUG
  size_t sync_i = 0;
#endif
  WriteLock wl(&snapshots_mutex_);
  snapshots_version_ = version;
  // We update the list concurrently with the readers.
  // Both new and old lists are sorted and the new list is subset of the
  // previous list plus some new items. Thus if a snapshot repeats in
  // both new and old lists, it will appear upper in the new list. So if
  // we simply insert the new snapshots in order, if an overwritten item
  // is still valid in the new list is either written to the same place in
  // the array or it is written in a higher palce before it gets
  // overwritten by another item. This guarantess a reader that reads the
  // list bottom-up will eventaully see a snapshot that repeats in the
  // update, either before it gets overwritten by the writer or
  // afterwards.
  size_t i = 0;
  auto it = snapshots.begin();
  for (; it != snapshots.end() && i < SNAPSHOT_CACHE_SIZE; it++, i++) {
    snapshot_cache_[i].store(*it, std::memory_order_release);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:", ++sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:", sync_i);
  }
#ifndef NDEBUG
  // Release the remaining sync points since they are useless given that the
  // reader would also use lock to access snapshots
  for (++sync_i; sync_i <= 10; ++sync_i) {
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:", sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:", sync_i);
  }
#endif
  snapshots_.clear();
  for (; it != snapshots.end(); it++) {
    // Insert them to a vector that is less efficient to access
    // concurrently
    snapshots_.push_back(*it);
  }
  // Update the size at the end. Otherwise a parallel reader might read
  // items that are not set yet.
  snapshots_total_.store(snapshots.size(), std::memory_order_release);
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:p:end");
  TEST_SYNC_POINT("WritePreparedTxnDB::UpdateSnapshots:s:end");
}

void WritePreparedTxnDB::CheckAgainstSnapshots(const CommitEntry& evicted) {
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:start");
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:start");
#ifndef NDEBUG
  size_t sync_i = 0;
#endif
  // First check the snapshot cache that is efficient for concurrent access
  auto cnt = snapshots_total_.load(std::memory_order_acquire);
  // The list might get updated concurrently as we are reading from it. The
  // reader should be able to read all the snapshots that are still valid
  // after the update. Since the survived snapshots are written in a higher
  // place before gets overwritten the reader that reads bottom-up will
  // eventully see it.
  const bool next_is_larger = true;
  SequenceNumber snapshot_seq = kMaxSequenceNumber;
  size_t ip1 = std::min(cnt, SNAPSHOT_CACHE_SIZE);
  for (; 0 < ip1; ip1--) {
    snapshot_seq = snapshot_cache_[ip1 - 1].load(std::memory_order_acquire);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:",
                        ++sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:", sync_i);
    if (!MaybeUpdateOldCommitMap(evicted.prep_seq, evicted.commit_seq,
                                 snapshot_seq, !next_is_larger)) {
      break;
    }
  }
#ifndef NDEBUG
  // Release the remaining sync points before accquiring the lock
  for (++sync_i; sync_i <= 10; ++sync_i) {
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:", sync_i);
    TEST_IDX_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:", sync_i);
  }
#endif
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:p:end");
  TEST_SYNC_POINT("WritePreparedTxnDB::CheckAgainstSnapshots:s:end");
  if (UNLIKELY(SNAPSHOT_CACHE_SIZE < cnt && ip1 == SNAPSHOT_CACHE_SIZE &&
               snapshot_seq < evicted.prep_seq)) {
    // Then access the less efficient list of snapshots_
    ReadLock rl(&snapshots_mutex_);
    // Items could have moved from the snapshots_ to snapshot_cache_ before
    // accquiring the lock. To make sure that we do not miss a valid snapshot,
    // read snapshot_cache_ again while holding the lock.
    for (size_t i = 0; i < SNAPSHOT_CACHE_SIZE; i++) {
      snapshot_seq = snapshot_cache_[i].load(std::memory_order_acquire);
      if (!MaybeUpdateOldCommitMap(evicted.prep_seq, evicted.commit_seq,
                                   snapshot_seq, next_is_larger)) {
        break;
      }
    }
    for (auto snapshot_seq_2 : snapshots_) {
      if (!MaybeUpdateOldCommitMap(evicted.prep_seq, evicted.commit_seq,
                                   snapshot_seq_2, next_is_larger)) {
        break;
      }
    }
  }
}

bool WritePreparedTxnDB::MaybeUpdateOldCommitMap(
    const uint64_t& prep_seq, const uint64_t& commit_seq,
    const uint64_t& snapshot_seq, const bool next_is_larger = true) {
  // If we do not store an entry in old_commit_map we assume it is committed in
  // all snapshots. if commit_seq <= snapshot_seq, it is considered already in
  // the snapshot so we need not to keep the entry around for this snapshot.
  if (commit_seq <= snapshot_seq) {
    // continue the search if the next snapshot could be smaller than commit_seq
    return !next_is_larger;
  }
  // then snapshot_seq < commit_seq
  if (prep_seq <= snapshot_seq) {  // overlapping range
    WriteLock wl(&old_commit_map_mutex_);
    old_commit_map_empty_.store(false, std::memory_order_release);
    old_commit_map_[prep_seq] = commit_seq;
    // Storing once is enough. No need to check it for other snapshots.
    return false;
  }
  // continue the search if the next snapshot could be larger than prep_seq
  return next_is_larger;
}

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
