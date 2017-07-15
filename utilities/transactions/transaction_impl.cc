//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/transaction_impl.h"

#include <map>
#include <set>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "utilities/transactions/transaction_db_impl.h"
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

struct WriteOptions;

std::atomic<TransactionID> TransactionImpl::txn_id_counter_(1);

TransactionID TransactionImpl::GenTxnID() {
  return txn_id_counter_.fetch_add(1);
}

TransactionImpl::TransactionImpl(TransactionDB* txn_db,
                                 const WriteOptions& write_options,
                                 const TransactionOptions& txn_options)
    : TransactionBaseImpl(txn_db->GetRootDB(), write_options),
      txn_db_impl_(nullptr),
      txn_id_(0),
      waiting_cf_id_(0),
      waiting_key_(nullptr),
      expiration_time_(0),
      lock_timeout_(0),
      deadlock_detect_(false),
      deadlock_detect_depth_(0) {
  txn_db_impl_ = dynamic_cast<TransactionDBImpl*>(txn_db);
  assert(txn_db_impl_);
  db_impl_ = dynamic_cast<DBImpl*>(txn_db->GetRootDB());
  assert(db_impl_);
  Initialize(txn_options);
}

void TransactionImpl::Initialize(const TransactionOptions& txn_options) {
  txn_id_ = GenTxnID();

  txn_state_ = STARTED;

  deadlock_detect_ = txn_options.deadlock_detect;
  deadlock_detect_depth_ = txn_options.deadlock_detect_depth;
  write_batch_.SetMaxBytes(txn_options.max_write_batch_size);

  lock_timeout_ = txn_options.lock_timeout * 1000;
  if (lock_timeout_ < 0) {
    // Lock timeout not set, use default
    lock_timeout_ =
        txn_db_impl_->GetTxnDBOptions().transaction_lock_timeout * 1000;
  }

  if (txn_options.expiration >= 0) {
    expiration_time_ = start_time_ + txn_options.expiration * 1000;
  } else {
    expiration_time_ = 0;
  }

  if (txn_options.set_snapshot) {
    SetSnapshot();
  }

  if (expiration_time_ > 0) {
    txn_db_impl_->InsertExpirableTransaction(txn_id_, this);
  }
}

TransactionImpl::~TransactionImpl() {
  txn_db_impl_->UnLock(this, &GetTrackedKeys());
  if (expiration_time_ > 0) {
    txn_db_impl_->RemoveExpirableTransaction(txn_id_);
  }
  if (!name_.empty() && txn_state_ != COMMITED) {
    txn_db_impl_->UnregisterTransaction(this);
  }
}

void TransactionImpl::Clear() {
  txn_db_impl_->UnLock(this, &GetTrackedKeys());
  TransactionBaseImpl::Clear();
}

void TransactionImpl::Reinitialize(TransactionDB* txn_db,
                                   const WriteOptions& write_options,
                                   const TransactionOptions& txn_options) {
  if (!name_.empty() && txn_state_ != COMMITED) {
    txn_db_impl_->UnregisterTransaction(this);
  }
  TransactionBaseImpl::Reinitialize(txn_db->GetRootDB(), write_options);
  Initialize(txn_options);
}

bool TransactionImpl::IsExpired() const {
  if (expiration_time_ > 0) {
    if (db_->GetEnv()->NowMicros() >= expiration_time_) {
      // Transaction is expired.
      return true;
    }
  }

  return false;
}

Status TransactionImpl::CommitBatch(WriteBatch* batch) {
  TransactionKeyMap keys_to_unlock;
  Status s = LockBatch(batch, &keys_to_unlock);

  if (!s.ok()) {
    return s;
  }

  bool can_commit = false;

  if (IsExpired()) {
    s = Status::Expired();
  } else if (expiration_time_ > 0) {
    TransactionState expected = STARTED;
    can_commit = std::atomic_compare_exchange_strong(&txn_state_, &expected,
                                                     AWAITING_COMMIT);
  } else if (txn_state_ == STARTED) {
    // lock stealing is not a concern
    can_commit = true;
  }

  if (can_commit) {
    txn_state_.store(AWAITING_COMMIT);
    s = db_->Write(write_options_, batch);
    if (s.ok()) {
      txn_state_.store(COMMITED);
    }
  } else if (txn_state_ == LOCKS_STOLEN) {
    s = Status::Expired();
  } else {
    s = Status::InvalidArgument("Transaction is not in state for commit.");
  }

  txn_db_impl_->UnLock(this, &keys_to_unlock);

  return s;
}

Status TransactionImpl::Prepare() {
  Status s;

  if (name_.empty()) {
    return Status::InvalidArgument(
        "Cannot prepare a transaction that has not been named.");
  }

  if (IsExpired()) {
    return Status::Expired();
  }

  bool can_prepare = false;

  if (expiration_time_ > 0) {
    // must concern ourselves with expiraton and/or lock stealing
    // need to compare/exchange bc locks could be stolen under us here
    TransactionState expected = STARTED;
    can_prepare = std::atomic_compare_exchange_strong(&txn_state_, &expected,
                                                      AWAITING_PREPARE);
  } else if (txn_state_ == STARTED) {
    // expiration and lock stealing is not possible
    can_prepare = true;
  }

  if (can_prepare) {
    txn_state_.store(AWAITING_PREPARE);
    // transaction can't expire after preparation
    expiration_time_ = 0;
    WriteOptions write_options = write_options_;
    write_options.disableWAL = false;
    WriteBatchInternal::MarkEndPrepare(GetWriteBatch()->GetWriteBatch(), name_);
    s = db_impl_->WriteImpl(write_options, GetWriteBatch()->GetWriteBatch(),
                            /*callback*/ nullptr, &log_number_, /*log ref*/ 0,
                            /* disable_memtable*/ true);
    if (s.ok()) {
      assert(log_number_ != 0);
      dbimpl_->MarkLogAsContainingPrepSection(log_number_);
      txn_state_.store(PREPARED);
    }
  } else if (txn_state_ == LOCKS_STOLEN) {
    s = Status::Expired();
  } else if (txn_state_ == PREPARED) {
    s = Status::InvalidArgument("Transaction has already been prepared.");
  } else if (txn_state_ == COMMITED) {
    s = Status::InvalidArgument("Transaction has already been committed.");
  } else if (txn_state_ == ROLLEDBACK) {
    s = Status::InvalidArgument("Transaction has already been rolledback.");
  } else {
    s = Status::InvalidArgument("Transaction is not in state for commit.");
  }

  return s;
}

Status TransactionImpl::Commit() {
  Status s;
  bool commit_single = false;
  bool commit_prepared = false;

  if (IsExpired()) {
    return Status::Expired();
  }

  if (expiration_time_ > 0) {
    // we must atomicaly compare and exchange the state here because at
    // this state in the transaction it is possible for another thread
    // to change our state out from under us in the even that we expire and have
    // our locks stolen. In this case the only valid state is STARTED because
    // a state of PREPARED would have a cleared expiration_time_.
    TransactionState expected = STARTED;
    commit_single = std::atomic_compare_exchange_strong(&txn_state_, &expected,
                                                        AWAITING_COMMIT);
    TEST_SYNC_POINT("TransactionTest::ExpirableTransactionDataRace:1");
  } else if (txn_state_ == PREPARED) {
    // expiration and lock stealing is not a concern
    commit_prepared = true;
  } else if (txn_state_ == STARTED) {
    // expiration and lock stealing is not a concern
    commit_single = true;
  }

  if (commit_single) {
    assert(!commit_prepared);
    if (WriteBatchInternal::Count(GetCommitTimeWriteBatch()) > 0) {
      s = Status::InvalidArgument(
          "Commit-time batch contains values that will not be committed.");
    } else {
      txn_state_.store(AWAITING_COMMIT);
      s = db_->Write(write_options_, GetWriteBatch()->GetWriteBatch());
      Clear();
      if (s.ok()) {
        txn_state_.store(COMMITED);
      }
    }
  } else if (commit_prepared) {
    txn_state_.store(AWAITING_COMMIT);

    // We take the commit-time batch and append the Commit marker.
    // The Memtable will ignore the Commit marker in non-recovery mode
    WriteBatch* working_batch = GetCommitTimeWriteBatch();
    WriteBatchInternal::MarkCommit(working_batch, name_);

    // any operations appended to this working_batch will be ignored from WAL
    working_batch->MarkWalTerminationPoint();

    // insert prepared batch into Memtable only skipping WAL.
    // Memtable will ignore BeginPrepare/EndPrepare markers
    // in non recovery mode and simply insert the values
    WriteBatchInternal::Append(working_batch, GetWriteBatch()->GetWriteBatch());

    s = db_impl_->WriteImpl(write_options_, working_batch, nullptr, nullptr,
                            log_number_);
    if (!s.ok()) {
      return s;
    }

    // FindObsoleteFiles must now look to the memtables
    // to determine what prep logs must be kept around,
    // not the prep section heap.
    assert(log_number_ > 0);
    dbimpl_->MarkLogAsHavingPrepSectionFlushed(log_number_);
    txn_db_impl_->UnregisterTransaction(this);

    Clear();
    txn_state_.store(COMMITED);
  } else if (txn_state_ == LOCKS_STOLEN) {
    s = Status::Expired();
  } else if (txn_state_ == COMMITED) {
    s = Status::InvalidArgument("Transaction has already been committed.");
  } else if (txn_state_ == ROLLEDBACK) {
    s = Status::InvalidArgument("Transaction has already been rolledback.");
  } else {
    s = Status::InvalidArgument("Transaction is not in state for commit.");
  }

  return s;
}

Status TransactionImpl::Rollback() {
  Status s;
  if (txn_state_ == PREPARED) {
    WriteBatch rollback_marker;
    WriteBatchInternal::MarkRollback(&rollback_marker, name_);
    txn_state_.store(AWAITING_ROLLBACK);
    s = db_impl_->WriteImpl(write_options_, &rollback_marker);
    if (s.ok()) {
      // we do not need to keep our prepared section around
      assert(log_number_ > 0);
      dbimpl_->MarkLogAsHavingPrepSectionFlushed(log_number_);
      Clear();
      txn_state_.store(ROLLEDBACK);
    }
  } else if (txn_state_ == STARTED) {
    // prepare couldn't have taken place
    Clear();
  } else if (txn_state_ == COMMITED) {
    s = Status::InvalidArgument("This transaction has already been committed.");
  } else {
    s = Status::InvalidArgument(
        "Two phase transaction is not in state for rollback.");
  }

  return s;
}

Status TransactionImpl::RollbackToSavePoint() {
  if (txn_state_ != STARTED) {
    return Status::InvalidArgument("Transaction is beyond state for rollback.");
  }

  // Unlock any keys locked since last transaction
  const std::unique_ptr<TransactionKeyMap>& keys =
      GetTrackedKeysSinceSavePoint();

  if (keys) {
    txn_db_impl_->UnLock(this, keys.get());
  }

  return TransactionBaseImpl::RollbackToSavePoint();
}

// Lock all keys in this batch.
// On success, caller should unlock keys_to_unlock
Status TransactionImpl::LockBatch(WriteBatch* batch,
                                  TransactionKeyMap* keys_to_unlock) {
  class Handler : public WriteBatch::Handler {
   public:
    // Sorted map of column_family_id to sorted set of keys.
    // Since LockBatch() always locks keys in sorted order, it cannot deadlock
    // with itself.  We're not using a comparator here since it doesn't matter
    // what the sorting is as long as it's consistent.
    std::map<uint32_t, std::set<std::string>> keys_;

    Handler() {}

    void RecordKey(uint32_t column_family_id, const Slice& key) {
      std::string key_str = key.ToString();

      auto iter = (keys_)[column_family_id].find(key_str);
      if (iter == (keys_)[column_family_id].end()) {
        // key not yet seen, store it.
        (keys_)[column_family_id].insert({std::move(key_str)});
      }
    }

    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
      RecordKey(column_family_id, key);
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      RecordKey(column_family_id, key);
      return Status::OK();
    }
    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {
      RecordKey(column_family_id, key);
      return Status::OK();
    }
  };

  // Iterating on this handler will add all keys in this batch into keys
  Handler handler;
  batch->Iterate(&handler);

  Status s;

  // Attempt to lock all keys
  for (const auto& cf_iter : handler.keys_) {
    uint32_t cfh_id = cf_iter.first;
    auto& cfh_keys = cf_iter.second;

    for (const auto& key_iter : cfh_keys) {
      const std::string& key = key_iter;

      s = txn_db_impl_->TryLock(this, cfh_id, key, true /* exclusive */);
      if (!s.ok()) {
        break;
      }
      TrackKey(keys_to_unlock, cfh_id, std::move(key), kMaxSequenceNumber,
               false, true /* exclusive */);
    }

    if (!s.ok()) {
      break;
    }
  }

  if (!s.ok()) {
    txn_db_impl_->UnLock(this, keys_to_unlock);
  }

  return s;
}

// Attempt to lock this key.
// Returns OK if the key has been successfully locked.  Non-ok, otherwise.
// If check_shapshot is true and this transaction has a snapshot set,
// this key will only be locked if there have been no writes to this key since
// the snapshot time.
Status TransactionImpl::TryLock(ColumnFamilyHandle* column_family,
                                const Slice& key, bool read_only,
                                bool exclusive, bool untracked) {
  uint32_t cfh_id = GetColumnFamilyID(column_family);
  std::string key_str = key.ToString();
  bool previously_locked;
  bool lock_upgrade = false;
  Status s;

  // lock this key if this transactions hasn't already locked it
  SequenceNumber current_seqno = kMaxSequenceNumber;
  SequenceNumber new_seqno = kMaxSequenceNumber;

  const auto& tracked_keys = GetTrackedKeys();
  const auto tracked_keys_cf = tracked_keys.find(cfh_id);
  if (tracked_keys_cf == tracked_keys.end()) {
    previously_locked = false;
  } else {
    auto iter = tracked_keys_cf->second.find(key_str);
    if (iter == tracked_keys_cf->second.end()) {
      previously_locked = false;
    } else {
      if (!iter->second.exclusive && exclusive) {
        lock_upgrade = true;
      }
      previously_locked = true;
      current_seqno = iter->second.seq;
    }
  }

  // Lock this key if this transactions hasn't already locked it or we require
  // an upgrade.
  if (!previously_locked || lock_upgrade) {
    s = txn_db_impl_->TryLock(this, cfh_id, key_str, exclusive);
  }

  SetSnapshotIfNeeded();

  // Even though we do not care about doing conflict checking for this write,
  // we still need to take a lock to make sure we do not cause a conflict with
  // some other write.  However, we do not need to check if there have been
  // any writes since this transaction's snapshot.
  // TODO(agiardullo): could optimize by supporting shared txn locks in the
  // future
  if (untracked || snapshot_ == nullptr) {
    // Need to remember the earliest sequence number that we know that this
    // key has not been modified after.  This is useful if this same
    // transaction
    // later tries to lock this key again.
    if (current_seqno == kMaxSequenceNumber) {
      // Since we haven't checked a snapshot, we only know this key has not
      // been modified since after we locked it.
      new_seqno = db_->GetLatestSequenceNumber();
    } else {
      new_seqno = current_seqno;
    }
  } else {
    // If a snapshot is set, we need to make sure the key hasn't been modified
    // since the snapshot.  This must be done after we locked the key.
    if (s.ok()) {
      s = ValidateSnapshot(column_family, key, current_seqno, &new_seqno);

      if (!s.ok()) {
        // Failed to validate key
        if (!previously_locked) {
          // Unlock key we just locked
          if (lock_upgrade) {
            s = txn_db_impl_->TryLock(this, cfh_id, key_str,
                                      false /* exclusive */);
            assert(s.ok());
          } else {
            txn_db_impl_->UnLock(this, cfh_id, key.ToString());
          }
        }
      }
    }
  }

  if (s.ok()) {
    // Let base class know we've conflict checked this key.
    TrackKey(cfh_id, key_str, new_seqno, read_only, exclusive);
  }

  return s;
}

// Return OK() if this key has not been modified more recently than the
// transaction snapshot_.
Status TransactionImpl::ValidateSnapshot(ColumnFamilyHandle* column_family,
                                         const Slice& key,
                                         SequenceNumber prev_seqno,
                                         SequenceNumber* new_seqno) {
  assert(snapshot_);

  SequenceNumber seq = snapshot_->GetSequenceNumber();
  if (prev_seqno <= seq) {
    // If the key has been previous validated at a sequence number earlier
    // than the curent snapshot's sequence number, we already know it has not
    // been modified.
    return Status::OK();
  }

  *new_seqno = seq;

  assert(dynamic_cast<DBImpl*>(db_) != nullptr);
  auto db_impl = reinterpret_cast<DBImpl*>(db_);

  ColumnFamilyHandle* cfh =
      column_family ? column_family : db_impl->DefaultColumnFamily();

  return TransactionUtil::CheckKeyForConflicts(db_impl, cfh, key.ToString(),
                                               snapshot_->GetSequenceNumber(),
                                               false /* cache_only */);
}

bool TransactionImpl::TryStealingLocks() {
  assert(IsExpired());
  TransactionState expected = STARTED;
  return std::atomic_compare_exchange_strong(&txn_state_, &expected,
                                             LOCKS_STOLEN);
}

void TransactionImpl::UnlockGetForUpdate(ColumnFamilyHandle* column_family,
                                         const Slice& key) {
  txn_db_impl_->UnLock(this, GetColumnFamilyID(column_family), key.ToString());
}

Status TransactionImpl::SetName(const TransactionName& name) {
  Status s;
  if (txn_state_ == STARTED) {
    if (name_.length()) {
      s = Status::InvalidArgument("Transaction has already been named.");
    } else if (txn_db_impl_->GetTransactionByName(name) != nullptr) {
      s = Status::InvalidArgument("Transaction name must be unique.");
    } else if (name.length() < 1 || name.length() > 512) {
      s = Status::InvalidArgument(
          "Transaction name length must be between 1 and 512 chars.");
    } else {
      name_ = name;
      txn_db_impl_->RegisterTransaction(this);
    }
  } else {
    s = Status::InvalidArgument("Transaction is beyond state for naming.");
  }
  return s;
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
