//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

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
#include "rocksdb/status.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/string_util.h"
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
    : db_(txn_db),
      txn_db_impl_(nullptr),
      txn_id_(GenTxnID()),
      write_options_(write_options),
      snapshot_(nullptr),
      cmp_(GetColumnFamilyUserComparator(txn_db->DefaultColumnFamily())),
      write_batch_(new WriteBatchWithIndex(cmp_, 0, true)),
      start_time_(
          txn_options.expiration >= 0 ? db_->GetEnv()->NowMicros() / 1000 : 0),
      expiration_time_(txn_options.expiration >= 0
                           ? start_time_ + txn_options.expiration
                       : 0),
  lock_timeout_(txn_options.lock_timeout) {
  txn_db_impl_ = dynamic_cast<TransactionDBImpl*>(txn_db);
  assert(txn_db_impl_);

  if (lock_timeout_ < 0) {
    // Lock timeout not set, use default
    lock_timeout_ = txn_db_impl_->GetTxnDBOptions().transaction_lock_timeout;
  }

  if (txn_options.set_snapshot) {
    SetSnapshot();
  }
}

TransactionImpl::~TransactionImpl() {
  Cleanup();

  if (snapshot_ != nullptr) {
    db_->ReleaseSnapshot(snapshot_);
  }
}

void TransactionImpl::SetSnapshot() {
  if (snapshot_ != nullptr) {
    db_->ReleaseSnapshot(snapshot_);
  }

  snapshot_ = db_->GetSnapshot();
}

void TransactionImpl::Cleanup() {
  write_batch_->Clear();
  num_entries_ = 0;
  txn_db_impl_->UnLock(this, &tracked_keys_);
  tracked_keys_.clear();
  save_points_.reset(nullptr);
}

bool TransactionImpl::IsExpired() const {
  if (expiration_time_ > 0) {
    if (db_->GetEnv()->NowMicros() >= expiration_time_ * 1000) {
      // Transaction is expired.
      return true;
    }
  }

  return false;
}

Status TransactionImpl::CommitBatch(WriteBatch* batch) {
  TransactionKeyMap keys_to_unlock;

  Status s = LockBatch(batch, &keys_to_unlock);

  if (s.ok()) {
    s = DoCommit(batch);

    txn_db_impl_->UnLock(this, &keys_to_unlock);
  }

  return s;
}

Status TransactionImpl::Commit() {
  Status s = DoCommit(write_batch_->GetWriteBatch());

  Cleanup();

  return s;
}

Status TransactionImpl::DoCommit(WriteBatch* batch) {
  Status s;

  // Do write directly on base db as TransctionDB::Write() would attempt to
  // do conflict checking that we've already done.
  DB* db = db_->GetBaseDB();

  if (expiration_time_ > 0) {
    // We cannot commit a transaction that is expired as its locks might have
    // been released.
    // To avoid race conditions, we need to use a WriteCallback to check the
    // expiration time once we're on the writer thread.
    TransactionCallback callback(this);

    assert(dynamic_cast<DBImpl*>(db) != nullptr);
    auto db_impl = reinterpret_cast<DBImpl*>(db);
    s = db_impl->WriteWithCallback(write_options_, batch, &callback);
  } else {
    s = db->Write(write_options_, batch);
  }

  return s;
}

void TransactionImpl::Rollback() { Cleanup(); }

void TransactionImpl::SetSavePoint() {
  if (num_entries_ > 0) {
    // If transaction is empty, no need to record anything.

    if (save_points_ == nullptr) {
      save_points_.reset(new std::stack<size_t>());
    }
    save_points_->push(num_entries_);
  }
}

void TransactionImpl::RollbackToSavePoint() {
  size_t savepoint_entries = 0;

  if (save_points_ != nullptr && save_points_->size() > 0) {
    savepoint_entries = save_points_->top();
    save_points_->pop();
  }

  assert(savepoint_entries <= num_entries_);

  if (savepoint_entries == num_entries_) {
    // No changes to rollback
  } else if (savepoint_entries == 0) {
    // Rollback everything
    Rollback();
  } else {
    assert(dynamic_cast<DBImpl*>(db_->GetBaseDB()) != nullptr);
    auto db_impl = reinterpret_cast<DBImpl*>(db_->GetBaseDB());

    WriteBatchWithIndex* new_batch = new WriteBatchWithIndex(cmp_, 0, true);
    Status s = TransactionUtil::CopyFirstN(
        savepoint_entries, write_batch_.get(), new_batch, db_impl);
    if (!s.ok()) {
      // TODO:  Should we change this function to return a Status or should we
      // somehow make it so RollbackToSavePoint() can never fail?? Not easy to
      // handle the case where a client accesses a column family that's been
      // dropped.
      // After chatting with Siying, I'm going to send a diff that adds
      // savepoint support in WriteBatchWithIndex and let reviewers decide which
      // approach is cleaner.
      fprintf(stderr, "STATUS: %s \n", s.ToString().c_str());
      delete new_batch;
    } else {
      write_batch_.reset(new_batch);
    }

    num_entries_ = savepoint_entries;
  }
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

      s = txn_db_impl_->TryLock(this, cfh_id, key);
      if (!s.ok()) {
        break;
      }
      (*keys_to_unlock)[cfh_id].insert({std::move(key), kMaxSequenceNumber});
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

Status TransactionImpl::TryLock(ColumnFamilyHandle* column_family,
                                const SliceParts& key, bool check_snapshot) {
  size_t key_size = 0;
  for (int i = 0; i < key.num_parts; ++i) {
    key_size += key.parts[i].size();
  }

  std::string str;
  str.reserve(key_size);

  for (int i = 0; i < key.num_parts; ++i) {
    str.append(key.parts[i].data(), key.parts[i].size());
  }

  return TryLock(column_family, str, check_snapshot);
}

// Attempt to lock this key.
// Returns OK if the key has been successfully locked.  Non-ok, otherwise.
// If check_shapshot is true and this transaction has a snapshot set,
// this key will only be locked if there have been no writes to this key since
// the snapshot time.
Status TransactionImpl::TryLock(ColumnFamilyHandle* column_family,
                                const Slice& key, bool check_snapshot) {
  uint32_t cfh_id = GetColumnFamilyID(column_family);
  std::string key_str = key.ToString();
  bool previously_locked;
  Status s;

  // lock this key if this transactions hasn't already locked it
  auto iter = tracked_keys_[cfh_id].find(key_str);
  if (iter == tracked_keys_[cfh_id].end()) {
    previously_locked = false;

    s = txn_db_impl_->TryLock(this, cfh_id, key_str);

    if (s.ok()) {
      // Record that we've locked this key
      auto result = tracked_keys_[cfh_id].insert({key_str, kMaxSequenceNumber});
      iter = result.first;
    }
  } else {
    previously_locked = true;
  }

  if (s.ok()) {
    // If a snapshot is set, we need to make sure the key hasn't been modified
    // since the snapshot.  This must be done after we locked the key.
    if (!check_snapshot || snapshot_ == nullptr) {
      // Need to remember the earliest sequence number that we know that this
      // key has not been modified after.  This is useful if this same
      // transaction
      // later tries to lock this key again.
      if (iter->second == kMaxSequenceNumber) {
        // Since we haven't checked a snapshot, we only know this key has not
        // been modified since after we locked it.
        iter->second = db_->GetLatestSequenceNumber();
      }
    } else {
      // If the key has been previous validated at a sequence number earlier
      // than the curent snapshot's sequence number, we already know it has not
      // been modified.
      bool already_validated = iter->second <= snapshot_->GetSequenceNumber();

      if (!already_validated) {
        s = CheckKeySequence(column_family, key);

        if (s.ok()) {
          // Record that there have been no writes to this key after this
          // sequence.
          iter->second = snapshot_->GetSequenceNumber();
        } else {
          // Failed to validate key
          if (!previously_locked) {
            // Unlock key we just locked
            txn_db_impl_->UnLock(this, cfh_id, key.ToString());
            tracked_keys_[cfh_id].erase(iter);
          }
        }
      }
    }
  }

  return s;
}

// Return OK() if this key has not been modified more recently than the
// transaction snapshot_.
Status TransactionImpl::CheckKeySequence(ColumnFamilyHandle* column_family,
                                         const Slice& key) {
  Status result;
  if (snapshot_ != nullptr) {
    assert(dynamic_cast<DBImpl*>(db_->GetBaseDB()) != nullptr);
    auto db_impl = reinterpret_cast<DBImpl*>(db_->GetBaseDB());

    ColumnFamilyHandle* cfh = column_family ? column_family :
      db_impl->DefaultColumnFamily();

    result = TransactionUtil::CheckKeyForConflicts(
        db_impl, cfh, key.ToString(),
        snapshot_->GetSequenceNumber());
  }

  return result;
}

Status TransactionImpl::Get(const ReadOptions& read_options,
                            ColumnFamilyHandle* column_family, const Slice& key,
                            std::string* value) {
  return write_batch_->GetFromBatchAndDB(db_, read_options, column_family, key,
                                         value);
}

Status TransactionImpl::GetForUpdate(const ReadOptions& read_options,
                                     ColumnFamilyHandle* column_family,
                                     const Slice& key, std::string* value) {
  Status s = TryLock(column_family, key);

  if (s.ok() && value != nullptr) {
    s = Get(read_options, column_family, key, value);
  }
  return s;
}

std::vector<Status> TransactionImpl::MultiGet(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  size_t num_keys = keys.size();
  values->resize(num_keys);

  std::vector<Status> stat_list(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    std::string* value = values ? &(*values)[i] : nullptr;
    stat_list[i] = Get(read_options, column_family[i], keys[i], value);
  }

  return stat_list;
}

std::vector<Status> TransactionImpl::MultiGetForUpdate(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  // Regardless of whether the MultiGet succeeded, track these keys.
  size_t num_keys = keys.size();
  values->resize(num_keys);

  // Lock all keys
  for (size_t i = 0; i < num_keys; ++i) {
    Status s = TryLock(column_family[i], keys[i]);
    if (!s.ok()) {
      // Fail entire multiget if we cannot lock all keys
      return std::vector<Status>(num_keys, s);
    }
  }

  // TODO(agiardullo): optimize multiget?
  std::vector<Status> stat_list(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    std::string* value = values ? &(*values)[i] : nullptr;
    stat_list[i] = Get(read_options, column_family[i], keys[i], value);
  }

  return stat_list;
}

Iterator* TransactionImpl::GetIterator(const ReadOptions& read_options) {
  Iterator* db_iter = db_->NewIterator(read_options);
  assert(db_iter);

  return write_batch_->NewIteratorWithBase(db_iter);
}

Iterator* TransactionImpl::GetIterator(const ReadOptions& read_options,
                                       ColumnFamilyHandle* column_family) {
  Iterator* db_iter = db_->NewIterator(read_options, column_family);
  assert(db_iter);

  return write_batch_->NewIteratorWithBase(column_family, db_iter);
}

Status TransactionImpl::Put(ColumnFamilyHandle* column_family, const Slice& key,
                            const Slice& value) {
  Status s = TryLock(column_family, key);

  if (s.ok()) {
    write_batch_->Put(column_family, key, value);
    num_entries_++;
  }

  return s;
}

Status TransactionImpl::Put(ColumnFamilyHandle* column_family,
                            const SliceParts& key, const SliceParts& value) {
  Status s = TryLock(column_family, key);

  if (s.ok()) {
    write_batch_->Put(column_family, key, value);
    num_entries_++;
  }

  return s;
}

Status TransactionImpl::Merge(ColumnFamilyHandle* column_family,
                              const Slice& key, const Slice& value) {
  Status s = TryLock(column_family, key);

  if (s.ok()) {
    write_batch_->Merge(column_family, key, value);
    num_entries_++;
  }

  return s;
}

Status TransactionImpl::Delete(ColumnFamilyHandle* column_family,
                               const Slice& key) {
  Status s = TryLock(column_family, key);

  if (s.ok()) {
    write_batch_->Delete(column_family, key);
    num_entries_++;
  }

  return s;
}

Status TransactionImpl::Delete(ColumnFamilyHandle* column_family,
                               const SliceParts& key) {
  Status s = TryLock(column_family, key);

  if (s.ok()) {
    write_batch_->Delete(column_family, key);
    num_entries_++;
  }

  return s;
}

Status TransactionImpl::PutUntracked(ColumnFamilyHandle* column_family,
                                     const Slice& key, const Slice& value) {
  // Even though we do not care about doing conflict checking for this write,
  // we still need to take a lock to make sure we do not cause a conflict with
  // some other write.  However, we do not need to check if there have been
  // any writes since this transaction's snapshot.
  bool check_snapshot = false;

  // TODO(agiardullo): could optimize by supporting shared txn locks in the
  // future
  Status s = TryLock(column_family, key, check_snapshot);

  if (s.ok()) {
    write_batch_->Put(column_family, key, value);
    num_entries_++;
  }

  return s;
}

Status TransactionImpl::PutUntracked(ColumnFamilyHandle* column_family,
                                     const SliceParts& key,
                                     const SliceParts& value) {
  bool check_snapshot = false;
  Status s = TryLock(column_family, key, check_snapshot);

  if (s.ok()) {
    write_batch_->Put(column_family, key, value);
    num_entries_++;
  }

  return s;
}

Status TransactionImpl::MergeUntracked(ColumnFamilyHandle* column_family,
                                       const Slice& key, const Slice& value) {
  bool check_snapshot = false;
  Status s = TryLock(column_family, key, check_snapshot);

  if (s.ok()) {
    write_batch_->Merge(column_family, key, value);
    num_entries_++;
  }

  return s;
}

Status TransactionImpl::DeleteUntracked(ColumnFamilyHandle* column_family,
                                        const Slice& key) {
  bool check_snapshot = false;
  Status s = TryLock(column_family, key, check_snapshot);

  if (s.ok()) {
    write_batch_->Delete(column_family, key);
    num_entries_++;
  }

  return s;
}

Status TransactionImpl::DeleteUntracked(ColumnFamilyHandle* column_family,
                                        const SliceParts& key) {
  bool check_snapshot = false;
  Status s = TryLock(column_family, key, check_snapshot);

  if (s.ok()) {
    write_batch_->Delete(column_family, key);
    num_entries_++;
  }

  return s;
}

void TransactionImpl::PutLogData(const Slice& blob) {
  write_batch_->PutLogData(blob);
  num_entries_++;
}

WriteBatchWithIndex* TransactionImpl::GetWriteBatch() {
  return write_batch_.get();
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
