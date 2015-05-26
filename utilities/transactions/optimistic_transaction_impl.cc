//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include "utilities/transactions/optimistic_transaction_impl.h"

#include <algorithm>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "util/string_util.h"
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

struct WriteOptions;

OptimisticTransactionImpl::OptimisticTransactionImpl(
    OptimisticTransactionDB* txn_db, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options)
    : txn_db_(txn_db),
      db_(txn_db->GetBaseDB()),
      write_options_(write_options),
      snapshot_(nullptr),
      cmp_(txn_options.cmp),
      write_batch_(new WriteBatchWithIndex(txn_options.cmp, 0, true)) {
  if (txn_options.set_snapshot) {
    SetSnapshot();
  } else {
    start_sequence_number_ = db_->GetLatestSequenceNumber();
  }
}

OptimisticTransactionImpl::~OptimisticTransactionImpl() {
  tracked_keys_.clear();
  if (snapshot_ != nullptr) {
    db_->ReleaseSnapshot(snapshot_);
  }
}

void OptimisticTransactionImpl::SetSnapshot() {
  if (snapshot_ != nullptr) {
    db_->ReleaseSnapshot(snapshot_);
  }

  snapshot_ = db_->GetSnapshot();
  start_sequence_number_ = snapshot_->GetSequenceNumber();
}

Status OptimisticTransactionImpl::Commit() {
  // Set up callback which will call CheckTransactionForConflicts() to
  // check whether this transaction is safe to be committed.
  OptimisticTransactionCallback callback(this);

  DBImpl* db_impl = dynamic_cast<DBImpl*>(db_->GetRootDB());
  if (db_impl == nullptr) {
    // This should only happen if we support creating transactions from
    // a StackableDB and someone overrides GetRootDB().
    return Status::InvalidArgument(
        "DB::GetRootDB() returned an unexpected DB class");
  }

  Status s = db_impl->WriteWithCallback(
      write_options_, write_batch_->GetWriteBatch(), &callback);

  if (s.ok()) {
    tracked_keys_.clear();
    write_batch_->Clear();
    num_entries_ = 0;
  }

  return s;
}

void OptimisticTransactionImpl::Rollback() {
  tracked_keys_.clear();
  write_batch_->Clear();
  num_entries_ = 0;
}

void OptimisticTransactionImpl::SetSavePoint() {
  if (num_entries_ > 0) {
    // If transaction is empty, no need to record anything.

    if (save_points_ == nullptr) {
      save_points_.reset(new std::stack<size_t>());
    }
    save_points_->push(num_entries_);
  }
}

void OptimisticTransactionImpl::RollbackToSavePoint() {
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
    DBImpl* db_impl = dynamic_cast<DBImpl*>(db_->GetRootDB());
    assert(db_impl);

    WriteBatchWithIndex* new_batch = new WriteBatchWithIndex(cmp_, 0, true);
    Status s = TransactionUtil::CopyFirstN(
        savepoint_entries, write_batch_.get(), new_batch, db_impl);

    if (!s.ok()) {
      // TODO:  Should we change this function to return a Status or should we
      // somehow make it
      // so RollbackToSavePoint() can never fail??
      // Consider moving this functionality into WriteBatchWithIndex
      fprintf(stderr, "STATUS: %s \n", s.ToString().c_str());
      delete new_batch;
    } else {
      write_batch_.reset(new_batch);
    }

    num_entries_ = savepoint_entries;
  }
}

// Record this key so that we can check it for conflicts at commit time.
void OptimisticTransactionImpl::RecordOperation(
    ColumnFamilyHandle* column_family, const Slice& key) {
  uint32_t cfh_id = GetColumnFamilyID(column_family);

  SequenceNumber seq;
  if (snapshot_) {
    seq = start_sequence_number_;
  } else {
    seq = db_->GetLatestSequenceNumber();
  }

  std::string key_str = key.ToString();

  auto iter = tracked_keys_[cfh_id].find(key_str);
  if (iter == tracked_keys_[cfh_id].end()) {
    // key not yet seen, store it.
    tracked_keys_[cfh_id].insert({std::move(key_str), seq});
  } else {
    SequenceNumber old_seq = iter->second;
    if (seq < old_seq) {
      // Snapshot has changed since we last saw this key, need to
      // store the earliest seen sequence number.
      tracked_keys_[cfh_id][key_str] = seq;
    }
  }
}

void OptimisticTransactionImpl::RecordOperation(
    ColumnFamilyHandle* column_family, const SliceParts& key) {
  size_t key_size = 0;
  for (int i = 0; i < key.num_parts; ++i) {
    key_size += key.parts[i].size();
  }

  std::string str;
  str.reserve(key_size);

  for (int i = 0; i < key.num_parts; ++i) {
    str.append(key.parts[i].data(), key.parts[i].size());
  }

  RecordOperation(column_family, str);
}

Status OptimisticTransactionImpl::Get(const ReadOptions& read_options,
                                      ColumnFamilyHandle* column_family,
                                      const Slice& key, std::string* value) {
  return write_batch_->GetFromBatchAndDB(db_, read_options, column_family, key,
                                         value);
}

Status OptimisticTransactionImpl::GetForUpdate(
    const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const Slice& key, std::string* value) {
  // Regardless of whether the Get succeeded, track this key.
  RecordOperation(column_family, key);

  if (value == nullptr) {
    return Status::OK();
  } else {
    return Get(read_options, column_family, key, value);
  }
}

std::vector<Status> OptimisticTransactionImpl::MultiGet(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  // Regardless of whether the MultiGet succeeded, track these keys.
  size_t num_keys = keys.size();
  values->resize(num_keys);

  // TODO(agiardullo): optimize multiget?
  std::vector<Status> stat_list(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    std::string* value = values ? &(*values)[i] : nullptr;
    stat_list[i] = Get(read_options, column_family[i], keys[i], value);
  }

  return stat_list;
}

std::vector<Status> OptimisticTransactionImpl::MultiGetForUpdate(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  // Regardless of whether the MultiGet succeeded, track these keys.
  size_t num_keys = keys.size();
  values->resize(num_keys);

  // TODO(agiardullo): optimize multiget?
  std::vector<Status> stat_list(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    // Regardless of whether the Get succeeded, track this key.
    RecordOperation(column_family[i], keys[i]);

    std::string* value = values ? &(*values)[i] : nullptr;
    stat_list[i] = Get(read_options, column_family[i], keys[i], value);
  }

  return stat_list;
}

Iterator* OptimisticTransactionImpl::GetIterator(
    const ReadOptions& read_options) {
  Iterator* db_iter = db_->NewIterator(read_options);
  assert(db_iter);

  return write_batch_->NewIteratorWithBase(db_iter);
}

Iterator* OptimisticTransactionImpl::GetIterator(
    const ReadOptions& read_options, ColumnFamilyHandle* column_family) {
  Iterator* db_iter = db_->NewIterator(read_options, column_family);
  assert(db_iter);

  return write_batch_->NewIteratorWithBase(column_family, db_iter);
}

Status OptimisticTransactionImpl::Put(ColumnFamilyHandle* column_family,
                                      const Slice& key, const Slice& value) {
  RecordOperation(column_family, key);

  write_batch_->Put(column_family, key, value);
  num_entries_++;

  return Status::OK();
}

Status OptimisticTransactionImpl::Put(ColumnFamilyHandle* column_family,
                                      const SliceParts& key,
                                      const SliceParts& value) {
  RecordOperation(column_family, key);

  write_batch_->Put(column_family, key, value);
  num_entries_++;

  return Status::OK();
}

Status OptimisticTransactionImpl::Merge(ColumnFamilyHandle* column_family,
                                        const Slice& key, const Slice& value) {
  RecordOperation(column_family, key);

  write_batch_->Merge(column_family, key, value);

  return Status::OK();
}

Status OptimisticTransactionImpl::Delete(ColumnFamilyHandle* column_family,
                                         const Slice& key) {
  RecordOperation(column_family, key);

  write_batch_->Delete(column_family, key);

  return Status::OK();
}

Status OptimisticTransactionImpl::Delete(ColumnFamilyHandle* column_family,
                                         const SliceParts& key) {
  RecordOperation(column_family, key);

  write_batch_->Delete(column_family, key);

  return Status::OK();
}

Status OptimisticTransactionImpl::PutUntracked(
    ColumnFamilyHandle* column_family, const Slice& key, const Slice& value) {
  write_batch_->Put(column_family, key, value);
  num_entries_++;

  return Status::OK();
}

Status OptimisticTransactionImpl::PutUntracked(
    ColumnFamilyHandle* column_family, const SliceParts& key,
    const SliceParts& value) {
  write_batch_->Put(column_family, key, value);
  num_entries_++;

  return Status::OK();
}

Status OptimisticTransactionImpl::MergeUntracked(
    ColumnFamilyHandle* column_family, const Slice& key, const Slice& value) {
  write_batch_->Merge(column_family, key, value);
  num_entries_++;

  return Status::OK();
}

Status OptimisticTransactionImpl::DeleteUntracked(
    ColumnFamilyHandle* column_family, const Slice& key) {
  write_batch_->Delete(column_family, key);
  num_entries_++;

  return Status::OK();
}

Status OptimisticTransactionImpl::DeleteUntracked(
    ColumnFamilyHandle* column_family, const SliceParts& key) {
  write_batch_->Delete(column_family, key);
  num_entries_++;

  return Status::OK();
}

void OptimisticTransactionImpl::PutLogData(const Slice& blob) {
  write_batch_->PutLogData(blob);
  num_entries_++;
}

WriteBatchWithIndex* OptimisticTransactionImpl::GetWriteBatch() {
  return write_batch_.get();
}

// Returns OK if it is safe to commit this transaction.  Returns Status::Busy
// if there are read or write conflicts that would prevent us from committing OR
// if we can not determine whether there would be any such conflicts.
//
// Should only be called on writer thread in order to avoid any race conditions
// in detecting
// write conflicts.
Status OptimisticTransactionImpl::CheckTransactionForConflicts(DB* db) {
  Status result;

  assert(dynamic_cast<DBImpl*>(db) != nullptr);
  auto db_impl = reinterpret_cast<DBImpl*>(db);

  return TransactionUtil::CheckKeysForConflicts(db_impl, &tracked_keys_);
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
