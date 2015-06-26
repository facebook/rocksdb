//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include "utilities/transactions/optimistic_transaction_impl.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "util/string_util.h"

namespace rocksdb {

struct WriteOptions;

OptimisticTransactionImpl::OptimisticTransactionImpl(
    OptimisticTransactionDB* txn_db, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options)
    : txn_db_(txn_db),
      db_(txn_db->GetBaseDB()),
      write_options_(write_options),
      snapshot_(nullptr),
      write_batch_(txn_options.cmp, 0, true) {
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
      write_options_, write_batch_.GetWriteBatch(), &callback);

  if (s.ok()) {
    tracked_keys_.clear();
    write_batch_.Clear();
  }

  return s;
}

void OptimisticTransactionImpl::Rollback() {
  tracked_keys_.clear();
  write_batch_.Clear();
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
  return write_batch_.GetFromBatchAndDB(db_, read_options, column_family, key,
                                        value);
}

Status OptimisticTransactionImpl::GetForUpdate(
    const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const Slice& key, std::string* value) {
  // Regardless of whether the Get succeeded, track this key.
  RecordOperation(column_family, key);

  return Get(read_options, column_family, key, value);
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
    std::string* value = &(*values)[i];
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

    std::string* value = &(*values)[i];
    stat_list[i] = Get(read_options, column_family[i], keys[i], value);
  }

  return stat_list;
}

void OptimisticTransactionImpl::Put(ColumnFamilyHandle* column_family,
                                    const Slice& key, const Slice& value) {
  RecordOperation(column_family, key);

  write_batch_.Put(column_family, key, value);
}

void OptimisticTransactionImpl::Put(ColumnFamilyHandle* column_family,
                                    const SliceParts& key,
                                    const SliceParts& value) {
  RecordOperation(column_family, key);

  write_batch_.Put(column_family, key, value);
}

void OptimisticTransactionImpl::Merge(ColumnFamilyHandle* column_family,
                                      const Slice& key, const Slice& value) {
  RecordOperation(column_family, key);

  write_batch_.Merge(column_family, key, value);
}

void OptimisticTransactionImpl::Delete(ColumnFamilyHandle* column_family,
                                       const Slice& key) {
  RecordOperation(column_family, key);

  write_batch_.Delete(column_family, key);
}

void OptimisticTransactionImpl::Delete(ColumnFamilyHandle* column_family,
                                       const SliceParts& key) {
  RecordOperation(column_family, key);

  write_batch_.Delete(column_family, key);
}

void OptimisticTransactionImpl::PutUntracked(ColumnFamilyHandle* column_family,
                                             const Slice& key,
                                             const Slice& value) {
  write_batch_.Put(column_family, key, value);
}

void OptimisticTransactionImpl::PutUntracked(ColumnFamilyHandle* column_family,
                                             const SliceParts& key,
                                             const SliceParts& value) {
  write_batch_.Put(column_family, key, value);
}

void OptimisticTransactionImpl::MergeUntracked(
    ColumnFamilyHandle* column_family, const Slice& key, const Slice& value) {
  write_batch_.Merge(column_family, key, value);
}

void OptimisticTransactionImpl::DeleteUntracked(
    ColumnFamilyHandle* column_family, const Slice& key) {
  write_batch_.Delete(column_family, key);
}

void OptimisticTransactionImpl::DeleteUntracked(
    ColumnFamilyHandle* column_family, const SliceParts& key) {
  write_batch_.Delete(column_family, key);
}

void OptimisticTransactionImpl::PutLogData(const Slice& blob) {
  write_batch_.PutLogData(blob);
}

WriteBatchWithIndex* OptimisticTransactionImpl::GetWriteBatch() {
  return &write_batch_;
}

// Returns OK if it is safe to commit this transaction.  Returns Status::Busy
// if there are read or write conflicts that would prevent us from committing OR
// if we can not determine whether there would be any such conflicts.
//
// Should only be called on writer thread.
Status OptimisticTransactionImpl::CheckTransactionForConflicts(DB* db) {
  Status result;

  assert(dynamic_cast<DBImpl*>(db) != nullptr);
  auto db_impl = reinterpret_cast<DBImpl*>(db);

  for (auto& tracked_keys_iter : tracked_keys_) {
    uint32_t cf_id = tracked_keys_iter.first;
    const auto& keys = tracked_keys_iter.second;

    SuperVersion* sv = db_impl->GetAndRefSuperVersion(cf_id);
    if (sv == nullptr) {
      result =
          Status::Busy("Could not access column family " + ToString(cf_id));
      break;
    }

    SequenceNumber earliest_seq =
        db_impl->GetEarliestMemTableSequenceNumber(sv, true);

    // For each of the keys in this transaction, check to see if someone has
    // written to this key since the start of the transaction.
    for (const auto& key_iter : keys) {
      const auto& key = key_iter.first;
      const SequenceNumber key_seq = key_iter.second;

      // Since it would be too slow to check the SST files, we will only use
      // the memtables to check whether there have been any recent writes
      // to this key after it was accessed in this transaction.  But if the
      // Memtables do not contain a long enough history, we must fail the
      // transaction.
      if (earliest_seq == kMaxSequenceNumber) {
        // The age of this memtable is unknown.  Cannot rely on it to check
        // for recent writes.  This error shouldn't happen often in practice as
        // the
        // Memtable should have a valid earliest sequence number except in some
        // corner cases (such as error cases during recovery).
        result = Status::Busy(
            "Could not commit transaction with as the MemTable does not "
            "countain a long enough history to check write at SequenceNumber: ",
            ToString(key_seq));

      } else if (key_seq < earliest_seq) {
        // The age of this memtable is too new to use to check for recent
        // writes.
        char msg[255];
        snprintf(
            msg, sizeof(msg),
            "Could not commit transaction with write at SequenceNumber %" PRIu64
            " as the MemTable only contains changes newer than SequenceNumber "
            "%" PRIu64
            ".  Increasing the value of the "
            "max_write_buffer_number_to_maintain option could reduce the "
            "frequency "
            "of this error.",
            key_seq, earliest_seq);
        result = Status::Busy(msg);
      } else {
        SequenceNumber seq = kMaxSequenceNumber;
        Status s = db_impl->GetLatestSequenceForKeyFromMemtable(sv, key, &seq);
        if (!s.ok()) {
          result = s;
        } else if (seq != kMaxSequenceNumber && seq > key_seq) {
          result = Status::Busy();
        }
      }

      if (!result.ok()) {
        break;
      }
    }

    db_impl->ReturnAndCleanupSuperVersion(cf_id, sv);

    if (!result.ok()) {
      break;
    }
  }

  return result;
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
