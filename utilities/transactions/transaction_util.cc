//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/transaction_util.h"

#include <inttypes.h>
#include <string>
#include <vector>

#include "db/db_impl.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/string_util.h"

namespace rocksdb {

Status TransactionUtil::CheckKeyForConflicts(DBImpl* db_impl,
                                             ColumnFamilyHandle* column_family,
                                             const std::string& key,
                                             SequenceNumber key_seq) {
  Status result;

  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* sv = db_impl->GetAndRefSuperVersion(cfd);

  if (sv == nullptr) {
    result = Status::Busy("Could not access column family " +
                          cfh->GetName());
  }

  if (result.ok()) {
    SequenceNumber earliest_seq =
        db_impl->GetEarliestMemTableSequenceNumber(sv, true);

    result = CheckKey(db_impl, sv, earliest_seq, key_seq, key);

    db_impl->ReturnAndCleanupSuperVersion(cfd, sv);
  }

  return result;
}

Status TransactionUtil::CheckKey(DBImpl* db_impl, SuperVersion* sv,
                                 SequenceNumber earliest_seq,
                                 SequenceNumber key_seq,
                                 const std::string& key) {
  Status result;

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
        "Transaction ould not check for conflicts as the MemTable does not "
        "countain a long enough history to check write at SequenceNumber: ",
        ToString(key_seq));

  } else if (key_seq < earliest_seq) {
    // The age of this memtable is too new to use to check for recent
    // writes.
    char msg[255];
    snprintf(msg, sizeof(msg),
             "Transaction could not check for conflicts for opearation at "
             "SequenceNumber %" PRIu64
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

  return result;
}

Status TransactionUtil::CheckKeysForConflicts(DBImpl* db_impl,
                                              TransactionKeyMap* key_map) {
  Status result;

  for (auto& key_map_iter : *key_map) {
    uint32_t cf_id = key_map_iter.first;
    const auto& keys = key_map_iter.second;

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

      result = CheckKey(db_impl, sv, earliest_seq, key_seq, key);

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

Status TransactionUtil::CopyFirstN(size_t num, WriteBatchWithIndex* batch,
                                   WriteBatchWithIndex* new_batch,
                                   DBImpl* db_impl) {
  // Handler for iterating through batch and copying entries to new_batch
  class Handler : public WriteBatch::Handler {
   public:
    WriteBatchWithIndex* batch;
    const size_t limit;
    DBImpl* db_impl;
    size_t seen = 0;
    std::unordered_map<uint32_t, SuperVersion*> super_versions;
    std::unordered_map<uint32_t, ColumnFamilyHandle*> handles;

    Handler(WriteBatchWithIndex* dest, size_t new_limit, DBImpl* db)
        : batch(dest), limit(new_limit), db_impl(db) {}

    ~Handler() {
      for (auto& iter : super_versions) {
        db_impl->ReturnAndCleanupSuperVersionUnlocked(iter.first, iter.second);
      }
    }

    Status GetColumnFamily(uint32_t column_family_id,
                           ColumnFamilyHandle** cfh) {
      // Need to look up ColumnFamilyHandle for this column family id.  Since
      // doing this requires grabbing a mutex, lets only do it once per column
      // family and cache it.
      // In order to ensure that the ColumnFamilyHandle is still valid, we need
      // to hold the superversion.
      const auto& iter = handles.find(column_family_id);
      if (iter == handles.end()) {
        // Don't have ColumnFamilyHandle cached, look it up from the db.
        SuperVersion* sv =
            db_impl->GetAndRefSuperVersionUnlocked(column_family_id);
        if (sv == nullptr) {
          return Status::InvalidArgument(
              "Could not find column family for ID " +
              ToString(column_family_id));
        }
        super_versions.insert({column_family_id, sv});

        *cfh = db_impl->GetColumnFamilyHandleUnlocked(column_family_id);
        if (*cfh == nullptr) {
          return Status::InvalidArgument(
              "Could not find column family handle for ID " +
              ToString(column_family_id));
        }
        handles.insert({column_family_id, *cfh});
      } else {
        *cfh = iter->second;
      }

      return Status::OK();
    }

    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
      if (seen >= limit) {
        // Found the first N entries, return Aborted to stop the Iteration.
        return Status::Aborted();
      }
      ColumnFamilyHandle* cfh = nullptr;
      Status s = GetColumnFamily(column_family_id, &cfh);
      if (s.ok()) {
        batch->Put(cfh, key, value);
      }
      seen++;
      return s;
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      if (seen >= limit) {
        // Found the first N entries, return Aborted to stop the Iteration.
        return Status::Aborted();
      }
      ColumnFamilyHandle* cfh = nullptr;
      Status s = GetColumnFamily(column_family_id, &cfh);
      if (s.ok()) {
        batch->Merge(cfh, key, value);
      }
      seen++;
      return s;
    }
    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {
      if (seen >= limit) {
        // Found the first N entries, return Aborted to stop the Iteration.
        return Status::Aborted();
      }
      ColumnFamilyHandle* cfh = nullptr;
      Status s = GetColumnFamily(column_family_id, &cfh);
      if (s.ok()) {
        batch->Delete(cfh, key);
      }
      seen++;
      return s;
    }

    virtual void LogData(const Slice& blob) override {
      if (seen < limit) {
        batch->PutLogData(blob);
      }
      seen++;
    }
  };

  // Iterating on this handler will add all keys in this batch into a new batch
  // up to
  // the limit.
  Handler handler(new_batch, num, db_impl);
  Status s = batch->GetWriteBatch()->Iterate(&handler);

  if (s.IsAborted()) {
    // Handler returns Aborted when it is done copying to stop the iteration.
    s = Status::OK();
  }

  return s;
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
