//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <utility>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/utilities/transaction.h"

// Database with Transaction support.
//
// See transaction.h and examples/transaction_example.cc

namespace rocksdb {

class TransactionDBMutexFactory;

struct TransactionDBOptions {
  // Specifies the maximum number of keys that can be locked at the same time
  // per column family.
  // If the number of locked keys is greater than max_num_locks, transaction
  // writes (or GetForUpdate) will return an error.
  // If this value is not positive, no limit will be enforced.
  int64_t max_num_locks = -1;

  // Increasing this value will increase the concurrency by dividing the lock
  // table (per column family) into more sub-tables, each with their own
  // separate
  // mutex.
  size_t num_stripes = 16;

  // If positive, specifies the default wait timeout in milliseconds when
  // a transaction attempts to lock a key if not specified by
  // TransactionOptions::lock_timeout.
  //
  // If 0, no waiting is done if a lock cannot instantly be acquired.
  // If negative, there is no timeout.  Not using a timeout is not recommended
  // as it can lead to deadlocks.  Currently, there is no deadlock-detection to
  // recover
  // from a deadlock.
  int64_t transaction_lock_timeout = 1000;  // 1 second

  // If positive, specifies the wait timeout in milliseconds when writing a key
  // OUTSIDE of a transaction (ie by calling DB::Put(),Merge(),Delete(),Write()
  // directly).
  // If 0, no waiting is done if a lock cannot instantly be acquired.
  // If negative, there is no timeout and will block indefinitely when acquiring
  // a lock.
  //
  // Not using a timeout can lead to deadlocks.  Currently, there
  // is no deadlock-detection to recover from a deadlock.  While DB writes
  // cannot deadlock with other DB writes, they can deadlock with a transaction.
  // A negative timeout should only be used if all transactions have a small
  // expiration set.
  int64_t default_lock_timeout = 1000;  // 1 second

  // If set, the TransactionDB will use this implemenation of a mutex and
  // condition variable for all transaction locking instead of the default
  // mutex/condvar implementation.
  std::shared_ptr<TransactionDBMutexFactory> custom_mutex_factory;
};

struct TransactionOptions {
  // Setting set_snapshot=true is the same as calling
  // Transaction::SetSnapshot().
  bool set_snapshot = false;

  // Setting to true means that before acquiring locks, this transaction will
  // check if doing so will cause a deadlock. If so, it will return with
  // Status::Busy.  The user should retry their transaction.
  bool deadlock_detect = false;

  // TODO(agiardullo): TransactionDB does not yet support comparators that allow
  // two non-equal keys to be equivalent.  Ie, cmp->Compare(a,b) should only
  // return 0 if
  // a.compare(b) returns 0.


  // If positive, specifies the wait timeout in milliseconds when
  // a transaction attempts to lock a key.
  //
  // If 0, no waiting is done if a lock cannot instantly be acquired.
  // If negative, TransactionDBOptions::transaction_lock_timeout will be used.
  int64_t lock_timeout = -1;

  // Expiration duration in milliseconds.  If non-negative, transactions that
  // last longer than this many milliseconds will fail to commit.  If not set,
  // a forgotten transaction that is never committed, rolled back, or deleted
  // will never relinquish any locks it holds.  This could prevent keys from
  // being written by other writers.
  int64_t expiration = -1;

  // The number of traversals to make during deadlock detection.
  int64_t deadlock_detect_depth = 50;

  // The maximum number of bytes used for the write batch. 0 means no limit.
  size_t max_write_batch_size = 0;
};

struct KeyLockInfo {
  std::string key;
  std::vector<TransactionID> ids;
  bool exclusive;
};

class TransactionDB : public StackableDB {
 public:
  // Open a TransactionDB similar to DB::Open().
  // Internally call PrepareWrap() and WrapDB()
  static Status Open(const Options& options,
                     const TransactionDBOptions& txn_db_options,
                     const std::string& dbname, TransactionDB** dbptr);

  static Status Open(const DBOptions& db_options,
                     const TransactionDBOptions& txn_db_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     TransactionDB** dbptr);
  // The following functions are used to open a TransactionDB internally using
  // an opened DB or StackableDB.
  // 1. Call prepareWrap(), passing an empty std::vector<size_t> to
  // compaction_enabled_cf_indices.
  // 2. Open DB or Stackable DB with db_options and column_families passed to
  // prepareWrap()
  // Note: PrepareWrap() may change parameters, make copies before the
  // invocation if needed.
  // 3. Call Wrap*DB() with compaction_enabled_cf_indices in step 1 and handles
  // of the opened DB/StackableDB in step 2
  static void PrepareWrap(DBOptions* db_options,
                          std::vector<ColumnFamilyDescriptor>* column_families,
                          std::vector<size_t>* compaction_enabled_cf_indices);
  static Status WrapDB(DB* db, const TransactionDBOptions& txn_db_options,
                       const std::vector<size_t>& compaction_enabled_cf_indices,
                       const std::vector<ColumnFamilyHandle*>& handles,
                       TransactionDB** dbptr);
  static Status WrapStackableDB(
      StackableDB* db, const TransactionDBOptions& txn_db_options,
      const std::vector<size_t>& compaction_enabled_cf_indices,
      const std::vector<ColumnFamilyHandle*>& handles, TransactionDB** dbptr);
  virtual ~TransactionDB() {}

  // Starts a new Transaction.
  //
  // Caller is responsible for deleting the returned transaction when no
  // longer needed.
  //
  // If old_txn is not null, BeginTransaction will reuse this Transaction
  // handle instead of allocating a new one.  This is an optimization to avoid
  // extra allocations when repeatedly creating transactions.
  virtual Transaction* BeginTransaction(
      const WriteOptions& write_options,
      const TransactionOptions& txn_options = TransactionOptions(),
      Transaction* old_txn = nullptr) = 0;

  virtual Transaction* GetTransactionByName(const TransactionName& name) = 0;
  virtual void GetAllPreparedTransactions(std::vector<Transaction*>* trans) = 0;

  // Returns set of all locks held.
  //
  // The mapping is column family id -> KeyLockInfo
  virtual std::unordered_multimap<uint32_t, KeyLockInfo>
  GetLockStatusData() = 0;

 protected:
  // To Create an TransactionDB, call Open()
  explicit TransactionDB(DB* db) : StackableDB(db) {}

 private:
  // No copying allowed
  TransactionDB(const TransactionDB&);
  void operator=(const TransactionDB&);
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
