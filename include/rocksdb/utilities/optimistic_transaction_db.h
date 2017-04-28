//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"

namespace rocksdb {

class Transaction;

// Database with Transaction support.
//
// See optimistic_transaction.h and examples/transaction_example.cc

// Options to use when starting an Optimistic Transaction
struct OptimisticTransactionOptions {
  // Setting set_snapshot=true is the same as calling SetSnapshot().
  bool set_snapshot = false;

  // Should be set if the DB has a non-default comparator.
  // See comment in WriteBatchWithIndex constructor.
  const Comparator* cmp = BytewiseComparator();
};

class OptimisticTransactionDB {
 public:
  // Open an OptimisticTransactionDB similar to DB::Open().
  static Status Open(const Options& options, const std::string& dbname,
                     OptimisticTransactionDB** dbptr);

  static Status Open(const DBOptions& db_options, const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     OptimisticTransactionDB** dbptr);

  virtual ~OptimisticTransactionDB() {}

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
      const OptimisticTransactionOptions& txn_options =
          OptimisticTransactionOptions(),
      Transaction* old_txn = nullptr) = 0;

  // Return the underlying Database that was opened
  virtual DB* GetBaseDB() = 0;

 protected:
  // To Create an OptimisticTransactionDB, call Open()
  explicit OptimisticTransactionDB(DB* db) {}
  OptimisticTransactionDB() {}

 private:
  // No copying allowed
  OptimisticTransactionDB(const OptimisticTransactionDB&);
  void operator=(const OptimisticTransactionDB&);
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
