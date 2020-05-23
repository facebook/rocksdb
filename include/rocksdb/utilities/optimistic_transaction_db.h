//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/stackable_db.h"

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

enum class OccValidationPolicy {
  // Validate serially at commit stage, AFTER entering the write-group.
  // Isolation validation is processed single-threaded(since in the
  // write-group).
  // May suffer from high mutex contention, as per this link:
  // https://github.com/facebook/rocksdb/issues/4402
  kValidateSerial = 0,
  // Validate parallelly before commit stage, BEFORE entering the write-group to
  // reduce mutex contention. Each txn acquires locks for its write-set
  // records in some well-defined order.
  kValidateParallel = 1
};

struct OptimisticTransactionDBOptions {
  OccValidationPolicy validate_policy = OccValidationPolicy::kValidateParallel;

  // works only if validate_policy == OccValidationPolicy::kValidateParallel
  uint32_t occ_lock_buckets = (1 << 20);
};

class OptimisticTransactionDB : public StackableDB {
 public:
  // Open an OptimisticTransactionDB similar to DB::Open().
  static Status Open(const Options& options, const std::string& dbname,
                     OptimisticTransactionDB** dbptr);

  static Status Open(const DBOptions& db_options, const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     OptimisticTransactionDB** dbptr);

  static Status Open(const DBOptions& db_options,
                     const OptimisticTransactionDBOptions& occ_options,
                     const std::string& dbname,
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

  OptimisticTransactionDB(const OptimisticTransactionDB&) = delete;
  void operator=(const OptimisticTransactionDB&) = delete;

 protected:
  // To Create an OptimisticTransactionDB, call Open()
  explicit OptimisticTransactionDB(DB* db) : StackableDB(db) {}
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
