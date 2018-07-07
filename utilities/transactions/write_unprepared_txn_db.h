//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/write_prepared_txn_db.h"
#include "utilities/transactions/write_unprepared_txn.h"

namespace rocksdb {

class WriteUnpreparedTxn;

class WriteUnpreparedTxnDB : public WritePreparedTxnDB {
 public:
  using WritePreparedTxnDB::WritePreparedTxnDB;

  Status Initialize(const std::vector<size_t>& compaction_enabled_cf_indices,
                    const std::vector<ColumnFamilyHandle*>& handles) override;

  Transaction* BeginTransaction(const WriteOptions& write_options,
                                const TransactionOptions& txn_options,
                                Transaction* old_txn) override;

  // Struct to hold ownership of snapshot and read callback for cleanup.
  struct IteratorState;

  using WritePreparedTxnDB::NewIterator;
  Iterator* NewIterator(const ReadOptions& options,
                        ColumnFamilyHandle* column_family,
                        WriteUnpreparedTxn* txn);

 private:
  Status RollbackRecoveredTransaction(const DBImpl::RecoveredTransaction* rtxn);
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
