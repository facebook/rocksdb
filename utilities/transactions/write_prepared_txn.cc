//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/write_prepared_txn.h"

#include <map>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/transaction_db.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/pessimistic_transaction.h"

namespace rocksdb {

struct WriteOptions;

WritePreparedTxn::WritePreparedTxn(
    TransactionDB* txn_db, const WriteOptions& write_options,
    const TransactionOptions& txn_options)
    : PessimisticTransaction(txn_db, write_options, txn_options) {
  PessimisticTransaction::Initialize(txn_options);
}

Status WritePreparedTxn::CommitBatch(WriteBatch* /* unused */) {
  // TODO(myabandeh) Implement this
  throw std::runtime_error("CommitBatch not Implemented");
  return Status::OK();
}

Status WritePreparedTxn::PrepareInternal() {
  // TODO(myabandeh) Implement this
  throw std::runtime_error("Prepare not Implemented");
  return Status::OK();
}

Status WritePreparedTxn::CommitWithoutPrepareInternal() {
  // TODO(myabandeh) Implement this
  throw std::runtime_error("Commit not Implemented");
  return Status::OK();
}

Status WritePreparedTxn::CommitInternal() {
  // TODO(myabandeh) Implement this
  throw std::runtime_error("Commit not Implemented");
  return Status::OK();
}

Status WritePreparedTxn::Rollback() {
  // TODO(myabandeh) Implement this
  throw std::runtime_error("Rollback not Implemented");
  return Status::OK();
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
