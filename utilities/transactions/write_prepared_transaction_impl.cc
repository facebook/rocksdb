//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/write_prepared_transaction_impl.h"

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
#include "utilities/transactions/transaction_impl.h"
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

struct WriteOptions;

WritePreparedTxnImpl::WritePreparedTxnImpl(
    TransactionDB* txn_db, const WriteOptions& write_options,
    const TransactionOptions& txn_options)
    : PessimisticTxn(txn_db, write_options, txn_options) {
  PessimisticTxn::Initialize(txn_options);
}

Status WritePreparedTxnImpl::CommitBatch(WriteBatch* batch) {
  // TODO(myabandeh) Implement this
  throw std::runtime_error("CommitBatch not Implemented");
  return Status::OK();
}

Status WritePreparedTxnImpl::Prepare() {
  // TODO(myabandeh) Implement this
  throw std::runtime_error("Prepare not Implemented");
  return Status::OK();
}

Status WritePreparedTxnImpl::Commit() {
  // TODO(myabandeh) Implement this
  throw std::runtime_error("Commit not Implemented");
  return Status::OK();
}

Status WritePreparedTxnImpl::Rollback() {
  // TODO(myabandeh) Implement this
  throw std::runtime_error("Rollback not Implemented");
  return Status::OK();
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
