//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/write_unprepared_txn_db.h"
#include "rocksdb/utilities/transaction_db.h"

namespace rocksdb {

Transaction* WriteUnpreparedTxnDB::BeginTransaction(
const WriteOptions& write_options, const TransactionOptions& txn_options,
Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new WriteUnpreparedTxn(this, write_options, txn_options);
  }
}

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
