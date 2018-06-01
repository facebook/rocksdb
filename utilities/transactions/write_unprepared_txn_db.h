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

class WriteUnpreparedTxnDB : public WritePreparedTxnDB {
  using WritePreparedTxnDB::WritePreparedTxnDB;

  Transaction* BeginTransaction(const WriteOptions& write_options, const TransactionOptions& txn_options,
                                Transaction* old_txn) override;
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
