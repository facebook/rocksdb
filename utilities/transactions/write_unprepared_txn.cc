//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/write_unprepared_txn.h"
#include "db/db_impl.h"
#include "util/cast_util.h"
#include "utilities/transactions/write_unprepared_txn_db.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

namespace rocksdb {

WriteUnpreparedTxn::WriteUnpreparedTxn(WriteUnpreparedTxnDB* txn_db,
                                       const WriteOptions& write_options,
                                       const TransactionOptions& txn_options)
    : WritePreparedTxn(txn_db, write_options, txn_options), wupt_db_(txn_db) {}

Iterator* WriteUnpreparedTxn::GetIterator(const ReadOptions& options) {
  return GetIterator(options, wupt_db_->DefaultColumnFamily());
}

Iterator* WriteUnpreparedTxn::GetIterator(const ReadOptions& options,
                                          ColumnFamilyHandle* column_family) {
  // Make sure to get iterator from WriteUnprepareTxnDB, not the root db.
  Iterator* db_iter = wupt_db_->NewIterator(options, column_family, this);
  assert(db_iter);

  return write_batch_.NewIteratorWithBase(column_family, db_iter);
}

const std::map<SequenceNumber, size_t>&
WriteUnpreparedTxn::GetUnpreparedSequenceNumbers() {
  return unprep_seqs_;
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
