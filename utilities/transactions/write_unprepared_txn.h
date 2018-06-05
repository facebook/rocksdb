// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include "utilities/transactions/write_prepared_txn.h"
#include "utilities/transactions/write_unprepared_txn_db.h"

namespace rocksdb {

class WriteUnpreparedTxnDB;

class WriteUnpreparedTxn : public WritePreparedTxn {
 public:
  WriteUnpreparedTxn(WriteUnpreparedTxnDB* db,
                     const WriteOptions& write_options,
                     const TransactionOptions& txn_options);

  virtual ~WriteUnpreparedTxn() {}

  using Transaction::GetIterator;
  virtual Iterator* GetIterator(const ReadOptions& options) override;
  virtual Iterator* GetIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) override;

  const std::map<SequenceNumber, size_t>& GetUnpreparedSequenceNumbers();

 private:
  friend class WriteUnpreparedTransactionTest_ReadYourOwnWrite_Test;

  WriteUnpreparedTxnDB* wupt_db_;

  // List of unprep_seq sequence numbers that we have already written to DB.
  std::map<SequenceNumber, size_t> unprep_seqs_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
