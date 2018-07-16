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
class WriteUnpreparedTxn;

class WriteUnpreparedTxnReadCallback : public ReadCallback {
 public:
  WriteUnpreparedTxnReadCallback(WritePreparedTxnDB* db,
                                 SequenceNumber snapshot,
                                 SequenceNumber min_uncommitted,
                                 WriteUnpreparedTxn* txn)
      : db_(db),
        snapshot_(snapshot),
        min_uncommitted_(min_uncommitted),
        txn_(txn) {}

  virtual bool IsVisible(SequenceNumber seq) override;
  virtual SequenceNumber MaxUnpreparedSequenceNumber() override;

 private:
  WritePreparedTxnDB* db_;
  SequenceNumber snapshot_;
  SequenceNumber min_uncommitted_;
  WriteUnpreparedTxn* txn_;
};

class WriteUnpreparedTxn : public WritePreparedTxn {
 public:
  WriteUnpreparedTxn(WriteUnpreparedTxnDB* db,
                     const WriteOptions& write_options,
                     const TransactionOptions& txn_options);

  virtual ~WriteUnpreparedTxn() {}

  // Get and GetIterator needs to be overridden so that a ReadCallback to
  // handle read-your-own-write is used.
  using Transaction::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value) override;

  using Transaction::GetIterator;
  virtual Iterator* GetIterator(const ReadOptions& options) override;
  virtual Iterator* GetIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) override;

  const std::map<SequenceNumber, size_t>& GetUnpreparedSequenceNumbers();

 private:
  friend class WriteUnpreparedTransactionTest_ReadYourOwnWrite_Test;
  friend class WriteUnpreparedTxnDB;

  WriteUnpreparedTxnDB* wupt_db_;

  // Ordered list of unprep_seq sequence numbers that we have already written
  // to DB.
  //
  // This maps unprep_seq => prepare_batch_cnt for each prepared batch written
  // by this transactioin.
  std::map<SequenceNumber, size_t> unprep_seqs_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
