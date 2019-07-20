// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <set>

#include "utilities/transactions/write_prepared_txn.h"
#include "utilities/transactions/write_unprepared_txn_db.h"

namespace rocksdb {

class WriteUnpreparedTxnDB;
class WriteUnpreparedTxn;

// WriteUnprepared transactions needs to be able to read their own uncommitted
// writes, and supporting this requires some careful consideration. Because
// writes in the current transaction may be flushed to DB already, we cannot
// rely on the contents of WriteBatchWithIndex to determine whether a key should
// be visible or not, so we have to remember to check the DB for any uncommitted
// keys that should be visible to us. First, we will need to change the seek to
// snapshot logic, to seek to max_visible_seq = max(snap_seq, max_unprep_seq).
// Any key greater than max_visible_seq should not be visible because they
// cannot be unprepared by the current transaction and they are not in its
// snapshot.
//
// When we seek to max_visible_seq, one of these cases will happen:
// 1. We hit a unprepared key from the current transaction.
// 2. We hit a unprepared key from the another transaction.
// 3. We hit a committed key with snap_seq < seq < max_unprep_seq.
// 4. We hit a committed key with seq <= snap_seq.
//
// IsVisibleFullCheck handles all cases correctly.
//
// Other notes:
// Note that max_visible_seq is only calculated once at iterator construction
// time, meaning if the same transaction is adding more unprep seqs through
// writes during iteration, these newer writes may not be visible. This is not a
// problem for MySQL though because it avoids modifying the index as it is
// scanning through it to avoid the Halloween Problem. Instead, it scans the
// index once up front, and modifies based on a temporary copy.
//
// In DBIter, there is a "reseek" optimization if the iterator skips over too
// many keys. However, this assumes that the reseek seeks exactly to the
// required key. In write unprepared, even after seeking directly to
// max_visible_seq, some iteration may be required before hitting a visible key,
// and special precautions must be taken to avoid performing another reseek,
// leading to an infinite loop.
//
class WriteUnpreparedTxnReadCallback : public ReadCallback {
 public:
  WriteUnpreparedTxnReadCallback(WritePreparedTxnDB* db,
                                 SequenceNumber snapshot,
                                 SequenceNumber min_uncommitted,
                                 WriteUnpreparedTxn* txn)
      // Pass our last uncommitted seq as the snapshot to the parent class to
      // ensure that the parent will not prematurely filter out own writes. We
      // will do the exact comparison against snapshots in IsVisibleFullCheck
      // override.
      : ReadCallback(CalcMaxVisibleSeq(txn, snapshot), min_uncommitted),
        db_(db),
        txn_(txn),
        wup_snapshot_(snapshot) {}

  virtual bool IsVisibleFullCheck(SequenceNumber seq) override;

  void Refresh(SequenceNumber seq) override {
    max_visible_seq_ = std::max(max_visible_seq_, seq);
    wup_snapshot_ = seq;
  }

 private:
  static SequenceNumber CalcMaxVisibleSeq(WriteUnpreparedTxn* txn,
                                          SequenceNumber snapshot_seq) {
    SequenceNumber max_unprepared = CalcMaxUnpreparedSequenceNumber(txn);
    return std::max(max_unprepared, snapshot_seq);
  }
  static SequenceNumber CalcMaxUnpreparedSequenceNumber(
      WriteUnpreparedTxn* txn);
  WritePreparedTxnDB* db_;
  WriteUnpreparedTxn* txn_;
  SequenceNumber wup_snapshot_;
};

class WriteUnpreparedTxn : public WritePreparedTxn {
 public:
  WriteUnpreparedTxn(WriteUnpreparedTxnDB* db,
                     const WriteOptions& write_options,
                     const TransactionOptions& txn_options);

  virtual ~WriteUnpreparedTxn();

  using TransactionBaseImpl::Put;
  virtual Status Put(ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& value,
                     const bool assume_tracked = false) override;
  virtual Status Put(ColumnFamilyHandle* column_family, const SliceParts& key,
                     const SliceParts& value,
                     const bool assume_tracked = false) override;

  using TransactionBaseImpl::Merge;
  virtual Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value,
                       const bool assume_tracked = false) override;

  using TransactionBaseImpl::Delete;
  virtual Status Delete(ColumnFamilyHandle* column_family, const Slice& key,
                        const bool assume_tracked = false) override;
  virtual Status Delete(ColumnFamilyHandle* column_family,
                        const SliceParts& key,
                        const bool assume_tracked = false) override;

  using TransactionBaseImpl::SingleDelete;
  virtual Status SingleDelete(ColumnFamilyHandle* column_family,
                              const Slice& key,
                              const bool assume_tracked = false) override;
  virtual Status SingleDelete(ColumnFamilyHandle* column_family,
                              const SliceParts& key,
                              const bool assume_tracked = false) override;

  virtual Status RebuildFromWriteBatch(WriteBatch*) override;

  const std::map<SequenceNumber, size_t>& GetUnpreparedSequenceNumbers();

 protected:
  void Initialize(const TransactionOptions& txn_options) override;

  Status PrepareInternal() override;

  Status CommitWithoutPrepareInternal() override;
  Status CommitInternal() override;

  Status RollbackInternal() override;

  void Clear() override;

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

 private:
  friend class WriteUnpreparedTransactionTest_ReadYourOwnWrite_Test;
  friend class WriteUnpreparedTransactionTest_RecoveryTest_Test;
  friend class WriteUnpreparedTransactionTest_UnpreparedBatch_Test;
  friend class WriteUnpreparedTxnDB;

  Status MaybeFlushWriteBatchToDB();
  Status FlushWriteBatchToDB(bool prepared);
  Status HandleWrite(std::function<Status()> do_write);

  // For write unprepared, we check on every writebatch append to see if
  // max_write_batch_size_ has been exceeded, and then call
  // FlushWriteBatchToDB if so. This logic is encapsulated in
  // MaybeFlushWriteBatchToDB.
  size_t max_write_batch_size_;
  WriteUnpreparedTxnDB* wupt_db_;

  // Ordered list of unprep_seq sequence numbers that we have already written
  // to DB.
  //
  // This maps unprep_seq => prepare_batch_cnt for each unprepared batch
  // written by this transaction.
  //
  // Note that this contains both prepared and unprepared batches, since they
  // are treated similarily in prepare heap/commit map, so it simplifies the
  // commit callbacks.
  std::map<SequenceNumber, size_t> unprep_seqs_;

  // Recovered transactions have tracked_keys_ populated, but are not actually
  // locked for efficiency reasons. For recovered transactions, skip unlocking
  // keys when transaction ends.
  bool recovered_txn_;

  // Track the largest sequence number at which we performed snapshot
  // validation. If snapshot validation was skipped because no snapshot was set,
  // then this is set to kMaxSequenceNumber. This value is useful because it
  // means that for keys that have unprepared seqnos, we can guarantee that no
  // committed keys by other transactions can exist between
  // largest_validated_seq_ and max_unprep_seq. See
  // WriteUnpreparedTxnDB::NewIterator for an explanation for why this is
  // necessary for iterator Prev().
  //
  // Currently this value only increases during the lifetime of a transaction,
  // but in some cases, we should be able to restore the previously largest
  // value when calling RollbackToSavepoint.
  SequenceNumber largest_validated_seq_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
