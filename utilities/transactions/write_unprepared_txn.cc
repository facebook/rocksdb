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

bool WriteUnpreparedTxnReadCallback::IsVisible(SequenceNumber seq) {
  auto unprep_seqs = txn_->GetUnpreparedSequenceNumbers();

  // Since unprep_seqs maps prep_seq => prepare_batch_cnt, to check if seq is
  // in unprep_seqs, we have to check if seq is equal to prep_seq or any of
  // the prepare_batch_cnt seq nums after it.
  //
  // TODO(lth): Can be optimized with std::lower_bound if unprep_seqs is
  // large.
  for (const auto& it : unprep_seqs) {
    if (it.first <= seq && seq < it.first + it.second) {
      return true;
    }
  }

  return db_->IsInSnapshot(seq, snapshot_, min_uncommitted_);
}

SequenceNumber WriteUnpreparedTxnReadCallback::MaxUnpreparedSequenceNumber() {
  auto unprep_seqs = txn_->GetUnpreparedSequenceNumbers();
  if (unprep_seqs.size()) {
    return unprep_seqs.rbegin()->first + unprep_seqs.rbegin()->second - 1;
  }

  return 0;
}

WriteUnpreparedTxn::WriteUnpreparedTxn(WriteUnpreparedTxnDB* txn_db,
                                       const WriteOptions& write_options,
                                       const TransactionOptions& txn_options)
    : WritePreparedTxn(txn_db, write_options, txn_options), wupt_db_(txn_db) {}

Status WriteUnpreparedTxn::Get(const ReadOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, PinnableSlice* value) {
  auto snapshot = options.snapshot;
  auto snap_seq =
      snapshot != nullptr ? snapshot->GetSequenceNumber() : kMaxSequenceNumber;
  SequenceNumber min_uncommitted = 0;  // by default disable the optimization
  if (snapshot != nullptr) {
    min_uncommitted =
        static_cast_with_check<const SnapshotImpl, const Snapshot>(snapshot)
            ->min_uncommitted_;
  }

  WriteUnpreparedTxnReadCallback callback(wupt_db_, snap_seq, min_uncommitted,
                                          this);
  return write_batch_.GetFromBatchAndDB(db_, options, column_family, key, value,
                                        &callback);
}

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
