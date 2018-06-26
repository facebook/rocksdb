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
#include "util/cast_util.h"

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

// Struct to hold ownership of snapshot and read callback for iterator cleanup.
struct WriteUnpreparedTxnDB::IteratorState {
  IteratorState(WritePreparedTxnDB* txn_db, SequenceNumber sequence,
                std::shared_ptr<ManagedSnapshot> s,
                SequenceNumber min_uncommitted, WriteUnpreparedTxn* txn)
      : callback(txn_db, sequence, min_uncommitted, txn), snapshot(s) {}

  WriteUnpreparedTxnReadCallback callback;
  std::shared_ptr<ManagedSnapshot> snapshot;
};

namespace {
static void CleanupWriteUnpreparedTxnDBIterator(void* arg1, void* /*arg2*/) {
  delete reinterpret_cast<WriteUnpreparedTxnDB::IteratorState*>(arg1);
}
}  // anonymous namespace

Iterator* WriteUnpreparedTxnDB::NewIterator(const ReadOptions& options,
                                            ColumnFamilyHandle* column_family,
                                            WriteUnpreparedTxn* txn) {
  // TODO(lth): Refactor so that this logic is shared with WritePrepared.
  constexpr bool ALLOW_BLOB = true;
  constexpr bool ALLOW_REFRESH = true;
  std::shared_ptr<ManagedSnapshot> own_snapshot = nullptr;
  SequenceNumber snapshot_seq;
  SequenceNumber min_uncommitted = 0;
  if (options.snapshot != nullptr) {
    snapshot_seq = options.snapshot->GetSequenceNumber();
    min_uncommitted =
        static_cast_with_check<const SnapshotImpl, const Snapshot>(
            options.snapshot)
            ->min_uncommitted_;
  } else {
    auto* snapshot = GetSnapshot();
    // We take a snapshot to make sure that the related data in the commit map
    // are not deleted.
    snapshot_seq = snapshot->GetSequenceNumber();
    min_uncommitted =
        static_cast_with_check<const SnapshotImpl, const Snapshot>(snapshot)
            ->min_uncommitted_;
    own_snapshot = std::make_shared<ManagedSnapshot>(db_impl_, snapshot);
  }
  assert(snapshot_seq != kMaxSequenceNumber);
  auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  auto* state =
      new IteratorState(this, snapshot_seq, own_snapshot, min_uncommitted, txn);
  auto* db_iter =
      db_impl_->NewIteratorImpl(options, cfd, snapshot_seq, &state->callback,
                                !ALLOW_BLOB, !ALLOW_REFRESH);
  db_iter->RegisterCleanup(CleanupWriteUnpreparedTxnDBIterator, state, nullptr);
  return db_iter;
}

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
