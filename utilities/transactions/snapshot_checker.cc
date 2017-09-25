// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "db/snapshot_checker.h"

#include "utilities/transactions/pessimistic_transaction_db.h"

namespace rocksdb {

SnapshotChecker::SnapshotChecker(WritePreparedTxnDB* txn_db)
    : txn_db_(txn_db){};

bool SnapshotChecker::IsInSnapshot(SequenceNumber sequence,
                                   SequenceNumber snapshot_sequence) const {
  return txn_db_->IsInSnapshot(sequence, snapshot_sequence);
}

}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
