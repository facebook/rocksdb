// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include "rocksdb/types.h"

namespace rocksdb {

class WritePreparedTxnDB;

// Callback class created by WritePreparedTxnDB to check if a key
// is visible by a snapshot.
class SnapshotChecker {
 public:
  explicit SnapshotChecker(WritePreparedTxnDB* txn_db);

  bool IsInSnapshot(SequenceNumber sequence,
                    SequenceNumber snapshot_sequence) const;

 private:
#ifndef ROCKSDB_LITE
  const WritePreparedTxnDB* const txn_db_;
#endif  // !ROCKSDB_LITE
};

}  // namespace rocksdb
