// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include "rocksdb/types.h"

namespace rocksdb {

// Callback class that control GC of duplicate keys in flush/compaction
class SnapshotChecker {
 public:
  virtual ~SnapshotChecker() {}
  virtual bool IsInSnapshot(SequenceNumber sequence,
                            SequenceNumber snapshot_sequence) const = 0;
};

class DisableGCSnapshotChecker : public SnapshotChecker {
 public:
  virtual ~DisableGCSnapshotChecker() {}
  virtual bool IsInSnapshot(SequenceNumber /*sequence*/,
                            SequenceNumber /*snapshot_sequence*/) const override {
    // By returning false, we prevent all the values from being GCed
    return false;
  }
  static DisableGCSnapshotChecker* Instance() { return &instance_; }

 protected:
  static DisableGCSnapshotChecker instance_;
  explicit DisableGCSnapshotChecker() {}
};

class WritePreparedTxnDB;

// Callback class created by WritePreparedTxnDB to check if a key
// is visible by a snapshot.
class WritePreparedSnapshotChecker : public SnapshotChecker {
 public:
  explicit WritePreparedSnapshotChecker(WritePreparedTxnDB* txn_db);
  virtual ~WritePreparedSnapshotChecker() {}

  virtual bool IsInSnapshot(SequenceNumber sequence,
                            SequenceNumber snapshot_sequence) const override;

 private:
#ifndef ROCKSDB_LITE
  const WritePreparedTxnDB* const txn_db_;
#endif  // !ROCKSDB_LITE
};

}  // namespace rocksdb
