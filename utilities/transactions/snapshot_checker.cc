// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/snapshot_checker.h"

#include "port/lang.h"
#include "utilities/transactions/write_prepared_txn_db.h"

namespace ROCKSDB_NAMESPACE {

WritePreparedSnapshotChecker::WritePreparedSnapshotChecker(
    WritePreparedTxnDB* txn_db)
    : txn_db_(txn_db) {}

SnapshotCheckerResult WritePreparedSnapshotChecker::CheckInSnapshot(
    SequenceNumber sequence, SequenceNumber snapshot_sequence) const {
  bool snapshot_released = false;
  // TODO(myabandeh): set min_uncommitted
  bool in_snapshot = txn_db_->IsInSnapshot(
      sequence, snapshot_sequence, kMinUnCommittedSeq, &snapshot_released);
  if (snapshot_released) {
    return SnapshotCheckerResult::kSnapshotReleased;
  }
  return in_snapshot ? SnapshotCheckerResult::kInSnapshot
                     : SnapshotCheckerResult::kNotInSnapshot;
}

DisableGCSnapshotChecker* DisableGCSnapshotChecker::Instance() {
  STATIC_AVOID_DESTRUCTION(DisableGCSnapshotChecker, instance);
  return &instance;
}

bool DataIsDefinitelyInSnapshot(SequenceNumber seqno, SequenceNumber snapshot,
                                const SnapshotChecker* snapshot_checker) {
  return ((seqno) <= (snapshot) &&
          (snapshot_checker == nullptr ||
           LIKELY(snapshot_checker->CheckInSnapshot((seqno), (snapshot)) ==
                  SnapshotCheckerResult::kInSnapshot)));
}

bool DataIsDefinitelyNotInSnapshot(SequenceNumber seqno,
                                   SequenceNumber snapshot,
                                   const SnapshotChecker* snapshot_checker) {
  return ((seqno) > (snapshot) ||
          (snapshot_checker != nullptr &&
           UNLIKELY(snapshot_checker->CheckInSnapshot((seqno), (snapshot)) ==
                    SnapshotCheckerResult::kNotInSnapshot)));
}
}  // namespace ROCKSDB_NAMESPACE
