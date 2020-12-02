// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/lock/lock_manager.h"

#include "utilities/transactions/lock/point/point_lock_manager.h"

namespace ROCKSDB_NAMESPACE {

LockManager* NewLockManager(PessimisticTransactionDB* db,
                            const TransactionDBOptions& opt) {
  assert(db);
  // TODO: determine the lock manager implementation based on configuration.
  return new PointLockManager(db, opt);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
