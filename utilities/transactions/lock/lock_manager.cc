// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/lock/lock_manager.h"

#include "utilities/transactions/lock/point/point_lock_manager.h"

namespace ROCKSDB_NAMESPACE {

std::shared_ptr<LockManager> NewLockManager(PessimisticTransactionDB* db,
                                            const TransactionDBOptions& opt) {
  assert(db);
  if (opt.lock_mgr_handle) {
    // A custom lock manager was provided in options
    auto mgr = opt.lock_mgr_handle->getLockManager();
    return std::shared_ptr<LockManager>(opt.lock_mgr_handle, mgr);
  } else {
    // Use a point lock manager by default
    return std::shared_ptr<LockManager>(new PointLockManager(db, opt));
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
