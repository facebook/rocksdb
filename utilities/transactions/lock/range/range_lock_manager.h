//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//
// Generic definitions for a Range-based Lock Manager
//
#pragma once
#ifndef ROCKSDB_LITE

#include "utilities/transactions/lock/lock_manager.h"

namespace ROCKSDB_NAMESPACE {

/*
  A base class for all Range-based lock managers

  See also class RangeLockManagerHandle in
  include/rocksdb/utilities/transaction_db.h
*/
class RangeLockManagerBase : public LockManager {
 public:
  // Geting a point lock is reduced to getting a range lock on a single-point
  // range
  using LockManager::TryLock;
  Status TryLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
                 const std::string& key, Env* env, bool exclusive) override {
    Endpoint endp(key.data(), key.size(), false);
    return TryLock(txn, column_family_id, endp, endp, env, exclusive);
  }
};

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
