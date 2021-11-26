// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/lock/lock_manager.h"

#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "utilities/transactions/lock/point/point_lock_manager.h"
#include "utilities/transactions/lock/range/range_tree/range_tree_lock_manager.h"

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

namespace {
static int RegisterBuiltinLockManagerHandles(ObjectLibrary& library,
                                             const std::string& /*arg*/) {
#ifndef OS_WIN
  library.Register<LockManagerHandle>(
      RangeTreeLockManager::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<LockManagerHandle>* guard,
         std::string* /* errmsg */) {
        std::shared_ptr<TransactionDBMutexFactory> mutex_factory;
        guard->reset(new RangeTreeLockManager(mutex_factory));
        return guard->get();
      });
#endif  // OS_WIN
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}
}  // namespace
Status LockManagerHandle::CreateFromString(
    const ConfigOptions& config_options, const std::string& id,
    std::shared_ptr<LockManagerHandle>* result) {
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterBuiltinLockManagerHandles(*(ObjectLibrary::Default().get()), "");
  });
  // Make sure whatever LockManagerHandle is returned is prepared
  ConfigOptions prepare = config_options;
  prepare.invoke_prepare_options = true;
  return LoadSharedObject<LockManagerHandle>(prepare, id, nullptr, result);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
