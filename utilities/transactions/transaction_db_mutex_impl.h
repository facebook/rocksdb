//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/utilities/transaction_db_mutex.h"

namespace ROCKSDB_NAMESPACE {

class TransactionDBMutex;
class TransactionDBCondVar;

// Default implementation of TransactionDBMutexFactory.  May be overridden
// by TransactionDBOptions.custom_mutex_factory.
class TransactionDBMutexFactoryImpl : public TransactionDBMutexFactory {
 public:
  std::shared_ptr<TransactionDBMutex> AllocateMutex() override;
  std::shared_ptr<TransactionDBCondVar> AllocateCondVar() override;
};

}  // namespace ROCKSDB_NAMESPACE
