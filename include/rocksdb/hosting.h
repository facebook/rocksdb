//  Copyright (c) 2024-present, Meta, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Interface to be used by a host process to implement various APIs
// such as thread management, synchronization, and so on.
// TransactionDBMutexFactory could be moved here as well.
class Hosting {
 public:
  static void ThreadYield();
};

}  // namespace ROCKSDB_NAMESPACE
