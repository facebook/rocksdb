//  Copyright (c) 2024-present, Meta, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/hosting.h"

#include <cassert>

// Hosting process can optionally implement these APIs.
extern "C" void RocksDbThreadYield() __attribute__((__weak__));

namespace ROCKSDB_NAMESPACE {

void Hosting::ThreadYield() {
  if (RocksDbThreadYield) {
    RocksDbThreadYield();
  }
}

}  // namespace ROCKSDB_NAMESPACE
