//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class WalWriteCallback {
 public:
  virtual ~WalWriteCallback() {}

  // Will be called after wal write finishes.
  // Note: this callback is only invoked in pipelined write mode after a write
  // group's WAL write is finished.
  virtual void OnWalWriteFinish() = 0;
};

}  // namespace ROCKSDB_NAMESPACE