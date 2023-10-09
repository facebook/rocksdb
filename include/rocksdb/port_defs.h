//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file includes the common definitions used in the port/,
// the public API (this directory), and other directories

#pragma once

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

namespace port {
class CondVar;
}

enum class CpuPriority {
  kIdle = 0,
  kLow = 1,
  kNormal = 2,
  kHigh = 3,
};

}  // namespace ROCKSDB_NAMESPACE
