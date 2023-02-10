// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// This file includes the common definitions used in the port/
// and other directories.

#pragma once

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

enum class CpuPriority {
  kIdle = 0,
  kLow = 1,
  kNormal = 2,
  kHigh = 3,
};

}  // namespace ROCKSDB_NAMESPACE
