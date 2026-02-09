//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#pragma once

#include "rocksdb/experimental.h"

namespace ROCKSDB_NAMESPACE {

experimental::SstQueryFilterConfigsManager& DbStressSqfcManager();

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
