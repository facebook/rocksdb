//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#ifdef GFLAGS
#include "db_stress_tool/db_stress_shared_state.h"

namespace ROCKSDB_NAMESPACE {
const uint32_t SharedState::UNKNOWN_SENTINEL = 0xfffffffe;
const uint32_t SharedState::DELETION_SENTINEL = 0xffffffff;
#if defined(ROCKSDB_SUPPORT_THREAD_LOCAL)
#if defined(OS_SOLARIS)
__thread bool SharedState::ignore_read_error;
#else
thread_local bool SharedState::ignore_read_error;
#endif // OS_SOLARIS
#else
bool SharedState::ignore_read_error;
#endif // ROCKSDB_SUPPORT_THREAD_LOCAL
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
