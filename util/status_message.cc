// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/status.h"

namespace rocksdb {

const char* Status::msgs[] = {
    "",                                                   // kNone
    "Timeout Acquiring Mutex",                            // kMutexTimeout
    "Timeout waiting to lock key",                        // kLockTimeout
    "Failed to acquire lock due to max_num_locks limit",  // kLockLimit
    "No space left on device",                            // kNoSpace
    "Deadlock",                                           // kDeadlock
    "Stale file handle",                                  // kStaleFile
    "Memory limit reached"                                // kMemoryLimit
};

}  // namespace rocksdb
