// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <cstddef>
#include <string>

#ifndef ROCKSDB_USING_THREAD_STATUS
#define ROCKSDB_USING_THREAD_STATUS \
    !defined(ROCKSDB_LITE) && \
    !defined(NROCKSDB_THREAD_STATUS) && \
    !defined(OS_MACOSX) && \
    !defined(IOS_CROSS_COMPILE)
#endif

namespace rocksdb {

// A structure that describes the current status of a thread.
// The status of active threads can be fetched using
// rocksdb::GetThreadList().
struct ThreadStatus {
  enum ThreadType {
    ROCKSDB_HIGH_PRIORITY = 0x0,
    ROCKSDB_LOW_PRIORITY = 0x1,
    USER_THREAD = 0x2,
    TOTAL = 0x3
  };

#if ROCKSDB_USING_THREAD_STATUS
  ThreadStatus(const uint64_t _id,
               const ThreadType _thread_type,
               const std::string& _db_name,
               const std::string& _cf_name,
               const std::string& _event) :
      thread_id(_id), thread_type(_thread_type),
      db_name(_db_name),
      cf_name(_cf_name),
      event(_event) {}

  // An unique ID for the thread.
  const uint64_t thread_id;

  // The type of the thread, it could be ROCKSDB_HIGH_PRIORITY,
  // ROCKSDB_LOW_PRIORITY, and USER_THREAD
  const ThreadType thread_type;

  // The name of the DB instance where the thread is currently
  // involved with.  It would be set to empty string if the thread
  // does not involve in any DB operation.
  const std::string db_name;

  // The name of the column family where the thread is currently
  // It would be set to empty string if the thread does not involve
  // in any column family.
  const std::string cf_name;

  // The event that the current thread is involved.
  // It would be set to empty string if the information about event
  // is not currently available.
  const std::string event;
#endif  // ROCKSDB_USING_THREAD_STATUS
};

}  // namespace rocksdb
