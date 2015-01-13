// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file defines the structures for thread event and operation.
// Thread events are used to describe high level action of a
// thread such as doing compaction or flush, while thread operation
// are used to describe lower-level action such as reading /
// writing a file or waiting for a mutex.  Events and operations
// are designed to be independent.  Typically, a thread usually involves
// in one event and one operation at any specific point in time.

#pragma once

#include "include/rocksdb/thread_status.h"

#include <string>

namespace rocksdb {

#if ROCKSDB_USING_THREAD_STATUS

// The structure that describes a major thread event.
struct EventInfo {
  const ThreadStatus::EventType code;
  const std::string name;
};

// The global event table.
//
// When updating a status of a thread, the pointer of the EventInfo
// of the current ThreadStatusData will be pointing to one of the
// rows in this global table.
//
// Note that it's not designed to be constant as in the future we
// might consider adding global count to the EventInfo.
static EventInfo global_event_table[] = {
  {ThreadStatus::EVENT_UNKNOWN, ""},
  {ThreadStatus::EVENT_COMPACTION, "Compaction"},
  {ThreadStatus::EVENT_FLUSH, "Flush"}
};

// The structure that describes a operation.
struct OperationInfo {
  const ThreadStatus::OperationType code;
  const std::string name;
};

// The global operation table.
//
// When updating a status of a thread, the pointer of the OperationInfo
// of the current ThreadStatusData will be pointing to one of the
// rows in this global table.
static OperationInfo global_operation_table[] = {
  {ThreadStatus::OPERATION_UNKNOWN, ""},
  {ThreadStatus::OPERATION_WRITE_FILE, "Writing SST file"},
  {ThreadStatus::OPERATION_READ_FILE, "Reaing SST file"},
  {ThreadStatus::OPERATION_WAIT_DB_MUTEX, "Waiting DB Mutex"}
};

#else

struct EventInfo {
};

struct OperationInfo {
};

#endif  // ROCKSDB_USING_THREAD_STATUS
}  // namespace rocksdb
