// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file defines the structures for exposing run-time status of any
// rocksdb-related thread.  Such run-time status can be obtained via
// GetThreadList() API.
//
// Note that all thread-status features are still under-development, and
// thus APIs and class definitions might subject to change at this point.
// Will remove this comment once the APIs have been finalized.

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
  // The type of a thread.
  enum ThreadType : int {
    HIGH_PRIORITY = 0,  // RocksDB BG thread in high-pri thread pool
    LOW_PRIORITY,  // RocksDB BG thread in low-pri thread pool
    USER,  // User thread (Non-RocksDB BG thread)
    NUM_THREAD_TYPES
  };

  // The type used to refer to a thread operation.
  // A thread operation describes high-level action of a thread.
  // Examples include compaction and flush.
  enum OperationType : int {
    OP_UNKNOWN = 0,
    OP_COMPACTION,
    OP_FLUSH,
    NUM_OP_TYPES
  };

  enum OperationStage : int {
    STAGE_UNKNOWN = 0,
    STAGE_FLUSH_RUN,
    STAGE_FLUSH_WRITE_L0,
    STAGE_COMPACTION_PREPARE,
    STAGE_COMPACTION_RUN,
    STAGE_COMPACTION_PROCESS_KV,
    STAGE_COMPACTION_FILTER_V2,
    STAGE_COMPACTION_INSTALL,
    STAGE_COMPACTION_SYNC_FILE,
    STAGE_PICK_MEMTABLES_TO_FLUSH,
    STAGE_MEMTABLE_ROLLBACK,
    STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS,
    NUM_OP_STAGES
  };

  // The type used to refer to a thread state.
  // A state describes lower-level action of a thread
  // such as reading / writing a file or waiting for a mutex.
  enum StateType : int {
    STATE_UNKNOWN = 0,
    STATE_MUTEX_WAIT = 1,
    NUM_STATE_TYPES
  };

  ThreadStatus(const uint64_t _id,
               const ThreadType _thread_type,
               const std::string& _db_name,
               const std::string& _cf_name,
               const OperationType _operation_type,
               const int64_t _op_start_time,
               const OperationStage _operation_stage,
               const StateType _state_type) :
      thread_id(_id), thread_type(_thread_type),
      db_name(_db_name),
      cf_name(_cf_name),
      operation_type(_operation_type),
      op_start_time(_op_start_time),
      operation_stage(_operation_stage),
      state_type(_state_type) {}

  // An unique ID for the thread.
  const uint64_t thread_id;

  // The type of the thread, it could be HIGH_PRIORITY,
  // LOW_PRIORITY, and USER
  const ThreadType thread_type;

  // The name of the DB instance where the thread is currently
  // involved with.  It would be set to empty string if the thread
  // does not involve in any DB operation.
  const std::string db_name;

  // The name of the column family where the thread is currently
  // It would be set to empty string if the thread does not involve
  // in any column family.
  const std::string cf_name;

  // The operation (high-level action) that the current thread is involved.
  const OperationType operation_type;

  // The start time of the current status in the form of seconds since the
  // Epoch, 1970-01-01 00:00:00 (UTC).
  const int64_t op_start_time;

  // An integer showing the current stage where the thread is involved
  // in the current operation.
  const OperationStage operation_stage;

  // The state (lower-level action) that the current thread is involved.
  const StateType state_type;

  // The followings are a set of utility functions for interpreting
  // the information of ThreadStatus

  static const std::string& GetThreadTypeName(ThreadType thread_type);

  // Obtain the name of an operation given its type.
  static const std::string& GetOperationName(OperationType op_type);

  static const std::string TimeToString(int64_t op_start_time);

  // Obtain a human-readable string describing the specified operation stage.
  static const std::string& GetOperationStageName(
      OperationStage stage);

  // Obtain the name of a state given its type.
  static const std::string& GetStateName(StateType state_type);
};


}  // namespace rocksdb
