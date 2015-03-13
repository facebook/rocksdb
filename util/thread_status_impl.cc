// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#include "rocksdb/env.h"
#include "rocksdb/thread_status.h"
#include "util/thread_operation.h"

namespace rocksdb {

#if ROCKSDB_USING_THREAD_STATUS
const std::string& ThreadStatus::GetThreadTypeName(
    ThreadStatus::ThreadType thread_type) {
  static std::string thread_type_names[NUM_THREAD_TYPES + 1] = {
      "High Pri", "Low Pri", "User", "Unknown"};
  return thread_type_names[thread_type];
}

const std::string& ThreadStatus::GetOperationName(
    ThreadStatus::OperationType op_type) {
  return global_operation_table[op_type].name;
}

const std::string& ThreadStatus::GetOperationStageName(
    ThreadStatus::OperationStage stage) {
  return global_op_stage_table[stage].name;
}

const std::string& ThreadStatus::GetStateName(
    ThreadStatus::StateType state_type) {
  return global_state_table[state_type].name;
}

const std::string ThreadStatus::TimeToString(
    int64_t time) {
  if (time == 0) {
    return "";
  }
  return Env::Default()->TimeToString(time);
}

#else

const std::string& ThreadStatus::GetThreadTypeName(
    ThreadStatus::ThreadType thread_type) {
  static std::string dummy_str = "";
  return dummy_str;
}

const std::string& ThreadStatus::GetOperationName(
    ThreadStatus::OperationType op_type) {
  static std::string dummy_str = "";
  return dummy_str;
}

const std::string& ThreadStatus::GetOperationStageName(
    ThreadStatus::OperationStage stage) {
  static std::string dummy_str = "";
  return dummy_str;
}

const std::string& ThreadStatus::GetStateName(
    ThreadStatus::StateType state_type) {
  static std::string dummy_str = "";
  return dummy_str;
}

const std::string ThreadStatus::TimeToString(
    int64_t time) {
  static std::string dummy_str = "";
  return dummy_str;
}

#endif  // ROCKSDB_USING_THREAD_STATUS
}  // namespace rocksdb
