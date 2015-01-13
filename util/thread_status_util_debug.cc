// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "rocksdb/env.h"
#include "util/thread_status_updater.h"
#include "util/thread_status_util.h"

namespace rocksdb {

#ifndef NDEBUG
// the delay for debugging purpose.
static int operations_delay[ThreadStatus::NUM_OP_TYPES] ={0};
static int states_delay[ThreadStatus::NUM_STATE_TYPES] = {0};

void ThreadStatusUtil::TEST_SetStateDelay(
    const ThreadStatus::StateType state, int micro) {
  states_delay[state] = micro;
}

void ThreadStatusUtil::TEST_StateDelay(
    const ThreadStatus::StateType state) {
  Env::Default()->SleepForMicroseconds(
      states_delay[state]);
}

void ThreadStatusUtil::TEST_SetOperationDelay(
    const ThreadStatus::OperationType operation, int micro) {
  operations_delay[operation] = micro;
}


void ThreadStatusUtil::TEST_OperationDelay(
    const ThreadStatus::OperationType operation) {
  Env::Default()->SleepForMicroseconds(
      operations_delay[operation]);
}
#endif  // !NDEBUG

}  // namespace rocksdb
