/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef THRIFT_TEST_LOADGEN_MONITOR_H_
#define THRIFT_TEST_LOADGEN_MONITOR_H_ 1

#include <boost/shared_ptr.hpp>
#include <vector>
#include <stdint.h>

namespace apache { namespace thrift { namespace loadgen {

class ScoreBoard;

class Monitor {
 public:
  Monitor() {}
  virtual ~Monitor() {}

  /**
   * Create a new ScoreBoard object.
   *
   * This method will be called once for each worker thread.  All calls to
   * newScoreBoard() will be made before run() is invoked.
   *
   * For each Worker, newScoreBoard() will be called in the thread that will
   * run that worker.  However, the caller holds a lock and ensures that it
   * will only be called in one thread at a time, so newScoreBoard() doesn't
   * need to perform any locking internally.
   *
   * @param id  The id of the Worker that will use this ScoreBoard.
   */
  virtual boost::shared_ptr<ScoreBoard> newScoreBoard(int id) = 0;

  /**
   * Initialize monitoring information.
   *
   * This method is called once when the workers start, just before the initial
   * monitoring interval.  It can be used to get initial counter values from
   * all of the Workers, so that statistics reported in the first call to
   * redisplay() are accurate.
   */
  virtual void initializeInfo() {}

  /**
   * Redisplay the monitor information.
   *
   * @param intervalUsec The number of microseconds since the last call to
   *                     redisplay() or initializeInfo().
   */
  virtual void redisplay(uint64_t intervalUsec) = 0;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_MONITOR_H_
