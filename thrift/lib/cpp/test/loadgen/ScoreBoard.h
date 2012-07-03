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
#ifndef THRIFT_TEST_LOADGEN_SCOREBOARD_H_
#define THRIFT_TEST_LOADGEN_SCOREBOARD_H_ 1

#include <inttypes.h>

namespace apache { namespace thrift { namespace loadgen {

/**
 * A ScoreBoard object keeps statistics for a Worker.
 *
 * There is intended to be one ScoreBoard object per Worker, so that it doesn't
 * need to perform locking when recording statistics.
 *
 * The Monitor object is responsible for aggregating the statistics from the
 * various ScoreBoards.
 */
class ScoreBoard {
 public:
  virtual ~ScoreBoard() {}

  /**
   * opStarted() is invoked just before each call to
   * Worker::performOperation().
   */
  virtual void opStarted(uint32_t opType) = 0;

  /**
   * opSucceeded() is invoked after each successful call to
   * Worker::performOperation().
   */
  virtual void opSucceeded(uint32_t opType) = 0;

  /**
   * opFailed() is invoked if Worker::performOperation() throws an exception.
   */
  virtual void opFailed(uint32_t opType) = 0;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_SCOREBOARD_H_
