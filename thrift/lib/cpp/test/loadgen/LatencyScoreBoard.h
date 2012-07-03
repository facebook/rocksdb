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
#ifndef THRIFT_TEST_LOADGEN_LATENCYSCOREBOARD_H_
#define THRIFT_TEST_LOADGEN_LATENCYSCOREBOARD_H_ 1

#include "thrift/lib/cpp/test/loadgen/ScoreBoard.h"
#include "thrift/lib/cpp/test/loadgen/ScoreBoardOpVector.h"

namespace apache { namespace thrift { namespace loadgen {

/**
 * A ScoreBoard that tracks number of queries per second, as well as
 * information about how long each operation takes.
 *
 * This ScoreBoard calls gettimeofday() twice for each operation, so it does
 * add a small amount of overhead.  If you have extremely high performance
 * requirements, you could use QpsScoreBoard to track just the QPS rate and
 * eliminate the gettimeofday() calls.
 */
class LatencyScoreBoard : public ScoreBoard {
 public:
  class OpData {
   public:
    OpData();

    void addDataPoint(uint64_t latencyUsecs);

    void zero();
    void accumulate(const OpData* other);

    uint64_t getCount() const;
    uint64_t getCountSince(const OpData* other) const;
    double getLatencyAvg() const;
    double getLatencyAvgSince(const OpData* other) const;
    double getLatencyStdDev() const;
    double getLatencyStdDevSince(const OpData* other) const;

    uint64_t count_;
    uint64_t usecSum_;
    uint64_t sumOfSquares_;
  };

  LatencyScoreBoard(uint32_t numOpsHint)
    : startTime_(0)
    , opData_(numOpsHint) {}

  virtual void opStarted(uint32_t opType);
  virtual void opSucceeded(uint32_t opType);
  virtual void opFailed(uint32_t opType);

  /**
   * Get the OpData for a particular operation type
   */
  const OpData* getOpData(uint32_t opType);

  /**
   * Compute an OpData object with aggregate information over all operation
   * types.
   *
   * @param result A pointer to the OpData object to fill in with aggregate
   *               information.
   */
  void computeOpAggregate(OpData* result) const;

  /**
   * Zero out the statistics.
   */
  void zero();

  /**
   * Add the counters from another scoreboard to this one.
   */
  void accumulate(const LatencyScoreBoard* other);

 private:
  int64_t startTime_;
  ScoreBoardOpVector<OpData> opData_;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_LATENCYSCOREBOARD_H_
