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
#ifndef THRIFT_TEST_LOADGEN_QPSSCOREBOARD_H_
#define THRIFT_TEST_LOADGEN_QPSSCOREBOARD_H_ 1

#include "thrift/lib/cpp/test/loadgen/ScoreBoard.h"
#include "thrift/lib/cpp/test/loadgen/ScoreBoardOpVector.h"

namespace apache { namespace thrift { namespace loadgen {

/**
 * A ScoreBoard that tracks number of queries per second.
 *
 * This is a very simple scoreboard, that adds very little overhead.
 */
class QpsScoreBoard : public ScoreBoard {
 public:
  QpsScoreBoard(uint32_t numOpsHint) : opData_(numOpsHint) {}

  virtual void opStarted(uint32_t opType);
  virtual void opSucceeded(uint32_t opType);
  virtual void opFailed(uint32_t opType);

  /**
   * Get the number of operations performed for a specific operation type.
   */
  uint64_t getCount(uint32_t opType) const;

  /**
   * Compute the total number of operations performed, for all operation types.
   */
  uint64_t computeTotalCount() const;

  /**
   * Zero out the statistics.
   */
  void zero();

  /**
   * Add the counters from another scoreboard to this one.
   */
  void accumulate(const QpsScoreBoard* other);

 private:
  struct OpData {
    OpData() : count(0) {}

    void zero() {
      count = 0;
    }

    void accumulate(const OpData* other) {
      count += other->count;
    }

    uint64_t count;
  };

  ScoreBoardOpVector<OpData> opData_;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_QPSSCOREBOARD_H_
