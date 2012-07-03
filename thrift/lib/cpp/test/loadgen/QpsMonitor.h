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
#ifndef THRIFT_TEST_LOADGEN_QPSMONITOR_H_
#define THRIFT_TEST_LOADGEN_QPSMONITOR_H_ 1

#include "thrift/lib/cpp/test/loadgen/TerminalMonitor.h"
#include "thrift/lib/cpp/test/loadgen/OpEnabledState.h"
#include "thrift/lib/cpp/test/loadgen/QpsScoreBoard.h"

namespace apache { namespace thrift { namespace loadgen {

class LoadConfig;

/**
 * A Monitor implementation that prints QPS rates for all operations.
 */
class QpsMonitor : public TerminalMonitor {
 public:
  QpsMonitor(const boost::shared_ptr<LoadConfig>& config);

  virtual boost::shared_ptr<ScoreBoard> newScoreBoard(int id);

  virtual void initializeInfo();
  virtual uint32_t printHeader();
  virtual uint32_t printInfo(uint64_t intervalUsec);

  OpEnabledState* getEnabledState() {
    return &enabledState_;
  }

  const OpEnabledState* getEnabledState() const {
    return &enabledState_;
  }

  void printAllTimeQps(bool enabled) {
    printAllTime_ = enabled;
  }

 private:
  typedef std::vector< boost::shared_ptr<QpsScoreBoard> > ScoreBoardVector;

  void computeAggregate(QpsScoreBoard* scoreboard);

  int64_t initialTime_;
  uint64_t initialSum_;

  bool printAllTime_;
  OpEnabledState enabledState_;

  QpsScoreBoard aggregateScoreBoard_;
  ScoreBoardVector scoreboards_;
  boost::shared_ptr<LoadConfig> config_;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_QPSMONITOR_H_
