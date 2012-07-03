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
#ifndef THRIFT_TEST_LOADGEN_LATENCYMONITOR_H_
#define THRIFT_TEST_LOADGEN_LATENCYMONITOR_H_ 1

#include "thrift/lib/cpp/test/loadgen/TerminalMonitor.h"
#include "thrift/lib/cpp/test/loadgen/OpEnabledState.h"
#include "thrift/lib/cpp/test/loadgen/LatencyScoreBoard.h"

namespace apache { namespace thrift { namespace loadgen {

class LoadConfig;

/**
 * A Monitor implementation that prints QPS rates and latency information for
 * all operations.
 */
class LatencyMonitor : public TerminalMonitor {
 public:
  /// The various fields that LatencyMonitor knows how to print
  enum FieldEnum {
    FIELD_COUNT,
    FIELD_QPS,
    FIELD_LATENCY,
    FIELD_ALL_TIME_COUNT,
    FIELD_ALL_TIME_QPS,
    FIELD_ALL_TIME_LATENCY,
  };

  struct FieldInfo {
    FieldInfo(FieldEnum f, int w = -1)
      : field(f)
      , width(w)
      , dynamicWidth(true) {}

    FieldEnum field;
    int width;
    bool dynamicWidth;
  };

  typedef std::vector<FieldInfo> FieldInfoVector;

  LatencyMonitor(const boost::shared_ptr<LoadConfig>& config);

  /**
   * Set the fields printed for the specified operation.
   */
  void setFields(uint32_t opType, const FieldInfoVector* fields);

  /**
   * Set the fields printed for aggregate statistics for all operations.
   */
  void setTotalFields(const FieldInfoVector* fields);

  virtual boost::shared_ptr<ScoreBoard> newScoreBoard(int id);

  virtual void initializeInfo();
  virtual uint32_t printHeader();
  virtual uint32_t printInfo(uint64_t intervalUsec);

 private:
  typedef std::vector< boost::shared_ptr<LatencyScoreBoard> > ScoreBoardVector;

  void printOpHeader(FieldInfoVector* fields);
  void printOpInfo(FieldInfoVector* fields,
                   const LatencyScoreBoard::OpData* current,
                   const LatencyScoreBoard::OpData* prev,
                   const LatencyScoreBoard::OpData* initial,
                   uint64_t intervalUsec,
                   uint64_t allTimeUsec);

  uint32_t getFieldVectorWidth(const FieldInfoVector* fields) const;
  const char *getFieldName(FieldEnum field) const;
  uint32_t getDefaultFieldWidth(FieldEnum field) const;

  void formatFieldValue(FieldEnum field,
                        char* buf,
                        size_t buflen,
                        const LatencyScoreBoard::OpData* current,
                        const LatencyScoreBoard::OpData* prev,
                        const LatencyScoreBoard::OpData* initial,
                        uint64_t intervalUsec,
                        uint64_t allTimeUsec);
  void formatLatency(char* buf, size_t buflen, double avg, double stddev);

  void aggregateWorkerScorboards(LatencyScoreBoard* scoreboard);
  void printField(const char* value, int width);
  void printField(const char* value, FieldInfo* fieldInfo);

  void setDefaultOpFields();
  void printLegend();
  bool isFieldInUse(FieldEnum field);

  uint32_t numOpTypes_;

  /// The list of fields to print for each operation
  std::vector<FieldInfoVector> opFields_;
  /// The fields to print for the aggregate information over all operations
  FieldInfoVector totalFields_;

  int64_t initialTime_;
  LatencyScoreBoard initialScoreBoard_;

  /// A scoreboard with information aggregated across all of the workers
  LatencyScoreBoard aggregateScoreBoard_;
  /// A vector of the actual scoreboards used by the workers
  ScoreBoardVector scoreboards_;
  boost::shared_ptr<LoadConfig> config_;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_LATENCYMONITOR_H_
