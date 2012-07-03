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
#ifndef THRIFT_TEST_LOADGEN_WEIGHTEDLOADCONFIG_H_
#define THRIFT_TEST_LOADGEN_WEIGHTEDLOADCONFIG_H_ 1

#include "thrift/lib/cpp/test/loadgen/LoadConfig.h"

#include <string>
#include <vector>

namespace apache { namespace thrift { namespace loadgen {

class OpEnabledState;

class WeightedLoadConfig : public LoadConfig {
 public:
  WeightedLoadConfig(uint32_t numOps);

  void setOpInfo(uint32_t opType,
                 const std::string& name,
                 uint32_t weight);
  uint32_t getOpWeight(uint32_t opType);

  virtual uint32_t getNumOpTypes() const;
  virtual uint32_t pickOpType();
  virtual uint32_t pickOpsPerConnection() = 0;

  virtual std::string getOpName(uint32_t opType);

  /**
   * Update an OpEnabledState object to enable only operations that have a
   * non-zero weight.
   */
  virtual void configureEnabledState(OpEnabledState* enabledState) const;

 private:
  uint32_t weightsSum_;
  std::vector<uint32_t> weights_;
  std::vector<std::string> names_;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_WEIGHTEDLOADCONFIG_H_
