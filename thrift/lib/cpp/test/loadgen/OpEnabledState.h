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
#ifndef THRIFT_TEST_LOADGEN_OPENABLEDSTATE_H_
#define THRIFT_TEST_LOADGEN_OPENABLEDSTATE_H_ 1

#include <vector>

namespace apache { namespace thrift { namespace loadgen {

/**
 * This class tracks a boolean for each operation type, to tell if an operation
 * is enabled or not.
 *
 * This is mainly intended to be used by Monitor implementations, so they can
 * avoid monitoring (or at least printing) information for operations that are
 * disabled.
 */
class OpEnabledState {
 public:
  OpEnabledState(uint32_t numOpTypes) {
    enabled_.resize(numOpTypes, true);
  }

  void setEnabled(uint32_t opType, bool enabled) {
    assert(opType < enabled_.size());
    enabled_[opType] = enabled;
  }

  void setAllOpsEnabled(bool enabled) {
    for (std::vector<bool>::iterator it = enabled_.begin();
         it != enabled_.end();
         ++it) {
      *it = enabled;
    }
  }

  bool isEnabled(uint32_t opType) const {
    assert(opType < enabled_.size());
    return enabled_[opType];
  }

  uint32_t getNumEnabled() const {
    uint32_t num = 0;
    for (std::vector<bool>::const_iterator it = enabled_.begin();
         it != enabled_.end();
         ++it) {
      if (*it) {
        ++num;
      }
    }

    return num;
  }

 private:
  std::vector<bool> enabled_;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_OPENABLEDSTATE_H_
