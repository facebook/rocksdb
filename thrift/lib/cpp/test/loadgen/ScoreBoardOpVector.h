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
#ifndef THRIFT_TEST_LOADGEN_SCOREBOARDOPVECTOR_H_
#define THRIFT_TEST_LOADGEN_SCOREBOARDOPVECTOR_H_ 1

#include "thrift/lib/cpp/test/loadgen/ScoreBoard.h"

#include <assert.h>
#include <vector>
#include <stddef.h>

namespace apache { namespace thrift { namespace loadgen {

/**
 * A vector of per-operation scoreboard data.
 *
 * This is useful for implementing ScoreBoard classes.
 *
 * The OpDataT class must have:
 * - a default constructor
 * - a zero() method
 * - an accumulate() method
 */
template<typename OpDataT>
class ScoreBoardOpVector {
 private:
  typedef std::vector<OpDataT> DataVector;

 public:
  typedef typename DataVector::iterator Iterator;
  typedef typename DataVector::const_iterator ConstIterator;

  ScoreBoardOpVector(uint32_t numOpsHint) {
    resize(numOpsHint);
  }

  OpDataT* getOpData(uint32_t opType) {
    if (opType >= opData_.size()) {
      resize(opType + 1);
    }
    return &opData_[opType];
  }

  const OpDataT* getOpDataOrNull(uint32_t opType) const {
    if (opType >= opData_.size()) {
      return NULL;
    }
    return &opData_[opType];
  }

  void zero() {
    for (Iterator it = opData_.begin(); it != opData_.end(); ++it) {
      it->zero();
    }
  }

  void accumulate(const ScoreBoardOpVector<OpDataT>* other) {
    if (other->opData_.size() > opData_.size()) {
      resize(other->opData_.size());
    }

    uint32_t index = 0;
    for (ConstIterator it = other->opData_.begin();
         it != other->opData_.end();
         ++it, ++index) {
      opData_[index].accumulate(&(*it));
    }
  }

  void accumulateOverOps(OpDataT* result) const {
    result->zero();
    for (ConstIterator it = opData_.begin(); it != opData_.end(); ++it) {
      result->accumulate(&(*it));
    }
  }

  Iterator begin() {
    return opData_.begin();
  }
  Iterator end() {
    return opData_.end();
  }

  ConstIterator begin() const {
    return opData_.begin();
  }
  ConstIterator end() const {
    return opData_.end();
  }

 private:
  void resize(uint32_t numOps) {
    assert(numOps > opData_.size());

    // We could add some padding here.  In the past, when using glibc malloc
    // with QpsScoreBoard and just 1 operation, I've seen the opData for two
    // different worker threads end up being allocated on the same cache line.
    // This hurts peformance, since the two worker threads run on different
    // CPUs and they each keep evicting the cache line from the other CPU.
    // With jemalloc this doesn't seem to happen anymore, and allocating more
    // space then necessary here can very slightly slow down performance.

    opData_.resize(numOps);
  }

  DataVector opData_;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_SCOREBOARDOPVECTOR_H_
