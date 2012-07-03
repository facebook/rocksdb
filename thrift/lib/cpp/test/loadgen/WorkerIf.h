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
#ifndef THRIFT_TEST_LOADGEN_WORKERIF_H_
#define THRIFT_TEST_LOADGEN_WORKERIF_H_ 1

#include <boost/shared_ptr.hpp>

#include "thrift/lib/cpp/test/loadgen/IntervalTimer.h"

namespace apache { namespace thrift { namespace loadgen {

class ScoreBoard;

/**
 * Interface class for all Worker types to inherit from
 *
 * If you are implementing a new load generator, you generally should derive
 * from Worker<YourClientType> instead of using WorkerIf directly.  WorkerIf
 * only exists to provide a common base class for all Worker template
 * instantiations.
 */
class WorkerIf {
 public:
  virtual ~WorkerIf() {}

  /**
   * Run the worker
   */
  virtual void run() = 0;

  /**
   * Determine if this worker is still running.
   *
   * Returns false if this worker's run() function has returned.
   */
  virtual bool isAlive() const = 0;
};


/**
 * WorkerFactory
 */
class WorkerFactory {
 public:
  virtual ~WorkerFactory() {}

  virtual WorkerIf* newWorker(int id,
                              const boost::shared_ptr<ScoreBoard>& sb,
                              IntervalTimer* itimer) = 0;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_WORKERIF_H_
