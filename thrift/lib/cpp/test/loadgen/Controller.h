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
#ifndef THRIFT_TEST_LOADGEN_CONTROLLER_H_
#define THRIFT_TEST_LOADGEN_CONTROLLER_H_ 1

#include <boost/noncopyable.hpp>

#include "thrift/lib/cpp/concurrency/Monitor.h"
#include "thrift/lib/cpp/test/loadgen/LoadConfig.h"
#include "thrift/lib/cpp/test/loadgen/IntervalTimer.h"

namespace apache { namespace thrift {

namespace concurrency {

class PosixThreadFactory;

} // apache::thrift::concurrency

namespace loadgen {

class WorkerFactory;
class WorkerIf;
class Monitor;

class Controller : private boost::noncopyable {
 public:
  Controller(WorkerFactory* factory, Monitor* monitor,
      boost::shared_ptr<LoadConfig> config,
      apache::thrift::concurrency::PosixThreadFactory* threadFactory = NULL);

  void run(uint32_t numThreads, double monitorInterval = 1.0);

 private:
  class WorkerRunner;
  typedef std::vector< boost::shared_ptr<WorkerIf> > WorkerVector;

  void startWorkers(uint32_t numThreads);
  void runMonitor(double interval);
  boost::shared_ptr<WorkerIf> createWorker();

  concurrency::Monitor initMonitor_;
  uint32_t numThreads_;
  WorkerFactory* workerFactory_;
  Monitor* monitor_;
  WorkerVector workers_;
  IntervalTimer intervalTimer_;
  boost::shared_ptr<LoadConfig> config_;
  apache::thrift::concurrency::PosixThreadFactory* threadFactory_;
};

}}} // apache::thrift::loadgen

#endif // THRIFT_TEST_LOADGEN_CONTROLLER_H_
