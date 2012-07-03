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
#ifndef THRIFT_UTIL_TTHREADPOOLSERVERCREATOR_H_
#define THRIFT_UTIL_TTHREADPOOLSERVERCREATOR_H_ 1

#include "thrift/lib/cpp/util/SyncServerCreator.h"

namespace apache { namespace thrift {

namespace concurrency {
class ThreadFactory;
}
namespace server {
class TThreadPoolServer;
}

namespace util {

class TThreadPoolServerCreator : public SyncServerCreator {
 public:
  typedef server::TThreadPoolServer ServerType;

  enum OverloadAction {
    OVERLOAD_STOP_ACCEPTING,
    OVERLOAD_ACCEPT_AND_CLOSE,
  };

  /**
   * By default, stop calling accept() once we reach the maximum number pending
   * tasks.
   */
   static const OverloadAction DEFAULT_OVERLOAD_ACTION =
     OVERLOAD_STOP_ACCEPTING;

   /**
    * Use 30 threads by default.
    *
    * Make sure to set this number large enough for your service.  If there are
    * more active connections than number of threads, new connections will
    * block until one of the existing connections is closed!  Typically you
    * should also use a small receive timeout with TThreadPoolServer, to
    * quickly expire idle connections.
    */
   static const size_t DEFAULT_NUM_THREADS = 30;

   /**
    * By default, only allow up to 30 pending connections waiting for a thread
    * to run.
    *
    * Once all threads are in use, at most this many additional tasks may be
    * queued before the server starts taking the OverloadAction.
    */
   static const size_t DEFAULT_MAX_PENDING_TASKS = 30;

  /**
   * Create a new TThreadPoolServerCreator.
   */
  TThreadPoolServerCreator(const boost::shared_ptr<TProcessor>& processor,
                           uint16_t port,
                           bool framed = true);

  /**
   * Create a new TThreadPoolServerCreator.
   */
  TThreadPoolServerCreator(const boost::shared_ptr<TProcessor>& processor,
                           uint16_t port,
                           int numThreads,
                           bool framed = true);

  /**
   * Create a new TThreadPoolServerCreator.
   */
  TThreadPoolServerCreator(const boost::shared_ptr<TProcessor>& processor,
                           uint16_t port,
                           boost::shared_ptr<transport::TTransportFactory>& tf,
                           boost::shared_ptr<protocol::TProtocolFactory>& pf);

  /**
   * Set the action to take once all threads are in use and the pending task
   * queue is full.
   */
  void setOverloadAction(OverloadAction action) {
    overloadAction_ = action;
  }

  /**
   * Set the number of threads to use for the thread pool
   */
  void setNumThreads(size_t numThreads) {
    numThreads_ = numThreads;
  }

  /**
   * Set the maximum number of pending tasks.
   */
  void setMaxPendingTasks(size_t numTasks) {
    maxPendingTasks_ = numTasks;
  }

  /**
   * Set the thread factory
   */
  void setThreadFactory(const boost::shared_ptr<concurrency::ThreadFactory>&);

  virtual boost::shared_ptr<server::TServer> createServer();

  boost::shared_ptr<server::TThreadPoolServer> createThreadPoolServer();

 private:
  OverloadAction overloadAction_;
  size_t numThreads_;
  size_t maxPendingTasks_;
  boost::shared_ptr<concurrency::ThreadFactory> threadFactory_;
};

}}} // apache::thrift::util

#endif // THRIFT_UTIL_TTHREADPOOLSERVERCREATOR_H_
