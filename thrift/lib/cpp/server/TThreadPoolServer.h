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

#ifndef THRIFT_SERVER_TTHREADPOOLSERVER_H
#define THRIFT_SERVER_TTHREADPOOLSERVER_H 1

#include "thrift/lib/cpp/concurrency/ThreadManager.h"
#include "thrift/lib/cpp/server/TServer.h"
#include "thrift/lib/cpp/transport/TServerTransport.h"
#include "thrift/lib/cpp/concurrency/ThreadLocal.h"

#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift { namespace server {

using apache::thrift::concurrency::ThreadManager;
using apache::thrift::protocol::TProtocolFactory;
using apache::thrift::transport::TServerTransport;
using apache::thrift::transport::TTransportFactory;

class TThreadPoolServer : public TServer {
 public:
  template<typename ProcessorFactory>
  TThreadPoolServer(
      const boost::shared_ptr<ProcessorFactory>& processorFactory,
      const boost::shared_ptr<TServerTransport>& serverTransport,
      const boost::shared_ptr<TTransportFactory>& transportFactory,
      const boost::shared_ptr<TProtocolFactory>& protocolFactory,
      const boost::shared_ptr<ThreadManager>& threadManager,
      THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory)) :
    TServer(processorFactory, serverTransport, transportFactory,
            protocolFactory),
    threadManager_(threadManager),
    stop_(false),
    timeout_(0) {}

  template<typename Processor>
  TThreadPoolServer(
      const boost::shared_ptr<Processor>& processor,
      const boost::shared_ptr<TServerTransport>& serverTransport,
      const boost::shared_ptr<TTransportFactory>& transportFactory,
      const boost::shared_ptr<TProtocolFactory>& protocolFactory,
      const boost::shared_ptr<ThreadManager>& threadManager,
      THRIFT_OVERLOAD_IF(Processor, TProcessor)) :
    TServer(processor, serverTransport, transportFactory, protocolFactory),
    threadManager_(threadManager),
    stop_(false),
    timeout_(0) {}

  template<typename ProcessorFactory>
  TThreadPoolServer(
      const boost::shared_ptr<ProcessorFactory>& processorFactory,
      const boost::shared_ptr<TServerTransport>& serverTransport,
      const boost::shared_ptr<TDuplexTransportFactory>& duplexTransportFactory,
      const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
      const boost::shared_ptr<ThreadManager>& threadManager,
      THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory)):
    TServer(processorFactory, serverTransport,
            duplexTransportFactory, duplexProtocolFactory),
    threadManager_(threadManager),
    stop_(false),
    timeout_(0) {}

  template<typename Processor>
  TThreadPoolServer(
      const boost::shared_ptr<Processor>& processor,
      const boost::shared_ptr<TServerTransport>& serverTransport,
      const boost::shared_ptr<TDuplexTransportFactory>& duplexTransportFactory,
      const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
      const boost::shared_ptr<ThreadManager>& threadManager,
      THRIFT_OVERLOAD_IF(Processor, TProcessor)):
    TServer(processor, serverTransport,
            duplexTransportFactory, duplexProtocolFactory),
    threadManager_(threadManager),
    stop_(false),
    timeout_(0) {}

  virtual ~TThreadPoolServer();

  virtual void serve();

  virtual int64_t getTimeout() const;

  virtual void setTimeout(int64_t value);

  virtual void stop() {
    stop_ = true;
    serverTransport_->interrupt();
  }

  virtual TConnectionContext* getConnectionContext() const;

 protected:
  class Task;
  typedef concurrency::ThreadLocal<Task,
                                   concurrency::NoopThreadLocalManager<Task> >
    ThreadLocalTask;

  void setCurrentTask(Task* task);
  void clearCurrentTask();

  boost::shared_ptr<ThreadManager> threadManager_;
  volatile bool stop_;
  volatile int64_t timeout_;

  /**
   * Thread-local data storage to track the current connection being processed.
   */
  ThreadLocalTask currentTask_;
};

}}} // apache::thrift::server

#endif // #ifndef THRIFT_SERVER_TTHREADPOOLSERVER_H
