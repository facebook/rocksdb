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

#ifndef THRIFT_SERVER_TTHREADEDSERVER_H
#define THRIFT_SERVER_TTHREADEDSERVER_H 1

#include "thrift/lib/cpp/server/TServer.h"
#include "thrift/lib/cpp/transport/TServerTransport.h"
#include "thrift/lib/cpp/concurrency/Monitor.h"
#include "thrift/lib/cpp/concurrency/Thread.h"
#include "thrift/lib/cpp/concurrency/ThreadLocal.h"

#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift { namespace server {

using apache::thrift::TProcessor;
using apache::thrift::transport::TServerTransport;
using apache::thrift::transport::TTransportFactory;
using apache::thrift::concurrency::Monitor;
using apache::thrift::concurrency::ThreadFactory;

class TThreadedServer : public TServer {

 public:
  class Task;

  template<typename ProcessorFactory>
  TThreadedServer(const boost::shared_ptr<ProcessorFactory>& processorFactory,
                  const boost::shared_ptr<TServerTransport>& serverTransport,
                  const boost::shared_ptr<TTransportFactory>& transportFactory,
                  const boost::shared_ptr<TProtocolFactory>& protocolFactory,
                  THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory));

  template<typename ProcessorFactory>
  TThreadedServer(const boost::shared_ptr<ProcessorFactory>& processorFactory,
                  const boost::shared_ptr<TServerTransport>& serverTransport,
                  const boost::shared_ptr<TTransportFactory>& transportFactory,
                  const boost::shared_ptr<TProtocolFactory>& protocolFactory,
                  const boost::shared_ptr<ThreadFactory>& threadFactory,
                  THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory));

  template<typename ProcessorFactory>
  TThreadedServer(
    const boost::shared_ptr<ProcessorFactory>& processorFactory,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TDuplexTransportFactory>& duplexTransportFactory,
    const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
    const boost::shared_ptr<ThreadFactory>& threadFactory,
    THRIFT_OVERLOAD_IF(ProcessorFactory, TProcessorFactory));

  template<typename Processor>
  TThreadedServer(const boost::shared_ptr<Processor>& processor,
                  const boost::shared_ptr<TServerTransport>& serverTransport,
                  const boost::shared_ptr<TTransportFactory>& transportFactory,
                  const boost::shared_ptr<TProtocolFactory>& protocolFactory,
                  THRIFT_OVERLOAD_IF(Processor, TProcessor));

  template<typename Processor>
  TThreadedServer(const boost::shared_ptr<Processor>& processor,
                  const boost::shared_ptr<TServerTransport>& serverTransport,
                  const boost::shared_ptr<TTransportFactory>& transportFactory,
                  const boost::shared_ptr<TProtocolFactory>& protocolFactory,
                  const boost::shared_ptr<ThreadFactory>& threadFactory,
                  THRIFT_OVERLOAD_IF(Processor, TProcessor));

  template<typename Processor>
  TThreadedServer(
    const boost::shared_ptr<Processor>& processor,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TDuplexTransportFactory>& duplexTransportFactory,
    const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
    THRIFT_OVERLOAD_IF(Processor, TProcessor));

  template<typename Processor>
  TThreadedServer(
    const boost::shared_ptr<Processor>& processor,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TDuplexTransportFactory>& duplexTransportFactory,
    const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
    const boost::shared_ptr<ThreadFactory>& threadFactory,
    THRIFT_OVERLOAD_IF(Processor, TProcessor));

  virtual ~TThreadedServer();

  virtual void serve();

  void stop() {
    stop_ = true;
    serverTransport_->interrupt();
  }

  virtual TConnectionContext* getConnectionContext() const;

 protected:
  typedef concurrency::ThreadLocal<Task,
                                   concurrency::NoopThreadLocalManager<Task> >
    ThreadLocalTask;

  void init();
  void setCurrentTask(Task* task);

  boost::shared_ptr<ThreadFactory> threadFactory_;
  volatile bool stop_;

  Monitor tasksMonitor_;
  std::set<Task*> tasks_;

  /**
   * Thread-local data storage to track the current connection being processed.
   */
  ThreadLocalTask currentTask_;
};

template<typename ProcessorFactory>
TThreadedServer::TThreadedServer(
    const boost::shared_ptr<ProcessorFactory>& processorFactory,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TTransportFactory>& transportFactory,
    const boost::shared_ptr<TProtocolFactory>& protocolFactory,
    THRIFT_OVERLOAD_IF_DEFN(ProcessorFactory, TProcessorFactory)) :
  TServer(processorFactory, serverTransport, transportFactory,
          protocolFactory) {
  init();
}

template<typename ProcessorFactory>
TThreadedServer::TThreadedServer(
    const boost::shared_ptr<ProcessorFactory>& processorFactory,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TTransportFactory>& transportFactory,
    const boost::shared_ptr<TProtocolFactory>& protocolFactory,
    const boost::shared_ptr<ThreadFactory>& threadFactory,
    THRIFT_OVERLOAD_IF_DEFN(ProcessorFactory, TProcessorFactory)) :
  TServer(processorFactory, serverTransport, transportFactory,
          protocolFactory),
  threadFactory_(threadFactory) {
  init();
}

template<typename ProcessorFactory>
TThreadedServer::TThreadedServer(
  const boost::shared_ptr<ProcessorFactory>& processorFactory,
  const boost::shared_ptr<TServerTransport>& serverTransport,
  const boost::shared_ptr<TDuplexTransportFactory>& duplexTransportFactory,
  const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
  const boost::shared_ptr<ThreadFactory>& threadFactory,
  THRIFT_OVERLOAD_IF_DEFN(ProcessorFactory, TProcessorFactory)):
    TServer(processorFactory, serverTransport, duplexTransportFactory,
            duplexProtocolFactory),
    threadFactory_(threadFactory) {
  init();
}

template<typename Processor>
TThreadedServer::TThreadedServer(
    const boost::shared_ptr<Processor>& processor,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TTransportFactory>& transportFactory,
    const boost::shared_ptr<TProtocolFactory>& protocolFactory,
    THRIFT_OVERLOAD_IF_DEFN(Processor, TProcessor)) :
  TServer(processor, serverTransport, transportFactory, protocolFactory) {
  init();
}

template<typename Processor>
TThreadedServer::TThreadedServer(
    const boost::shared_ptr<Processor>& processor,
    const boost::shared_ptr<TServerTransport>& serverTransport,
    const boost::shared_ptr<TTransportFactory>& transportFactory,
    const boost::shared_ptr<TProtocolFactory>& protocolFactory,
    const boost::shared_ptr<ThreadFactory>& threadFactory,
    THRIFT_OVERLOAD_IF_DEFN(Processor, TProcessor)) :
  TServer(processor, serverTransport, transportFactory, protocolFactory),
  threadFactory_(threadFactory) {
  init();
}

template<typename Processor>
TThreadedServer::TThreadedServer(
  const boost::shared_ptr<Processor>& processor,
  const boost::shared_ptr<TServerTransport>& serverTransport,
  const boost::shared_ptr<TDuplexTransportFactory>& duplexTransportFactory,
  const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
  THRIFT_OVERLOAD_IF_DEFN(Processor, TProcessor)) :
    TServer(processor, serverTransport, duplexTransportFactory,
            duplexProtocolFactory) {
  init();
}

template<typename Processor>
TThreadedServer::TThreadedServer(
  const boost::shared_ptr<Processor>& processor,
  const boost::shared_ptr<TServerTransport>& serverTransport,
  const boost::shared_ptr<TDuplexTransportFactory>& duplexTransportFactory,
  const boost::shared_ptr<TDuplexProtocolFactory>& duplexProtocolFactory,
  const boost::shared_ptr<ThreadFactory>& threadFactory,
  THRIFT_OVERLOAD_IF_DEFN(Processor, TProcessor)):
    TServer(processor, serverTransport, duplexTransportFactory,
            duplexProtocolFactory),
    threadFactory_(threadFactory) {
  init();
}

}}} // apache::thrift::server

#endif // #ifndef _THRIFT_SERVER_TTHREADEDSERVER_H_
