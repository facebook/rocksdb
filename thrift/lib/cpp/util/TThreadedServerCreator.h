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
#ifndef THRIFT_UTIL_TTHREADEDSERVERCREATOR_H_
#define THRIFT_UTIL_TTHREADEDSERVERCREATOR_H_ 1

#include "thrift/lib/cpp/util/SyncServerCreator.h"

namespace apache { namespace thrift {

namespace concurrency {
class ThreadFactory;
}
namespace server {
class TThreadedServer;
}

namespace util {

class TThreadedServerCreator : public SyncServerCreator {
 public:
  typedef server::TThreadedServer ServerType;

  /**
   * Create a new TThreadedServerCreator.
   */
  TThreadedServerCreator(const boost::shared_ptr<TProcessor>& processor,
                         uint16_t port,
                         bool framed = true)
    : SyncServerCreator(processor, port, framed) {}

  /**
   * Create a new TThreadedServerCreator.
   */
  TThreadedServerCreator(const boost::shared_ptr<TProcessor>& processor,
                         uint16_t port,
                         boost::shared_ptr<transport::TTransportFactory>& tf,
                         boost::shared_ptr<protocol::TProtocolFactory>& pf)
    : SyncServerCreator(processor, port, tf, pf) {}

  /**
   * Set the thread factory
   */
  void setThreadFactory(const boost::shared_ptr<concurrency::ThreadFactory>&);

  virtual boost::shared_ptr<server::TServer> createServer();

  boost::shared_ptr<server::TThreadedServer> createThreadedServer();

 private:
  boost::shared_ptr<concurrency::ThreadFactory> threadFactory_;
};

}}} // apache::thrift::util

#endif // THRIFT_UTIL_TTHREADEDSERVERCREATOR_H_
