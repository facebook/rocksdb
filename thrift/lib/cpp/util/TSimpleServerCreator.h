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
#ifndef THRIFT_UTIL_TSIMPLESERVERCREATOR_H_
#define THRIFT_UTIL_TSIMPLESERVERCREATOR_H_ 1

#include "thrift/lib/cpp/util/SyncServerCreator.h"

namespace apache { namespace thrift {

namespace server {
class TSimpleServer;
}

namespace util {

class TSimpleServerCreator : public SyncServerCreator {
 public:
  typedef server::TSimpleServer ServerType;

  /**
   * Create a new TSimpleServerCreator.
   */
  TSimpleServerCreator(const boost::shared_ptr<TProcessor>& processor,
                       uint16_t port,
                       bool framed = true)
    : SyncServerCreator(processor, port, framed) {}

  /**
   * Create a new TSimpleServerCreator.
   */
  TSimpleServerCreator(const boost::shared_ptr<TProcessor>& processor,
                       uint16_t port,
                       boost::shared_ptr<transport::TTransportFactory>& tf,
                       boost::shared_ptr<protocol::TProtocolFactory>& pf)
    : SyncServerCreator(processor, port, tf, pf) {}

  virtual boost::shared_ptr<server::TServer> createServer();

  boost::shared_ptr<server::TSimpleServer> createSimpleServer();
};

}}} // apache::thrift::util

#endif // THRIFT_UTIL_TSIMPLESERVERCREATOR_H_
