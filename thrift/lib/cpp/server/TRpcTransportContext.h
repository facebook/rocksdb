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
#ifndef THRIFT_SERVER_TRPCTRANSPORTCONTEXT_H_
#define THRIFT_SERVER_TRPCTRANSPORTCONTEXT_H_ 1

#include "thrift/lib/cpp/server/TConnectionContext.h"

#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift {

namespace transport {
class TRpcTransport;
}

namespace server {

class TRpcTransportContext : public TConnectionContext {
 public:
  TRpcTransportContext(boost::shared_ptr<transport::TRpcTransport> transport)
    : transport_(transport) {}

  TRpcTransportContext(
      const boost::shared_ptr<transport::TRpcTransport>& transport,
      const boost::shared_ptr<protocol::TProtocol>& iprot,
      const boost::shared_ptr<protocol::TProtocol>& oprot)
    : transport_(transport),
      iprot_(iprot),
      oprot_(oprot) {}

  virtual const transport::TSocketAddress* getPeerAddress() const;

  const boost::shared_ptr<transport::TRpcTransport>& getTransport() const {
    return transport_;
  }

  virtual boost::shared_ptr<protocol::TProtocol> getInputProtocol() const {
    return iprot_;
  }

  virtual boost::shared_ptr<protocol::TProtocol> getOutputProtocol() const {
    return oprot_;
  }

 private:
  boost::shared_ptr<transport::TRpcTransport> transport_;
  boost::shared_ptr<protocol::TProtocol> iprot_;
  boost::shared_ptr<protocol::TProtocol> oprot_;
};

}}} // apache::thrift::server

#endif // THRIFT_SERVER_TRPCTRANSPORTCONTEXT_H_
