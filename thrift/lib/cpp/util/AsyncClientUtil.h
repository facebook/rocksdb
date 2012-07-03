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
#ifndef THRIFT_UTIL_ASYNCCLIENTUTIL_H_
#define THRIFT_UTIL_ASYNCCLIENTUTIL_H_ 1

#include "thrift/lib/cpp/async/TAsyncSocket.h"
#include "thrift/lib/cpp/async/TFramedAsyncChannel.h"
#include "thrift/lib/cpp/protocol/TBinaryProtocol.h"
#include "thrift/lib/cpp/transport/TBufferTransports.h"

namespace apache { namespace thrift { namespace util {

/*
 * Create an async client from a TSocketAddress
 */
template<typename ClientT,
         typename ProtocolFactoryT =
           protocol::TBinaryProtocolFactoryT<transport::TBufferBase>,
         typename ChannelT = async::TFramedAsyncChannel>
ClientT* createClient(async::TEventBase* eventBase,
                      const transport::TSocketAddress& address) {
  boost::shared_ptr<async::TAsyncTransport> transport(
      async::TAsyncSocket::newSocket(eventBase, address));
  boost::shared_ptr<async::TAsyncChannel> channel(
      ChannelT::newChannel(transport));

  ProtocolFactoryT protocolFactory;
  return new ClientT(channel, &protocolFactory);
}

/*
 * Create an async client from an IP and port
 */
template<typename ClientT,
         typename ProtocolFactoryT =
         protocol::TBinaryProtocolFactoryT<transport::TBufferBase>,
         typename ChannelT = async::TFramedAsyncChannel>
ClientT* createClient(async::TEventBase* eventBase,
                      const std::string& ip, uint16_t port) {
  // Note that we intentionally use setFromIpPort() and not setFromHostPort()
  // here.  If users want asynchronous operation they almost certainly don't
  // want us to perform a blocking DNS lookup operation that may take a long
  // time.
  transport::TSocketAddress address;
  address.setFromIpPort(ip, port);
  return createClient<ClientT, ProtocolFactoryT, ChannelT>(
      eventBase, address);
}

}}} // apache::thrift::util

#endif // THRIFT_UTIL_ASYNCCLIENTUTIL_H_
