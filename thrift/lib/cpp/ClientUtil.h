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
#ifndef THRIFT_CLIENTUTIL_H_
#define THRIFT_CLIENTUTIL_H_ 1

#include "thrift/lib/cpp/transport/TBufferTransports.h"
#include "thrift/lib/cpp/transport/TSocket.h"
#include "thrift/lib/cpp/protocol/TBinaryProtocol.h"

namespace apache { namespace thrift { namespace util {

/*
 * Versions that accept a host and port
 */

template<typename ClientT, typename ProtocolT, typename TransportT>
ClientT* createClient(const std::string& host, uint16_t port) {
  boost::shared_ptr<transport::TSocket> socket(
      new transport::TSocket(host, port));
  // We could specialize this to not create a wrapper transport when
  // TransportT is TTransport or TSocket.  However, everyone should always
  // use a TFramedTransport or TBufferedTransport wrapper for performance
  // reasons.
  boost::shared_ptr<TransportT> transport(new TransportT(socket));
  boost::shared_ptr<ProtocolT> protocol( new ProtocolT(transport));
  transport->open();

  return new ClientT(protocol);
}

template<typename ClientT, typename ProtocolT, typename TransportT>
boost::shared_ptr<ClientT> createClientPtr(const std::string& host,
                                           uint16_t port) {
  return boost::shared_ptr<ClientT>(
      createClient<ClientT, ProtocolT, TransportT>(host, port));
}

template<typename ClientT, typename ProtocolT>
ClientT* createClient(const std::string& host,
                      uint16_t port,
                      bool useFramed = true) {
  if (useFramed) {
    return createClient<ClientT, ProtocolT, transport::TFramedTransport>(
        host, port);
  } else {
    return createClient<ClientT, ProtocolT, transport::TBufferedTransport>(
        host, port);
  }
}

template<typename ClientT>
ClientT* createClient(const std::string& host,
                      uint16_t port,
                      bool useFramed = true) {
  return createClient<ClientT,
                      protocol::TBinaryProtocolT<transport::TBufferBase> >(
      host, port, useFramed);
}

template<typename ClientT, typename ProtocolT>
boost::shared_ptr<ClientT> createClientPtr(const std::string& host,
                                           uint16_t port,
                                           bool useFramed = true) {
  return boost::shared_ptr<ClientT>(
      createClient<ClientT, ProtocolT>(host, port, useFramed));
}

template<typename ClientT>
boost::shared_ptr<ClientT> createClientPtr(const std::string& host,
                                           uint16_t port,
                                           bool useFramed = true) {
  return boost::shared_ptr<ClientT>(
      createClient<ClientT>(host, port, useFramed));
}

/*
 * Versions that accept TSocketAddress
 */

template<typename ClientT, typename ProtocolT, typename TransportT>
ClientT* createClient(const transport::TSocketAddress* address) {
  boost::shared_ptr<transport::TSocket> socket(
      new transport::TSocket(address));
  // We could specialize this to not create a wrapper transport when
  // TransportT is TTransport or TSocket.  However, everyone should always
  // use a TFramedTransport or TBufferedTransport wrapper for performance
  // reasons.
  boost::shared_ptr<TransportT> transport(new TransportT(socket));
  boost::shared_ptr<ProtocolT> protocol( new ProtocolT(transport));
  transport->open();

  return new ClientT(protocol);
}

template<typename ClientT, typename ProtocolT, typename TransportT>
boost::shared_ptr<ClientT> createClientPtr(
    const transport::TSocketAddress* address) {
  return boost::shared_ptr<ClientT>(
      createClient<ClientT, ProtocolT, TransportT>(address));
}

template<typename ClientT, typename ProtocolT>
ClientT* createClient(const transport::TSocketAddress* address,
                      bool useFramed = true) {
  if (useFramed) {
    return createClient<ClientT, ProtocolT, transport::TFramedTransport>(
        address);
  } else {
    return createClient<ClientT, ProtocolT, transport::TBufferedTransport>(
        address);
  }
}

template<typename ClientT>
ClientT* createClient(const transport::TSocketAddress* address,
                      bool useFramed = true) {
  return createClient<ClientT,
                      protocol::TBinaryProtocolT<transport::TBufferBase> >(
      address, useFramed);
}

template<typename ClientT, typename ProtocolT>
boost::shared_ptr<ClientT> createClientPtr(
    const transport::TSocketAddress* address,
    bool useFramed = true) {
  return boost::shared_ptr<ClientT>(
      createClient<ClientT, ProtocolT>(address, useFramed));
}

template<typename ClientT>
boost::shared_ptr<ClientT> createClientPtr(
    const transport::TSocketAddress* address,
    bool useFramed = true) {
  return boost::shared_ptr<ClientT>(
      createClient<ClientT>(address, useFramed));
}

/*
 * Versions that accept TSocketAddress and socket options
 */

template<typename ClientT, typename ProtocolT, typename TransportT>
ClientT* createClient(const transport::TSocketAddress* address,
                      const transport::TSocket::Options& options) {
  boost::shared_ptr<transport::TSocket> socket(
      new transport::TSocket(address));
  socket->setSocketOptions(options);

  // We could specialize this to not create a wrapper transport when
  // TransportT is TTransport or TSocket.  However, everyone should always
  // use a TFramedTransport or TBufferedTransport wrapper for performance
  // reasons.
  boost::shared_ptr<TransportT> transport(new TransportT(socket));
  boost::shared_ptr<ProtocolT> protocol( new ProtocolT(transport));
  transport->open();

  return new ClientT(protocol);
}

template<typename ClientT, typename ProtocolT, typename TransportT>
boost::shared_ptr<ClientT> createClientPtr(
    const transport::TSocketAddress* address,
    const transport::TSocket::Options& options) {

  return boost::shared_ptr<ClientT>(
      createClient<ClientT, ProtocolT, TransportT>(address), options);
}

template<typename ClientT, typename ProtocolT>
ClientT* createClient(const transport::TSocketAddress* address,
                      const transport::TSocket::Options& options,
                      bool useFramed = true) {
  if (useFramed) {
    return createClient<ClientT, ProtocolT, transport::TFramedTransport>(
        address, options);
  } else {
    return createClient<ClientT, ProtocolT, transport::TBufferedTransport>(
        address, options);
  }
}

template<typename ClientT>
ClientT* createClient(const transport::TSocketAddress* address,
                      const transport::TSocket::Options& options,
                      bool useFramed = true
                      ) {
  return createClient<ClientT,
                      protocol::TBinaryProtocolT<transport::TBufferBase> >(
      address, options, useFramed);
}

template<typename ClientT, typename ProtocolT>
boost::shared_ptr<ClientT> createClientPtr(
    const transport::TSocketAddress* address,
    const transport::TSocket::Options& options,
    bool useFramed = true) {
  return boost::shared_ptr<ClientT>(
      createClient<ClientT, ProtocolT>(address, options, useFramed));
}

template<typename ClientT>
boost::shared_ptr<ClientT> createClientPtr(
    const transport::TSocketAddress* address,
    const transport::TSocket::Options& options,
    bool useFramed = true) {
  return boost::shared_ptr<ClientT>(
      createClient<ClientT>(address, options, useFramed));
}


}}} // apache::thrift::util

#endif // THRIFT_CLIENTUTIL_H_
