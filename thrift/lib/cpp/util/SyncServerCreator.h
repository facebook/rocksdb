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
#ifndef THRIFT_UTIL_SYNCSERVERCREATOR_H_
#define THRIFT_UTIL_SYNCSERVERCREATOR_H_ 1

#include "thrift/lib/cpp/util/ServerCreatorBase.h"

#include <stdint.h>

namespace apache { namespace thrift {

class TProcessor;

namespace transport {
class TTransportFactory;
class TDuplexTransportFactory;
class TServerSocket;
}

namespace util {

/**
 * Helper class for the standard synchronous server types (TSimpleServer,
 * TThreadedServer, TThreadPoolServer)
 */
class SyncServerCreator : public ServerCreatorBase {
 public:
  /**
   * Default to a 30 second receive timeout.
   *
   * Almost everyone should use a receive timeout, to avoid tracking connection
   * state indefinitely if a client drops of the network without closing the
   * connection.
   */
  static const int DEFAULT_RECEIVE_TIMEOUT = 30000;

  /**
   * Default to a 5 second send timeout.
   *
   * This is still a fairly large value.  Some users may want to decrease this.
   */
  static const int DEFAULT_SEND_TIMEOUT = 5000;

  /**
   * Don't use an accept timeout by default.
   *
   * Almost everyone wants this.  With a timeout, serve() will raise an
   * exception if a new connection doesn't arrive within the timeout interval.
   */
  static const int DEFAULT_ACCEPT_TIMEOUT = -1;

  /**
   * By default, don't mess with the kernel's default TCP send buffer size.
   */
  static const int DEFAULT_TCP_SEND_BUFFER = -1;

  /**
   * By default, don't mess with the kernel's default TCP receive buffer size.
   */
  static const int DEFAULT_TCP_RECEIVE_BUFFER = -1;

  /**
   * Create a new SyncServerCreator
   */
  SyncServerCreator(const boost::shared_ptr<TProcessor>& processor,
                    uint16_t port, bool framed = true);

  /**
   * Create a new SyncServerCreator
   */
  SyncServerCreator(const boost::shared_ptr<TProcessor>& processor,
                    uint16_t port,
                    boost::shared_ptr<transport::TTransportFactory>& tf,
                    boost::shared_ptr<protocol::TProtocolFactory>& pf);

  /**
   * Set the send timeout.
   *
   * Connections will be dropped if no progress is made sending a response
   * for this many milliseconds.
   */
  void setSendTimeout(int milliseconds);

  /**
   * Set the receive timeout.
   *
   * Connections will be dropped if there are no requests for this many
   * milliseconds.
   */
  void setRecvTimeout(int milliseconds);

  /**
   * Set the accept timeout.
   *
   * serve() will raise an exception if no new connections are received for
   * this many milliseconds.
   */
  void setAcceptTimeout(int milliseconds);

  /**
   * Set the TCP send buffer size.
   */
  void setTcpSendBuffer(int tcpSendBuffer);

  /**
   * Set the TCP receive buffer size.
   */
  void setTcpRecvBuffer(int tcpRecvBuffer);

  /**
   * Set the transport factory
   */
  void setTransportFactory(
      const boost::shared_ptr<transport::TTransportFactory>& tf) {
    transportFactory_ = tf;
  }

  /**
   * Set the duplex transport factory.  This overrides the base
   * transportFactory_ if one was specified
   */
  void setDuplexTransportFactory(
      const boost::shared_ptr<transport::TDuplexTransportFactory>& tf) {
    duplexTransportFactory_ = tf;
  }

  /**
   * Get the duplex transport factory, instantiating one from the base
   * transportFactory_ if needed
   */
  boost::shared_ptr<transport::TDuplexTransportFactory> getDuplexTransportFactory();
  /**
   * Create a new server.
   */
  virtual boost::shared_ptr<server::TServer> createServer() = 0;

 protected:
  void init(const boost::shared_ptr<TProcessor>& processor, uint16_t port);

  boost::shared_ptr<transport::TServerSocket> createServerSocket();

  uint16_t port_;
  int sendTimeout_;
  int recvTimeout_;
  int acceptTimeout_;
  int tcpSendBuffer_;
  int tcpRecvBuffer_;
  boost::shared_ptr<TProcessor> processor_;
  boost::shared_ptr<transport::TTransportFactory> transportFactory_;
  boost::shared_ptr<transport::TDuplexTransportFactory> duplexTransportFactory_;
};

}}} // apache::thrift::util

#endif // THRIFT_UTIL_SYNCSERVERCREATOR_H_
