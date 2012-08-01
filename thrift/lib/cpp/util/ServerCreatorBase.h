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
#ifndef THRIFT_UTIL_SERVERCREATORBASE_H_
#define THRIFT_UTIL_SERVERCREATORBASE_H_ 1

#include "thrift/lib/cpp/util/ServerCreator.h"

namespace apache { namespace thrift {

namespace protocol {
class TProtocolFactory;
class TDuplexProtocolFactory;
}
namespace server {
class TServerEventHandler;
}

namespace util {

/**
 * ServerCreatorBase is a helper class for subclass that wish to implement the
 * ServerCreator interface.
 *
 * ServerCreatorBase provides functionality that pretty much all ServerCreators
 * need.  It provides default protocol settings, and also
 * setServerEventHandler().
 */
class ServerCreatorBase : public ServerCreator {
 public:
  /**
   * Use strict read by default.
   *
   * We should always use strict read and strict write for all new code.
   * Only legacy services should allow requests without a thrift ID.
   */
  static const bool DEFAULT_STRICT_READ = true;

  /**
   * Use strict write by default.
   */
  static const bool DEFAULT_STRICT_WRITE = true;

  /**
   * Default string limit.  Set a reasonably small value so that servers won't
   * fall over when sent garbage data.
   */
  static const int32_t DEFAULT_STRING_LIMIT = 256*1024*1024;

  /**
   * Default container limit.  Set a reasonably small value so that servers
   * won't fall over when sent garbage data.
   */
  static const int32_t DEFAULT_CONTAINER_LIMIT = 256*1024*1024;

  /**
   * Set the TServerEventHandler to use with the new server.
   *
   * This event handler will be set on servers created by this ServerCreator.
   */
  void setServerEventHandler(
      const boost::shared_ptr<server::TServerEventHandler>& eventHandler) {
    serverEventHandler_ = eventHandler;
  }

  /**
   * Set whether or not TBinaryProtocol should include an identifier field.
   *
   * This field helps verify that incoming data is a valid thrift request,
   * rather than random garbage data.
   *
   * This setting is ignored if you explicitly specify a TProtocolFactory to
   * use.  It is only honored if you let the ServerCreator create a new
   * TBinaryProtocol for you.
   *
   * If unspecified, it defaults to true for both strictRead and strictWrite.
   *
   * @param strictRead Reject incoming requests that don't include a thrift
   *                   identifier.
   * @param strictWrite Send a thrift identifier in outgoing responses.
   */
  void setStrictProtocol(bool strictRead, bool strictWrite);

  /**
   * Set the maximum string size allowed by TBinaryProtocol.
   *
   * This setting helps prevent TBinaryProtocol from trying to allocate an
   * extremely large buffer if a request contains a bogus string length field.
   *
   * This setting is ignored if you explicitly specify a TProtocolFactory to
   * use.  It is only honored if you let the ServerCreator create a new
   * TBinaryProtocol for you.
   *
   * If unspecified, it defaults to 256MB.
   */
  void setStringSizeLimit(int32_t stringLimit);

  /**
   * Set the maximum container size allowed by TBinaryProtocol.
   *
   * This setting helps prevent TBinaryProtocol from trying to allocate an
   * extremely large buffer if a request contains a bogus container length
   * field.  (e.g., for list, map, and set fields.)
   *
   * This setting is ignored if you explicitly specify a TProtocolFactory to
   * use.  It is only honored if you let the ServerCreator create a new
   * TBinaryProtocol for you.
   *
   * If unspecified, it defaults to 256MB.
   */
  void setContainerSizeLimit(int32_t containerLimit);

  /**
   * Set the protocol factory to use.
   *
   * This causes the ServerCreator to use the specified protocol factory.
   * This overrides the TBinaryProtocol-related settings that would normally be
   * used by the ServerCreator.
   */
  void setProtocolFactory(const boost::shared_ptr<protocol::TProtocolFactory>&);

  /**
   * Set the protocol factory to use.
   *
   * This causes the ServerCreator to use the specified protocol factory
   * to construct custom input and output protocols. This overrides the
   * TBinaryProtocol-related settings that would normally be used by the
   * ServerCreator.
   */
  void setDuplexProtocolFactory(
    const boost::shared_ptr<protocol::TDuplexProtocolFactory>&);

  /**
   * Create a new server.
   */
  virtual boost::shared_ptr<server::TServer> createServer() = 0;

 protected:
  ServerCreatorBase();

  /**
   * Configure the TServer as desired.
   *
   * Subclasses should call this method in createServer().
   */
  virtual void configureServer(const boost::shared_ptr<server::TServer>& srv);

  /**
   * Get the TProtocolFactory to use for the server.
   *
   * Subclasses should call this method in createServer().
   */
  virtual boost::shared_ptr<protocol::TProtocolFactory> getProtocolFactory();

  virtual boost::shared_ptr<protocol::TDuplexProtocolFactory>
  getDuplexProtocolFactory();

  bool strictRead_;
  bool strictWrite_;
  int32_t stringLimit_;
  int32_t containerLimit_;
  boost::shared_ptr<server::TServerEventHandler> serverEventHandler_;
  boost::shared_ptr<protocol::TProtocolFactory> protocolFactory_;
  boost::shared_ptr<protocol::TDuplexProtocolFactory> duplexProtocolFactory_;
};

}}} // apache::thrift::util

#endif // THRIFT_UTIL_SERVERCREATORBASE_H_
