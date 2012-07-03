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

#ifndef _THRIFT_TRANSPORT_TSERVERTRANSPORT_H_
#define _THRIFT_TRANSPORT_TSERVERTRANSPORT_H_ 1

#include "thrift/lib/cpp/transport/TRpcTransport.h"
#include "thrift/lib/cpp/transport/TTransportException.h"
#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift { namespace transport {

/**
 * Server transport framework. A server needs to have some facility for
 * creating base transports to read/write from.
 *
 */
class TServerTransport {
 public:
  virtual ~TServerTransport() {}

  /**
   * Starts the server transport listening for new connections. Prior to this
   * call most transports will not return anything when accept is called.
   *
   * @throws TTransportException if we were unable to listen
   */
  virtual void listen() {}

  /**
   * Gets a new dynamically allocated transport object and passes it to the
   * caller. Note that it is the explicit duty of the caller to free the
   * allocated object. The returned TRpcTransport object must always be in the
   * opened state. NULL should never be returned, instead an Exception should
   * always be thrown.
   *
   * @return A new TRpcTransport object
   * @throws TTransportException if there is an error
   */
  boost::shared_ptr<TRpcTransport> accept() {
    boost::shared_ptr<TRpcTransport> result = acceptImpl();
    if (result == NULL) {
      throw TTransportException("accept() may not return NULL");
    }
    return result;
  }

  /**
   * For "smart" TServerTransport implementations that work in a multi
   * threaded context this can be used to break out of an accept() call.
   * It is expected that the transport will throw a TTransportException
   * with the interrupted error code.
   */
  virtual void interrupt() {}

  /**
   * Closes this transport such that future calls to accept will do nothing.
   */
  virtual void close() = 0;

  /**
   * Get the address on which this transport is listening.
   *
   * Should only be called after calling listen().  May raise
   * TTransportException on error.
   *
   * @param address The server's address will be stored in the TSocketAddress
   *                pointed to by this argument.
   */
  virtual void getAddress(TSocketAddress* address) = 0;

 protected:
  TServerTransport() {}

  /**
   * Subclasses should implement this function for accept.
   *
   * @return A newly allocated TRpcTransport object
   * @throw TTransportException If an error occurs
   */
  virtual boost::shared_ptr<TRpcTransport> acceptImpl() = 0;

};

}}} // apache::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TSERVERTRANSPORT_H_
