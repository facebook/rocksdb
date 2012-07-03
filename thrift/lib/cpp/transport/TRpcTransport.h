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
#ifndef THRIFT_TRANSPORT_TRPCTRANSPORT_H_
#define THRIFT_TRANSPORT_TRPCTRANSPORT_H_ 1

#include "thrift/lib/cpp/transport/TTransport.h"

namespace apache { namespace thrift { namespace transport {

class TSocketAddress;

/**
 * A TRpcTransport adds a getPeerAddress() method to the base TTransport
 * interface.
 */
class TRpcTransport : public TTransport {
 public:
  /**
   * Get the address of the peer to which this transport is connected.
   *
   * @return Returns a pointer to a TSocketAddress.  This struct is owned by
   *         the TRpcTransport and is guaranteed to remain valid for the
   *         lifetime of the TRpcTransport.  It is guaranteed to return
   *         non-NULL.  (On error, a TTransportException will be raised.)
   */
  virtual const TSocketAddress* getPeerAddress() = 0;
};

}}} // apache::thrift::transport

#endif // THRIFT_TRANSPORT_TRPCTRANSPORT_H_
