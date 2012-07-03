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

#ifndef THRIFT_TRANSPORT_THEADERTRANSPORT_H_
#define THRIFT_TRANSPORT_THEADERTRANSPORT_H_ 1

#include <tr1/functional>

#include "thrift/lib/cpp/transport/TBufferTransports.h"
#include "thrift/lib/cpp/transport/THeader.h"
#include "thrift/lib/cpp/transport/THttpServer.h"
#include "thrift/lib/cpp/transport/TTransport.h"
#include "thrift/lib/cpp/transport/TVirtualTransport.h"

#include "folly/experimental/io/IOBuf.h"
#include "folly/experimental/io/IOBufQueue.h"

#include <bitset>
#include "boost/scoped_array.hpp"
#include <pwd.h>
#include <unistd.h>

namespace apache { namespace thrift { namespace transport {

/**
 * Header transport. All writes go into an in-memory buffer until flush is
 * called, at which point the transport writes the length of the entire
 * binary chunk followed by the data payload. This allows the receiver on the
 * other end to always do fixed-length reads.
 *
 * Subclass TFramedTransport because most of the read/write methods are similar
 * and need similar buffers.  Major changes are readFrame & flush.
 */
class THeaderTransport
    : public TVirtualTransport<THeaderTransport, TFramedTransport>
    , public THeader {
 public:

  explicit THeaderTransport(const boost::shared_ptr<TTransport> transport)
    : THeader()
    ,transport_(transport)
    , outTransport_(transport)
    , httpTransport_(transport)
  {
    initBuffers();
  }

  THeaderTransport(const boost::shared_ptr<TTransport> transport,
                   std::bitset<CLIENT_TYPES_LEN> const* clientTypes)
    : THeader(clientTypes)
    , transport_(transport)
    , outTransport_(transport)
    , httpTransport_(transport)
  {
    initBuffers();
  }

  THeaderTransport(const boost::shared_ptr<TTransport> inTransport,
                   const boost::shared_ptr<TTransport> outTransport,
                   std::bitset<CLIENT_TYPES_LEN> const* clientTypes)
    : THeader(clientTypes)
    , transport_(inTransport)
    , outTransport_(outTransport)
    , httpTransport_(outTransport)
  {
    initBuffers();
  }

  THeaderTransport(const boost::shared_ptr<TTransport> transport, uint32_t sz,
                   std::bitset<CLIENT_TYPES_LEN> const* clientTypes)
    : THeader(clientTypes)
    , transport_(transport)
    , outTransport_(transport)
    , httpTransport_(transport)
  {
    initBuffers();
  }

  void resetProtocol();

  void open() {
    transport_->open();
  }

  bool isOpen() {
    return transport_->isOpen();
  }

  bool peek() {
    return (this->rBase_ < this->rBound_) || transport_->peek();
  }

  void close() {
    flush();
    transport_->close();
  }

  virtual uint32_t readSlow(uint8_t* buf, uint32_t len);
  virtual uint32_t readAll(uint8_t* buf, uint32_t len);
  virtual void flush();

  boost::shared_ptr<TTransport> getUnderlyingTransport() {
    return transport_;
  }

  /*
   * TVirtualTransport provides a default implementation of readAll().
   * We want to use the TBufferBase version instead.
   */
  using TBufferBase::readAll;

 protected:
  /**
   * Reads a frame of input from the underlying stream.
   *
   * Returns true if a frame was read successfully, or false on EOF.
   * (Raises a TTransportException if EOF occurs after a partial frame.)
   */
  bool readFrame(uint32_t minFrameSize);

  void allocateReadBuffer(uint32_t sz);
  uint32_t getWriteBytes();

  void initBuffers() {
    setReadBuffer(NULL, 0);
    setWriteBuffer(wBuf_.get(), wBufSize_);
  }

  boost::shared_ptr<TTransport> transport_;
  boost::shared_ptr<TTransport> outTransport_;

  boost::shared_ptr<TTransport> httpTransport_;

  // Buffer to use for readFrame/flush processing
  std::unique_ptr<folly::IOBuf> readBuf_;
};

/**
 * Wraps a transport into a header one.
 *
 */
class THeaderTransportFactory : public TTransportFactory {
 public:
  THeaderTransportFactory() {}

  virtual ~THeaderTransportFactory() {}

  /**
   * Wraps the transport into a header one.
   */
  virtual boost::shared_ptr<TTransport>
  getTransport(boost::shared_ptr<TTransport> trans) {
    return boost::shared_ptr<TTransport>(
      new THeaderTransport(trans, &clientTypes));
  }

  void setClientTypes(const std::bitset<CLIENT_TYPES_LEN>& clientTypes) {
    for (int i = 0; i < CLIENT_TYPES_LEN; ++i) {
      this->clientTypes[i] = clientTypes[i];
    }
  }

private:
  std::bitset<CLIENT_TYPES_LEN> clientTypes;
};

}}} // apache::thrift::transport

#endif // #ifndef THRIFT_TRANSPORT_THEADERTRANSPORT_H_
