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
#ifndef THRIFT_ASYNC_TUNFRAMEDASYNCCHANNEL_H_
#define THRIFT_ASYNC_TUNFRAMEDASYNCCHANNEL_H_ 1

#include "thrift/lib/cpp/async/TStreamAsyncChannel.h"

namespace apache { namespace thrift { namespace async {

namespace detail {

/**
 * Encapsulation of one outstanding write request on a TUnframedAsyncChannel.
 */
class TUnframedACWriteRequest :
      public TAsyncChannelWriteRequestBase<TUnframedACWriteRequest> {
 public:
  typedef std::tr1::function<void()> VoidCallback;

  TUnframedACWriteRequest(const VoidCallback& callback,
                          const VoidCallback& errorCallback,
                          transport::TMemoryBuffer* message,
                          TAsyncEventChannel* channel);

  void write(TAsyncTransport* transport,
             TAsyncTransport::WriteCallback* callback) THRIFT_NOEXCEPT;

  void writeSuccess() THRIFT_NOEXCEPT;
  void writeError(size_t bytesWritten,
                  const transport::TTransportException& ex) THRIFT_NOEXCEPT;
};

/**
 * Read state for TUnframedAsyncChannel
 */
template<typename ProtocolTraits_>
class TUnframedACReadState {
 public:
  typedef std::tr1::function<void()> VoidCallback;
  typedef ProtocolTraits_ ProtocolTraits;

  TUnframedACReadState();
  ~TUnframedACReadState();

  // Methods required by TStreamAsyncChannel

  void setCallbackBuffer(transport::TMemoryBuffer* buffer) {
    callbackBuffer_ = buffer;
  }
  void unsetCallbackBuffer() {
    callbackBuffer_ = NULL;
  }

  bool hasReadAheadData() {
    return (memBuffer_.available_read() > 0);
  }
  bool hasPartialMessage() {
    return (memBuffer_.available_read() > 0);
  }

  void getReadBuffer(void** bufReturn, size_t* lenReturn);
  bool readDataAvailable(size_t len);

  // Methods specific to TUnframedACReadState

  void setMaxMessageSize(uint32_t size) {
    maxMessageSize_ = size;
  }

  uint32_t getMaxMessageSize() const {
    return maxMessageSize_;
  }

  ProtocolTraits_* getProtocolTraits() {
    return &protocolTraits_;
  }
  const ProtocolTraits_* getProtocolTraits() const {
    return &protocolTraits_;
  }

 private:
  bool getMessageLength(uint8_t* buffer,
                        uint32_t bufferLength,
                        uint32_t* messageLength);

  /// maximum frame size accepted
  uint32_t maxMessageSize_;

  apache::thrift::transport::TMemoryBuffer memBuffer_;
  apache::thrift::transport::TMemoryBuffer* callbackBuffer_;
  ProtocolTraits_ protocolTraits_;
};

} // namespace detail

/**
 * TUnframedAsyncChannel
 *
 * This is a TAsyncChannel implementation that reads and writes raw (unframed)
 * messages.  When reading messages, ProtocolTraits_ is used to determine the
 * end of a message.
 */
template<typename ProtocolTraits_>
class TUnframedAsyncChannel :
  public TStreamAsyncChannel<detail::TUnframedACWriteRequest,
                             detail::TUnframedACReadState<ProtocolTraits_> > {
 private:
  typedef TStreamAsyncChannel<detail::TUnframedACWriteRequest,
                              detail::TUnframedACReadState<ProtocolTraits_> >
      Parent;
  typedef TUnframedAsyncChannel<ProtocolTraits_> Self;

 public:
  explicit TUnframedAsyncChannel(
    const boost::shared_ptr<TAsyncTransport>& transport
    )
    : Parent(transport) {}

  /**
   * Helper function to create a shared_ptr<TUnframedAsyncChannel>.
   *
   * This passes in the correct destructor object, since TUnframedAsyncChannel's
   * destructor is protected and cannot be invoked directly.
   */
  static boost::shared_ptr<Self> newChannel(
      const boost::shared_ptr<TAsyncTransport>& transport) {
    return boost::shared_ptr<Self>(new Self(transport),
                                   typename Self::Destructor());
  }

  /// size in bytes beyond which we'll reject a given message.
  void setMaxMessageSize(uint32_t size) {
    this->readState_.setMaxMessageSize(size);
  }

  uint32_t getMaxMessageSize() const {
    return this->readState_.getMaxMessageSize();
  }

 protected:
  /**
   * Protected destructor.
   *
   * Users of TUnframedAsyncChannel must never delete it directly.  Instead,
   * invoke destroy().
   */
  virtual ~TUnframedAsyncChannel() { }
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TUNFRAMEDASYNCCHANNEL_H_
