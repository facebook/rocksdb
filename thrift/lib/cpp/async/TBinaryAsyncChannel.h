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
#ifndef THRIFT_ASYNC_TBINARYASYNCCHANNEL_H_
#define THRIFT_ASYNC_TBINARYASYNCCHANNEL_H_ 1

#include "thrift/lib/cpp/async/TUnframedAsyncChannel.h"

namespace apache { namespace thrift { namespace async {

namespace detail {

/**
 * A class to determine the end of a raw TBinaryProtocol message.
 */
class TBinaryACProtocolTraits {
 public:
  TBinaryACProtocolTraits() : strictRead_(true) {}

  // Methods required by TUnframedACReadState
  bool getMessageLength(uint8_t* buffer,
                        uint32_t bufferLength,
                        uint32_t* messageLength);

  // Methods specific to TBinaryAsyncChannel

  void setStrictRead(bool strictRead) {
    strictRead_ = strictRead;
  }
  bool getStrictRead() const {
    return strictRead_;
  }

 private:
  bool strictRead_;
};

} // namespace detail

/**
 * TBinaryAsyncChannel
 *
 * This is a TAsyncChannel implementation that reads and writes raw (unframed)
 * messages encoded using TBinaryProtocol.
 */
class TBinaryAsyncChannel :
  public TUnframedAsyncChannel<detail::TBinaryACProtocolTraits> {
 private:
  typedef TUnframedAsyncChannel<detail::TBinaryACProtocolTraits> Parent;

 public:
  TBinaryAsyncChannel(const boost::shared_ptr<TAsyncTransport>& transport)
    : Parent(transport) {}

  /**
   * Helper function to create a shared_ptr<TBinaryAsyncChannel>.
   *
   * This passes in the correct destructor object, since TBinaryAsyncChannel's
   * destructor is protected and cannot be invoked directly.
   */
  static boost::shared_ptr<TBinaryAsyncChannel> newChannel(
      const boost::shared_ptr<TAsyncTransport>& transport) {
    return boost::shared_ptr<TBinaryAsyncChannel>(
        new TBinaryAsyncChannel(transport), Destructor());
  }

  // Note that we inherit setMaxMessageSize() and getMaxMessageSize()
  // from TUnframedAsyncChannel.

  void setStrictRead(bool strictRead) {
    readState_.getProtocolTraits()->setStrictRead(strictRead);
  }
  bool getStrictRead() const {
    return readState_.getProtocolTraits()->getStrictRead();
  }

 protected:
  /**
   * Protected destructor.
   *
   * Users of TBinaryAsyncChannel must never delete it directly.  Instead,
   * invoke destroy().
   */
  virtual ~TBinaryAsyncChannel() { }
};

class TBinaryAsyncChannelFactory : public TStreamAsyncChannelFactory {
 public:
  TBinaryAsyncChannelFactory()
    : maxMessageSize_(0x7fffffff)
    , recvTimeout_(0)
    , sendTimeout_(0)
    , strictRead_(true) {}

  void setMaxMessageSize(uint32_t bytes) {
    maxMessageSize_ = bytes;
  }

  void setRecvTimeout(uint32_t milliseconds) {
    recvTimeout_ = milliseconds;
  }

  void setSendTimeout(uint32_t milliseconds) {
    sendTimeout_ = milliseconds;
  }

  void setStrictRead(bool strict) {
    strictRead_ = strict;
  }

  virtual boost::shared_ptr<TAsyncEventChannel> newChannel(
      const boost::shared_ptr<TAsyncTransport>& transport) {
    boost::shared_ptr<TBinaryAsyncChannel> channel(
        TBinaryAsyncChannel::newChannel(transport));
    transport->setSendTimeout(sendTimeout_);
    channel->setMaxMessageSize(maxMessageSize_);
    channel->setRecvTimeout(recvTimeout_);
    channel->setStrictRead(strictRead_);
    return channel;
  }

 private:
  uint32_t maxMessageSize_;
  uint32_t recvTimeout_;
  uint32_t sendTimeout_;
  bool strictRead_;
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TBINARYASYNCCHANNEL_H_
