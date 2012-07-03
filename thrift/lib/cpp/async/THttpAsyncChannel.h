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
#ifndef THRIFT_ASYNC_THTTPASYNCCHANNEL_H_
#define THRIFT_ASYNC_THTTPASYNCCHANNEL_H_ 1

#include "thrift/lib/cpp/async/TStreamAsyncChannel.h"
#include "thrift/lib/cpp/util/THttpParser.h"

namespace apache { namespace thrift { namespace async {

class THttpAsyncChannel;

namespace detail {

/**
 * Encapsulation of one outstanding write request on a THttpAsyncChannel.
 */
class THttpACWriteRequest :
      public TAsyncChannelWriteRequestBase<THttpACWriteRequest> {
 public:
  typedef std::tr1::function<void()> VoidCallback;

  THttpACWriteRequest(const VoidCallback& callback,
                      const VoidCallback& errorCallback,
                      transport::TMemoryBuffer* message,
                      TAsyncEventChannel* channel);

  void write(TAsyncTransport* transport,
             TAsyncTransport::WriteCallback* callback) THRIFT_NOEXCEPT;

  void writeSuccess() THRIFT_NOEXCEPT;
  void writeError(size_t bytesWritten,
                  const transport::TTransportException& ex) THRIFT_NOEXCEPT;

 private:
  char lengthBuf_[64];
  THttpAsyncChannel* channel_;
};

/**
 * Read state for THttpAsyncChannel
 */
class THttpACReadState {
 public:
  typedef std::tr1::function<void()> VoidCallback;

  THttpACReadState() {
  }

  // Methods required by TStreamAsyncChannel

  void setCallbackBuffer(transport::TMemoryBuffer* buffer) {
    parser_->setDataBuffer(buffer);
  }
  void unsetCallbackBuffer() {
    parser_->unsetDataBuffer();
  }

  bool hasReadAheadData() {
    return parser_->hasReadAheadData();
  }
  bool hasPartialMessage() {
    return parser_->hasPartialMessage();
  }

  void getReadBuffer(void** bufReturn, size_t* lenReturn);
  bool readDataAvailable(size_t len);

  // Other methods specific to THttpAsyncChannel
  void setParser(boost::shared_ptr<apache::thrift::util::THttpParser> parser) {
    parser_ = parser;
  }
 private:
  boost::shared_ptr<apache::thrift::util::THttpParser> parser_;
};

} // namespace detail

/**
 * THttpAsyncChannel
 *
 * This is a TAsyncChannel implementation that reads and writes messages
 * encapuated in HTTP.
 *
 * Its messages are compatible with THttpTransport.
 */
class THttpAsyncChannel :
  public TStreamAsyncChannel<detail::THttpACWriteRequest,
                             detail::THttpACReadState> {
 private:
  typedef TStreamAsyncChannel<detail::THttpACWriteRequest,
                              detail::THttpACReadState> Parent;
  boost::shared_ptr<apache::thrift::util::THttpParser> parser_;

 public:
  explicit THttpAsyncChannel(
    const boost::shared_ptr<TAsyncTransport>& transport)
    : Parent(transport) {
  }

  /**
   * Helper function to create a shared_ptr<THttpAsyncChannel>.
   *
   * This passes in the correct destructor object, since THttpAsyncChannel's
   * destructor is protected and cannot be invoked directly.
   */
  static boost::shared_ptr<THttpAsyncChannel> newChannel(
      const boost::shared_ptr<TAsyncTransport>& transport) {
    return boost::shared_ptr<THttpAsyncChannel>(
        new THttpAsyncChannel(transport), Destructor());
  }

  /// size in bytes beyond which we'll reject a given http size.
  void setMaxHttpSize(uint32_t size) {
    parser_->setMaxSize(size);
  }

  uint32_t getMaxHttpSize() const {
    return parser_->getMaxSize();
  }

  void setParser(boost::shared_ptr<apache::thrift::util::THttpParser> parser) {
    parser_ = parser;
    readState_.setParser(parser);
  }

  boost::shared_ptr<apache::thrift::util::THttpParser> getParser() const {
    return parser_;
  }

  int constructHeader(iovec* ops,
                      int opsLen,
                      int contentLength,
                      char* contentLengthBuf) {
    return parser_->constructHeader(ops,
                                    opsLen,
                                    contentLength,
                                    contentLengthBuf);
  }

 protected:
  /**
   * Protected destructor.
   *
   * Users of THttpAsyncChannel must never delete it directly.  Instead,
   * invoke destroy().
   */
  virtual ~THttpAsyncChannel() { }
};

class THttpAsyncChannelFactory : public TStreamAsyncChannelFactory {
 public:
  THttpAsyncChannelFactory()
    : maxHttpSize_(0x7fffffff)
    , recvTimeout_(0)
    , sendTimeout_(0) {}

  void setMaxHttpSize(uint32_t bytes) {
    maxHttpSize_ = bytes;
  }

  void setRecvTimeout(uint32_t milliseconds) {
    recvTimeout_ = milliseconds;
  }

  void setSendTimeout(uint32_t milliseconds) {
    sendTimeout_ = milliseconds;
  }

  virtual boost::shared_ptr<TAsyncEventChannel> newChannel(
      const boost::shared_ptr<TAsyncTransport>& transport) {
    boost::shared_ptr<THttpAsyncChannel> channel(
        THttpAsyncChannel::newChannel(transport));
    transport->setSendTimeout(sendTimeout_);
    channel->setMaxHttpSize(maxHttpSize_);
    channel->setRecvTimeout(recvTimeout_);
    return channel;
  }

 private:
  uint32_t maxHttpSize_;
  uint32_t recvTimeout_;
  uint32_t sendTimeout_;
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_THTTPASYNCCHANNEL_H_
