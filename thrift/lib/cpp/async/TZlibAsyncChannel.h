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
#ifndef THRIFT_ASYNC_TZLIBASYNCCHANNEL_H_
#define THRIFT_ASYNC_TZLIBASYNCCHANNEL_H_ 1

#include "thrift/lib/cpp/async/TAsyncEventChannel.h"
#include "thrift/lib/cpp/transport/TZlibTransport.h"

namespace apache { namespace thrift { namespace async {

class TZlibAsyncChannel : public TAsyncEventChannel {
 public:
  TZlibAsyncChannel(const boost::shared_ptr<TAsyncEventChannel>& channel);

  /**
   * Helper function to create a shared_ptr<TZlibAsyncChannel>.
   *
   * This passes in the correct destructor object, since TZlibAsyncChannel's
   * destructor is protected and cannot be invoked directly.
   */
  static boost::shared_ptr<TZlibAsyncChannel> newChannel(
      const boost::shared_ptr<TAsyncEventChannel>& channel) {
    return boost::shared_ptr<TZlibAsyncChannel>(
        new TZlibAsyncChannel(channel), Destructor());
  }
  virtual bool readable() const {
    return channel_->readable();
  }
  virtual bool good() const {
    return channel_->good();
  }
  virtual bool error() const {
    return channel_->error();
  }
  virtual bool timedOut() const {
    return channel_->timedOut();
  }
  virtual bool isIdle() const {
    return channel_->isIdle();
  }

  virtual void sendMessage(const VoidCallback& cob,
                           const VoidCallback& errorCob,
                           transport::TMemoryBuffer* message);
  virtual void recvMessage(const VoidCallback& cob,
                           const VoidCallback& errorCob,
                           transport::TMemoryBuffer* message);
  virtual void sendAndRecvMessage(const VoidCallback& cob,
                                  const VoidCallback& errorCob,
                                  transport::TMemoryBuffer* sendBuf,
                                  transport::TMemoryBuffer* recvBuf);

  virtual boost::shared_ptr<TAsyncTransport> getTransport() {
    return channel_->getTransport();
  }

  virtual void attachEventBase(TEventBase* eventBase) {
    channel_->attachEventBase(eventBase);
  }
  virtual void detachEventBase() {
    channel_->detachEventBase();
  }

  virtual uint32_t getRecvTimeout() const {
    return channel_->getRecvTimeout();
  }

  virtual void setRecvTimeout(uint32_t milliseconds) {
    channel_->setRecvTimeout(milliseconds);
  }

 protected:
  /**
   * Protected destructor.
   *
   * Users of TZlibAsyncChannel must never delete it directly.  Instead,
   * invoke destroy().
   */
  virtual ~TZlibAsyncChannel() { }

 private:
  class SendRequest {
   public:
    SendRequest();

    bool isSet() const {
      return static_cast<bool>(callback_);
    }

    void set(const VoidCallback& callback,
             const VoidCallback& errorCallback,
             transport::TMemoryBuffer* message);

    void send(TAsyncEventChannel* channel);

   private:
    void invokeCallback(VoidCallback callback);
    void sendSuccess();
    void sendError();

    boost::shared_ptr<transport::TMemoryBuffer> compressedBuffer_;
    transport::TZlibTransport zlibTransport_;
    VoidCallback sendSuccess_;
    VoidCallback sendError_;

    VoidCallback callback_;
    VoidCallback errorCallback_;
  };

  class RecvRequest {
   public:
    RecvRequest();

    bool isSet() const {
      return static_cast<bool>(callback_);
    }

    void set(const VoidCallback& callback,
             const VoidCallback& errorCallback,
             transport::TMemoryBuffer* message);

    void recv(TAsyncEventChannel* channel);

   private:
    void invokeCallback(VoidCallback callback);
    void recvSuccess();
    void recvError();

    boost::shared_ptr<transport::TMemoryBuffer> compressedBuffer_;
    transport::TZlibTransport zlibTransport_;
    VoidCallback recvSuccess_;
    VoidCallback recvError_;

    VoidCallback callback_;
    VoidCallback errorCallback_;
    transport::TMemoryBuffer *callbackBuffer_;
  };

  boost::shared_ptr<TAsyncEventChannel> channel_;

  // TODO: support multiple pending send requests
  SendRequest sendRequest_;
  RecvRequest recvRequest_;
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TZLIBASYNCCHANNEL_H_
