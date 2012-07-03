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
#ifndef THRIFT_ASYNC_TSTREAMASYNCCHANNEL_H_
#define THRIFT_ASYNC_TSTREAMASYNCCHANNEL_H_ 1

#include "thrift/lib/cpp/async/TAsyncEventChannel.h"
#include "thrift/lib/cpp/async/TAsyncTransport.h"
#include "thrift/lib/cpp/async/TAsyncTimeout.h"

#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift { namespace async {

class TAsyncTransport;

template <class Subclass_>
class TAsyncChannelWriteRequestBase {
 public:
  typedef std::tr1::function<void()> VoidCallback;

  TAsyncChannelWriteRequestBase(const VoidCallback& callback,
                                const VoidCallback& errorCallback,
                                transport::TMemoryBuffer* message)
      : buffer_(message),
        next_(NULL),
        callback_(callback),
        errorCallback_(errorCallback) {

    // The WriteRequest's buffer consumes all of the data in message,
    // so we don't attempt to resend data; yet is also an observer
    // which prevents consumed data from being overwritten while it's pending
    // for the transport
    uint32_t len = message->available_read();
    message->borrow(NULL, &len);
    message->consume(len);
  }

  virtual ~TAsyncChannelWriteRequestBase() {
  }

  void setNext(Subclass_* next) {
    assert(next_ == NULL);
    next_ = next;
  }

  Subclass_* getNext() const {
    return next_;
  }

 protected:
  apache::thrift::transport::TMemoryBuffer buffer_;

  void invokeCallback() {
    // unlink the buffer before invoking the callback, since we are
    // now done with it.  Not strictly required but faster.
    buffer_.unlink();
    callback_();
  }

  void invokeErrorCallback() {
    // unlink the buffer before invoking the callback, since we are
    // now done with it.  Not strictly required but faster.
    buffer_.unlink();
    errorCallback_();
  }

 private:
  TAsyncChannelWriteRequestBase();

  Subclass_* next_;

  VoidCallback callback_;
  VoidCallback errorCallback_;
};


/**
 * TStreamAsyncChannel is a helper class for channel implementations that use
 * TAsyncTransport underneath.
 *
 * TStreamAsyncChannel provides the basic functionality for implementing a
 * message-based asynchronous channel on top of a streaming TAsyncTransport.
 *
 * It requires two template arguments that control how the stream is broken up
 * into messagess:
 *
 * WriteRequest_:
 *
 *   This template parameter controls how messages are written to the
 *   underlying stream.  It must implement the following methods:
 *
 *   - WriteRequest_(const VoidCallback& callback,
 *                   const VoidCallback& errorCallback,
 *                   transport::TMemoryBuffer* message);
 *
 *       The WriteRequest_ constructor accepts the success and error callbacks,
 *       and the TMemoryBuffer containing the data to send.  The WriteRequest_
 *       may consume data from the message, but does not own the TMemoryBuffer
 *       (i.e., it should not delete the TMemoryBuffer.)
 *
 *   - void setNext(WriteRequest_* next);
 *   - WriteRequest_* getNext() const;
 *
 *       These two methods support chaining together a list of WriteRequest_
 *       objects.  This is used when multiple write requests are pending on the
 *       channel.
 *
 *   - void write(TAsyncTransport* transport,
 *                TAsyncTransport::WriteCallback* callback) THRIFT_NOEXCEPT;
 *
 *       This method will be called to schedule the write.  The WriteRequest_
 *       should invoke the transport's write() or writev() method with the data
 *       to send, and set the specified callback as the transport callback.
 *
 *       Note that this API requires the WriteRequest_ to write the entire
 *       message with a single write() or writev() call.  This allows the code
 *       to let the TAsyncTransport perform the write queuing when multiple
 *       messages are pending.  (If needed we could rewrite this API in the
 *       future to relax this restriction.)
 *
 *   - void writeSuccess() THRIFT_NOEXCEPT;
 *   - void writeError(size_t bytesWritten,
 *                     const TTransportException& ex) THRIFT_NOEXCEPT;
 *
 *       Either writeSuccess() or writeError() will be invoked once the message
 *       write has completed.
 *
 * ReadState_:
 *
 *   This template parameter controls how the incoming stream is broken up into
 *   individual messages.  It must implement the following methods:
 *
 *   - ReadState_();
 *
 *     The ReadState_ constructor takes no arguments.
 *
 *   - void setCallbackBuffer(transport::TMemoryBuffer* buffer);
 *
 *     When a new read is started, setCallbackBuffer() is called to set the
 *     buffer into which the message data should be placed.
 *
 *   - void unsetCallbackBuffer();
 *
 *     unsetCallbackBuffer() is called to clear the callback buffer when after
 *     a full message has been read.
 *
 *   - bool hasReadAheadData();
 *
 *     Some ReadState_ implementations may perform read-ahead, and read past
 *     the end of the message when reading from the underlying transport.
 *     hasReadAheadData() is called when a new read starts, to see if the
 *     ReadState_ has pending data for a new message that has already been read
 *     from the transport.
 *
 *     If hasReadAheadData() returns true, readDataAvailable(0) will be called
 *     immediately, rather than waiting for new data from the transport.
 *
 *   - bool hasPartialMessage();
 *
 *     When EOF is read from the underlying transport, hasPartialMessage() is
 *     called to see if the EOF should be treated as an error or a normal
 *     close.  (It is an error if hasPartialMessage() returns true.)
 *
 *   - void getReadBuffer(void** bufReturn, size_t* lenReturn);
 *
 *     When data becomes available on the underlying transport, getReadBuffer()
 *     is called to get the buffer where the data should be placed.
 *
 *   - bool readDataAvailable(size_t len);
 *
 *     readDataAvailable() is called when new data has been read from the
 *     underlying transport.  The data will have been placed in the buffer
 *     returned by the previous getReadBuffer() call.
 */
template<typename WriteRequest_, typename ReadState_>
class TStreamAsyncChannel : public TAsyncEventChannel,
                            protected TAsyncTransport::ReadCallback,
                            protected TAsyncTransport::WriteCallback,
                            protected TAsyncTimeout {
 public:
  TStreamAsyncChannel(const boost::shared_ptr<TAsyncTransport>& transport);

  /**
   * Helper function to create a shared_ptr<TStreamAsyncChannel>.
   *
   * This passes in the correct destructor object, since TStreamAsyncChannel's
   * destructor is protected and cannot be invoked directly.
   */
  static boost::shared_ptr<TStreamAsyncChannel> newChannel(
      const boost::shared_ptr<TAsyncTransport>& transport) {
    return boost::shared_ptr<TStreamAsyncChannel>(
        new TStreamAsyncChannel(transport), Destructor());
  }

  /**
   * Destroy the channel.
   *
   * destroy() must be called to destroy the channel.  The normal destructor
   * is private, and should not be invoked directly.  This prevents callers
   * from deleting a TStreamAsyncChannel while it is invoking a callback.
   */
  virtual void destroy();

  // Methods inherited from TAsyncEventChannel
  virtual bool readable() const;
  virtual bool good() const;
  virtual bool error() const;
  virtual bool timedOut() const;

  /**
   * Send a message to the channel; note that "errorCob" will be called
   * after a partial write as well as other errors.  We will call "errorCob"
   * immediately (before return) if the channel is unusable for some reason,
   * and "cob" immediately if we're able to perform the write without delay.
   */
  virtual void sendMessage(const VoidCallback& cob,
                           const VoidCallback& errorCob,
                           apache::thrift::transport::TMemoryBuffer* message);

  /**
   * Receive a message from the channel; note that "errorCob" will be called
   * after a partial read as well as other errors.  We will call "errorCob"
   * immediately (before return) if the channel is unusable for some reason,
   * and "cob" immediately if we're able to perform the read without delay.
   *
   * Note that an EOF is considered normal, so "cob" will be called although
   * "good()" will be false.
   */
  virtual void recvMessage(const VoidCallback& cob,
                           const VoidCallback& errorCob,
                           apache::thrift::transport::TMemoryBuffer* message);

  /**
   * Send a message to the channel and receive the response; note that the
   * "errorCob: will be called after a write error and no receive is attempted.
   * Also, a partial write or read will result in errorCob being called.
   * We call "errorCob" before return if the channel is unusable for some
   * reason. It is conceivable that "cob" will be called before return if data
   * is somehow available in the channel when a read is first attempted.
   */
  virtual void sendAndRecvMessage(
                            const VoidCallback& cob,
                            const VoidCallback& errorCob,
                            transport::TMemoryBuffer* sendBuf,
                            transport::TMemoryBuffer* recvBuf);

  /**
   * Close this channel.
   *
   * This gracefully closes the channel, waiting for all pending send
   * requests to complete before actually closing the underlying transport.
   *
   * If a recvMessage() call is pending, it will be immediately failed.
   */
  void close();

  /**
   * Close the channel immediately.
   *
   * This closes the channel immediately, dropping any outstanding messages
   * waiting to be sent.
   *
   * If a recvMessage() call is pending, it will be immediately failed.
   */
  void closeNow();

  /**
   * Attach the channel to a TEventBase.
   *
   * This may only be called if the channel is not currently attached to a
   * TEventBase (by an earlier call to detachEventBase()).
   *
   * This method must be invoked in the TEventBase's thread.
   */
  void attachEventBase(TEventBase* eventBase);

  /**
   * Detach the channel from its TEventBase.
   *
   * This may only be called when the channel is idle and has no reads or
   * writes pending.  Once detached, the channel may not be used again until it
   * is re-attached to a TEventBase by calling attachEventBase().
   *
   * This method must be called from the current TEventBase's thread.
   */
  void detachEventBase();

  /**
   * Get the TEventBase used by this channel.
   */
  TEventBase* getEventBase() const;

  /**
   * Set the timeout for receiving messages.
   *
   * When set to a non-zero value, the entire message must be received within
   * the specified number of milliseconds, or the receive will fail and the
   * channel will be closed.
   *
   * If setRecvTimeout() is invoked while a recvMessage() call is currently in
   * progress, the timeout will be restarted using the new value.
   */
  void setRecvTimeout(uint32_t milliseconds);

  /**
   * Get the receive timeout.
   *
   * @return Returns the current receive timeout, in milliseconds.  A return
   *         value of 0 indicates that no timeout is set.
   */
  uint32_t getRecvTimeout() const {
    return recvTimeout_;
  }

  /**
   * Get the TAsyncTransport used by this channel.
   */
  virtual boost::shared_ptr<TAsyncTransport> getTransport() {
    return transport_;
  }

  /**
   * Determine if this channel is idle (i.e., has no outstanding reads or
   * writes).
   */
  bool isIdle() const {
    return (writeReqHead_ == NULL) && (!readCallback_) &&
      !transport_->connecting();
  }

 protected:
  struct ReadQueueEntry {
    ReadQueueEntry(const VoidCallback& cob,
                   const VoidCallback& errorCob,
                   apache::thrift::transport::TMemoryBuffer* message) {
      readCallback = cob;
      readErrorCallback = errorCob;
      readBuffer = message;
    }
    VoidCallback readCallback;
    VoidCallback readErrorCallback;
    transport::TMemoryBuffer *readBuffer;
    int64_t startTime;
  };

  /**
   * Protected destructor.
   *
   * Users of TStreamAsyncChannel must never delete it directly.  Instead,
   * invoke destroy().
   */
  virtual ~TStreamAsyncChannel() {}

  // callbacks from TAsyncTransport
  void getReadBuffer(void** bufReturn, size_t* lenReturn);
  void readDataAvailable(size_t len) THRIFT_NOEXCEPT;
  void readEOF() THRIFT_NOEXCEPT;
  void readError(const transport::TTransportException& ex) THRIFT_NOEXCEPT;

  void writeSuccess() THRIFT_NOEXCEPT;
  void writeError(size_t bytesWritten,
                  const transport::TTransportException& ex) THRIFT_NOEXCEPT;

  // callback from TAsyncTimeout
  void timeoutExpired() THRIFT_NOEXCEPT;

  bool invokeReadDataAvailable(size_t len) THRIFT_NOEXCEPT;
  void processReadEOF() THRIFT_NOEXCEPT;
  void invokeReadCallback(VoidCallback cb,
                          char const* callbackName) THRIFT_NOEXCEPT;

  void pushWriteRequest(WriteRequest_* req) {
    if (writeReqTail_ == NULL) {
      assert(writeReqHead_ == NULL);
      writeReqHead_ = req;
    } else {
      writeReqTail_->setNext(req);
    }
    writeReqTail_ = req;
  }

  WriteRequest_* popWriteRequest() {
    assert(writeReqHead_ != NULL);

    WriteRequest_* req = writeReqHead_;
    writeReqHead_ = req->getNext();
    if (writeReqHead_ == NULL) {
      assert(writeReqTail_ == req);
      writeReqTail_ = NULL;
    }
    return req;
  }

  void failAllReads();

  boost::shared_ptr<TAsyncTransport> transport_;
  WriteRequest_* writeReqHead_;
  WriteRequest_* writeReqTail_;

  ReadState_ readState_;
  VoidCallback readCallback_;
  VoidCallback readErrorCallback_;
  std::list<ReadQueueEntry> readCallbackQ_;

  uint32_t recvTimeout_;
  // true if a timeout has occurred
  bool timedOut_;

 private:
  // Forbidden copy constructor and assignment opererator
  TStreamAsyncChannel(TStreamAsyncChannel const &);
  TStreamAsyncChannel& operator=(TStreamAsyncChannel const &);
};

class TStreamAsyncChannelFactory {
 public:
  virtual ~TStreamAsyncChannelFactory() {}

  virtual boost::shared_ptr<TAsyncEventChannel> newChannel(
      const boost::shared_ptr<TAsyncTransport>& transport) = 0;
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TSTREAMASYNCCHANNEL_H_
