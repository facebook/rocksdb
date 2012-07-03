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
#ifndef THRIFT_ASYNC_TNOTIFICATIONPIPE_H
#define THRIFT_ASYNC_TNOTIFICATIONPIPE_H 1

#include "thrift/lib/cpp/async/TDelayedDestruction.h"
#include "thrift/lib/cpp/async/TEventBase.h"
#include "thrift/lib/cpp/async/TEventHandler.h"
#include "thrift/lib/cpp/concurrency/Mutex.h"
#include <boost/shared_ptr.hpp>
#include <exception>
#include <limits.h>

namespace apache { namespace thrift { namespace async {

/**
 * A simple notification pipe for sending messages to a TEventBase thread.
 *
 * TNotificationPipe is a unidirectional pipe for sending small, atomic
 * messages.
 *
 * TNotificationPipe cannot be send messages larger than a fixed size.
 * TNotificationPipe::kMaxMessageSize defines the maximum message size
 * supported.  If you need to pass larger amounts of data between threads,
 * consider just passing a pointer to the data over the pipe, and using some
 * external mechanism to synchronize management of the memory.
 *
 *
 * TNotificationPipe provides two parallel APIs for writing and closing the
 * pipe: a thread-safe version and a non-thread-safe version.  Which version to
 * use depends on how the caller uses the pipe:
 *
 * - If there is only a single writer thread, you can use the non-thread-safe
 *   versions of trySendMessage() and close().  This guarantees close() is
 *   never called by one thread while another thread is attempting to send a
 *   message.
 *
 * - If there are multiple writers, but the pipe is never closed by the
 *   writers, you can use the non-thread-safe version of trySendMessage().
 *   Multiple simultaneous trySendMessage() calls will not interfere with each
 *   other.  Since none of the writer threads call close, a call to close()
 *   cannot be running simultaneously with a write attempt.  (With this model,
 *   the TNotificationPipe is never closed until it is destroyed.  It is up to
 *   the caller to ensure the TNotificationPipe is not destroyed while write
 *   threads still have a pointer or reference to it.)
 *
 * In other circumstances (if one thread may call close while another thread is
 * simultaneously trying to write), the thread-safe versions
 * trySendMessageSync() and closeSync() must be used.
 */
class TNotificationPipe : public TDelayedDestruction,
                          private TEventHandler,
                          private TEventBase::LoopCallback {
 public:
  /**
   * A callback interface for receiving notification of messages from the pipe.
   */
  class Callback {
   public:
    virtual ~Callback() {}

    /**
     * notificationMessage() will be invoked whenever a new
     * message is available from the pipe.
     */
    virtual void notificationMessage(const void *msg, uint32_t msgSize) = 0;

    /**
     * notificationPipeError() will be invoked if an error occurs while reading
     * from the pipe.  Before notificationPipeError() is invoked, the read
     * callback will automatically be uninstalled and the pipe will be closed.
     */
    virtual void notificationPipeError(const std::exception& ex) = 0;

    /**
     * notificationPipeClosed() is invoked in the read thread after the write
     * end of the pipe is closed.
     */
    virtual void notificationPipeClosed() = 0;
  };

  /**
   * Helper function to create a new shared_ptr<TNotificationPipe>.
   *
   * This simply sets the correct destructor to call destroy() instead of
   * directly deleting the TNotificationPipe.
   */
  static boost::shared_ptr<TNotificationPipe> newPipe(TEventBase *base) {
    return boost::shared_ptr<TNotificationPipe>(new TNotificationPipe(base),
                                                Destructor());
  }

  /**
   * Create a new TNotificationPipe.
   *
   * @param eventBase  The TEventBase to use for receiving read notifications
   *     from this pipe.  All read events will be processed in this
   *     TEventBase's thread.  trySendMessage() may be called from any thread.
   */
  TNotificationPipe(TEventBase *eventBase);

  /**
   * Destroy this TNotificationPipe.
   *
   * This method may only be called from the read thread.
   *
   * This will automatically close the pipe if it is not already closed.
   */
  virtual void destroy();

  /**
   * Close the pipe.
   *
   * This version of close() is not thread-safe.  It should only be used if the
   * caller is sure no other thread is attempting to write a message at the
   * same time.
   *
   * Use closeSync() if other threads may be attempting to send a message
   * simultaneously.  The other threads must use also use the thread-safe
   * trySendMessageSync() or trySendFrameSync() calls.
   */
  void close();

  /**
   * A thread-safe version of close().
   */
  void closeSync();

  /**
   * Send a message over the pipe.
   *
   * trySendMessage() is best-effort.  It will either immediately succeed to
   * send the message, or it will fail immediately if the pipe reader is too
   * busy and it's backlog of unread messages is too large.
   *
   * trySendMessage() also does not support arbitrarily large messages.
   * It will also fail immediately if msgSize is larger than (PIPE_BUF - 4).
   *
   * If trySendMessage() succeeds, the message is guaranteed to be delivered to
   * the pipe reader, except in the case where the pipe reader explicitly stops
   * reading and destroys the pipe before processing all of its messages.
   *
   * On failure a TTransportException is thrown. The error code will be
   * TTransportException::BAD_ARGS if the message is too large,
   * TTransportException::TIMED_OUT if the message cannot be sent right now
   * because the pipe is full, or TTransportException::NOT_OPEN if the pipe has
   * already been closed.
   *
   * This method is thread safe with other simultaneous trySendMessage() calls,
   * but not with close() calls.  Use trySendMessageSync() and closeSync() if a
   * close may occur simultaneously on another thread.
   */
  void trySendMessage(const void *msg, uint32_t msgSize);

  /**
   * A thread-safe version of trySendMessage().
   *
   * This may be called simultaneously with closeSync().
   */
  void trySendMessageSync(const void *msg, uint32_t msgSize);

  /**
   * Send a message over the pipe.
   *
   * This is identical to trySendMessage(), except that the caller must provide
   * 4 bytes at the beginning of the message where we can write a frame length.
   * This allows us to avoid copying the message into a new buffer.
   * (trySendMessage() always has to make a copy of the message.)
   *
   * @param frame  A pointer to the frame buffer.  trySendFrame() will
   *     overwrite the first 4 bytes of this buffer.  When the read callback
   *     receives the message, it will not see these first 4 bytes.
   * @param frameSize  The full size of the frame buffer.  This must be at
   *     least 4 bytes long.  The actual message size that will be sent is
   *     frameSize - 4.
   */
  void trySendFrame(void *frame, uint32_t frameSize);

  /**
   * A thread-safe version of trySendFrame().
   *
   * This may be called simultaneously with closeSync().
   */
  void trySendFrameSync(void *frame, uint32_t frameSize);

  /**
   * Get the number of messages which haven't been processed.
   */
  int64_t getNumNotProcessed() const {
    return numInputs_ - numOutputs_;
  }

  /**
   * Set the callback to receive read notifications from this pipe.
   *
   * This method must be invoked from the pipe's read thread.
   *
   * May throw TLibraryException on error.  The callback will always be unset
   * (NULL) after an error.
   */
  void setReadCallback(Callback *callback);

  /**
   * Mark the pipe read event handler as an "internal" event handler.
   *
   * This causes the notification pipe not to be counted when determining if
   * the TEventBase has any more active events to wait on.  This is intended to
   * be used only be internal TEventBase code.  This API is not guaranteed to
   * remain stable or portable in the future.
   *
   * May throw TLibraryException if it fails to re-register its event handler
   * with the correct flags.
   */
  void setInternal(bool internal);

  /**
   * Get the maximum number of messages that will be read on a single iteration
   * of the event loop.
   */
  uint32_t getMaxReadAtOnce() const {
    return maxReadAtOnce_;
  }

  /**
   * Set the maximum number of messages to read each iteration of the event
   * loop.
   *
   * If messages are being received faster than they can be processed, this
   * helps limit the rate at which they will be read.  This can be used to
   * prevent the notification pipe reader from starving other users of the
   * event loop.
   */
  void setMaxReadAtOnce(uint32_t numMessages) {
    maxReadAtOnce_ = numMessages;
  }

  /**
   * The maximum message size that can be sent over a TNotificationPipe.
   *
   * This restriction ensures that trySendMessage() can send all messages
   * atomically.  This is (PIPE_BUF - 4) bytes.  (On Linux, this is 4092
   * bytes.)
   */
  static const uint32_t kMaxMessageSize = PIPE_BUF - 4;

  /**
   * The default maximum number of messages that will be read each time around
   * the event loop.
   *
   * This value used for each TNotificationPipe can be changed using the
   * setMaxReadAtOnce() method.
   */
  static const uint32_t kDefaultMaxReadAtOnce = 10;

 private:
  enum ReadAction {
    kDoNothing,
    kContinue,
    kWaitForRead,
    kRunInNextLoop,
  };

  // Forbidden copy constructor and assignment opererator
  TNotificationPipe(TNotificationPipe const &);
  TNotificationPipe& operator=(TNotificationPipe const &);

  // TEventHandler methods
  virtual void handlerReady(uint16_t events) THRIFT_NOEXCEPT;

  // TEventBase::LoopCallback methods
  virtual void runLoopCallback() THRIFT_NOEXCEPT;

  void initPipe();
  void registerPipeEvent();
  void readMessages(ReadAction action);
  ReadAction performRead();
  ReadAction processReadData(uint32_t* messagesProcessed);
  ReadAction handleError(const char* fmt, ...)
    __attribute__((format(printf, 2, 3)));
  void checkMessage(uint32_t msgSize);
  void writeFrame(const void *frame, uint32_t frameSize);

  TEventBase *eventBase_;
  Callback *readCallback_;
  int readPipe_;
  int writePipe_;
  bool internal_;
  uint32_t maxReadAtOnce_;
  int64_t numInputs_;
  int64_t numOutputs_;

  /**
   * Mutex for guarding numInputs_
   */
  concurrency::Mutex numInputsMutex_;

  /**
   * A mutex that guards writePipe_.
   *
   * This is used by closeSync(), trySendMessageSync(), and trySendFrameSync(),
   * since trySendMessageSync() and trySendFrameSync() read writePipe_
   * and closeSync() resets it to -1.
   */
  concurrency::NoStarveReadWriteMutex writePipeMutex_;

  /**
   * A pointer to the end of valid read data in the read buffer.
   */
  uint8_t *readPtr_;
  /**
   * An internal read buffer
   *
   * This is large enough to contain the maximum possible message plus the
   * mssage length.
   */
  uint8_t readBuffer_[kMaxMessageSize + 4];
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TNOTIFICATIONPIPE_H
