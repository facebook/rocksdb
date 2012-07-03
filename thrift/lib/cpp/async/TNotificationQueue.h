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
#ifndef THRIFT_ASYNC_TNOTIFICATIONQUEUE_H_
#define THRIFT_ASYNC_TNOTIFICATIONQUEUE_H_ 1

#include "thrift/lib/cpp/Thrift.h"
#include "thrift/lib/cpp/async/TEventBase.h"
#include "thrift/lib/cpp/async/TEventHandler.h"

#include "external/google_base/spinlock.h"
#include "external/glog/logging.h"
#include <deque>
#include "folly/eventfd.h"

namespace apache { namespace thrift { namespace async {

/**
 * An exception class to be thrown when a TNotificationQueue is full.
 */
class TQueueFullException : public TLibraryException {
 public:
  TQueueFullException() :
      TLibraryException("unable to add message to TNotificationQueue: "
                        "queue is full") {}
};

/**
 * A producer-consumer queue for passing messages between TEventBase threads.
 *
 * Messages can be added to the queue from any thread.  Multiple consumers may
 * listen to the queue from multiple TEventBase threads.
 *
 * A TNotificationQueue may not be destroyed while there are still consumers
 * registered to receive events from the queue.  It is the user's
 * responsibility to ensure that all consumers are unregistered before the
 * queue is destroyed.
 *
 * MessageT should be MoveConstructible (i.e., must support either a move
 * constructor or a copy constructor, or both).  Ideally it's move constructor
 * (or copy constructor if no move constructor is provided) should never throw
 * exceptions.  If the constructor may throw, the consumers could end up
 * spinning trying to move a message off the queue and failing, and then
 * retrying.
 */
template<typename MessageT>
class TNotificationQueue {
 public:
  /**
   * A callback interface for consuming messages from the queue as they arrive.
   */
  class Consumer : private TEventHandler {
   public:
    enum : uint16_t { kDefaultMaxReadAtOnce = 10 };

    Consumer()
      : queue_(NULL),
        destroyedFlagPtr_(NULL),
        maxReadAtOnce_(kDefaultMaxReadAtOnce) {}

    virtual ~Consumer();

    /**
     * messageAvailable() will be invoked whenever a new
     * message is available from the pipe.
     */
    virtual void messageAvailable(MessageT&& message) = 0;

    /**
     * Begin consuming messages from the specified queue.
     *
     * messageAvailable() will be called whenever a message is available.  This
     * consumer will continue to consume messages until stopConsuming() is
     * called.
     *
     * A Consumer may only consume messages from a single TNotificationQueue at
     * a time.  startConsuming() should not be called if this consumer is
     * already consuming.
     */
    void startConsuming(TEventBase* eventBase, TNotificationQueue* queue) {
      init(eventBase, queue);
      registerHandler(READ | PERSIST);
    }

    /**
     * Same as above but registers this event handler as internal so that it
     * doesn't count towards the pending reader count for the IOLoop.
     */
    void startConsumingInternal(
        TEventBase* eventBase, TNotificationQueue* queue) {
      init(eventBase, queue);
      registerInternalHandler(READ | PERSIST);
    }

    /**
     * Stop consuming messages.
     *
     * startConsuming() may be called again to resume consumption of messages
     * at a later point in time.
     */
    void stopConsuming();

    /**
     * Get the TNotificationQueue that this consumer is currently consuming
     * messages from.  Returns NULL if the consumer is not currently consuming
     * events from any queue.
     */
    TNotificationQueue* getCurrentQueue() const {
      return queue_;
    }

    /**
     * Set a limit on how many messages this consumer will read each iteration
     * around the event loop.
     *
     * This helps rate-limit how much work the Consumer will do each event loop
     * iteration, to prevent it from starving other event handlers.
     *
     * A limit of 0 means no limit will be enforced.  If unset, the limit
     * defaults to kDefaultMaxReadAtOnce (defined to 10 above).
     */
    void setMaxReadAtOnce(uint32_t maxAtOnce) {
      maxReadAtOnce_ = maxAtOnce;
    }
    uint32_t getMaxReadAtOnce() const {
      return maxReadAtOnce_;
    }

   private:
    void init(TEventBase* eventBase, TNotificationQueue* queue);

    virtual void handlerReady(uint16_t events) THRIFT_NOEXCEPT;

    TNotificationQueue* queue_;
    bool* destroyedFlagPtr_;
    uint32_t maxReadAtOnce_;
  };

  enum class FdType {
    EVENTFD,
    PIPE
  };

  /**
   * Create a new TNotificationQueue.
   *
   * If the maxSize parameter is specified, this sets the maximum queue size
   * that will be enforced by tryPutMessage().  (This size is advisory, and may
   * be exceeded if producers explicitly use putMessage() instead of
   * tryPutMessage().)
   *
   * The fdType parameter determines the type of file descriptor used
   * internally to signal message availability.  The default (eventfd) is
   * preferable for performance and because it won't fail when the queue gets
   * too long.  It is not available on on older and non-linux kernels, however.
   * In this case the code will fall back to using a pipe, the parameter is
   * mostly for testing purposes.
   */
  explicit TNotificationQueue(uint32_t maxSize = 0,
                              FdType fdType = FdType::EVENTFD)
    : spinlock_(),
      eventfd_(-1),
      pipeFds_({-1, -1}),
      advisoryMaxQueueSize_(maxSize),
      queue_() {
    if (fdType == FdType::EVENTFD) {
      eventfd_ = folly::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE);
      if (eventfd_ == -1) {
        if (errno == ENOSYS || errno == EINVAL) {
          // eventfd not availalble
          T_ERROR("failed to create eventfd for TNotificationQueue: %d, "
                  "falling back to pipe mode", errno);
          fdType = FdType::PIPE;
        } else {
          // some other error
          throw TLibraryException("Failed to create eventfd for "
                                  "TNotificationQueue", errno);
        }
      }
    }
    if (fdType == FdType::PIPE) {
      if (pipe(pipeFds_)) {
        throw TLibraryException("Failed to create pipe for TNotificationQueue",
                                errno);
      }
      try {
        // put both ends of the pipe into non-blocking mode
        if (fcntl(pipeFds_[0], F_SETFL, O_RDONLY | O_NONBLOCK) != 0) {
          throw TLibraryException("failed to put TNotificationQueue pipe read "
                                  "endpoint into non-blocking mode", errno);
        }
        if (fcntl(pipeFds_[1], F_SETFL, O_WRONLY | O_NONBLOCK) != 0) {
          throw TLibraryException("failed to put TNotificationQueue pipe write "
                                  "endpoint into non-blocking mode", errno);
        }
      } catch (...) {
        ::close(pipeFds_[0]);
        ::close(pipeFds_[1]);
        throw;
      }
    }
  }

  ~TNotificationQueue() {
    if (eventfd_ >= 0) {
      ::close(eventfd_);
      eventfd_ = -1;
    }
    if (pipeFds_[0] >= 0) {
      ::close(pipeFds_[0]);
      pipeFds_[0] = -1;
    }
    if (pipeFds_[1] >= 0) {
      ::close(pipeFds_[1]);
      pipeFds_[1] = -1;
    }
  }

  /**
   * Set the advisory maximum queue size.
   *
   * This maximum queue size affects calls to tryPutMessage().  Message
   * producers can still use the putMessage() call to unconditionally put a
   * message on the queue, ignoring the configured maximum queue size.  This
   * can cause the queue size to exceed the configured maximum.
   */
  void setMaxQueueSize(uint32_t max) {
    advisoryMaxQueueSize_ = max;
  }

  /**
   * Attempt to put a message on the queue if the queue is not already full.
   *
   * If the queue is full, a TQueueFullException will be thrown.  The
   * setMaxQueueSize() function controls the maximum queue size.
   *
   * This method may contend briefly on a spinlock if many threads are
   * concurrently accessing the queue, but for all intents and purposes it will
   * immediately place the message on the queue and return.
   *
   * tryPutMessage() may throw std::bad_alloc if memory allocation fails, and
   * may throw any other exception thrown by the MessageT move/copy
   * constructor.
   */
  void tryPutMessage(MessageT&& message) {
    putMessageImpl(std::move(message), advisoryMaxQueueSize_);
  }
  void tryPutMessage(const MessageT& message) {
    putMessageImpl(message, advisoryMaxQueueSize_);
  }

  /**
   * Unconditionally put a message on the queue.
   *
   * This method is like tryPutMessage(), but ignores the maximum queue size
   * and always puts the message on the queue, even if the maximum queue size
   * would be exceeded.
   *
   * putMessage() may throw std::bad_alloc if memory allocation fails, and may
   * throw any other exception thrown by the MessageT move/copy constructor.
   */
  void putMessage(MessageT&& message) {
    putMessageImpl(std::move(message), 0);
  }
  void putMessage(const MessageT& message) {
    putMessageImpl(message, 0);
  }

  /**
   * Put several messages on the queue.
   */
  template<typename InputIteratorT>
  void putMessages(InputIteratorT first, InputIteratorT last) {
    typedef typename std::iterator_traits<InputIteratorT>::iterator_category
      IterCategory;
    putMessagesImpl(first, last, IterCategory());
  }

  /**
   * Try to immediately pull a message off of the queue, without blocking.
   *
   * If a message is immediately available, the result parameter will be
   * updated to contain the message contents and true will be returned.
   *
   * If no message is available, false will be returned and result will be left
   * unmodified.
   */
  bool tryConsume(MessageT& result) {
    if (!tryConsumeEvent()) {
      return false;
    }

    try {
      facebook::SpinLockHolder guard(&spinlock_);
      result = std::move(queue_.front());
      queue_.pop_front();
    } catch (...) {
      // Handle an exception if the assignment operator happens to throw.
      // We consumed an event but weren't able to pop the message off the
      // queue.  Signal the event again since the message is still in the
      // queue.
      signalEvent(1);
      throw;
    }

    return true;
  }

 private:
  // Forbidden copy constructor and assignment operator
  TNotificationQueue(TNotificationQueue const &) = delete;
  TNotificationQueue& operator=(TNotificationQueue const &) = delete;

  inline void checkQueueSize(size_t maxSize) const {
    assert(spinlock_.IsHeld());
    if (maxSize > 0 && queue_.size() >= maxSize) {
      throw TQueueFullException();
    }
  }

  inline void signalEvent(uint64_t numAdded = 1) const {
    static const uint8_t kPipeMessage[] = {
      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
    };

    ssize_t bytes_written = 0;
    ssize_t bytes_expected = 0;
    if (eventfd_ >= 0) {
      bytes_expected = static_cast<ssize_t>(sizeof(numAdded));
      bytes_written = ::write(eventfd_, &numAdded, sizeof(numAdded));
    } else {
      // pipe semantics, add one message for each numAdded
      bytes_expected = numAdded;
      do {
        size_t messageSize = std::min(numAdded, sizeof(kPipeMessage));
        ssize_t rc = ::write(pipeFds_[1], kPipeMessage, messageSize);
        if (rc < 0) {
          // TODO: if the pipe is full, write will fail with EAGAIN.
          // See task #1044651 for how this could be handled
          break;
        }
        numAdded -= rc;
        bytes_written += rc;
      } while (numAdded > 0);
    }
    if (bytes_written != bytes_expected) {
      throw TLibraryException("failed to signal TNotificationQueue after "
                              "write", errno);
    }
  }

  bool tryConsumeEvent() {
    uint64_t value = 0;
    ssize_t rc = -1;
    if (eventfd_ >= 0) {
      rc = ::read(eventfd_, &value, sizeof(value));
    } else {
      uint8_t value8;
      rc = ::read(pipeFds_[0], &value8, sizeof(value8));
      value = value8;
    }
    if (rc < 0) {
      // EAGAIN should pretty much be the only error we can ever get.
      // This means someone else already processed the only available message.
      assert(errno == EAGAIN);
      return false;
    }
    assert(value == 1);
    return true;
  }

  void putMessageImpl(MessageT&& message, size_t maxSize) {
    {
      facebook::SpinLockHolder guard(&spinlock_);
      checkQueueSize(maxSize);
      queue_.push_back(std::move(message));
    }
    signalEvent();
  }

  void putMessageImpl(const MessageT& message, size_t maxSize) {
    {
      facebook::SpinLockHolder guard(&spinlock_);
      checkQueueSize(maxSize);
      queue_.push_back(message);
    }
    signalEvent();
  }

  template<typename InputIteratorT>
  void putMessagesImpl(InputIteratorT first, InputIteratorT last,
                       std::input_iterator_tag) {
    uint64_t numAdded = 0;
    {
      facebook::SpinLockHolder guard(&spinlock_);
      while (first != last) {
        queue_.push_back(*first);
        ++first;
        ++numAdded;
      }
    }
    signalEvent(numAdded);
  }

  template<typename InputIteratorT>
  void putMessagesImpl(InputIteratorT first, InputIteratorT last,
                       std::forward_iterator_tag) {
    uint64_t numAdded = std::distance(first, last);
    {
      facebook::SpinLockHolder guard(&spinlock_);
      queue_.insert(queue_.end(), first, last);
    }
    signalEvent(numAdded);
  }

  facebook::SpinLock spinlock_;
  int eventfd_;
  int pipeFds_[2]; // to fallback to on older/non-linux systems
  uint32_t advisoryMaxQueueSize_;
  std::deque<MessageT> queue_;
};

template<typename MessageT>
TNotificationQueue<MessageT>::Consumer::~Consumer() {
  // If we are in the middle of a call to handlerReady(), destroyedFlagPtr_
  // will be non-NULL.  Mark the value that it points to, so that
  // handlerReady() will know the callback is destroyed, and that it cannot
  // access any member variables anymore.
  if (destroyedFlagPtr_) {
    *destroyedFlagPtr_ = true;
  }
}

template<typename MessageT>
void TNotificationQueue<MessageT>::Consumer::handlerReady(uint16_t events)
    THRIFT_NOEXCEPT {
  uint32_t numProcessed = 0;
  while (true) {
    // Try to decrement the eventfd.
    //
    // We decrement the eventfd before checking the queue, and only pop a
    // message off the queue if we read from the eventfd.
    //
    // Reading the eventfd first allows us to not have to hold the spinlock
    // while accessing the eventfd.  If we popped from the queue first, we
    // would have to hold the lock while reading from or writing to the
    // eventfd.  (Multiple consumers may be woken up from a single eventfd
    // notification.  If we popped from the queue first, we could end up
    // popping a message from the queue before the eventfd has been notified by
    // the producer, unless the consumer and producer both held the spinlock
    // around the entire operation.)
    if (!queue_->tryConsumeEvent()) {
      // no message available right now
      return;
    }

    // Now pop the message off of the queue.
    // We successfully consumed the eventfd notification.
    // There should be a message available for us to consume.
    //
    // We have to manually acquire and release the spinlock here, rather than
    // using SpinLockHolder since the MessageT has to be constructed while
    // holding the spinlock and available after we release it.  SpinLockHolder
    // unfortunately doesn't provide a release() method.  (We can't construct
    // MessageT first since we have no guarantee that MessageT has a default
    // constructor.
    queue_->spinlock_.Lock();
    bool locked = true;

    try {
      // The eventfd is incremented once for every message, and only
      // decremented when a message is popped off.  There should always be a
      // message here to read.
      CHECK(!queue_->queue_.empty());

      // Pull a message off the queue.
      MessageT msg(std::move(queue_->queue_.front()));
      queue_->queue_.pop_front();

      // Check to see if the queue is empty now.
      // We use this as an optimization to see if we should bother trying to
      // loop again and read another message after invoking this callback.
      bool wasEmpty = queue_->queue_.empty();

      // Now unlock the spinlock before we invoke the callback.
      queue_->spinlock_.Unlock();
      locked = false;

      // Call the callback
      bool callbackDestroyed = false;
      CHECK(destroyedFlagPtr_ == NULL);
      destroyedFlagPtr_ = &callbackDestroyed;
      messageAvailable(std::move(msg));

      // If the callback was destroyed before it returned, we are done
      if (callbackDestroyed) {
        return;
      }
      destroyedFlagPtr_ = NULL;

      // If the callback is no longer installed, we are done.
      if (queue_ == NULL) {
        return;
      }

      // If we have hit maxReadAtOnce_, we are done.
      ++numProcessed;
      if (maxReadAtOnce_ > 0 && numProcessed >= maxReadAtOnce_) {
        return;
      }

      // If the queue was empty before we invoked the callback, it's probable
      // that it is still empty now.  Just go ahead and return, rather than
      // looping again and trying to re-read from the eventfd.  (If a new
      // message had in fact arrived while we were invoking the callback, we
      // will simply be woken up the next time around the event loop and will
      // process the message then.)
      if (wasEmpty) {
        return;
      }
    } catch (const std::exception& ex) {
      // This catch block is really just to handle the case where the MessageT
      // constructor throws.  The messageAvailable() callback itself is
      // declared as noexcept and should never throw.
      //
      // If the MessageT constructor does throw we try to handle it as best as
      // we can, but we can't work miracles.  We will just ignore the error for
      // now and return.  The next time around the event loop we will end up
      // trying to read the message again.  If MessageT continues to throw we
      // will never make forward progress and will keep trying each time around
      // the event loop.
      if (locked) {
        // Unlock the spinlock.
        queue_->spinlock_.Unlock();

        // Push a notification back on the eventfd since we didn't actually
        // read the message off of the queue.
        queue_->signalEvent(1);
      }

      return;
    }
  }
}

template<typename MessageT>
void TNotificationQueue<MessageT>::Consumer::init(
    TEventBase* eventBase,
    TNotificationQueue* queue) {
  assert(eventBase->isInEventBaseThread());
  assert(queue_ == NULL);
  assert(!isHandlerRegistered());

  queue_ = queue;
  if (queue_->eventfd_ >= 0) {
    initHandler(eventBase, queue_->eventfd_);
  } else {
    initHandler(eventBase, queue_->pipeFds_[0]);
  }
}

template<typename MessageT>
void TNotificationQueue<MessageT>::Consumer::stopConsuming() {
  if (queue_ == NULL) {
    assert(!isHandlerRegistered());
    return;
  }

  assert(isHandlerRegistered());
  unregisterHandler();
  detachEventBase();
  queue_ = NULL;
}

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TNOTIFICATIONQUEUE_H_
