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
#ifndef THRIFT_ASYNC_TASYNCTIMEOUT_H_
#define THRIFT_ASYNC_TASYNCTIMEOUT_H_ 1

#include "thrift/lib/cpp/thrift_config.h"
#include "thrift/lib/cpp/async/TEventUtil.h"
#include <boost/noncopyable.hpp>

namespace apache { namespace thrift { namespace async {

class TEventBase;

/**
 * TAsyncTimeout is used to asynchronously wait for a timeout to occur.
 */
class TAsyncTimeout : private boost::noncopyable {
 public:
  enum InternalEnum {
    INTERNAL,
    NORMAL
  };

  /**
   * Create a new TAsyncTimeout object, driven by the specified TEventBase.
   */
  explicit TAsyncTimeout(TEventBase* eventBase);

  /**
   * Create a new internal TAsyncTimeout object.
   *
   * Internal timeouts are like regular timeouts, but will not stop the
   * TEventBase loop from exiting if the only remaining events are internal
   * timeouts.
   *
   * This is useful for implementing fallback timeouts to abort the TEventBase
   * loop if the other events have not been processed within a specified time
   * period: if the event loop takes too long the timeout will fire and can
   * stop the event loop.  However, if all other events complete, the event
   * loop will exit even though the internal timeout is still installed.
   */
  TAsyncTimeout(TEventBase* eventBase, InternalEnum internal);

  /**
   * Create a new TAsyncTimeout object, not yet assigned to a TEventBase.
   *
   * attachEventBase() must be called prior to scheduling the timeout.
   */
  TAsyncTimeout();

  /**
   * TAsyncTimeout destructor.
   *
   * The timeout will be automatically cancelled if it is running.
   */
  virtual ~TAsyncTimeout();

  /**
   * timeoutExpired() is invoked when the timeout period has expired.
   */
  virtual void timeoutExpired() THRIFT_NOEXCEPT = 0;

  /**
   * Schedule the timeout to fire in the specified number of milliseconds.
   *
   * After the specified number of milliseconds has elapsed, timeoutExpired()
   * will be invoked by the TEventBase's main loop.
   *
   * If the timeout is already running, it will be rescheduled with the
   * new timeout value.
   *
   * @param milliseconds  The timeout duration, in milliseconds.
   *
   * @return Returns true if the timeout was successfully scheduled,
   *         and false if an error occurred.  After an error, the timeout is
   *         always unscheduled, even if scheduleTimeout() was just
   *         rescheduling an existing timeout.
   */
  bool scheduleTimeout(uint32_t milliseconds);

  /**
   * Cancel the timeout, if it is running.
   */
  void cancelTimeout();

  /**
   * Returns true if the timeout is currently scheduled.
   */
  bool isScheduled() {
    return TEventUtil::isEventRegistered(&event_);
  }

  /**
   * Attach the timeout to a TEventBase.
   *
   * This may only be called if the timeout is not currently attached to a
   * TEventBase (either by using the default constructor, or by calling
   * detachEventBase()).
   *
   * This method must be invoked in the TEventBase's thread.
   *
   * The internal parameter specifies if this timeout should be treated as an
   * internal event.  TEventBase::loop() will return when there are no more
   * non-internal events remaining.
   */
  void attachEventBase(TEventBase* eventBase,
                       InternalEnum internal = NORMAL);

  /**
   * Detach the timeout from its TEventBase.
   *
   * This may only be called when the timeout is not running.
   * Once detached, the timeout may not be scheduled again until it is
   * re-attached to a TEventBase by calling attachEventBase().
   *
   * This method must be called from the current TEventBase's thread.
   */
  void detachEventBase();

 private:
  static void libeventCallback(int fd, short events, void* arg);

  struct event event_;

  /*
   * In debug builds, store a pointer to the TEventBase.  We only use this
   * for some assert() statements, to make sure that TAsyncTimeout is always
   * used from the correct thread.
   */
#ifndef NDEBUG
  TEventBase* eventBase_;
#endif
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TASYNCTIMEOUT_H_
