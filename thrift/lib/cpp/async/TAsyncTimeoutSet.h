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
#ifndef THRIFT_ASYNC_TASYNCTIMEOUTSET_H_
#define THRIFT_ASYNC_TASYNCTIMEOUTSET_H_ 1

#include "thrift/lib/cpp/async/TAsyncTimeout.h"
#include "thrift/lib/cpp/async/TDelayedDestruction.h"

#include <chrono>
#include <cstddef>
#include <memory>

namespace apache { namespace thrift { namespace async {

/**
 * TAsyncTimeoutSet exists for efficiently managing a group of timeouts events
 * that always have the same timeout interval.
 *
 * TAsyncTimeoutSet takes advantage of the fact that the timeouts are always
 * scheduled in sorted order.  (Since each timeout has the same interval, when
 * a new timeout is scheduled it will always be the last timeout in the set.)
 * This avoids the need to perform any additional sorting of the timeouts
 * within a single TAsyncTimeoutSet.
 *
 * TAsyncTimeoutSet is useful whenever you have a large group of objects that
 * each need their own timeout, but with the same interval for each object.
 * For example, managing idle timeouts for thousands of connection, or
 * scheduling health checks for a large group of servers.
 */
class TAsyncTimeoutSet : private TAsyncTimeout, public TDelayedDestruction {
 public:
  typedef std::unique_ptr<TAsyncTimeoutSet, Destructor> UniquePtr;

  /**
   * A callback to be notified when a timeout has expired.
   *
   * TAsyncTimeoutSet::Callback is very similar to TAsyncTimeout.  The primary
   * distinction is that TAsyncTimeout can choose its timeout interval each
   * time it is scheduled.  On the other hand, TAsyncTimeoutSet::Callback
   * always uses the timeout interval defined by the TAsyncTimeoutSet where it
   * is scheduled.
   */
  class Callback {
   public:
    Callback()
      : timeoutSet_(NULL),
        expiration_(0),
        prev_(NULL),
        next_(NULL) {}

    virtual ~Callback();

    /**
     * timeoutExpired() is invoked when the timeout has expired.
     */
    virtual void timeoutExpired() THRIFT_NOEXCEPT = 0;

    /**
     * Cancel the timeout, if it is running.
     *
     * If the timeout is not scheduled, cancelTimeout() does nothing.
     */
    void cancelTimeout() {
      if (timeoutSet_ == NULL) {
        // We're not scheduled, so there's nothing to do.
        return;
      }
      cancelTimeoutImpl();
    }

    /**
     * Return true if this timeout is currently scheduled, and false otherwise.
     */
    bool isScheduled() const {
      return timeoutSet_ != NULL;
    }

   private:
    // Get the time remaining until this timeout expires
    std::chrono::milliseconds getTimeRemaining(
          std::chrono::milliseconds now) const {
      if (now >= expiration_) {
        return std::chrono::milliseconds(0);
      }
      return expiration_ - now;
    }

    void setScheduled(TAsyncTimeoutSet* timeoutSet, Callback* prev);
    void cancelTimeoutImpl();

    TAsyncTimeoutSet* timeoutSet_;
    std::chrono::milliseconds expiration_;
    Callback* prev_;
    Callback* next_;

    // Give TAsyncTimeoutSet direct access to our members so it can take care
    // of scheduling/cancelling.
    friend class TAsyncTimeoutSet;
  };

  /**
   * Create a new TAsyncTimeoutSet with the specified interval.
   */
  TAsyncTimeoutSet(TEventBase* eventBase,
                   std::chrono::milliseconds intervalMS,
                   std::chrono::milliseconds atMostEveryN =
                      std::chrono::milliseconds(0));

  /**
   * Destroy the TAsyncTimeoutSet.
   *
   * Normally a TAsyncTimeoutSet should only be destroyed when there are no
   * more callbacks pending in the set.  If there are timeout callbacks pending
   * for this set, destroying the TAsyncTimeoutSet will automatically cancel
   * them.  If you destroy a TAsyncTimeoutSet with callbacks pending, your
   * callback code needs to be aware that the callbacks will never be invoked.
   */
  virtual void destroy();

  /**
   * Get the interval for this TAsyncTimeoutSet.
   *
   * Returns the timeout interval in milliseconds.  All callbacks scheduled
   * with scheduleTimeout() will be invoked after this amount of time has
   * passed since the call to scheduleTimeout().
   */
  std::chrono::milliseconds getInterval() const {
    return interval_;
  }

  /**
   * Schedule the specified Callback to be invoked after the TAsyncTimeoutSet's
   * specified timeout interval.
   *
   * If the callback is already scheduled, this cancels the existing timeout
   * before scheduling the new timeout.
   */
  void scheduleTimeout(Callback* callback);

  /**
   * Limit how frequently this TAsyncTimeoutSet will fire.
   */
  void fireAtMostEvery(const std::chrono::milliseconds& ms) {
    atMostEveryN_ = ms;
  }

  /**
   * Get a pointer to the next Callback scheduled to be invoked (may be null).
   */
  Callback* front() { return head_; }
  const Callback* front() const { return head_; }

 protected:
  /**
   * Protected destructor.
   *
   * Use destroy() instead.  See the comments in TDelayedDestruction for more
   * details.
   */
  virtual ~TAsyncTimeoutSet();

 private:
  // Forbidden copy constructor and assignment operator
  TAsyncTimeoutSet(TAsyncTimeoutSet const &) = delete;
  TAsyncTimeoutSet& operator=(TAsyncTimeoutSet const &) = delete;

  // Private methods to be invoked by TAsyncTimeoutSet::Callback
  void headChanged();

  // Methods inherited from TAsyncTimeout
  virtual void timeoutExpired() THRIFT_NOEXCEPT;

  std::chrono::milliseconds interval_;
  std::chrono::milliseconds atMostEveryN_;
  bool inTimeoutExpired_;
  Callback* head_;
  Callback* tail_;
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TASYNCTIMEOUTSET_H_
