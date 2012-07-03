// Copyright (c) 2006- Facebook
// Distributed under the Thrift Software License
//
// See accompanying file LICENSE or visit the Thrift site at:
// http://developers.facebook.com/thrift/

#ifndef THRIFT_ASYNC_TEVENTBASE_H_
#define THRIFT_ASYNC_TEVENTBASE_H_ 1

#include "thrift/lib/cpp/Thrift.h"
#include "thrift/lib/cpp/async/TAsyncTimeout.h"
#include "thrift/lib/cpp/server/TServer.h"
#include "thrift/lib/cpp/transport/TTransportUtils.h"
#include "thrift/lib/cpp/concurrency/ThreadManager.h"
#include <memory>
#include <stack>
#include <list>
#include <queue>
#include <cstdlib>
#include <set>
#include <utility>
#include <boost/intrusive/list.hpp>
#include <boost/utility.hpp>
#include <tr1/functional>
#include <event.h>  // libevent
#include <errno.h>
#include <math.h>

namespace apache { namespace thrift { namespace async {

typedef std::tr1::function<void()> Cob;
template <typename MessageT>
class TNotificationQueue;

/**
 * This class is a wrapper for all asynchronous I/O processing functionality
 * used in thrift.
 *
 * TEventBase provides a main loop that notifies TEventHandler callback objects
 * when I/O is ready on a file descriptor, and notifies TAsyncTimeout objects
 * when a specified timeout has expired.  More complex, higher-level callback
 * mechanisms can then be built on top of TEventHandler and TAsyncTimeout.
 *
 * A TEventBase object can only drive an event loop for a single thread.  To
 * take advantage of multiple CPU cores, most asynchronous I/O servers have one
 * thread per CPU, and use a separate TEventBase for each thread.
 *
 * In general, most TEventBase methods may only be called from the thread
 * running the TEventBase's loop.  There are a few exceptions to this rule, for
 * methods that are explicitly intended to allow communication with a
 * TEventBase from other threads.  When it is safe to call a method from
 * another thread it is explicitly listed in the method comments.
 */
class TEventBase : private boost::noncopyable {
 public:
  /**
   * A callback interface to use with runInLoop()
   *
   * Derive from this class if you need to delay some code execution until the
   * next iteration of the event loop.  This allows you to schedule code to be
   * invoked from the top-level of the loop, after your immediate callers have
   * returned.
   *
   * If a LoopCallback object is destroyed while it is scheduled to be run in
   * the next loop iteration, it will automatically be cancelled.
   */
  class LoopCallback {
   public:
    virtual ~LoopCallback() {}

    virtual void runLoopCallback() THRIFT_NOEXCEPT = 0;
    void cancelLoopCallback() {
      hook_.unlink();
    }

    bool isLoopCallbackScheduled() const {
      return hook_.is_linked();
    }

   private:
    typedef boost::intrusive::list_member_hook<
      boost::intrusive::link_mode<boost::intrusive::auto_unlink> > ListHook;

    ListHook hook_;

    typedef boost::intrusive::list<
      LoopCallback,
      boost::intrusive::member_hook<LoopCallback, ListHook,
                                    &LoopCallback::hook_>,
      boost::intrusive::constant_time_size<false> > List;

    // TEventBase needs access to LoopCallbackList (and therefore to hook_)
    friend class TEventBase;
  };

  /**
   * Create a new TEventBase object.
   */
  TEventBase();

  /**
   * Create a new TEventBase object that will use the specified libevent
   * event_base object to drive the event loop.
   *
   * The TEventBase will take ownership of this event_base, and will call
   * event_base_free(evb) when the TEventBase is destroyed.
   */
  explicit TEventBase(event_base* evb);
  ~TEventBase();

  /**
   * Runs the event loop.
   *
   * loop() will loop waiting for I/O or timeouts and invoking TEventHandler
   * and TAsyncTimeout callbacks as their events become ready.  loop() will
   * only return when there are no more events remaining to process, or after
   * terminateLoopSoon() has been called.
   *
   * loop() may be called again to restart event processing after a previous
   * call to loop() or loopForever() has returned.
   *
   * Returns true if the loop completed normally (if it processed all
   * outstanding requests, or if terminateLoopSoon() was called).  If an error
   * occurs waiting for events, false will be returned.
   */
  bool loop();

  /**
   * Runs the event loop.
   *
   * loopForever() behaves like loop(), except that it keeps running even if
   * when there are no more user-supplied TEventHandlers or TAsyncTimeouts
   * registered.  It will only return after terminateLoopSoon() has been
   * called.
   *
   * This is useful for callers that want to wait for other threads to call
   * runInEventBaseThread(), even when there are no other scheduled events.
   *
   * loopForever() may be called again to restart event processing after a
   * previous call to loop() or loopForever() has returned.
   *
   * Throws a TLibraryException if an error occurs.
   */
  void loopForever();

  /**
   * Causes the event loop to exit soon.
   *
   * This will cause an existing call to loop() or loopForever() to stop event
   * processing and return, even if there are still events remaining to be
   * processed.
   *
   * It is safe to call terminateLoopSoon() from another thread to cause loop()
   * to wake up and return in the TEventBase loop thread.  terminateLoopSoon()
   * may also be called from the loop thread itself (for example, a
   * TEventHandler or TAsyncTimeout callback may call terminateLoopSoon() to
   * cause the loop to exit after the callback returns.)
   *
   * Note that the caller is responsible for ensuring that cleanup of all event
   * callbacks occurs properly.  Since terminateLoopSoon() causes the loop to
   * exit even when there are pending events present, there may be remaining
   * callbacks present waiting to be invoked.  If the loop is later restarted
   * pending events will continue to be processed normally, however if the
   * TEventBase is destroyed after calling terminateLoopSoon() it is the
   * caller's responsibility to ensure that cleanup happens properly even if
   * some outstanding events are never processed.
   */
  void terminateLoopSoon();

  /**
   * Adds the given callback to a queue of things run after the current pass
   * through the event loop completes.  Note that if this callback calls
   * runInLoop() the new callback won't be called until the main event loop
   * has gone through a cycle.
   *
   * This method may only be called from the TEventBase's thread.  This
   * essentially allows an event handler to schedule an additional callback to
   * be invoked after it returns.
   *
   * Use runInEventBaseThread() to schedule functions from another thread.
   */
  void runInLoop(LoopCallback* callback);

  /**
   * Convenience function to call runInLoop() with a tr1::function.
   *
   * This creates a LoopCallback object to wrap the tr1::function, and invoke
   * the tr1::function when the loop callback fires.  This is slightly more
   * expensive than defining your own LoopCallback, but more convenient in
   * areas that aren't performance sensitive where you just want to use
   * tr1::bind.  (tr1::bind is fairly slow on even by itself.)
   *
   * This method may only be called from the TEventBase's thread.  This
   * essentially allows an event handler to schedule an additional callback to
   * be invoked after it returns.
   *
   * Use runInEventBaseThread() to schedule functions from another thread.
   */
  void runInLoop(const Cob& c);

  /**
   * Run the specified function in the TEventBase's thread.
   *
   * This method is thread-safe, and may be called from another thread.
   *
   * If runInEventBaseThread() is called when the TEventBase loop is not
   * running, the function call will be delayed until the next time the loop is
   * started.
   *
   * If runInEventBaseThread() returns true the function has successfully been
   * scheduled to run in the loop thread.  However, if the loop is terminated
   * (and never later restarted) before it has a chance to run the requested
   * function, the function may never be run at all.  The caller is responsible
   * for handling this situation correctly if they may terminate the loop with
   * outstanding runInEventBaseThread() calls pending.
   *
   * If two calls to runInEventBaseThread() are made from the same thread, the
   * functions will always be run in the order that they were scheduled.
   * Ordering between functions scheduled from separate threads is not
   * guaranteed.
   *
   * @param fn  The function to run.  The function must not throw any
   *     exceptions.
   * @param arg An argument to pass to the function.
   *
   * @return Returns true if the function was successfully scheduled, or false
   *         if there was an error scheduling the function.
   */
  template<typename T>
  bool runInEventBaseThread(void (*fn)(T*), T* arg) {
    return runInEventBaseThread(reinterpret_cast<void (*)(void*)>(fn),
                                reinterpret_cast<void*>(arg));
  }

  bool runInEventBaseThread(void (*fn)(void*), void* arg);

  /**
   * Run the specified function in the TEventBase's thread
   *
   * This version of runInEventBaseThread() takes a tr1::function object.
   * Note that this is less efficient than the version that takes a plain
   * function pointer and void* argument, as it has to allocate memory to copy
   * the tr1::function object.
   *
   * If the TEventBase loop is terminated before it has a chance to run this
   * function, the allocated memory will be leaked.  The caller is responsible
   * for ensuring that the TEventBase loop is not terminated before this
   * function can run.
   *
   * The function must not throw any exceptions.
   */
  bool runInEventBaseThread(const std::tr1::function<void()>& fn);

  /**
   * Runs the given Cob at some time after the specified number of
   * milliseconds.  (No guarantees exactly when.)
   *
   * @return  true iff the cob was successfully registered.
   */
  bool runAfterDelay(const Cob& c, int milliseconds);

  /**
   * Set the maximum desired latency in us and provide a callback which will be
   * called when that latency is exceeded.
   */
  void setMaxLatency(int64_t maxLatency, const Cob& maxLatencyCob) {
    maxLatency_ = maxLatency;
    maxLatencyCob_ = maxLatencyCob;
  }
  /**
   * Set smoothing coefficient for loop load average; # of milliseconds
   * for exp(-1) (1/2.71828...) decay.
   */
  void setLoadAvgMsec(uint32_t ms);

  /**
   * Get the average loop time in microseconds (an exponentially-smoothed ave)
   */
  double getAvgLoopTime() const {
    return avgLoopTime_;
  }

  /**
   * Verify that current thread is the TEventBase thread, if the TEventBase is
   * running.
   *
   * This is primarily intended for debugging, to assert that functions that
   * register or unregister events are only ever invoked in the TEventBase's
   * thread.
   */
  bool isInEventBaseThread() const {
    return !running_ || pthread_equal(loopThread_, pthread_self());
  }

  // --------- interface to underlying libevent base ------------
  // Avoid using these functions if possible.  These functions are not
  // guaranteed to always be present if we ever provide alternative TEventBase
  // implementations that do not use libevent internally.
  event_base* getLibeventBase() const { return evb_; }
  static const char* getLibeventVersion() { return event_get_version(); }
  static const char* getLibeventMethod() { return event_get_method(); }

 private:
  // --------- libevent callbacks (not for client use) ------------

  /**
   * Called after a delay to break out of an idle event loop.  We need to
   * use this instead of event_base_loopexit() since the latter installs
   * an event within libevent which is queued until it expires.  Installing
   * our own timed event lets us delete it when another event causes the
   * loop to exit earlier..
   */
  static void loopTimerCallback(int fd, short which, void* arg);

  static void runTr1FunctionPtr(std::tr1::function<void()>* fn);

  // small object used as a callback arg with enough info to execute the
  // appropriate client-provided Cob
  class CobTimeout : public TAsyncTimeout {
   public:
    CobTimeout(TEventBase* b, const Cob& c) : TAsyncTimeout(b), cob_(c) {}

    virtual void timeoutExpired() THRIFT_NOEXCEPT;

   private:
    Cob cob_;

   public:
    typedef boost::intrusive::list_member_hook<
      boost::intrusive::link_mode<boost::intrusive::auto_unlink> > ListHook;

    ListHook hook;

    typedef boost::intrusive::list<
      CobTimeout,
      boost::intrusive::member_hook<CobTimeout, ListHook, &CobTimeout::hook>,
      boost::intrusive::constant_time_size<false> > List;
  };

  typedef LoopCallback::List LoopCallbackList;
  class FunctionRunner;

  // executes any callbacks queued by runInLoop()
  void runLoopCallbacks();

  void initNotificationQueue();

  CobTimeout::List pendingCobTimeouts_;

  LoopCallbackList loopCallbacks_;

  // stop_ is set by terminateLoopSoon() and is used by the main loop
  // to determine if it should exit
  bool stop_;
  // running_ is set to true while loop() is running
  bool running_;
  // The ID of the thread running the main loop.
  // Only valid while running_ is true.
  pthread_t loopThread_;

  // pointer to underlying event_base class doing the heavy lifting
  event_base* evb_;

  // A notification queue for runInEventBaseThread() to use
  // to send function requests to the TEventBase thread.
  std::unique_ptr<TNotificationQueue<std::pair<void (*)(void*), void*>>> queue_;
  std::unique_ptr<FunctionRunner> fnRunner_;

  // limit for latency in microseconds (0 disables)
  int64_t maxLatency_;

  // smoothed loop time used to invoke latency callbacks; differs from
  // avgLoopTime_ in that it's scaled down after triggering a callback
  // to reduce spamminess
  double maxLatencyLoopTime_;

  // exponentially-smoothed average loop time for latency-limiting
  double avgLoopTime_;

  // set to true if the event_base_loop(EVLOOP_ONCE) returned because
  // the loop timeout fired, rather than because it found events to process
  bool loopTimedOut_;

  // factor used for exponential smoothing of load average
  double expCoeff_;

  // callback called when latency limit is exceeded
  Cob maxLatencyCob_;

  // we'll wait this long before running deferred callbacks if the event
  // loop is idle.
  static const int kDEFAULT_IDLE_WAIT_USEC = 20000; // 20ms
};

}}} // apache::thrift::async

#endif // #ifndef THRIFT_ASYNC_TEVENTBASE_H_
