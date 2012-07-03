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
#ifndef THRIFT_ASYNC_TASYNCSIGNALHANDLER_H_
#define THRIFT_ASYNC_TASYNCSIGNALHANDLER_H_ 1

#include "thrift/lib/cpp/thrift_config.h"
#include <event.h>
#include <map>

namespace apache { namespace thrift { namespace async {

class TEventBase;

/**
 * A handler to receive notification about POSIX signals.
 *
 * TAsyncSignalHandler allows code to process signals from within a TEventBase
 * loop.  Standard signal handlers interrupt execution of the main thread, and
 * are run while the main thread is paused.  As a result, great care must be
 * taken to avoid race conditions if the signal handler has to access or modify
 * any data used by the main thread.
 *
 * TAsyncSignalHandler solves this problem by running the TAsyncSignalHandler
 * callback in normal thread of execution, as a TEventBase callback.
 *
 * TAsyncSignalHandler may only be used in a single thread.  It will only
 * process signals received by the thread where the TAsyncSignalHandler is
 * registered.  It is the user's responsibility to ensure that signals are
 * delivered to the desired thread in multi-threaded programs.
 */
class TAsyncSignalHandler {
 public:
  /**
   * Create a new TAsyncSignalHandler.
   */
  TAsyncSignalHandler(TEventBase* eventBase);
  virtual ~TAsyncSignalHandler();

  /**
   * Register to receive callbacks about the specified signal.
   *
   * Once the handler has been registered for a particular signal,
   * signalReceived() will be called each time this thread receives this
   * signal.
   *
   * Throws a TException if an error occurs, or if this handler is already
   * registered for this signal.
   */
  void registerSignalHandler(int signum);

  /**
   * Unregister for callbacks about the specified signal.
   *
   * Throws a TException if an error occurs, or if this signal was not
   * registered.
   */
  void unregisterSignalHandler(int signum);

  /**
   * signalReceived() will called to indicate that the specified signal has
   * been received.
   *
   * signalReceived() will always be invoked from the TEventBase loop (i.e.,
   * after the main POSIX signal handler has returned control to the TEventBase
   * thread).
   */
  virtual void signalReceived(int signum) THRIFT_NOEXCEPT = 0;

 private:
  typedef std::map<int, struct event> SignalEventMap;

  // Forbidden copy constructor and assignment operator
  TAsyncSignalHandler(TAsyncSignalHandler const &);
  TAsyncSignalHandler& operator=(TAsyncSignalHandler const &);

  static void libeventCallback(int signum, short events, void* arg);

  TEventBase* eventBase_;
  SignalEventMap signalEvents_;
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TASYNCSIGNALHANDLER_H_
