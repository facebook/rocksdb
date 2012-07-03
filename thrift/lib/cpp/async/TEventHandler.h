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
#ifndef THRIFT_ASYNC_TEVENTHANDLER_H_
#define THRIFT_ASYNC_TEVENTHANDLER_H_ 1

#include "thrift/lib/cpp/thrift_config.h"
#include "thrift/lib/cpp/async/TEventUtil.h"
#include <boost/noncopyable.hpp>
#include <stddef.h>

namespace apache { namespace thrift { namespace async {

class TEventBase;

/**
 * The TEventHandler class is used to asynchronously wait for events on a file
 * descriptor.
 *
 * Users that wish to wait on I/O events should derive from TEventHandler and
 * implement the handlerReady() method.
 */
class TEventHandler : private boost::noncopyable {
 public:
  enum EventFlags {
    NONE = 0,
    READ = EV_READ,
    WRITE = EV_WRITE,
    READ_WRITE = (READ | WRITE),
    PERSIST = EV_PERSIST
  };

  /**
   * Create a new TEventHandler object.
   *
   * @param eventBase  The TEventBase to use to drive this event handler.
   *                   This may be NULL, in which case the TEventBase must be
   *                   set separately using initHandler() or attachEventBase()
   *                   before the handler can be registered.
   * @param fd         The file descriptor that this TEventHandler will
   *                   monitor.  This may be -1, in which case the file
   *                   descriptor must be set separately using initHandler() or
   *                   changeHandlerFD() before the handler can be registered.
   */
  explicit TEventHandler(TEventBase* eventBase = NULL, int fd = -1);

  /**
   * TEventHandler destructor.
   *
   * The event will be automatically unregistered if it is still registered.
   */
  virtual ~TEventHandler();

  /**
   * handlerReady() is invoked when the handler is ready.
   *
   * @param events  A bitset indicating the events that are ready.
   */
  virtual void handlerReady(uint16_t events) THRIFT_NOEXCEPT = 0;

  /**
   * Register the handler.
   *
   * If the handler is already registered, the registration will be updated
   * to wait on the new set of events.
   *
   * @param events   A bitset specifying the events to monitor.
   *                 If the PERSIST bit is set, the handler will remain
   *                 registered even after handlerReady() is called.
   *
   * @return Returns true if the handler was successfully registered,
   *         or false if an error occurred.  After an error, the handler is
   *         always unregistered, even if it was already registered prior to
   *         this call to registerHandler().
   */
  bool registerHandler(uint16_t events) {
    return registerImpl(events, false);
  }

  /**
   * Unregister the handler, if it is registered.
   */
  void unregisterHandler();

  /**
   * Returns true if the handler is currently registered.
   */
  bool isHandlerRegistered() const {
    return TEventUtil::isEventRegistered(&event_);
  }

  /**
   * Attach the handler to a TEventBase.
   *
   * This may only be called if the handler is not currently attached to a
   * TEventBase (either by using the default constructor, or by calling
   * detachEventBase()).
   *
   * This method must be invoked in the TEventBase's thread.
   */
  void attachEventBase(TEventBase* eventBase);

  /**
   * Detach the handler from its TEventBase.
   *
   * This may only be called when the handler is not currently registered.
   * Once detached, the handler may not be registered again until it is
   * re-attached to a TEventBase by calling attachEventBase().
   *
   * This method must be called from the current TEventBase's thread.
   */
  void detachEventBase();

  /**
   * Change the file descriptor that this handler is associated with.
   *
   * This may only be called when the handler is not currently registered.
   */
  void changeHandlerFD(int fd);

  /**
   * Attach the handler to a TEventBase, and change the file descriptor.
   *
   * This method may only be called if the handler is not currently attached to
   * a TEventBase.  This is primarily intended to be used to initialize
   * TEventHandler objects created using the default constructor.
   */
  void initHandler(TEventBase* eventBase, int fd);

  /**
   * Return the set of events that we're currently registered for.
   */
  uint16_t getRegisteredEvents() const {
    return (isHandlerRegistered()) ?
      event_.ev_events : 0;
  }

  /**
   * Register the handler as an internal event.
   *
   * This event will not count as an active event for determining if the
   * TEventBase loop has more events to process.  The TEventBase loop runs
   * only as long as there are active TEventHandlers, however "internal" event
   * handlers are not counted.  Therefore this event handler will not prevent
   * TEventBase loop from exiting with no more work to do if there are no other
   * non-internal event handlers registered.
   *
   * This is intended to be used only in very rare cases by the internal
   * TEventBase code.  This API is not guaranteed to remain stable or portable
   * in the future.
   */
  bool registerInternalHandler(uint16_t events) {
    return registerImpl(events, true);
  }

 private:
  bool registerImpl(uint16_t events, bool internal);
  void ensureNotRegistered(const char* fn);

  static void libeventCallback(int fd, short events, void* arg);

  struct event event_;
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TEVENTHANDLER_H_
