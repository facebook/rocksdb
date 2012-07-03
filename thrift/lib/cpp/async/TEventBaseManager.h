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
#ifndef THRIFT_ASYNC_TEVENTBASEMANAGER_H
#define THRIFT_ASYNC_TEVENTBASEMANAGER_H 1

#include "thrift/lib/cpp/concurrency/ThreadLocal.h"

namespace apache { namespace thrift { namespace async {

class TEventBase;

/**
 * Manager for per-thread TEventBase objects.
 *   This class will find or create a TEventBase for the current
 *   thread, associated with thread-specific storage for that thread.
 *   Although a typical application will generally only have one
 *   TEventBaseManager, there is no restriction on multiple instances;
 *   the TEventBases belong to one instance are isolated from those of
 *   another.
 */
class TEventBaseManager {
 public:
  TEventBaseManager() {}
  ~TEventBaseManager() {}

  /**
   * Get the TEventBase for this thread, or create one if none exists yet.
   *
   * If no TEventBase exists for this thread yet, a new one will be created and
   * returned.  May throw std::bad_alloc if allocation fails.
   */
  TEventBase* getEventBase() const {
    // localStore_.get() will never return NULL.
    // InfoManager::allocate() will throw an exception instead if it cannot
    // allocate a new EventBaseInfo or TEventBase.
    return localStore_.get()->eventBase;
  }

  /**
   * Get the TEventBase for this thread.
   *
   * Returns NULL if no TEventBase has been created for this thread yet.
   */
  TEventBase* getExistingEventBase() const {
    EventBaseInfo* info = localStore_.getNoAlloc();
    if (info == NULL) {
      return NULL;
    }
    return info->eventBase;
  }

  /**
   * Set the TEventBase to be used by this thread.
   *
   * This may only be called if no TEventBase has been defined for this thread
   * yet.  If a TEventBase is already defined for this thread, a
   * TLibraryException is thrown.  std::bad_alloc may also be thrown if
   * allocation fails while setting the TEventBase.
   *
   * This should typically be invoked by the code that will call loop() on the
   * TEventBase, to make sure the TEventBaseManager points to the correct
   * TEventBase that is actually running in this thread.
   */
  void setEventBase(TEventBase *eventBase, bool takeOwnership);

  /**
   * Clear the TEventBase for this thread.
   *
   * This can be used if the code driving the TEventBase loop() has finished
   * the loop and new events should no longer be added to the TEventBase.
   */
  void clearEventBase();

 private:
  struct EventBaseInfo {
    EventBaseInfo(TEventBase *evb, bool owned)
      : eventBase(evb),
        owned(owned) {}

    TEventBase *eventBase;
    bool owned;
  };

  class InfoManager {
   public:
    EventBaseInfo* allocate();
    void destroy(EventBaseInfo* info);

    void replace(EventBaseInfo* oldInfo, EventBaseInfo* newInfo) {
      if (oldInfo != newInfo) {
        destroy(oldInfo);
      }
    }
  };

  // Forbidden copy constructor and assignment opererator
  TEventBaseManager(TEventBaseManager const &);
  TEventBaseManager& operator=(TEventBaseManager const &);

  concurrency::ThreadLocal<EventBaseInfo, InfoManager> localStore_;
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TEVENTBASEMANAGER_H
