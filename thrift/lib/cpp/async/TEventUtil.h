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
#ifndef THRIFT_ASYNC_TEVENTUTIL_H_
#define THRIFT_ASYNC_TEVENTUTIL_H_ 1

#include <event.h>  // libevent

namespace apache { namespace thrift { namespace async {

/**
 * low-level libevent utility functions
 */
class TEventUtil {
 public:
  static bool isEventRegistered(const struct event* ev) {
    // If any of these flags are set, the event is registered.
    enum {
      EVLIST_REGISTERED = (EVLIST_INSERTED | EVLIST_ACTIVE |
                           EVLIST_TIMEOUT | EVLIST_SIGNAL)
    };
    return (ev->ev_flags & EVLIST_REGISTERED);
  }
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TEVENTUTIL_H_
