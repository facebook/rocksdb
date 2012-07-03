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
#ifndef THRIFT_ASYNC_TASYNCEVENTCHANNEL_H_
#define THRIFT_ASYNC_TASYNCEVENTCHANNEL_H_ 1

#include "thrift/lib/cpp/async/TAsyncChannel.h"
#include "thrift/lib/cpp/async/TDelayedDestruction.h"

namespace apache { namespace thrift { namespace async {

class TEventBase;

/**
 * TAsyncEventChannel defines an API for TAsyncChannel objects that are driven
 * by TEventBase.
 */
class TAsyncEventChannel : public TAsyncChannel,
                           public TDelayedDestruction {
 public:

  /**
   * Determine if this channel is idle (i.e., has no outstanding reads or
   * writes).
   */
  virtual bool isIdle() const = 0;

  /**
   * Attach the channel to a TEventBase.
   *
   * This may only be called if the channel is not currently attached to a
   * TEventBase (by an earlier call to detachEventBase()).
   *
   * This method must be invoked in the TEventBase's thread.
   */
  virtual void attachEventBase(TEventBase* eventBase) = 0;

  /**
   * Detach the channel from its TEventBase.
   *
   * This may only be called when the channel is idle and has no reads or
   * writes pending.  Once detached, the channel may not be used again until it
   * is re-attached to a TEventBase by calling attachEventBase().
   *
   * This method must be called from the current TEventBase's thread.
   */
  virtual void detachEventBase() = 0;

  /**
   * Get the receive timeout.
   *
   * @return Returns the current receive timeout, in milliseconds.  A return
   *         value of 0 indicates that no timeout is set.
   */
  virtual uint32_t getRecvTimeout() const = 0;

  /**
   * Set the timeout for receiving messages.
   *
   * When set to a non-zero value, the entire message must be received within
   * the specified number of milliseconds, or the receive will fail and the
   * channel will be closed.
   */
  virtual void setRecvTimeout(uint32_t milliseconds) = 0;

 protected:
  /**
   * Protected destructor.
   *
   * Users of TAsyncEventChannel must never delete it directly. Instead, invoke
   * destroy() instead. (See the documentation in TDelayedDestruction.h for
   * more details.)
   */

  virtual ~TAsyncEventChannel() { }
};

}}} // apache::thrift::async

#endif // THRIFT_ASYNC_TASYNCEVENTCHANNEL_H_
