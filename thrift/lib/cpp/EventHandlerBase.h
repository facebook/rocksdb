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

#ifndef THRIFT_EVENTHANDLERBASE_H_
#define THRIFT_EVENTHANDLERBASE_H_ 1

#include <string>
#include <vector>
#include <memory>
#include "thrift/lib/cpp/server/TConnectionContext.h"
#include <boost/shared_ptr.hpp>

namespace apache { namespace thrift {

using server::TConnectionContext;

/**
 * Virtual interface class that can handle events from the processor. To
 * use this you should subclass it and implement the methods that you care
 * about. Your subclass can also store local data that you may care about,
 * such as additional "arguments" to these methods (stored in the object
 * instance's state).
 */
class TProcessorEventHandler {
 public:

  virtual ~TProcessorEventHandler() {}

  /**
   * Called before calling other callback methods.
   * Expected to return some sort of context object.
   * The return value is passed to all other callbacks
   * for that function invocation.
   */
  virtual void* getContext(const char* fn_name,
                           TConnectionContext* connectionContext) {
    return NULL;
  }

  /**
   * Expected to free resources associated with a context.
   */
  virtual void freeContext(void* ctx, const char* fn_name) { }

  /**
   * Called before reading arguments.
   */
  virtual void preRead(void* ctx, const char* fn_name) {}

  /**
   * Called between reading arguments and calling the handler.
   */
  virtual void postRead(void* ctx, const char* fn_name, uint32_t bytes) {}

  /**
   * Called between calling the handler and writing the response.
   */
  virtual void preWrite(void* ctx, const char* fn_name) {}

  /**
   * Called after writing the response.
   */
  virtual void postWrite(void* ctx, const char* fn_name, uint32_t bytes) {}

  /**
   * Called when an async function call completes successfully.
   */
  virtual void asyncComplete(void* ctx, const char* fn_name) {}

  /**
   * Called if the handler throws an undeclared exception.
   */
  virtual void handlerError(void* ctx, const char* fn_name) {}

 protected:
  TProcessorEventHandler() {}
};

/**
 * A helper class used by the generated code to free each context.
 */
class TProcessorContextFreer {
 public:
  TProcessorContextFreer(boost::shared_ptr<TProcessorEventHandler> handler,
                         void* context, const char* method) :
    handler_(handler), context_(context), method_(method) {}
  ~TProcessorContextFreer() {
    if (handler_ != NULL) {
      handler_->freeContext(context_, method_);
    }
  }

  void unregister() { handler_.reset(); }

 private:
  boost::shared_ptr<TProcessorEventHandler> handler_;
  void* context_;
  const char* method_;
};

class ContextStack {
  friend class EventHandlerBase;

 public:
  ContextStack(
    const std::vector<boost::shared_ptr<TProcessorEventHandler>>& handlers,
    const char* method,
    TConnectionContext* connectionContext)
      : handlers_(handlers)
      , method_(method) {
    for (auto handler: handlers) {
      ctxs.push_back(handler->getContext(method, connectionContext));
    }
  }

  ~ContextStack() {
    for (size_t i = 0; i < handlers_.size(); i++) {
      handlers_[i]->freeContext(ctxs[i], method_);
    }
  }

 private:
  std::vector<void*> ctxs;
  std::vector<boost::shared_ptr<TProcessorEventHandler>> handlers_;
  const char* method_;
};

class EventHandlerBase {
 private:
  int setEventHandlerPos_;
  ContextStack* s_;

 public:
  EventHandlerBase()
      : setEventHandlerPos_(-1)
      , s_(NULL)
    {}

  void addEventHandler(
      const boost::shared_ptr<TProcessorEventHandler>& handler) {
    handlers_.push_back(handler);
  }

  void clearEventHandlers() {
    handlers_.clear();
    setEventHandlerPos_ = -1;
    if (eventHandler_) {
      setEventHandler(eventHandler_);
    }
  }

  boost::shared_ptr<TProcessorEventHandler> getEventHandler() {
    return eventHandler_;
  }

  void setEventHandler(boost::shared_ptr<TProcessorEventHandler> eventHandler) {
    eventHandler_ = eventHandler;
    if (setEventHandlerPos_ > 0) {
      handlers_.erase(handlers_.begin() + setEventHandlerPos_);
    }
    setEventHandlerPos_ = handlers_.size();
    handlers_.push_back(eventHandler);
  }

  /**
   * These functions are only used in the client handler
   * implementation.  The server process functions maintain
   * ContextStack on the stack and binds ctx in to the async calls.
   *
   * Clients are not thread safe, so using a member variable is okay.
   * Client send_ and recv_ functions contain parameters based off of
   * the function call, and adding a parameter there would change the
   * function signature enough that other thrift users might break.
   *
   * The generated code should be the ONLY user of s_.  All other functions
   * should just use the ContextStack parameter.
   */
  ContextStack* getContextStack() {
    return s_;
  }

  // Context only freed by freer, this is only used across function calls.
  void setContextStack(ContextStack* s) {
    s_ = s;
  }

 protected:
  std::unique_ptr<ContextStack> getContextStack(
      const char* fn_name,
      TConnectionContext* connectionContext) {
    std::unique_ptr<ContextStack> ctx(
        new ContextStack(handlers_, fn_name, connectionContext));
    return ctx;
  }

  void preWrite(ContextStack* s, const char* fn_name) {
    if (s) {
      for (size_t  i = 0; i < handlers_.size(); i++) {
        handlers_[i]->preWrite(s->ctxs[i], fn_name);
      }
    }
  }

  void postWrite(ContextStack* s, const char* fn_name,
                 uint32_t bytes) {
    if (s) {
      for (size_t i = 0; i < handlers_.size(); i++) {
        handlers_[i]->postWrite(s->ctxs[i], fn_name, bytes);
      }
    }
  }

  void preRead(ContextStack* s, const char* fn_name) {
    if (s) {
      for (size_t i = 0; i < handlers_.size(); i++) {
        handlers_[i]->preRead(s->ctxs[i], fn_name);
      }
    }
  }

  void postRead(ContextStack* s, const char* fn_name,
                uint32_t bytes) {
    if (s) {
      for (size_t i = 0; i < handlers_.size(); i++) {
        handlers_[i]->postRead(s->ctxs[i], fn_name, bytes);
      }
    }
  }

  void handlerError(ContextStack* s, const char* fn_name) {
    if (s) {
      for (size_t i = 0; i < handlers_.size(); i++) {
        handlers_[i]->handlerError(s->ctxs[i], fn_name);
      }
    }
  }

  void asyncComplete(ContextStack* s, const char* fn_name) {
    if (s) {
      for (size_t i = 0; i < handlers_.size(); i++) {
        handlers_[i]->asyncComplete(s->ctxs[i], fn_name);
      }
    }
  }

  std::vector<boost::shared_ptr<TProcessorEventHandler>> handlers_;
  boost::shared_ptr<TProcessorEventHandler> eventHandler_;
};

class TProcessorEventHandlerFactory {
 public:
  virtual boost::shared_ptr<TProcessorEventHandler> getEventHandler() = 0;
};

/**
 * Base class for all thrift processors. Used to automatically attach event
 * handlers to processors at creation time.
 */
class TProcessorBase : public EventHandlerBase {
 public:
  TProcessorBase();

  static void addProcessorEventHandlerFactory(
    boost::shared_ptr<TProcessorEventHandlerFactory> factory);

  static void removeProcessorEventHandlerFactory(
    boost::shared_ptr<TProcessorEventHandlerFactory> factory);

 private:
  static std::vector<boost::shared_ptr<TProcessorEventHandlerFactory>>
    registeredHandlerFactories_;
};

/**
 * Base class for all thrift clients. Used to automatically attach event
 * handlers to clients at creation time.
 */
class TClientBase : public EventHandlerBase {
 public:
  TClientBase();

  static void addClientEventHandlerFactory(
    boost::shared_ptr<TProcessorEventHandlerFactory> factory);

  static void removeClientEventHandlerFactory(
    boost::shared_ptr<TProcessorEventHandlerFactory> factory);

 private:
  static std::vector<boost::shared_ptr<TProcessorEventHandlerFactory>>
    registeredHandlerFactories_;
};

}} // apache::thrift

#endif // #ifndef THRIFT_EVENTHANDLERBASE_H_
