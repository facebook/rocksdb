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
#ifndef THRIFT_TEST_SCOPEDEVENTBASETHREAD_H_
#define THRIFT_TEST_SCOPEDEVENTBASETHREAD_H_ 1

#include <boost/shared_ptr.hpp>
#include <memory>

namespace apache { namespace thrift { namespace async {
class TEventBase;
}}}
namespace apache { namespace thrift { namespace concurrency {
class Thread;
}}}

namespace apache { namespace thrift { namespace test {

/**
 * A helper class to start a new thread running a TEventBase loop.
 *
 * The new thread will be started by the ScopedEventBaseThread constructor.
 * When the ScopedEventBaseThread object is destroyed, the thread will be
 * stopped.
 */
class ScopedEventBaseThread {
 public:
  ScopedEventBaseThread();
  ~ScopedEventBaseThread();

  ScopedEventBaseThread(ScopedEventBaseThread&& other);
  ScopedEventBaseThread &operator=(ScopedEventBaseThread&& other);

  /**
   * Get a pointer to the TEventBase driving this thread.
   */
  async::TEventBase *getEventBase() const {
    return eventBase_.get();
  }

 private:
  ScopedEventBaseThread(const ScopedEventBaseThread& other) = delete;
  ScopedEventBaseThread& operator=(const ScopedEventBaseThread& other) = delete;

  std::unique_ptr<async::TEventBase> eventBase_;
  boost::shared_ptr<concurrency::Thread> thread_;
};

}}} // apache::thrift::test

#endif // THRIFT_TEST_SCOPEDEVENTBASETHREAD_H_
