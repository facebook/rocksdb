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
#ifndef THRIFT_UTIL_TEVENTSERVERCREATOR_H_
#define THRIFT_UTIL_TEVENTSERVERCREATOR_H_ 1

#include "thrift/lib/cpp/util/ServerCreatorBase.h"

#include <stdint.h>

namespace apache { namespace thrift {

class TProcessor;

namespace concurrency {
class ThreadManager;
}

namespace async {
class TAsyncProcessor;
class TEventServer;
}

namespace util {

class TEventServerCreator : public ServerCreatorBase {
 public:
  typedef async::TEventServer ServerType;

  /// Use 8 IO worker threads by default.
  static const size_t DEFAULT_NUM_IO_THREADS = 8;

  /// Use 8 task worker threads by default.
  static const size_t DEFAULT_NUM_TASK_THREADS = 8;

  /// Default limit on the size of each worker's idle connection pool
  static const uint32_t DEFAULT_CONN_POOL_SIZE = 64;

  /// By default, close connections after they have are idle for 60 seconds
  static const int DEFAULT_RECV_TIMEOUT = 60000;

  /**
   * By default, reject requests over 64MB.
   *
   * This avoids allocating giant buffers if a client sends a bogus frame
   * length.
   */
  static const size_t DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;

  /**
   * Start dropping connections to reduce load if a worker's event loop begins
   * taking longer than 2 seconds to process a single event loop.
   */
  static const int64_t DEFAULT_WORKER_LATENCY = 2000;

  /// Default size of each connection's write buffer
  static const size_t DEFAULT_WRITE_BUFFER_SIZE = 1024;

  /// Default size of each connection's read buffer
  static const size_t DEFAULT_READ_BUFFER_SIZE = 1024;

  /// Maximum size of read buffer allocated to each idle connection
  static const size_t DEFAULT_IDLE_READ_BUF_LIMIT = 8192;

  /// Maximum size of write buffer allocated to each idle connection
  static const size_t DEFAULT_IDLE_WRITE_BUF_LIMIT = 8192;

  /**
   * By check to see if we should shrink oversized read/write buffers after
   * every 64 calls on connection.
   */
  static const int DEFAULT_RESIZE_EVERY_N = 64;

  /**
   * Create a new TEventServerCreator to be used for building a native-mode
   * TEventServer.
   */
  TEventServerCreator(
      const boost::shared_ptr<async::TAsyncProcessor>& asyncProcessor,
      uint16_t port,
      size_t numIoThreads = DEFAULT_NUM_IO_THREADS);

  /**
   * Create a new TEventServerCreator to be used for building a queuing-mode
   * TEventServer.
   */
  TEventServerCreator(
      const boost::shared_ptr<TProcessor>& syncProcessor,
      uint16_t port,
      size_t numIoThreads = DEFAULT_NUM_IO_THREADS,
      size_t numTaskThreads = DEFAULT_NUM_TASK_THREADS);

  /**
   * Set the number of IO threads to use.
   */
  void setNumIoThreads(size_t numIoThreads) {
    numIoThreads_ = numIoThreads;
  }

  /**
   * Set the number of task threads to use.
   */
  void setNumTaskThreads(size_t numTaskThreads) {
    numTaskThreads_ = numTaskThreads;
  }

  /**
   * Set the thread manager to use for task queue threads.
   */
  void setTaskQueueThreadManager(
      const boost::shared_ptr<concurrency::ThreadManager>& threadManager) {
    taskQueueThreadManager_ = threadManager;
  }

  /**
   * Set the maximum number of TEventConnection objects to cache in each worker
   * thread.
   *
   * By default, when a connection is closed the worker thread caches the
   * TEventConnection object and re-uses it when a new connection comes in,
   * instead of deleting it and allocating a new one for the next connection.
   * This value caps the number of unused TEventConnection objects that will be
   * cached.
   */
  void setMaxConnectionPoolSize(uint32_t size) {
    maxConnPoolSize_ = size;
  }

  /**
   * Set the maximum amount of time a connection may be idle before it is
   * closed.
   */
  void setRecvTimeout(int milliseconds) {
    recvTimeout_ = milliseconds;
  }

  /// Set the maximum allowed request frame size.
  void setMaxFrameSize(uint32_t maxFrameSize) {
    maxFrameSize_ = maxFrameSize;
  }

  /// Set the default write buffer size for new connections
  void setReadBufferDefaultSize(size_t size) {
    defaultReadBufferSize_ = size;
  }

  /// Set the default write buffer size for new connections
  void setWriteBufferDefaultSize(size_t size) {
    defaultWriteBufferSize_ = size;
  }

  /**
   * Set the maximum read buffer size for idle connections.
   *
   * This value is checked when the connection is closed, and also every N
   * connections if setResizeBufferEveryN() has been set.  If the read buffer
   * is larger than the allowed limit, free it.
   *
   * This prevents a single large request on a connection from continuing to
   * taking up a lot of buffer space.
   */
  void setIdleReadBufferLimit(size_t limit) {
    idleReadBufferLimit_ = limit;
  }

  /**
   * Set the maximum write buffer size for idle connections.
   *
   * This value is checked when the connection is closed, and also every N
   * connections if setResizeBufferEveryN() has been set.  If the write buffer
   * is larger than the allowed limit, free it.
   *
   * This prevents a single large response on a connection from continuing to
   * taking up a lot of buffer space.
   */
  void setIdleWriteBufferLimit(size_t limit) {
    idleWriteBufferLimit_ = limit;
  }

  /// Check a connection's buffer size limits every count requests.
  void setResizeBufferEveryN(int32_t count) {
    resizeBufferEveryN_ = count;
  }

  /**
   * Set the worker latency limit.
   *
   * Begin taking overload actions when it takes longer than this amount of
   * time to process a single iteration of the event loop.
   */
  void setWorkerLatencyLimit(int64_t milliseconds) {
    workerLatencyLimit_ = milliseconds;
  }

  virtual boost::shared_ptr<server::TServer> createServer();

  boost::shared_ptr<async::TEventServer> createEventServer();

 private:
  boost::shared_ptr<TProcessor> syncProcessor_;
  boost::shared_ptr<async::TAsyncProcessor> asyncProcessor_;
  boost::shared_ptr<concurrency::ThreadManager> taskQueueThreadManager_;
  uint16_t port_;
  size_t numIoThreads_;
  size_t numTaskThreads_;
  uint32_t maxConnPoolSize_;
  int recvTimeout_;
  uint32_t maxFrameSize_;
  size_t defaultReadBufferSize_;
  size_t defaultWriteBufferSize_;
  size_t idleReadBufferLimit_;
  size_t idleWriteBufferLimit_;
  int32_t resizeBufferEveryN_;
  int32_t workerLatencyLimit_;
};

}}} // apache::thrift::util

#endif // THRIFT_UTIL_TEVENTSERVERCREATOR_H_
